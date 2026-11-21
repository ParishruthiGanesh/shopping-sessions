//! Shopping Sessions, native pipeline. rules.md is the specification; the
//! stages below follow its order.
//!
//!   1. links    parse, union-find, name each person by their smallest ID
//!   2. parse    events and clicks, in parallel over chunks of the mapped files;
//!               each event copy is validated on the spot and sent to a
//!               partition chosen by its event_id
//!   3. dedup    per partition: keep the earliest-ingested copy of each event,
//!               then send the valid survivors to a partition chosen by person
//!   4. sessions per partition: group by person, sort by time, sessionize,
//!               credit and assist, and write the user_daily rows
//!   5. campaign_daily from the per-thread totals

mod key;
mod parquet;
mod scan;
mod time;

use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use key::{Key, KeyMap};
use parquet::{Column, Kind, Values, Writer};
use time::DAY;

const PARTITION_BITS: u32 = 8;
const PARTITIONS: usize = 1 << PARTITION_BITS;
const BLOCK_BITS: u32 = 12;
const BLOCK: usize = 1 << BLOCK_BITS;
const CHUNK: usize = 1 << 20;
const INVALID: u32 = u32::MAX;
const SESSION_GAP: u32 = 30 * 60 * 1000;
const WEEK: u32 = 7 * DAY as u32;
const ROW_GROUP: usize = 1 << 17;

// ------------------------------------------------------------------ inputs

/// The two libc calls this needs, declared here so the build has no dependencies at all.
mod libc {
    use std::ffi::{c_int, c_void};
    pub const PROT_READ: c_int = 1;
    pub const MAP_PRIVATE: c_int = 2;
    pub const MADV_WILLNEED: c_int = 3;
    pub const MAP_FAILED: *mut c_void = !0 as *mut c_void;
    extern "C" {
        pub fn mmap(addr: *mut c_void, len: usize, prot: c_int, flags: c_int, fd: c_int, offset: i64) -> *mut c_void;
        pub fn madvise(addr: *mut c_void, len: usize, advice: c_int) -> c_int;
    }
}

/// A read-only mapping of one input file.
struct Mapped {
    pointer: *const u8,
    len: usize,
}
// SAFETY: the mapping is read-only and lives until the process exits.
unsafe impl Sync for Mapped {}
unsafe impl Send for Mapped {}

impl Mapped {
    fn open(path: &Path) -> Option<Mapped> {
        let file = File::open(path).unwrap_or_else(|error| fail(&format!("{}: {error}", path.display())));
        let len = file.metadata().map(|meta| meta.len() as usize).unwrap_or(0);
        if len == 0 {
            return None;
        }
        // SAFETY: a private read-only mapping of an open file.
        let pointer = unsafe {
            libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, file.as_raw_fd(), 0)
        };
        if pointer == libc::MAP_FAILED {
            fail(&format!("{}: mmap failed", path.display()));
        }
        // SAFETY: advice on the mapping just made.
        unsafe { libc::madvise(pointer, len, libc::MADV_WILLNEED) };
        Some(Mapped { pointer: pointer.cast(), len })
    }
    fn bytes(&self) -> &[u8] {
        // SAFETY: the mapping is `len` bytes and never unmapped.
        unsafe { std::slice::from_raw_parts(self.pointer, self.len) }
    }
}

fn fail(message: &str) -> ! {
    eprintln!("shopping-sessions: {message}");
    std::process::exit(1);
}

/// Every `*.csv` in a folder, mapped, with its chunks: `(file, start, end)`.
fn map_folder(folder: &Path, files: &mut Vec<Mapped>) -> Vec<(usize, usize, usize)> {
    let mut paths: Vec<PathBuf> = match fs::read_dir(folder) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().is_some_and(|extension| extension == "csv") && path.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    paths.sort();
    let mut tasks = Vec::new();
    for path in paths {
        if let Some(mapped) = Mapped::open(&path) {
            let at = files.len();
            tasks.extend(scan::chunks(mapped.bytes(), CHUNK).into_iter().map(|(start, end)| (at, start, end)));
            files.push(mapped);
        }
    }
    // Largest chunks first, so the threads finish together.
    tasks.sort_by_key(|&(_, start, end)| std::cmp::Reverse(end - start));
    tasks
}

/// A two-column lookup file: `(key, second column)` per line.
fn read_lookup(path: &Path) -> Vec<(Key, Vec<u8>)> {
    let mut rows = Vec::new();
    if let Some(mapped) = Mapped::open(path) {
        let data = mapped.bytes();
        for (start, end) in scan::chunks(data, usize::MAX) {
            scan::scan::<2>(data, start, end, |fields| {
                rows.push((key::key(data, fields[0].0, fields[0].1), data[fields[1].0..fields[1].1].to_vec()));
            });
        }
    }
    rows
}

fn manifest_time(text: &[u8], name: &str) -> i64 {
    let find = |from: usize, needle: &[u8]| {
        text[from..].windows(needle.len()).position(|window| window == needle).map(|at| from + at)
    };
    let parsed = (|| {
        let at = find(0, format!("\"{name}\"").as_bytes())? + name.len() + 2;
        let open = find(find(at, b":")?, b"\"")? + 1;
        let close = find(open, b"\"")?;
        time::parse(&text[open..close])
    })();
    parsed.unwrap_or_else(|| fail(&format!("manifest.json: no valid {name}")))
}

// ------------------------------------------------------------------ records

/// Rows headed for one partition, in fixed-size blocks so nothing is ever moved.
struct Part<T> {
    blocks: Vec<Vec<T>>,
    current: Vec<T>,
}

impl<T> Part<T> {
    fn new() -> Self {
        Self { blocks: Vec::new(), current: Vec::new() }
    }
    #[cold]
    fn grow(&mut self) {
        let full = std::mem::replace(&mut self.current, Vec::with_capacity(BLOCK));
        if !full.is_empty() {
            self.blocks.push(full);
        }
    }
    #[inline(always)]
    fn push(&mut self, value: T) {
        if self.current.len() == self.current.capacity() {
            self.grow();
        }
        // SAFETY: there is spare capacity.
        unsafe {
            let len = self.current.len();
            self.current.as_mut_ptr().add(len).write(value);
            self.current.set_len(len + 1);
        }
    }
    fn into_blocks(mut self) -> Vec<Vec<T>> {
        if !self.current.is_empty() {
            self.blocks.push(self.current);
        }
        self.blocks
    }
}

fn parts<T>() -> Vec<Part<T>> {
    (0..PARTITIONS).map(|_| Part::new()).collect()
}

/// Regroups each thread's partitions into one list of blocks per partition.
fn gather<T>(threads: Vec<Vec<Part<T>>>) -> Vec<Mutex<Vec<Vec<T>>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for thread in threads {
        for (at, part) in thread.into_iter().enumerate() {
            out[at].extend(part.into_blocks());
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

#[inline(always)]
fn partition(key: Key) -> usize {
    (key::hash(key) >> (64 - PARTITION_BITS)) as usize
}

/// One copy of an event. `ts` is INVALID when the copy fails validation or is
/// late: it still takes part in the dedup, and if it is the kept copy the event is dropped.
#[repr(C)]
struct Event {
    id: [u64; 2],
    user: [u64; 2],
    ingest: u32, // seconds
    ts: u32,     // milliseconds since `base`
    price: u32,  // 0 unless a purchase
    category: u32,
}

/// A surviving event, by person.
#[repr(C)]
struct Visit {
    person: [u64; 2],
    ts: u32,
    price: u32,
    category: u32,
}

/// A valid click, by person.
#[repr(C)]
struct Click {
    person: [u64; 2],
    id: [u64; 2],
    ts: u32,
    campaign: u32,
}

#[inline(always)]
fn split(key: Key) -> [u64; 2] {
    [(key >> 64) as u64, key as u64]
}
#[inline(always)]
fn join(parts: [u64; 2]) -> Key {
    (u128::from(parts[0]) << 64) | u128::from(parts[1])
}

struct Context {
    window_start: i64,
    window_end: i64,
    /// `window_start - 7 days`: every stored timestamp is milliseconds after this.
    base: i64,
    products: KeyMap<u32>,
    unknown_category: u32,
    categories: Vec<Vec<u8>>,
    campaigns: KeyMap<u32>,
    campaign_rows: Vec<(Vec<u8>, Vec<u8>)>,
    days: usize,
    first_day: i64,
    day_shift: u64,
}

// ------------------------------------------------------------------ 1. links

/// The person (their smallest ID) of every ID that a link joins to another.
fn build_links(pairs: Vec<Vec<(Key, Key)>>) -> KeyMap<Key> {
    let total: usize = pairs.iter().map(Vec::len).sum();
    let mut ids: KeyMap<Key> = KeyMap::with_capacity(total + total / 4);
    let mut parent: Vec<u32> = Vec::with_capacity(total * 2);
    let mut keys: Vec<Key> = Vec::with_capacity(total * 2);
    fn find(parent: &mut [u32], mut at: u32) -> u32 {
        let mut root = at;
        while parent[root as usize] != root {
            root = parent[root as usize];
        }
        while parent[at as usize] != root {
            at = std::mem::replace(&mut parent[at as usize], root);
        }
        root
    }
    for &(a, b) in pairs.iter().flatten() {
        let mut index = |key: Key| {
            let next = keys.len() as u128;
            let index = ids.insert(key, next);
            if index == next {
                keys.push(key);
                parent.push(next as u32);
            }
            index as u32
        };
        let (x, y) = (index(a), index(b));
        let (x, y) = (find(&mut parent, x), find(&mut parent, y));
        if x != y {
            parent[x as usize] = y;
        }
    }
    let mut name = keys.clone();
    for at in 0..keys.len() {
        let root = find(&mut parent, at as u32) as usize;
        if key::compare(keys[at], name[root]).is_lt() {
            name[root] = keys[at];
        }
    }
    for slot in ids.slots_mut() {
        if slot.0 != 0 {
            slot.1 = name[parent[slot.1 as usize] as usize];
        }
    }
    ids
}

// ------------------------------------------------------------------ 2. parse

fn parse_events(context: &Context, data: &[u8], start: usize, end: usize, out: &mut [Part<Event>]) {
    scan::scan::<7>(data, start, end, |fields| {
        let id = key::key(data, fields[0].0, fields[0].1);
        if id == 0 {
            return;
        }
        let user = key::key(data, fields[1].0, fields[1].1);
        let ingest = time::parse(&data[fields[3].0..fields[3].1]);
        let mut event = Event {
            id: split(id),
            user: split(user),
            ingest: ingest.map_or(INVALID, |ms| ms.div_euclid(1000).clamp(0, i64::from(INVALID - 1)) as u32),
            ts: INVALID,
            price: 0,
            category: 0,
        };
        'valid: {
            if user == 0 || fields[5].0 == fields[5].1 {
                break 'valid;
            }
            let Some(ts) = time::parse(&data[fields[2].0..fields[2].1]) else { break 'valid };
            if ts < context.window_start || ts >= context.window_end {
                break 'valid;
            }
            let Some(ingest) = ingest else { break 'valid };
            if ingest - ts > DAY {
                break 'valid;
            }
            match &data[fields[4].0..fields[4].1] {
                b"view" | b"cart" => {}
                b"purchase" => {
                    let Some(price) = time::digits(&data[fields[6].0..fields[6].1], 8) else { break 'valid };
                    if !(1..=10_000_000).contains(&price) {
                        break 'valid;
                    }
                    event.price = price as u32;
                    let product = key::key(data, fields[5].0, fields[5].1);
                    event.category = context.products.get(product).unwrap_or(context.unknown_category);
                }
                _ => break 'valid,
            }
            event.ts = (ts - context.base) as u32;
        }
        out[partition(id)].push(event);
    });
}

fn parse_clicks(context: &Context, links: &KeyMap<Key>, data: &[u8], start: usize, end: usize, out: &mut [Part<Click>]) {
    scan::scan::<4>(data, start, end, |fields| {
        // Copies of a click are identical and nothing counts clicks, so no dedup is needed:
        // a second copy can never change which campaign is credited or assists.
        let id = key::key(data, fields[0].0, fields[0].1);
        let user = key::key(data, fields[1].0, fields[1].1);
        if id == 0 || user == 0 {
            return;
        }
        let Some(campaign) = context.campaigns.get(key::key(data, fields[2].0, fields[2].1)) else { return };
        let Some(ts) = time::parse(&data[fields[3].0..fields[3].1]) else { return };
        if ts < context.base || ts >= context.window_end {
            return;
        }
        let person = links.get(user).unwrap_or(user);
        out[partition(person)].push(Click { person: split(person), id: split(id), ts: (ts - context.base) as u32, campaign });
    });
}

// ------------------------------------------------------------------ 3. dedup

fn dedup(blocks: &mut [Vec<Event>], table: &mut Vec<u64>, links: &KeyMap<Key>, out: &mut [Part<Visit>]) {
    let total: usize = blocks.iter().map(Vec::len).sum();
    if total == 0 {
        return;
    }
    let size = (total * 2).next_power_of_two();
    table.clear();
    table.resize(size, 0);
    let mask = size - 1;
    let bases: Vec<*mut Event> = blocks.iter_mut().map(|block| block.as_mut_ptr()).collect();
    for (block_at, block) in blocks.iter().enumerate() {
        for at in 0..block.len() {
            // SAFETY: `at` is inside the block; nothing else holds a reference to this event.
            let event = unsafe { &mut *bases[block_at].add(at) };
            let hash = key::hash(join(event.id));
            let tag = hash & 0xFFFF_FFFF_0000_0000;
            let entry = tag | ((block_at << BLOCK_BITS | at) as u64 + 1);
            let mut slot = hash as usize & mask;
            loop {
                let seen = table[slot];
                if seen == 0 {
                    table[slot] = entry;
                    break;
                }
                if seen & 0xFFFF_FFFF_0000_0000 == tag {
                    let index = (seen as u32 - 1) as usize;
                    // SAFETY: the index was stored from a valid block and offset, and is not this event.
                    let other = unsafe { &mut *bases[index >> BLOCK_BITS].add(index & (BLOCK - 1)) };
                    if other.id == event.id {
                        // Two copies: the earlier ingest is kept, valid or not.
                        if event.ingest < other.ingest {
                            other.ts = INVALID;
                            table[slot] = entry;
                        } else {
                            event.ts = INVALID;
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
            }
        }
    }
    for block in blocks.iter() {
        for (at, event) in block.iter().enumerate() {
            if let Some(ahead) = block.get(at + 8) {
                links.prefetch(join(ahead.user));
            }
            if event.ts != INVALID {
                let user = join(event.user);
                let person = links.get(user).unwrap_or(user);
                out[partition(person)].push(Visit { person: split(person), ts: event.ts, price: event.price, category: event.category });
            }
        }
    }
}

// ------------------------------------------------------------------ 4. sessions

#[derive(Clone, Copy, Default)]
struct CampaignDay {
    sessions: u64,
    purchasing: u64,
    revenue: u64,
    assisted: u64,
}

#[derive(Clone, Copy)]
struct PersonClick {
    id: Key,
    ts: u32,
    campaign: u32,
}

const USER_COLUMNS: [Column; 6] = [
    Column { name: "user_id", kind: Kind::Text, nullable: false },
    Column { name: "day", kind: Kind::Date, nullable: false },
    Column { name: "sessions", kind: Kind::Int32, nullable: false },
    Column { name: "events", kind: Kind::Int32, nullable: false },
    Column { name: "revenue_cents", kind: Kind::Int64, nullable: false },
    Column { name: "top_category", kind: Kind::Text, nullable: true },
];

/// One thread's state for stage 4: scratch space, its user_daily file and its campaign totals.
struct Worker<'a> {
    context: &'a Context,
    path: PathBuf,
    writer: Option<Writer>,
    campaign_days: Vec<CampaignDay>,
    // Scratch, reused across partitions.
    table: Vec<u32>,
    people: Vec<Key>,
    visit_person: Vec<u32>,
    visit_start: Vec<u32>,
    visits: Vec<u64>,
    purchases: Vec<(u32, u32)>,
    click_person: Vec<u32>,
    click_start: Vec<u32>,
    clicks: Vec<PersonClick>,
    session_start: Vec<u32>,
    session_day: Vec<u32>,
    category_revenue: Vec<u64>,
    touched: Vec<u32>,
    // The row group being built.
    rows: usize,
    user_id: Vec<u8>,
    day: Vec<u8>,
    sessions: Vec<u8>,
    events: Vec<u8>,
    revenue: Vec<u8>,
    top_valid: Vec<u8>,
    top: Vec<u8>,
}

impl<'a> Worker<'a> {
    fn new(context: &'a Context, path: PathBuf) -> Self {
        Self {
            context,
            path,
            writer: None,
            campaign_days: vec![CampaignDay::default(); (context.campaign_rows.len() + 1) * context.days],
            table: Vec::new(),
            people: Vec::new(),
            visit_person: Vec::new(),
            visit_start: Vec::new(),
            visits: Vec::new(),
            purchases: Vec::new(),
            click_person: Vec::new(),
            click_start: Vec::new(),
            clicks: Vec::new(),
            session_start: Vec::new(),
            session_day: Vec::new(),
            category_revenue: vec![0; context.categories.len()],
            touched: Vec::new(),
            rows: 0,
            user_id: Vec::new(),
            day: Vec::new(),
            sessions: Vec::new(),
            events: Vec::new(),
            revenue: Vec::new(),
            top_valid: Vec::new(),
            top: Vec::new(),
        }
    }

    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }
        let writer = self.writer.get_or_insert_with(|| {
            Writer::create(&self.path, &USER_COLUMNS).unwrap_or_else(|error| fail(&format!("{}: {error}", self.path.display())))
        });
        let plain = |data| Values { data, validity: None };
        let columns = [
            plain(&self.user_id[..]),
            plain(&self.day),
            plain(&self.sessions),
            plain(&self.events),
            plain(&self.revenue),
            Values { data: &self.top, validity: Some(&self.top_valid) },
        ];
        writer.row_group(self.rows, &columns).unwrap_or_else(|error| fail(&format!("write: {error}")));
        self.rows = 0;
        for buffer in [&mut self.user_id, &mut self.day, &mut self.sessions, &mut self.events, &mut self.revenue, &mut self.top_valid, &mut self.top] {
            buffer.clear();
        }
    }

    /// One user_daily row; the top category comes from `category_revenue`, which is reset.
    fn row(&mut self, person: Key, day: u32, sessions: u32, events: u32, revenue: u64) {
        let length_at = self.user_id.len();
        self.user_id.extend_from_slice(&[0; 4]);
        key::write_bytes(person, &mut self.user_id);
        let length = (self.user_id.len() - length_at - 4) as u32;
        self.user_id[length_at..length_at + 4].copy_from_slice(&length.to_le_bytes());
        self.day.extend_from_slice(&((self.context.first_day + i64::from(day)) as i32).to_le_bytes());
        self.sessions.extend_from_slice(&sessions.to_le_bytes());
        self.events.extend_from_slice(&events.to_le_bytes());
        self.revenue.extend_from_slice(&revenue.to_le_bytes());
        if self.rows % 8 == 0 {
            self.top_valid.push(0);
        }
        if !self.touched.is_empty() {
            let categories = &self.context.categories;
            let mut best = self.touched[0];
            for &category in &self.touched[1..] {
                let (sum, best_sum) = (self.category_revenue[category as usize], self.category_revenue[best as usize]);
                if sum > best_sum || (sum == best_sum && categories[category as usize] < categories[best as usize]) {
                    best = category;
                }
            }
            for &category in &self.touched {
                self.category_revenue[category as usize] = 0;
            }
            self.touched.clear();
            let name = &categories[best as usize];
            self.top.extend_from_slice(&(name.len() as u32).to_le_bytes());
            self.top.extend_from_slice(name);
            *self.top_valid.last_mut().unwrap() |= 1 << (self.rows % 8);
        }
        self.rows += 1;
    }

    fn partition(&mut self, visit_blocks: &[Vec<Visit>], click_blocks: &[Vec<Click>]) {
        let total: usize = visit_blocks.iter().map(Vec::len).sum();
        if total == 0 {
            return;
        }
        // Number the people of this partition and count their events.
        let size = (total * 2).next_power_of_two();
        let mask = size - 1;
        self.table.clear();
        self.table.resize(size, 0);
        self.people.clear();
        self.visit_person.clear();
        self.visit_start.clear();
        for visit in visit_blocks.iter().flatten() {
            let person = join(visit.person);
            let mut slot = key::hash(person) as usize & mask;
            let index = loop {
                let seen = self.table[slot];
                if seen == 0 {
                    self.people.push(person);
                    self.visit_start.push(0);
                    self.table[slot] = self.people.len() as u32;
                    break self.people.len() as u32 - 1;
                }
                if self.people[seen as usize - 1] == person {
                    break seen - 1;
                }
                slot = (slot + 1) & mask;
            };
            self.visit_person.push(index);
            self.visit_start[index as usize] += 1;
        }
        let people = self.people.len();
        // Counts to start offsets, then place each event with its person: time in
        // the high half for sorting, and a reference to its purchase in the low half.
        let mut sum = 0u32;
        for count in self.visit_start.iter_mut() {
            sum += std::mem::replace(count, sum);
        }
        self.visit_start.push(sum);
        self.visits.clear();
        self.visits.resize(total, 0);
        self.purchases.clear();
        for (visit, &person) in visit_blocks.iter().flatten().zip(&self.visit_person) {
            let mut value = u64::from(visit.ts) << 32;
            if visit.price != 0 {
                self.purchases.push((visit.price, visit.category));
                value |= self.purchases.len() as u64;
            }
            let at = &mut self.visit_start[person as usize];
            self.visits[*at as usize] = value;
            *at += 1;
        }
        // The same for the clicks of people who have events.
        self.click_person.clear();
        self.click_start.clear();
        self.click_start.resize(people + 1, 0);
        for click in click_blocks.iter().flatten() {
            let person = join(click.person);
            let mut slot = key::hash(person) as usize & mask;
            let index = loop {
                let seen = self.table[slot];
                if seen == 0 {
                    break u32::MAX;
                }
                if self.people[seen as usize - 1] == person {
                    break seen - 1;
                }
                slot = (slot + 1) & mask;
            };
            self.click_person.push(index);
            if index != u32::MAX {
                self.click_start[index as usize] += 1;
            }
        }
        let mut sum = 0u32;
        for count in self.click_start.iter_mut() {
            sum += std::mem::replace(count, sum);
        }
        self.clicks.clear();
        self.clicks.resize(sum as usize, PersonClick { id: 0, ts: 0, campaign: 0 });
        for (click, &person) in click_blocks.iter().flatten().zip(&self.click_person) {
            if person != u32::MAX {
                let at = &mut self.click_start[person as usize];
                self.clicks[*at as usize] = PersonClick { id: join(click.id), ts: click.ts, campaign: click.campaign };
                *at += 1;
            }
        }
        // After placing, each start offset has moved to its person's end.
        let mut visits = std::mem::take(&mut self.visits);
        let mut clicks = std::mem::take(&mut self.clicks);
        let purchases = std::mem::take(&mut self.purchases);
        let (mut visit_from, mut click_from) = (0usize, 0usize);
        for person in 0..people {
            let (visit_to, click_to) = (self.visit_start[person] as usize, self.click_start[person] as usize);
            self.person(self.people[person], &mut visits[visit_from..visit_to], &mut clicks[click_from..click_to], &purchases);
            (visit_from, click_from) = (visit_to, click_to);
        }
        (self.visits, self.clicks, self.purchases) = (visits, clicks, purchases);
        if self.rows >= ROW_GROUP {
            self.flush();
        }
    }

    /// One person: their events (time << 32 | purchase reference) and their valid clicks.
    fn person(&mut self, person: Key, visits: &mut [u64], clicks: &mut [PersonClick], purchases: &[(u32, u32)]) {
        let context = self.context;
        let days = context.days;
        let organic = context.campaign_rows.len();
        visits.sort_unstable();
        // By time; within one instant the smallest click_id first.
        clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key::compare(a.id, b.id)));
        self.session_start.clear();
        self.session_day.clear();

        let (mut next_click, mut best) = (0usize, usize::MAX);
        let (mut row_day, mut row_sessions, mut row_events, mut row_revenue) = (u32::MAX, 0u32, 0u32, 0u64);
        let mut at = 0;
        while at < visits.len() {
            // A session: from `at` until a gap of over 30 minutes.
            let start = (visits[at] >> 32) as u32;
            let day = ((u64::from(start) + context.day_shift) / DAY as u64) as u32 - 7;
            if day != row_day {
                if row_day != u32::MAX {
                    self.row(person, row_day, row_sessions, row_events, row_revenue);
                }
                (row_day, row_sessions, row_events, row_revenue) = (day, 0, 0, 0);
            }
            let first = at;
            let (mut previous, mut revenue, mut purchased) = (start, 0u64, false);
            while at < visits.len() {
                let ts = (visits[at] >> 32) as u32;
                if ts - previous > SESSION_GAP {
                    break;
                }
                previous = ts;
                let purchase = visits[at] as u32;
                if purchase != 0 {
                    let (price, category) = purchases[purchase as usize - 1];
                    revenue += u64::from(price);
                    purchased = true;
                    let sum = &mut self.category_revenue[category as usize];
                    if *sum == 0 {
                        self.touched.push(category);
                    }
                    *sum += u64::from(price);
                }
                at += 1;
            }
            // Credit: the latest click at or before the start, if within 7 days.
            while next_click < clicks.len() && clicks[next_click].ts <= start {
                if best == usize::MAX || clicks[next_click].ts != clicks[best].ts {
                    best = next_click;
                }
                next_click += 1;
            }
            let campaign = if best != usize::MAX && start - clicks[best].ts <= WEEK { clicks[best].campaign as usize } else { organic };
            let total = &mut self.campaign_days[campaign * days + day as usize];
            total.sessions += 1;
            total.purchasing += u64::from(purchased);
            total.revenue += revenue;
            row_sessions += 1;
            row_events += (at - first) as u32;
            row_revenue += revenue;
            self.session_start.push(start);
            self.session_day.push(day);
        }
        if row_day != u32::MAX {
            self.row(person, row_day, row_sessions, row_events, row_revenue);
        }
        if clicks.is_empty() {
            return;
        }
        // Assists: per campaign, merge the 7 days after each click into intervals;
        // every session starting inside one is assisted once.
        clicks.sort_unstable_by_key(|click| (click.campaign, click.ts));
        let (starts, session_days) = (&self.session_start[..], &self.session_day[..]);
        let mut at = 0;
        while at < clicks.len() {
            let campaign = clicks[at].campaign;
            let from = clicks[at].ts;
            let mut to = from + WEEK;
            at += 1;
            while at < clicks.len() && clicks[at].campaign == campaign && clicks[at].ts <= to {
                to = clicks[at].ts + WEEK;
                at += 1;
            }
            let mut session = starts.partition_point(|&start| start < from);
            let end = session + starts[session..].partition_point(|&start| start <= to);
            while session < end {
                let day = session_days[session];
                let same = session_days[session..end].partition_point(|&other| other == day);
                self.campaign_days[campaign as usize * days + day as usize].assisted += same as u64;
                session += same;
            }
        }
    }
}

// ------------------------------------------------------------------ main

fn main() {
    let started = Instant::now();
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let data_dir = PathBuf::from(env("DATA_DIR", "/data"));
    let output_dir = PathBuf::from(env("OUTPUT_DIR", "/output"));
    let threads = env("NCPU", "0")
        .parse::<usize>()
        .ok()
        .filter(|&count| count > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from));
    let log = |stage: &str| eprintln!("{:8.3}s {stage}", started.elapsed().as_secs_f64());

    let manifest = fs::read(data_dir.join("manifest.json")).unwrap_or_else(|error| fail(&format!("manifest.json: {error}")));
    let window_start = manifest_time(&manifest, "window_start");
    let window_end = manifest_time(&manifest, "window_end");
    let base = window_start - 7 * DAY;
    if window_end - base + 7 * DAY >= i64::from(u32::MAX) {
        fail("the window is too long for 32-bit millisecond offsets (limit about 35 days)");
    }

    let mut categories: Vec<Vec<u8>> = Vec::new();
    let mut category_ids: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
    let product_rows = read_lookup(&data_dir.join("products.csv"));
    let mut products = KeyMap::with_capacity(product_rows.len());
    for (product, category) in product_rows {
        let id = *category_ids.entry(category.clone()).or_insert_with(|| {
            categories.push(category);
            categories.len() as u32 - 1
        });
        if product != 0 {
            products.insert(product, id);
        }
    }
    categories.push(b"unknown".to_vec());
    let campaign_rows = read_lookup(&data_dir.join("campaigns.csv"));
    let mut campaigns = KeyMap::with_capacity(campaign_rows.len());
    let mut campaign_names = Vec::new();
    for (campaign, channel) in campaign_rows {
        if campaign != 0 && campaigns.insert(campaign, campaign_names.len() as u32) == campaign_names.len() as u32 {
            let mut name = Vec::new();
            key::write_bytes(campaign, &mut name);
            campaign_names.push((name, channel));
        }
    }
    let first_day = window_start.div_euclid(DAY);
    let context = Context {
        window_start,
        window_end,
        base,
        products,
        unknown_category: categories.len() as u32 - 1,
        categories,
        campaigns,
        campaign_rows: campaign_names,
        days: ((window_end - 1).div_euclid(DAY) - first_day + 1).max(1) as usize,
        first_day,
        day_shift: window_start.rem_euclid(DAY) as u64,
    };
    let context = &context;

    let mut files = Vec::new();
    let link_tasks = map_folder(&data_dir.join("links"), &mut files);
    let event_tasks = map_folder(&data_dir.join("events"), &mut files);
    let click_tasks = map_folder(&data_dir.join("clicks"), &mut files);
    let files = &files;
    log("mapped inputs");

    // 1 and 2. One thread parses the links and joins them while the others start
    // on the events; the clicks wait for the links, since they are sent by person.
    let links: OnceLock<KeyMap<Key>> = OnceLock::new();
    let (next_event, next_click) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let parsed: Vec<(Vec<Part<Event>>, Vec<Part<Click>>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (links, link_tasks, event_tasks, click_tasks) = (&links, &link_tasks, &event_tasks, &click_tasks);
                let (next_event, next_click) = (&next_event, &next_click);
                scope.spawn(move || {
                    if thread == 0 {
                        let mut pairs = Vec::new();
                        for &(file, start, end) in link_tasks {
                            let data = files[file].bytes();
                            scan::scan::<2>(data, start, end, |fields| {
                                let a = key::key(data, fields[0].0, fields[0].1);
                                let b = key::key(data, fields[1].0, fields[1].1);
                                if a != 0 && b != 0 && a != b {
                                    pairs.push((a, b));
                                }
                            });
                        }
                        let _ = links.set(build_links(vec![pairs]));
                        log("links joined");
                    }
                    let (mut events, mut clicks) = (parts(), parts());
                    while let Some(&(file, start, end)) = event_tasks.get(next_event.fetch_add(1, Ordering::Relaxed)) {
                        parse_events(context, files[file].bytes(), start, end, &mut events);
                    }
                    let links = loop {
                        match links.get() {
                            Some(links) => break links,
                            None => std::thread::yield_now(),
                        }
                    };
                    while let Some(&(file, start, end)) = click_tasks.get(next_click.fetch_add(1, Ordering::Relaxed)) {
                        parse_clicks(context, links, files[file].bytes(), start, end, &mut clicks);
                    }
                    (events, clicks)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let links = links.get().unwrap();
    let (events, clicks): (Vec<_>, Vec<_>) = parsed.into_iter().unzip();
    let (events, clicks) = (gather(events), gather(clicks));
    log("parsed events and clicks");

    // 3. Dedup each event partition and regroup the survivors by person.
    let next = AtomicUsize::new(0);
    let visits: Vec<Vec<Part<Visit>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (events, next) = (&events, &next);
                scope.spawn(move || {
                    let mut out = parts();
                    let mut table = Vec::new();
                    while let Some(blocks) = events.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let mut blocks = std::mem::take(&mut *blocks.lock().unwrap());
                        dedup(&mut blocks, &mut table, links, &mut out);
                        std::mem::forget(blocks); // freed at exit; unmapping now would only cost time
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let visits = gather(visits);
    log("deduplicated events");

    // 4. Sessions, credit and assists per person partition; each thread writes its own user_daily file.
    let user_dir = output_dir.join("user_daily");
    let campaign_dir = output_dir.join("campaign_daily");
    for folder in [&user_dir, &campaign_dir] {
        fs::create_dir_all(folder).unwrap_or_else(|error| fail(&format!("{}: {error}", folder.display())));
    }
    let next = AtomicUsize::new(0);
    let totals: Vec<(Vec<CampaignDay>, bool)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|thread| {
                let (visits, clicks, next, user_dir) = (&visits, &clicks, &next, &user_dir);
                scope.spawn(move || {
                    let mut worker = Worker::new(context, user_dir.join(format!("part-{thread}.parquet")));
                    loop {
                        let at = next.fetch_add(1, Ordering::Relaxed);
                        if at >= PARTITIONS {
                            break;
                        }
                        let visit_blocks = std::mem::take(&mut *visits[at].lock().unwrap());
                        let click_blocks = std::mem::take(&mut *clicks[at].lock().unwrap());
                        worker.partition(&visit_blocks, &click_blocks);
                        std::mem::forget((visit_blocks, click_blocks));
                    }
                    worker.flush();
                    let wrote = worker.writer.is_some();
                    if let Some(writer) = worker.writer.take() {
                        writer.finish().unwrap_or_else(|error| fail(&format!("write: {error}")));
                    }
                    (std::mem::take(&mut worker.campaign_days), wrote)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    if !totals.iter().any(|&(_, wrote)| wrote) {
        // No rows at all: the table still needs a file with its schema.
        let writer = Writer::create(&user_dir.join("part-0.parquet"), &USER_COLUMNS);
        writer.and_then(Writer::finish).unwrap_or_else(|error| fail(&format!("write: {error}")));
    }
    log("sessions done, user_daily written");

    // 5. campaign_daily: add up the threads' totals.
    let columns = [
        Column { name: "campaign_id", kind: Kind::Text, nullable: false },
        Column { name: "channel", kind: Kind::Text, nullable: false },
        Column { name: "day", kind: Kind::Date, nullable: false },
        Column { name: "sessions", kind: Kind::Int64, nullable: false },
        Column { name: "purchasing_sessions", kind: Kind::Int64, nullable: false },
        Column { name: "revenue_cents", kind: Kind::Int64, nullable: false },
        Column { name: "assisted_sessions", kind: Kind::Int64, nullable: false },
    ];
    let mut data: [Vec<u8>; 7] = Default::default();
    let mut rows = 0;
    let text = |out: &mut Vec<u8>, value: &[u8]| {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    };
    for campaign in 0..=context.campaign_rows.len() {
        for day in 0..context.days {
            let mut sum = CampaignDay::default();
            for (total, _) in &totals {
                let total = &total[campaign * context.days + day];
                sum.sessions += total.sessions;
                sum.purchasing += total.purchasing;
                sum.revenue += total.revenue;
                sum.assisted += total.assisted;
            }
            if sum.sessions == 0 && sum.assisted == 0 {
                continue;
            }
            let (name, channel): (&[u8], &[u8]) = match context.campaign_rows.get(campaign) {
                Some((name, channel)) => (name, channel),
                None => (b"organic", b"organic"),
            };
            text(&mut data[0], name);
            text(&mut data[1], channel);
            data[2].extend_from_slice(&((context.first_day + day as i64) as i32).to_le_bytes());
            for (column, value) in [sum.sessions, sum.purchasing, sum.revenue, sum.assisted].into_iter().enumerate() {
                data[3 + column].extend_from_slice(&value.to_le_bytes());
            }
            rows += 1;
        }
    }
    let values: Vec<Values> = data.iter().map(|data| Values { data, validity: None }).collect();
    let written = Writer::create(&campaign_dir.join("part-0.parquet"), &columns).and_then(|mut writer| {
        writer.row_group(rows, &values)?;
        writer.finish()
    });
    written.unwrap_or_else(|error| fail(&format!("campaign_daily: {error}")));
    log("campaign_daily written");
    // Leave without unmapping or freeing anything: the kernel does it faster.
    std::process::exit(0);
}
