//! Shopping Sessions, native pipeline. rules.md is the specification; the
//! stages below follow its order.
//!
//!   1. links    parse, union-find (lock-free, all threads), name each person
//!               by their smallest ID
//!   2. parse    events and clicks, in parallel over chunks of the mapped files;
//!               each event copy is validated on the spot. A valid copy goes
//!               straight to its person's partition as a 12-byte Visit, the one
//!               shuffle of the events; every copy, valid or not, also leaves a
//!               16-byte Sighting (event_id, ingest_ts, where the Visit went),
//!               bucketed by event_id
//!   3. dedup    per bucket of Sightings: every copy but the earliest-ingested
//!               of each event_id is marked killed in its partition
//!   4. sessions per partition: drop the killed Visits, group by person, sort
//!               by time, sessionize, credit and assist, write user_daily rows
//!   5. campaign_daily from the per-thread totals

// The scanners and the ID packing are written for AVX2. Cargo reads native/.cargo/config.toml
// only when it runs inside native/: built from elsewhere, the binary would be silently slow.
#[cfg(not(all(target_feature = "avx2", target_feature = "bmi2")))]
compile_error!("build inside native/ so that .cargo/config.toml applies (it enables AVX2 and BMI2)");

mod bump;
mod key;
mod parquet;
mod scan;
mod time;

#[global_allocator]
static ALLOCATOR: bump::Bump = bump::Bump;

use std::fs::{self, File};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use key::{Key, KeyMap};
use parquet::{Column, Kind, Values, Writer};
use time::DAY;

const PARTITION_BITS: u32 = 8;
const PARTITIONS: usize = 1 << PARTITION_BITS;
const CHUNK: usize = 1 << 18;
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
/// Only the header is read here; the bounds are nominal, and `chunk` moves each
/// to the start of a line when a thread takes it. Finding every line end here
/// took 16 ms at 20M events: one page fault per chunk, one after another.
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
            let data = mapped.bytes();
            // After the header line; a file that is only a header (or not even that) has no lines.
            if let Some(header) = data.iter().position(|&byte| byte == b'\n') {
                let at = files.len();
                let mut start = header + 1;
                while start < data.len() {
                    let end = start.saturating_add(CHUNK).min(data.len());
                    tasks.push((at, start, end));
                    start = end;
                }
            }
            files.push(mapped);
        }
    }
    // Largest chunks first, so the threads finish together.
    tasks.sort_by_key(|&(_, start, end)| std::cmp::Reverse(end - start));
    tasks
}

/// The lines of a task from `map_folder`: each bound moves to the start of the
/// first line at or after it, so a line belongs to the chunk it starts in.
/// The first chunk of a file starts right after the header, on a line start already.
fn chunk<'a>(files: &'a [Mapped], &(file, start, end): &(usize, usize, usize)) -> (&'a [u8], usize, usize) {
    let data = files[file].bytes();
    let line_start = |at: usize| {
        if at >= data.len() || data[at - 1] == b'\n' {
            return at.min(data.len());
        }
        data[at..].iter().position(|&byte| byte == b'\n').map_or(data.len(), |offset| at + offset + 1)
    };
    let (start, end) = (line_start(start), line_start(end));
    (data, start, end.max(start))
}

/// A two-column lookup file: `(key, second column)` per line.
fn read_lookup(path: &Path) -> Vec<(Key, Vec<u8>)> {
    let mut rows = Vec::new();
    if let Some(mapped) = Mapped::open(path) {
        let data = mapped.bytes();
        for (start, end) in scan::chunks(data, usize::MAX) {
            scan::scan::<2>(&mut scan::Index::default(), data, start, end, |fields| {
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
        time::parse(&text[open..close], &mut time::Dates::new())
    })();
    parsed.unwrap_or_else(|| fail(&format!("manifest.json: no valid {name}")))
}

// ------------------------------------------------------------------ records

/// Bytes per block of rows. Every row type uses the same size, so a block
/// that one stage has finished with can hold another stage's rows. Small, since
/// every thread has a block open for each stream and partition, and the huge
/// pages of the bump region are zeroed whole when first touched (24 KB rather
/// than 192 KB: 3.9 -> 3.1 GB peak at 20M events, and 0.035 s faster).
const BLOCK_BYTES: usize = 48 << 9;
const SLAB_BYTES: usize = 64 * BLOCK_BYTES;

/// A thread's supply of blocks: carved from large slabs, and reused when given back.
struct Arena {
    free: Vec<*mut u8>,
    cursor: *mut u8,
    left: usize,
}

thread_local! {
    static ARENA: std::cell::RefCell<Arena> = const { std::cell::RefCell::new(Arena { free: Vec::new(), cursor: std::ptr::null_mut(), left: 0 }) };
}

impl Arena {
    fn take() -> *mut u8 {
        ARENA.with_borrow_mut(|arena| {
            if let Some(block) = arena.free.pop() {
                return block;
            }
            if arena.left == 0 {
                let layout = std::alloc::Layout::from_size_align(SLAB_BYTES, 4096).unwrap();
                // SAFETY: the layout has a size.
                let slab = unsafe { std::alloc::alloc(layout) };
                if slab.is_null() {
                    fail("out of memory");
                }
                (arena.cursor, arena.left) = (slab, SLAB_BYTES / BLOCK_BYTES);
            }
            let block = arena.cursor;
            // SAFETY: the slab has `left` blocks after `cursor`.
            arena.cursor = unsafe { block.add(BLOCK_BYTES) };
            arena.left -= 1;
            block
        })
    }
    fn give(block: *mut u8) {
        ARENA.with_borrow_mut(|arena| arena.free.push(block));
    }
}

/// A block of rows: `len` of them at `rows`.
struct Block<T> {
    rows: *mut T,
    len: usize,
}
// SAFETY: a block is owned by whoever holds it, like a Vec.
unsafe impl<T: Send> Send for Block<T> {}

impl<T> Block<T> {
    const CAPACITY: usize = BLOCK_BYTES / std::mem::size_of::<T>();
    fn rows(&self) -> &[T] {
        // SAFETY: `len` rows were written at `rows`.
        unsafe { std::slice::from_raw_parts(self.rows, self.len) }
    }
    /// Hands the memory back to this thread's arena. The rows need no dropping.
    fn recycle(self) {
        Arena::give(self.rows.cast());
    }
}

/// Bytes of rows a partition gathers before writing them out: three cache lines.
const STAGE: usize = 192;

#[repr(C, align(64))]
struct Stage([u8; STAGE]);

/// One thread's rows headed for each of the partitions, in fixed-size blocks so
/// nothing is ever moved.
///
/// Rows are gathered in a small staging buffer per partition and written to the
/// block three whole cache lines at a time with streaming stores. Writing rows
/// straight to 256 blocks made every store miss: each new line had to be read
/// from memory before it could be written. The counts that say where the next
/// row goes are kept apart, in one small array that stays in the L1 cache, so a
/// row costs one staging line and nothing else.
struct Parts<T> {
    /// Rows pushed to each partition: the number the next one gets. Buffers are
    /// written out whenever they fill, so the count also says how full each is.
    counts: Vec<u32>,
    stages: Vec<Stage>,
    /// The block being filled, and how many rows are in it.
    current: Vec<*mut T>,
    filled: Vec<usize>,
    blocks: Vec<Vec<Block<T>>>,
}
// SAFETY: as for Block.
unsafe impl<T: Send> Send for Parts<T> {}

impl<T> Parts<T> {
    /// Rows per staging buffer. A block holds a whole number of buffers, and
    /// starts on a page, so every write of the buffer starts on a cache line.
    const PER_STAGE: usize = {
        assert!(STAGE % std::mem::size_of::<T>() == 0 && BLOCK_BYTES % STAGE == 0);
        STAGE / std::mem::size_of::<T>()
    };

    fn new() -> Self {
        Self {
            counts: vec![0; PARTITIONS],
            stages: (0..PARTITIONS).map(|_| Stage([0; STAGE])).collect(),
            current: vec![std::ptr::null_mut(); PARTITIONS],
            // "Full" until the first write, so that empty partitions cost nothing.
            filled: vec![Block::<T>::CAPACITY; PARTITIONS],
            blocks: (0..PARTITIONS).map(|_| Vec::new()).collect(),
        }
    }

    #[inline(always)]
    fn count(&self, at: usize) -> u32 {
        self.counts[at]
    }

    #[inline(always)]
    fn push(&mut self, at: usize, value: T) {
        // SAFETY: `at` is a partition number, below PARTITIONS, the length of
        // every list; the staged count is below PER_STAGE, so the row fits.
        unsafe {
            let count = self.counts.get_unchecked_mut(at);
            let staged = *count as usize % Self::PER_STAGE;
            *count += 1;
            self.stages.get_unchecked_mut(at).0.as_mut_ptr().cast::<T>().add(staged).write_unaligned(value);
            if staged + 1 == Self::PER_STAGE {
                self.drain(at);
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, at: usize) {
        if !self.current[at].is_null() {
            self.blocks[at].push(Block { rows: self.current[at], len: self.filled[at] });
        }
        self.current[at] = Arena::take().cast();
        self.filled[at] = 0;
    }

    /// Writes the full staging buffer to the block.
    #[inline(never)]
    fn drain(&mut self, at: usize) {
        if self.filled[at] == Block::<T>::CAPACITY {
            self.grow(at);
        }
        // SAFETY: the block has room for PER_STAGE more rows (its capacity is a
        // multiple of PER_STAGE), and the destination is 64-byte aligned.
        unsafe {
            use std::arch::x86_64::{__m256i, _mm256_load_si256, _mm256_stream_si256};
            let to = self.current[at].add(self.filled[at]).cast::<__m256i>();
            let from = self.stages[at].0.as_ptr().cast::<__m256i>();
            for step in 0..STAGE / 32 {
                _mm256_stream_si256(to.add(step), _mm256_load_si256(from.add(step)));
            }
        }
        self.filled[at] += Self::PER_STAGE;
    }

    /// Writes out every partly filled staging buffer and makes the streaming
    /// stores visible to other threads. Call on the thread that pushed, last.
    fn seal(&mut self) {
        for at in 0..PARTITIONS {
            let staged = self.counts[at] as usize % Self::PER_STAGE;
            if staged == 0 {
                continue;
            }
            if self.filled[at] == Block::<T>::CAPACITY {
                self.grow(at);
            }
            // SAFETY: `staged` rows are in the buffer and fit in the block, which
            // has room for a whole buffer.
            unsafe {
                std::ptr::copy_nonoverlapping(self.stages[at].0.as_ptr(), self.current[at].add(self.filled[at]).cast::<u8>(), staged * std::mem::size_of::<T>());
            }
            self.filled[at] += staged;
        }
        // SAFETY: a store fence has no preconditions.
        unsafe { std::arch::x86_64::_mm_sfence() };
    }

    /// Each partition's blocks. Call after `seal`.
    fn into_blocks(mut self) -> Vec<Vec<Block<T>>> {
        for at in 0..PARTITIONS {
            if !self.current[at].is_null() {
                self.blocks[at].push(Block { rows: self.current[at], len: self.filled[at] });
            }
        }
        self.blocks
    }
}

/// Regroups each thread's partitions into one list of blocks per partition.
fn gather<T>(threads: Vec<Parts<T>>) -> Vec<Mutex<Vec<Block<T>>>> {
    let mut out: Vec<Vec<Block<T>>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for thread in threads {
        for (at, blocks) in thread.into_blocks().into_iter().enumerate() {
            out[at].extend(blocks);
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

/// Regroups each thread's partitions into one list per partition of each thread's blocks, in thread order.
fn gather_threads<T>(threads: Vec<Parts<T>>) -> Vec<Mutex<Vec<Vec<Block<T>>>>> {
    let mut out: Vec<Vec<Vec<Block<T>>>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for thread in threads {
        for (at, blocks) in thread.into_blocks().into_iter().enumerate() {
            out[at].push(blocks);
        }
    }
    out.into_iter().map(Mutex::new).collect()
}

#[inline(always)]
fn partition(key: Key) -> usize {
    (key::hash(key) >> (64 - PARTITION_BITS)) as usize
}

/// A valid copy of an event, sent to its person's partition. `ts` is milliseconds
/// since `base` (below 2^31), with PURCHASE set for a purchase: its price and
/// category are in the partition's purchase rows, in the same order.
#[derive(Clone, Copy)]
#[repr(C)]
struct Visit {
    person: [u32; 2],
    ts: u32,
}

const PURCHASE: u32 = 1 << 31;

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Purchase {
    price: u32,
    category: u32,
}

/// Every copy of an event, valid or not, by event_id: what the dedup looks at.
/// `place` says where the copy's Visit went: its partition << 24 and its number
/// among the Visits this thread sent there, or NOWHERE for an invalid copy.
#[repr(C)]
struct Sighting {
    id: Key,
    ingest: u32, // seconds
    place: u32,
}

const NOWHERE: u32 = u32::MAX;
const NUMBER_BITS: u32 = 24;

#[inline(always)]
fn halves(key: Key) -> [u32; 2] {
    [key as u32, (key >> 32) as u32]
}
#[inline(always)]
fn whole(halves: [u32; 2]) -> Key {
    u64::from(halves[0]) | u64::from(halves[1]) << 32
}

/// A valid click, by person.
#[derive(Clone, Copy)]
#[repr(C)]
struct Click {
    person: Key,
    id: Key,
    ts: u32,
    campaign: u32,
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

const SHARD_BITS: u32 = 8;
const SHARDS: usize = 1 << SHARD_BITS;

/// The person (their smallest ID) of every ID that a link joins to another,
/// in shards by the top bits of the ID's hash so the threads can build it together.
struct Links {
    shards: Vec<KeyMap<Key>>,
}

impl Links {
    #[inline(always)]
    fn person(&self, user: Key) -> Key {
        let hash = key::hash(user);
        self.shards[(hash >> (64 - SHARD_BITS)) as usize].get_hashed(user, hash).unwrap_or(user)
    }
    #[inline(always)]
    fn prefetch(&self, user: Key) {
        let hash = key::hash(user);
        self.shards[(hash >> (64 - SHARD_BITS)) as usize].prefetch_hashed(hash);
    }
}

/// One shard's IDs, numbered in the order they were first seen.
#[derive(Default)]
struct Shard {
    ids: Option<KeyMap<Key>>,
    keys: Vec<Key>,
    numbers: Vec<Vec<u32>>,
}

/// One side of a link on its way to a shard: the ID, and `link << 1 | side`
/// with links numbered within the thread that parsed them.
type Side = (Key, u32);

/// Step 1, by many threads: number the IDs of each shard. `sides[thread][shard]` are the
/// parsed links; the shard keeps each side's number in the same order, in `numbers[thread]`.
fn number_ids(sides: &[Vec<Vec<Side>>], shards: &[Mutex<Shard>], next: &AtomicUsize) {
    loop {
        let shard_at = next.fetch_add(1, Ordering::Relaxed);
        if shard_at >= SHARDS {
            return;
        }
        let total: usize = sides.iter().map(|thread| thread[shard_at].len()).sum();
        let mut ids: KeyMap<Key> = KeyMap::with_capacity(total);
        let mut keys = Vec::with_capacity(total);
        let mut numbers = Vec::with_capacity(sides.len());
        for thread in sides {
            numbers.push(Vec::with_capacity(thread[shard_at].len()));
            for &(key, _) in &thread[shard_at] {
                let next = keys.len() as Key;
                let local = ids.insert(key, next);
                if local == next {
                    keys.push(key);
                }
                numbers.last_mut().unwrap().push(local as u32);
            }
        }
        *shards[shard_at].lock().unwrap() = Shard { ids: Some(ids), keys, numbers };
    }
}

/// Runs `work` on `threads` threads and waits for all of them.
fn parallel(threads: usize, work: impl Fn() + Sync) {
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(&work);
        }
    });
}

/// The root of `at` in a union-find forest shared by threads. A parent always
/// has a higher number than its child, so the forest stays acyclic whatever the
/// interleaving; pointing a node at its grandparent (path splitting) keeps that true.
#[inline(always)]
fn find(parent: &[AtomicU32], mut at: u32) -> u32 {
    loop {
        let up = parent[at as usize].load(Ordering::Relaxed);
        if up == at {
            return at;
        }
        let above = parent[up as usize].load(Ordering::Relaxed);
        if above != up {
            parent[at as usize].store(above, Ordering::Relaxed);
        }
        at = up;
    }
}

/// Joins the sets of `a` and `b`: the root with the lower number goes under the other.
#[inline(always)]
fn union(parent: &[AtomicU32], mut a: u32, mut b: u32) {
    loop {
        (a, b) = (find(parent, a), find(parent, b));
        if a == b {
            return;
        }
        let (low, high) = (a.min(b), a.max(b));
        // Only a root's parent is ever swapped; if `low` stopped being a root, look again.
        if parent[low as usize].compare_exchange(low, high, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            return;
        }
    }
}

/// Step 2, by every thread: union-find over the numbers, then name each person by
/// their smallest ID and write the name into every shard.
fn join_links(sides: &[Vec<Vec<Side>>], counts: &[usize], shards: Vec<Mutex<Shard>>, threads: usize) -> Links {
    let mut bases = Vec::with_capacity(SHARDS);
    let mut total = 0usize;
    for shard in &shards {
        bases.push(total as u32);
        total += shard.lock().unwrap().keys.len();
    }
    assert!(total < u32::MAX as usize, "too many linked IDs");
    let starts: Vec<usize> = counts.iter().scan(0, |sum, &count| Some(std::mem::replace(sum, *sum + count))).collect();
    let link_count: usize = counts.iter().sum();
    // Each link's two IDs, numbered across all shards.
    let links: Vec<AtomicU32> = (0..2 * link_count).map(|_| AtomicU32::new(0)).collect();
    let parent: Vec<AtomicU32> = (0..total as u32).map(AtomicU32::new).collect();
    let next = AtomicUsize::new(0);
    parallel(threads, || loop {
        let shard_at = next.fetch_add(1, Ordering::Relaxed);
        if shard_at >= SHARDS {
            return;
        }
        let shard = shards[shard_at].lock().unwrap();
        for ((thread, numbers), &start) in sides.iter().zip(&shard.numbers).zip(&starts) {
            for (&(_, side), &local) in thread[shard_at].iter().zip(numbers) {
                links[2 * (start + (side >> 1) as usize) + (side & 1) as usize].store(bases[shard_at] + local, Ordering::Relaxed);
            }
        }
    });
    const SPAN: usize = 1 << 14;
    let next = AtomicUsize::new(0);
    parallel(threads, || loop {
        let from = next.fetch_add(SPAN, Ordering::Relaxed);
        if from >= link_count {
            return;
        }
        for link in from..(from + SPAN).min(link_count) {
            union(&parent, links[2 * link].load(Ordering::Relaxed), links[2 * link + 1].load(Ordering::Relaxed));
        }
    });
    // Every ID points at its root; each root gets its smallest packed ID. Packed IDs
    // compare as integers; the rare interned ones are compared byte-wise after.
    let name: Vec<AtomicU64> = (0..total).map(|_| AtomicU64::new(u64::MAX)).collect();
    let interned: Mutex<Vec<(u32, Key)>> = Mutex::new(Vec::new());
    let next = AtomicUsize::new(0);
    parallel(threads, || loop {
        let shard_at = next.fetch_add(1, Ordering::Relaxed);
        if shard_at >= SHARDS {
            return;
        }
        let shard = shards[shard_at].lock().unwrap();
        for (local, &key) in shard.keys.iter().enumerate() {
            let at = bases[shard_at] + local as u32;
            let root = find(&parent, at);
            parent[at as usize].store(root, Ordering::Relaxed);
            if key::is_long(key) {
                interned.lock().unwrap().push((root, key));
            } else {
                name[root as usize].fetch_min(key, Ordering::Relaxed);
            }
        }
    });
    for (root, key) in interned.into_inner().unwrap() {
        let name = &name[root as usize];
        let current = name.load(Ordering::Relaxed);
        if current == u64::MAX || key::compare(key, current).is_lt() {
            name.store(key, Ordering::Relaxed);
        }
    }
    let next = AtomicUsize::new(0);
    parallel(threads, || loop {
        let shard_at = next.fetch_add(1, Ordering::Relaxed);
        if shard_at >= SHARDS {
            return;
        }
        let mut shard = shards[shard_at].lock().unwrap();
        let base = bases[shard_at] as usize;
        for slot in shard.ids.as_mut().unwrap().slots_mut() {
            if slot.0 != 0 {
                let root = parent[base + slot.1 as usize].load(Ordering::Relaxed);
                slot.1 = name[root as usize].load(Ordering::Relaxed);
            }
        }
    });
    Links { shards: shards.into_iter().map(|shard| shard.into_inner().unwrap().ids.unwrap()).collect() }
}

// ------------------------------------------------------------------ 2. parse

/// A valid event copy waiting for its person to be looked up.
#[derive(Clone, Copy, Default)]
struct Pending {
    user: Key,
    id: Key,
    ingest: u32,
    ts: u32,
    purchase: Purchase,
}

/// How many valid copies ahead the person's slot in the links is prefetched.
const EVENT_AHEAD: usize = 16;

thread_local! {
    /// The valid copies of the chunk being parsed: small enough to stay in the L2 cache.
    static PENDING: std::cell::RefCell<Vec<Pending>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Where one thread's parsed events go.
struct EventParts {
    visits: Parts<Visit>,
    purchases: Parts<Purchase>,
    sightings: Parts<Sighting>,
}

impl EventParts {
    fn new() -> Self {
        Self { visits: Parts::new(), purchases: Parts::new(), sightings: Parts::new() }
    }

    /// Sends a valid copy to its person's partition, and its sighting to the dedup.
    #[inline(always)]
    fn place(&mut self, links: &Links, pending: &Pending) {
        let person = links.person(pending.user);
        let at = partition(person);
        let number = self.visits.count(at);
        if number >= 1 << NUMBER_BITS {
            fail("too many events of one partition for one thread");
        }
        let purchase = pending.purchase.price != 0;
        self.visits.push(at, Visit { person: halves(person), ts: pending.ts | if purchase { PURCHASE } else { 0 } });
        if purchase {
            self.purchases.push(at, pending.purchase);
        }
        self.sight(pending.id, pending.ingest, (at as u32) << NUMBER_BITS | number);
    }

    #[inline(always)]
    fn sight(&mut self, id: Key, ingest: u32, place: u32) {
        self.sightings.push(partition(id), Sighting { id, ingest, place });
    }

    fn seal(&mut self) {
        self.visits.seal();
        self.purchases.seal();
        self.sightings.seal();
    }
}

/// Parses a chunk of events in two passes: the lines into a list of valid copies,
/// then the copies to their people. Looking people up in the second, short loop
/// lets many lookups wait on memory at once; inside the parse they stalled it.
fn parse_events(context: &Context, links: &Links, data: &[u8], start: usize, end: usize, out: &mut EventParts) {
    PENDING.with_borrow_mut(|pending| {
        pending.clear();
        parse_event_lines(context, data, start, end, out, pending);
        for (at, copy) in pending.iter().enumerate() {
            if let Some(ahead) = pending.get(at + EVENT_AHEAD) {
                links.prefetch(ahead.user);
            }
            out.place(links, copy);
        }
    });
}

fn parse_event_lines(context: &Context, data: &[u8], start: usize, end: usize, out: &mut EventParts, pending: &mut Vec<Pending>) {
    let mut dates = time::Dates::new();
    scan::lines::<7>(data, start, end, |fields| {
        // SAFETY: the scan only reports fields inside `data`.
        let field = |at: usize| unsafe { data.get_unchecked(fields[at].0..fields[at].1) };
        let id = key::key(data, fields[0].0, fields[0].1);
        if id == 0 {
            return;
        }
        let user = key::key(data, fields[1].0, fields[1].1);
        let ingest = time::parse(field(3), &mut dates);
        let ingest_seconds = ingest.map_or(INVALID, |ms| ms.div_euclid(1000).clamp(0, i64::from(INVALID - 1)) as u32);
        let mut copy = Pending { user, id, ingest: ingest_seconds, ts: INVALID, purchase: Purchase::default() };
        'valid: {
            if user == 0 || fields[5].0 == fields[5].1 {
                break 'valid;
            }
            let Some(ts) = time::parse(field(2), &mut dates) else { break 'valid };
            if ts < context.window_start || ts >= context.window_end {
                break 'valid;
            }
            let Some(ingest) = ingest else { break 'valid };
            if ingest - ts > DAY {
                break 'valid;
            }
            let kind = field(4);
            if kind.len() == 4 {
                let kind = u32::from_le_bytes(kind.try_into().unwrap());
                if kind != u32::from_le_bytes(*b"view") && kind != u32::from_le_bytes(*b"cart") {
                    break 'valid;
                }
            } else if kind == b"purchase" {
                let Some(price) = time::digits(field(6), 8) else { break 'valid };
                if !(1..=10_000_000).contains(&price) {
                    break 'valid;
                }
                let product = key::key(data, fields[5].0, fields[5].1);
                copy.purchase = Purchase { price: price as u32, category: context.products.get(product).unwrap_or(context.unknown_category) };
            } else {
                break 'valid;
            }
            copy.ts = (ts - context.base) as u32;
        }
        if copy.ts == INVALID {
            // An invalid copy still takes part in the dedup: if it is the kept copy, the event is dropped.
            out.sight(id, ingest_seconds, NOWHERE);
            return;
        }
        pending.push(copy);
    });
}

thread_local! {
    /// The valid clicks of the chunk being parsed, as `(user, click)`.
    static PENDING_CLICKS: std::cell::RefCell<Vec<(Key, Click)>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Parses a chunk of clicks, then sends them to their people, as for events.
fn parse_clicks(context: &Context, links: &Links, data: &[u8], start: usize, end: usize, out: &mut Parts<Click>) {
    PENDING_CLICKS.with_borrow_mut(|pending| {
        pending.clear();
        let mut dates = time::Dates::new();
        scan::lines::<4>(data, start, end, |fields| {
            // Copies of a click are identical and nothing counts clicks, so no dedup is needed:
            // a second copy can never change which campaign is credited or assists.
            let id = key::key(data, fields[0].0, fields[0].1);
            let user = key::key(data, fields[1].0, fields[1].1);
            if id == 0 || user == 0 {
                return;
            }
            let Some(campaign) = context.campaigns.get(key::key(data, fields[2].0, fields[2].1)) else { return };
            let Some(ts) = time::parse(&data[fields[3].0..fields[3].1], &mut dates) else { return };
            if ts < context.base || ts >= context.window_end {
                return;
            }
            pending.push((user, Click { person: 0, id, ts: (ts - context.base) as u32, campaign }));
        });
        for (at, &(user, mut click)) in pending.iter().enumerate() {
            if let Some(&(ahead, _)) = pending.get(at + EVENT_AHEAD) {
                links.prefetch(ahead);
            }
            let person = links.person(user);
            click.person = person;
            out.push(partition(person), click);
        }
    });
}

// ------------------------------------------------------------------ 3. dedup

/// How many events ahead the dedup prefetches its table slot.
const DEDUP_AHEAD: usize = 16;

#[inline(always)]
fn prefetch<T>(pointer: *const T) {
    // SAFETY: prefetching never faults, whatever the address.
    unsafe { std::arch::x86_64::_mm_prefetch(pointer.cast(), std::arch::x86_64::_MM_HINT_T0) };
}

/// The sightings of one bucket of event_ids, `blocks[thread]`: every copy but the
/// earliest-ingested of each event_id is killed, by adding `thread << 32 | number`
/// to `kills[partition]`. Copies with equal ingest_ts are identical, so the first
/// seen is kept. An invalid kept copy (NOWHERE) kills the others and is dropped itself.
fn dedup(blocks: &[Vec<Block<Sighting>>], table: &mut Vec<u64>, kills: &mut [Vec<u64>]) {
    let total: usize = blocks.iter().flatten().map(|block| block.len).sum();
    if total == 0 {
        return;
    }
    let size = (total * 2).next_power_of_two();
    table.clear();
    table.resize(size, 0);
    let mask = size - 1;
    let flat: Vec<(usize, &Block<Sighting>)> = blocks.iter().enumerate().flat_map(|(thread, blocks)| blocks.iter().map(move |block| (thread, block))).collect();
    let capacity = Block::<Sighting>::CAPACITY;
    let mut kill = |thread: usize, place: u32| {
        if place != NOWHERE {
            kills[(place >> NUMBER_BITS) as usize].push((thread as u64) << 32 | u64::from(place & ((1 << NUMBER_BITS) - 1)));
        }
    };
    for (block_at, &(thread, block)) in flat.iter().enumerate() {
        let rows = block.rows();
        for (at, sighting) in rows.iter().enumerate() {
            if let Some(ahead) = rows.get(at + DEDUP_AHEAD) {
                // The table is larger than the L2 cache: ask for the slot of a later sighting now.
                prefetch(table.as_ptr().wrapping_add(key::hash(ahead.id) as usize & mask));
            }
            let hash = key::hash(sighting.id);
            let tag = hash & 0xFFFF_FFFF_0000_0000;
            let entry = tag | ((block_at * capacity + at) as u64 + 1);
            let mut slot = hash as usize & mask;
            loop {
                let seen = table[slot];
                if seen == 0 {
                    table[slot] = entry;
                    break;
                }
                if seen & 0xFFFF_FFFF_0000_0000 == tag {
                    let index = (seen as u32 - 1) as usize;
                    let (other_thread, other_block) = flat[index / capacity];
                    let other = &other_block.rows()[index % capacity];
                    if other.id == sighting.id {
                        // Two copies: the earlier ingest is kept, valid or not.
                        if sighting.ingest < other.ingest {
                            kill(other_thread, other.place);
                            table[slot] = entry;
                        } else {
                            kill(thread, sighting.place);
                        }
                        break;
                    }
                }
                slot = (slot + 1) & mask;
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
    thread_offsets: Vec<usize>,
    killed: Vec<u64>,
    visits: Vec<u64>,
    purchases: Vec<(u32, u32)>,
    click_person: Vec<u32>,
    click_start: Vec<u32>,
    clicks: Vec<PersonClick>,
    click_keys: Vec<u64>,
    /// The campaigns with clicks in the 7 days before the current session, how many each, and where each is in `window`.
    window: Vec<u32>,
    window_count: Vec<u32>,
    window_at: Vec<u32>,
    /// For each campaign in the window, the number of the first session it assists.
    window_since: Vec<u32>,
    /// The current person's sessions so far, and `(day, first session)` of each day they fall on.
    session: u32,
    day_runs: Vec<(u32, u32)>,
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
            thread_offsets: Vec::new(),
            killed: Vec::new(),
            visits: Vec::new(),
            purchases: Vec::new(),
            click_person: Vec::new(),
            click_start: Vec::new(),
            clicks: Vec::new(),
            click_keys: Vec::new(),
            window: Vec::new(),
            window_count: vec![0; context.campaign_rows.len()],
            window_at: vec![0; context.campaign_rows.len()],
            window_since: vec![0; context.campaign_rows.len()],
            session: 0,
            day_runs: Vec::new(),
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

    /// One partition: its Visits by thread, its purchases by thread (one per
    /// Visit with PURCHASE, in the same order), the Visits the dedup killed
    /// (`thread << 32 | number`), and its clicks.
    fn partition(&mut self, visit_blocks: &[Vec<Block<Visit>>], purchase_blocks: &[Vec<Block<Purchase>>], kills: &[u64], click_blocks: &[Block<Click>]) {
        let total: usize = visit_blocks.iter().flatten().map(|block| block.len).sum();
        if total == 0 {
            return;
        }
        // The killed Visits, as a bit per Visit in thread order.
        let mut offset = 0usize;
        self.thread_offsets.clear();
        for blocks in visit_blocks {
            self.thread_offsets.push(offset);
            offset += blocks.iter().map(|block| block.len).sum::<usize>();
        }
        self.killed.clear();
        self.killed.resize(total.div_ceil(64), 0);
        for &kill in kills {
            let at = self.thread_offsets[(kill >> 32) as usize] + (kill as u32) as usize;
            self.killed[at / 64] |= 1 << (at % 64);
        }
        self.purchases.clear();
        for block in purchase_blocks.iter().flatten() {
            self.purchases.extend(block.rows().iter().map(|purchase| (purchase.price, purchase.category)));
        }
        // Number the people of this partition and count their events.
        let size = (total * 2).next_power_of_two();
        let mask = size - 1;
        self.table.clear();
        self.table.resize(size, 0);
        self.people.clear();
        self.visit_person.clear();
        self.visit_start.clear();
        for (at, visit) in visit_blocks.iter().flatten().flat_map(Block::rows).enumerate() {
            if self.killed[at / 64] >> (at % 64) & 1 != 0 {
                self.visit_person.push(u32::MAX);
                continue;
            }
            let person = whole(visit.person);
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
        self.visits.resize(sum as usize, 0);
        let mut purchase_at = 0u64;
        for (visit, &person) in visit_blocks.iter().flatten().flat_map(Block::rows).zip(&self.visit_person) {
            let mut value = u64::from(visit.ts & !PURCHASE) << 32;
            if visit.ts & PURCHASE != 0 {
                // Killed or not, a purchase Visit has its purchase row.
                purchase_at += 1;
                value |= purchase_at;
            }
            if person != u32::MAX {
                let at = &mut self.visit_start[person as usize];
                self.visits[*at as usize] = value;
                *at += 1;
            }
        }
        // The same for the clicks of people who have events.
        self.click_person.clear();
        self.click_start.clear();
        self.click_start.resize(people + 1, 0);
        for click in click_blocks.iter().flat_map(Block::rows) {
            let person = click.person;
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
        for (click, &person) in click_blocks.iter().flat_map(Block::rows).zip(&self.click_person) {
            if person != u32::MAX {
                let at = &mut self.click_start[person as usize];
                self.clicks[*at as usize] = PersonClick { id: click.id, ts: click.ts, campaign: click.campaign };
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
        // The clicks by time, as `ts << 32 | index`; ties in time are settled by click_id below.
        self.click_keys.clear();
        self.click_keys.extend(clicks.iter().enumerate().map(|(at, click)| u64::from(click.ts) << 32 | at as u64));
        self.click_keys.sort_unstable();
        let keys = std::mem::take(&mut self.click_keys);
        let click = |key: u64| &clicks[key as u32 as usize];

        // `keys[low..high]` are the clicks in the 7 days up to the current session's
        // start: they assist it, and the latest of them is credited with it.
        let (mut low, mut high, mut best) = (0usize, 0usize, usize::MAX);
        let (mut row_day, mut row_sessions, mut row_events, mut row_revenue) = (u32::MAX, 0u32, 0u32, 0u64);
        let mut at = 0;
        while at < visits.len() {
            // A session: from `at` until a gap of over 30 minutes.
            let start = (visits[at] >> 32) as u32;
            let day = ((u64::from(start) + context.day_shift) / DAY as u64) as u32 - 7;
            if self.day_runs.last().is_none_or(|&(run_day, _)| run_day != day) {
                self.day_runs.push((day, self.session));
            }
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
            // Clicks at or before the start join the window. The credit goes to the
            // latest; among clicks at the same instant, to the smallest click_id.
            while high < keys.len() && (keys[high] >> 32) as u32 <= start {
                let index = keys[high] as u32 as usize;
                if best == usize::MAX
                    || clicks[index].ts != clicks[best].ts
                    || key::compare(clicks[index].id, clicks[best].id).is_lt()
                {
                    best = index;
                }
                self.window_add(click(keys[high]).campaign);
                high += 1;
            }
            // Clicks more than 7 days before the start leave it.
            while low < high && ((keys[low] >> 32) as u32) < start.saturating_sub(WEEK) {
                self.window_remove(click(keys[low]).campaign);
                low += 1;
            }
            let campaign = if best != usize::MAX && start - clicks[best].ts <= WEEK { clicks[best].campaign as usize } else { organic };
            let total = &mut self.campaign_days[campaign * days + day as usize];
            total.sessions += 1;
            total.purchasing += u64::from(purchased);
            total.revenue += revenue;
            // Every campaign with a click in the window assists the session once:
            // counted when the campaign leaves the window, for all its sessions at once.
            self.session += 1;
            row_sessions += 1;
            row_events += (at - first) as u32;
            row_revenue += revenue;
        }
        if row_day != u32::MAX {
            self.row(person, row_day, row_sessions, row_events, row_revenue);
        }
        for at in 0..self.window.len() {
            let campaign = self.window[at];
            self.window_count[campaign as usize] = 0;
            self.assist(campaign, self.window_since[campaign as usize], self.session);
        }
        self.window.clear();
        self.day_runs.clear();
        self.session = 0;
        self.click_keys = keys;
    }

    /// `campaign` assisted this person's sessions `from..to` (numbered in time
    /// order): add them up per day. Days only grow with the session number.
    #[inline(always)]
    fn assist(&mut self, campaign: u32, from: u32, to: u32) {
        if from >= to {
            return;
        }
        let days = self.context.days;
        let mut run = self.day_runs.partition_point(|&(_, first)| first <= from) - 1;
        let mut at = from;
        while at < to {
            let (day, _) = self.day_runs[run];
            let end = self.day_runs.get(run + 1).map_or(to, |&(_, first)| first.min(to));
            self.campaign_days[campaign as usize * days + day as usize].assisted += u64::from(end - at);
            at = end;
            run += 1;
        }
    }

    /// One more of the window's clicks is of `campaign`.
    #[inline(always)]
    fn window_add(&mut self, campaign: u32) {
        let count = &mut self.window_count[campaign as usize];
        if *count == 0 {
            self.window_at[campaign as usize] = self.window.len() as u32;
            self.window_since[campaign as usize] = self.session;
            self.window.push(campaign);
        }
        *count += 1;
    }

    /// One fewer of the window's clicks is of `campaign`.
    #[inline(always)]
    fn window_remove(&mut self, campaign: u32) {
        let count = &mut self.window_count[campaign as usize];
        *count -= 1;
        if *count == 0 {
            self.assist(campaign, self.window_since[campaign as usize], self.session);
            let at = self.window_at[campaign as usize] as usize;
            let last = self.window.pop().unwrap();
            if last != campaign {
                self.window[at] = last;
                self.window_at[last as usize] = at as u32;
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
    // Address space for the large allocations: generous, since only touched pages count.
    let memory = env("MEM_GB", "0").parse::<usize>().ok().filter(|&gb| gb > 0).unwrap_or(16);
    bump::reserve(memory << 30);
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

    // 1. Links: parse and number the IDs with every thread, then one thread joins
    // them while the others start on the events.
    let next = AtomicUsize::new(0);
    let sides: Vec<(Vec<Vec<Side>>, usize)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (link_tasks, next) = (&link_tasks, &next);
                scope.spawn(move || {
                    let mut sides: Vec<Vec<Side>> = (0..SHARDS).map(|_| Vec::new()).collect();
                    let mut count = 0u32;
                    while let Some(task) = link_tasks.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let (data, start, end) = chunk(files, task);
                        scan::lines::<2>(data, start, end, |fields| {
                            let a = key::key(data, fields[0].0, fields[0].1);
                            let b = key::key(data, fields[1].0, fields[1].1);
                            // A self-link changes nothing: an ID alone is its own person anyway.
                            if a != 0 && b != 0 && a != b {
                                for (key, side) in [(a, 0), (b, 1)] {
                                    sides[(key::hash(key) >> (64 - SHARD_BITS)) as usize].push((key, count << 1 | side));
                                }
                                count += 1;
                            }
                        });
                    }
                    (sides, count as usize)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    log("links parsed");
    let (sides, counts): (Vec<_>, Vec<usize>) = sides.into_iter().unzip();
    let shards: Vec<Mutex<Shard>> = (0..SHARDS).map(|_| Mutex::new(Shard::default())).collect();
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| number_ids(&sides, &shards, &next));
        }
    });
    log("links parsed and numbered");
    let links = join_links(&sides, &counts, shards, threads);
    drop(sides);
    let links = &links;
    log("links joined");

    // 2. Events, then clicks.
    let (next_event, next_click) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let parsed: Vec<(EventParts, Parts<Click>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (event_tasks, click_tasks, next_event, next_click) = (&event_tasks, &click_tasks, &next_event, &next_click);
                scope.spawn(move || {
                    let (mut events, mut clicks) = (EventParts::new(), Parts::new());
                    while let Some(task) = event_tasks.get(next_event.fetch_add(1, Ordering::Relaxed)) {
                        let (data, start, end) = chunk(files, task);
                        parse_events(context, links, data, start, end, &mut events);
                    }
                    while let Some(task) = click_tasks.get(next_click.fetch_add(1, Ordering::Relaxed)) {
                        let (data, start, end) = chunk(files, task);
                        parse_clicks(context, links, data, start, end, &mut clicks);
                    }
                    events.seal();
                    clicks.seal();
                    (events, clicks)
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let (mut visits, mut purchases, mut sightings, mut clicks) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for (events, thread_clicks) in parsed {
        visits.push(events.visits);
        purchases.push(events.purchases);
        sightings.push(events.sightings);
        clicks.push(thread_clicks);
    }
    let (visits, purchases, sightings, clicks) = (gather_threads(visits), gather_threads(purchases), gather_threads(sightings), gather(clicks));
    log("parsed events and clicks");

    // 3. Dedup: each bucket of event_ids marks the copies to drop, by partition.
    let next = AtomicUsize::new(0);
    let thread_kills: Vec<Vec<Vec<u64>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let (sightings, next) = (&sightings, &next);
                scope.spawn(move || {
                    let mut kills: Vec<Vec<u64>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
                    let mut table = Vec::new();
                    while let Some(blocks) = sightings.get(next.fetch_add(1, Ordering::Relaxed)) {
                        let blocks = std::mem::take(&mut *blocks.lock().unwrap());
                        dedup(&blocks, &mut table, &mut kills);
                        blocks.into_iter().flatten().for_each(Block::recycle);
                    }
                    kills
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().unwrap()).collect()
    });
    let mut kills: Vec<Vec<u64>> = (0..PARTITIONS).map(|_| Vec::new()).collect();
    for thread in thread_kills {
        for (at, list) in thread.into_iter().enumerate() {
            kills[at].extend(list);
        }
    }
    // Largest partitions first (a whale's can take ten times the average), so the threads finish together.
    let mut order: Vec<usize> = (0..PARTITIONS).collect();
    order.sort_by_cached_key(|&at| std::cmp::Reverse(visits[at].lock().unwrap().iter().flatten().map(|block| block.len).sum::<usize>()));
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
                let (visits, purchases, kills, clicks, next, user_dir, order) = (&visits, &purchases, &kills, &clicks, &next, &user_dir, &order);
                scope.spawn(move || {
                    let mut worker = Worker::new(context, user_dir.join(format!("part-{thread}.parquet")));
                    loop {
                        let Some(&at) = order.get(next.fetch_add(1, Ordering::Relaxed)) else { break };
                        let visit_blocks = std::mem::take(&mut *visits[at].lock().unwrap());
                        let purchase_blocks = std::mem::take(&mut *purchases[at].lock().unwrap());
                        let click_blocks = std::mem::take(&mut *clicks[at].lock().unwrap());
                        worker.partition(&visit_blocks, &purchase_blocks, &kills[at], &click_blocks);
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
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        if let Some(peak) = status.lines().find(|line| line.starts_with("VmHWM")) {
            eprintln!("peak memory: {}", peak.trim_start_matches("VmHWM:").trim());
        }
    }
    // Leave without unmapping or freeing anything: the kernel does it faster.
    std::process::exit(0);
}
