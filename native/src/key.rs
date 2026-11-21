//! IDs as 128-bit keys. An ID of up to 15 bytes is packed exactly: its bytes
//! big-endian in the top 15 bytes and its length in the low byte, so comparing
//! two short keys as integers is comparing the IDs byte-wise. Longer IDs (the
//! generator writes none) are interned in a global table and carry 0xFF in the
//! low byte. Key 0 is the empty field, i.e. null.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub type Key = u128;

#[derive(Default)]
struct Long {
    ids: HashMap<Vec<u8>, u32>,
    list: Vec<Vec<u8>>,
}

static LONG: Mutex<Option<Long>> = Mutex::new(None);

#[cold]
fn long_key(bytes: &[u8]) -> Key {
    let mut guard = LONG.lock().unwrap();
    let long = guard.get_or_insert_with(Long::default);
    let index = match long.ids.get(bytes) {
        Some(&index) => index,
        None => {
            let index = long.list.len() as u32;
            long.list.push(bytes.to_vec());
            long.ids.insert(bytes.to_vec(), index);
            index
        }
    };
    (u128::from(index) << 8) | 0xFF
}

/// The key of the field `data[start..end]`.
#[inline(always)]
pub fn key(data: &[u8], start: usize, end: usize) -> Key {
    let len = end - start;
    if len == 0 {
        return 0;
    }
    if len > 15 {
        return long_key(&data[start..end]);
    }
    let raw = if start + 16 <= data.len() {
        // SAFETY: the 16 bytes are inside `data`.
        u128::from_be_bytes(unsafe { data.as_ptr().add(start).cast::<[u8; 16]>().read_unaligned() })
    } else {
        let mut buffer = [0u8; 16];
        buffer[..len].copy_from_slice(&data[start..end]);
        u128::from_be_bytes(buffer)
    };
    (raw & (!0u128 << (8 * (16 - len)))) | len as u128
}

pub fn key_of(bytes: &[u8]) -> Key {
    key(bytes, 0, bytes.len())
}

#[inline(always)]
pub fn is_long(key: Key) -> bool {
    key as u8 == 0xFF
}

/// The ID's bytes, appended to `out`.
#[inline(always)]
pub fn write_bytes(key: Key, out: &mut Vec<u8>) {
    if is_long(key) {
        let guard = LONG.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().list[(key >> 8) as usize]);
    } else {
        let len = (key as u8) as usize;
        out.extend_from_slice(&key.to_be_bytes()[..len]);
    }
}

/// Byte-wise order of the two IDs.
#[inline(always)]
pub fn compare(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        return a.cmp(&b);
    }
    let (mut x, mut y) = (Vec::new(), Vec::new());
    write_bytes(a, &mut x);
    write_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
pub fn hash(key: Key) -> u64 {
    let low = key as u64 ^ 0x9E37_79B9_7F4A_7C15;
    let high = (key >> 64) as u64 ^ 0xD6E8_FEB8_6659_FD93;
    let product = u128::from(low) * u128::from(high);
    let folded = (product as u64) ^ ((product >> 64) as u64);
    folded ^ (folded >> 29)
}

/// An open-addressing map from keys to a small value, built by one thread, then read by all.
pub struct KeyMap<V: Copy> {
    slots: Vec<(Key, V)>,
    mask: usize,
    len: usize,
}

impl<V: Copy + Default> KeyMap<V> {
    pub fn with_capacity(entries: usize) -> Self {
        let size = (entries * 2).max(16).next_power_of_two();
        Self { slots: vec![(0, V::default()); size], mask: size - 1, len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    #[cold]
    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, vec![(0, V::default()); (self.mask + 1) * 2]);
        self.mask = self.slots.len() - 1;
        for (key, value) in old {
            if key != 0 {
                let mut at = hash(key) as usize & self.mask;
                while self.slots[at].0 != 0 {
                    at = (at + 1) & self.mask;
                }
                self.slots[at] = (key, value);
            }
        }
    }

    /// Inserts unless the key (never 0) is there already; returns the value now stored.
    #[inline(always)]
    pub fn insert(&mut self, key: Key, value: V) -> V {
        if self.len * 10 >= self.slots.len() * 6 {
            self.grow();
        }
        let mut at = hash(key) as usize & self.mask;
        loop {
            // SAFETY: `at` is masked to the table size.
            let slot = unsafe { self.slots.get_unchecked_mut(at) };
            if slot.0 == key {
                return slot.1;
            }
            if slot.0 == 0 {
                *slot = (key, value);
                self.len += 1;
                return value;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn get(&self, key: Key) -> Option<V> {
        if key == 0 {
            return None;
        }
        let mut at = hash(key) as usize & self.mask;
        loop {
            // SAFETY: `at` is masked to the table size.
            let slot = unsafe { self.slots.get_unchecked(at) };
            if slot.0 == key {
                return Some(slot.1);
            }
            if slot.0 == 0 {
                return None;
            }
            at = (at + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn prefetch(&self, key: Key) {
        let at = hash(key) as usize & self.mask;
        #[cfg(target_arch = "x86_64")]
        // SAFETY: prefetching never faults.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().add(at).cast(), _MM_HINT_T0);
        }
        let _ = at;
    }

    pub fn slots_mut(&mut self) -> &mut [(Key, V)] {
        &mut self.slots
    }
}
