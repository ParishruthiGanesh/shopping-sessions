//! IDs as exact 64-bit codes. An ID of 1 to 10 characters from `0-9`, `A-Z`,
//! `_` and `a-z` (every ID the generator writes: `u` or `e` and up to eight
//! base-36 digits, `p` and digits, `c_` and digits, and upper-cased variants)
//! is packed 6 bits per character, first character highest, each character
//! numbered 1 to 63 in ASCII order and 0 after the end. So the code is exact,
//! and comparing two codes as integers compares the IDs byte by byte.
//!
//! Any other ID is interned in a global table and its code has the top bit set
//! (`LONG`); comparing such a code goes through the bytes. Code 0 is the empty
//! field, i.e. null.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;

pub type Key = u64;

const LONG: Key = 1 << 63;
/// Longest ID that is packed rather than interned.
const PACKED: usize = 10;

#[derive(Default)]
struct Long {
    ids: HashMap<Vec<u8>, u64>,
    list: Vec<Vec<u8>>,
}

static LONGS: Mutex<Option<Long>> = Mutex::new(None);

#[cold]
#[inline(never)]
fn long_key(bytes: &[u8]) -> Key {
    let mut guard = LONGS.lock().unwrap();
    let long = guard.get_or_insert_with(Long::default);
    if let Some(&index) = long.ids.get(bytes) {
        return LONG | index;
    }
    let index = long.list.len() as u64;
    long.list.push(bytes.to_vec());
    long.ids.insert(bytes.to_vec(), index);
    LONG | index
}

/// Each byte's 6-bit number, or 0 for a byte that cannot be packed.
static DIGIT: [u8; 256] = {
    let mut table = [0u8; 256];
    let mut at = 0;
    while at < 256 {
        let byte = at as u8;
        table[at] = match byte {
            b'0'..=b'9' => byte - b'0' + 1,
            b'A'..=b'Z' => byte - b'A' + 11,
            b'_' => 37,
            b'a'..=b'z' => byte - b'a' + 38,
            _ => 0,
        };
        at += 1;
    }
    table
};

/// The byte of each 6-bit number: the inverse of DIGIT.
static BYTE: [u8; 64] = {
    let mut table = [0u8; 64];
    let mut at = 0;
    while at < 256 {
        if DIGIT[at] != 0 {
            table[DIGIT[at] as usize] = at as u8;
        }
        at += 1;
    }
    table
};

/// The packed code of 1 to 10 bytes at `at`, with 16 readable bytes there; None
/// if a byte cannot be packed.
#[inline(always)]
unsafe fn pack16(at: *const u8, len: usize) -> Option<Key> {
    use std::arch::x86_64::*;
    let bytes = _mm_loadu_si128(at.cast());
    let set = |byte: u8| _mm_set1_epi8(byte as i8);
    let inside = _mm_cmpgt_epi8(_mm_set1_epi8(len as i8), _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
    // Signed compares: bytes of 128 and up are negative and fall in no range.
    let range = |low: u8, high: u8| _mm_and_si128(_mm_cmpgt_epi8(bytes, set(low - 1)), _mm_cmpgt_epi8(set(high + 1), bytes));
    let (digit, upper, under, lower) = (range(b'0', b'9'), range(b'A', b'Z'), _mm_cmpeq_epi8(bytes, set(b'_')), range(b'a', b'z'));
    let known = _mm_or_si128(_mm_or_si128(digit, upper), _mm_or_si128(under, lower));
    if _mm_movemask_epi8(_mm_andnot_si128(known, inside)) != 0 {
        return None;
    }
    let offset = _mm_or_si128(
        _mm_or_si128(_mm_and_si128(digit, set(b'0' - 1)), _mm_and_si128(upper, set(b'A' - 11))),
        _mm_or_si128(_mm_and_si128(under, set(b'_' - 37)), _mm_and_si128(lower, set(b'a' - 38))),
    );
    let digits = _mm_and_si128(_mm_sub_epi8(bytes, offset), inside);
    // Pairs to 12 bits, then fours to 24 bits.
    let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x0140));
    let fours = _mm_madd_epi16(pairs, _mm_set1_epi32(0x0001_1000));
    let (a, b, c) = (_mm_extract_epi32::<0>(fours) as u32, _mm_extract_epi32::<1>(fours) as u32, _mm_extract_epi32::<2>(fours) as u32);
    Some(u64::from(a) << 36 | u64::from(b) << 12 | u64::from(c) >> 12)
}

/// The key of the field `data[start..end]`.
#[inline(always)]
pub fn key(data: &[u8], start: usize, end: usize) -> Key {
    let len = end - start;
    if len.wrapping_sub(1) < PACKED && start + 16 <= data.len() {
        // SAFETY: the 16 bytes are inside `data`.
        if let Some(key) = unsafe { pack16(data.as_ptr().add(start), len) } {
            return key;
        }
    }
    key_slow(&data[start..end])
}

#[inline(never)]
pub fn key_slow(bytes: &[u8]) -> Key {
    if bytes.is_empty() {
        return 0;
    }
    if bytes.len() <= PACKED && bytes.iter().all(|&byte| DIGIT[byte as usize] != 0) {
        let mut key = 0u64;
        for at in 0..PACKED {
            key = key << 6 | u64::from(bytes.get(at).map_or(0, |&byte| DIGIT[byte as usize]));
        }
        return key;
    }
    long_key(bytes)
}

#[inline(always)]
pub fn is_long(key: Key) -> bool {
    key & LONG != 0
}

/// The ID's bytes, appended to `out`.
#[inline(always)]
pub fn write_bytes(key: Key, out: &mut Vec<u8>) {
    if is_long(key) {
        let guard = LONGS.lock().unwrap();
        out.extend_from_slice(&guard.as_ref().unwrap().list[(key & !LONG) as usize]);
    } else {
        for at in (0..PACKED).rev() {
            let digit = (key >> (6 * at)) & 63;
            if digit == 0 {
                break;
            }
            out.push(BYTE[digit as usize]);
        }
    }
}

/// Byte-wise order of the two IDs.
#[inline(always)]
pub fn compare(a: Key, b: Key) -> Ordering {
    if !is_long(a) && !is_long(b) {
        return a.cmp(&b);
    }
    compare_slow(a, b)
}

#[cold]
#[inline(never)]
fn compare_slow(a: Key, b: Key) -> Ordering {
    let (mut x, mut y) = (Vec::new(), Vec::new());
    write_bytes(a, &mut x);
    write_bytes(b, &mut y);
    x.cmp(&y)
}

#[inline(always)]
pub fn hash(key: Key) -> u64 {
    let product = u128::from(key ^ 0x9E37_79B9_7F4A_7C15) * 0xD6E8_FEB8_6659_FD93;
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
        Self { slots: Self::empty(size), mask: size - 1, len: 0 }
    }

    /// `size` empty slots, written rather than left to the kernel's lazy zero
    /// pages: reading such a page and then writing it costs two page faults and,
    /// with other threads running, a TLB shootdown.
    fn empty(size: usize) -> Vec<(Key, V)> {
        let mut slots = Vec::with_capacity(size);
        slots.resize(size, (0, V::default()));
        slots
    }

    #[cold]
    fn grow(&mut self) {
        let old = std::mem::replace(&mut self.slots, Self::empty((self.mask + 1) * 2));
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
        self.get_hashed(key, hash(key))
    }

    /// `get` for a caller that already has `hash(key)`.
    #[inline(always)]
    pub fn get_hashed(&self, key: Key, hash: u64) -> Option<V> {
        if key == 0 {
            return None;
        }
        let mut at = hash as usize & self.mask;
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
    pub fn prefetch_hashed(&self, hash: u64) {
        let at = hash as usize & self.mask;
        // SAFETY: prefetching never faults.
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            _mm_prefetch(self.slots.as_ptr().wrapping_add(at).cast(), _MM_HINT_T0);
        }
    }

    pub fn slots_mut(&mut self) -> &mut [(Key, V)] {
        &mut self.slots
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(key: Key) -> Vec<u8> {
        let mut out = Vec::new();
        write_bytes(key, &mut out);
        out
    }

    #[test]
    fn exact_and_ordered() {
        let ids: Vec<&[u8]> = vec![
            b"u1", b"U1", b"u10", b"u1a", b"u_", b"c_123", b"e0", b"e00", b"zzzzzzzzzz", b"0", b"_", b"A", b"z", b"u839lbrub",
            b"zzzzzzzzzzz", b" u1", b"u1 ", b"u-1", b"\xc3\xa9", b"\"", b"u1\r", b"9999999999", b"aaaaaaaaaa", b"Zz_09azZ_",
        ];
        let mut padded: Vec<u8> = Vec::new();
        let mut spans = Vec::new();
        for id in &ids {
            spans.push((padded.len(), padded.len() + id.len()));
            padded.extend_from_slice(id);
            padded.push(b',');
        }
        padded.extend_from_slice(&[b'x'; 16]);
        let keys: Vec<Key> = spans.iter().map(|&(start, end)| key(&padded, start, end)).collect();
        for (at, id) in ids.iter().enumerate() {
            assert_eq!(&bytes(keys[at])[..], *id, "round trip of {:?}", String::from_utf8_lossy(id));
            assert_eq!(keys[at], key_slow(id), "fast and slow paths of {:?}", String::from_utf8_lossy(id));
            for (other, other_id) in ids.iter().enumerate() {
                assert_eq!(compare(keys[at], keys[other]), id.cmp(other_id));
                assert_eq!(keys[at] == keys[other], id == other_id);
            }
        }
        assert_eq!(key(b"", 0, 0), 0);
    }
}
