//! The CSV dialect of rules.md: only `\n` ends a line, every comma separates
//! fields, and a field is either free of quotes or wrapped in one pair of them.
//! Lines with the wrong number of fields or a stray quote are skipped.

#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn structurals(block: *const u8) -> u64 {
    use std::arch::x86_64::*;
    let (comma, newline, quote) = (_mm256_set1_epi8(b',' as i8), _mm256_set1_epi8(b'\n' as i8), _mm256_set1_epi8(b'"' as i8));
    let half = |at: *const u8| {
        let bytes = _mm256_loadu_si256(at.cast());
        let hit = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(bytes, comma), _mm256_cmpeq_epi8(bytes, newline)),
            _mm256_cmpeq_epi8(bytes, quote),
        );
        u64::from(_mm256_movemask_epi8(hit) as u32)
    };
    half(block) | (half(block.add(32)) << 32)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn structurals(block: *const u8) -> u64 {
    let mut mask = 0u64;
    for at in 0..64 {
        let byte = *block.add(at);
        mask |= u64::from(byte == b',' || byte == b'\n' || byte == b'"') << at;
    }
    mask
}

/// Calls `line` with the unquoted `(start, end)` of each field for every
/// well-formed line of `N` fields in `data[start..end]`. `start` is the start of
/// a line and `end` is just after a newline or the end of the file.
#[inline(always)]
pub fn scan<const N: usize>(data: &[u8], start: usize, end: usize, mut line: impl FnMut(&[(usize, usize); N])) {
    let mut fields = [(0usize, 0usize); N];
    let mut quotes = [0u8; N];
    let mut count = 0usize; // fields closed on this line
    let mut field_start = start;
    let mut field_quotes = 0u8;

    macro_rules! close {
        ($at:expr) => {
            if count < N {
                fields[count] = (field_start, $at);
                quotes[count] = field_quotes;
            }
            count += 1;
            field_start = $at + 1;
            field_quotes = 0;
        };
    }
    macro_rules! finish {
        () => {
            let complete = count == N;
            count = 0;
            if complete {
                let mut out = fields;
                let mut good = true;
                for at in 0..N {
                    if quotes[at] != 0 {
                        let (from, to) = fields[at];
                        if quotes[at] != 2 || to - from < 2 || data[from] != b'"' || data[to - 1] != b'"' {
                            good = false;
                        }
                        out[at] = (from + 1, to.wrapping_sub(1));
                    }
                }
                if good {
                    line(&out);
                }
            }
        };
    }

    let mut block = start;
    while block < end {
        let mut mask = if block + 64 <= end {
            // SAFETY: the 64 bytes are inside `data`.
            unsafe { structurals(data.as_ptr().add(block)) }
        } else {
            let mut tail = [0u8; 64];
            tail[..end - block].copy_from_slice(&data[block..end]);
            // SAFETY: `tail` is 64 bytes.
            unsafe { structurals(tail.as_ptr()) }
        };
        while mask != 0 {
            let at = block + mask.trailing_zeros() as usize;
            mask &= mask - 1;
            // SAFETY: `at` is below `end`.
            match unsafe { *data.get_unchecked(at) } {
                b'"' => field_quotes = field_quotes.saturating_add(1),
                b',' => {
                    close!(at);
                }
                _ => {
                    close!(at);
                    finish!();
                }
            }
        }
        block += 64;
    }
    if field_start < end || count > 0 {
        // The file's last line has no newline.
        close!(end);
        finish!();
    }
}

/// Splits `data` after its header line into chunks of about `size` bytes that end on line ends.
pub fn chunks(data: &[u8], size: usize) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let Some(header) = data.iter().position(|&byte| byte == b'\n') else {
        return out;
    };
    let mut start = header + 1;
    while start < data.len() {
        let mut end = start.saturating_add(size).min(data.len());
        if end < data.len() {
            end = match data[end..].iter().position(|&byte| byte == b'\n') {
                Some(at) => end + at + 1,
                None => data.len(),
            };
        }
        out.push((start, end));
        start = end;
    }
    out
}
