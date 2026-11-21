//! The CSV dialect of rules.md: only `\n` ends a line, every comma separates
//! fields, and a field is either free of quotes or wrapped in one pair of them.
//! Lines with the wrong number of fields or a stray quote are skipped.

/// Bytes per step of the scan. One 32-byte compare at a time: joining two
/// movemasks into a 64-bit mask makes LLVM rebuild it bit by bit.
const STEP: usize = 32;

/// Bit masks over the 32 bytes at `block`: separators (commas and newlines), newlines, quotes.
#[cfg(target_feature = "avx2")]
#[inline(always)]
unsafe fn masks(block: *const u8) -> (u32, u32, u32) {
    use std::arch::x86_64::*;
    let bytes = _mm256_loadu_si256(block.cast());
    let newline = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'\n' as i8));
    let separator = _mm256_or_si256(newline, _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b',' as i8)));
    let quote = _mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(b'"' as i8));
    (_mm256_movemask_epi8(separator) as u32, _mm256_movemask_epi8(newline) as u32, _mm256_movemask_epi8(quote) as u32)
}

#[cfg(not(target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks(block: *const u8) -> (u32, u32, u32) {
    let (mut separator, mut newline, mut quote) = (0u32, 0u32, 0u32);
    for at in 0..STEP {
        let byte = *block.add(at);
        newline |= u32::from(byte == b'\n') << at;
        separator |= u32::from(byte == b'\n' || byte == b',') << at;
        quote |= u32::from(byte == b'"') << at;
    }
    (separator, newline, quote)
}

/// Calls `line` with the unquoted `(start, end)` of each field for every
/// well-formed line of `N` fields (at most 16) in `data[start..end]`. `start` is
/// the start of a line and `end` is just after a newline or the end of the file.
///
/// Only separators are visited one by one. Quotes are counted per line: a line
/// is well formed exactly when it has two quotes for each field that both starts
/// and ends with one, and no others.
#[inline(always)]
pub fn scan<const N: usize>(data: &[u8], start: usize, end: usize, mut line: impl FnMut(&[(usize, usize); N])) {
    let mut separators = [0usize; 16]; // where each field of the current line ends
    let mut count = 0usize;
    let mut line_start = start;
    let mut quotes = 0u32;

    macro_rules! finish {
        () => {
            if count == N {
                let mut fields = [(0usize, 0usize); N];
                let mut from = line_start;
                for at in 0..N {
                    fields[at] = (from, separators[at]);
                    from = separators[at] + 1;
                }
                let mut good = true;
                if quotes != 0 {
                    let mut wrapped = 0;
                    for field in fields.iter_mut() {
                        if field.1 - field.0 >= 2 && data[field.0] == b'"' && data[field.1 - 1] == b'"' {
                            *field = (field.0 + 1, field.1 - 1);
                            wrapped += 1;
                        }
                    }
                    good = quotes == 2 * wrapped;
                }
                if good {
                    line(&fields);
                }
            }
        };
    }

    let mut block = start;
    while block < end {
        let (mut separator, newline, mut quote) = if block + STEP <= end {
            // SAFETY: the bytes are inside `data`.
            unsafe { masks(data.as_ptr().add(block)) }
        } else {
            let mut tail = [0u8; STEP];
            tail[..end - block].copy_from_slice(&data[block..end]);
            // SAFETY: `tail` is a whole step.
            unsafe { masks(tail.as_ptr()) }
        };
        while separator != 0 {
            let bit = separator.trailing_zeros();
            separators[count & 15] = block + bit as usize;
            count += 1;
            if newline >> bit & 1 != 0 {
                let before = (1u32 << bit) - 1;
                quotes += (quote & before).count_ones();
                quote &= !before;
                finish!();
                line_start = block + bit as usize + 1;
                count = 0;
                quotes = 0;
            }
            separator &= separator - 1;
        }
        quotes += quote.count_ones();
        block += STEP;
    }
    if line_start < end {
        // The file's last line has no newline.
        separators[count & 15] = end;
        count += 1;
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
