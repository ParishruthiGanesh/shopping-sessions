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

/// The scan's working memory, kept by each thread between chunks.
#[derive(Default)]
pub struct Index {
    /// Where each separator is, as an offset from the start of the chunk.
    separators: Vec<u32>,
    /// Where each newline is: a subset of `separators`.
    newlines: Vec<u32>,
    /// Per 32-byte step: its quote mask, and how many quotes come before it.
    quotes: Vec<(u32, u32)>,
}

/// Appends the offset of every set bit of `mask` (plus `base`) at `out`, and
/// returns the pointer after them. Writes up to 8 slots more than it keeps.
#[inline(always)]
unsafe fn flatten(mut mask: u32, base: u32, mut out: *mut u32) -> *mut u32 {
    let count = mask.count_ones() as usize;
    let end = out.add(count);
    // Four at a time without looking at the count: mispredicting a loop exit
    // costs more than writing a few slots that the next call overwrites.
    loop {
        for slot in 0..4 {
            *out.add(slot) = base + mask.trailing_zeros();
            mask &= mask.wrapping_sub(1);
        }
        out = out.add(4);
        if mask == 0 {
            return end;
        }
    }
}

/// Calls `line` with the unquoted `(start, end)` of each field for every
/// well-formed line of `N` fields (at most 16) in `data[start..end]`. `start` is the start
/// of a line and `end` is just after a newline or the end of the file; the
/// chunk must be shorter than 4 GB.
///
/// Stage one finds every separator, newline and quote with vector compares.
/// Stage two walks the lines: a line has `N` fields exactly when its newline is
/// the `N`th separator after the previous newline, and it is well formed exactly
/// when it has two quotes for each field that both starts and ends with one,
/// and no others.
#[inline(always)]
pub fn scan<const N: usize>(index: &mut Index, data: &[u8], start: usize, end: usize, mut line: impl FnMut(&[(usize, usize); N])) {
    let len = end - start;
    if len == 0 {
        return;
    }
    assert!(len < u32::MAX as usize - 64 && end <= data.len());
    let steps = len.div_ceil(STEP);
    // Worst case every byte is a separator; the slack is for `flatten` and the closing newline.
    index.separators.clear();
    index.separators.reserve(len + 32);
    index.newlines.clear();
    index.newlines.reserve(len + 32);
    index.quotes.clear();
    index.quotes.reserve(steps + 1);
    // SAFETY: the reserved capacities cover everything written below: at most one
    // slot per input byte plus the slack, and one quote entry per step.
    unsafe {
        let chunk = data.as_ptr().add(start);
        let separators = index.separators.as_mut_ptr();
        let newlines = index.newlines.as_mut_ptr();
        let quotes = index.quotes.as_mut_ptr();
        let (mut separator_at, mut newline_at) = (separators, newlines);
        let mut quotes_before = 0u32;
        for step in 0..steps {
            let offset = step * STEP;
            let (separator, newline, quote) = if offset + STEP <= len {
                masks(chunk.add(offset))
            } else {
                let mut tail = [0u8; STEP];
                tail[..len - offset].copy_from_slice(&data[start + offset..end]);
                masks(tail.as_ptr())
            };
            separator_at = flatten(separator, offset as u32, separator_at);
            newline_at = flatten(newline, offset as u32, newline_at);
            *quotes.add(step) = (quote, quotes_before);
            quotes_before += quote.count_ones();
        }
        if *chunk.add(len - 1) != b'\n' {
            // The file's last line has no newline: close it as if it had one.
            *separator_at = len as u32;
            *newline_at = len as u32;
            (separator_at, newline_at) = (separator_at.add(1), newline_at.add(1));
        }
        // Stage two looks up to N separators ahead: past the last one it must find
        // nothing that could pass for a newline.
        for slot in 0..16 {
            *separator_at.add(slot) = u32::MAX;
        }
        *quotes.add(steps) = (0, quotes_before);
        let lines = newline_at.offset_from(newlines) as usize;
        let quotes_until = |offset: usize| {
            let (mask, before) = *quotes.add(offset / STEP);
            before + (mask & ((1u32 << (offset % STEP)) - 1)).count_ones()
        };

        let mut separator_at = separators;
        let mut line_start = 0usize;
        for line_at in 0..lines {
            let newline = *newlines.add(line_at);
            if *separator_at.add(N - 1) != newline {
                // Too few or too many fields: skip to the separator that is this newline.
                while *separator_at != newline {
                    separator_at = separator_at.add(1);
                }
                separator_at = separator_at.add(1);
                line_start = newline as usize + 1;
                continue;
            }
            let mut fields = [(0usize, 0usize); N];
            let mut from = line_start;
            for (at, field) in fields.iter_mut().enumerate() {
                let to = *separator_at.add(at) as usize;
                *field = (start + from, start + to);
                from = to + 1;
            }
            separator_at = separator_at.add(N);
            let line_quotes = quotes_until(newline as usize) - quotes_until(line_start);
            line_start = newline as usize + 1;
            if line_quotes != 0 {
                let mut wrapped = 0;
                for field in fields.iter_mut() {
                    // A field of two or more bytes that starts and ends with a quote loses both.
                    let both = field.1 - field.0 >= 2 && *data.get_unchecked(field.0) == b'"' && *data.get_unchecked(field.1 - 1) == b'"';
                    *field = (field.0 + usize::from(both), field.1 - usize::from(both));
                    wrapped += u32::from(both);
                }
                if line_quotes != 2 * wrapped {
                    continue;
                }
            }
            line(&fields);
        }
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

/// Comma, newline and quote masks of the 64 bytes at `at`.
#[inline(always)]
unsafe fn block(at: *const u8) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    let (low, high) = (_mm256_loadu_si256(at.cast()), _mm256_loadu_si256(at.add(32).cast()));
    let mask = |byte: u8| {
        let byte = _mm256_set1_epi8(byte as i8);
        let low = _mm256_movemask_epi8(_mm256_cmpeq_epi8(low, byte)) as u32;
        let high = _mm256_movemask_epi8(_mm256_cmpeq_epi8(high, byte)) as u32;
        u64::from(low) | u64::from(high) << 32
    };
    (mask(b','), mask(b'\n'), mask(b'"'))
}

/// `scan` in one pass and no working memory: 64 bytes at a time, each field is
/// recorded (and unquoted) as its separator is reached, and each newline closes
/// a line. The same lines, the same fields. `start` must not be 0 (it is after
/// the header line).
#[inline(always)]
pub fn lines<const N: usize>(data: &[u8], start: usize, end: usize, mut line: impl FnMut(&[(usize, usize); N])) {
    const { assert!(N >= 1 && N <= 16) };
    let len = end - start;
    if len == 0 {
        return;
    }
    assert!(start > 0 && end <= data.len());
    // The current line's fields so far; past N only the count matters.
    let mut fields = [(0usize, 0usize); 16];
    let (mut count, mut wrapped) = (0usize, 0u64);
    let (mut from, mut line_quotes) = (start, 0u64);
    let mut quotes_before = 0u64;
    let mut offset = 0usize;
    // The file's last line may have no newline: then one more block closes it as if it had.
    let unclosed = data[end - 1] != b'\n';
    while offset < len + usize::from(unclosed) {
        // SAFETY: a whole block is read only when it is inside `data[start..end]`;
        // the last, partial block is copied out first.
        let (comma, newline, quote) = unsafe {
            if offset + 64 <= len {
                block(data.as_ptr().add(start + offset))
            } else {
                let mut tail = [0u8; 64];
                tail[..len - offset].copy_from_slice(&data[start + offset..end]);
                if unclosed {
                    tail[len - offset] = b'\n';
                }
                block(tail.as_ptr())
            }
        };
        let mut separators = comma | newline;
        while separators != 0 {
            let bit = separators.trailing_zeros();
            let at = start + offset + bit as usize;
            // The field `data[from..at]` loses a quote at each end if it has one at both
            // and two bytes or more. Without branches: which fields are quoted is random.
            // `at - 1` is inside `data` (`at` is at least `start`, which is not 0), and so
            // is the clamped first byte.
            // SAFETY: both offsets are inside `data`.
            let (first, last) = unsafe { (*data.get_unchecked(from.min(at - 1)), *data.get_unchecked(at - 1)) };
            let both = (at - from >= 2) & (first == b'"') & (last == b'"');
            fields[count & 15] = (from + usize::from(both), at - usize::from(both));
            wrapped += u64::from(both);
            count += 1;
            from = at + 1;
            if newline >> bit & 1 != 0 {
                let quotes = quotes_before + u64::from((quote & ((1u64 << bit) - 1)).count_ones());
                // N fields exactly, and no quotes but the wrapping ones.
                if count == N && quotes - line_quotes == 2 * wrapped {
                    // SAFETY: the first N of the 16 entries, N being at most 16.
                    line(unsafe { &*fields.as_ptr().cast::<[(usize, usize); N]>() });
                }
                (count, wrapped, line_quotes) = (0, 0, quotes);
            }
            separators &= separators - 1;
        }
        quotes_before += u64::from(quote.count_ones());
        offset += 64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect<const N: usize>(fast: bool, data: &[u8], start: usize, end: usize) -> Vec<[(usize, usize); N]> {
        let mut out = Vec::new();
        let mut index = Index::default();
        if fast {
            lines::<N>(data, start, end, |fields| out.push(*fields));
        } else {
            scan::<N>(&mut index, data, start, end, |fields| out.push(*fields));
        }
        out
    }

    /// `lines` and `scan` agree on random lines of commas, quotes, carriage
    /// returns and letters, short and longer than the window, with or without a final newline.
    #[test]
    fn lines_matches_scan() {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        let mut random = move |below: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % below
        };
        for round in 0..3000 {
            let mut data = b"header\n".to_vec();
            for _ in 0..random(40) {
                let len = match random(10) { 0 => random(300), 1 => 0, _ => random(90) };
                for _ in 0..len {
                    data.push(match random(12) { 0..=2 => b',', 3 => b'"', 4 => b'\r', 5 => b' ', _ => b'a' + random(3) as u8 });
                }
                // Mostly well-formed lines of 2 or 4 fields, some wrapped in quotes.
                if random(2) == 0 {
                    let fields = if random(2) == 0 { 2 } else { 4 };
                    for at in 0..fields {
                        if at > 0 {
                            data.push(b',');
                        }
                        let quoted = random(4) == 0;
                        if quoted {
                            data.push(b'"');
                        }
                        let long = random(8) == 0;
                        for _ in 0..random(if long { 60 } else { 12 }) {
                            data.push(b'a' + random(26) as u8);
                        }
                        if quoted {
                            data.push(b'"');
                        }
                    }
                }
                data.push(b'\n');
            }
            if round % 3 == 0 {
                data.extend_from_slice(b"x,y,z,w");
            }
            for (start, end) in chunks(&data, 1 + random(400) as usize) {
                assert_eq!(collect::<2>(true, &data, start, end), collect::<2>(false, &data, start, end));
                assert_eq!(collect::<4>(true, &data, start, end), collect::<4>(false, &data, start, end));
            }
        }
    }
}
