//! Timestamps: the three valid forms of rules.md, as milliseconds since the epoch.

pub const MINUTE: i64 = 60_000;
pub const DAY: i64 = 86_400_000;

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

#[inline(always)]
fn two(bytes: &[u8], at: usize) -> Option<i64> {
    let (a, b) = (bytes[at].wrapping_sub(b'0'), bytes[at + 1].wrapping_sub(b'0'));
    (a < 10 && b < 10).then(|| i64::from(a) * 10 + i64::from(b))
}

/// `YYYY-MM-DDTHH:MM:SS` at the start of `bytes` (at least 19 long), as UTC milliseconds.
#[inline(always)]
fn civil(bytes: &[u8]) -> Option<i64> {
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = two(bytes, 0)? * 100 + two(bytes, 2)?;
    let (month, day) = (two(bytes, 5)?, two(bytes, 8)?);
    let (hour, minute, second) = (two(bytes, 11)?, two(bytes, 14)?, two(bytes, 17)?);
    if !(1..=12).contains(&month) || day < 1 || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        2 => 28 + i64::from(leap),
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day > days_in_month {
        return None;
    }
    Some(days_from_civil(year, month, day) * DAY + (hour * 3_600 + minute * 60 + second) * 1_000)
}

/// F1, F2 or F3; `None` for a bad timestamp. An F3 value too long to be in any window is `None` too.
#[inline(always)]
pub fn parse(bytes: &[u8]) -> Option<i64> {
    let len = bytes.len();
    if len == 20 && bytes[19] == b'Z' {
        return civil(bytes);
    }
    if len == 25 && (bytes[19] == b'+' || bytes[19] == b'-') && bytes[22] == b':' {
        let (hours, minutes) = (two(bytes, 20)?, two(bytes, 23)?);
        if hours > 23 || minutes > 59 {
            return None;
        }
        let offset = (hours * 60 + minutes) * MINUTE;
        let local = civil(bytes)?;
        return Some(if bytes[19] == b'-' { local + offset } else { local - offset });
    }
    digits(bytes, 18)
}

/// A digits-only field of at most `max` significant digits, leading zeros allowed.
#[inline(always)]
pub fn digits(bytes: &[u8], max: usize) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0i64;
    let mut significant = 0usize;
    for &byte in bytes {
        let digit = byte.wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        significant += usize::from(significant > 0 || digit > 0);
        if significant > max {
            // Too long for any window or price; keep checking that the rest are digits
            // only for form's sake: either way the value is unusable.
            return None;
        }
        value = value * 10 + i64::from(digit);
    }
    Some(value)
}
