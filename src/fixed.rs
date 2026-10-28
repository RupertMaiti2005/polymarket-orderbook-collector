//! Exact decimal-string to fixed-point conversion. Exchanges send prices and
//! sizes as decimal strings; going through `f64` would introduce rounding
//! error, so we parse the digits directly.

/// Fixed-point scale used by the output schema: `1_000_000` = 1 unit.
pub const SCALE: i64 = 1_000_000;
const SCALE_DIGITS: usize = 6;

/// Parses a non-negative decimal string such as `"0.533"` or `"1495.95"` into
/// fixed point with six decimal places. Digits beyond the sixth decimal place
/// must be zero, otherwise the value cannot be represented exactly and an
/// error is returned.
pub fn parse_fixed(s: &str) -> anyhow::Result<i64> {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    anyhow::ensure!(
        !(int_part.is_empty() && frac_part.is_empty()),
        "empty decimal {s:?}"
    );
    anyhow::ensure!(
        int_part.bytes().all(|b| b.is_ascii_digit()) && frac_part.bytes().all(|b| b.is_ascii_digit()),
        "invalid decimal {s:?}"
    );
    let (kept, dropped) = frac_part.split_at(frac_part.len().min(SCALE_DIGITS));
    anyhow::ensure!(
        dropped.bytes().all(|b| b == b'0'),
        "decimal {s:?} has more than {SCALE_DIGITS} significant decimal places"
    );

    let mut value: i64 = 0;
    for b in int_part.bytes() {
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(i64::from(b - b'0')))
            .ok_or_else(|| anyhow::anyhow!("decimal {s:?} overflows"))?;
    }
    let mut frac: i64 = 0;
    for b in kept.bytes() {
        frac = frac * 10 + i64::from(b - b'0');
    }
    frac *= 10_i64.pow((SCALE_DIGITS - kept.len()) as u32);
    value
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(|| anyhow::anyhow!("decimal {s:?} overflows"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_decimals() {
        assert_eq!(parse_fixed("0.533").unwrap(), 533_000);
        assert_eq!(parse_fixed("1495.95").unwrap(), 1_495_950_000);
        assert_eq!(parse_fixed("0").unwrap(), 0);
        assert_eq!(parse_fixed("5").unwrap(), 5_000_000);
        assert_eq!(parse_fixed(".5").unwrap(), 500_000);
        assert_eq!(parse_fixed("1.").unwrap(), 1_000_000);
        assert_eq!(parse_fixed("0.000001").unwrap(), 1);
        assert_eq!(parse_fixed("0.1234560000").unwrap(), 123_456);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_fixed("").is_err());
        assert!(parse_fixed(".").is_err());
        assert!(parse_fixed("-1").is_err());
        assert!(parse_fixed("1e5").is_err());
        assert!(parse_fixed("0.0000001").is_err());
        assert!(parse_fixed("99999999999999999999").is_err());
    }
}
