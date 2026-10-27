//! A minimal arbitrary-precision decimal for define-time constant folding.
//!
//! [`Numeric`] carries a sign, a decimal-digit vector (the unscaled integer
//! value, most-significant digit first) and a scale. Define time parses the
//! number literals of a constant expression into it and adds them exactly,
//! to refuse a constant sum Postgres could not compute at plan time
//! (`validate::check_constant_sum`). Postgres evaluates every value at run
//! time; this type computes none.

use std::cmp::Ordering;
use std::fmt;

/// An exact base-10 value: `(-1)^negative * digits / 10^scale`, matching
/// Postgres `numeric`'s semantics (arbitrary precision, no binary rounding).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Numeric {
    negative: bool,
    /// The unscaled magnitude's decimal digits, most-significant first.
    /// Normalized to have no leading zero digit unless the value is exactly
    /// zero, in which case this is `[0]`.
    digits: Vec<u8>,
    scale: u32,
}

/// Why a string failed to parse as a [`Numeric`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericParseError {
    /// The text wasn't a valid decimal number (empty, non-digit characters,
    /// more than one `.`, or nothing but a sign/point).
    Invalid { text: String },
}

impl fmt::Display for NumericParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NumericParseError::Invalid { text } => {
                write!(f, "'{text}' is not a valid decimal number")
            }
        }
    }
}

impl std::error::Error for NumericParseError {}

impl Numeric {
    /// Parses a plain decimal string (`-123.450`, `0`, `.5`) — the shape
    /// both the grammar's [`crate::defs::Expr::NumberLiteral`] source text
    /// and Postgres's own `numeric` text output take. No exponent form:
    /// neither producer emits one.
    pub fn parse(text: &str) -> Result<Numeric, NumericParseError> {
        let invalid = || NumericParseError::Invalid {
            text: text.to_string(),
        };

        let trimmed = text.trim();
        let (negative, rest) = match trimmed.strip_prefix('-') {
            Some(r) => (true, r),
            None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
        };

        let (int_part, frac_part) = match rest.split_once('.') {
            Some((i, f)) => (i, f),
            None => (rest, ""),
        };

        if int_part.is_empty() && frac_part.is_empty() {
            return Err(invalid());
        }
        if !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return Err(invalid());
        }

        let scale = frac_part.len() as u32;
        let mut digits: Vec<u8> = int_part
            .bytes()
            .chain(frac_part.bytes())
            .map(|b| b - b'0')
            .collect();
        if digits.is_empty() {
            digits.push(0);
        }
        strip_leading_zeros(&mut digits);

        let is_zero = digits.iter().all(|&d| d == 0);
        Ok(Numeric {
            negative: negative && !is_zero,
            digits,
            scale,
        })
    }

    /// Postgres `numeric` addition: aligns both operands to
    /// `max(self.scale, other.scale)` — Postgres's result-scale rule for
    /// `+` — then adds or subtracts magnitudes per the usual sign rules.
    pub fn add(&self, other: &Numeric) -> Numeric {
        let scale = self.scale.max(other.scale);
        let mut a = self.scaled_digits(scale);
        let mut b = other.scaled_digits(scale);
        let len = a.len().max(b.len());
        pad_front(&mut a, len);
        pad_front(&mut b, len);

        if self.negative == other.negative {
            let sum = add_magnitudes(&a, &b);
            let is_zero = sum.iter().all(|&d| d == 0);
            Numeric {
                negative: self.negative && !is_zero,
                digits: sum,
                scale,
            }
        } else {
            match compare_magnitudes(&a, &b) {
                Ordering::Equal => Numeric {
                    negative: false,
                    digits: vec![0],
                    scale,
                },
                Ordering::Greater => Numeric {
                    negative: self.negative,
                    digits: sub_magnitudes(&a, &b),
                    scale,
                },
                Ordering::Less => Numeric {
                    negative: other.negative,
                    digits: sub_magnitudes(&b, &a),
                    scale,
                },
            }
        }
    }

    /// This value's digit vector re-expressed at `scale` (which must be
    /// `>= self.scale`), by appending trailing zero digits — the digit
    /// vector's fractional end, since digits are most-significant first.
    fn scaled_digits(&self, scale: u32) -> Vec<u8> {
        let pad = (scale - self.scale) as usize;
        let mut digits = self.digits.clone();
        digits.extend(std::iter::repeat_n(0, pad));
        digits
    }
}

impl fmt::Display for Numeric {
    /// Renders in Postgres `numeric`'s own text form: no leading zeros in
    /// the integer part (but always at least one digit), exactly `scale`
    /// digits after the point, no sign on zero.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scale = self.scale as usize;
        if self.negative {
            write!(f, "-")?;
        }

        if scale == 0 {
            for d in &self.digits {
                write!(f, "{d}")?;
            }
            return Ok(());
        }

        if self.digits.len() <= scale {
            write!(f, "0.")?;
            for _ in 0..(scale - self.digits.len()) {
                write!(f, "0")?;
            }
            for d in &self.digits {
                write!(f, "{d}")?;
            }
        } else {
            let split = self.digits.len() - scale;
            for d in &self.digits[..split] {
                write!(f, "{d}")?;
            }
            write!(f, ".")?;
            for d in &self.digits[split..] {
                write!(f, "{d}")?;
            }
        }
        Ok(())
    }
}

/// Pads `digits` with leading zeros (the front, since digits are
/// most-significant first) until it has length `len`.
fn pad_front(digits: &mut Vec<u8>, len: usize) {
    if digits.len() < len {
        let pad = len - digits.len();
        digits.splice(0..0, std::iter::repeat_n(0, pad));
    }
}

fn strip_leading_zeros(digits: &mut Vec<u8>) {
    let mut leading = 0;
    while leading + 1 < digits.len() && digits[leading] == 0 {
        leading += 1;
    }
    if leading > 0 {
        digits.drain(0..leading);
    }
}

/// Adds two equal-length magnitude digit vectors (most-significant first),
/// grade-school style, returning a normalized (no spurious leading zero)
/// result one digit longer at most.
fn add_magnitudes(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert_eq!(a.len(), b.len());
    let mut result = vec![0u8; a.len()];
    let mut carry = 0u8;
    for i in (0..a.len()).rev() {
        let sum = a[i] + b[i] + carry;
        result[i] = sum % 10;
        carry = sum / 10;
    }
    if carry > 0 {
        result.insert(0, carry);
    }
    strip_leading_zeros(&mut result);
    result
}

/// Subtracts `b` from `a` (equal-length magnitude digit vectors), assuming
/// `a >= b` (checked by the caller via [`compare_magnitudes`]).
fn sub_magnitudes(a: &[u8], b: &[u8]) -> Vec<u8> {
    debug_assert_eq!(a.len(), b.len());
    let mut result = vec![0u8; a.len()];
    let mut borrow = 0i8;
    for i in (0..a.len()).rev() {
        let mut diff = a[i] as i8 - b[i] as i8 - borrow;
        if diff < 0 {
            diff += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        result[i] = diff as u8;
    }
    strip_leading_zeros(&mut result);
    result
}

/// Compares two equal-length magnitude digit vectors numerically.
fn compare_magnitudes(a: &[u8], b: &[u8]) -> Ordering {
    debug_assert_eq!(a.len(), b.len());
    a.cmp(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(text: &str) -> Numeric {
        Numeric::parse(text).unwrap()
    }

    #[test]
    fn round_trips_plain_integers() {
        assert_eq!(n("123").to_string(), "123");
        assert_eq!(n("0").to_string(), "0");
    }

    #[test]
    fn round_trips_decimals_with_leading_zero_integer_part() {
        assert_eq!(n("0.5").to_string(), "0.5");
        assert_eq!(n(".5").to_string(), "0.5");
        assert_eq!(n("0.001").to_string(), "0.001");
    }

    #[test]
    fn strips_redundant_leading_zeros() {
        assert_eq!(n("007.50").to_string(), "7.50");
        assert_eq!(n("00").to_string(), "0");
    }

    #[test]
    fn preserves_trailing_fractional_zeros() {
        assert_eq!(n("1.50").to_string(), "1.50");
        assert_eq!(n("1.500").to_string(), "1.500");
    }

    #[test]
    fn negative_values_round_trip() {
        assert_eq!(n("-5.25").to_string(), "-5.25");
    }

    #[test]
    fn negative_zero_normalizes_to_positive() {
        assert_eq!(n("-0").to_string(), "0");
        assert_eq!(n("-0.00").to_string(), "0.00");
    }

    #[test]
    fn rejects_invalid_text() {
        assert!(Numeric::parse("").is_err());
        assert!(Numeric::parse(".").is_err());
        assert!(Numeric::parse("-").is_err());
        assert!(Numeric::parse("1.2.3").is_err());
        assert!(Numeric::parse("abc").is_err());
        assert!(Numeric::parse("1a").is_err());
    }

    #[test]
    fn add_matches_common_scale_rule() {
        assert_eq!(n("1.5").add(&n("2.25")).to_string(), "3.75");
        assert_eq!(n("10").add(&n("0.001")).to_string(), "10.001");
        assert_eq!(n("1.50").add(&n("1.5")).to_string(), "3.00");
    }

    #[test]
    fn add_handles_signs() {
        assert_eq!(n("-1.5").add(&n("2.25")).to_string(), "0.75");
        assert_eq!(n("1.5").add(&n("-2.25")).to_string(), "-0.75");
        assert_eq!(n("-1.5").add(&n("-2.25")).to_string(), "-3.75");
        assert_eq!(n("1.5").add(&n("-1.5")).to_string(), "0.0");
    }

    #[test]
    fn add_handles_large_values() {
        let big = "9".repeat(50);
        let a = Numeric::parse(&big).unwrap();
        let sum = a.add(&n("1")).to_string();
        assert_eq!(sum, format!("1{}", "0".repeat(50)));
    }

    #[test]
    fn add_is_deterministic() {
        let a = n("123.456");
        let b = n("789.1");
        assert_eq!(a.add(&b), a.add(&b));
    }
}
