//! A minimal arbitrary-precision decimal sufficient to reproduce Postgres
//! `numeric` addition exactly.
//!
//! Postgres `numeric` is base-10, arbitrary precision, with no binary-float
//! equivalent in Rust `std` — `f64` would introduce rounding Postgres never
//! does. Rather than add a decimal crate (`rust_decimal`/`bigdecimal`), this
//! hand-rolls just enough: a sign, a decimal-digit vector (the unscaled
//! integer value, most-significant digit first), and a scale (digits after
//! the point). That's sufficient to align two operands to a common scale and
//! add their digit vectors like grade-school addition — the only operation
//! this slice needs (per ADR-0004, the grammar and the evaluator's function
//! set are one list, and today that list is just `+`). If a future issue
//! adds `-`/`*`/`/`, this type may need to grow (`*` and `/` change scale by
//! rules addition doesn't need), and a real decimal crate should be
//! reconsidered at that point rather than growing this by hand indefinitely.

#![cfg_attr(not(any(test, feature = "test-util")), allow(dead_code))]

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

    /// Postgres `numeric` comparison (the `>` operator needs it): aligns both
    /// operands to a common scale, the same way [`Self::add`] does, then
    /// compares magnitudes, accounting for sign.
    pub fn compare(&self, other: &Numeric) -> Ordering {
        if self.negative != other.negative {
            return if self.negative {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }
        let scale = self.scale.max(other.scale);
        let mut a = self.scaled_digits(scale);
        let mut b = other.scaled_digits(scale);
        let len = a.len().max(b.len());
        pad_front(&mut a, len);
        pad_front(&mut b, len);
        let magnitude_order = compare_magnitudes(&a, &b);
        if self.negative {
            magnitude_order.reverse()
        } else {
            magnitude_order
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

    #[cfg(any(test, feature = "test-util"))]
    fn is_zero(&self) -> bool {
        self.digits.iter().all(|&d| d == 0)
    }

    /// This value's decimal exponent: `floor(log10(|self|))`, i.e. the
    /// power-of-ten position of the leading digit. Zero has no well-defined
    /// exponent; by convention (matching Postgres's `ndigits == 0` case)
    /// it's treated as `0`.
    #[cfg(any(test, feature = "test-util"))]
    fn decimal_exponent(&self) -> i64 {
        if self.is_zero() {
            0
        } else {
            self.digits.len() as i64 - self.scale as i64 - 1
        }
    }

    /// Postgres numeric stores digits in base-10000 ("NBASE") groups; this is
    /// the index of the group containing the leading digit, `floor(exponent
    /// / 4)`. Needed only to reproduce [`select_div_scale`]'s result-scale
    /// rule for `/`, which is defined in terms of these groups.
    #[cfg(any(test, feature = "test-util"))]
    fn nbase_weight(&self) -> i64 {
        floor_div4(self.decimal_exponent())
    }

    /// The integer value (`0..=9999`) of this value's leading base-10000
    /// digit group — e.g. `1000` for `100000000000`, `3` for `3`. Zero by
    /// convention for a zero value.
    #[cfg(any(test, feature = "test-util"))]
    fn leading_nbase_digit(&self) -> u32 {
        if self.is_zero() {
            return 0;
        }
        let e = self.decimal_exponent();
        let w = floor_div4(e);
        let top_digits = (e - w * 4 + 1) as usize;
        self.digits[..top_digits]
            .iter()
            .fold(0u32, |acc, &d| acc * 10 + d as u32)
    }

    /// Postgres `numeric` division: `self / other`, rounded to the same
    /// result scale Postgres's `div_var`/`select_div_scale` would pick.
    ///
    /// Postgres computes this from the operands' base-10000 digit-group
    /// weights rather than a fixed scale, which is why `1/3` and `100/3` come
    /// out to different numbers of fractional digits. That weight-based rule
    /// isn't in any public API — it was reverse-engineered empirically
    /// (running the matrix of cases this module's tests cover against a real
    /// Postgres instance) rather than transcribed from `numeric.c`, so it's
    /// validated against the tested range, not proven exhaustively.
    ///
    /// # Panics
    ///
    /// If `other` is zero. Callers (aggregate `AVG`) only ever divide by a
    /// non-zero group row count.
    #[cfg(any(test, feature = "test-util"))]
    pub fn div(&self, other: &Numeric) -> Numeric {
        assert!(!other.is_zero(), "division by zero");

        let rscale = select_div_scale(self, other);

        // Scale so the integer division below directly yields the quotient's
        // digits at `rscale` fractional places: multiply the dividend by
        // 10^(other.scale + rscale) and the divisor by 10^(self.scale), so
        // dividend/divisor == (self/other) * 10^rscale.
        let dividend = append_zeros(&self.digits, other.scale as usize + rscale as usize);
        let divisor = append_zeros(&other.digits, self.scale as usize);

        let (mut quotient, remainder) = div_digits(&dividend, &divisor);

        // Round half away from zero: if the remainder is at least half the
        // divisor, bump the last kept digit up.
        let doubled_remainder = mul_small(&remainder, 2);
        if cmp_biguint(&doubled_remainder, &divisor) != Ordering::Less {
            increment(&mut quotient);
        }

        strip_leading_zeros(&mut quotient);
        let is_zero = quotient.iter().all(|&d| d == 0);
        Numeric {
            negative: (self.negative != other.negative) && !is_zero,
            digits: quotient,
            scale: rscale,
        }
    }
}

/// Postgres's `select_div_scale`, reverse-engineered: the number of
/// fractional digits `/` keeps, at least [`NUMERIC_MIN_SIG_DIGITS`]
/// significant digits' worth relative to the quotient's magnitude, and never
/// fewer than either operand's own scale.
#[cfg(any(test, feature = "test-util"))]
fn select_div_scale(v1: &Numeric, v2: &Numeric) -> u32 {
    const NUMERIC_MIN_SIG_DIGITS: i64 = 16;
    const DEC_DIGITS: i64 = 4;

    let w1 = v1.nbase_weight();
    let w2 = v2.nbase_weight();
    // If the numerator's leading digit group is <= the denominator's, the
    // quotient's leading group is one narrower than the naive weight
    // difference suggests.
    let correction = if v1.leading_nbase_digit() <= v2.leading_nbase_digit() {
        1
    } else {
        0
    };
    let qweight = w1 - w2 - correction;

    let rscale = (NUMERIC_MIN_SIG_DIGITS - DEC_DIGITS * qweight)
        .max(v1.scale as i64)
        .max(v2.scale as i64)
        .max(0);
    rscale as u32
}

#[cfg(any(test, feature = "test-util"))]
fn floor_div4(a: i64) -> i64 {
    if a >= 0 { a / 4 } else { (a - 3) / 4 }
}

/// Appends `k` zero digits to the end of a most-significant-first digit
/// vector — i.e. multiplies the represented integer by `10^k`.
#[cfg(any(test, feature = "test-util"))]
fn append_zeros(digits: &[u8], k: usize) -> Vec<u8> {
    let mut v = digits.to_vec();
    v.extend(std::iter::repeat_n(0, k));
    v
}

/// Multiplies a big-endian decimal digit vector by a single digit `0..=9`.
#[cfg(any(test, feature = "test-util"))]
fn mul_small(digits: &[u8], m: u8) -> Vec<u8> {
    if m == 0 {
        return vec![0];
    }
    let mut result = Vec::with_capacity(digits.len() + 1);
    let mut carry = 0u32;
    for &d in digits.iter().rev() {
        let v = d as u32 * m as u32 + carry;
        result.push((v % 10) as u8);
        carry = v / 10;
    }
    while carry > 0 {
        result.push((carry % 10) as u8);
        carry /= 10;
    }
    result.reverse();
    strip_leading_zeros(&mut result);
    result
}

/// Compares two big-endian decimal digit vectors as unsigned integers,
/// ignoring any leading-zero padding (unlike [`compare_magnitudes`], which
/// requires equal length).
#[cfg(any(test, feature = "test-util"))]
fn cmp_biguint(a: &[u8], b: &[u8]) -> Ordering {
    let mut a = a.to_vec();
    let mut b = b.to_vec();
    strip_leading_zeros(&mut a);
    strip_leading_zeros(&mut b);
    if a.len() != b.len() {
        a.len().cmp(&b.len())
    } else {
        a.cmp(&b)
    }
}

/// Subtracts `b` from `a` (unsigned big-endian decimal digit vectors of
/// possibly differing length), assuming `a >= b`.
#[cfg(any(test, feature = "test-util"))]
fn sub_biguint(a: &[u8], b: &[u8]) -> Vec<u8> {
    let mut a = a.to_vec();
    strip_leading_zeros(&mut a);
    let mut b = b.to_vec();
    strip_leading_zeros(&mut b);
    if b.len() < a.len() {
        pad_front(&mut b, a.len());
    }
    sub_magnitudes(&a, &b)
}

/// Adds one to a big-endian decimal digit vector in place, growing it by one
/// digit on overflow (e.g. `999` -> `1000`).
#[cfg(any(test, feature = "test-util"))]
fn increment(digits: &mut Vec<u8>) {
    for d in digits.iter_mut().rev() {
        if *d == 9 {
            *d = 0;
        } else {
            *d += 1;
            return;
        }
    }
    digits.insert(0, 1);
}

/// Long division of two big-endian decimal digit vectors, grade-school
/// style: one quotient digit per dividend digit consumed. Returns
/// `(quotient, remainder)` with `quotient.len() == dividend.len()` (leading
/// zeros included, un-normalized — callers strip them) and `remainder ==
/// dividend % divisor` exactly.
#[cfg(any(test, feature = "test-util"))]
fn div_digits(dividend: &[u8], divisor: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut divisor_trimmed = divisor.to_vec();
    strip_leading_zeros(&mut divisor_trimmed);

    let mut quotient = Vec::with_capacity(dividend.len());
    let mut remainder: Vec<u8> = vec![0];
    for &d in dividend {
        remainder.push(d);
        strip_leading_zeros(&mut remainder);

        let mut chosen = 0u8;
        for candidate in (0..=9u8).rev() {
            let product = mul_small(&divisor_trimmed, candidate);
            if cmp_biguint(&product, &remainder) != Ordering::Greater {
                chosen = candidate;
                break;
            }
        }
        let product = mul_small(&divisor_trimmed, chosen);
        remainder = sub_biguint(&remainder, &product);
        quotient.push(chosen);
    }
    (quotient, remainder)
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
    fn compare_handles_different_scales() {
        assert_eq!(n("1.50").compare(&n("1.5")), Ordering::Equal);
        assert_eq!(n("2").compare(&n("1.999")), Ordering::Greater);
        assert_eq!(n("1.999").compare(&n("2")), Ordering::Less);
    }

    #[test]
    fn compare_handles_signs() {
        assert_eq!(n("-1").compare(&n("1")), Ordering::Less);
        assert_eq!(n("1").compare(&n("-1")), Ordering::Greater);
        assert_eq!(n("-5").compare(&n("-1")), Ordering::Less);
        assert_eq!(n("-1").compare(&n("-5")), Ordering::Greater);
        assert_eq!(n("0").compare(&n("-0")), Ordering::Equal);
    }

    #[test]
    fn add_is_deterministic() {
        let a = n("123.456");
        let b = n("789.1");
        assert_eq!(a.add(&b), a.add(&b));
    }

    /// Expected values in this and the other `div_*` tests below were
    /// captured directly from a real Postgres instance (`select a::numeric /
    /// b::numeric`), per this module's doc comment on [`Numeric::div`].
    #[test]
    fn div_matches_postgres_scale_selection() {
        assert_eq!(n("1").div(&n("3")).to_string(), "0.33333333333333333333");
        assert_eq!(n("100").div(&n("3")).to_string(), "33.3333333333333333");
        assert_eq!(n("3").div(&n("3")).to_string(), "1.00000000000000000000");
        assert_eq!(n("4").div(&n("3")).to_string(), "1.3333333333333333");
        assert_eq!(
            n("100000000000").div(&n("3")).to_string(),
            "33333333333.33333333"
        );
        assert_eq!(
            n("1").div(&n("300000000000")).to_string(),
            "0.0000000000033333333333333333"
        );
    }

    #[test]
    fn div_clamps_to_at_least_the_operands_own_scale() {
        assert_eq!(
            n("1.23456789012345").div(&n("3")).to_string(),
            "0.41152263004115000000"
        );
    }

    #[test]
    fn div_handles_signs() {
        assert_eq!(n("-5").div(&n("3")).to_string(), "-1.6666666666666667");
        assert_eq!(n("5").div(&n("-3")).to_string(), "-1.6666666666666667");
        assert_eq!(n("-5").div(&n("-3")).to_string(), "1.6666666666666667");
    }

    #[test]
    fn div_of_zero_numerator_is_zero_at_full_scale() {
        assert_eq!(n("0").div(&n("3")).to_string(), "0.00000000000000000000");
    }

    #[test]
    fn div_terminating_cases_are_exact() {
        assert_eq!(n("10").div(&n("4")).to_string(), "2.5000000000000000");
        assert_eq!(n("1").div(&n("8")).to_string(), "0.12500000000000000000");
    }

    #[test]
    #[should_panic(expected = "division by zero")]
    fn div_by_zero_panics() {
        let _ = n("1").div(&n("0"));
    }
}
