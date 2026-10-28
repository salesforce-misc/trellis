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

    /// The value with its sign flipped (zero stays zero).
    pub fn negate(&self) -> Numeric {
        Numeric {
            negative: !self.negative && !self.is_zero(),
            digits: self.digits.clone(),
            scale: self.scale,
        }
    }

    /// The absolute value.
    pub fn abs(&self) -> Numeric {
        Numeric {
            negative: false,
            digits: self.digits.clone(),
            scale: self.scale,
        }
    }

    /// Whether the value is zero, at any scale.
    pub fn is_zero(&self) -> bool {
        self.digits.iter().all(|&d| d == 0)
    }

    /// Postgres `numeric` subtraction: `self + (-other)`.
    pub fn sub(&self, other: &Numeric) -> Numeric {
        self.add(&other.negate())
    }

    /// Postgres `numeric` multiplication. The exact product's scale is the
    /// sum of the operands' (Postgres's `mul_var` result scale before it
    /// rounds to its display scale, which only matters past 16 digits).
    pub fn mul(&self, other: &Numeric) -> Numeric {
        let mut product = vec![0u32; self.digits.len() + other.digits.len()];
        for (i, &a) in self.digits.iter().enumerate().rev() {
            for (j, &b) in other.digits.iter().enumerate().rev() {
                product[i + j + 1] += u32::from(a) * u32::from(b);
            }
        }
        for k in (1..product.len()).rev() {
            product[k - 1] += product[k] / 10;
            product[k] %= 10;
        }
        let mut digits: Vec<u8> = product.into_iter().map(|d| d as u8).collect();
        strip_leading_zeros(&mut digits);
        let is_zero = digits.iter().all(|&d| d == 0);
        Numeric {
            negative: (self.negative != other.negative) && !is_zero,
            digits,
            scale: self.scale + other.scale,
        }
    }

    /// Postgres `numeric` remainder (`%`): the result has the dividend's sign
    /// and the common scale of the operands, and `None` for a zero divisor.
    pub fn rem(&self, other: &Numeric) -> Option<Numeric> {
        if other.is_zero() {
            return None;
        }
        let scale = self.scale.max(other.scale);
        let (_, remainder) =
            divmod_magnitudes(&self.scaled_digits(scale), &other.scaled_digits(scale));
        let is_zero = remainder.iter().all(|&d| d == 0);
        Some(Numeric {
            negative: self.negative && !is_zero,
            digits: remainder,
            scale,
        })
    }

    /// Postgres `numeric` division: the quotient rounded (half away from
    /// zero) to the scale `select_div_scale` picks, and `None` for a zero
    /// divisor. The scale gives the quotient at least sixteen significant
    /// digits, and at least each operand's own scale (a quotient's tail below
    /// that is gone, which matters when a later `%` or zero test reads it).
    pub fn div(&self, other: &Numeric) -> Option<Numeric> {
        if other.is_zero() {
            return None;
        }
        let rscale = select_div_scale(self, other);
        // The quotient to `rscale + 1` digits, truncated, then rounded.
        let shift = i64::from(rscale) + 1 + i64::from(other.scale) - i64::from(self.scale);
        let (mut dividend, mut divisor) = (self.digits.clone(), other.digits.clone());
        if shift >= 0 {
            dividend.extend(std::iter::repeat_n(0, shift as usize));
        } else {
            divisor.extend(std::iter::repeat_n(0, (-shift) as usize));
        }
        let (mut quotient, _) = divmod_magnitudes(&dividend, &divisor);
        // At least `rscale + 1` digits, so the last one is the rounding digit.
        while quotient.len() < rscale as usize + 2 {
            quotient.insert(0, 0);
        }
        let round_up = quotient.pop().is_some_and(|digit| digit >= 5);
        if round_up {
            let mut one = vec![1];
            pad_front(&mut one, quotient.len());
            quotient = add_magnitudes(&quotient, &one);
        }
        strip_leading_zeros(&mut quotient);
        let is_zero = quotient.iter().all(|&d| d == 0);
        Some(Numeric {
            negative: (self.negative != other.negative) && !is_zero,
            digits: quotient,
            scale: rscale,
        })
    }

    /// The weight (in base-10000 digits, as Postgres stores a `numeric`) and
    /// value of the leading base-10000 digit; `(0, 0)` for zero.
    fn leading_base_10000_digit(&self) -> (i64, u32) {
        if self.is_zero() {
            return (0, 0);
        }
        // `digits[0]` sits at decimal exponent `exponent`.
        let exponent = self.digits.len() as i64 - 1 - i64::from(self.scale);
        let weight = exponent.div_euclid(4);
        let mut first = 0u32;
        for p in (4 * weight..4 * weight + 4).rev() {
            let at = exponent - p;
            let digit = if at >= 0 && (at as usize) < self.digits.len() {
                u32::from(self.digits[at as usize])
            } else {
                0
            };
            first = first * 10 + digit;
        }
        (weight, first)
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

/// Postgres's `select_div_scale`: the result scale of `a / b`, giving the
/// quotient sixteen significant digits, and no fewer fractional digits than
/// either operand has, within `0..=1000`.
fn select_div_scale(a: &Numeric, b: &Numeric) -> u32 {
    let (weight_a, first_a) = a.leading_base_10000_digit();
    let (weight_b, first_b) = b.leading_base_10000_digit();
    let mut quotient_weight = weight_a - weight_b;
    if first_a <= first_b {
        quotient_weight -= 1;
    }
    let scale = (16 - quotient_weight * 4)
        .max(i64::from(a.scale))
        .max(i64::from(b.scale))
        .clamp(0, 1000);
    scale as u32
}

/// Schoolbook long division of two magnitude digit vectors (most-significant
/// first): the quotient and the remainder, both normalized. `divisor` is
/// nonzero.
fn divmod_magnitudes(dividend: &[u8], divisor: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let mut quotient = Vec::with_capacity(dividend.len());
    let mut remainder: Vec<u8> = vec![0];
    for &digit in dividend {
        remainder.push(digit);
        strip_leading_zeros(&mut remainder);
        let mut count = 0u8;
        loop {
            let len = remainder.len().max(divisor.len());
            let mut a = remainder.clone();
            let mut b = divisor.to_vec();
            pad_front(&mut a, len);
            pad_front(&mut b, len);
            if compare_magnitudes(&a, &b) == Ordering::Less {
                break;
            }
            remainder = sub_magnitudes(&a, &b);
            count += 1;
        }
        quotient.push(count);
    }
    strip_leading_zeros(&mut quotient);
    if quotient.is_empty() {
        quotient.push(0);
    }
    (quotient, remainder)
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
    fn sub_mul_and_sign_operations() {
        assert_eq!(n("5").sub(&n("7.5")).to_string(), "-2.5");
        assert_eq!(n("-2").mul(&n("3.5")).to_string(), "-7.0");
        assert_eq!(n("0").mul(&n("-3")).to_string(), "0");
        assert_eq!(
            n("99999999999999999999")
                .mul(&n("99999999999999999999"))
                .to_string(),
            "9999999999999999999800000000000000000001"
        );
        assert_eq!(n("-0.50").negate().negate().to_string(), "-0.50");
        assert!(n("0.00").negate().is_zero());
        assert_eq!(n("-3").abs().to_string(), "3");
    }

    #[test]
    fn rem_and_div_follow_the_dividends_sign() {
        assert_eq!(n("-7").rem(&n("2")).unwrap().to_string(), "-1");
        assert_eq!(n("7").rem(&n("-2")).unwrap().to_string(), "1");
        assert_eq!(n("-1.5").rem(&n("1.0")).unwrap().to_string(), "-0.5");
        assert_eq!(n("4").rem(&n("2")).unwrap().to_string(), "0");
        assert!(n("4").rem(&n("0.0")).is_none());
        assert!(n("4").div(&n("0")).is_none());
        assert!(n("0").div(&n("9223372036854775808")).unwrap().is_zero());
        // Postgres: `select 1::numeric / 3` is 0.33333333333333333333.
        assert_eq!(
            n("1").div(&n("3")).unwrap().to_string(),
            "0.33333333333333333333"
        );
        assert_eq!(
            n("2").div(&n("3")).unwrap().to_string(),
            "0.66666666666666666667"
        );
        assert_eq!(
            n("-7").div(&n("2")).unwrap().to_string(),
            "-3.5000000000000000"
        );
        // Quotients as Postgres 17 prints them.
        for (a, b, expected) in [
            ("1", "7", "0.14285714285714285714"),
            ("123456789", "7", "17636684.142857142857"),
            ("0.5", "3", "0.16666666666666666667"),
            ("1", "1000000", "0.000001000000000000000000"),
            ("100.00", "3", "33.3333333333333333"),
            (
                "12345678901234567890",
                "0.000123",
                "100371373180768844634146.341463",
            ),
            ("5", "-0.0003", "-16666.666666666667"),
        ] {
            assert_eq!(n(a).div(&n(b)).unwrap().to_string(), expected, "{a} / {b}");
        }
        // The quotient's tail beyond the chosen scale is rounded away, as
        // Postgres does: this is `-4294967294.00000000` there, and its
        // remainder by one is zero.
        let q = n("9223372036854775808").div(&n("-2147483649")).unwrap();
        assert_eq!(q.to_string(), "-4294967294.00000000");
        assert!(q.rem(&n("1")).unwrap().is_zero());
        assert_eq!(
            n("9223372036854775808")
                .div(&n("1"))
                .unwrap()
                .to_string()
                .split('.')
                .next(),
            Some("9223372036854775808")
        );
        // A tiny quotient keeps its significant digits.
        let tiny = n("1").div(&n(&format!("1{}", "0".repeat(400)))).unwrap();
        assert!(!tiny.is_zero());
    }

    #[test]
    fn add_is_deterministic() {
        let a = n("123.456");
        let b = n("789.1");
        assert_eq!(a.add(&b), a.add(&b));
    }
}
