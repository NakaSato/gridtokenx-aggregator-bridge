use anyhow::anyhow;
use rust_decimal::Decimal;

/// Safe conversion from f64 (IoT reading) to Decimal with error context and validation.
pub fn f64_to_decimal(val: f64, label: &str) -> anyhow::Result<Decimal> {
    if val.is_nan() || val.is_infinite() {
        return Err(anyhow!("Invalid numeric value for {}: {}", label, val));
    }

    Decimal::from_f64_retain(val).ok_or_else(|| {
        anyhow!(
            "Failed to convert {} ({}) to Decimal (precision loss or out of range)",
            label,
            val
        )
    })
}

/// Safe conversion of energy/power values, ensuring non-negative results where applicable.
pub fn to_positive_decimal(val: f64, label: &str) -> anyhow::Result<Decimal> {
    let dec = f64_to_decimal(val, label)?;
    if dec < Decimal::ZERO {
        return Err(anyhow!("Unexpected negative value for {}: {}", label, dec));
    }
    Ok(dec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn f64_to_decimal_converts_finite_value() {
        let d = f64_to_decimal(12.5, "kwh").unwrap();
        assert_eq!(d, Decimal::from_str("12.5").unwrap());
    }

    #[test]
    fn f64_to_decimal_allows_negative() {
        // Net power can be negative (export); only NaN/inf are rejected here.
        let d = f64_to_decimal(-3.25, "net_kw").unwrap();
        assert_eq!(d, Decimal::from_str("-3.25").unwrap());
    }

    #[test]
    fn f64_to_decimal_rejects_nan_and_infinite() {
        assert!(f64_to_decimal(f64::NAN, "x").is_err());
        assert!(f64_to_decimal(f64::INFINITY, "x").is_err());
        assert!(f64_to_decimal(f64::NEG_INFINITY, "x").is_err());
    }

    #[test]
    fn f64_to_decimal_error_carries_label() {
        let err = f64_to_decimal(f64::NAN, "energy_consumed").unwrap_err();
        assert!(err.to_string().contains("energy_consumed"));
    }

    #[test]
    fn to_positive_decimal_accepts_zero_and_positive() {
        assert_eq!(to_positive_decimal(0.0, "kwh").unwrap(), Decimal::ZERO);
        assert_eq!(
            to_positive_decimal(7.0, "kwh").unwrap(),
            Decimal::from_str("7").unwrap()
        );
    }

    #[test]
    fn to_positive_decimal_rejects_negative() {
        let err = to_positive_decimal(-0.01, "generated").unwrap_err();
        assert!(err.to_string().contains("negative"));
        assert!(err.to_string().contains("generated"));
    }

    #[test]
    fn to_positive_decimal_rejects_nan() {
        // NaN fails at the inner f64_to_decimal guard before the sign check.
        assert!(to_positive_decimal(f64::NAN, "kwh").is_err());
    }
}
