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
