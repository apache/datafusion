use half::f16;
use rust_decimal::{Decimal, RoundingStrategy};

pub fn bool_to_str(value: bool) -> String {
    if value {
        "t".to_string()
    } else {
        "f".to_string()
    }
}

pub fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.to_string()
    }
}

pub fn float2_to_str(value: f16) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        decimal_to_str(Decimal::from_f32_retain(value.to_f32()).unwrap())
    }
}

pub fn float4_to_str(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        decimal_to_str(Decimal::from_f32_retain(value).unwrap())
    }
}

pub fn float8_to_str(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        decimal_to_str(Decimal::from_f64_retain(value).unwrap())
    }
}

pub fn decimal_to_str(value: Decimal) -> String {
    value.round_dp(12).normalize().to_string()
}
