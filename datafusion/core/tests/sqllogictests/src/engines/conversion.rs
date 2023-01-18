use half::f16;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use bigdecimal::BigDecimal;

pub const NULL_STR: &str = "NULL";

pub fn bool_to_str(value: bool) -> String {
    if value {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

pub fn varchar_to_str(value: &str) -> String {
    if value.is_empty() {
        "(empty)".to_string()
    } else {
        value.to_string()
    }
}

pub fn f16_to_str(value: f16) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f16::INFINITY {
        "Infinity".to_string()
    } else if value == f16::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn f32_to_str(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f32::INFINITY {
        "Infinity".to_string()
    } else if value == f32::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn f64_to_str(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
    }
}

pub fn i128_to_str(value: i128, scale: u32) -> String {
    big_decimal_to_str(BigDecimal::from_str(&Decimal::from_i128_with_scale(value, scale).to_string()).unwrap())
}

pub fn decimal_to_str(value: Decimal) -> String {
    big_decimal_to_str(BigDecimal::from_str(&value.to_string()).unwrap())
}

pub fn big_decimal_to_str(value: BigDecimal) -> String {
    value.round(12).normalized().to_string()
}
