// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::consumer::types::from_substrait_type;
use crate::logical_plan::consumer::utils::{DEFAULT_TIMEZONE, next_struct_field_name};
use crate::variation_const::FLOAT_16_TYPE_NAME;
#[expect(deprecated)]
use crate::variation_const::{
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_MONTH_DAY_NANO_TYPE_NAME,
    INTERVAL_MONTH_DAY_NANO_TYPE_REF, INTERVAL_YEAR_MONTH_TYPE_REF,
    LARGE_CONTAINER_TYPE_VARIATION_REF, TIME_32_TYPE_VARIATION_REF,
    TIME_64_TYPE_VARIATION_REF, TIMESTAMP_MICRO_TYPE_VARIATION_REF,
    TIMESTAMP_MILLI_TYPE_VARIATION_REF, TIMESTAMP_NANO_TYPE_VARIATION_REF,
    TIMESTAMP_SECOND_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use datafusion::arrow::array::{AsArray, MapArray, new_empty_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{Field, IntervalDayTime, IntervalMonthDayNano};
use datafusion::arrow::temporal_conversions::NANOSECONDS;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::common::{
    ScalarValue, not_impl_err, plan_err, substrait_datafusion_err, substrait_err,
};
use datafusion::logical_expr::Expr;
use prost::Message;
use std::sync::Arc;
use substrait::proto;
use substrait::proto::expression::Literal;
use substrait::proto::expression::literal::user_defined::Val;
use substrait::proto::expression::literal::{
    IntervalCompound, IntervalDayToSecond, IntervalYearToMonth, LiteralType,
    interval_day_to_second,
};

pub async fn from_literal(
    consumer: &impl SubstraitConsumer,
    expr: &Literal,
) -> datafusion::common::Result<Expr> {
    let scalar_value = from_substrait_literal_without_names(consumer, expr)?;
    Ok(Expr::Literal(scalar_value, None))
}

pub(crate) fn from_substrait_literal_without_names(
    consumer: &impl SubstraitConsumer,
    lit: &Literal,
) -> datafusion::common::Result<ScalarValue> {
    from_substrait_literal(consumer, lit, &vec![], &mut 0)
}

pub(crate) fn from_substrait_literal(
    consumer: &impl SubstraitConsumer,
    lit: &Literal,
    dfs_names: &Vec<String>,
    name_idx: &mut usize,
) -> datafusion::common::Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        #[expect(deprecated)]
        Some(LiteralType::Timestamp(t)) => {
            // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
            #[expect(deprecated)]
            match lit.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampSecond(Some(*t), None)
                }
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMillisecond(Some(*t), None)
                }
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMicrosecond(Some(*t), None)
                }
                TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampNanosecond(Some(*t), None)
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::PrecisionTimestamp(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(Some(pt.value), None),
            3 => ScalarValue::TimestampMillisecond(Some(pt.value), None),
            6 => ScalarValue::TimestampMicrosecond(Some(pt.value), None),
            9 => ScalarValue::TimestampNanosecond(Some(pt.value), None),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::PrecisionTimestampTz(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            3 => ScalarValue::TimestampMillisecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            6 => ScalarValue::TimestampMicrosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            9 => ScalarValue::TimestampNanosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::PrecisionTime(pt)) => match pt.precision {
            0 => match lit.type_variation_reference {
                TIME_32_TYPE_VARIATION_REF => {
                    ScalarValue::Time32Second(Some(pt.value as i32))
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            },
            3 => match lit.type_variation_reference {
                TIME_32_TYPE_VARIATION_REF => {
                    ScalarValue::Time32Millisecond(Some(pt.value as i32))
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            },
            6 => match lit.type_variation_reference {
                TIME_64_TYPE_VARIATION_REF => {
                    ScalarValue::Time64Microsecond(Some(pt.value))
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            },
            9 => match lit.type_variation_reference {
                TIME_64_TYPE_VARIATION_REF => {
                    ScalarValue::Time64Nanosecond(Some(pt.value))
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            },
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTime"
                );
            }
        },
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8View(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => {
                ScalarValue::LargeBinary(Some(b.clone()))
            }
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::BinaryView(Some(b.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d
                .value
                .clone()
                .try_into()
                .or(substrait_err!("Failed to parse decimal value"))?;
            let p = d.precision.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal precision: {e}")
            })?;
            let s = d.scale.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal scale: {e}")
            })?;
            ScalarValue::Decimal128(Some(i128::from_le_bytes(value)), p, s)
        }
        Some(LiteralType::List(l)) => {
            // Each element should start the name index from the same value, then we increase it
            // once at the end
            let mut element_name_idx = *name_idx;
            let elements = l
                .values
                .iter()
                .map(|el| {
                    element_name_idx = *name_idx;
                    from_substrait_literal(consumer, el, dfs_names, &mut element_name_idx)
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            *name_idx = element_name_idx;
            if elements.is_empty() {
                return substrait_err!(
                    "Empty list must be encoded as EmptyList literal type, not List"
                );
            }
            let element_type = elements[0].data_type();
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::List(
                    ScalarValue::new_list_nullable(elements.as_slice(), &element_type),
                ),
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(elements.as_slice(), &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::EmptyList(l)) => {
            let element_type = from_substrait_type(
                consumer,
                l.r#type.clone().unwrap().as_ref(),
                dfs_names,
                name_idx,
            )?;
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => {
                    ScalarValue::List(ScalarValue::new_list_nullable(&[], &element_type))
                }
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(&[], &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::Map(m)) => {
            // Each entry should start the name index from the same value, then we increase it
            // once at the end
            let mut entry_name_idx = *name_idx;
            let entries = m
                .key_values
                .iter()
                .map(|kv| {
                    entry_name_idx = *name_idx;
                    let key_sv = from_substrait_literal(
                        consumer,
                        kv.key.as_ref().unwrap(),
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    let value_sv = from_substrait_literal(
                        consumer,
                        kv.value.as_ref().unwrap(),
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    ScalarStructBuilder::new()
                        .with_scalar(Field::new("key", key_sv.data_type(), false), key_sv)
                        .with_scalar(
                            Field::new("value", value_sv.data_type(), true),
                            value_sv,
                        )
                        .build()
                })
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            *name_idx = entry_name_idx;

            if entries.is_empty() {
                return substrait_err!(
                    "Empty map must be encoded as EmptyMap literal type, not Map"
                );
            }

            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries[0].data_type(), false)),
                OffsetBuffer::new(vec![0, entries.len() as i32].into()),
                ScalarValue::iter_to_array(entries)?.as_struct().to_owned(),
                None,
                false,
            )))
        }
        Some(LiteralType::EmptyMap(m)) => {
            let key = match &m.key {
                Some(k) => Ok(k),
                _ => plan_err!("Missing key type for empty map"),
            }?;
            let value = match &m.value {
                Some(v) => Ok(v),
                _ => plan_err!("Missing value type for empty map"),
            }?;
            let key_type = from_substrait_type(consumer, key, dfs_names, name_idx)?;
            let value_type = from_substrait_type(consumer, value, dfs_names, name_idx)?;

            // new_empty_array on a MapType creates a too empty array
            // We want it to contain an empty struct array to align with an empty MapBuilder one
            let entries = Field::new_struct(
                "entries",
                vec![
                    Field::new("key", key_type, false),
                    Field::new("value", value_type, true),
                ],
                false,
            );
            let struct_array =
                new_empty_array(entries.data_type()).as_struct().to_owned();
            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(entries),
                OffsetBuffer::new(vec![0, 0].into()),
                struct_array,
                None,
                false,
            )))
        }
        Some(LiteralType::Struct(s)) => {
            let mut builder = ScalarStructBuilder::new();
            for (i, field) in s.fields.iter().enumerate() {
                let name = next_struct_field_name(i, dfs_names, name_idx)?;
                let sv = from_substrait_literal(consumer, field, dfs_names, name_idx)?;
                // We assume everything to be nullable, since Arrow's strict about things matching
                // and it's hard to match otherwise.
                builder = builder.with_scalar(Field::new(name, sv.data_type(), true), sv);
            }
            builder.build()?
        }
        Some(LiteralType::Null(null_type)) => {
            let data_type =
                from_substrait_type(consumer, null_type, dfs_names, name_idx)?;
            ScalarValue::try_from(&data_type)?
        }
        Some(LiteralType::IntervalDayToSecond(IntervalDayToSecond {
            days,
            seconds,
            subseconds,
            precision_mode,
        })) => {
            use interval_day_to_second::PrecisionMode;
            // DF only supports millisecond precision, so for any more granular type we lose precision
            let milliseconds = match precision_mode {
                #[expect(deprecated)]
                Some(PrecisionMode::Microseconds(ms)) => ms / 1000,
                None => {
                    if *subseconds != 0 {
                        return substrait_err!(
                            "Cannot set subseconds field of IntervalDayToSecond without setting precision"
                        );
                    } else {
                        0_i32
                    }
                }
                Some(PrecisionMode::Precision(0)) => *subseconds as i32 * 1000,
                Some(PrecisionMode::Precision(3)) => *subseconds as i32,
                Some(PrecisionMode::Precision(6)) => (subseconds / 1000) as i32,
                Some(PrecisionMode::Precision(9)) => (subseconds / 1000 / 1000) as i32,
                _ => {
                    return not_impl_err!(
                        "Unsupported Substrait interval day to second precision mode: {precision_mode:?}"
                    );
                }
            };

            ScalarValue::new_interval_dt(*days, (seconds * 1000) + milliseconds)
        }
        Some(LiteralType::IntervalYearToMonth(IntervalYearToMonth { years, months })) => {
            ScalarValue::new_interval_ym(*years, *months)
        }
        Some(LiteralType::IntervalCompound(IntervalCompound {
            interval_year_to_month,
            interval_day_to_second,
        })) => match (interval_year_to_month, interval_day_to_second) {
            (
                Some(IntervalYearToMonth { years, months }),
                Some(IntervalDayToSecond {
                    days,
                    seconds,
                    subseconds,
                    precision_mode:
                        Some(interval_day_to_second::PrecisionMode::Precision(p)),
                }),
            ) => {
                if *p < 0 || *p > 9 {
                    return plan_err!(
                        "Unsupported Substrait interval day to second precision: {}",
                        p
                    );
                }
                let nanos = *subseconds * i64::pow(10, (9 - p) as u32);
                ScalarValue::new_interval_mdn(
                    *years * 12 + months,
                    *days,
                    *seconds as i64 * NANOSECONDS + nanos,
                )
            }
            _ => return plan_err!("Substrait compound interval missing components"),
        },
        Some(LiteralType::FixedChar(c)) => ScalarValue::Utf8(Some(c.clone())),
        Some(LiteralType::UserDefined(user_defined)) => {
            if let Ok(value) = consumer.consume_user_defined_literal(user_defined) {
                return Ok(value);
            }

            // Helper function to prevent duplicating this code - can be inlined once the non-extension path is removed
            let interval_month_day_nano =
                |user_defined: &proto::expression::literal::UserDefined| -> datafusion::common::Result<ScalarValue> {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval month day nano value is empty");
                    };
                    let value_slice: [u8; 16] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval month day nano value"
                            )
                        })?;
                    let months =
                        i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                    let days = i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                    let nanoseconds =
                        i64::from_le_bytes(value_slice[8..16].try_into().unwrap());
                    Ok(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds,
                        },
                    )))
                };

            if let Some(name) = consumer
                .get_extensions()
                .types
                .get(&user_defined.type_reference)
            {
                match name.as_ref() {
                    FLOAT_16_TYPE_NAME => {
                        // Rules for encoding fp16 Substrait literals are defined as part of Arrow here:
                        //
                        // https://github.com/apache/arrow/blame/bab558061696ddc1841148d6210424b12923d48e/format/substrait/extension_types.yaml#L112

                        let Some(value) = user_defined.val.as_ref() else {
                            return substrait_err!("Float16 value is empty");
                        };
                        let Val::Value(value_any) = value else {
                            return substrait_err!(
                                "Float16 value is not a value type literal"
                            );
                        };
                        if value_any.type_url != "google.protobuf.UInt32Value" {
                            return substrait_err!(
                                "Float16 value is not a google.protobuf.UInt32Value"
                            );
                        }
                        let decoded_value =
                            pbjson_types::UInt32Value::decode(value_any.value.clone())
                                .map_err(|err| {
                                    substrait_datafusion_err!(
                                        "Failed to decode float16 value: {err}"
                                    )
                                })?;
                        let u32_bytes = decoded_value.value.to_le_bytes();
                        let f16_val =
                            half::f16::from_le_bytes(u32_bytes[0..2].try_into().unwrap());
                        return Ok(ScalarValue::Float16(Some(f16_val)));
                    }
                    // Kept for backwards compatibility - producers should use IntervalCompound instead
                    #[expect(deprecated)]
                    INTERVAL_MONTH_DAY_NANO_TYPE_NAME => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                            "Unsupported Substrait user defined type with ref {} and name {}",
                            user_defined.type_reference,
                            name
                        );
                    }
                }
            } else {
                #[expect(deprecated)]
                match user_defined.type_reference {
                    // Kept for backwards compatibility, producers should useIntervalYearToMonth instead
                    INTERVAL_YEAR_MONTH_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval year month value is empty");
                        };
                        let value_slice: [u8; 4] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval year month value"
                                )
                            })?;
                        ScalarValue::IntervalYearMonth(Some(i32::from_le_bytes(
                            value_slice,
                        )))
                    }
                    // Kept for backwards compatibility, producers should useIntervalDayToSecond instead
                    INTERVAL_DAY_TIME_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval day time value is empty");
                        };
                        let value_slice: [u8; 8] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval day time value"
                                )
                            })?;
                        let days =
                            i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                        let milliseconds =
                            i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days,
                            milliseconds,
                        }))
                    }
                    // Kept for backwards compatibility, producers should useIntervalCompound instead
                    INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                            "Unsupported Substrait user defined type literal with ref {}",
                            user_defined.type_reference
                        );
                    }
                }
            }
        }
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::utils::tests::test_consumer;

    #[test]
    fn interval_compound_different_precision() -> datafusion::common::Result<()> {
        // DF producer (and thus roundtrip) always uses precision = 9,
        // this test exists to test with some other value.
        let substrait = Literal {
            nullable: false,
            type_variation_reference: 0,
            literal_type: Some(LiteralType::IntervalCompound(IntervalCompound {
                interval_year_to_month: Some(IntervalYearToMonth {
                    years: 1,
                    months: 2,
                }),
                interval_day_to_second: Some(IntervalDayToSecond {
                    days: 3,
                    seconds: 4,
                    subseconds: 5,
                    precision_mode: Some(
                        interval_day_to_second::PrecisionMode::Precision(6),
                    ),
                }),
            })),
        };

        let consumer = test_consumer();
        assert_eq!(
            from_substrait_literal_without_names(&consumer, &substrait)?,
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                months: 14,
                days: 3,
                nanoseconds: 4_000_005_000
            }))
        );

        Ok(())
    }
}
