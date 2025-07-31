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

use crate::logical_plan::producer::to_substrait_precision;
use crate::logical_plan::producer::utils::flatten_names;
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_INTERVAL_DAY_TYPE_VARIATION_REF,
    DEFAULT_MAP_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    DICTIONARY_MAP_TYPE_VARIATION_REF, DURATION_INTERVAL_DAY_TYPE_VARIATION_REF,
    LARGE_CONTAINER_TYPE_VARIATION_REF, TIME_32_TYPE_VARIATION_REF,
    TIME_64_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use datafusion::arrow::datatypes::{DataType, IntervalUnit};
use datafusion::common::{internal_err, not_impl_err, plan_err, DFSchemaRef};
use substrait::proto::{r#type, NamedStruct};

pub(crate) fn to_substrait_type(
    dt: &DataType,
    nullable: bool,
) -> datafusion::common::Result<substrait::proto::Type> {
    let nullability = if nullable {
        r#type::Nullability::Nullable as i32
    } else {
        r#type::Nullability::Required as i32
    };
    match dt {
        DataType::Null => internal_err!("Null cast is not valid"),
        DataType::Boolean => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        // Float16 is not supported in Substrait
        DataType::Float32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp32(r#type::Fp32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Float64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp64(r#type::Fp64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Timestamp(unit, tz) => {
            let precision = to_substrait_precision(unit);
            let kind = match tz {
                None => r#type::Kind::PrecisionTimestamp(r#type::PrecisionTimestamp {
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                    precision,
                }),
                Some(_) => {
                    // If timezone is present, no matter what the actual tz value is, it indicates the
                    // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
                    // As the timezone is lost, this conversion may be lossy for downstream use of the value.
                    r#type::Kind::PrecisionTimestampTz(r#type::PrecisionTimestampTz {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                        precision,
                    })
                }
            };
            Ok(substrait::proto::Type { kind: Some(kind) })
        }
        DataType::Time32(unit) => {
            let precision = to_substrait_precision(unit);
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::PrecisionTime(r#type::PrecisionTime {
                    precision,
                    type_variation_reference: TIME_32_TYPE_VARIATION_REF,
                    nullability,
                })),
            })
        }
        DataType::Time64(unit) => {
            let precision = to_substrait_precision(unit);
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::PrecisionTime(r#type::PrecisionTime {
                    precision,
                    type_variation_reference: TIME_64_TYPE_VARIATION_REF,
                    nullability,
                })),
            })
        }
        DataType::Date32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_32_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Date64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_64_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Interval(interval_unit) => {
            match interval_unit {
                IntervalUnit::YearMonth => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalYear(r#type::IntervalYear {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                    })),
                }),
                IntervalUnit::DayTime => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalDay(r#type::IntervalDay {
                        type_variation_reference: DEFAULT_INTERVAL_DAY_TYPE_VARIATION_REF,
                        nullability,
                        precision: Some(3), // DayTime precision is always milliseconds
                    })),
                }),
                IntervalUnit::MonthDayNano => {
                    Ok(substrait::proto::Type {
                        kind: Some(r#type::Kind::IntervalCompound(
                            r#type::IntervalCompound {
                                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                                nullability,
                                precision: 9, // nanos
                            },
                        )),
                    })
                }
            }
        }
        DataType::Duration(duration_unit) => {
            let precision = to_substrait_precision(duration_unit);
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::IntervalDay(r#type::IntervalDay {
                    type_variation_reference: DURATION_INTERVAL_DAY_TYPE_VARIATION_REF,
                    nullability,
                    precision: Some(precision),
                })),
            })
        }
        DataType::Binary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::FixedSizeBinary(length) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::FixedBinary(r#type::FixedBinary {
                length: *length,
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeBinary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::BinaryView => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeUtf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8View => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::List(inner) => {
            let inner_type = to_substrait_type(inner.data_type(), inner.is_nullable())?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::LargeList(inner) => {
            let inner_type = to_substrait_type(inner.data_type(), inner.is_nullable())?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::Map(inner, _) => match inner.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                let key_type = to_substrait_type(
                    key_and_value[0].data_type(),
                    key_and_value[0].is_nullable(),
                )?;
                let value_type = to_substrait_type(
                    key_and_value[1].data_type(),
                    key_and_value[1].is_nullable(),
                )?;
                Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::Map(Box::new(r#type::Map {
                        key: Some(Box::new(key_type)),
                        value: Some(Box::new(value_type)),
                        type_variation_reference: DEFAULT_MAP_TYPE_VARIATION_REF,
                        nullability,
                    }))),
                })
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        DataType::Dictionary(key_type, value_type) => {
            let key_type = to_substrait_type(key_type, nullable)?;
            let value_type = to_substrait_type(value_type, nullable)?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::Map(Box::new(r#type::Map {
                    key: Some(Box::new(key_type)),
                    value: Some(Box::new(value_type)),
                    type_variation_reference: DICTIONARY_MAP_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::Struct(fields) => {
            let field_types = fields
                .iter()
                .map(|field| to_substrait_type(field.data_type(), field.is_nullable()))
                .collect::<datafusion::common::Result<Vec<_>>>()?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::Struct(r#type::Struct {
                    types: field_types,
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                })),
            })
        }
        DataType::Decimal128(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_128_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        DataType::Decimal256(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_256_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        _ => not_impl_err!("Unsupported cast type: {dt:?}"),
    }
}

pub(crate) fn to_substrait_named_struct(
    schema: &DFSchemaRef,
) -> datafusion::common::Result<NamedStruct> {
    let mut names = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        flatten_names(field, false, &mut names)?;
    }

    let field_types = r#type::Struct {
        types: schema
            .fields()
            .iter()
            .map(|f| to_substrait_type(f.data_type(), f.is_nullable()))
            .collect::<datafusion::common::Result<_>>()?,
        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
        nullability: r#type::Nullability::Required as i32,
    };

    Ok(NamedStruct {
        names,
        r#struct: Some(field_types),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::tests::test_consumer;
    use crate::logical_plan::consumer::{
        from_substrait_named_struct, from_substrait_type_without_names,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema, TimeUnit};
    use datafusion::common::{DFSchema, Result};
    use std::sync::Arc;

    #[test]
    fn round_trip_types() -> Result<()> {
        round_trip_type(DataType::Boolean)?;
        round_trip_type(DataType::Int8)?;
        round_trip_type(DataType::UInt8)?;
        round_trip_type(DataType::Int16)?;
        round_trip_type(DataType::UInt16)?;
        round_trip_type(DataType::Int32)?;
        round_trip_type(DataType::UInt32)?;
        round_trip_type(DataType::Int64)?;
        round_trip_type(DataType::UInt64)?;
        round_trip_type(DataType::Float32)?;
        round_trip_type(DataType::Float64)?;

        for tz in [None, Some("UTC".into())] {
            round_trip_type(DataType::Timestamp(TimeUnit::Second, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Nanosecond, tz))?;
        }

        round_trip_type(DataType::Time32(TimeUnit::Second))?;
        round_trip_type(DataType::Time32(TimeUnit::Millisecond))?;
        round_trip_type(DataType::Time64(TimeUnit::Microsecond))?;
        round_trip_type(DataType::Time64(TimeUnit::Nanosecond))?;
        round_trip_type(DataType::Date32)?;
        round_trip_type(DataType::Date64)?;
        round_trip_type(DataType::Binary)?;
        round_trip_type(DataType::FixedSizeBinary(10))?;
        round_trip_type(DataType::LargeBinary)?;
        round_trip_type(DataType::BinaryView)?;
        round_trip_type(DataType::Utf8)?;
        round_trip_type(DataType::LargeUtf8)?;
        round_trip_type(DataType::Utf8View)?;
        round_trip_type(DataType::Decimal128(10, 2))?;
        round_trip_type(DataType::Decimal256(30, 2))?;

        round_trip_type(DataType::List(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;
        round_trip_type(DataType::LargeList(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;

        round_trip_type(DataType::Map(
            Field::new_struct(
                "entries",
                [
                    Field::new("key", DataType::Utf8, false).into(),
                    Field::new("value", DataType::Int32, true).into(),
                ],
                false,
            )
            .into(),
            false,
        ))?;
        round_trip_type(DataType::Dictionary(
            Box::new(DataType::Utf8),
            Box::new(DataType::Int32),
        ))?;

        round_trip_type(DataType::Struct(
            vec![
                Field::new("c0", DataType::Int32, true),
                Field::new("c1", DataType::Utf8, true),
            ]
            .into(),
        ))?;

        round_trip_type(DataType::Interval(IntervalUnit::YearMonth))?;
        round_trip_type(DataType::Interval(IntervalUnit::MonthDayNano))?;
        round_trip_type(DataType::Interval(IntervalUnit::DayTime))?;

        round_trip_type(DataType::Duration(TimeUnit::Second))?;
        round_trip_type(DataType::Duration(TimeUnit::Millisecond))?;
        round_trip_type(DataType::Duration(TimeUnit::Microsecond))?;
        round_trip_type(DataType::Duration(TimeUnit::Nanosecond))?;

        Ok(())
    }

    fn round_trip_type(dt: DataType) -> Result<()> {
        println!("Checking round trip of {dt:?}");

        // As DataFusion doesn't consider nullability as a property of the type, but field,
        // it doesn't matter if we set nullability to true or false here.
        let substrait = to_substrait_type(&dt, true)?;
        let consumer = test_consumer();
        let roundtrip_dt = from_substrait_type_without_names(&consumer, &substrait)?;
        assert_eq!(dt, roundtrip_dt);
        Ok(())
    }

    #[test]
    fn named_struct_names() -> Result<()> {
        let schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(vec![
            Field::new("int", DataType::Int32, true),
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                )])),
                true,
            ),
            Field::new("trailer", DataType::Float64, true),
        ]))?);

        let named_struct = to_substrait_named_struct(&schema)?;

        // Struct field names should be flattened DFS style
        // List field names should be omitted
        assert_eq!(
            named_struct.names,
            vec!["int", "struct", "inner", "trailer"]
        );

        let roundtrip_schema =
            from_substrait_named_struct(&test_consumer(), &named_struct)?;
        assert_eq!(schema.as_ref(), &roundtrip_schema);
        Ok(())
    }
}
