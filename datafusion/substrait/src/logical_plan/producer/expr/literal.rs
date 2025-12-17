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

use crate::logical_plan::producer::{SubstraitProducer, to_substrait_type};
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DECIMAL_128_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF, FLOAT_16_TYPE_NAME,
    LARGE_CONTAINER_TYPE_VARIATION_REF, TIME_32_TYPE_VARIATION_REF,
    TIME_64_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use datafusion::arrow::array::{Array, GenericListArray, OffsetSizeTrait};
use datafusion::arrow::temporal_conversions::NANOSECONDS;
use datafusion::common::{ScalarValue, exec_err, not_impl_err};
use substrait::proto::expression::literal::interval_day_to_second::PrecisionMode;
use substrait::proto::expression::literal::map::KeyValue;
use substrait::proto::expression::literal::{
    Decimal, IntervalCompound, IntervalDayToSecond, IntervalYearToMonth, List,
    LiteralType, Map, PrecisionTime, PrecisionTimestamp, Struct,
};
use substrait::proto::expression::{Literal, RexType};
use substrait::proto::{Expression, r#type};

pub fn from_literal(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> datafusion::common::Result<Expression> {
    to_substrait_literal_expr(producer, value)
}

pub(crate) fn to_substrait_literal_expr(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> datafusion::common::Result<Expression> {
    let literal = to_substrait_literal(producer, value)?;
    Ok(Expression {
        rex_type: Some(RexType::Literal(literal)),
    })
}

pub(crate) fn to_substrait_literal(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> datafusion::common::Result<Literal> {
    if value.is_null() {
        return Ok(Literal {
            nullable: true,
            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
            literal_type: Some(LiteralType::Null(to_substrait_type(
                producer,
                &value.data_type(),
                true,
            )?)),
        });
    }
    let (literal_type, type_variation_reference) = match value {
        ScalarValue::Boolean(Some(b)) => {
            (LiteralType::Boolean(*b), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Int8(Some(n)) => {
            (LiteralType::I8(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt8(Some(n)) => (
            LiteralType::I8(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int16(Some(n)) => {
            (LiteralType::I16(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt16(Some(n)) => (
            LiteralType::I16(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int32(Some(n)) => (LiteralType::I32(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt32(Some(n)) => (
            LiteralType::I32(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int64(Some(n)) => (LiteralType::I64(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt64(Some(n)) => (
            LiteralType::I64(*n as i64),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Float16(Some(f)) => {
            // Rules for encoding fp16 Substrait literals are defined as part of Arrow here:
            //
            // https://github.com/apache/arrow/blame/bab558061696ddc1841148d6210424b12923d48e/format/substrait/extension_types.yaml#L112
            //
            // fp16 literals are encoded as user defined literals with
            // a google.protobuf.UInt32Value message where the lower 16 bits are
            // the fp16 value.
            let type_anchor = producer.register_type(FLOAT_16_TYPE_NAME.to_string());

            // The spec says "lower 16 bits" but neglects to mention the endianness.
            // Let's just use little-endian for now.
            //
            // See https://github.com/apache/arrow/issues/47846
            let f_bytes = f.to_le_bytes();
            let value = u32::from_le_bytes([f_bytes[0], f_bytes[1], 0, 0]);

            let value = pbjson_types::UInt32Value { value };
            let encoded_value = prost::Message::encode_to_vec(&value);
            (
                LiteralType::UserDefined(
                    substrait::proto::expression::literal::UserDefined {
                        type_reference: type_anchor,
                        type_parameters: vec![],
                        val: Some(substrait::proto::expression::literal::user_defined::Val::Value(
                            pbjson_types::Any {
                                type_url: "google.protobuf.UInt32Value".to_string(),
                                value: encoded_value.into(),
                            },
                        )),
                    },
                ),
                DEFAULT_TYPE_VARIATION_REF,
            )
        }
        ScalarValue::Float32(Some(f)) => {
            (LiteralType::Fp32(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Float64(Some(f)) => {
            (LiteralType::Fp64(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::TimestampSecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        // If timezone is present, no matter what the actual tz value is, it indicates the
        // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
        // As the timezone is lost, this conversion may be lossy for downstream use of the value.
        ScalarValue::TimestampSecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Date32(Some(d)) => {
            (LiteralType::Date(*d), DATE_32_TYPE_VARIATION_REF)
        }
        // Date64 literal is not supported in Substrait
        ScalarValue::IntervalYearMonth(Some(i)) => (
            LiteralType::IntervalYearToMonth(IntervalYearToMonth {
                // DF only tracks total months, but there should always be 12 months in a year
                years: *i / 12,
                months: *i % 12,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::IntervalMonthDayNano(Some(i)) => (
            LiteralType::IntervalCompound(IntervalCompound {
                interval_year_to_month: Some(IntervalYearToMonth {
                    years: i.months / 12,
                    months: i.months % 12,
                }),
                interval_day_to_second: Some(IntervalDayToSecond {
                    days: i.days,
                    seconds: (i.nanoseconds / NANOSECONDS) as i32,
                    subseconds: i.nanoseconds % NANOSECONDS,
                    precision_mode: Some(PrecisionMode::Precision(9)), // nanoseconds
                }),
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::IntervalDayTime(Some(i)) => (
            LiteralType::IntervalDayToSecond(IntervalDayToSecond {
                days: i.days,
                seconds: i.milliseconds / 1000,
                subseconds: (i.milliseconds % 1000) as i64,
                precision_mode: Some(PrecisionMode::Precision(3)), // 3 for milliseconds
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Binary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeBinary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::BinaryView(Some(b)) => (
            LiteralType::Binary(b.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::FixedSizeBinary(_, Some(b)) => (
            LiteralType::FixedBinary(b.clone()),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8(Some(s)) => (
            LiteralType::String(s.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeUtf8(Some(s)) => (
            LiteralType::String(s.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8View(Some(s)) => (
            LiteralType::String(s.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Decimal128(v, p, s) if v.is_some() => (
            LiteralType::Decimal(Decimal {
                value: v.unwrap().to_le_bytes().to_vec(),
                precision: *p as i32,
                scale: *s as i32,
            }),
            DECIMAL_128_TYPE_VARIATION_REF,
        ),
        ScalarValue::List(l) => (
            convert_array_to_literal_list(producer, l)?,
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeList(l) => (
            convert_array_to_literal_list(producer, l)?,
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Map(m) => {
            let map = if m.is_empty() || m.value(0).is_empty() {
                let mt = to_substrait_type(producer, m.data_type(), m.is_nullable())?;
                let mt = match mt {
                    substrait::proto::Type {
                        kind: Some(r#type::Kind::Map(mt)),
                    } => Ok(mt.as_ref().to_owned()),
                    _ => exec_err!("Unexpected type for a map: {mt:?}"),
                }?;
                LiteralType::EmptyMap(mt)
            } else {
                let keys = (0..m.keys().len())
                    .map(|i| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(&m.keys(), i)?,
                        )
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                let values = (0..m.values().len())
                    .map(|i| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(&m.values(), i)?,
                        )
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?;

                let key_values = keys
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(k, v)| {
                        Ok(KeyValue {
                            key: Some(k),
                            value: Some(v),
                        })
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?;
                LiteralType::Map(Map { key_values })
            };
            (map, DEFAULT_CONTAINER_TYPE_VARIATION_REF)
        }
        ScalarValue::Time32Second(Some(t)) => (
            LiteralType::PrecisionTime(PrecisionTime {
                precision: 0,
                value: *t as i64,
            }),
            TIME_32_TYPE_VARIATION_REF,
        ),
        ScalarValue::Time32Millisecond(Some(t)) => (
            LiteralType::PrecisionTime(PrecisionTime {
                precision: 3,
                value: *t as i64,
            }),
            TIME_32_TYPE_VARIATION_REF,
        ),
        ScalarValue::Time64Microsecond(Some(t)) => (
            LiteralType::PrecisionTime(PrecisionTime {
                precision: 6,
                value: *t,
            }),
            TIME_64_TYPE_VARIATION_REF,
        ),
        ScalarValue::Time64Nanosecond(Some(t)) => (
            LiteralType::PrecisionTime(PrecisionTime {
                precision: 9,
                value: *t,
            }),
            TIME_64_TYPE_VARIATION_REF,
        ),
        ScalarValue::Struct(s) => (
            LiteralType::Struct(Struct {
                fields: s
                    .columns()
                    .iter()
                    .map(|col| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(col, 0)?,
                        )
                    })
                    .collect::<datafusion::common::Result<Vec<_>>>()?,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        _ => (
            not_impl_err!("Unsupported literal: {value:?}")?,
            DEFAULT_TYPE_VARIATION_REF,
        ),
    };

    Ok(Literal {
        nullable: false,
        type_variation_reference,
        literal_type: Some(literal_type),
    })
}

fn convert_array_to_literal_list<T: OffsetSizeTrait>(
    producer: &mut impl SubstraitProducer,
    array: &GenericListArray<T>,
) -> datafusion::common::Result<LiteralType> {
    assert_eq!(array.len(), 1);
    let nested_array = array.value(0);

    let values = (0..nested_array.len())
        .map(|i| {
            to_substrait_literal(
                producer,
                &ScalarValue::try_from_array(&nested_array, i)?,
            )
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    if values.is_empty() {
        let lt =
            match to_substrait_type(producer, array.data_type(), array.is_nullable())? {
                substrait::proto::Type {
                    kind: Some(r#type::Kind::List(lt)),
                } => lt.as_ref().to_owned(),
                _ => unreachable!(),
            };
        Ok(LiteralType::EmptyList(lt))
    } else {
        Ok(LiteralType::List(List { values }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::from_substrait_literal_without_names;
    use crate::logical_plan::consumer::tests::test_consumer;
    use crate::logical_plan::producer::DefaultSubstraitProducer;
    use datafusion::arrow::array::{Int64Builder, MapBuilder, StringBuilder};
    use datafusion::arrow::datatypes::{
        DataType, Field, IntervalDayTime, IntervalMonthDayNano,
    };
    use datafusion::common::Result;
    use datafusion::common::scalar::ScalarStructBuilder;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[test]
    fn round_trip_literals() -> Result<()> {
        round_trip_literal(ScalarValue::Boolean(None))?;
        round_trip_literal(ScalarValue::Boolean(Some(true)))?;
        round_trip_literal(ScalarValue::Boolean(Some(false)))?;

        round_trip_literal(ScalarValue::Int8(None))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MIN)))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MAX)))?;
        round_trip_literal(ScalarValue::UInt8(None))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MIN)))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MAX)))?;

        round_trip_literal(ScalarValue::Int16(None))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MIN)))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MAX)))?;
        round_trip_literal(ScalarValue::UInt16(None))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MIN)))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MAX)))?;

        round_trip_literal(ScalarValue::Int32(None))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MIN)))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MAX)))?;
        round_trip_literal(ScalarValue::UInt32(None))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MIN)))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MAX)))?;

        round_trip_literal(ScalarValue::Int64(None))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MIN)))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MAX)))?;
        round_trip_literal(ScalarValue::UInt64(None))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MIN)))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MAX)))?;

        for (ts, tz) in [
            (Some(12345), None),
            (None, None),
            (Some(12345), Some("UTC".into())),
            (None, Some("UTC".into())),
        ] {
            round_trip_literal(ScalarValue::TimestampSecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMillisecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMicrosecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampNanosecond(ts, tz))?;
        }

        // Test Time32 literals
        round_trip_literal(ScalarValue::Time32Second(Some(45296)))?;
        round_trip_literal(ScalarValue::Time32Second(None))?;
        round_trip_literal(ScalarValue::Time32Millisecond(Some(45296789)))?;
        round_trip_literal(ScalarValue::Time32Millisecond(None))?;

        // Test Time64 literals
        round_trip_literal(ScalarValue::Time64Microsecond(Some(45296789123)))?;
        round_trip_literal(ScalarValue::Time64Microsecond(None))?;
        round_trip_literal(ScalarValue::Time64Nanosecond(Some(45296789123000)))?;
        round_trip_literal(ScalarValue::Time64Nanosecond(None))?;

        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(Arc::new(GenericListArray::new_null(
            Field::new_list_field(DataType::Float32, true).into(),
            1,
        ))))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(Arc::new(
            GenericListArray::new_null(
                Field::new_list_field(DataType::Float32, true).into(),
                1,
            ),
        )))?;

        // Null map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(false)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Empty map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Valid map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.keys().append_value("key1");
        map_builder.keys().append_value("key2");
        map_builder.values().append_value(1);
        map_builder.values().append_value(2);
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        let c0 = Field::new("c0", DataType::Boolean, true);
        let c1 = Field::new("c1", DataType::Int32, true);
        let c2 = Field::new("c2", DataType::Utf8, true);
        round_trip_literal(
            ScalarStructBuilder::new()
                .with_scalar(c0.to_owned(), ScalarValue::Boolean(Some(true)))
                .with_scalar(c1.to_owned(), ScalarValue::Int32(Some(1)))
                .with_scalar(c2.to_owned(), ScalarValue::Utf8(None))
                .build()?,
        )?;
        round_trip_literal(ScalarStructBuilder::new_null(vec![c0, c1, c2]))?;

        round_trip_literal(ScalarValue::IntervalYearMonth(Some(17)))?;
        round_trip_literal(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(17, 25, 1234567890),
        )))?;
        round_trip_literal(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            57, 123456,
        ))))?;

        Ok(())
    }

    fn round_trip_literal(scalar: ScalarValue) -> Result<()> {
        println!("Checking round trip of {scalar:?}");
        let state = SessionContext::default().state();
        let mut producer = DefaultSubstraitProducer::new(&state);
        let substrait_literal = to_substrait_literal(&mut producer, &scalar)?;
        let roundtrip_scalar =
            from_substrait_literal_without_names(&test_consumer(), &substrait_literal)?;
        assert_eq!(scalar, roundtrip_scalar);
        Ok(())
    }
}
