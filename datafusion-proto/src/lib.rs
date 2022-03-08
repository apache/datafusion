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

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/datafusion.rs"));
}

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod roundtrip_tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit, UnionMode},
        logical_plan::{col, Expr},
        physical_plan::{aggregates, functions::BuiltinScalarFunction::Sqrt},
        prelude::*,
        scalar::ScalarValue,
    };

    // Given a DataFusion type, convert it to protobuf and back, using debug formatting to test
    // equality.
    macro_rules! roundtrip_test {
        ($initial_struct:ident, $proto_type:ty, $struct_type:ty) => {
            let proto: $proto_type = (&$initial_struct).try_into().unwrap();

            let round_trip: $struct_type = (&proto).try_into().unwrap();

            assert_eq!(
                format!("{:?}", $initial_struct),
                format!("{:?}", round_trip)
            );
        };
    }

    fn new_box_field(name: &str, dt: DataType, nullable: bool) -> Box<Field> {
        Box::new(Field::new(name, dt, nullable))
    }

    #[test]
    fn scalar_values_error_serialization() {
        let should_fail_on_seralize: Vec<ScalarValue> = vec![
            //Should fail due to inconsistent types
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::Int16(None),
                    ScalarValue::Float32(Some(32.0)),
                ])),
                Box::new(DataType::List(new_box_field("item", DataType::Int16, true))),
            ),
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(32.0)),
                ])),
                Box::new(DataType::List(new_box_field("item", DataType::Int16, true))),
            ),
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::List(
                        None,
                        Box::new(DataType::List(new_box_field(
                            "level2",
                            DataType::Float32,
                            true,
                        ))),
                    ),
                    ScalarValue::List(
                        Some(Box::new(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ])),
                        Box::new(DataType::List(new_box_field(
                            "level2",
                            DataType::Float32,
                            true,
                        ))),
                    ),
                    ScalarValue::List(
                        None,
                        Box::new(DataType::List(new_box_field(
                            "lists are typed inconsistently",
                            DataType::Int16,
                            true,
                        ))),
                    ),
                ])),
                Box::new(DataType::List(new_box_field(
                    "level1",
                    DataType::List(new_box_field("level2", DataType::Float32, true)),
                    true,
                ))),
            ),
        ];

        for test_case in should_fail_on_seralize.into_iter() {
            let res: std::result::Result<
                super::protobuf::ScalarValue,
                super::to_proto::Error,
            > = (&test_case).try_into();
            assert!(
                res.is_err(),
                "The value {:?} should not have been able to serialize. Serialized to :{:?}",
                test_case, res
            );
        }
    }

    #[test]
    fn round_trip_scalar_values() {
        let should_pass: Vec<ScalarValue> = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::List(None, Box::new(DataType::Boolean)),
            ScalarValue::Date32(None),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampNanosecond(None, None),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(f32::MAX)),
            ScalarValue::Float32(Some(f32::MIN)),
            ScalarValue::Float32(Some(-2000.0)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(f64::MAX)),
            ScalarValue::Float64(Some(f64::MIN)),
            ScalarValue::Float64(Some(-2000.0)),
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int8(Some(i8::MAX)),
            ScalarValue::Int8(Some(0)),
            ScalarValue::Int8(Some(-15)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int16(Some(i16::MAX)),
            ScalarValue::Int16(Some(0)),
            ScalarValue::Int16(Some(-15)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int32(Some(i32::MAX)),
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(-15)),
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
            ScalarValue::Int64(Some(0)),
            ScalarValue::Int64(Some(-15)),
            ScalarValue::UInt8(Some(u8::MAX)),
            ScalarValue::UInt8(Some(0)),
            ScalarValue::UInt16(Some(u16::MAX)),
            ScalarValue::UInt16(Some(0)),
            ScalarValue::UInt32(Some(u32::MAX)),
            ScalarValue::UInt32(Some(0)),
            ScalarValue::UInt64(Some(u64::MAX)),
            ScalarValue::UInt64(Some(0)),
            ScalarValue::Utf8(Some(String::from("Test string   "))),
            ScalarValue::LargeUtf8(Some(String::from("Test Large utf8"))),
            ScalarValue::Date32(Some(0)),
            ScalarValue::Date32(Some(i32::MAX)),
            ScalarValue::TimestampNanosecond(Some(0), None),
            ScalarValue::TimestampNanosecond(Some(i64::MAX), None),
            ScalarValue::TimestampMicrosecond(Some(0), None),
            ScalarValue::TimestampMicrosecond(Some(i64::MAX), None),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::Float32(Some(-213.1)),
                    ScalarValue::Float32(None),
                    ScalarValue::Float32(Some(5.5)),
                    ScalarValue::Float32(Some(2.0)),
                    ScalarValue::Float32(Some(1.0)),
                ])),
                Box::new(DataType::List(new_box_field(
                    "level1",
                    DataType::Float32,
                    true,
                ))),
            ),
            ScalarValue::List(
                Some(Box::new(vec![
                    ScalarValue::List(
                        None,
                        Box::new(DataType::List(new_box_field(
                            "level2",
                            DataType::Float32,
                            true,
                        ))),
                    ),
                    ScalarValue::List(
                        Some(Box::new(vec![
                            ScalarValue::Float32(Some(-213.1)),
                            ScalarValue::Float32(None),
                            ScalarValue::Float32(Some(5.5)),
                            ScalarValue::Float32(Some(2.0)),
                            ScalarValue::Float32(Some(1.0)),
                        ])),
                        Box::new(DataType::List(new_box_field(
                            "level2",
                            DataType::Float32,
                            true,
                        ))),
                    ),
                ])),
                Box::new(DataType::List(new_box_field(
                    "level1",
                    DataType::List(new_box_field("level2", DataType::Float32, true)),
                    true,
                ))),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let proto: super::protobuf::ScalarValue = (&test_case).try_into().unwrap();
            let _roundtrip: ScalarValue = (&proto).try_into().unwrap();
        }
    }

    #[test]
    fn round_trip_scalar_types() {
        let should_pass: Vec<DataType> = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Utf8,
            DataType::LargeUtf8,
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Boolean, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field("Level2", DataType::Date32, true)),
                true,
            )),
        ];

        let should_fail: Vec<DataType> = vec![
            DataType::Null,
            DataType::Float16,
            //Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Decimal(1345, 5431),
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            //Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            //Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ],
                UnionMode::Dense,
            ),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                    Field::new(
                        "nested_struct",
                        DataType::Struct(vec![
                            Field::new("nullable", DataType::Boolean, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("datatype", DataType::Binary, false),
                        ]),
                        true,
                    ),
                ],
                UnionMode::Sparse,
            ),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in should_pass.into_iter() {
            let proto: super::protobuf::ScalarType = (&test_case).try_into().unwrap();
            let roundtrip: DataType = (&proto).try_into().unwrap();
            assert_eq!(format!("{:?}", test_case), format!("{:?}", roundtrip));
        }

        let mut success: Vec<DataType> = Vec::new();
        for test_case in should_fail.into_iter() {
            let proto: std::result::Result<
                super::protobuf::ScalarType,
                super::to_proto::Error,
            > = (&test_case).try_into();
            if proto.is_ok() {
                success.push(test_case)
            }
        }
        assert!(
            success.is_empty(),
            "These should have resulted in an error but completed successfully: {:?}",
            success
        );
    }

    #[test]
    fn round_trip_datatype() {
        let test_cases: Vec<DataType> = vec![
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            //Add more timestamp tests
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time32(TimeUnit::Microsecond),
            DataType::Time32(TimeUnit::Nanosecond),
            DataType::Time64(TimeUnit::Second),
            DataType::Time64(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Time64(TimeUnit::Nanosecond),
            DataType::Duration(TimeUnit::Second),
            DataType::Duration(TimeUnit::Millisecond),
            DataType::Duration(TimeUnit::Microsecond),
            DataType::Duration(TimeUnit::Nanosecond),
            DataType::Interval(IntervalUnit::YearMonth),
            DataType::Interval(IntervalUnit::DayTime),
            DataType::Binary,
            DataType::FixedSizeBinary(0),
            DataType::FixedSizeBinary(1234),
            DataType::FixedSizeBinary(-432),
            DataType::LargeBinary,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Decimal(1345, 5431),
            //Recursive list tests
            DataType::List(new_box_field("Level1", DataType::Binary, true)),
            DataType::List(new_box_field(
                "Level1",
                DataType::List(new_box_field(
                    "Level2",
                    DataType::FixedSizeBinary(53),
                    false,
                )),
                true,
            )),
            //Fixed size lists
            DataType::FixedSizeList(new_box_field("Level1", DataType::Binary, true), 4),
            DataType::FixedSizeList(
                new_box_field(
                    "Level1",
                    DataType::List(new_box_field(
                        "Level2",
                        DataType::FixedSizeBinary(53),
                        false,
                    )),
                    true,
                ),
                41,
            ),
            //Struct Testing
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
            ]),
            DataType::Struct(vec![
                Field::new("nullable", DataType::Boolean, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("datatype", DataType::Binary, false),
                Field::new(
                    "nested_struct",
                    DataType::Struct(vec![
                        Field::new("nullable", DataType::Boolean, false),
                        Field::new("name", DataType::Utf8, false),
                        Field::new("datatype", DataType::Binary, false),
                    ]),
                    true,
                ),
            ]),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ],
                UnionMode::Sparse,
            ),
            DataType::Union(
                vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                    Field::new(
                        "nested_struct",
                        DataType::Struct(vec![
                            Field::new("nullable", DataType::Boolean, false),
                            Field::new("name", DataType::Utf8, false),
                            Field::new("datatype", DataType::Binary, false),
                        ]),
                        true,
                    ),
                ],
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Utf8),
                Box::new(DataType::Struct(vec![
                    Field::new("nullable", DataType::Boolean, false),
                    Field::new("name", DataType::Utf8, false),
                    Field::new("datatype", DataType::Binary, false),
                ])),
            ),
            DataType::Dictionary(
                Box::new(DataType::Decimal(10, 50)),
                Box::new(DataType::FixedSizeList(
                    new_box_field("Level1", DataType::Binary, true),
                    4,
                )),
            ),
        ];

        for test_case in test_cases.into_iter() {
            let proto: super::protobuf::ArrowType = (&test_case).into();
            let roundtrip: DataType = (&proto).try_into().unwrap();
            assert_eq!(format!("{:?}", test_case), format!("{:?}", roundtrip));
        }
    }

    #[test]
    fn roundtrip_null_scalar_values() {
        let test_types = vec![
            ScalarValue::Boolean(None),
            ScalarValue::Float32(None),
            ScalarValue::Float64(None),
            ScalarValue::Int8(None),
            ScalarValue::Int16(None),
            ScalarValue::Int32(None),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(None),
            ScalarValue::UInt16(None),
            ScalarValue::UInt32(None),
            ScalarValue::UInt64(None),
            ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(None),
            ScalarValue::Date32(None),
            ScalarValue::TimestampMicrosecond(None, None),
            ScalarValue::TimestampNanosecond(None, None),
            //ScalarValue::List(None, DataType::Boolean)
        ];

        for test_case in test_types.into_iter() {
            let proto_scalar: super::protobuf::ScalarValue =
                (&test_case).try_into().unwrap();
            let returned_scalar: datafusion::scalar::ScalarValue =
                (&proto_scalar).try_into().unwrap();
            assert_eq!(
                format!("{:?}", &test_case),
                format!("{:?}", returned_scalar)
            );
        }
    }

    #[test]
    fn roundtrip_not() {
        let test_expr = Expr::Not(Box::new(Expr::Literal((1.0).into())));

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_is_null() {
        let test_expr = Expr::IsNull(Box::new(col("id")));

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_is_not_null() {
        let test_expr = Expr::IsNotNull(Box::new(col("id")));

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_between() {
        let test_expr = Expr::Between {
            expr: Box::new(Expr::Literal((1.0).into())),
            negated: true,
            low: Box::new(Expr::Literal((2.0).into())),
            high: Box::new(Expr::Literal((3.0).into())),
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_case() {
        let test_expr = Expr::Case {
            expr: Some(Box::new(Expr::Literal((1.0).into()))),
            when_then_expr: vec![(
                Box::new(Expr::Literal((2.0).into())),
                Box::new(Expr::Literal((3.0).into())),
            )],
            else_expr: Some(Box::new(Expr::Literal((4.0).into()))),
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_cast() {
        let test_expr = Expr::Cast {
            expr: Box::new(Expr::Literal((1.0).into())),
            data_type: DataType::Boolean,
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_sort_expr() {
        let test_expr = Expr::Sort {
            expr: Box::new(Expr::Literal((1.0).into())),
            asc: true,
            nulls_first: true,
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_negative() {
        let test_expr = Expr::Negative(Box::new(Expr::Literal((1.0).into())));

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_inlist() {
        let test_expr = Expr::InList {
            expr: Box::new(Expr::Literal((1.0).into())),
            list: vec![Expr::Literal((2.0).into())],
            negated: true,
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_wildcard() {
        let test_expr = Expr::Wildcard;

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_sqrt() {
        let test_expr = Expr::ScalarFunction {
            fun: Sqrt,
            args: vec![col("col")],
        };
        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }

    #[test]
    fn roundtrip_approx_percentile_cont() {
        let test_expr = Expr::AggregateFunction {
            fun: aggregates::AggregateFunction::ApproxPercentileCont,
            args: vec![col("bananas"), lit(0.42)],
            distinct: false,
        };

        roundtrip_test!(test_expr, super::protobuf::LogicalExprNode, Expr);
    }
}
