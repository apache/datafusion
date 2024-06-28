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

use std::sync::Arc;

use arrow_schema::{DataType, FieldRef, IntervalUnit, TimeUnit};

use crate::logical_type::type_signature::TypeSignature;
use crate::logical_type::LogicalType;

pub type ExtensionTypeRef = Arc<dyn ExtensionType + Send + Sync>;

pub trait ExtensionType: std::fmt::Debug {
    fn display_name(&self) -> &str;
    fn type_signature(&self) -> TypeSignature;
    fn physical_type(&self) -> DataType;

    fn is_comparable(&self) -> bool;
    fn is_orderable(&self) -> bool;
    fn is_numeric(&self) -> bool;
    fn is_floating(&self) -> bool;
}

impl ExtensionType for LogicalType {
    fn display_name(&self) -> &str {
        use crate::logical_type::LogicalType::*;
        match self {
            Null => "Null",
            Boolean => "Boolean",
            Int8 => "Int8",
            Int16 => "Int16",
            Int32 => "Int32",
            Int64 => "Int64",
            UInt8 => "Uint8",
            UInt16 => "Uint16",
            UInt32 => "Uint32",
            UInt64 => "Uint64",
            Float16 => "Float16",
            Float32 => "Float16",
            Float64 => "Float64",
            Date32 => "Date32",
            Date64 => "Date64",
            Time32(_) => "Time32",
            Time64(_) => "Time64",
            Timestamp(_, _) => "Timestamp",
            Duration(_) => "Duration",
            Interval(_) => "Interval",
            Binary => "Binary",
            FixedSizeBinary(_) => "FixedSizeBinary",
            LargeBinary => "LargeBinary",
            Utf8 => "Utf8",
            LargeUtf8 => "LargeUtf8",
            List(_) => "List",
            FixedSizeList(_, _) => "FixedSizeList",
            LargeList(_) => "LargeList",
            Struct(_) => "Struct",
            Map(_, _) => "Map",
            Decimal128(_, _) => "Decimal128",
            Decimal256(_, _) => "Decimal256",
            Extension(ext) => ext.display_name(),
        }
    }

    fn type_signature(&self) -> TypeSignature {
        use crate::logical_type::LogicalType::*;
        fn time_unit_to_param(tu: &TimeUnit) -> &'static str {
            match tu {
                TimeUnit::Second => "second",
                TimeUnit::Millisecond => "millisecond",
                TimeUnit::Microsecond => "microsecond",
                TimeUnit::Nanosecond => "nanosecond",
            }
        }

        match self {
            Boolean => TypeSignature::new("boolean"),
            Int32 => TypeSignature::new("int32"),
            Int64 => TypeSignature::new("int64"),
            UInt64 => TypeSignature::new("uint64"),
            Float32 => TypeSignature::new("float32"),
            Float64 => TypeSignature::new("float64"),
            Timestamp(tu, zone) => {
                let params = if let Some(zone) = zone {
                    vec![time_unit_to_param(tu).into(), zone.as_ref().into()]
                } else {
                    vec![time_unit_to_param(tu).into()]
                };

                TypeSignature::new_with_params("timestamp", params)
            }
            Binary => TypeSignature::new("binary"),
            Utf8 => TypeSignature::new("string"),
            Struct(fields) => {
                let params = fields.iter().map(|f| f.name().into()).collect();
                TypeSignature::new_with_params("struct", params)
            }
            Extension(ext) => ext.type_signature(),
            Null => TypeSignature::new("null"),
            Int8 => TypeSignature::new("int8"),
            Int16 => TypeSignature::new("int16"),
            UInt8 => TypeSignature::new("uint8"),
            UInt16 => TypeSignature::new("uint16"),
            UInt32 => TypeSignature::new("uint32"),
            Float16 => TypeSignature::new("float16"),
            Date32 => TypeSignature::new("date_32"),
            Date64 => TypeSignature::new("date_64"),
            Time32(tu) => TypeSignature::new_with_params(
                "time_32",
                vec![time_unit_to_param(tu).into()],
            ),
            Time64(tu) => TypeSignature::new_with_params(
                "time_64",
                vec![time_unit_to_param(tu).into()],
            ),
            Duration(tu) => TypeSignature::new_with_params(
                "duration",
                vec![time_unit_to_param(tu).into()],
            ),
            Interval(iu) => {
                let iu = match iu {
                    IntervalUnit::YearMonth => "year_month",
                    IntervalUnit::DayTime => "day_time",
                    IntervalUnit::MonthDayNano => "month_day_nano",
                };
                TypeSignature::new_with_params("interval", vec![iu.into()])
            }
            FixedSizeBinary(size) => TypeSignature::new_with_params(
                "fixed_size_binary",
                vec![size.to_string().into()],
            ),
            LargeBinary => TypeSignature::new("large_binary"),
            LargeUtf8 => TypeSignature::new("large_utf_8"),
            List(f) => TypeSignature::new_with_params(
                "list",
                vec![f.data_type().display_name().into()],
            ),
            FixedSizeList(f, size) => TypeSignature::new_with_params(
                "fixed_size_list",
                vec![f.data_type().display_name().into(), size.to_string().into()],
            ),
            LargeList(f) => TypeSignature::new_with_params(
                "large_list",
                vec![f.data_type().display_name().into()],
            ),
            Map(f, b) => TypeSignature::new_with_params(
                "map",
                vec![f.data_type().display_name().into(), b.to_string().into()],
            ),
            Decimal128(a, b) => TypeSignature::new_with_params(
                "decimal_128",
                vec![a.to_string().into(), b.to_string().into()],
            ),
            Decimal256(a, b) => TypeSignature::new_with_params(
                "decimal_256",
                vec![a.to_string().into(), b.to_string().into()],
            ),
        }
    }

    fn physical_type(&self) -> DataType {
        use crate::logical_type::LogicalType::*;
        match self {
            Boolean => DataType::Boolean,
            Int32 => DataType::Int32,
            Int64 => DataType::Int64,
            UInt64 => DataType::UInt64,
            Float32 => DataType::Float32,
            Float64 => DataType::Float64,
            Timestamp(tu, zone) => DataType::Timestamp(tu.clone(), zone.clone()),
            Binary => DataType::Binary,
            Utf8 => DataType::Utf8,
            Struct(fields) => {
                let fields = fields
                    .iter()
                    .map(|f| FieldRef::new(f.as_ref().clone().into()))
                    .collect::<Vec<_>>();
                DataType::Struct(fields.into())
            }
            Extension(ext) => ext.physical_type(),
            Null => DataType::Null,
            Int8 => DataType::Int8,
            Int16 => DataType::Int16,
            UInt8 => DataType::UInt8,
            UInt16 => DataType::UInt16,
            UInt32 => DataType::UInt32,
            Float16 => DataType::Float16,
            Date32 => DataType::Date32,
            Date64 => DataType::Date64,
            Time32(tu) => DataType::Time32(tu.to_owned()),
            Time64(tu) => DataType::Time64(tu.to_owned()),
            Duration(tu) => DataType::Duration(tu.to_owned()),
            Interval(iu) => DataType::Interval(iu.to_owned()),
            FixedSizeBinary(size) => DataType::FixedSizeBinary(size.to_owned()),
            LargeBinary => DataType::LargeBinary,
            LargeUtf8 => DataType::LargeUtf8,
            List(f) => DataType::List(FieldRef::new(f.as_ref().clone().into())),
            FixedSizeList(f, size) => DataType::FixedSizeList(FieldRef::new(f.as_ref().clone().into()), size.to_owned()),
            LargeList(f) => DataType::LargeList(FieldRef::new(f.as_ref().clone().into())),
            Map(f, b) => DataType::Map(FieldRef::new(f.as_ref().clone().into()), b.to_owned()),
            Decimal128(a, b) => DataType::Decimal128(a.to_owned(), b.to_owned()),
            Decimal256(a, b) => DataType::Decimal256(a.to_owned(), b.to_owned()),
        }
    }

    fn is_comparable(&self) -> bool {
        use crate::logical_type::LogicalType::*;
        match self {
            Null
            | Boolean
            | Int8
            | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Date32
            | Date64
            | Time32(_)
            | Time64(_)
            | Timestamp(_, _)
            | Duration(_)
            | Interval(_)
            | Binary
            | FixedSizeBinary(_)
            | LargeBinary
            | Utf8
            | LargeUtf8
            | Decimal128(_, _)
            | Decimal256(_, _) => true,
            Extension(ext) => ext.is_comparable(),
            _ => false,
        }
    }

    fn is_orderable(&self) -> bool {
        todo!()
    }

    #[inline]
    fn is_numeric(&self) -> bool {
        use crate::logical_type::LogicalType::*;
        match self {
            UInt8
            | UInt16
            | UInt32
            | UInt64
            | Int8
            | Int16
            | Int32
            | Int64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _) => true,
            Extension(t) => t.is_numeric(),
            _ => false,
        }
    }

    #[inline]
    fn is_floating(&self) -> bool {
        use crate::logical_type::LogicalType::*;
        match self {
            Float16 | Float32 | Float64 => true,
            Extension(t) => t.is_floating(),
            _ => false,
        }
    }
}
