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

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion::common::types::LogicalType;
use datafusion::common::{Result, internal_err};
use datafusion::logical_expr::{TypeSignatureClass, Volatility};

pub fn volatility_to_substrait(volatility: &Volatility) -> (bool, bool) {
    match volatility {
        Volatility::Immutable => (true, false),
        Volatility::Stable => (true, true),
        Volatility::Volatile => (false, false),
    }
}

pub fn type_class_to_substrait(class: &TypeSignatureClass) -> Result<String> {
    match class {
        TypeSignatureClass::Any => Ok("any".to_string()),
        TypeSignatureClass::Timestamp => Ok("timestamp".to_string()),
        TypeSignatureClass::Time => Ok("time".to_string()),
        TypeSignatureClass::Interval => Ok("interval".to_string()),
        TypeSignatureClass::Duration => Ok("duration".to_string()),
        TypeSignatureClass::Integer => Ok("integer".to_string()),
        TypeSignatureClass::Float => Ok("float".to_string()),
        TypeSignatureClass::Decimal => Ok("decimal".to_string()),
        TypeSignatureClass::Numeric => Ok("numeric".to_string()),
        TypeSignatureClass::Binary => Ok("binary".to_string()),
        TypeSignatureClass::Native(logical_type) => {
            let data_type = logical_type.native().default_cast_for(&DataType::Null)?;
            arrow_type_to_substrait(&data_type)
        }
    }
}

pub fn arrow_type_to_substrait(data_type: &DataType) -> Result<String> {
    let data_type = match data_type {
        // Dict-encoded columns carry the same logical type as their value type.
        DataType::Dictionary(_, value_type) => value_type.as_ref(),
        other => other,
    };

    match data_type {
        DataType::Null => Ok("any".to_string()),
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 => Ok("i8".to_string()),
        DataType::Int16 => Ok("i16".to_string()),
        DataType::Int32 => Ok("i32".to_string()),
        DataType::Int64 => Ok("i64".to_string()),
        DataType::UInt8 => Ok("u8".to_string()),
        DataType::UInt16 => Ok("u16".to_string()),
        DataType::UInt32 => Ok("u32".to_string()),
        DataType::UInt64 => Ok("u64".to_string()),
        DataType::Float16 => Ok("fp16".to_string()),
        DataType::Float32 => Ok("fp32".to_string()),
        DataType::Float64 => Ok("fp64".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Ok("string".to_string())
        }
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok("binary".to_string()),
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Time32(unit) | DataType::Time64(unit) => {
            Ok(format!("time<{}>", time_unit_name(unit)))
        }
        DataType::Timestamp(unit, timezone) => {
            let name = if timezone.is_some() {
                "timestamp_tz"
            } else {
                "timestamp"
            };
            Ok(format!("{name}<{}>", time_unit_name(unit)))
        }
        DataType::Duration(unit) => Ok(format!("duration<{}>", time_unit_name(unit))),
        DataType::Interval(unit) => Ok(match unit {
            IntervalUnit::YearMonth => "interval_year_month",
            IntervalUnit::DayTime => "interval_day_time",
            IntervalUnit::MonthDayNano => "interval_month_day_nano",
        }
        .to_string()),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            Ok(format!("decimal<{precision},{scale}>"))
        }
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Ok(format!(
            "list<{}>",
            arrow_type_to_substrait(field.data_type())?
        )),
        DataType::FixedSizeList(field, size) => Ok(format!(
            "fixed_size_list<{},{}>",
            arrow_type_to_substrait(field.data_type())?,
            size
        )),
        DataType::Struct(fields) => {
            let field_types = fields
                .iter()
                .map(|field| arrow_type_to_substrait(field.data_type()))
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("struct<{}>", field_types.join(",")))
        }
        DataType::Map(field, _) => {
            // Arrow maps encode key/value as Struct<key, value> in the inner field.
            if let DataType::Struct(fields) = field.data_type()
                && fields.len() == 2
            {
                let key = arrow_type_to_substrait(fields[0].data_type())?;
                let val = arrow_type_to_substrait(fields[1].data_type())?;
                return Ok(format!("map<{key},{val}>"));
            }
            internal_err!("unexpected Map inner type: {:?}", field.data_type())
        }
        other => {
            internal_err!("unsupported Arrow DataType for Substrait YAML: {other:?}")
        }
    }
}

fn time_unit_name(unit: &TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}
