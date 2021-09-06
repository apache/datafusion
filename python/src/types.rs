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

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use pyo3::{FromPyObject, PyAny, PyResult};

use crate::errors;

/// utility struct to convert PyObj to native DataType
#[derive(Debug, Clone)]
pub struct PyDataType {
    pub data_type: DataType,
}

impl<'source> FromPyObject<'source> for PyDataType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let str_ob = ob.to_string();
        let id = ob.getattr("id")?.extract::<i32>()?;
        let data_type = data_type_id(&id, &str_ob)?;
        Ok(PyDataType { data_type })
    }
}

fn data_type_id(id: &i32, str_ob: &str) -> Result<DataType, errors::DataFusionError> {
    // see https://github.com/apache/arrow/blob/3694794bdfd0677b95b8c95681e392512f1c9237/python/pyarrow/includes/libarrow.pxd
    // this is not ideal as it does not generalize for non-basic types
    // Find a way to get a unique name from the pyarrow.DataType
    if str_ob.contains("date") {
        Ok(data_type_date(str_ob)?)
    } else if str_ob.contains("time") {
        Ok(data_type_timestamp(str_ob)?)
    } else {
        Ok(match id {
            1 => DataType::Boolean,
            2 => DataType::UInt8,
            3 => DataType::Int8,
            4 => DataType::UInt16,
            5 => DataType::Int16,
            6 => DataType::UInt32,
            7 => DataType::Int32,
            8 => DataType::UInt64,
            9 => DataType::Int64,
            10 => DataType::Float16,
            11 => DataType::Float32,
            12 => DataType::Float64,
            13 => DataType::Utf8,
            14 => DataType::Binary,
            34 => DataType::LargeUtf8,
            35 => DataType::LargeBinary,
            other => {
                return Err(errors::DataFusionError::Common(format!(
                    "The type {} is not valid",
                    other
                )))
            }
        })
    }
}

fn data_type_timestamp(str_ob: &str) -> Result<DataType, errors::DataFusionError> {
    // maps to usage from apache/arrow/pyarrow/types.pxi
    Ok(match str_ob.as_ref() {
        "time32[s]" => DataType::Time32(TimeUnit::Second),
        "time32[ms]" => DataType::Time32(TimeUnit::Millisecond),
        "time64[us]" => DataType::Time64(TimeUnit::Microsecond),
        "time64[ns]" => DataType::Time64(TimeUnit::Nanosecond),
        "timestamp[s]" => DataType::Timestamp(TimeUnit::Second, None),
        "timestamp[ms]" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "timestamp[us]" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp[ns]" => DataType::Timestamp(TimeUnit::Nanosecond, None),
        _ => data_type_timestamp_infer(str_ob)?,
    })
}

fn data_type_date(str_ob: &str) -> Result<DataType, errors::DataFusionError> {
    // maps to usage from apache/arrow/pyarrow/types.pxi
    Ok(match str_ob.as_ref() {
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "date32[day]" => DataType::Date32,
        "date64[ms]" => DataType::Date64,
        _ => {
            return Err(errors::DataFusionError::Common(format!(
                "invalid date {} provided",
                str_ob
            )))
        }
    })
}

fn time_unit_str(unit: &str) -> Result<TimeUnit, errors::DataFusionError> {
    Ok(match unit {
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        "ns" => TimeUnit::Nanosecond,
        _ => {
            return Err(errors::DataFusionError::Common(format!(
                "invalid timestamp unit {} provided",
                unit
            )))
        }
    })
}

fn data_type_timestamp_infer(str_ob: &str) -> Result<DataType, errors::DataFusionError> {
    // parse the timestamp string object - this approach is less than idea, as it requires maintaining
    // this and more direct access methods are better
    let chunks: Vec<_> = str_ob.split("[").collect();
    let timestamp_str: String = chunks[0].to_string();
    let unit_tz: String = chunks[1].to_string().replace(",", "").replace("]", "");

    let mut tz: Option<String> = None;
    let unit: TimeUnit;

    if unit_tz.len() < 3 {
        unit = time_unit_str(&unit_tz)?;
    } else {
        // manage timezones
        let chunks: Vec<_> = unit_tz.split(" ").collect();
        let tz_part: Vec<_> = unit_tz.split("=").collect();
        unit = time_unit_str(&chunks[0])?;
        tz = Some(tz_part[1].to_string());
    }

    Ok(match timestamp_str.as_ref() {
        "time32" => DataType::Time32(unit),
        "time64" => DataType::Time64(unit),
        "timestamp" => DataType::Timestamp(unit, tz),
        _ => {
            return Err(errors::DataFusionError::Common(format!(
                "invalid timestamp string {} provided",
                str_ob
            )))
        }
    })
}
