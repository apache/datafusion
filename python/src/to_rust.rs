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

use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::arrow::{
    array::{make_array_from_raw, ArrayRef},
    datatypes::Field,
    datatypes::Schema,
    ffi,
    record_batch::RecordBatch,
};
use datafusion::scalar::ScalarValue;
use libc::uintptr_t;
use pyo3::prelude::*;

use crate::{errors, types::PyDataType};

/// converts a pyarrow Scalar into a Rust Scalar
pub fn to_rust_scalar(ob: &PyAny) -> PyResult<ScalarValue> {
    let t = ob
        .getattr("__class__")?
        .getattr("__name__")?
        .extract::<&str>()?;

    let p = ob.call_method0("as_py")?;

    Ok(match t {
        "Int8Scalar" => ScalarValue::Int8(Some(p.extract::<i8>()?)),
        "Int16Scalar" => ScalarValue::Int16(Some(p.extract::<i16>()?)),
        "Int32Scalar" => ScalarValue::Int32(Some(p.extract::<i32>()?)),
        "Int64Scalar" => ScalarValue::Int64(Some(p.extract::<i64>()?)),
        "UInt8Scalar" => ScalarValue::UInt8(Some(p.extract::<u8>()?)),
        "UInt16Scalar" => ScalarValue::UInt16(Some(p.extract::<u16>()?)),
        "UInt32Scalar" => ScalarValue::UInt32(Some(p.extract::<u32>()?)),
        "UInt64Scalar" => ScalarValue::UInt64(Some(p.extract::<u64>()?)),
        "FloatScalar" => ScalarValue::Float32(Some(p.extract::<f32>()?)),
        "DoubleScalar" => ScalarValue::Float64(Some(p.extract::<f64>()?)),
        "BooleanScalar" => ScalarValue::Boolean(Some(p.extract::<bool>()?)),
        "StringScalar" => ScalarValue::Utf8(Some(p.extract::<String>()?)),
        "LargeStringScalar" => ScalarValue::LargeUtf8(Some(p.extract::<String>()?)),
        other => {
            return Err(errors::DataFusionError::Common(format!(
                "Type \"{}\"not yet implemented",
                other
            ))
            .into())
        }
    })
}
