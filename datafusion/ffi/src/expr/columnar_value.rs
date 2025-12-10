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

use abi_stable::StableAbi;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::ColumnarValue;

use crate::arrow_wrappers::WrappedArray;

/// A stable struct for sharing [`ColumnarValue`] across FFI boundaries.
/// Scalar values are passed as an Arrow array of length 1.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_ColumnarValue {
    Array(WrappedArray),
    Scalar(WrappedArray),
}

impl TryFrom<ColumnarValue> for FFI_ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnarValue::Array(v) => {
                FFI_ColumnarValue::Array(WrappedArray::try_from(&v)?)
            }
            ColumnarValue::Scalar(v) => {
                FFI_ColumnarValue::Scalar(WrappedArray::try_from(&v)?)
            }
        })
    }
}

impl TryFrom<FFI_ColumnarValue> for ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: FFI_ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            FFI_ColumnarValue::Array(v) => ColumnarValue::Array(v.try_into()?),
            FFI_ColumnarValue::Scalar(v) => {
                ColumnarValue::Scalar(ScalarValue::try_from(v)?)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::create_array;
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_expr::ColumnarValue;

    use crate::expr::columnar_value::FFI_ColumnarValue;

    #[test]
    fn ffi_columnar_value_round_trip() -> Result<(), DataFusionError> {
        let array = create_array!(Int32, [1, 2, 3, 4, 5]);

        for original in [
            ColumnarValue::Array(array),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ] {
            let ffi_variant = FFI_ColumnarValue::try_from(original.clone())?;

            let returned_value = ColumnarValue::try_from(ffi_variant)?;

            assert_eq!(format!("{returned_value:?}"), format!("{original:?}"));
        }

        Ok(())
    }
}
