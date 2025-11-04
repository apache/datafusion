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

use crate::arrow_wrappers::WrappedArray;
use crate::expr::util::{rvec_u8_to_scalar_value, scalar_value_to_rvec_u8};
use abi_stable::std_types::RVec;
use abi_stable::StableAbi;
use datafusion_common::DataFusionError;
use datafusion_expr::ColumnarValue;

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_ColumnarValue {
    Array(WrappedArray),
    Scalar(RVec<u8>),
}

impl TryFrom<ColumnarValue> for FFI_ColumnarValue {
    type Error = DataFusionError;
    fn try_from(value: ColumnarValue) -> Result<Self, Self::Error> {
        Ok(match value {
            ColumnarValue::Array(v) => {
                FFI_ColumnarValue::Array(WrappedArray::try_from(&v)?)
            }
            ColumnarValue::Scalar(v) => {
                FFI_ColumnarValue::Scalar(scalar_value_to_rvec_u8(&v)?)
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
                ColumnarValue::Scalar(rvec_u8_to_scalar_value(&v)?)
            }
        })
    }
}
