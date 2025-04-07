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
use arrow::{datatypes::DataType, ffi::FFI_ArrowSchema};
use datafusion::{error::DataFusionError, logical_expr::ReturnInfo};

use crate::arrow_wrappers::WrappedSchema;

/// A stable struct for sharing a [`ReturnInfo`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ReturnInfo {
    return_type: WrappedSchema,
    nullable: bool,
}

impl TryFrom<ReturnInfo> for FFI_ReturnInfo {
    type Error = DataFusionError;

    fn try_from(value: ReturnInfo) -> Result<Self, Self::Error> {
        let return_type = WrappedSchema(FFI_ArrowSchema::try_from(value.return_type())?);
        Ok(Self {
            return_type,
            nullable: value.nullable(),
        })
    }
}

impl TryFrom<FFI_ReturnInfo> for ReturnInfo {
    type Error = DataFusionError;

    fn try_from(value: FFI_ReturnInfo) -> Result<Self, Self::Error> {
        let return_type = DataType::try_from(&value.return_type.0)?;

        Ok(ReturnInfo::new(return_type, value.nullable))
    }
}
