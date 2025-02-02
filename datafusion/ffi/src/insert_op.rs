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
use datafusion::logical_expr::logical_plan::dml::InsertOp;

/// FFI safe version of [`InsertOp`].
#[repr(C)]
#[derive(StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_InsertOp {
    Append,
    Overwrite,
    Replace,
}

impl From<FFI_InsertOp> for InsertOp {
    fn from(value: FFI_InsertOp) -> Self {
        match value {
            FFI_InsertOp::Append => InsertOp::Append,
            FFI_InsertOp::Overwrite => InsertOp::Overwrite,
            FFI_InsertOp::Replace => InsertOp::Replace,
        }
    }
}

impl From<InsertOp> for FFI_InsertOp {
    fn from(value: InsertOp) -> Self {
        match value {
            InsertOp::Append => FFI_InsertOp::Append,
            InsertOp::Overwrite => FFI_InsertOp::Overwrite,
            InsertOp::Replace => FFI_InsertOp::Replace,
        }
    }
}
