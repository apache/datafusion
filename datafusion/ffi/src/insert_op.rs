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
use datafusion_expr::logical_plan::dml::InsertOp;

/// FFI safe version of [`InsertOp`].
#[repr(C)]
#[derive(StableAbi)]
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

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::dml::InsertOp;

    use super::FFI_InsertOp;

    fn test_round_trip_insert_op(insert_op: InsertOp) {
        let ffi_insert_op: FFI_InsertOp = insert_op.into();
        let round_trip: InsertOp = ffi_insert_op.into();

        assert_eq!(insert_op, round_trip);
    }

    /// This test ensures we have not accidentally mapped the FFI
    /// enums to the wrong internal enums values.
    #[test]
    fn test_all_round_trip_insert_ops() {
        test_round_trip_insert_op(InsertOp::Append);
        test_round_trip_insert_op(InsertOp::Overwrite);
        test_round_trip_insert_op(InsertOp::Replace);
    }
}
