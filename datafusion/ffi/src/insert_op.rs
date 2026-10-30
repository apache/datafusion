// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::logical_plan::dml::InsertOp;

/// FFI safe version of [`InsertOp`].
#[expect(non_camel_case_types)]
#[repr(u8)]
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
