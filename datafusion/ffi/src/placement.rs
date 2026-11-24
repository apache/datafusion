// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion_expr::ExpressionPlacement;

#[expect(non_camel_case_types)]
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FFI_ExpressionPlacement {
    Literal,
    Column,
    MoveTowardsLeafNodes,
    KeepInPlace,
}

impl From<ExpressionPlacement> for FFI_ExpressionPlacement {
    fn from(value: ExpressionPlacement) -> Self {
        match value {
            ExpressionPlacement::Literal => Self::Literal,
            ExpressionPlacement::Column => Self::Column,
            ExpressionPlacement::MoveTowardsLeafNodes => Self::MoveTowardsLeafNodes,
            ExpressionPlacement::KeepInPlace => Self::KeepInPlace,
        }
    }
}

impl From<FFI_ExpressionPlacement> for ExpressionPlacement {
    fn from(value: FFI_ExpressionPlacement) -> Self {
        match value {
            FFI_ExpressionPlacement::Literal => Self::Literal,
            FFI_ExpressionPlacement::Column => Self::Column,
            FFI_ExpressionPlacement::MoveTowardsLeafNodes => Self::MoveTowardsLeafNodes,
            FFI_ExpressionPlacement::KeepInPlace => Self::KeepInPlace,
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::logical_expr::ExpressionPlacement;

    use super::FFI_ExpressionPlacement;

    fn test_round_trip_placement(placement: ExpressionPlacement) {
        let ffi_placement: FFI_ExpressionPlacement = placement.into();
        let round_trip: ExpressionPlacement = ffi_placement.into();

        assert_eq!(placement, round_trip);
    }

    #[test]
    fn test_all_round_trip_placement() {
        test_round_trip_placement(ExpressionPlacement::Literal);
        test_round_trip_placement(ExpressionPlacement::Column);
        test_round_trip_placement(ExpressionPlacement::MoveTowardsLeafNodes);
        test_round_trip_placement(ExpressionPlacement::KeepInPlace);
    }
}
