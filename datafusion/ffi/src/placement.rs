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
