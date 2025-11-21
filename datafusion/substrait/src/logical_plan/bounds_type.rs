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

use datafusion::{
    common::{plan_datafusion_err, Result},
    logical_expr::WindowFrameUnits,
};
use substrait::proto::expression::window_function::BoundsType;

/// Wrapper for the Substrait `BoundsType` to add the `GROUPS` (value 3)
/// variant which `substrait::proto::expression::window_function::BoundsType`
/// currently does not support. This type centralizes conversions to/from the
/// Substrait `i32` representation and to DataFusion's `WindowFrameUnits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundsTypeExt {
    Rows,
    Range,
    Groups,
    Unspecified,
}

const SUBSTRAIT_GROUPS_VALUE: i32 = 3;
impl BoundsTypeExt {
    /// Convert from an i32 value (from Substrait protobuf) to BoundsTypeExt
    pub(crate) fn from_i32(value: i32) -> Result<Self> {
        match value {
            v if v == BoundsType::Rows as i32 => Ok(BoundsTypeExt::Rows),
            v if v == BoundsType::Range as i32 => Ok(BoundsTypeExt::Range),
            v if v == SUBSTRAIT_GROUPS_VALUE => Ok(BoundsTypeExt::Groups),
            v if v == BoundsType::Unspecified as i32 => Ok(BoundsTypeExt::Unspecified),
            _ => Err(plan_datafusion_err!("Invalid bound type: {}", value)),
        }
    }

    /// Convert to i32 value for Substrait protobuf
    pub(crate) fn to_i32(self) -> i32 {
        match self {
            BoundsTypeExt::Rows => BoundsType::Rows as i32,
            BoundsTypeExt::Range => BoundsType::Range as i32,
            BoundsTypeExt::Groups => 3, // Groups variant from Substrait spec
            BoundsTypeExt::Unspecified => BoundsType::Unspecified as i32,
        }
    }

    /// Convert to WindowFrameUnits, applying default logic for Unspecified
    pub(crate) fn to_window_frame_units(self, order_by_empty: bool) -> WindowFrameUnits {
        match self {
            BoundsTypeExt::Rows => WindowFrameUnits::Rows,
            BoundsTypeExt::Range => WindowFrameUnits::Range,
            BoundsTypeExt::Groups => WindowFrameUnits::Groups,
            BoundsTypeExt::Unspecified => {
                // If the plan does not specify the bounds type, then we use a simple logic to determine the units
                // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
                // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
                if order_by_empty {
                    WindowFrameUnits::Rows
                } else {
                    WindowFrameUnits::Range
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_i32_known_values() {
        // Rows
        let v = BoundsType::Rows as i32;
        assert_eq!(BoundsTypeExt::from_i32(v).unwrap(), BoundsTypeExt::Rows);

        // Range
        let v = BoundsType::Range as i32;
        assert_eq!(BoundsTypeExt::from_i32(v).unwrap(), BoundsTypeExt::Range);

        // Groups (custom value)
        let v = SUBSTRAIT_GROUPS_VALUE;
        assert_eq!(BoundsTypeExt::from_i32(v).unwrap(), BoundsTypeExt::Groups);

        // Unspecified
        let v = BoundsType::Unspecified as i32;
        assert_eq!(
            BoundsTypeExt::from_i32(v).unwrap(),
            BoundsTypeExt::Unspecified
        );
    }

    #[test]
    fn test_from_i32_invalid() {
        let invalid = 999;
        let res = BoundsTypeExt::from_i32(invalid);
        assert!(res.is_err(), "expected error for invalid bounds type");
    }

    #[test]
    fn test_to_i32_roundtrip() {
        assert_eq!(BoundsTypeExt::Rows.to_i32(), BoundsType::Rows as i32);
        assert_eq!(BoundsTypeExt::Range.to_i32(), BoundsType::Range as i32);
        assert_eq!(BoundsTypeExt::Groups.to_i32(), SUBSTRAIT_GROUPS_VALUE);
        assert_eq!(
            BoundsTypeExt::Unspecified.to_i32(),
            BoundsType::Unspecified as i32
        );
    }

    #[test]
    fn test_to_window_frame_units_unspecified_defaulting() {
        // When order_by_empty = true -> default to Rows
        assert_eq!(
            BoundsTypeExt::Unspecified.to_window_frame_units(true),
            WindowFrameUnits::Rows
        );

        // When order_by_empty = false -> default to Range
        assert_eq!(
            BoundsTypeExt::Unspecified.to_window_frame_units(false),
            WindowFrameUnits::Range
        );
    }
}
