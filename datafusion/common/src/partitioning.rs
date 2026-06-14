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

use crate::utils::compare_rows;
use crate::{Result, ScalarValue, error::_plan_err};
use arrow::compute::SortOptions;
use std::cmp::Ordering;
use std::fmt::{self, Display};

/// A boundary between adjacent range partitions.
///
/// A split point is a tuple with one [`ScalarValue`] per partitioning
/// expression. Split points are interpreted lexicographically according to the
/// ordering of the range partitioning that owns them.
///
/// `N` split points define `N + 1` partitions:
///
/// ```text
/// partition 0: key < split_points[0]
/// partition 1: split_points[0] <= key < split_points[1]
/// ...
/// partition N - 1: split_points[N - 2] <= key < split_points[N - 1]
/// partition N: split_points[N - 1] <= key
/// ```
///
/// Values equal to split point `i` belong to partition `i + 1`, so interior
/// partitions are lower-inclusive and upper-exclusive.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct SplitPoint {
    values: Vec<ScalarValue>,
}

impl SplitPoint {
    /// Creates a new split point from its tuple values.
    pub fn new(values: Vec<ScalarValue>) -> Self {
        Self { values }
    }

    /// Returns the tuple values for this split point.
    pub fn values(&self) -> &[ScalarValue] {
        &self.values
    }
}

impl Display for SplitPoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let values = self
            .values
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "({values})")
    }
}

/// Validates that split points match the ordering width and are strictly
/// ordered according to the provided sort options.
pub fn validate_range_split_points(
    split_points: &[SplitPoint],
    sort_options: &[SortOptions],
) -> Result<()> {
    let width = sort_options.len();
    for (idx, split_point) in split_points.iter().enumerate() {
        let split_point_width = split_point.values().len();
        if split_point_width != width {
            return _plan_err!(
                "Range partitioning split point {idx} has width {split_point_width}, but ordering has width {width}"
            );
        }
    }

    for (idx, split_points) in split_points.windows(2).enumerate() {
        if compare_rows(
            split_points[0].values(),
            split_points[1].values(),
            sort_options,
        )? != Ordering::Less
        {
            return _plan_err!(
                "Range partitioning split points must be strictly ordered: split point {idx} ({}) must be less than split point {} ({})",
                split_points[0],
                idx + 1,
                split_points[1]
            );
        }
    }

    Ok(())
}
