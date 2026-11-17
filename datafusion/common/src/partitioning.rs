// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
    pub(crate) values: Vec<ScalarValue>,
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

/// Selects `partition_count - 1` evenly spaced split points from `samples` to
/// form `partition_count` range partitions.
///
/// Returns `None` when `partition_count == 0` or
/// `partition_count > samples.len() + 1`.
///
/// # Public Only for Internal Use:
///
/// This is not a public API and is for internal use only; see [API policy] for details.
///
/// [API policy]: https://datafusion.apache.org/contributor-guide/api-health.html#internal-public-apis
#[doc(hidden)]
pub fn downsample_split_points(
    samples: &[SplitPoint],
    partition_count: usize,
) -> Option<Vec<SplitPoint>> {
    if partition_count == 0 || partition_count > samples.len() + 1 {
        return None;
    }
    if partition_count == 1 {
        return Some(vec![]);
    }
    if partition_count == samples.len() + 1 {
        return Some(samples.to_vec());
    }

    let sample_count = samples.len();
    Some(
        (1..partition_count)
            .map(|partition| {
                // Use a wider intermediate so valid slice lengths cannot overflow
                // when calculating the evenly spaced sample index.
                let sample_index = ((partition as u128 * sample_count as u128)
                    / partition_count as u128)
                    as usize;
                samples[sample_index].clone()
            })
            .collect(),
    )
}

/// Validates that split points match the ordering width and are strictly
/// ordered according to the provided sort options.
// TODO: Relax strict (`<`) split-point ordering to non-strict (`<=`) so empty
// interior partitions and skewed quantile cuts with duplicate boundary values
// can be represented (see https://github.com/apache/datafusion/pull/26593).
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
