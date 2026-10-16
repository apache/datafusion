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

use std::cmp::Ordering;

use arrow::array::{Array, RecordBatch};
use arrow::compute::concat_batches;
use arrow_schema::SortOptions;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{JoinType, Operator};

/// Bounds of the non-null keys in an array sorted with `options`.
pub(super) fn non_null_range(values: &dyn Array, options: SortOptions) -> (usize, usize) {
    let null_count = values.logical_null_count();
    if options.nulls_first {
        (null_count, values.len())
    } else {
        (0, values.len() - null_count)
    }
}

/// Keep the unmatched prefix and any trailing NULL keys outside the matching range.
pub(super) fn unmatched_buffered_batch(
    batch: &RecordBatch,
    min_marked: usize,
    non_null_end: usize,
) -> Result<RecordBatch> {
    let prefix = batch.slice(0, min_marked.min(non_null_end));
    if non_null_end == batch.num_rows() {
        return Ok(prefix);
    }
    let nulls = batch.slice(non_null_end, batch.num_rows() - non_null_end);
    Ok(concat_batches(&batch.schema(), [&prefix, &nulls])?)
}

// Returns boolean for whether the join is a right existence join served by
// `RightExistencePWMJStream`, which reads nothing but a single min/max off the buffered side.
//
// `RightMark` belongs here too: deciding its mark column is the same one-key comparison as
// `RightSemi`/`RightAnti`, just kept instead of used to filter, so it needs no more of the
// buffered side than they do.
pub(super) fn is_right_existence_join(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark
    )
}

// Returns boolean to check if the join type needs to record
// buffered side matches for classic joins
pub(super) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(join_type, JoinType::Full | JoinType::Left)
}

// Whether `operator` also holds when the two keys are equal: `<=` and `>=` do, `<` and `>`
// do not.
pub(super) fn matches_on_equal(operator: Operator) -> Result<bool> {
    match operator {
        Operator::Lt | Operator::Gt => Ok(false),
        Operator::LtEq | Operator::GtEq => Ok(true),
        _ => internal_err!("PiecewiseMergeJoin should not contain operator, {operator}"),
    }
}

// Whether the predicate holds for a streamed key that compares `ordering` to a buffered key
// under the join's sort options. Those are chosen so that `Less` means the predicate holds
// for every operator; `Equal` does too for `<=`/`>=`.
pub(super) fn is_match(ordering: Ordering, match_on_equal: bool) -> bool {
    match ordering {
        Ordering::Less => true,
        Ordering::Equal => match_on_equal,
        Ordering::Greater => false,
    }
}

// Returns the first index in `[lo, hi)` for which `matches` holds, or `hi` if none does.
//
// `matches` must be monotone over that range -- false up to some index, true from there on.
// Both PiecewiseMergeJoin streams search a sorted buffered side, where every match set is a
// suffix, so the first match is a partition point: `O(log(hi - lo))` comparisons instead of a
// walk.
pub(super) fn first_match(
    lo: usize,
    hi: usize,
    matches: impl Fn(usize) -> bool,
) -> usize {
    let (mut first, mut above) = (lo, hi);
    while first < above {
        let mid = first + (above - first) / 2;
        if matches(mid) {
            above = mid;
        } else {
            first = mid + 1;
        }
    }
    first
}

#[cfg(test)]
mod tests {
    use super::first_match;

    /// `first_match` against a linear scan for every range, answer and start up to 40, also
    /// bounding its comparisons by `ceil(log2(hi - lo + 1))`.
    #[test]
    fn first_match_agrees_with_linear_scan() {
        for hi in 0..=40usize {
            for boundary in 0..=hi {
                for lo in 0..=hi {
                    let probes = std::cell::Cell::new(0usize);
                    let matches = |idx: usize| {
                        assert!(
                            idx >= lo && idx < hi,
                            "probed {idx} outside [{lo}, {hi})"
                        );
                        probes.set(probes.get() + 1);
                        idx >= boundary
                    };
                    let expected = (lo..hi).find(|&idx| idx >= boundary).unwrap_or(hi);
                    assert_eq!(
                        first_match(lo, hi, matches),
                        expected,
                        "hi={hi} boundary={boundary} lo={lo}"
                    );

                    let bound = (usize::BITS - (hi - lo).leading_zeros()) as usize;
                    assert!(
                        probes.get() <= bound,
                        "hi={hi} boundary={boundary} lo={lo}: {} probes > {bound}",
                        probes.get()
                    );
                }
            }
        }
    }
}
