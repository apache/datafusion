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

use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::PartitionEvaluator;

use super::helpers::{
    create_default_value, gather_array_by_indices, get_classifier_mask,
};
use super::kinds::MREdgeKind;
use super::mask_cache::MaskIndexCache;

#[derive(Debug)]
pub(crate) struct MatchRecognizeEdgeEvaluator {
    pub(super) cache: MaskIndexCache,
    pub(super) kind: MREdgeKind,
    pub(super) has_finalized: bool,
}

impl MatchRecognizeEdgeEvaluator {
    pub(crate) fn new(mask_provided: bool, kind: MREdgeKind) -> Self {
        Self {
            cache: MaskIndexCache::new(mask_provided),
            kind,
            has_finalized: false,
        }
    }
}

impl PartitionEvaluator for MatchRecognizeEdgeEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let data_arr = &values[0];

        if !self.cache.has_mask() {
            return match self.kind {
                MREdgeKind::First => {
                    let first_val = if !data_arr.is_empty() {
                        ScalarValue::try_from_array(data_arr.as_ref(), 0)?
                    } else {
                        create_default_value(data_arr.data_type())?
                    };
                    first_val.to_array_of_size(num_rows)
                }
                MREdgeKind::Last => Ok(Arc::clone(data_arr)),
            };
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.ensure_built(mask_arr);

        let gather_indices: Vec<Option<usize>> = match self.kind {
            MREdgeKind::First => {
                let first_seen_idx = self.cache.first_match_index();
                (0..num_rows)
                    .map(|idx| first_seen_idx.filter(|&first_idx| idx >= first_idx))
                    .collect()
            }
            MREdgeKind::Last => (0..num_rows)
                .map(|idx| self.cache.nearest_match_index_at_or_before(idx))
                .collect(),
        };

        gather_array_by_indices(data_arr, gather_indices)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let data_arr = &values[0];
        let idx = range.start;

        if !self.cache.has_mask() {
            return match self.kind {
                MREdgeKind::First => {
                    if !data_arr.is_empty() {
                        ScalarValue::try_from_array(data_arr.as_ref(), 0)
                    } else {
                        create_default_value(data_arr.data_type())
                    }
                }
                MREdgeKind::Last => ScalarValue::try_from_array(data_arr.as_ref(), idx),
            };
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.ensure_built(mask_arr);

        match self.kind {
            MREdgeKind::First => match self.cache.first_match_index() {
                Some(first_idx) if idx >= first_idx => {
                    ScalarValue::try_from_array(data_arr.as_ref(), first_idx)
                }
                _ => create_default_value(data_arr.data_type()),
            },
            MREdgeKind::Last => {
                if let Some(r) = self.cache.nearest_match_index_at_or_before(idx) {
                    ScalarValue::try_from_array(data_arr.as_ref(), r)
                } else {
                    create_default_value(data_arr.data_type())
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn is_causal(&self) -> bool {
        true
    }

    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let size = state.out_col.len();
        if size == 0 {
            return Ok(());
        }

        let buffer_size = 1;

        let prune = |state: &mut WindowAggState| {
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        };

        match self.kind {
            MREdgeKind::First => {
                if !self.has_finalized {
                    let result =
                        ScalarValue::try_from_array(state.out_col.as_ref(), size - 1)?;
                    self.has_finalized = !result.is_null();
                }

                if self.has_finalized {
                    prune(state);
                    self.cache.clear();
                }
            }
            MREdgeKind::Last => {
                prune(state);
                self.cache.clear();
            }
        }
        Ok(())
    }
}
