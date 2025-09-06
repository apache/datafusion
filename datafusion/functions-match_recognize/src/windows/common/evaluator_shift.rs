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

use arrow::array::{Array, ArrayRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::PartitionEvaluator;
use datafusion_functions_window::utils::shift_with_default_value;

use super::helpers::{
    compute_target_index, gather_with_default, get_classifier_mask, resolve_default,
};
use super::kinds::MRShiftKind;
use super::mask_cache::MaskIndexCache;

#[derive(Debug)]
pub(crate) struct MatchRecognizeShiftEvaluator {
    pub(super) cache: MaskIndexCache,
    pub(super) offset: usize,
    pub(super) default_value: ScalarValue,
    pub(super) kind: MRShiftKind,
}

impl MatchRecognizeShiftEvaluator {
    pub(crate) fn new(
        mask_provided: bool,
        offset: usize,
        default_value: ScalarValue,
        kind: MRShiftKind,
    ) -> Self {
        Self {
            cache: MaskIndexCache::new(mask_provided),
            offset,
            default_value,
            kind,
        }
    }
}

impl PartitionEvaluator for MatchRecognizeShiftEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let data_arr = &values[0];
        let default_val = resolve_default(&self.default_value, data_arr.data_type())?;

        if !self.cache.has_mask() {
            let offset_signed = self.kind.offset_sign() * self.offset as i64;
            return shift_with_default_value(data_arr, offset_signed, &default_val);
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.ensure_built(mask_arr);

        let mut gather_indices: Vec<Option<usize>> = Vec::with_capacity(num_rows);
        let mut default_mask: Vec<bool> = Vec::with_capacity(num_rows);

        for row_index in 0..num_rows {
            match self.cache.nearest_match_index_at_or_before(row_index) {
                Some(last_matching_index) => {
                    let target = compute_target_index(
                        self.kind,
                        last_matching_index,
                        self.offset,
                        num_rows,
                    );
                    match target {
                        Some(target_index) => {
                            gather_indices.push(Some(target_index));
                            default_mask.push(false);
                        }
                        None => {
                            gather_indices.push(None);
                            default_mask.push(true);
                        }
                    }
                }
                None => {
                    gather_indices.push(None);
                    default_mask.push(false);
                }
            }
        }

        gather_with_default(data_arr, gather_indices, default_mask, &default_val)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let data_arr = &values[0];
        let row_index = range.start;
        let default_val = resolve_default(&self.default_value, data_arr.data_type())?;

        if !self.cache.has_mask() {
            let target =
                compute_target_index(self.kind, row_index, self.offset, data_arr.len());
            return match target {
                Some(t) => ScalarValue::try_from_array(data_arr.as_ref(), t),
                None => Ok(default_val),
            };
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.ensure_built(mask_arr);
        if let Some(last_matching_index) =
            self.cache.nearest_match_index_at_or_before(row_index)
        {
            let target = compute_target_index(
                self.kind,
                last_matching_index,
                self.offset,
                data_arr.len(),
            );
            match target {
                Some(target_index) => {
                    ScalarValue::try_from_array(data_arr.as_ref(), target_index)
                }
                None => Ok(default_val),
            }
        } else {
            super::helpers::create_default_value(data_arr.data_type())
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn is_causal(&self) -> bool {
        self.kind.is_causal()
    }
}
