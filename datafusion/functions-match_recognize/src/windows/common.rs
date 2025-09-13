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

use std::fmt::Debug;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::compute::take as arrow_take;
use arrow::datatypes::DataType;
use datafusion_common::{
    arrow_datafusion_err, exec_datafusion_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::PartitionEvaluator;
use datafusion_expr::{col, Expr};
use datafusion_functions_window::utils::shift_with_default_value;
// removed: get_scalar_value_from_args and PhysicalExpr

// Expression helpers ---------------------------------------------------------

/// Try to construct a boolean classifier bits column `Expr` from an expression
/// by extracting a single symbol predicate. Returns `None` if no symbol could
/// be determined.
pub(super) fn bits_col_expr_from_expr(expr: &Expr) -> Option<Expr> {
    let symbols = datafusion_expr::utils::find_symbol_predicates(expr);
    if symbols.is_empty() {
        None
    } else {
        // If multiple symbols are present, upstream planning should have rejected it.
        // Use the first one deterministically here.
        Some(col(classifier_bits_col_name(&symbols[0])))
    }
}

// Kinds ---------------------------------------------------------------------

#[derive(Copy, Clone, Debug)]
pub(super) enum MRShiftKind {
    Prev,
    Next,
}

impl MRShiftKind {
    pub(super) fn offset_sign(&self) -> i64 {
        match self {
            MRShiftKind::Prev => 1,
            MRShiftKind::Next => -1,
        }
    }

    pub(super) fn is_causal(&self) -> bool {
        matches!(self, MRShiftKind::Prev)
    }
}

#[derive(Copy, Clone, Debug)]
pub(super) enum MREdgeKind {
    First,
    Last,
}

// SymbolIndexCache -----------------------------------------------------------

#[derive(Debug)]
pub(super) struct MaskIndexCache {
    pub(super) mask_provided: bool,
    pub(super) nearest_match_index_by_row: Option<Vec<Option<usize>>>,
    pub(super) first_match_index: Option<usize>,
}

impl MaskIndexCache {
    pub(super) fn new(mask_provided: bool) -> Self {
        Self {
            mask_provided,
            nearest_match_index_by_row: None,
            first_match_index: None,
        }
    }

    #[inline]
    pub(super) fn is_filtered(&self) -> bool {
        self.mask_provided
    }

    pub(super) fn build_if_needed(&mut self, mask_arr: &BooleanArray) {
        if self.nearest_match_index_by_row.is_none() && self.mask_provided {
            let (vec, first) = Self::build_nearest_before(mask_arr);
            self.nearest_match_index_by_row = Some(vec);
            self.first_match_index = first;
        }
    }

    #[inline]
    pub(super) fn nearest_match_index_at_or_before(&self, idx: usize) -> Option<usize> {
        self.nearest_match_index_by_row
            .as_ref()
            .and_then(|v| v[idx])
    }

    #[inline]
    pub(super) fn first_match_index(&self) -> Option<usize> {
        self.first_match_index
    }

    pub(super) fn clear(&mut self) {
        self.nearest_match_index_by_row = None;
        self.first_match_index = None;
    }

    fn build_nearest_before(
        mask_arr: &BooleanArray,
    ) -> (Vec<Option<usize>>, Option<usize>) {
        let mut nearest = Vec::with_capacity(mask_arr.len());
        let mut last_match: Option<usize> = None;
        let mut first_match: Option<usize> = None;

        for i in 0..mask_arr.len() {
            if mask_arr.is_valid(i) && mask_arr.value(i) {
                last_match = Some(i);
                if first_match.is_none() {
                    first_match = last_match;
                }
            }
            nearest.push(last_match);
        }
        (nearest, first_match)
    }
}

// Evaluators ----------------------------------------------------------------

#[derive(Debug)]
pub(super) struct MatchRecognizeEdgeEvaluator {
    pub(super) cache: MaskIndexCache,
    pub(super) kind: MREdgeKind,
    pub(super) has_finalized: bool,
}

impl MatchRecognizeEdgeEvaluator {
    pub(super) fn new(mask_provided: bool, kind: MREdgeKind) -> Self {
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

        if !self.cache.is_filtered() {
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
        self.cache.build_if_needed(mask_arr);

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

        if !self.cache.is_filtered() {
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
        self.cache.build_if_needed(mask_arr);

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

#[derive(Debug)]
pub(super) struct MatchRecognizeShiftEvaluator {
    pub(super) cache: MaskIndexCache,
    pub(super) offset: usize,
    pub(super) default_value: ScalarValue,
    pub(super) kind: MRShiftKind,
}

impl MatchRecognizeShiftEvaluator {
    pub(super) fn new(
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

        if !self.cache.is_filtered() {
            let offset_signed = self.kind.offset_sign() * self.offset as i64;
            return shift_with_default_value(data_arr, offset_signed, &default_val);
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.build_if_needed(mask_arr);

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

        if !self.cache.is_filtered() {
            let target =
                compute_target_index(self.kind, row_index, self.offset, data_arr.len());
            return match target {
                Some(t) => ScalarValue::try_from_array(data_arr.as_ref(), t),
                None => Ok(default_val),
            };
        }

        let mask_arr = get_classifier_mask(values)?;
        self.cache.build_if_needed(mask_arr);
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
            create_default_value(data_arr.data_type())
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn is_causal(&self) -> bool {
        self.kind.is_causal()
    }
}

// Helpers -------------------------------------------------------------------

pub(super) fn get_classifier_mask(values: &[ArrayRef]) -> Result<&BooleanArray> {
    // The classifier mask is the last argument when present.
    // The caller ensures it is only invoked when the mask exists.
    values
        .last()
        .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
        .ok_or_else(|| {
            exec_datafusion_err!("classifier mask column must be a BooleanArray")
        })
}

pub(super) fn create_default_value(data_type: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(data_type)
}

pub(super) fn resolve_default(
    default: &ScalarValue,
    data_type: &DataType,
) -> Result<ScalarValue> {
    if default.is_null() {
        create_default_value(data_type)
    } else {
        // Ensure provided default matches the input data type to avoid
        // producing mixed ScalarValue variants (e.g., Int64 defaults with Int32 data),
        // which would later cause ScalarValue::iter_to_array type inconsistencies.
        default.cast_to(data_type)
    }
}

#[inline]
pub(super) fn compute_target_index(
    kind: MRShiftKind,
    base: usize,
    offset: usize,
    upper_bound: usize,
) -> Option<usize> {
    match kind {
        MRShiftKind::Prev => base.checked_sub(offset),
        MRShiftKind::Next => {
            let t = base + offset;
            (t < upper_bound).then_some(t)
        }
    }
}

// shift_with_default_value now shared via datafusion_functions_window::utils

pub(super) fn gather_array_by_indices(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
) -> Result<ArrayRef> {
    let idx_u32: Vec<Option<u32>> = indices
        .into_iter()
        .map(|opt| opt.map(|v| v as u32))
        .collect();
    let idx_array = UInt32Array::from(idx_u32);
    arrow_take(data_arr.as_ref(), &idx_array, None).map_err(|e| arrow_datafusion_err!(e))
}

pub(super) fn gather_with_default(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
    default_mask: Vec<bool>,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    let gathered = gather_array_by_indices(data_arr, indices)?;
    if default_value.is_null() {
        return Ok(gathered);
    }
    if !default_mask.iter().any(|b| *b) {
        return Ok(gathered);
    }

    let default_arr = default_value.to_array_of_size(gathered.len())?;
    let mask_array = BooleanArray::from(default_mask);
    arrow::compute::kernels::zip::zip(&mask_array, &default_arr, &gathered)
        .map_err(|e| arrow_datafusion_err!(e))
}
