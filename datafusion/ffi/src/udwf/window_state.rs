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

use arrow::array::ArrayRef;
use datafusion_common::DataFusionError;
use datafusion_expr::window_state::WindowAggState;

use crate::{arrow_wrappers::WrappedArray, udwf::range::FFI_Range};

/// Holds the state of evaluating a window function
#[repr(C)]
#[derive(Debug)]
pub struct FFI_WindowAggState {
    pub window_frame_range: FFI_Range,
    pub last_calculated_index: usize,
    pub offset_pruned_rows: usize,
    /// The accumulated output column
    pub out_col: WrappedArray,
    pub n_row_result_missing: usize,
    pub is_end: bool,
}

impl TryFrom<WindowAggState> for FFI_WindowAggState {
    type Error = DataFusionError;

    fn try_from(s: WindowAggState) -> Result<Self, Self::Error> {
        Ok(Self {
            window_frame_range: FFI_Range::from(s.window_frame_range.clone()),
            last_calculated_index: s.last_calculated_index,
            offset_pruned_rows: s.offset_pruned_rows,
            out_col: WrappedArray::try_from(&s.out_col)
                .map_err(DataFusionError::from)?,
            n_row_result_missing: s.n_row_result_missing,
            is_end: s.is_end,
        })
    }
}

impl TryFrom<FFI_WindowAggState> for WindowAggState {
    type Error = DataFusionError;

    fn try_from(s: FFI_WindowAggState) -> Result<Self, Self::Error> {
        let out_col: ArrayRef = s.out_col
            .try_into()
            .map_err(DataFusionError::from)?;

        Ok(WindowAggState {
            window_frame_range: s.window_frame_range.into(),
            window_frame_ctx: None,
            last_calculated_index: s.last_calculated_index,
            offset_pruned_rows: s.offset_pruned_rows,
            out_col,
            n_row_result_missing: s.n_row_result_missing,
            is_end: s.is_end,
        })
    }
}
