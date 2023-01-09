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

//! partition evaluation module

use crate::window::window_expr::BuiltinWindowState;
use crate::window::WindowAggState;
use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use std::fmt::Debug;
use std::ops::Range;

/// Partition evaluator
pub trait PartitionEvaluator: Debug + Send {
    /// Whether the evaluator should be evaluated with rank
    fn include_rank(&self) -> bool {
        false
    }

    /// Returns state of the Built-in Window Function
    fn state(&self) -> Result<BuiltinWindowState> {
        // If we do not use state we just return Default
        Ok(BuiltinWindowState::Default)
    }

    fn update_state(
        &mut self,
        _state: &WindowAggState,
        _range_columns: &[ArrayRef],
        _sort_partition_points: &[Range<usize>],
    ) -> Result<()> {
        // If we do not use state, update_state does nothing
        Ok(())
    }

    fn get_range(&self, _state: &WindowAggState, _n_rows: usize) -> Result<Range<usize>> {
        Err(DataFusionError::NotImplemented(
            "get_range is not implemented for this window function".to_string(),
        ))
    }

    /// evaluate the partition evaluator against the partition
    fn evaluate(&self, _values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate is not implemented by default".into(),
        ))
    }

    /// evaluate window function result inside given range
    fn evaluate_stateful(&mut self, _values: &[ArrayRef]) -> Result<ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_stateful is not implemented by default".into(),
        ))
    }

    /// evaluate the partition evaluator against the partition but with rank
    fn evaluate_with_rank(
        &self,
        _num_rows: usize,
        _ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition_with_rank is not implemented by default".into(),
        ))
    }

    /// evaluate window function result inside given range
    fn evaluate_inside_range(
        &self,
        _values: &[ArrayRef],
        _range: Range<usize>,
    ) -> Result<ScalarValue> {
        Err(DataFusionError::NotImplemented(
            "evaluate_inside_range is not implemented by default".into(),
        ))
    }
}
