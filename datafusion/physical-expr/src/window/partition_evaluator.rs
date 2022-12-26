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

use arrow::array::ArrayRef;
use datafusion_common::Result;
use datafusion_common::{DataFusionError, ScalarValue};
use std::ops::Range;

/// Partition evaluator
pub trait PartitionEvaluator {
    /// Whether the evaluator should be evaluated with rank
    fn include_rank(&self) -> bool {
        false
    }

    fn uses_window_frame(&self) -> bool {
        false
    }

    /// evaluate the partition evaluator against the partition
    fn evaluate(&self, _values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        Err(DataFusionError::NotImplemented(
            "evaluate_partition is not implemented by default".into(),
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
