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

use crate::aggregates::group_values::multi_group_by::GroupColumn;

use arrow::array::Int32Array;
use std::sync::Arc;
pub struct DictionaryGroupValuesColumn {
    inner: Box<dyn GroupColumn>,
}
impl DictionaryGroupValuesColumn {
    pub fn new(inner: Box<dyn GroupColumn>) -> Self {
        Self { inner }
    }
}

impl GroupColumn for DictionaryGroupValuesColumn {
    fn equal_to(
        &self,
        lhs_row: usize,
        array: &arrow::array::ArrayRef,
        rhs_row: usize,
    ) -> bool {
        false
    }
    fn append_val(
        &mut self,
        array: &arrow::array::ArrayRef,
        row: usize,
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &arrow::array::ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut arrow::array::BooleanBufferBuilder,
    ) {
    }
    fn vectorized_append(
        &mut self,
        array: &arrow::array::ArrayRef,
        rows: &[usize],
    ) -> datafusion_common::Result<()> {
        Ok(())
    }
    fn len(&self) -> usize {
        0
    }
    fn size(&self) -> usize {
        0
    }
    fn build(self: Box<Self>) -> arrow::array::ArrayRef {
        Arc::new(Int32Array::from(vec![12, 3, 3]))
    }
    fn take_n(&mut self, n: usize) -> arrow::array::ArrayRef {
        Arc::new(Int32Array::from(vec![12, 3, 3]))
    }
}
