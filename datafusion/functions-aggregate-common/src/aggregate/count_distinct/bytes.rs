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

//! [`BytesDistinctCountAccumulator`] for Utf8/LargeUtf8/Binary/LargeBinary values

use arrow::array::{ArrayRef, OffsetSizeTrait};
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::array_into_list_array_nullable;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::binary_map::{ArrowBytesSet, OutputType};
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewSet;
use std::fmt::Debug;
use std::sync::Arc;

/// Specialized implementation of
/// `COUNT DISTINCT` for [`StringArray`] [`LargeStringArray`],
/// [`BinaryArray`] and [`LargeBinaryArray`].
///
/// [`StringArray`]: arrow::array::StringArray
/// [`LargeStringArray`]: arrow::array::LargeStringArray
/// [`BinaryArray`]: arrow::array::BinaryArray
/// [`LargeBinaryArray`]: arrow::array::LargeBinaryArray
#[derive(Debug)]
pub struct BytesDistinctCountAccumulator<O: OffsetSizeTrait>(ArrowBytesSet<O>);

impl<O: OffsetSizeTrait> BytesDistinctCountAccumulator<O> {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesSet::new(output_type))
    }
}

impl<O: OffsetSizeTrait> Accumulator for BytesDistinctCountAccumulator<O> {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let set = self.0.take();
        let arr = set.into_state();
        let list = Arc::new(array_into_list_array_nullable(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.insert(&values[0]);

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                self.0.insert(&list);
            };
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.non_null_len() as i64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.0.size()
    }
}

/// Specialized implementation of
/// `COUNT DISTINCT` for [`StringViewArray`] and [`BinaryViewArray`].
///
/// [`StringViewArray`]: arrow::array::StringViewArray
/// [`BinaryViewArray`]: arrow::array::BinaryViewArray
#[derive(Debug)]
pub struct BytesViewDistinctCountAccumulator(ArrowBytesViewSet);

impl BytesViewDistinctCountAccumulator {
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesViewSet::new(output_type))
    }
}

impl Accumulator for BytesViewDistinctCountAccumulator {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let set = self.0.take();
        let arr = set.into_state();
        let list = Arc::new(array_into_list_array_nullable(arr));
        Ok(vec![ScalarValue::List(list)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.insert(&values[0]);

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                self.0.insert(&list);
            };
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.non_null_len() as i64)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.0.size()
    }
}
