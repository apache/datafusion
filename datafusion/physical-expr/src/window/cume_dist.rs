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

//! Defines physical expression for `cume_dist` that can evaluated
//! at runtime during query execution

use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field};
use datafusion_common::Result;
use datafusion_expr::PartitionEvaluator;
use std::any::Any;
use std::iter;
use std::ops::Range;
use std::sync::Arc;

/// CumeDist calculates the cume_dist in the window function with order by
#[derive(Debug)]
pub struct CumeDist {
    name: String,
    data_type: DataType,
}

/// Create a cume_dist window function
pub fn cume_dist(name: String, data_type: &DataType) -> CumeDist {
    CumeDist {
        name,
        data_type: data_type.clone(),
    }
}

impl BuiltInWindowFunctionExpr for CumeDist {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        Ok(Field::new(self.name(), self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(CumeDistEvaluator {}))
    }
}

#[derive(Debug)]
pub(crate) struct CumeDistEvaluator;

impl PartitionEvaluator for CumeDistEvaluator {
    fn evaluate_all_with_rank(
        &self,
        num_rows: usize,
        ranks_in_partition: &[Range<usize>],
    ) -> Result<ArrayRef> {
        let scalar = num_rows as f64;
        let result = Float64Array::from_iter_values(
            ranks_in_partition
                .iter()
                .scan(0_u64, |acc, range| {
                    let len = range.end - range.start;
                    *acc += len as u64;
                    let value: f64 = (*acc as f64) / scalar;
                    let result = iter::repeat(value).take(len);
                    Some(result)
                })
                .flatten(),
        );
        Ok(Arc::new(result))
    }

    fn include_rank(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::cast::as_float64_array;

    fn test_i32_result(
        expr: &CumeDist,
        num_rows: usize,
        ranks: Vec<Range<usize>>,
        expected: Vec<f64>,
    ) -> Result<()> {
        let result = expr
            .create_evaluator()?
            .evaluate_all_with_rank(num_rows, &ranks)?;
        let result = as_float64_array(&result)?;
        let result = result.values();
        assert_eq!(expected, *result);
        Ok(())
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_cume_dist() -> Result<()> {
        let r = cume_dist("arr".into(), &DataType::Float64);

        let expected = vec![0.0; 0];
        test_i32_result(&r, 0, vec![], expected)?;

        let expected = vec![1.0; 1];
        test_i32_result(&r, 1, vec![0..1], expected)?;

        let expected = vec![1.0; 2];
        test_i32_result(&r, 2, vec![0..2], expected)?;

        let expected = vec![0.5, 0.5, 1.0, 1.0];
        test_i32_result(&r, 4, vec![0..2, 2..4], expected)?;

        let expected = vec![0.25, 0.5, 0.75, 1.0];
        test_i32_result(&r, 4, vec![0..1, 1..2, 2..3, 3..4], expected)?;

        Ok(())
    }
}
