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

use crate::expressions::format_state_name;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::aggregate::utils::down_cast_any_ref;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

/// Expression for a SUM(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctSum {
    /// Column name
    name: String,
    /// The DataType for the final sum
    data_type: DataType,
    /// The input arguments, only contains 1 item for sum
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl DistinctSum {
    /// Create a SUM(DISTINCT) aggregate function.
    pub fn new(
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: String,
        data_type: DataType,
    ) -> Self {
        Self {
            name,
            data_type,
            exprs,
        }
    }
}

impl AggregateExpr for DistinctSum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        // State field is a List which stores items to rebuild hash set.
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "sum distinct"),
            Field::new("item", self.data_type.clone(), true),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctSumAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for DistinctSum {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.exprs.len() == x.exprs.len()
                    && self
                        .exprs
                        .iter()
                        .zip(x.exprs.iter())
                        .all(|(this, other)| this.eq(other))
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct DistinctSumAccumulator {
    hash_values: HashSet<ScalarValue, RandomState>,
    data_type: DataType,
}
impl DistinctSumAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            hash_values: HashSet::default(),
            data_type: data_type.clone(),
        })
    }
}

impl Accumulator for DistinctSumAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let mut distinct_values = Vec::new();
            self.hash_values
                .iter()
                .for_each(|distinct_value| distinct_values.push(distinct_value.clone()));
            vec![ScalarValue::new_list(
                Some(distinct_values),
                self.data_type.clone(),
            )]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        (0..values[0].len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let v = ScalarValue::try_from_array(arr, index)?;
                self.hash_values.insert(v);
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let scalar = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::List(Some(scalar), _) = scalar {
                scalar.iter().for_each(|scalar| {
                    if !ScalarValue::is_null(scalar) {
                        self.hash_values.insert(scalar.clone());
                    }
                });
            } else {
                return Err(DataFusionError::Internal(
                    "Unexpected accumulator state".into(),
                ));
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let mut sum_value = ScalarValue::try_from(&self.data_type)?;
        for distinct_value in self.hash_values.iter() {
            sum_value = sum_value.add(distinct_value)?;
        }
        Ok(sum_value)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_hashset(&self.hash_values)
            - std::mem::size_of_val(&self.hash_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    fn run_update_batch(
        return_type: DataType,
        arrays: &[ArrayRef],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctSum::new(vec![], String::from("__col_name__"), return_type);

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    macro_rules! generic_test_sum_distinct {
        ($ARRAY:expr, $DATATYPE:expr, $EXPECTED:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(DistinctSum::new(
                vec![col("a", &schema)?],
                "count_distinct_a".to_string(),
                $EXPECTED.get_datatype(),
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(())
        }};
    }

    #[test]
    fn sum_distinct_update_batch() -> Result<()> {
        let array_int64: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 3]));
        let arrays = vec![array_int64];
        let (states, result) = run_update_batch(DataType::Int64, &arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(4)));

        Ok(())
    }

    #[test]
    fn sum_distinct_i32_with_nulls() -> Result<()> {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(2),
            Some(2),
            Some(3),
        ]));
        generic_test_sum_distinct!(array, DataType::Int32, ScalarValue::from(6_i32))
    }

    #[test]
    fn sum_distinct_u32_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(UInt32Array::from(vec![
            Some(1_u32),
            Some(1_u32),
            Some(3_u32),
            Some(3_u32),
            None,
        ]));
        generic_test_sum_distinct!(array, DataType::UInt32, ScalarValue::from(4_u32))
    }

    #[test]
    fn sum_distinct_f64() -> Result<()> {
        let array: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 1_f64, 3_f64, 3_f64, 3_f64]));
        generic_test_sum_distinct!(array, DataType::Float64, ScalarValue::from(4_f64))
    }

    #[test]
    fn sum_distinct_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i % 2) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(35, 0)?,
        );
        generic_test_sum_distinct!(
            array,
            DataType::Decimal128(35, 0),
            ScalarValue::Decimal128(Some(1), 38, 0)
        )
    }
}
