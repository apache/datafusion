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

//! Defines physical expressions that can evaluated at runtime during query execution

use std::any::Any;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::BitAnd;
use std::sync::Arc;

use crate::aggregate::row_accumulator::RowAccumulator;
use crate::aggregate::utils::down_cast_any_ref;
use crate::{AggregateExpr, PhysicalExpr, GroupsAccumulator};
use arrow::array::{Array, Int64Array};
use arrow::compute;
use arrow::compute::kernels::cast;
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::Field};
use arrow_array::builder::PrimitiveBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{UInt64Type, Int64Type, UInt32Type, Int32Type};
use arrow_array::{PrimitiveArray, UInt64Array, ArrowNumericType};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer};
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use datafusion_row::accessor::RowAccessor;

use crate::expressions::format_state_name;

use super::groups_accumulator::accumulate::{accumulate_all, accumulate_all_nullable};

/// COUNT aggregate expression
/// Returns the amount of non-null values of the given expression.
#[derive(Debug, Clone)]
pub struct Count {
    name: String,
    data_type: DataType,
    nullable: bool,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl Count {
    /// Create a new COUNT aggregate function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            exprs: vec![expr],
            data_type,
            nullable: true,
        }
    }

    pub fn new_with_multiple_exprs(
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            exprs,
            data_type,
            nullable: true,
        }
    }
}

/// An accumulator to compute the average of PrimitiveArray<T>.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calcuates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct CountGroupsAccumulator<T>
where T: ArrowNumericType + Send,
{
    /// The type of the returned count
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// If we have seen a null input value for this group_index
    null_inputs: BooleanBufferBuilder,

    // Bind it to struct
    phantom: PhantomData<T>
}


impl<T> CountGroupsAccumulator<T>
where T: ArrowNumericType + Send,
{
    pub fn new(return_data_type: &DataType) -> Self {
        Self {
            return_data_type: return_data_type.clone(),
            counts: vec![],
            null_inputs: BooleanBufferBuilder::new(0),
            phantom: PhantomData {}
        }
    }

        /// Adds one to each group's counter
        fn increment_counts(
            &mut self,
            group_indices: &[usize],
            values: &PrimitiveArray<T>,
            opt_filter: Option<&arrow_array::BooleanArray>,
            total_num_groups: usize,
        ) {
            self.counts.resize(total_num_groups, 0);
    
            if values.null_count() == 0 {
                accumulate_all(
                    group_indices,
                    values,
                    opt_filter,
                    |group_index, _new_value| {
                        self.counts[group_index] += 1;
                    }
                )
            }else {
                accumulate_all_nullable(
                    group_indices,
                    values,
                    opt_filter,
                    |group_index, _new_value, is_valid| {
                        if is_valid {
                            self.counts[group_index] += 1;
                        }
                    },
                )
            }
        }

        /// Adds the counts with the partial counts
        fn update_counts_with_partial_counts(
            &mut self,
            group_indices: &[usize],
            partial_counts: &UInt64Array,
            opt_filter: Option<&arrow_array::BooleanArray>,
            total_num_groups: usize,
        ) {
            self.counts.resize(total_num_groups, 0);
    
            if partial_counts.null_count() == 0 {
                accumulate_all(
                    group_indices,
                    partial_counts,
                    opt_filter,
                    |group_index, partial_count| {
                        self.counts[group_index] += partial_count;
                    },
                )
            } else {
                accumulate_all_nullable(
                    group_indices,
                    partial_counts,
                    opt_filter,
                    |group_index, partial_count, is_valid| {
                        if is_valid {
                            self.counts[group_index] += partial_count;
                        }
                    },
                )
            }
        }

        /// Returns a NullBuffer representing which group_indices have
        /// null values (if they saw a null input)
        /// Resets `self.null_inputs`;
        fn build_nulls(&mut self) -> Option<NullBuffer> {
            let nulls = NullBuffer::new(self.null_inputs.finish());
            if nulls.null_count() > 0 {
                Some(nulls)
            } else {
                None
            }
        }
}

impl <T> GroupsAccumulator for CountGroupsAccumulator<T>
where T: ArrowNumericType + Send
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values.get(0).unwrap().as_primitive::<T>();

        self.increment_counts(group_indices, values, opt_filter, total_num_groups);

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values.get(0).unwrap().as_primitive::<UInt64Type>();
        self.update_counts_with_partial_counts(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
        );

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ArrayRef> {
        let counts = std::mem::take(&mut self.counts);
        let nulls = self.build_nulls();

        // don't evaluate averages with null inputs to avoid errors on null vaues
        let array: PrimitiveArray<UInt64Type> = if let Some(nulls) = nulls.as_ref() {
            let mut builder = PrimitiveBuilder::<UInt64Type>::with_capacity(nulls.len());
            let iter = counts.into_iter().zip(nulls.iter());

            for (count, is_valid) in iter {
                if is_valid {
                    builder.append_value(count)
                } else {
                    builder.append_null();
                }
            }
            builder.finish()
        } else {
            PrimitiveArray::<UInt64Type>::new(counts.into(), nulls) // no copy
        };
        // TODO remove cast
        let array = cast(&array, &self.return_data_type)?;

        Ok(array)
    }

    // return arrays for sums and counts
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        // TODO nulls
        let nulls = self.build_nulls();
        let counts = std::mem::take(&mut self.counts);
        let counts = UInt64Array::from(counts); // zero copy
        Ok(vec![
            Arc::new(counts) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<usize>()
    }
}

/// count null values for multiple columns
/// for each row if one column value is null, then null_count + 1
fn null_count_for_multiple_cols(values: &[ArrayRef]) -> usize {
    if values.len() > 1 {
        let result_bool_buf: Option<BooleanBuffer> = values
            .iter()
            .map(|a| a.nulls())
            .fold(None, |acc, b| match (acc, b) {
                (Some(acc), Some(b)) => Some(acc.bitand(b.inner())),
                (Some(acc), None) => Some(acc),
                (None, Some(b)) => Some(b.inner().clone()),
                _ => None,
            });
        result_bool_buf.map_or(0, |b| values[0].len() - b.count_set_bits())
    } else {
        values[0].null_count()
    }
}

impl AggregateExpr for Count {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "count"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        true
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(CountRowAccumulator::new(start_index)))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountAccumulator::new()))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        match &self.data_type {
            DataType::UInt64 => {
                Ok(Box::new(CountGroupsAccumulator::<UInt64Type>::new(
                    &self.data_type,
                )))
            },
                DataType::Int64 => {
                Ok(Box::new(CountGroupsAccumulator::<Int64Type>::new(
                    &self.data_type,
                )))
            },
                DataType::UInt32 => {
                Ok(Box::new(CountGroupsAccumulator::<UInt32Type>::new(
                    &self.data_type,
                )))
            },
                DataType::Int32 => {
                Ok(Box::new(CountGroupsAccumulator::<Int32Type>::new(
                    &self.data_type,
                )))
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "CountGroupsAccumulator not supported for {}",
                self.data_type
            ))),
        }

    }
}

impl PartialEq<dyn Any> for Count {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.exprs.len() == x.exprs.len()
                    && self
                        .exprs
                        .iter()
                        .zip(x.exprs.iter())
                        .all(|(expr1, expr2)| expr1.eq(expr2))
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count -= (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], Int64Array);
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[derive(Debug)]
struct CountRowAccumulator {
    state_index: usize,
}

impl CountRowAccumulator {
    pub fn new(index: usize) -> Self {
        Self { state_index: index }
    }
}

impl RowAccumulator for CountRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let array = &values[0];
        let delta = (array.len() - null_count_for_multiple_cols(values)) as u64;
        accessor.add_u64(self.state_index, delta);
        Ok(())
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        if !values.iter().any(|s| matches!(s, ScalarValue::Null)) {
            accessor.add_u64(self.state_index, 1)
        }
        Ok(())
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        match value {
            ScalarValue::Null => {
                // do not update the accumulator
            }
            _ => accessor.add_u64(self.state_index, 1),
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let counts = downcast_value!(states[0], Int64Array);
        let delta = &compute::sum(counts);
        if let Some(d) = delta {
            accessor.add_i64(self.state_index, *d);
        }
        Ok(())
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(
            accessor.get_u64_opt(self.state_index()).unwrap_or(0) as i64,
        )))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.state_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::tests::aggregate;
    use crate::expressions::{col, lit};
    use crate::generic_test_op;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn count_elements() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Count, ScalarValue::from(5i64))
    }

    #[test]
    fn count_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ]));
        generic_test_op!(a, DataType::Int32, Count, ScalarValue::from(3i64))
    }

    #[test]
    fn count_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        generic_test_op!(a, DataType::Boolean, Count, ScalarValue::from(0i64))
    }

    #[test]
    fn count_empty() -> Result<()> {
        let a: Vec<bool> = vec![];
        let a: ArrayRef = Arc::new(BooleanArray::from(a));
        generic_test_op!(a, DataType::Boolean, Count, ScalarValue::from(0i64))
    }

    #[test]
    fn count_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(StringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(a, DataType::Utf8, Count, ScalarValue::from(5i64))
    }

    #[test]
    fn count_large_utf8() -> Result<()> {
        let a: ArrayRef =
            Arc::new(LargeStringArray::from(vec!["a", "bb", "ccc", "dddd", "ad"]));
        generic_test_op!(a, DataType::LargeUtf8, Count, ScalarValue::from(5i64))
    }

    #[test]
    fn count_multi_cols() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            None,
        ]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
            Some(4),
        ]));
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![a, b])?;

        let agg = Arc::new(Count::new_with_multiple_exprs(
            vec![col("a", &schema)?, col("b", &schema)?],
            "bla".to_string(),
            DataType::Int64,
        ));
        let actual = aggregate(&batch, agg)?;
        let expected = ScalarValue::from(2i64);

        assert_eq!(expected, actual);
        Ok(())
    }

    #[test]
    fn count_eq() -> Result<()> {
        let count = Count::new(lit(1i8), "COUNT(1)".to_string(), DataType::Int64);
        let arc_count: Arc<dyn AggregateExpr> = Arc::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        ));
        let box_count: Box<dyn AggregateExpr> = Box::new(Count::new(
            lit(1i8),
            "COUNT(1)".to_string(),
            DataType::Int64,
        ));
        let count2 = Count::new(lit(1i8), "COUNT(2)".to_string(), DataType::Int64);

        assert!(arc_count.eq(&box_count));
        assert!(box_count.eq(&arc_count));
        assert!(arc_count.eq(&count));
        assert!(count.eq(&box_count));
        assert!(count.eq(&arc_count));

        assert!(count2.ne(&arc_count));

        Ok(())
    }
}
