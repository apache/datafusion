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

use crate::utils::is_valid_decimal_precision;
use crate::{arithmetic_overflow_error, EvalMode};
use arrow::array::{
    cast::AsArray, types::Decimal128Type, Array, ArrayRef, BooleanArray, Decimal128Array,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::Volatility::Immutable;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, EmitTo, GroupsAccumulator, ReversedUDAF, Signature,
};
use std::{any::Any, sync::Arc};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SumDecimal {
    /// Aggregate function signature
    signature: Signature,
    /// The data type of the SUM result. This will always be a decimal type
    /// with the same precision and scale as specified in this struct
    result_type: DataType,
    /// Decimal precision
    precision: u8,
    /// Decimal scale
    scale: i8,
    eval_mode: EvalMode,
}

impl SumDecimal {
    pub fn try_new(data_type: DataType, eval_mode: EvalMode) -> DFResult<Self> {
        let (precision, scale) = match data_type {
            DataType::Decimal128(p, s) => (p, s),
            _ => {
                return Err(DataFusionError::Internal(
                    "Invalid data type for SumDecimal".into(),
                ))
            }
        };
        Ok(Self {
            signature: Signature::user_defined(Immutable),
            result_type: data_type,
            precision,
            scale,
            eval_mode,
        })
    }
}

impl AggregateUDFImpl for SumDecimal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumDecimalAccumulator::new(
            self.precision,
            self.scale,
            self.eval_mode,
        )))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        // For decimal sum, we always track is_empty regardless of eval_mode
        // This matches Spark's behavior where DecimalType always uses shouldTrackIsEmpty = true
        let data_type = self.result_type.clone();
        Ok(vec![
            Arc::new(Field::new("sum", data_type, true)),
            Arc::new(Field::new("is_empty", DataType::Boolean, false)),
        ])
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.result_type.clone())
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumDecimalGroupsAccumulator::new(
            self.result_type.clone(),
            self.precision,
            self.eval_mode,
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> DFResult<ScalarValue> {
        ScalarValue::new_primitive::<Decimal128Type>(
            None,
            &DataType::Decimal128(self.precision, self.scale),
        )
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn is_nullable(&self) -> bool {
        // SumDecimal is always nullable because overflows can cause null values
        true
    }
}

#[derive(Debug)]
struct SumDecimalAccumulator {
    sum: Option<i128>,
    is_empty: bool,
    precision: u8,
    scale: i8,
    eval_mode: EvalMode,
}

impl SumDecimalAccumulator {
    fn new(precision: u8, scale: i8, eval_mode: EvalMode) -> Self {
        // For decimal sum, always track is_empty regardless of eval_mode
        // This matches Spark's behavior where DecimalType always uses shouldTrackIsEmpty = true
        Self {
            sum: Some(0),
            is_empty: true,
            precision,
            scale,
            eval_mode,
        }
    }

    fn update_single(&mut self, values: &Decimal128Array, idx: usize) -> DFResult<()> {
        // If already overflowed (sum is None but not empty), stay in overflow state
        if !self.is_empty && self.sum.is_none() {
            return Ok(());
        }

        let v = unsafe { values.value_unchecked(idx) };
        let running_sum = self.sum.unwrap_or(0);
        let (new_sum, is_overflow) = running_sum.overflowing_add(v);

        if is_overflow || !is_valid_decimal_precision(new_sum, self.precision) {
            if self.eval_mode == EvalMode::Ansi {
                return Err(DataFusionError::from(arithmetic_overflow_error("decimal")));
            }
            self.sum = None;
            self.is_empty = false;
            return Ok(());
        }

        self.sum = Some(new_sum);
        self.is_empty = false;
        Ok(())
    }
}

impl Accumulator for SumDecimalAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        assert_eq!(
            values.len(),
            1,
            "Expect only one element in 'values' but found {}",
            values.len()
        );

        // For decimal sum, always check for overflow regardless of eval_mode (per Spark's expectation)
        if !self.is_empty && self.sum.is_none() {
            return Ok(());
        }

        let values = &values[0];
        let data = values.as_primitive::<Decimal128Type>();

        // Update is_empty: it remains true only if it was true AND all values are null
        self.is_empty = self.is_empty && values.len() == values.null_count();

        if self.is_empty {
            return Ok(());
        }

        for i in 0..data.len() {
            if data.is_null(i) {
                continue;
            }
            self.update_single(data, i)?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.is_empty {
            ScalarValue::new_primitive::<Decimal128Type>(
                None,
                &DataType::Decimal128(self.precision, self.scale),
            )
        } else {
            match self.sum {
                Some(sum_value) if is_valid_decimal_precision(sum_value, self.precision) => {
                    ScalarValue::try_new_decimal128(sum_value, self.precision, self.scale)
                }
                _ => ScalarValue::new_primitive::<Decimal128Type>(
                    None,
                    &DataType::Decimal128(self.precision, self.scale),
                ),
            }
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = match self.sum {
            Some(sum_value) => {
                ScalarValue::try_new_decimal128(sum_value, self.precision, self.scale)?
            }
            None => ScalarValue::new_primitive::<Decimal128Type>(
                None,
                &DataType::Decimal128(self.precision, self.scale),
            )?,
        };

        // For decimal sum, always return 2 state values regardless of eval_mode
        Ok(vec![sum, ScalarValue::from(self.is_empty)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // For decimal sum, always expect 2 state arrays regardless of eval_mode
        assert_eq!(
            states.len(),
            2,
            "Expect two elements in 'states' but found {}",
            states.len()
        );
        assert_eq!(states[0].len(), 1);
        assert_eq!(states[1].len(), 1);

        let that_sum_array = states[0].as_primitive::<Decimal128Type>();
        let that_sum = if that_sum_array.is_null(0) {
            None
        } else {
            Some(that_sum_array.value(0))
        };

        let that_is_empty = states[1].as_boolean().value(0);
        let that_overflowed = !that_is_empty && that_sum.is_none();
        let this_overflowed = !self.is_empty && self.sum.is_none();

        if that_overflowed || this_overflowed {
            self.sum = None;
            self.is_empty = false;
            return Ok(());
        }

        if that_is_empty {
            return Ok(());
        }

        if self.is_empty {
            self.sum = that_sum;
            self.is_empty = false;
            return Ok(());
        }

        let left = self.sum.unwrap();
        let right = that_sum.unwrap();
        let (new_sum, is_overflow) = left.overflowing_add(right);

        if is_overflow || !is_valid_decimal_precision(new_sum, self.precision) {
            if self.eval_mode == EvalMode::Ansi {
                return Err(DataFusionError::from(arithmetic_overflow_error("decimal")));
            } else {
                self.sum = None;
                self.is_empty = false;
            }
        } else {
            self.sum = Some(new_sum);
        }

        Ok(())
    }
}

struct SumDecimalGroupsAccumulator {
    sum: Vec<Option<i128>>,
    is_empty: Vec<bool>,
    result_type: DataType,
    precision: u8,
    eval_mode: EvalMode,
}

impl SumDecimalGroupsAccumulator {
    fn new(result_type: DataType, precision: u8, eval_mode: EvalMode) -> Self {
        Self {
            sum: Vec::new(),
            is_empty: Vec::new(),
            result_type,
            precision,
            eval_mode,
        }
    }

    fn resize_helper(&mut self, total_num_groups: usize) {
        // For decimal sum, always initialize properly regardless of eval_mode
        self.sum.resize(total_num_groups, Some(0));
        self.is_empty.resize(total_num_groups, true);
    }

    #[inline]
    fn update_single(&mut self, group_index: usize, value: i128) -> DFResult<()> {
        // For decimal sum, always check for overflow regardless of eval_mode
        if !self.is_empty[group_index] && self.sum[group_index].is_none() {
            return Ok(());
        }

        let running_sum = self.sum[group_index].unwrap_or(0);
        let (new_sum, is_overflow) = running_sum.overflowing_add(value);

        if is_overflow || !is_valid_decimal_precision(new_sum, self.precision) {
            if self.eval_mode == EvalMode::Ansi {
                return Err(DataFusionError::from(arithmetic_overflow_error("decimal")));
            }
            self.sum[group_index] = None;
        } else {
            self.sum[group_index] = Some(new_sum);
        }
        self.is_empty[group_index] = false;
        Ok(())
    }
}

impl GroupsAccumulator for SumDecimalGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        assert_eq!(values.len(), 1);
        let values = values[0].as_primitive::<Decimal128Type>();
        let data = values.values();

        self.resize_helper(total_num_groups);

        let iter = group_indices.iter().zip(data.iter());
        if values.null_count() == 0 {
            for (&group_index, &value) in iter {
                self.update_single(group_index, value)?;
            }
        } else {
            for (idx, (&group_index, &value)) in iter.enumerate() {
                if values.is_null(idx) {
                    continue;
                }
                self.update_single(group_index, value)?;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                let result =
                    Decimal128Array::from_iter(self.sum.iter().zip(self.is_empty.iter()).map(
                        |(&sum, &empty)| {
                            if empty {
                                None
                            } else {
                                match sum {
                                    Some(v) if is_valid_decimal_precision(v, self.precision) => {
                                        Some(v)
                                    }
                                    _ => None,
                                }
                            }
                        },
                    ))
                        .with_data_type(self.result_type.clone());

                self.sum.clear();
                self.is_empty.clear();
                Ok(Arc::new(result))
            }
            EmitTo::First(n) => {
                let result = Decimal128Array::from_iter(
                    self.sum
                        .drain(..n)
                        .zip(self.is_empty.drain(..n))
                        .map(|(sum, empty)| {
                            if empty {
                                None
                            } else {
                                match sum {
                                    Some(v) if is_valid_decimal_precision(v, self.precision) => {
                                        Some(v)
                                    }
                                    _ => None,
                                }
                            }
                        }),
                )
                    .with_data_type(self.result_type.clone());

                Ok(Arc::new(result))
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sum);

        let sum_array = Decimal128Array::from_iter(sums.iter().copied())
            .with_data_type(self.result_type.clone());

        // For decimal sum, always return 2 state arrays regardless of eval_mode
        let is_empty = emit_to.take_needed(&mut self.is_empty);
        Ok(vec![
            Arc::new(sum_array),
            Arc::new(BooleanArray::from(is_empty)),
        ])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        assert!(opt_filter.is_none(), "opt_filter is not supported yet");

        self.resize_helper(total_num_groups);

        // For decimal sum, always expect 2 arrays regardless of eval_mode
        assert_eq!(
            values.len(),
            2,
            "Expected two arrays: 'sum' and 'is_empty', but found {}",
            values.len()
        );

        let that_sum = values[0].as_primitive::<Decimal128Type>();
        let that_is_empty = values[1].as_boolean();

        for (idx, &group_index) in group_indices.iter().enumerate() {
            let that_sum_val = if that_sum.is_null(idx) {
                None
            } else {
                Some(that_sum.value(idx))
            };

            let that_is_empty_val = that_is_empty.value(idx);
            let that_overflowed = !that_is_empty_val && that_sum_val.is_none();
            let this_overflowed = !self.is_empty[group_index] && self.sum[group_index].is_none();

            if that_overflowed || this_overflowed {
                self.sum[group_index] = None;
                self.is_empty[group_index] = false;
                continue;
            }

            if that_is_empty_val {
                continue;
            }

            if self.is_empty[group_index] {
                self.sum[group_index] = that_sum_val;
                self.is_empty[group_index] = false;
                continue;
            }

            let left = self.sum[group_index].unwrap();
            let right = that_sum_val.unwrap();
            let (new_sum, is_overflow) = left.overflowing_add(right);

            if is_overflow || !is_valid_decimal_precision(new_sum, self.precision) {
                if self.eval_mode == EvalMode::Ansi {
                    return Err(DataFusionError::from(arithmetic_overflow_error("decimal")));
                } else {
                    self.sum[group_index] = None;
                    self.is_empty[group_index] = false;
                }
            } else {
                self.sum[group_index] = Some(new_sum);
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.sum.capacity() * std::mem::size_of::<Option<i128>>()
            + self.is_empty.capacity() * std::mem::size_of::<bool>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::builder::{Decimal128Builder, StringBuilder};
    use arrow::array::RecordBatch;
    use arrow::datatypes::*;
    use datafusion::common::Result;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::AggregateUDF;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;

    #[test]
    fn invalid_data_type() {
        assert!(SumDecimal::try_new(DataType::Int32, EvalMode::Legacy).is_err());
    }

    #[tokio::test]
    async fn sum_no_overflow() -> Result<()> {
        let num_rows = 8192;
        let batch = create_record_batch(num_rows);
        let mut batches = Vec::new();
        for _ in 0..10 {
            batches.push(batch.clone());
        }
        let partitions = &[batches];
        let c0: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c0", 0));
        let c1: Arc<dyn PhysicalExpr> = Arc::new(Column::new("c1", 1));

        let data_type = DataType::Decimal128(8, 2);
        let schema = Arc::clone(&partitions[0][0].schema());
        let scan: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(partitions, Arc::clone(&schema), None).unwrap(),
        )));

        let aggregate_udf = Arc::new(AggregateUDF::new_from_impl(SumDecimal::try_new(
            data_type.clone(),
            EvalMode::Legacy,
        )?));

        let aggr_expr = AggregateExprBuilder::new(aggregate_udf, vec![c1])
            .schema(Arc::clone(&schema))
            .alias("sum")
            .with_ignore_nulls(false)
            .with_distinct(false)
            .build()?;

        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(c0, "c0".to_string())]),
            vec![aggr_expr.into()],
            vec![None], // no filter expressions
            scan,
            Arc::clone(&schema),
        )?);

        let mut stream = aggregate
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        while let Some(batch) = stream.next().await {
            let _batch = batch?;
        }

        Ok(())
    }

    fn create_record_batch(num_rows: usize) -> RecordBatch {
        let mut decimal_builder = Decimal128Builder::with_capacity(num_rows);
        let mut string_builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for i in 0..num_rows {
            decimal_builder.append_value(i as i128);
            string_builder.append_value(format!("this is string #{}", i % 1024));
        }
        let decimal_array = Arc::new(decimal_builder.finish());
        let string_array = Arc::new(string_builder.finish());

        let mut fields = vec![];
        let mut columns: Vec<ArrayRef> = vec![];

        // string column
        fields.push(Field::new("c0", DataType::Utf8, false));
        columns.push(string_array);

        // decimal column
        fields.push(Field::new("c1", DataType::Decimal128(38, 10), false));
        columns.push(decimal_array);

        let schema = Schema::new(fields);
        RecordBatch::try_new(Arc::new(schema), columns).unwrap()
    }
}
