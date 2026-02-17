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

use arrow::array::{
    as_primitive_array, cast::AsArray, Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType,
    BooleanArray, Int64Array, PrimitiveArray,
};
use arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int16Type, Int32Type, Int64Type, Int8Type,
};
use std::{any::Any, sync::Arc};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::{Accumulator, AggregateUDFImpl, GroupsAccumulator, ReversedUDAF, Signature};
use datafusion_expr::function::AccumulatorArgs;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SumInteger {
    signature: Signature,
    eval_mode: EvalMode,
}

impl SumInteger {
    pub fn try_new(data_type: DataType, eval_mode: EvalMode) -> DFResult<Self> {
        match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Self {
                signature: Signature::user_defined(Immutable),
                eval_mode,
            }),
            _ => Err(DataFusionError::Internal(
                "Invalid data type for SumInteger".into(),
            )),
        }
    }
}

impl AggregateUDFImpl for SumInteger {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumIntegerAccumulator::new(self.eval_mode)))
    }

    fn state_fields(&self, _args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                Arc::new(Field::new("sum", DataType::Int64, true)),
                Arc::new(Field::new("has_all_nulls", DataType::Boolean, false)),
            ])
        } else {
            Ok(vec![Arc::new(Field::new("sum", DataType::Int64, true))])
        }
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumIntGroupsAccumulator::new(self.eval_mode)))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }
}

#[derive(Debug)]
struct SumIntegerAccumulator {
    sum: Option<i64>,
    eval_mode: EvalMode,
    has_all_nulls: bool,
}

impl SumIntegerAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        if eval_mode == EvalMode::Try {
            Self {
                // Try mode starts with 0 (because if this is init to None we cant say if it is none due to all nulls or due to an overflow)
                sum: Some(0),
                has_all_nulls: true,
                eval_mode,
            }
        } else {
            Self {
                sum: None,
                has_all_nulls: false,
                eval_mode,
            }
        }
    }
}

impl Accumulator for SumIntegerAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        // accumulator internal to add sum and return null sum (and has_nulls false) if there is an overflow in Try Eval mode
        fn update_sum_internal<T>(
            int_array: &PrimitiveArray<T>,
            eval_mode: EvalMode,
            mut sum: i64,
        ) -> Result<Option<i64>, DataFusionError>
        where
            T: ArrowPrimitiveType,
        {
            for i in 0..int_array.len() {
                if !int_array.is_null(i) {
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to convert value {:?} to i64",
                            int_array.value(i)
                        ))
                    })?;
                    match eval_mode {
                        EvalMode::Legacy => {
                            sum = v.add_wrapping(sum);
                        }
                        EvalMode::Ansi | EvalMode::Try => {
                            match v.add_checked(sum) {
                                Ok(v) => sum = v,
                                Err(_e) => {
                                    return if eval_mode == EvalMode::Ansi {
                                        Err(DataFusionError::from(arithmetic_overflow_error(
                                            "integer",
                                        )))
                                    } else {
                                        Ok(None)
                                    };
                                }
                            };
                        }
                    }
                }
            }
            Ok(Some(sum))
        }

        if self.eval_mode == EvalMode::Try && !self.has_all_nulls && self.sum.is_none() {
            // we saw an overflow earlier (Try eval mode). Skip processing
            return Ok(());
        }
        let values = &values[0];
        if values.len() == values.null_count() {
            Ok(())
        } else {
            // No nulls so there should be a non-null sum / null incase overflow in Try eval
            let running_sum = self.sum.unwrap_or(0);
            let sum = match values.data_type() {
                DataType::Int64 => update_sum_internal(
                    as_primitive_array::<Int64Type>(values),
                    self.eval_mode,
                    running_sum,
                )?,
                DataType::Int32 => update_sum_internal(
                    as_primitive_array::<Int32Type>(values),
                    self.eval_mode,
                    running_sum,
                )?,
                DataType::Int16 => update_sum_internal(
                    as_primitive_array::<Int16Type>(values),
                    self.eval_mode,
                    running_sum,
                )?,
                DataType::Int8 => update_sum_internal(
                    as_primitive_array::<Int8Type>(values),
                    self.eval_mode,
                    running_sum,
                )?,
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "unsupported data type: {:?}",
                        values.data_type()
                    )));
                }
            };
            self.sum = sum;
            self.has_all_nulls = false;
            Ok(())
        }
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.has_all_nulls {
            Ok(ScalarValue::Int64(None))
        } else {
            Ok(ScalarValue::Int64(self.sum))
        }
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        if self.eval_mode == EvalMode::Try {
            Ok(vec![
                ScalarValue::Int64(self.sum),
                ScalarValue::Boolean(Some(self.has_all_nulls)),
            ])
        } else {
            Ok(vec![ScalarValue::Int64(self.sum)])
        }
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let expected_state_len = if self.eval_mode == EvalMode::Try {
            2
        } else {
            1
        };
        if expected_state_len != states.len() {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected {} elements but found {}",
                expected_state_len,
                states.len()
            )));
        }

        let that_sum_array = states[0].as_primitive::<Int64Type>();
        let that_sum = if that_sum_array.is_null(0) {
            None
        } else {
            Some(that_sum_array.value(0))
        };

        // Check for overflow for early termination
        if self.eval_mode == EvalMode::Try {
            let that_has_all_nulls = states[1].as_boolean().value(0);
            let that_overflowed = !that_has_all_nulls && that_sum.is_none();
            let this_overflowed = !self.has_all_nulls && self.sum.is_none();
            if that_overflowed || this_overflowed {
                self.sum = None;
                self.has_all_nulls = false;
                return Ok(());
            }
            if that_has_all_nulls {
                return Ok(());
            }
            if self.has_all_nulls {
                self.sum = that_sum;
                self.has_all_nulls = false;
                return Ok(());
            }
        } else {
            if that_sum.is_none() {
                return Ok(());
            }
            if self.sum.is_none() {
                self.sum = that_sum;
                return Ok(());
            }
        }

        // safe to unwrap (since we checked nulls above) but handling error just in case state is corrupt
        let left = self.sum.ok_or_else(|| {
            DataFusionError::Internal(
                "Invalid state in merging batch. Current batch's sum is None".to_string(),
            )
        })?;
        let right = that_sum.ok_or_else(|| {
            DataFusionError::Internal(
                "Invalid state in merging batch. Incoming sum is None".to_string(),
            )
        })?;

        match self.eval_mode {
            EvalMode::Legacy => {
                self.sum = Some(left.add_wrapping(right));
            }
            EvalMode::Ansi | EvalMode::Try => match left.add_checked(right) {
                Ok(v) => self.sum = Some(v),
                Err(_) => {
                    if self.eval_mode == EvalMode::Ansi {
                        return Err(DataFusionError::from(arithmetic_overflow_error("integer")));
                    } else {
                        self.sum = None;
                        self.has_all_nulls = false;
                    }
                }
            },
        }
        Ok(())
    }
}

struct SumIntGroupsAccumulator {
    sums: Vec<Option<i64>>,
    has_all_nulls: Vec<bool>,
    eval_mode: EvalMode,
}

impl SumIntGroupsAccumulator {
    fn new(eval_mode: EvalMode) -> Self {
        Self {
            sums: Vec::new(),
            eval_mode,
            has_all_nulls: Vec::new(),
        }
    }

    fn resize_helper(&mut self, total_num_groups: usize) {
        if self.eval_mode == EvalMode::Try {
            self.sums.resize(total_num_groups, Some(0));
            self.has_all_nulls.resize(total_num_groups, true);
        } else {
            self.sums.resize(total_num_groups, None);
            self.has_all_nulls.resize(total_num_groups, false);
        }
    }
}

impl GroupsAccumulator for SumIntGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        fn update_groups_sum_internal<T>(
            int_array: &PrimitiveArray<T>,
            group_indices: &[usize],
            sums: &mut [Option<i64>],
            has_all_nulls: &mut [bool],
            eval_mode: EvalMode,
        ) -> DFResult<()>
        where
            T: ArrowPrimitiveType,
            T::Native: ArrowNativeType,
        {
            for (i, &group_index) in group_indices.iter().enumerate() {
                if !int_array.is_null(i) {
                    // there is an overflow in prev group in try eval. Skip processing
                    if eval_mode == EvalMode::Try
                        && !has_all_nulls[group_index]
                        && sums[group_index].is_none()
                    {
                        continue;
                    }
                    let v = int_array.value(i).to_i64().ok_or_else(|| {
                        DataFusionError::Internal("Failed to convert value to i64".to_string())
                    })?;
                    match eval_mode {
                        EvalMode::Legacy => {
                            sums[group_index] =
                                Some(sums[group_index].unwrap_or(0).add_wrapping(v));
                        }
                        EvalMode::Ansi | EvalMode::Try => {
                            match sums[group_index].unwrap_or(0).add_checked(v) {
                                Ok(new_sum) => {
                                    sums[group_index] = Some(new_sum);
                                }
                                Err(_) => {
                                    if eval_mode == EvalMode::Ansi {
                                        return Err(DataFusionError::from(
                                            arithmetic_overflow_error("integer"),
                                        ));
                                    } else {
                                        sums[group_index] = None;
                                    }
                                }
                            };
                        }
                    }
                    has_all_nulls[group_index] = false
                }
            }
            Ok(())
        }

        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");
        let values = &values[0];
        self.resize_helper(total_num_groups);

        match values.data_type() {
            DataType::Int64 => update_groups_sum_internal(
                as_primitive_array::<Int64Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                self.eval_mode,
            )?,
            DataType::Int32 => update_groups_sum_internal(
                as_primitive_array::<Int32Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                self.eval_mode,
            )?,
            DataType::Int16 => update_groups_sum_internal(
                as_primitive_array::<Int16Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                self.eval_mode,
            )?,
            DataType::Int8 => update_groups_sum_internal(
                as_primitive_array::<Int8Type>(values),
                group_indices,
                &mut self.sums,
                &mut self.has_all_nulls,
                self.eval_mode,
            )?,
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported data type for SumIntGroupsAccumulator: {:?}",
                    values.data_type()
                )))
            }
        };
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        match emit_to {
            EmitTo::All => {
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .iter()
                        .zip(self.has_all_nulls.iter())
                        .map(|(&sum, &is_null)| if is_null { None } else { sum }),
                )) as ArrayRef;

                self.sums.clear();
                self.has_all_nulls.clear();
                Ok(result)
            }
            EmitTo::First(n) => {
                let result = Arc::new(Int64Array::from_iter(
                    self.sums
                        .drain(..n)
                        .zip(self.has_all_nulls.drain(..n))
                        .map(|(sum, is_null)| if is_null { None } else { sum }),
                )) as ArrayRef;
                Ok(result)
            }
        }
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);

        if self.eval_mode == EvalMode::Try {
            let has_all_nulls = emit_to.take_needed(&mut self.has_all_nulls);
            Ok(vec![
                Arc::new(Int64Array::from(sums)),
                Arc::new(BooleanArray::from(has_all_nulls)),
            ])
        } else {
            Ok(vec![Arc::new(Int64Array::from(sums))])
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        debug_assert!(opt_filter.is_none(), "opt_filter is not supported yet");

        let expected_state_len = if self.eval_mode == EvalMode::Try {
            2
        } else {
            1
        };
        if expected_state_len != values.len() {
            return Err(DataFusionError::Internal(format!(
                "Invalid state while merging batch. Expected {} elements but found {}",
                expected_state_len,
                values.len()
            )));
        }
        let that_sums = values[0].as_primitive::<Int64Type>();

        self.resize_helper(total_num_groups);

        let that_sums_is_all_nulls = if self.eval_mode == EvalMode::Try {
            Some(values[1].as_boolean())
        } else {
            None
        };

        for (idx, &group_index) in group_indices.iter().enumerate() {
            let that_sum = if that_sums.is_null(idx) {
                None
            } else {
                Some(that_sums.value(idx))
            };

            if self.eval_mode == EvalMode::Try {
                let that_has_all_nulls = that_sums_is_all_nulls.unwrap().value(idx);

                let that_overflowed = !that_has_all_nulls && that_sum.is_none();
                let this_overflowed =
                    !self.has_all_nulls[group_index] && self.sums[group_index].is_none();

                if that_overflowed || this_overflowed {
                    self.sums[group_index] = None;
                    self.has_all_nulls[group_index] = false;
                    continue;
                }

                if that_has_all_nulls {
                    continue;
                }

                if self.has_all_nulls[group_index] {
                    self.sums[group_index] = that_sum;
                    self.has_all_nulls[group_index] = false;
                    continue;
                }
            } else {
                if that_sum.is_none() {
                    continue;
                }
                if self.sums[group_index].is_none() {
                    self.sums[group_index] = that_sum;
                    continue;
                }
            }

            // Both sides have non-null. Update sums now
            let left = self.sums[group_index].unwrap();
            let right = that_sum.unwrap();

            match self.eval_mode {
                EvalMode::Legacy => {
                    self.sums[group_index] = Some(left.add_wrapping(right));
                }
                EvalMode::Ansi | EvalMode::Try => {
                    match left.add_checked(right) {
                        Ok(v) => self.sums[group_index] = Some(v),
                        Err(_) => {
                            if self.eval_mode == EvalMode::Ansi {
                                return Err(DataFusionError::from(arithmetic_overflow_error(
                                    "integer",
                                )));
                            } else {
                                // overflow. update flag accordingly
                                self.sums[group_index] = None;
                                self.has_all_nulls[group_index] = false;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}
