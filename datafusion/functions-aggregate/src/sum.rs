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

//! Defines `SUM` and `SUM DISTINCT` aggregate accumulators

use ahash::RandomState;
use datafusion_expr::utils::AggregateOrderSensitivity;
use std::any::Any;
use std::collections::HashSet;
use std::mem::{size_of, size_of_val};

use arrow::array::Array;
use arrow::array::ArrowNativeTypeOp;
use arrow::array::{ArrowNumericType, AsArray};
use arrow::datatypes::ArrowPrimitiveType;
use arrow::datatypes::{ArrowNativeType, FieldRef};
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Float64Type, Int64Type, UInt64Type,
    DECIMAL128_MAX_PRECISION, DECIMAL256_MAX_PRECISION,
};
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::{
    exec_err, not_impl_err, utils::take_function_args, HashMap, Result, ScalarValue,
};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, Volatility,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_functions_aggregate_common::utils::Hashable;
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    Sum,
    sum,
    expression,
    "Returns the sum of a group of values.",
    sum_udaf
);

/// Sum only supports a subset of numeric types, instead relying on type coercion
///
/// This macro is similar to [downcast_primitive](arrow::array::downcast_primitive)
///
/// `args` is [AccumulatorArgs]
/// `helper` is a macro accepting (ArrowPrimitiveType, DataType)
macro_rules! downcast_sum {
    ($args:ident, $helper:ident) => {
        match $args.return_field.data_type().clone() {
            DataType::UInt64 => {
                $helper!(UInt64Type, $args.return_field.data_type().clone())
            }
            DataType::Int64 => {
                $helper!(Int64Type, $args.return_field.data_type().clone())
            }
            DataType::Float64 => {
                $helper!(Float64Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal128(_, _) => {
                $helper!(Decimal128Type, $args.return_field.data_type().clone())
            }
            DataType::Decimal256(_, _) => {
                $helper!(Decimal256Type, $args.return_field.data_type().clone())
            }
            _ => {
                not_impl_err!(
                    "Sum not supported for {}: {}",
                    $args.name,
                    $args.return_field.data_type()
                )
            }
        }
    };
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the sum of all values in the specified column.",
    syntax_example = "sum(expression)",
    sql_example = r#"```sql
> SELECT sum(column_name) FROM table_name;
+-----------------------+
| sum(column_name)       |
+-----------------------+
| 12345                 |
+-----------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug)]
pub struct Sum {
    signature: Signature,
}

impl Sum {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Sum {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateUDFImpl for Sum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sum"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [args] = take_function_args(self.name(), arg_types)?;

        // Refer to https://www.postgresql.org/docs/8.2/functions-aggregate.html doc
        // smallint, int, bigint, real, double precision, decimal, or interval.

        fn coerced_type(data_type: &DataType) -> Result<DataType> {
            match data_type {
                DataType::Dictionary(_, v) => coerced_type(v),
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                    Ok(data_type.clone())
                }
                dt if dt.is_signed_integer() => Ok(DataType::Int64),
                dt if dt.is_unsigned_integer() => Ok(DataType::UInt64),
                dt if dt.is_floating() => Ok(DataType::Float64),
                _ => exec_err!("Sum not supported for {}", data_type),
            }
        }

        Ok(vec![coerced_type(args)?])
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::Int64 => Ok(DataType::Int64),
            DataType::UInt64 => Ok(DataType::UInt64),
            DataType::Float64 => Ok(DataType::Float64),
            DataType::Decimal128(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL128_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal128(new_precision, *scale))
            }
            DataType::Decimal256(precision, scale) => {
                // in the spark, the result type is DECIMAL(min(38,precision+10), s)
                // ref: https://github.com/apache/spark/blob/fcf636d9eb8d645c24be3db2d599aba2d7e2955a/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Sum.scala#L66
                let new_precision = DECIMAL256_MAX_PRECISION.min(*precision + 10);
                Ok(DataType::Decimal256(new_precision, *scale))
            }
            other => {
                exec_err!("[return_type] SUM not supported for {}", other)
            }
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(DistinctSumAccumulator::<$t>::try_new(&$dt)?))
                };
            }
            downcast_sum!(args, helper)
        } else {
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SumAccumulator::<$t>::new($dt.clone())))
                };
            }
            downcast_sum!(args, helper)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        if args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "sum distinct"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.return_type().clone(), true),
                false,
            )
            .into()])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, "sum"),
                args.return_type().clone(),
                true,
            )
            .into()])
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        !args.is_distinct
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PrimitiveGroupsAccumulator::<$t, _>::new(
                    &$dt,
                    |x, y| *x = x.add_wrapping(y),
                )))
            };
        }
        downcast_sum!(args, helper)
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        if args.is_distinct {
            // distinct path: use our sliding‐window distinct‐sum
            macro_rules! helper_distinct {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SlidingDistinctSumAccumulator::try_new(&$dt)?))
                };
            }
            downcast_sum!(args, helper_distinct)
        } else {
            // non‐distinct path: existing sliding sum
            macro_rules! helper {
                ($t:ty, $dt:expr) => {
                    Ok(Box::new(SlidingSumAccumulator::<$t>::new($dt.clone())))
                };
            }
            downcast_sum!(args, helper)
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        // `SUM` is only monotonically increasing when its input is unsigned.
        // TODO: Expand these utilizing statistics.
        match data_type {
            DataType::UInt8 => SetMonotonicity::Increasing,
            DataType::UInt16 => SetMonotonicity::Increasing,
            DataType::UInt32 => SetMonotonicity::Increasing,
            DataType::UInt64 => SetMonotonicity::Increasing,
            _ => SetMonotonicity::NotMonotonic,
        }
    }
}

/// This accumulator computes SUM incrementally
struct SumAccumulator<T: ArrowNumericType> {
    sum: Option<T::Native>,
    data_type: DataType,
}

impl<T: ArrowNumericType> std::fmt::Debug for SumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: None,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if let Some(x) = arrow::compute::sum(values) {
            let v = self.sum.get_or_insert(T::Native::usize_as(0));
            *v = v.add_wrapping(x);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.sum, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// This accumulator incrementally computes sums over a sliding window
///
/// This is separate from [`SumAccumulator`] as requires additional state
struct SlidingSumAccumulator<T: ArrowNumericType> {
    sum: T::Native,
    count: u64,
    data_type: DataType,
}

impl<T: ArrowNumericType> std::fmt::Debug for SlidingSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlidingSumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> SlidingSumAccumulator<T> {
    fn new(data_type: DataType) -> Self {
        Self {
            sum: T::Native::usize_as(0),
            count: 0,
            data_type,
        }
    }
}

impl<T: ArrowNumericType> Accumulator for SlidingSumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?, self.count.into()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.count += (values.len() - values.null_count()) as u64;
        if let Some(x) = arrow::compute::sum(values) {
            self.sum = self.sum.add_wrapping(x)
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let values = states[0].as_primitive::<T>();
        if let Some(x) = arrow::compute::sum(values) {
            self.sum = self.sum.add_wrapping(x)
        }
        if let Some(x) = arrow::compute::sum(states[1].as_primitive::<UInt64Type>()) {
            self.count += x;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let v = (self.count != 0).then_some(self.sum);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        if let Some(x) = arrow::compute::sum(values) {
            self.sum = self.sum.sub_wrapping(x)
        }
        self.count -= (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

struct DistinctSumAccumulator<T: ArrowPrimitiveType> {
    values: HashSet<Hashable<T::Native>, RandomState>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for DistinctSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctSumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowPrimitiveType> DistinctSumAccumulator<T> {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            values: HashSet::default(),
            data_type: data_type.clone(),
        })
    }
}

impl<T: ArrowPrimitiveType> Accumulator for DistinctSumAccumulator<T> {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let distinct_values = self
                .values
                .iter()
                .map(|value| {
                    ScalarValue::new_primitive::<T>(Some(value.0), &self.data_type)
                })
                .collect::<Result<Vec<_>>>()?;

            vec![ScalarValue::List(ScalarValue::new_list_nullable(
                &distinct_values,
                &self.data_type,
            ))]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(Hashable(array.value(idx)));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(Hashable(*x));
            }),
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for x in states[0].as_list::<i32>().iter().flatten() {
            self.update_batch(&[x])?
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc.add_wrapping(distinct_value.0)
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.values.capacity() * size_of::<T::Native>()
    }
}

/// A sliding‐window accumulator for `SUM(DISTINCT)` over Int64 columns.
/// Maintains a running sum so that `evaluate()` is O(1).
#[derive(Debug)]
pub struct SlidingDistinctSumAccumulator {
    /// Map each distinct value → its current count in the window
    counts: HashMap<i64, usize, RandomState>,
    /// Running sum of all distinct keys currently in the window
    sum: i64,
    /// Data type (must be Int64)
    data_type: DataType,
}

impl SlidingDistinctSumAccumulator {
    /// Create a new accumulator; only `DataType::Int64` is supported.
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        // TODO support other numeric types
        if *data_type != DataType::Int64 {
            return exec_err!("SlidingDistinctSumAccumulator only supports Int64");
        }
        Ok(Self {
            counts: HashMap::default(),
            sum: 0,
            data_type: data_type.clone(),
        })
    }
}

impl Accumulator for SlidingDistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_primitive::<Int64Type>();
        for &v in arr.values() {
            let cnt = self.counts.entry(v).or_insert(0);
            if *cnt == 0 {
                // first occurrence in window
                self.sum = self.sum.wrapping_add(v);
            }
            *cnt += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // O(1) wrap of running sum
        Ok(ScalarValue::Int64(Some(self.sum)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Serialize distinct keys for cross-partition merge if needed
        let keys = self
            .counts
            .keys()
            .cloned()
            .map(Some)
            .map(ScalarValue::Int64)
            .collect::<Vec<_>>();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &keys,
            &self.data_type,
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Merge distinct keys from other partitions
        let list_arr = states[0].as_list::<i32>();
        for maybe_inner in list_arr.iter().flatten() {
            for idx in 0..maybe_inner.len() {
                if let ScalarValue::Int64(Some(v)) =
                    ScalarValue::try_from_array(&*maybe_inner, idx)?
                {
                    let cnt = self.counts.entry(v).or_insert(0);
                    if *cnt == 0 {
                        self.sum = self.sum.wrapping_add(v);
                    }
                    *cnt += 1;
                }
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let arr = values[0].as_primitive::<Int64Type>();
        for &v in arr.values() {
            if let Some(cnt) = self.counts.get_mut(&v) {
                *cnt -= 1;
                if *cnt == 0 {
                    // last copy leaving window
                    self.sum = self.sum.wrapping_sub(v);
                    self.counts.remove(&v);
                }
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
