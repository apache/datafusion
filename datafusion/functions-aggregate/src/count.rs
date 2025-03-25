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

use ahash::RandomState;
use arrow::array::{
    ArrowPrimitiveType, BinaryArray, BinaryViewArray, GenericByteArray,
    GenericByteViewArray, OffsetSizeTrait, StringArray, StringViewArray,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use datafusion_common::stats::Precision;
use datafusion_expr::expr::WindowFunction;
use datafusion_functions_aggregate_common::aggregate::count_distinct::BytesViewDistinctCountAccumulator;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::nulls::filtered_null_mask;
use datafusion_functions_aggregate_common::utils::Hashable;
use datafusion_macros::user_doc;
use datafusion_physical_expr::expressions;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::{size_of, size_of_val};
use std::ops::BitAnd;
use std::sync::Arc;

use arrow::{
    array::{ArrayRef, AsArray},
    compute,
    datatypes::{
        DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field,
        Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use arrow::{
    array::{Array, BooleanArray, Int64Array, ListArray, PrimitiveArray},
    buffer::BooleanBuffer,
};
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, Result, ScalarValue,
};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{
    function::AccumulatorArgs, utils::format_state_name, Accumulator, AggregateUDFImpl,
    Documentation, EmitTo, GroupsAccumulator, SetMonotonicity, Signature, Volatility,
};
use datafusion_expr::{
    Expr, ReversedUDAF, StatisticsArgs, TypeSignature, WindowFunctionDefinition,
};
use datafusion_functions_aggregate_common::aggregate::count_distinct::{
    BytesDistinctCountAccumulator, FloatDistinctCountAccumulator,
    PrimitiveDistinctCountAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate_indices;
use datafusion_physical_expr_common::binary_map::OutputType;

use arrow::datatypes::{ByteArrayType, GenericStringType};
use datafusion_common::cast::{as_list_array, as_primitive_array};
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use std::convert::TryFrom;

make_udaf_expr_and_func!(
    Count,
    count,
    expr,
    "Count the number of non-null values in the column",
    count_udaf
);

pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        count_udaf(),
        vec![expr],
        true,
        None,
        None,
        None,
    ))
}

/// Creates aggregation to count all rows.
///
/// In SQL this is `SELECT COUNT(*) ... `
///
/// The expression is equivalent to `COUNT(*)`, `COUNT()`, `COUNT(1)`, and is
/// aliased to a column named `"count(*)"` for backward compatibility.
///
/// Example
/// ```
/// # use datafusion_functions_aggregate::count::count_all;
/// # use datafusion_expr::col;
/// // create `count(*)` expression
/// let expr = count_all();
/// assert_eq!(expr.schema_name().to_string(), "count(*)");
/// // if you need to refer to this column, use the `schema_name` function
/// let expr = col(expr.schema_name().to_string());
/// ```
pub fn count_all() -> Expr {
    count(Expr::Literal(COUNT_STAR_EXPANSION)).alias("count(*)")
}

/// Creates window aggregation to count all rows.
///
/// In SQL this is `SELECT COUNT(*) OVER (..) ... `
///
/// The expression is equivalent to `COUNT(*)`, `COUNT()`, `COUNT(1)`
///
/// Example
/// ```
/// # use datafusion_functions_aggregate::count::count_all_window;
/// # use datafusion_expr::col;
/// // create `count(*)` OVER ... window function expression
/// let expr = count_all_window();
/// assert_eq!(
///   expr.schema_name().to_string(),
///   "count(Int64(1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
/// );
/// // if you need to refer to this column, use the `schema_name` function
/// let expr = col(expr.schema_name().to_string());
/// ```
pub fn count_all_window() -> Expr {
    Expr::WindowFunction(WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(count_udaf()),
        vec![Expr::Literal(COUNT_STAR_EXPANSION)],
    ))
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the number of non-null values in the specified column. To include null values in the total count, use `count(*)`.",
    syntax_example = "count(expression)",
    sql_example = r#"```sql
> SELECT count(column_name) FROM table_name;
+-----------------------+
| count(column_name)     |
+-----------------------+
| 100                   |
+-----------------------+

> SELECT count(*) FROM table_name;
+------------------+
| count(*)         |
+------------------+
| 120              |
+------------------+
```"#,
    standard_argument(name = "expression",)
)]
pub struct Count {
    signature: Signature,
}

impl Debug for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Count")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::VariadicAny, TypeSignature::Nullary],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for Count {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "count distinct"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new_list_field(args.input_types[0].clone(), true),
                // For group count distinct accumulator, null list item stands for an
                // empty value set (i.e., all NULL value so far for that group).
                true,
            )])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )])
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !acc_args.is_distinct {
            return Ok(Box::new(CountAccumulator::new()));
        }

        if acc_args.exprs.len() > 1 {
            return not_impl_err!("COUNT DISTINCT with multiple arguments");
        }

        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(match data_type {
            // try and use a specialized accumulator if possible, otherwise fall back to generic accumulator
            DataType::Int8 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int8Type>::new(data_type),
            ),
            DataType::Int16 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int16Type>::new(data_type),
            ),
            DataType::Int32 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int32Type>::new(data_type),
            ),
            DataType::Int64 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int64Type>::new(data_type),
            ),
            DataType::UInt8 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt8Type>::new(data_type),
            ),
            DataType::UInt16 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt16Type>::new(data_type),
            ),
            DataType::UInt32 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt32Type>::new(data_type),
            ),
            DataType::UInt64 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt64Type>::new(data_type),
            ),
            DataType::Decimal128(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal128Type,
            >::new(data_type)),
            DataType::Decimal256(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal256Type,
            >::new(data_type)),

            DataType::Date32 => Box::new(
                PrimitiveDistinctCountAccumulator::<Date32Type>::new(data_type),
            ),
            DataType::Date64 => Box::new(
                PrimitiveDistinctCountAccumulator::<Date64Type>::new(data_type),
            ),
            DataType::Time32(TimeUnit::Millisecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time32MillisecondType>::new(
                    data_type,
                ),
            ),
            DataType::Time32(TimeUnit::Second) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time32SecondType>::new(data_type),
            ),
            DataType::Time64(TimeUnit::Microsecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time64MicrosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time64NanosecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampMicrosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampMillisecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampNanosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Second, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampSecondType>::new(data_type),
            ),

            DataType::Float16 => {
                Box::new(FloatDistinctCountAccumulator::<Float16Type>::new())
            }
            DataType::Float32 => {
                Box::new(FloatDistinctCountAccumulator::<Float32Type>::new())
            }
            DataType::Float64 => {
                Box::new(FloatDistinctCountAccumulator::<Float64Type>::new())
            }

            DataType::Utf8 => {
                Box::new(BytesDistinctCountAccumulator::<i32>::new(OutputType::Utf8))
            }
            DataType::Utf8View => {
                Box::new(BytesViewDistinctCountAccumulator::new(OutputType::Utf8View))
            }
            DataType::LargeUtf8 => {
                Box::new(BytesDistinctCountAccumulator::<i64>::new(OutputType::Utf8))
            }
            DataType::Binary => Box::new(BytesDistinctCountAccumulator::<i32>::new(
                OutputType::Binary,
            )),
            DataType::BinaryView => Box::new(BytesViewDistinctCountAccumulator::new(
                OutputType::BinaryView,
            )),
            DataType::LargeBinary => Box::new(BytesDistinctCountAccumulator::<i64>::new(
                OutputType::Binary,
            )),

            // Use the generic accumulator based on `ScalarValue` for all other types
            _ => Box::new(DistinctCountAccumulator {
                values: HashSet::default(),
                state_data_type: data_type.clone(),
            }),
        })
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        // groups accumulator only supports `COUNT(c1)` or `COUNT(distinct c1)`, not
        // `COUNT(c1, c2)`, etc
        args.exprs.len() == 1
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        if args.is_distinct {
            if args.exprs.len() > 1 {
                return not_impl_err!("COUNT DISTINCT with multiple arguments");
            }

            let data_type = &args.exprs[0].data_type(args.schema)?;
            Ok(match data_type {
                // try and use a specialized accumulator if possible, otherwise fall back to generic accumulator
                DataType::Int8 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Int8Type,
                >::new(data_type.clone())),
                DataType::Int16 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Int16Type,
                >::new(data_type.clone())),
                DataType::Int32 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Int32Type,
                >::new(data_type.clone())),
                DataType::Int64 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Int64Type,
                >::new(data_type.clone())),
                DataType::UInt8 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    UInt8Type,
                >::new(data_type.clone())),
                DataType::UInt16 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    UInt16Type,
                >::new(data_type.clone())),
                DataType::UInt32 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    UInt32Type,
                >::new(data_type.clone())),
                DataType::UInt64 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    UInt64Type,
                >::new(data_type.clone())),
                DataType::Decimal128(_, _) => Box::new(
                    PrimitiveDistinctCountGroupsAccumulator::<Decimal128Type>::new(
                        data_type.clone(),
                    ),
                ),
                DataType::Decimal256(_, _) => Box::new(
                    PrimitiveDistinctCountGroupsAccumulator::<Decimal256Type>::new(
                        data_type.clone(),
                    ),
                ),

                DataType::Date32 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Date32Type,
                >::new(data_type.clone())),
                DataType::Date64 => Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                    Date64Type,
                >::new(data_type.clone())),
                DataType::Time32(TimeUnit::Millisecond) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        Time32MillisecondType,
                    >::new(data_type.clone()))
                }
                DataType::Time32(TimeUnit::Second) => Box::new(
                    PrimitiveDistinctCountGroupsAccumulator::<Time32SecondType>::new(
                        data_type.clone(),
                    ),
                ),
                DataType::Time64(TimeUnit::Microsecond) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        Time64MicrosecondType,
                    >::new(data_type.clone()))
                }
                DataType::Time64(TimeUnit::Nanosecond) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        Time64NanosecondType,
                    >::new(data_type.clone()))
                }
                DataType::Timestamp(TimeUnit::Microsecond, _) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        TimestampMicrosecondType,
                    >::new(data_type.clone()))
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        TimestampMillisecondType,
                    >::new(data_type.clone()))
                }
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    Box::new(PrimitiveDistinctCountGroupsAccumulator::<
                        TimestampNanosecondType,
                    >::new(data_type.clone()))
                }
                DataType::Timestamp(TimeUnit::Second, _) => Box::new(
                    PrimitiveDistinctCountGroupsAccumulator::<TimestampSecondType>::new(
                        data_type.clone(),
                    ),
                ),

                DataType::Float16 => Box::new(FloatDistinctCountGroupsAccumulator::<
                    Float16Type,
                >::new(data_type.clone())),
                DataType::Float32 => Box::new(FloatDistinctCountGroupsAccumulator::<
                    Float32Type,
                >::new(data_type.clone())),
                DataType::Float64 => Box::new(FloatDistinctCountGroupsAccumulator::<
                    Float64Type,
                >::new(data_type.clone())),

                // DataType::Utf8 => Box::new(
                //     BytesDistinctCountGroupsAccumulator::<i32>::new(OutputType::Utf8),
                // ),
                // DataType::Utf8View => Box::new(
                //     BytesViewDistinctCountGroupsAccumulator::new(OutputType::Utf8View),
                // ),
                // DataType::LargeUtf8 => Box::new(
                //     BytesDistinctCountGroupsAccumulator::<i64>::new(OutputType::Utf8),
                // ),
                // DataType::Binary => Box::new(
                //     BytesDistinctCountGroupsAccumulator::<i32>::new(OutputType::Binary),
                // ),
                // DataType::BinaryView => Box::new(
                //     BytesViewDistinctCountGroupsAccumulator::new(OutputType::BinaryView),
                // ),
                // DataType::LargeBinary => {
                //     Box::new(BytesDistinctCountGroupsAccumulator::<i64>::new(
                //         OutputType::Binary,
                //     ))
                // }

                // Use the generic accumulator based on `ScalarValue` for all other types
                _ => Box::new(DistinctCountGroupsAccumulator::new(data_type.clone())),
            })
        } else {
            Ok(Box::new(CountGroupsAccumulator::new()))
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        if statistics_args.is_distinct {
            return None;
        }
        if let Precision::Exact(num_rows) = statistics_args.statistics.num_rows {
            if statistics_args.exprs.len() == 1 {
                // TODO optimize with exprs other than Column
                if let Some(col_expr) = statistics_args.exprs[0]
                    .as_any()
                    .downcast_ref::<expressions::Column>()
                {
                    let current_val = &statistics_args.statistics.column_statistics
                        [col_expr.index()]
                    .null_count;
                    if let &Precision::Exact(val) = current_val {
                        return Some(ScalarValue::Int64(Some((num_rows - val) as i64)));
                    }
                } else if let Some(lit_expr) = statistics_args.exprs[0]
                    .as_any()
                    .downcast_ref::<expressions::Literal>()
                {
                    if lit_expr.value() == &COUNT_STAR_EXPANSION {
                        return Some(ScalarValue::Int64(Some(num_rows as i64)));
                    }
                }
            }
        }
        None
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        // `COUNT` is monotonically increasing as it always increases or stays
        // the same as new values are seen.
        SetMonotonicity::Increasing
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
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
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

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

/// An accumulator to compute the counts of [`PrimitiveArray<T>`].
/// Stores values as native types, and does overflow checking
///
/// Unlike most other accumulators, COUNT never produces NULLs. If no
/// non-null values are seen in any group the output is 0. Thus, this
/// accumulator has no additional null or seen filter tracking.
#[derive(Debug)]
struct CountGroupsAccumulator {
    /// Count per group.
    ///
    /// Note this is an i64 and not a u64 (or usize) because the
    /// output type of count is `DataType::Int64`. Thus by using `i64`
    /// for the counts, the output [`Int64Array`] can be created
    /// without copy.
    counts: Vec<i64>,
}

impl CountGroupsAccumulator {
    pub fn new() -> Self {
        Self { counts: vec![] }
    }
}

impl GroupsAccumulator for CountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = &values[0];

        // Add one to each group's counter for each non null, non
        // filtered value
        self.counts.resize(total_num_groups, 0);
        accumulate_indices(
            group_indices,
            values.logical_nulls().as_ref(),
            opt_filter,
            |group_index| {
                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        // Since aggregate filter should be applied in partial stage, in final stage there should be no filter
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<Int64Type>();

        // intermediate counts are always created as non null
        assert_eq!(partial_counts.null_count(), 0);
        let partial_counts = partial_counts.values();

        // Adds the counts with the partial counts
        self.counts.resize(total_num_groups, 0);
        group_indices.iter().zip(partial_counts.iter()).for_each(
            |(&group_index, partial_count)| {
                self.counts[group_index] += partial_count;
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);

        // Count is always non null (null inputs just don't contribute to the overall values)
        let nulls = None;
        let array = PrimitiveArray::<Int64Type>::new(counts.into(), nulls);

        Ok(Arc::new(array))
    }

    // return arrays for counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts: PrimitiveArray<Int64Type> = Int64Array::from(counts); // zero copy, no nulls
        Ok(vec![Arc::new(counts) as ArrayRef])
    }

    /// Converts an input batch directly to a state batch
    ///
    /// The state of `COUNT` is always a single Int64Array:
    /// * `1` (for non-null, non filtered values)
    /// * `0` (for null values)
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = &values[0];

        let state_array = match (values.logical_nulls(), opt_filter) {
            (None, None) => {
                // In case there is no nulls in input and no filter, returning array of 1
                Arc::new(Int64Array::from_value(1, values.len()))
            }
            (Some(nulls), None) => {
                // If there are any nulls in input values -- casting `nulls` (true for values, false for nulls)
                // of input array to Int64
                let nulls = BooleanArray::new(nulls.into_inner(), None);
                compute::cast(&nulls, &DataType::Int64)?
            }
            (None, Some(filter)) => {
                // If there is only filter
                // - applying filter null mask to filter values by bitand filter values and nulls buffers
                //   (using buffers guarantees absence of nulls in result)
                // - casting result of bitand to Int64 array
                let (filter_values, filter_nulls) = filter.clone().into_parts();

                let state_buf = match filter_nulls {
                    Some(filter_nulls) => &filter_values & filter_nulls.inner(),
                    None => filter_values,
                };

                let boolean_state = BooleanArray::new(state_buf, None);
                compute::cast(&boolean_state, &DataType::Int64)?
            }
            (Some(nulls), Some(filter)) => {
                // For both input nulls and filter
                // - applying filter null mask to filter values by bitand filter values and nulls buffers
                //   (using buffers guarantees absence of nulls in result)
                // - applying values null mask to filter buffer by another bitand on filter result and
                //   nulls from input values
                // - casting result to Int64 array
                let (filter_values, filter_nulls) = filter.clone().into_parts();

                let filter_buf = match filter_nulls {
                    Some(filter_nulls) => &filter_values & filter_nulls.inner(),
                    None => filter_values,
                };
                let state_buf = &filter_buf & nulls.inner();

                let boolean_state = BooleanArray::new(state_buf, None);
                compute::cast(&boolean_state, &DataType::Int64)?
            }
        };

        Ok(vec![state_array])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * size_of::<usize>()
    }
}

/// count null values for multiple columns
/// for each row if one column value is null, then null_count + 1
fn null_count_for_multiple_cols(values: &[ArrayRef]) -> usize {
    if values.len() > 1 {
        let result_bool_buf: Option<BooleanBuffer> = values
            .iter()
            .map(|a| a.logical_nulls())
            .fold(None, |acc, b| match (acc, b) {
                (Some(acc), Some(b)) => Some(acc.bitand(b.inner())),
                (Some(acc), None) => Some(acc),
                (None, Some(b)) => Some(b.into_inner()),
                _ => None,
            });
        result_bool_buf.map_or(0, |b| values[0].len() - b.count_set_bits())
    } else {
        values[0]
            .logical_nulls()
            .map_or(0, |nulls| nulls.null_count())
    }
}

/// General purpose distinct accumulator that works for any DataType by using
/// [`ScalarValue`].
///
/// It stores intermediate results as a `ListArray`
///
/// Note that many types have specialized accumulators that are (much)
/// more efficient such as [`PrimitiveDistinctCountAccumulator`] and
/// [`BytesDistinctCountAccumulator`]
#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<ScalarValue, RandomState>,
    state_data_type: DataType,
}

impl DistinctCountAccumulator {
    // calculating the size for fixed length values, taking first batch size *
    // number of batches This method is faster than .full_size(), however it is
    // not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        size_of_val(self)
            + (size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .next()
                .map(|vals| ScalarValue::size(vals) - size_of_val(vals))
                .unwrap_or(0)
            + size_of::<DataType>()
    }

    // calculates the size as accurately as possible. Note that calling this
    // method is expensive
    fn full_size(&self) -> usize {
        size_of_val(self)
            + (size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| ScalarValue::size(vals) - size_of_val(vals))
                .sum::<usize>()
            + size_of::<DataType>()
    }
}

impl Accumulator for DistinctCountAccumulator {
    /// Returns the distinct values seen so far as (one element) ListArray.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let scalars = self.values.iter().cloned().collect::<Vec<_>>();
        let arr =
            ScalarValue::new_list_nullable(scalars.as_slice(), &self.state_data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        if arr.data_type() == &DataType::Null {
            return Ok(());
        }

        (0..arr.len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                self.values.insert(scalar);
            }
            Ok(())
        })
    }

    /// Merges multiple sets of distinct values into the current set.
    ///
    /// The input to this function is a `ListArray` with **multiple** rows,
    /// where each row contains the values from a partial aggregate's phase (e.g.
    /// the result of calling `Self::state` on multiple accumulators).
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "array_agg states must be singleton!");
        let array = &states[0];
        let list_array = array.as_list::<i32>();
        for inner_array in list_array.iter() {
            let Some(inner_array) = inner_array else {
                return internal_err!(
                    "Intermediate results of COUNT DISTINCT should always be non null"
                );
            };
            self.update_batch(&[inner_array])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        match &self.state_data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }
}

/// GroupsAccumulator for COUNT DISTINCT operations
#[derive(Debug)]
pub struct DistinctCountGroupsAccumulator {
    /// One HashSet per group to track distinct values
    distinct_sets: Vec<HashSet<ScalarValue, RandomState>>,
    data_type: DataType,
}

impl DistinctCountGroupsAccumulator {
    pub fn new(data_type: DataType) -> Self {
        Self {
            distinct_sets: vec![],
            data_type,
        }
    }

    fn ensure_sets(&mut self, total_num_groups: usize) {
        if self.distinct_sets.len() < total_num_groups {
            self.distinct_sets
                .resize_with(total_num_groups, HashSet::default);
        }
    }
}

impl GroupsAccumulator for DistinctCountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        self.ensure_sets(total_num_groups);

        let array = &values[0];

        // Use a pattern similar to accumulate_indices to process rows
        // that are not null and pass the filter
        let nulls = array.logical_nulls();

        match (nulls.as_ref(), opt_filter) {
            (None, None) => {
                // No nulls, no filter - process all rows
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    if let Ok(scalar) = ScalarValue::try_from_array(array, row_idx) {
                        self.distinct_sets[group_idx].insert(scalar);
                    }
                }
            }
            (Some(nulls), None) => {
                // Has nulls, no filter
                for (row_idx, (&group_idx, is_valid)) in
                    group_indices.iter().zip(nulls.iter()).enumerate()
                {
                    if is_valid {
                        if let Ok(scalar) = ScalarValue::try_from_array(array, row_idx) {
                            self.distinct_sets[group_idx].insert(scalar);
                        }
                    }
                }
            }
            (None, Some(filter)) => {
                // No nulls, has filter
                for (row_idx, (&group_idx, filter_value)) in
                    group_indices.iter().zip(filter.iter()).enumerate()
                {
                    if let Some(true) = filter_value {
                        if let Ok(scalar) = ScalarValue::try_from_array(array, row_idx) {
                            self.distinct_sets[group_idx].insert(scalar);
                        }
                    }
                }
            }
            (Some(nulls), Some(filter)) => {
                // Has nulls and filter
                let iter = filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(nulls.iter())
                    .enumerate();

                for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                    if is_valid && filter_value == Some(true) {
                        if let Ok(scalar) = ScalarValue::try_from_array(array, row_idx) {
                            self.distinct_sets[group_idx].insert(scalar);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let distinct_sets: Vec<HashSet<ScalarValue, RandomState>> =
            emit_to.take_needed(&mut self.distinct_sets);

        let counts = distinct_sets
            .iter()
            .map(|set| set.len() as i64)
            .collect::<Vec<_>>();
        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(
            values.len(),
            1,
            "COUNT DISTINCT merge expects a single state array"
        );
        self.ensure_sets(total_num_groups);

        let list_array = as_list_array(&values[0])?;

        // For each group in the incoming batch
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if i < list_array.len() {
                let inner_array = list_array.value(i);
                // Add each value to our set for this group
                for j in 0..inner_array.len() {
                    if !inner_array.is_null(j) {
                        let scalar = ScalarValue::try_from_array(&inner_array, j)?;
                        self.distinct_sets[group_idx].insert(scalar);
                    }
                }
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let distinct_sets: Vec<HashSet<ScalarValue, RandomState>> =
            emit_to.take_needed(&mut self.distinct_sets);

        let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
        offsets.push(0);
        let mut curr_len = 0i32;

        let mut value_iter = distinct_sets
            .into_iter()
            .flat_map(|set| {
                // build offset
                curr_len += set.len() as i32;
                offsets.push(curr_len);
                // convert into iter
                set.into_iter()
            })
            .peekable();
        let data_array: ArrayRef = if value_iter.peek().is_none() {
            arrow::array::new_empty_array(&self.data_type) as _
        } else {
            Arc::new(ScalarValue::iter_to_array(value_iter)?) as _
        };
        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));

        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offset_buffer,
            data_array,
            None,
        );

        Ok(vec![Arc::new(list_array) as _])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // For a single distinct value per row, create a list array with that value
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        let values = ArrayRef::clone(&values[0]);

        let offsets =
            OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
        let nulls = filtered_null_mask(opt_filter, &values);
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            offsets,
            values,
            nulls,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        // Base size of the struct
        let mut size = size_of::<Self>();

        // Size of the vector holding the HashSets
        size += size_of::<Vec<HashSet<ScalarValue, RandomState>>>()
            + self.distinct_sets.capacity()
                * size_of::<HashSet<ScalarValue, RandomState>>();

        // Estimate HashSet contents size more efficiently
        // Instead of iterating through all values which is expensive, use an approximation
        for set in &self.distinct_sets {
            // Base size of the HashSet
            size += set.capacity() * size_of::<(ScalarValue, ())>();

            // Estimate ScalarValue size using sample-based approach
            // Only look at up to 10 items as a sample
            let sample_size = 10.min(set.len());
            if sample_size > 0 {
                let avg_size = set
                    .iter()
                    .take(sample_size)
                    .map(|v| v.size())
                    .sum::<usize>()
                    / sample_size;

                // Extrapolate to the full set
                size += avg_size * (set.len() - sample_size);
            }
        }

        size
    }
}

/// A specialized GroupsAccumulator for count distinct operations with primitive types
/// This is more efficient than the general DistinctCountGroupsAccumulator for primitive types
#[derive(Debug)]
pub struct PrimitiveDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    /// One HashSet per group to track distinct values
    distinct_sets: Vec<HashSet<T::Native, RandomState>>,
    data_type: DataType,
}

impl<T> PrimitiveDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    pub fn new(data_type: DataType) -> Self {
        Self {
            distinct_sets: vec![],
            data_type,
        }
    }

    fn ensure_sets(&mut self, total_num_groups: usize) {
        if self.distinct_sets.len() < total_num_groups {
            self.distinct_sets
                .resize_with(total_num_groups, HashSet::default);
        }
    }
}

impl<T> GroupsAccumulator for PrimitiveDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        self.ensure_sets(total_num_groups);

        let array = as_primitive_array::<T>(&values[0])?;
        let data = array.values();

        // Implement a manual iteration rather than using accumulate_indices with a closure
        // that needs row_index
        match (array.logical_nulls(), opt_filter) {
            (None, None) => {
                // No nulls, no filter - process all rows
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    self.distinct_sets[group_idx].insert(data[row_idx]);
                }
            }
            (Some(nulls), None) => {
                // Has nulls, no filter
                for (row_idx, (&group_idx, is_valid)) in
                    group_indices.iter().zip(nulls.iter()).enumerate()
                {
                    if is_valid {
                        self.distinct_sets[group_idx].insert(data[row_idx]);
                    }
                }
            }
            (None, Some(filter)) => {
                // No nulls, has filter
                for (row_idx, (&group_idx, filter_value)) in
                    group_indices.iter().zip(filter.iter()).enumerate()
                {
                    if let Some(true) = filter_value {
                        self.distinct_sets[group_idx].insert(data[row_idx]);
                    }
                }
            }
            (Some(nulls), Some(filter)) => {
                // Has nulls and filter
                let iter = filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(nulls.iter())
                    .enumerate();

                for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                    if is_valid && filter_value == Some(true) {
                        self.distinct_sets[group_idx].insert(data[row_idx]);
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let counts = distinct_sets
            .iter()
            .map(|set| set.len() as i64)
            .collect::<Vec<_>>();

        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(
            values.len(),
            1,
            "COUNT DISTINCT merge expects a single state array"
        );
        self.ensure_sets(total_num_groups);

        let list_array = as_list_array(&values[0])?;

        // For each group in the incoming batch
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if i < list_array.len() {
                let inner_array = list_array.value(i);
                if !inner_array.is_empty() {
                    // Get the primitive array from the list and extend our set with its values
                    let primitive_array = as_primitive_array::<T>(&inner_array)?;
                    self.distinct_sets[group_idx].extend(primitive_array.values());
                }
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
        offsets.push(0);
        let mut values = Vec::new();

        // Create the values array by flattening all sets
        for set in distinct_sets {
            let start_len = values.len();
            values.extend(set.into_iter());
            offsets.push(values.len() as i32);
        }

        // Create the primitive array from the flattened values
        let values_array = Arc::new(
            PrimitiveArray::<T>::from_iter_values(values.into_iter())
                .with_data_type(self.data_type.clone()),
        ) as ArrayRef;

        // Create list array with the offsets
        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offset_buffer,
            values_array,
            None,
        );

        Ok(vec![Arc::new(list_array) as _])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // For a single distinct value per row, create a list array with that value
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        let values = ArrayRef::clone(&values[0]);

        let offsets =
            OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
        let nulls = filtered_null_mask(opt_filter, &values);
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            offsets,
            values,
            nulls,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        let mut total_size = std::mem::size_of::<Self>();

        // Size of vector container
        total_size += std::mem::size_of::<Vec<HashSet<T::Native, RandomState>>>();

        // Size of actual sets and their contents
        for set in &self.distinct_sets {
            let set_size = std::mem::size_of::<HashSet<T::Native, RandomState>>()
                + set.capacity() * std::mem::size_of::<T::Native>();
            total_size += set_size;
        }

        total_size
    }
}

#[derive(Debug)]
pub struct FloatDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    /// One HashSet per group to track distinct values
    distinct_sets: Vec<HashSet<Hashable<T::Native>, RandomState>>,
    data_type: DataType,
}

impl<T> FloatDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
{
    pub fn new(data_type: DataType) -> Self {
        Self {
            distinct_sets: vec![],
            data_type,
        }
    }

    fn ensure_sets(&mut self, total_num_groups: usize) {
        if self.distinct_sets.len() < total_num_groups {
            self.distinct_sets
                .resize_with(total_num_groups, HashSet::default);
        }
    }
}

impl<T> GroupsAccumulator for FloatDistinctCountGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        self.ensure_sets(total_num_groups);

        let array = as_primitive_array::<T>(&values[0])?;
        let data = array.values();

        // Implement a manual iteration rather than using accumulate_indices with a closure
        // that needs row_index
        match (array.logical_nulls(), opt_filter) {
            (None, None) => {
                // No nulls, no filter - process all rows
                for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                    self.distinct_sets[group_idx].insert(Hashable(data[row_idx]));
                }
            }
            (Some(nulls), None) => {
                // Has nulls, no filter
                for (row_idx, (&group_idx, is_valid)) in
                    group_indices.iter().zip(nulls.iter()).enumerate()
                {
                    if is_valid {
                        self.distinct_sets[group_idx].insert(Hashable(data[row_idx]));
                    }
                }
            }
            (None, Some(filter)) => {
                // No nulls, has filter
                for (row_idx, (&group_idx, filter_value)) in
                    group_indices.iter().zip(filter.iter()).enumerate()
                {
                    if let Some(true) = filter_value {
                        self.distinct_sets[group_idx].insert(Hashable(data[row_idx]));
                    }
                }
            }
            (Some(nulls), Some(filter)) => {
                // Has nulls and filter
                let iter = filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(nulls.iter())
                    .enumerate();

                for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                    if is_valid && filter_value == Some(true) {
                        self.distinct_sets[group_idx].insert(Hashable(data[row_idx]));
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let counts = distinct_sets
            .iter()
            .map(|set| set.len() as i64)
            .collect::<Vec<_>>();

        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(
            values.len(),
            1,
            "COUNT DISTINCT merge expects a single state array"
        );
        self.ensure_sets(total_num_groups);

        let list_array = as_list_array(&values[0])?;

        // For each group in the incoming batch
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if i < list_array.len() {
                let inner_array = list_array.value(i);
                if !inner_array.is_empty() {
                    // Get the primitive array from the list and extend our set with its values
                    let primitive_array = as_primitive_array::<T>(&inner_array)?;
                    self.distinct_sets[group_idx]
                        .extend(primitive_array.values().iter().map(|v| Hashable(*v)));
                }
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
        offsets.push(0);
        let mut values = Vec::new();

        // Create the values array by flattening all sets
        for set in distinct_sets {
            let start_len = values.len();
            values.extend(set.into_iter().map(|v| v.0));
            offsets.push(values.len() as i32);
        }

        // Create the primitive array from the flattened values
        let values_array = Arc::new(
            PrimitiveArray::<T>::from_iter_values(values.into_iter())
                .with_data_type(self.data_type.clone()),
        ) as ArrayRef;

        // Create list array with the offsets
        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(self.data_type.clone(), true)),
            offset_buffer,
            values_array,
            None,
        );

        Ok(vec![Arc::new(list_array) as _])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // For a single distinct value per row, create a list array with that value
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        let values = ArrayRef::clone(&values[0]);

        let offsets =
            OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
        let nulls = filtered_null_mask(opt_filter, &values);
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            offsets,
            values,
            nulls,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        let mut total_size = std::mem::size_of::<Self>();

        // Size of vector container
        total_size +=
            std::mem::size_of::<Vec<HashSet<Hashable<T::Native>, RandomState>>>();

        // Size of actual sets and their contents
        for set in &self.distinct_sets {
            let set_size =
                std::mem::size_of::<HashSet<Hashable<T::Native>, RandomState>>()
                    + set.capacity() * std::mem::size_of::<Hashable<T::Native>>();
            total_size += set_size;
        }

        total_size
    }
}

/// A specialized GroupsAccumulator for count distinct operations with string/binary view types
#[derive(Debug)]
pub struct BytesViewDistinctCountGroupsAccumulator {
    /// One HashSet per group to track distinct values
    distinct_sets: Vec<HashSet<Vec<u8>, RandomState>>,
    output_type: OutputType,
}

impl BytesViewDistinctCountGroupsAccumulator {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            distinct_sets: vec![],
            output_type,
        }
    }

    fn ensure_sets(&mut self, total_num_groups: usize) {
        if self.distinct_sets.len() < total_num_groups {
            self.distinct_sets
                .resize_with(total_num_groups, HashSet::default);
        }
    }
}

impl GroupsAccumulator for BytesViewDistinctCountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        self.ensure_sets(total_num_groups);

        // Handle binary view or string view arrays
        if let Some(array) = values[0].as_any().downcast_ref::<BinaryViewArray>() {
            // Implement a manual iteration rather than using accumulate_indices
            match (array.logical_nulls(), opt_filter) {
                (None, None) => {
                    // No nulls, no filter - process all rows
                    for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                        let value = array.value(row_idx);
                        self.distinct_sets[group_idx].insert(value.to_vec());
                    }
                }
                (Some(nulls), None) => {
                    // Has nulls, no filter
                    for (row_idx, (&group_idx, is_valid)) in
                        group_indices.iter().zip(nulls.iter()).enumerate()
                    {
                        if is_valid {
                            let value = array.value(row_idx);
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
                (None, Some(filter)) => {
                    // No nulls, has filter
                    for (row_idx, (&group_idx, filter_value)) in
                        group_indices.iter().zip(filter.iter()).enumerate()
                    {
                        if let Some(true) = filter_value {
                            let value = array.value(row_idx);
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
                (Some(nulls), Some(filter)) => {
                    // Has nulls and filter
                    let iter = filter
                        .iter()
                        .zip(group_indices.iter())
                        .zip(nulls.iter())
                        .enumerate();

                    for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                        if is_valid && filter_value == Some(true) {
                            let value = array.value(row_idx);
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
            }
        } else if let Some(array) = values[0].as_any().downcast_ref::<StringViewArray>() {
            // Implement a manual iteration rather than using accumulate_indices
            match (array.logical_nulls(), opt_filter) {
                (None, None) => {
                    // No nulls, no filter - process all rows
                    for (row_idx, &group_idx) in group_indices.iter().enumerate() {
                        let value = array.value(row_idx).as_bytes();
                        self.distinct_sets[group_idx].insert(value.to_vec());
                    }
                }
                (Some(nulls), None) => {
                    // Has nulls, no filter
                    for (row_idx, (&group_idx, is_valid)) in
                        group_indices.iter().zip(nulls.iter()).enumerate()
                    {
                        if is_valid {
                            let value = array.value(row_idx).as_bytes();
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
                (None, Some(filter)) => {
                    // No nulls, has filter
                    for (row_idx, (&group_idx, filter_value)) in
                        group_indices.iter().zip(filter.iter()).enumerate()
                    {
                        if let Some(true) = filter_value {
                            let value = array.value(row_idx).as_bytes();
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
                (Some(nulls), Some(filter)) => {
                    // Has nulls and filter
                    let iter = filter
                        .iter()
                        .zip(group_indices.iter())
                        .zip(nulls.iter())
                        .enumerate();

                    for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
                        if is_valid && filter_value == Some(true) {
                            let value = array.value(row_idx).as_bytes();
                            self.distinct_sets[group_idx].insert(value.to_vec());
                        }
                    }
                }
            }
        } else {
            return Err(datafusion_common::DataFusionError::Internal(format!(
                "Unsupported array type for BytesViewDistinctCountGroupsAccumulator: {:?}",
                values[0].data_type()
            )));
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let counts = distinct_sets
            .iter()
            .map(|set| set.len() as i64)
            .collect::<Vec<_>>();

        Ok(Arc::new(Int64Array::from(counts)))
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        _opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(
            values.len(),
            1,
            "COUNT DISTINCT merge expects a single state array"
        );
        self.ensure_sets(total_num_groups);

        let list_array = as_list_array(&values[0])?;

        // For each group in the incoming batch
        for (i, &group_idx) in group_indices.iter().enumerate() {
            if i < list_array.len() {
                let inner_array = list_array.value(i);
                if !inner_array.is_empty() {
                    // Handle binary view or string view arrays
                    if let Some(array) =
                        inner_array.as_any().downcast_ref::<BinaryViewArray>()
                    {
                        for j in 0..array.len() {
                            if !array.is_null(j) {
                                let value = array.value(j);
                                self.distinct_sets[group_idx].insert(value.to_vec());
                            }
                        }
                    } else if let Some(array) =
                        inner_array.as_any().downcast_ref::<StringViewArray>()
                    {
                        for j in 0..array.len() {
                            if !array.is_null(j) {
                                let value = array.value(j).as_bytes();
                                self.distinct_sets[group_idx].insert(value.to_vec());
                            }
                        }
                    } else {
                        return Err(datafusion_common::DataFusionError::Internal(format!(
                            "Unsupported inner array type for BytesViewDistinctCountGroupsAccumulator: {:?}",
                            inner_array.data_type()
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

        let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
        offsets.push(0);

        // Create arrays for views
        let inner_array: ArrayRef =
            match self.output_type {
                OutputType::Utf8View => {
                    let mut string_values = Vec::new();

                    // Collect all string values from all sets
                    for set in &distinct_sets {
                        for v in set {
                            // Safety: we know the value is valid UTF-8 since it came from a StringViewArray
                            let s = unsafe { std::str::from_utf8_unchecked(v) };
                            string_values.push(s.to_string());
                        }
                    }

                    // Use from_iter_values which works correctly
                    let array = StringViewArray::from_iter_values(
                        string_values.iter().map(|s| s.as_str()),
                    );
                    Arc::new(array)
                }
                OutputType::BinaryView => {
                    let mut bytes_values = Vec::new();

                    // Collect all byte values from all sets
                    for set in &distinct_sets {
                        for v in set {
                            bytes_values.push(v.clone());
                        }
                    }

                    // Use from_iter_values which works correctly
                    let array = BinaryViewArray::from_iter_values(
                        bytes_values.iter().map(|v| v.as_slice()),
                    );
                    Arc::new(array)
                }
                _ => return Err(datafusion_common::DataFusionError::Internal(
                    "Unsupported output type for BytesViewDistinctCountGroupsAccumulator"
                        .to_string(),
                )),
            };

        // Count elements in each set for offsets
        for set in distinct_sets {
            offsets.push(offsets.last().unwrap() + set.len() as i32);
        }

        // Create list array with the offsets
        let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(inner_array.data_type().clone(), true)),
            offset_buffer,
            inner_array,
            None,
        );

        Ok(vec![Arc::new(list_array) as _])
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        // For a single distinct value per row, create a list array with that value
        assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
        let values = ArrayRef::clone(&values[0]);

        let offsets =
            OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
        let nulls = filtered_null_mask(opt_filter, &values);
        let list_array = ListArray::new(
            Arc::new(Field::new_list_field(values.data_type().clone(), true)),
            offsets,
            values,
            nulls,
        );

        Ok(vec![Arc::new(list_array)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        let mut total_size = std::mem::size_of::<Self>();

        // Size of vector container
        total_size += std::mem::size_of::<Vec<HashSet<Vec<u8>, RandomState>>>();

        // Size of actual sets and their contents (similar to BytesDistinctCountGroupsAccumulator)
        for set in &self.distinct_sets {
            let container_size = std::mem::size_of::<HashSet<Vec<u8>, RandomState>>()
                + set.capacity() * std::mem::size_of::<Vec<u8>>();

            // Add sizes of actual byte vectors
            let content_size = set.iter().map(|v| v.capacity()).sum::<usize>();

            total_size += container_size + content_size;
        }

        total_size
    }
}

// /// A specialized GroupsAccumulator for count distinct operations with string/binary types (for Utf8 & Binary arrays)
// #[derive(Debug)]
// pub struct BytesDistinctCountGroupsAccumulator<O: OffsetSizeTrait> {
//     /// One HashSet per group to track distinct values
//     distinct_sets: Vec<HashSet<Vec<u8>, RandomState>>,
//     output_type: OutputType,
//     _phantom: PhantomData<O>,
// }

// impl<O: OffsetSizeTrait> BytesDistinctCountGroupsAccumulator<O> {
//     pub fn new(output_type: OutputType) -> Self {
//         Self {
//             distinct_sets: vec![],
//             output_type,
//             _phantom: PhantomData,
//         }
//     }

//     fn ensure_sets(&mut self, total_num_groups: usize) {
//         if self.distinct_sets.len() < total_num_groups {
//             self.distinct_sets
//                 .resize_with(total_num_groups, HashSet::default);
//         }
//     }
// }

// impl<O> GroupsAccumulator for BytesDistinctCountGroupsAccumulator<O>
// where
//     O: 'static + OffsetSizeTrait + Debug,
// {
//     fn update_batch(
//         &mut self,
//         values: &[ArrayRef],
//         group_indices: &[usize],
//         opt_filter: Option<&BooleanArray>,
//         total_num_groups: usize,
//     ) -> Result<()> {
//         assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
//         self.ensure_sets(total_num_groups);

//         if let Some(array) = values[0]
//             .as_any()
//             .downcast_ref::<GenericByteArray<GenericStringType<O>>>()
//         {
//             // String array case
//             match (array.logical_nulls(), opt_filter) {
//                 (None, None) => {
//                     for (row_idx, &group_idx) in group_indices.iter().enumerate() {
//                         if row_idx < array.len() {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//                 (Some(nulls), None) => {
//                     for (row_idx, (&group_idx, is_valid)) in
//                         group_indices.iter().zip(nulls.iter()).enumerate()
//                     {
//                         if is_valid && row_idx < array.len() {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//                 (None, Some(filter)) => {
//                     for (row_idx, (&group_idx, filter_value)) in
//                         group_indices.iter().zip(filter.iter()).enumerate()
//                     {
//                         if let Some(true) = filter_value {
//                             if row_idx < array.len() {
//                                 let value = array.value(row_idx);
//                                 self.distinct_sets[group_idx].insert(value.to_vec());
//                             }
//                         }
//                     }
//                 }
//                 (Some(nulls), Some(filter)) => {
//                     let iter = filter
//                         .iter()
//                         .zip(group_indices.iter())
//                         .zip(nulls.iter())
//                         .enumerate();

//                     for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
//                         if is_valid && filter_value == Some(true) && row_idx < array.len()
//                         {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//             }
//         } else if let Some(array) = values[0]
//             .as_any()
//             .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
//         {
//             // Binary array case
//             match (array.logical_nulls(), opt_filter) {
//                 (None, None) => {
//                     for (row_idx, &group_idx) in group_indices.iter().enumerate() {
//                         if row_idx < array.len() {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//                 (Some(nulls), None) => {
//                     for (row_idx, (&group_idx, is_valid)) in
//                         group_indices.iter().zip(nulls.iter()).enumerate()
//                     {
//                         if is_valid && row_idx < array.len() {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//                 (None, Some(filter)) => {
//                     for (row_idx, (&group_idx, filter_value)) in
//                         group_indices.iter().zip(filter.iter()).enumerate()
//                     {
//                         if let Some(true) = filter_value {
//                             if row_idx < array.len() {
//                                 let value = array.value(row_idx);
//                                 self.distinct_sets[group_idx].insert(value.to_vec());
//                             }
//                         }
//                     }
//                 }
//                 (Some(nulls), Some(filter)) => {
//                     let iter = filter
//                         .iter()
//                         .zip(group_indices.iter())
//                         .zip(nulls.iter())
//                         .enumerate();

//                     for (row_idx, ((filter_value, &group_idx), is_valid)) in iter {
//                         if is_valid && filter_value == Some(true) && row_idx < array.len()
//                         {
//                             let value = array.value(row_idx);
//                             self.distinct_sets[group_idx].insert(value.to_vec());
//                         }
//                     }
//                 }
//             }
//         } else {
//             return Err(datafusion_common::DataFusionError::Internal(format!(
//                 "Cannot process array of type {:?} with BytesDistinctCountGroupsAccumulator",
//                 values[0].data_type()
//             )));
//         }

//         Ok(())
//     }

//     fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
//         let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

//         let counts = distinct_sets
//             .iter()
//             .map(|set| set.len() as i64)
//             .collect::<Vec<_>>();

//         Ok(Arc::new(Int64Array::from(counts)))
//     }

//     fn merge_batch(
//         &mut self,
//         values: &[ArrayRef],
//         group_indices: &[usize],
//         _opt_filter: Option<&BooleanArray>,
//         total_num_groups: usize,
//     ) -> Result<()> {
//         assert_eq!(
//             values.len(),
//             1,
//             "COUNT DISTINCT merge expects a single state array"
//         );
//         self.ensure_sets(total_num_groups);

//         let list_array = as_list_array(&values[0])?;

//         // For each group in the incoming batch
//         for (i, &group_idx) in group_indices.iter().enumerate() {
//             if i < list_array.len() {
//                 let inner_array = list_array.value(i);
//                 if !inner_array.is_empty() {
//                     // Try both String and Binary types
//                     if let Some(bytes_array) = inner_array
//                         .as_any()
//                         .downcast_ref::<GenericByteArray<GenericStringType<O>>>()
//                     {
//                         for j in 0..bytes_array.len() {
//                             if !bytes_array.is_null(j) {
//                                 let value = bytes_array.value(j);
//                                 self.distinct_sets[group_idx].insert(value.to_vec());
//                             }
//                         }
//                     } else if let Some(bytes_array) = inner_array
//                         .as_any()
//                         .downcast_ref::<GenericByteArray<GenericBinaryType<O>>>()
//                     {
//                         for j in 0..bytes_array.len() {
//                             if !bytes_array.is_null(j) {
//                                 let value = bytes_array.value(j);
//                                 self.distinct_sets[group_idx].insert(value.to_vec());
//                             }
//                         }
//                     }
//                 }
//             }
//         }

//         Ok(())
//     }

//     fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
//         let distinct_sets = emit_to.take_needed(&mut self.distinct_sets);

//         let mut offsets = Vec::with_capacity(distinct_sets.len() + 1);
//         offsets.push(0);

//         // First pass to calculate total byte length and build offsets
//         let mut total_bytes_len = 0;
//         let mut value_offsets = Vec::new();
//         value_offsets.push(O::default());

//         let mut total_values = 0;
//         for set in &distinct_sets {
//             let set_len = set.len();
//             total_values += set_len;

//             for value in set {
//                 total_bytes_len += value.len();
//                 let curr_offset = *value_offsets.last().unwrap();
//                 let value_len = O::from_usize(value.len()).ok_or_else(|| {
//                     datafusion_common::DataFusionError::Internal(
//                         "Failed to convert offset".to_string(),
//                     )
//                 })?;

//                 let mut next_offset = curr_offset;
//                 next_offset += value_len;
//                 value_offsets.push(next_offset);
//             }

//             offsets.push(offsets.last().unwrap() + set_len as i32);
//         }

//         // Create buffer for all bytes concatenated
//         let mut bytes = Vec::with_capacity(total_bytes_len);
//         for set in distinct_sets {
//             for value in set {
//                 bytes.extend_from_slice(&value);
//             }
//         }

//         // Create appropriate array based on output type
//         let inner_array: ArrayRef = match self.output_type {
//             OutputType::Utf8 => {
//                 // Create a StringArray
//                 let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(value_offsets));
//                 let values_buffer = Buffer::from_vec(bytes);
//                 if O::IS_LARGE {
//                     // If O is i64, use LargeStringArray
//                     let large_string_array =
//                         arrow::array::GenericByteArray::<GenericStringType<i64>>::new(
//                             offset_buffer.try_into().unwrap(),
//                             values_buffer,
//                             None,
//                         );
//                     Arc::new(large_string_array)
//                 } else {
//                     // If O is i32, use StringArray
//                     let string_array =
//                         arrow::array::GenericByteArray::<GenericStringType<i32>>::new(
//                             offset_buffer.try_into().unwrap(),
//                             values_buffer,
//                             None,
//                         );
//                     Arc::new(string_array)
//                 }
//             }
//             OutputType::Binary => {
//                 // Create a BinaryArray
//                 let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(value_offsets));
//                 let values_buffer = Buffer::from_vec(bytes);
//                 if O::IS_LARGE {
//                     // If O is i64, use LargeBinaryArray
//                     let large_binary_array =
//                         arrow::array::GenericByteArray::<GenericBinaryType<i64>>::new(
//                             offset_buffer.try_into().unwrap(),
//                             values_buffer,
//                             None,
//                         );
//                     Arc::new(large_binary_array)
//                 } else {
//                     // If O is i32, use BinaryArray
//                     let binary_array =
//                         arrow::array::GenericByteArray::<GenericBinaryType<i32>>::new(
//                             offset_buffer.try_into().unwrap(),
//                             values_buffer,
//                             None,
//                         );
//                     Arc::new(binary_array)
//                 }
//             }
//             _ => {
//                 return Err(datafusion_common::DataFusionError::Internal(
//                     "Unsupported output type for BytesDistinctCountGroupsAccumulator"
//                         .to_string(),
//                 ))
//             }
//         };

//         // Create list array with the offsets
//         let offset_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
//         let list_array = ListArray::new(
//             Arc::new(Field::new_list_field(inner_array.data_type().clone(), true)),
//             offset_buffer,
//             inner_array,
//             None,
//         );

//         Ok(vec![Arc::new(list_array) as _])
//     }

//     fn convert_to_state(
//         &self,
//         values: &[ArrayRef],
//         opt_filter: Option<&BooleanArray>,
//     ) -> Result<Vec<ArrayRef>> {
//         // For a single distinct value per row, create a list array with that value
//         assert_eq!(values.len(), 1, "COUNT DISTINCT expects a single argument");
//         let values = ArrayRef::clone(&values[0]);

//         let offsets =
//             OffsetBuffer::new(ScalarBuffer::from_iter(0..values.len() as i32 + 1));
//         let nulls = filtered_null_mask(opt_filter, &values);
//         let list_array = ListArray::new(
//             Arc::new(Field::new_list_field(values.data_type().clone(), true)),
//             offsets,
//             values,
//             nulls,
//         );

//         Ok(vec![Arc::new(list_array)])
//     }

//     fn supports_convert_to_state(&self) -> bool {
//         true
//     }

//     fn size(&self) -> usize {
//         let mut total_size = std::mem::size_of::<Self>();

//         // Size of vector container
//         total_size += std::mem::size_of::<Vec<HashSet<Vec<u8>, RandomState>>>();

//         // Size of actual sets and their contents
//         for set in &self.distinct_sets {
//             let container_size = std::mem::size_of::<HashSet<Vec<u8>, RandomState>>()
//                 + set.capacity() * std::mem::size_of::<Vec<u8>>();

//             // Add sizes of actual byte vectors
//             let content_size = set.iter().map(|v| v.capacity()).sum::<usize>();

//             total_size += container_size + content_size;
//         }

//         total_size
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, NullArray, StringArray};

    #[test]
    fn count_accumulator_nulls() -> Result<()> {
        let mut accumulator = CountAccumulator::new();
        accumulator.update_batch(&[Arc::new(NullArray::new(10))])?;
        assert_eq!(accumulator.evaluate()?, ScalarValue::Int64(Some(0)));
        Ok(())
    }

    #[test]
    fn test_distinct_count_groups_basic() -> Result<()> {
        let mut accumulator = DistinctCountGroupsAccumulator::new(DataType::Int32);
        let values = vec![Arc::new(Int32Array::from(vec![1, 2, 1, 3, 2, 1])) as ArrayRef];

        // 3 groups
        let group_indices = vec![0, 1, 0, 2, 1, 0];
        accumulator.update_batch(&values, &group_indices, None, 3)?;

        let result = accumulator.evaluate(EmitTo::All)?;
        let counts = result.as_primitive::<Int64Type>();

        // Group 0 should have distinct values [1] (1 appears 3 times) -> count 1
        // Group 1 should have distinct values [2] (2 appears 2 times) -> count 1
        // Group 2 should have distinct values [3] (3 appears 1 time) -> count 1
        assert_eq!(counts.value(0), 1); // Group 0: distinct values 1, 1, 1 -> count 1
        assert_eq!(counts.value(1), 1); // Group 1: distinct values 2, 2 -> count 1
        assert_eq!(counts.value(2), 1); // Group 2: distinct values 3 -> count 1

        Ok(())
    }

    #[test]
    fn test_distinct_count_groups_with_filter() -> Result<()> {
        let mut accumulator = DistinctCountGroupsAccumulator::new(DataType::Utf8);
        let values = vec![
            Arc::new(StringArray::from(vec!["a", "b", "a", "c", "b", "d"])) as ArrayRef,
        ];
        // 2 groups
        let group_indices = vec![0, 0, 0, 1, 1, 1];
        let filter = BooleanArray::from(vec![true, true, false, true, false, true]);
        accumulator.update_batch(&values, &group_indices, Some(&filter), 2)?;

        let result = accumulator.evaluate(EmitTo::All)?;
        let counts = result.as_primitive::<Int64Type>();

        // Group 0 should have ["a", "b"] (filter excludes the second "a")
        // Group 1 should have ["c", "d"] (filter excludes "b")
        assert_eq!(counts.value(0), 2);
        assert_eq!(counts.value(1), 2);

        Ok(())
    }
}
