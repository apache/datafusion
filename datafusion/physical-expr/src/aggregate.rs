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

pub(crate) mod groups_accumulator {
    #[allow(unused_imports)]
    pub(crate) mod accumulate {
        pub use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::NullState;
    }
    pub use datafusion_functions_aggregate_common::aggregate::groups_accumulator::{
        accumulate::NullState, GroupsAccumulatorAdapter,
    };
}
pub(crate) mod stats {
    pub use datafusion_functions_aggregate_common::stats::StatsType;
}
pub mod utils {
    pub use datafusion_functions_aggregate_common::utils::{
        get_accum_scalar_values_as_arrays, get_sort_options, ordering_fields,
        DecimalAverager, Hashable,
    };
}

use std::any::Any;
use std::fmt::Debug;
use std::ops::Shr;
use std::sync::Arc;

use crate::expressions::Column;

use arrow::array::{ArrowPrimitiveType, Int32Array, PrimitiveArray, RecordBatch};
use arrow::compute::{unary, SortOptions};
use arrow::datatypes::{
    ArrowNativeType, DataType, FieldRef, Int32Type, Schema, SchemaRef, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use datafusion_common::cast::as_primitive_array;
use datafusion_common::{internal_err, not_impl_err, Result, ScalarValue};
use datafusion_expr::{AggregateUDF, ColumnarValue, ReversedUDAF, SetMonotonicity};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_expr_common::type_coercion::aggregates::check_arg_count;
use datafusion_functions_aggregate_common::accumulator::{
    AccumulatorArgs, StateFieldsArgs,
};
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

/// Builder for physical [`AggregateFunctionExpr`]
///
/// `AggregateFunctionExpr` contains the information necessary to call
/// an aggregate expression.
#[derive(Debug, Clone)]
pub struct AggregateExprBuilder {
    fun: Arc<AggregateUDF>,
    /// Physical expressions of the aggregate function
    args: Vec<Arc<dyn PhysicalExpr>>,
    alias: Option<String>,
    /// A human readable name
    human_display: String,
    /// Arrow Schema for the aggregate function
    schema: SchemaRef,
    /// The physical order by expressions
    order_bys: Vec<PhysicalSortExpr>,
    /// Whether to ignore null values
    ignore_nulls: bool,
    /// Whether is distinct aggregate function
    is_distinct: bool,
    /// Whether the expression is reversed
    is_reversed: bool,
}

impl AggregateExprBuilder {
    pub fn new(fun: Arc<AggregateUDF>, args: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            fun,
            args,
            alias: None,
            human_display: String::default(),
            schema: Arc::new(Schema::empty()),
            order_bys: vec![],
            ignore_nulls: false,
            is_distinct: false,
            is_reversed: false,
        }
    }

    /// Constructs an `AggregateFunctionExpr` from the builder
    ///
    /// Note that an [`Self::alias`] must be provided before calling this method.
    ///
    /// # Example: Create an [`AggregateUDF`]
    ///
    /// In the following example, [`AggregateFunctionExpr`] will be built using [`AggregateExprBuilder`]
    /// which provides a build function. Full example could be accessed from the source file.
    ///
    /// ```
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{DataType, FieldRef};
    /// # use datafusion_common::{Result, ScalarValue};
    /// # use datafusion_expr::{col, ColumnarValue, Documentation, Signature, Volatility, Expr};
    /// # use datafusion_expr::{AggregateUDFImpl, AggregateUDF, Accumulator, function::{AccumulatorArgs, StateFieldsArgs}};
    /// # use arrow::datatypes::Field;
    /// #
    /// # #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    /// # struct FirstValueUdf {
    /// #     signature: Signature,
    /// # }
    /// #
    /// # impl FirstValueUdf {
    /// #     fn new() -> Self {
    /// #         Self {
    /// #             signature: Signature::any(1, Volatility::Immutable),
    /// #         }
    /// #     }
    /// # }
    /// #
    /// # impl AggregateUDFImpl for FirstValueUdf {
    /// #     fn as_any(&self) -> &dyn Any {
    /// #         unimplemented!()
    /// #     }
    /// #
    /// #     fn name(&self) -> &str {
    /// #         unimplemented!()
    /// #     }
    /// #
    /// #     fn signature(&self) -> &Signature {
    /// #         unimplemented!()
    /// #     }
    /// #
    /// #     fn return_type(&self, args: &[DataType]) -> Result<DataType> {
    /// #         unimplemented!()
    /// #     }
    /// #
    /// #     fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
    /// #         unimplemented!()
    /// #         }
    /// #     
    /// #     fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
    /// #         unimplemented!()
    /// #     }
    /// #
    /// #     fn documentation(&self) -> Option<&Documentation> {
    /// #         unimplemented!()
    /// #     }
    /// # }
    /// #
    /// # let first_value = AggregateUDF::from(FirstValueUdf::new());
    /// # let expr = first_value.call(vec![col("a")]);
    /// #
    /// # use datafusion_physical_expr::expressions::Column;
    /// # use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    /// # use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    /// # use datafusion_physical_expr::expressions::PhysicalSortExpr;
    /// # use datafusion_physical_expr::PhysicalSortRequirement;
    /// #
    /// fn build_aggregate_expr() -> Result<()> {
    ///     let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
    ///     let order_by = vec![PhysicalSortExpr {
    ///         expr: Arc::new(Column::new("x", 1)) as Arc<dyn PhysicalExpr>,
    ///         options: Default::default(),
    ///     }];
    ///
    ///     let first_value = AggregateUDF::from(FirstValueUdf::new());
    ///
    ///     let aggregate_expr = AggregateExprBuilder::new(
    ///         Arc::new(first_value),
    ///         args
    ///     )
    ///     .order_by(order_by)
    ///     .alias("first_a_by_x")
    ///     .ignore_nulls()
    ///     .build()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// This creates a physical expression equivalent to SQL:
    /// `first_value(a ORDER BY x) IGNORE NULLS AS first_a_by_x`
    pub fn build(self) -> Result<AggregateFunctionExpr> {
        let Self {
            fun,
            args,
            alias,
            human_display,
            schema,
            order_bys,
            ignore_nulls,
            is_distinct,
            is_reversed,
        } = self;
        if args.is_empty() {
            return internal_err!("args should not be empty");
        }

        let ordering_types = order_bys
            .iter()
            .map(|e| e.expr.data_type(&schema))
            .collect::<Result<Vec<_>>>()?;

        let ordering_fields = utils::ordering_fields(&order_bys, &ordering_types);

        let input_exprs_fields = args
            .iter()
            .map(|arg| arg.return_field(&schema))
            .collect::<Result<Vec<_>>>()?;

        check_arg_count(
            fun.name(),
            &input_exprs_fields,
            &fun.signature().type_signature,
        )?;

        let return_field = fun.return_field(&input_exprs_fields)?;
        let is_nullable = fun.is_nullable();
        let name = match alias {
            None => {
                return internal_err!(
                    "AggregateExprBuilder::alias must be provided prior to calling build"
                )
            }
            Some(alias) => alias,
        };

        Ok(AggregateFunctionExpr {
            fun: Arc::unwrap_or_clone(fun),
            args,
            return_field,
            name,
            human_display,
            schema: Arc::unwrap_or_clone(schema),
            order_bys,
            ignore_nulls,
            ordering_fields,
            is_distinct,
            input_fields: input_exprs_fields,
            is_reversed,
            is_nullable,
        })
    }

    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn human_display(mut self, name: String) -> Self {
        self.human_display = name;
        self
    }

    pub fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    pub fn order_by(mut self, order_bys: Vec<PhysicalSortExpr>) -> Self {
        self.order_bys = order_bys;
        self
    }

    pub fn reversed(mut self) -> Self {
        self.is_reversed = true;
        self
    }

    pub fn with_reversed(mut self, is_reversed: bool) -> Self {
        self.is_reversed = is_reversed;
        self
    }

    pub fn distinct(mut self) -> Self {
        self.is_distinct = true;
        self
    }

    pub fn with_distinct(mut self, is_distinct: bool) -> Self {
        self.is_distinct = is_distinct;
        self
    }

    pub fn ignore_nulls(mut self) -> Self {
        self.ignore_nulls = true;
        self
    }

    pub fn with_ignore_nulls(mut self, ignore_nulls: bool) -> Self {
        self.ignore_nulls = ignore_nulls;
        self
    }
}

/// Physical aggregate expression of a UDAF.
///
/// Instances are constructed via [`AggregateExprBuilder`].
#[derive(Debug, Clone)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Output / return field of this aggregate
    return_field: FieldRef,
    /// Output column name that this expression creates
    name: String,
    /// Simplified name for `tree` explain.
    human_display: String,
    schema: Schema,
    // The physical order by expressions
    order_bys: Vec<PhysicalSortExpr>,
    // Whether to ignore null values
    ignore_nulls: bool,
    // fields used for order sensitive aggregation functions
    ordering_fields: Vec<FieldRef>,
    is_distinct: bool,
    is_reversed: bool,
    input_fields: Vec<FieldRef>,
    is_nullable: bool,
}

impl AggregateFunctionExpr {
    /// Return the `AggregateUDF` used by this `AggregateFunctionExpr`
    pub fn fun(&self) -> &AggregateUDF {
        &self.fun
    }

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    pub fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    /// Human readable name such as `"MIN(c2)"`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Simplified name for `tree` explain.
    pub fn human_display(&self) -> &str {
        &self.human_display
    }

    /// Return if the aggregation is distinct
    pub fn is_distinct(&self) -> bool {
        self.is_distinct
    }

    /// Return if the aggregation ignores nulls
    pub fn ignore_nulls(&self) -> bool {
        self.ignore_nulls
    }

    /// Return if the aggregation is reversed
    pub fn is_reversed(&self) -> bool {
        self.is_reversed
    }

    /// Return if the aggregation is nullable
    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    /// the field of the final result of this aggregation.
    pub fn field(&self) -> FieldRef {
        self.return_field
            .as_ref()
            .clone()
            .with_name(&self.name)
            .into()
    }

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    pub fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let acc_args = AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            order_bys: self.order_bys.as_ref(),
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };

        self.fun.accumulator(acc_args)
    }

    /// the field of the final result of this aggregation.
    pub fn state_fields(&self) -> Result<Vec<FieldRef>> {
        let args = StateFieldsArgs {
            name: &self.name,
            input_fields: &self.input_fields,
            return_field: Arc::clone(&self.return_field),
            ordering_fields: &self.ordering_fields,
            is_distinct: self.is_distinct,
        };

        self.fun.state_fields(args)
    }

    /// Returns the ORDER BY expressions for the aggregate function.
    pub fn order_bys(&self) -> &[PhysicalSortExpr] {
        if self.order_sensitivity().is_insensitive() {
            &[]
        } else {
            &self.order_bys
        }
    }

    /// Indicates whether aggregator can produce the correct result with any
    /// arbitrary input ordering. By default, we assume that aggregate expressions
    /// are order insensitive.
    pub fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        if self.order_bys.is_empty() {
            AggregateOrderSensitivity::Insensitive
        } else {
            // If there is an ORDER BY clause, use the sensitivity of the implementation:
            self.fun.order_sensitivity()
        }
    }

    /// Sets the indicator whether ordering requirements of the aggregator is
    /// satisfied by its input. If this is not the case, aggregators with order
    /// sensitivity `AggregateOrderSensitivity::Beneficial` can still produce
    /// the correct result with possibly more work internally.
    ///
    /// # Returns
    ///
    /// Returns `Ok(Some(updated_expr))` if the process completes successfully.
    /// If the expression can benefit from existing input ordering, but does
    /// not implement the method, returns an error. Order insensitive and hard
    /// requirement aggregators return `Ok(None)`.
    pub fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<AggregateFunctionExpr>> {
        let Some(updated_fn) = self
            .fun
            .clone()
            .with_beneficial_ordering(beneficial_ordering)?
        else {
            return Ok(None);
        };

        AggregateExprBuilder::new(Arc::new(updated_fn), self.args.to_vec())
            .order_by(self.order_bys.clone())
            .schema(Arc::new(self.schema.clone()))
            .alias(self.name().to_string())
            .with_ignore_nulls(self.ignore_nulls)
            .with_distinct(self.is_distinct)
            .with_reversed(self.is_reversed)
            .build()
            .map(Some)
    }

    /// Creates accumulator implementation that supports retract
    pub fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let args = AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            order_bys: self.order_bys.as_ref(),
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };

        let accumulator = self.fun.create_sliding_accumulator(args)?;

        // Accumulators that have window frame startings different
        // than `UNBOUNDED PRECEDING`, such as `1 PRECEDING`, need to
        // implement retract_batch method in order to run correctly
        // currently in DataFusion.
        //
        // If this `retract_batches` is not present, there is no way
        // to calculate result correctly. For example, the query
        //
        // ```sql
        // SELECT
        //  SUM(a) OVER(ORDER BY a ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum_a
        // FROM
        //  t
        // ```
        //
        // 1. First sum value will be the sum of rows between `[0, 1)`,
        //
        // 2. Second sum value will be the sum of rows between `[0, 2)`
        //
        // 3. Third sum value will be the sum of rows between `[1, 3)`, etc.
        //
        // Since the accumulator keeps the running sum:
        //
        // 1. First sum we add to the state sum value between `[0, 1)`
        //
        // 2. Second sum we add to the state sum value between `[1, 2)`
        // (`[0, 1)` is already in the state sum, hence running sum will
        // cover `[0, 2)` range)
        //
        // 3. Third sum we add to the state sum value between `[2, 3)`
        // (`[0, 2)` is already in the state sum).  Also we need to
        // retract values between `[0, 1)` by this way we can obtain sum
        // between [1, 3) which is indeed the appropriate range.
        //
        // When we use `UNBOUNDED PRECEDING` in the query starting
        // index will always be 0 for the desired range, and hence the
        // `retract_batch` method will not be called. In this case
        // having retract_batch is not a requirement.
        //
        // This approach is a a bit different than window function
        // approach. In window function (when they use a window frame)
        // they get all the desired range during evaluation.
        if !accumulator.supports_retract_batch() {
            return not_impl_err!(
                "Aggregate can not be used as a sliding accumulator because \
                     `retract_batch` is not implemented: {}",
                self.name
            );
        }
        Ok(accumulator)
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator`] will be called.
    pub fn groups_accumulator_supported(&self) -> bool {
        let args = AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            order_bys: self.order_bys.as_ref(),
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };
        self.fun.groups_accumulator_supported(args)
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    pub fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        let args = AccumulatorArgs {
            return_field: Arc::clone(&self.return_field),
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            order_bys: self.order_bys.as_ref(),
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };
        self.fun.create_groups_accumulator(args)
    }

    /// Construct an expression that calculates the aggregate in reverse.
    /// Typically the "reverse" expression is itself (e.g. SUM, COUNT).
    /// For aggregates that do not support calculation in reverse,
    /// returns None (which is the default value).
    pub fn reverse_expr(&self) -> Option<AggregateFunctionExpr> {
        match self.fun.reverse_udf() {
            ReversedUDAF::NotSupported => None,
            ReversedUDAF::Identical => Some(self.clone()),
            ReversedUDAF::Reversed(reverse_udf) => {
                let mut name = self.name().to_string();
                // If the function is changed, we need to reverse order_by clause as well
                // i.e. First(a order by b asc null first) -> Last(a order by b desc null last)
                if self.fun().name() != reverse_udf.name() {
                    replace_order_by_clause(&mut name);
                }
                replace_fn_name_clause(&mut name, self.fun.name(), reverse_udf.name());

                AggregateExprBuilder::new(reverse_udf, self.args.to_vec())
                    .order_by(self.order_bys.iter().map(|e| e.reverse()).collect())
                    .schema(Arc::new(self.schema.clone()))
                    .alias(name)
                    .with_ignore_nulls(self.ignore_nulls)
                    .with_distinct(self.is_distinct)
                    .with_reversed(!self.is_reversed)
                    .build()
                    .ok()
            }
        }
    }

    /// Returns all expressions used in the [`AggregateFunctionExpr`].
    /// These expressions are  (1)function arguments, (2) order by expressions.
    pub fn all_expressions(&self) -> AggregatePhysicalExpressions {
        let args = self.expressions();
        let order_by_exprs = self
            .order_bys()
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect();
        AggregatePhysicalExpressions {
            args,
            order_by_exprs,
        }
    }

    /// Rewrites [`AggregateFunctionExpr`], with new expressions given. The argument should be consistent
    /// with the return value of the [`AggregateFunctionExpr::all_expressions`] method.
    /// Returns `Some(Arc<dyn AggregateExpr>)` if re-write is supported, otherwise returns `None`.
    pub fn with_new_expressions(
        &self,
        args: Vec<Arc<dyn PhysicalExpr>>,
        order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<AggregateFunctionExpr> {
        if args.len() != self.args.len()
            || (self.order_sensitivity() != AggregateOrderSensitivity::Insensitive
                && order_by_exprs.len() != self.order_bys.len())
        {
            return None;
        }

        let new_order_bys = self
            .order_bys
            .iter()
            .zip(order_by_exprs)
            .map(|(req, new_expr)| PhysicalSortExpr {
                expr: new_expr,
                options: req.options,
            })
            .collect();

        Some(AggregateFunctionExpr {
            fun: self.fun.clone(),
            args,
            return_field: Arc::clone(&self.return_field),
            name: self.name.clone(),
            // TODO: Human name should be updated after re-write to not mislead
            human_display: self.human_display.clone(),
            schema: self.schema.clone(),
            order_bys: new_order_bys,
            ignore_nulls: self.ignore_nulls,
            ordering_fields: self.ordering_fields.clone(),
            is_distinct: self.is_distinct,
            is_reversed: false,
            input_fields: self.input_fields.clone(),
            is_nullable: self.is_nullable,
        })
    }

    /// If this function is max, return (output_field, true)
    /// if the function is min, return (output_field, false)
    /// otherwise return None (the default)
    ///
    /// output_field is the name of the column produced by this aggregate
    ///
    /// Note: this is used to use special aggregate implementations in certain conditions
    pub fn get_minmax_desc(&self) -> Option<(FieldRef, bool)> {
        self.fun.is_descending().map(|flag| (self.field(), flag))
    }

    /// Returns default value of the function given the input is Null
    /// Most of the aggregate function return Null if input is Null,
    /// while `count` returns 0 if input is Null
    pub fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.fun.default_value(data_type)
    }

    /// Indicates whether the aggregation function is monotonic as a set
    /// function. See [`SetMonotonicity`] for details.
    pub fn set_monotonicity(&self) -> SetMonotonicity {
        let field = self.field();
        let data_type = field.data_type();
        self.fun.inner().set_monotonicity(data_type)
    }

    /// Returns `PhysicalSortExpr` based on the set monotonicity of the function.
    pub fn get_result_ordering(&self, aggr_func_idx: usize) -> Option<PhysicalSortExpr> {
        // If the aggregate expressions are set-monotonic, the output data is
        // naturally ordered with it per group or partition.
        let monotonicity = self.set_monotonicity();
        if monotonicity == SetMonotonicity::NotMonotonic {
            return None;
        }
        let expr = Arc::new(Column::new(self.name(), aggr_func_idx));
        let options =
            SortOptions::new(monotonicity == SetMonotonicity::Decreasing, false);
        Some(PhysicalSortExpr { expr, options })
    }
}

/// Stores the physical expressions used inside the `AggregateExpr`.
pub struct AggregatePhysicalExpressions {
    /// Aggregate function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// Order by expressions
    pub order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl PartialEq for AggregateFunctionExpr {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.return_field == other.return_field
            && self.fun == other.fun
            && self.args.len() == other.args.len()
            && self
                .args
                .iter()
                .zip(other.args.iter())
                .all(|(this_arg, other_arg)| this_arg.eq(other_arg))
    }
}

fn replace_order_by_clause(order_by: &mut String) {
    let suffixes = [
        (" DESC NULLS FIRST]", " ASC NULLS LAST]"),
        (" ASC NULLS FIRST]", " DESC NULLS LAST]"),
        (" DESC NULLS LAST]", " ASC NULLS FIRST]"),
        (" ASC NULLS LAST]", " DESC NULLS FIRST]"),
    ];

    if let Some(start) = order_by.find("ORDER BY [") {
        if let Some(end) = order_by[start..].find(']') {
            let order_by_start = start + 9;
            let order_by_end = start + end;

            let column_order = &order_by[order_by_start..=order_by_end];
            for (suffix, replacement) in suffixes {
                if column_order.ends_with(suffix) {
                    let new_order = column_order.replace(suffix, replacement);
                    order_by.replace_range(order_by_start..=order_by_end, &new_order);
                    break;
                }
            }
        }
    }
}

fn replace_fn_name_clause(aggr_name: &mut String, fn_name_old: &str, fn_name_new: &str) {
    *aggr_name = aggr_name.replace(fn_name_old, fn_name_new);
}

/// Represents a GROUPING physical expression
///
/// The GROUPING function returns a bitmask indicating which columns are aggregated
/// in a GROUP BY GROUPING SETS, ROLLUP, or CUBE query.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct GroupingExpr {
    name: String,
    /// A human readable name
    human_display: String,
    /// The grouping id value
    indices: Option<Vec<i32>>,
}

impl GroupingExpr {
    /// Create a new GROUPING physical expression
    pub fn new(name: String, human_display: String, indices: Option<Vec<i32>>) -> Self {
        Self {
            name,
            human_display,
            indices,
        }
    }

    /// Get the indices
    pub fn indices(&self) -> &Option<Vec<i32>> {
        &self.indices
    }

    fn grouping<T: ArrowPrimitiveType>(
        &self,
        grouping_id_col: &PrimitiveArray<T>,
        indices: &[i32],
    ) -> PrimitiveArray<Int32Type>
    where
        T::Native: Shr<i32, Output = T::Native>,
    {
        unary::<_, _, Int32Type>(grouping_id_col, |grouping_id| {
            let mut result = 0i32;
            for index in indices.iter() {
                let bit = (grouping_id >> *index).as_usize() & 1;
                result = (result << 1) | (bit as i32);
            }
            result
        })
    }

    pub fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        PhysicalExpr::evaluate(self, batch)
    }
}

impl std::fmt::Display for GroupingExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Some(indices) = &self.indices {
            write!(
                f,
                "GROUPING({})",
                indices
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(",")
            )?;
        } else {
            write!(f, "0")?;
        }

        write!(f, " AS {}", self.name)
    }
}

impl PhysicalExpr for GroupingExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let Some(indices) = &self.indices else {
            return Ok(ColumnarValue::Array(Arc::new(Int32Array::from(
                std::vec![0; batch.num_rows()],
            ))));
        };
        // Get the grouping_id column from the batch
        let Some(grouping_id_col) = batch.column_by_name(INTERNAL_GROUPING_ID) else {
            return internal_err!(
                "GROUPING expression requires {} column in the schema",
                INTERNAL_GROUPING_ID
            );
        };

        match grouping_id_col.data_type() {
            DataType::UInt8 => {
                let result = self
                    .grouping(as_primitive_array::<UInt8Type>(grouping_id_col)?, indices);
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::UInt16 => {
                let result = self.grouping(
                    as_primitive_array::<UInt16Type>(grouping_id_col)?,
                    indices,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::UInt32 => {
                let result = self.grouping(
                    as_primitive_array::<UInt32Type>(grouping_id_col)?,
                    indices,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            DataType::UInt64 => {
                let result = self.grouping(
                    as_primitive_array::<UInt64Type>(grouping_id_col)?,
                    indices,
                );
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
            _ => {
                internal_err!(
                    "GROUPING expression requires a primitive array, but got {}",
                    grouping_id_col.data_type()
                )
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{self}"))
    }

    fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::new(arrow::datatypes::Field::new(
            std::format!("{self}"),
            self.data_type(input_schema)?,
            self.nullable(input_schema)?,
        )))
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &arrow::array::BooleanArray,
    ) -> Result<ColumnarValue> {
        let tmp_batch = arrow::compute::filter_record_batch(batch, selection)?;

        let tmp_result = self.evaluate(&tmp_batch)?;

        if batch.num_rows() == tmp_batch.num_rows() {
            // All values from the `selection` filter are true.
            Ok(tmp_result)
        } else if let ColumnarValue::Array(a) = tmp_result {
            datafusion_physical_expr_common::utils::scatter(selection, a.as_ref())
                .map(ColumnarValue::Array)
        } else if let ColumnarValue::Scalar(ScalarValue::Boolean(value)) = &tmp_result {
            // When the scalar is true or false, skip the scatter process
            if let Some(v) = value {
                if *v {
                    Ok(ColumnarValue::from(
                        Arc::new(selection.clone()) as arrow::array::ArrayRef
                    ))
                } else {
                    Ok(tmp_result)
                }
            } else {
                let array =
                    arrow::array::BooleanArray::from(std::vec![None; batch.num_rows()]);
                datafusion_physical_expr_common::utils::scatter(selection, &array)
                    .map(ColumnarValue::Array)
            }
        } else {
            Ok(tmp_result)
        }
    }

    fn evaluate_bounds(
        &self,
        _children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<datafusion_expr::interval_arithmetic::Interval> {
        not_impl_err!("Not implemented for {self}")
    }

    fn propagate_constraints(
        &self,
        _interval: &datafusion_expr::interval_arithmetic::Interval,
        _children: &[&datafusion_expr::interval_arithmetic::Interval],
    ) -> Result<Option<Vec<datafusion_expr::interval_arithmetic::Interval>>> {
        Ok(Some(std::vec![]))
    }

    fn evaluate_statistics(
        &self,
        children: &[&datafusion_expr::statistics::Distribution],
    ) -> Result<datafusion_expr::statistics::Distribution> {
        let children_ranges = children
            .iter()
            .map(|c| c.range())
            .collect::<Result<Vec<_>>>()?;
        let children_ranges_refs = children_ranges.iter().collect::<Vec<_>>();
        let output_interval = self.evaluate_bounds(children_ranges_refs.as_slice())?;
        let dt = output_interval.data_type();
        if dt.eq(&DataType::Boolean) {
            let p = if output_interval
                .eq(&datafusion_expr::interval_arithmetic::Interval::CERTAINLY_TRUE)
            {
                ScalarValue::new_one(&dt)
            } else if output_interval
                .eq(&datafusion_expr::interval_arithmetic::Interval::CERTAINLY_FALSE)
            {
                ScalarValue::new_zero(&dt)
            } else {
                ScalarValue::try_from(&dt)
            }?;
            datafusion_expr::statistics::Distribution::new_bernoulli(p)
        } else {
            datafusion_expr::statistics::Distribution::new_from_interval(output_interval)
        }
    }

    fn propagate_statistics(
        &self,
        parent: &datafusion_expr::statistics::Distribution,
        children: &[&datafusion_expr::statistics::Distribution],
    ) -> Result<Option<Vec<datafusion_expr::statistics::Distribution>>> {
        let children_ranges = children
            .iter()
            .map(|c| c.range())
            .collect::<Result<Vec<_>>>()?;
        let children_ranges_refs = children_ranges.iter().collect::<Vec<_>>();
        let parent_range = parent.range()?;
        let Some(propagated_children) =
            self.propagate_constraints(&parent_range, children_ranges_refs.as_slice())?
        else {
            return Ok(None);
        };
        itertools::izip!(propagated_children.into_iter(), children_ranges, children)
            .map(|(new_interval, old_interval, child)| {
                if new_interval == old_interval {
                    // We weren't able to narrow the range, preserve the old statistics.
                    Ok((*child).clone())
                } else if new_interval.data_type().eq(&DataType::Boolean) {
                    let dt = old_interval.data_type();
                    let p = if new_interval.eq(&datafusion_expr::interval_arithmetic::Interval::CERTAINLY_TRUE) {
                        ScalarValue::new_one(&dt)
                    } else if new_interval.eq(&datafusion_expr::interval_arithmetic::Interval::CERTAINLY_FALSE) {
                        ScalarValue::new_zero(&dt)
                    } else {
                        std::unreachable!("Given that we have a range reduction for a boolean interval, we should have certainty")
                    }?;
                    datafusion_expr::statistics::Distribution::new_bernoulli(p)
                } else {
                    datafusion_expr::statistics::Distribution::new_from_interval(new_interval)
                }
            })
            .collect::<Result<_>>()
            .map(Some)
    }

    fn get_properties(
        &self,
        _children: &[datafusion_expr::sort_properties::ExprProperties],
    ) -> Result<datafusion_expr::sort_properties::ExprProperties> {
        Ok(datafusion_expr::sort_properties::ExprProperties::new_unknown())
    }

    fn snapshot(&self) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // By default, we return None to indicate that this PhysicalExpr does not
        // have any dynamic references or state.
        // This is a safe default behavior.
        Ok(None)
    }

    fn snapshot_generation(&self) -> u64 {
        // By default, we return 0 to indicate that this PhysicalExpr does not
        // have any dynamic references or state.
        // Since the recursive algorithm XORs the generations of all children the overall
        // generation will be 0 if no children have a non-zero generation, meaning that
        // static expressions will always return 0.
        0
    }

    fn is_volatile_node(&self) -> bool {
        false
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum AggregateExpr {
    GroupingExpr(Arc<GroupingExpr>),
    AggregateFunctionExpr(Arc<AggregateFunctionExpr>),
}

impl AggregateExpr {
    pub fn get_minmax_desc(&self) -> Option<(FieldRef, bool)> {
        match self {
            AggregateExpr::AggregateFunctionExpr(expr) => expr.get_minmax_desc(),
            _ => None,
        }
    }

    pub fn order_bys(&self) -> &[PhysicalSortExpr] {
        match self {
            AggregateExpr::AggregateFunctionExpr(expr) => expr.order_bys(),
            _ => &[],
        }
    }

    pub fn name(&self) -> &str {
        match self {
            AggregateExpr::GroupingExpr(expr) => &expr.name,
            AggregateExpr::AggregateFunctionExpr(expr) => &expr.name,
        }
    }

    pub fn human_display(&self) -> &str {
        match self {
            AggregateExpr::GroupingExpr(expr) => &expr.human_display,
            AggregateExpr::AggregateFunctionExpr(expr) => &expr.human_display,
        }
    }
}

impl From<Arc<AggregateFunctionExpr>> for AggregateExpr {
    fn from(expr: Arc<AggregateFunctionExpr>) -> Self {
        AggregateExpr::AggregateFunctionExpr(expr)
    }
}

impl From<Arc<GroupingExpr>> for AggregateExpr {
    fn from(expr: Arc<GroupingExpr>) -> Self {
        AggregateExpr::GroupingExpr(expr)
    }
}

/// Internal column used when the aggregation is a grouping set.
///
/// This column contains a bitmask where each bit represents a grouping
/// expression. The least significant bit corresponds to the rightmost
/// grouping expression. A bit value of 0 indicates that the corresponding
/// column is included in the grouping set, while a value of 1 means it is excluded.
///
/// For example, for the grouping expressions CUBE(a, b), the grouping ID
/// column will have the following values:
///     0b00: Both `a` and `b` are included
///     0b01: `b` is excluded
///     0b10: `a` is excluded
///     0b11: Both `a` and `b` are excluded
///
/// This internal column is necessary because excluded columns are replaced
/// with `NULL` values. To handle these cases correctly, we must distinguish
/// between an actual `NULL` value in a column and a column being excluded from the set.
pub const INTERNAL_GROUPING_ID: &str = "__grouping_id";
