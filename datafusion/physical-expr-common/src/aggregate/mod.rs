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

use std::fmt::Debug;
use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use datafusion_common::exec_err;
use datafusion_common::{internal_err, not_impl_err, DFSchema, Result};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::type_coercion::aggregates::check_arg_count;
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::ReversedUDAF;
use datafusion_expr::{
    function::AccumulatorArgs, Accumulator, AggregateUDF, Expr, GroupsAccumulator,
};

use crate::physical_expr::PhysicalExpr;
use crate::sort_expr::{LexOrdering, PhysicalSortExpr};
use crate::utils::reverse_order_bys;

use self::utils::down_cast_any_ref;

pub mod count_distinct;
pub mod groups_accumulator;
pub mod merge_arrays;
pub mod stats;
pub mod tdigest;
pub mod utils;

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
///
/// `input_exprs` and `sort_exprs` are used for customizing Accumulator
/// whose behavior depends on arguments such as the `ORDER BY`.
///
/// For example to call `ARRAY_AGG(x ORDER BY y)` would pass `y` to `sort_exprs`, `x` to `input_exprs`
///
/// `input_exprs` and `sort_exprs` are used for customizing Accumulator as the arguments in `AccumulatorArgs`,
/// if you don't need them it is fine to pass empty slice `&[]`.
///
/// `is_reversed` is used to indicate whether the aggregation is running in reverse order,
/// it could be used to hint Accumulator to accumulate in the reversed order,
/// you can just set to false if you are not reversing expression
///
/// You can also create expression by [`AggregateExprBuilder`]
#[allow(clippy::too_many_arguments)]
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_exprs: &[Expr],
    sort_exprs: &[Expr],
    ordering_req: &[PhysicalSortExpr],
    schema: &Schema,
    name: impl Into<String>,
    ignore_nulls: bool,
    is_distinct: bool,
) -> Result<Arc<dyn AggregateExpr>> {
    let mut builder =
        AggregateExprBuilder::new(Arc::new(fun.clone()), input_phy_exprs.to_vec());
    builder = builder.sort_exprs(sort_exprs.to_vec());
    builder = builder.order_by(ordering_req.to_vec());
    builder = builder.logical_exprs(input_exprs.to_vec());
    builder = builder.schema(Arc::new(schema.clone()));
    builder = builder.name(name);

    if ignore_nulls {
        builder = builder.ignore_nulls();
    }
    if is_distinct {
        builder = builder.distinct();
    }

    builder.build()
}

#[allow(clippy::too_many_arguments)]
// This is not for external usage, consider creating with `create_aggregate_expr` instead.
pub fn create_aggregate_expr_with_dfschema(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    input_exprs: &[Expr],
    sort_exprs: &[Expr],
    ordering_req: &[PhysicalSortExpr],
    dfschema: &DFSchema,
    name: impl Into<String>,
    ignore_nulls: bool,
    is_distinct: bool,
    is_reversed: bool,
) -> Result<Arc<dyn AggregateExpr>> {
    let mut builder =
        AggregateExprBuilder::new(Arc::new(fun.clone()), input_phy_exprs.to_vec());
    builder = builder.sort_exprs(sort_exprs.to_vec());
    builder = builder.order_by(ordering_req.to_vec());
    builder = builder.logical_exprs(input_exprs.to_vec());
    builder = builder.dfschema(dfschema.clone());
    let schema: Schema = dfschema.into();
    builder = builder.schema(Arc::new(schema));
    builder = builder.name(name);

    if ignore_nulls {
        builder = builder.ignore_nulls();
    }
    if is_distinct {
        builder = builder.distinct();
    }
    if is_reversed {
        builder = builder.reversed();
    }

    builder.build()
}

/// Builder for physical [`AggregateExpr`]
///
/// `AggregateExpr` contains the information necessary to call
/// an aggregate expression.
#[derive(Debug, Clone)]
pub struct AggregateExprBuilder {
    fun: Arc<AggregateUDF>,
    /// Physical expressions of the aggregate function
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Logical expressions of the aggregate function, it will be deprecated in <https://github.com/apache/datafusion/issues/11359>
    logical_args: Vec<Expr>,
    name: String,
    /// Arrow Schema for the aggregate function
    schema: SchemaRef,
    /// Datafusion Schema for the aggregate function
    dfschema: DFSchema,
    /// The logical order by expressions, it will be deprecated in <https://github.com/apache/datafusion/issues/11359>
    sort_exprs: Vec<Expr>,
    /// The physical order by expressions
    ordering_req: LexOrdering,
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
            logical_args: vec![],
            name: String::new(),
            schema: Arc::new(Schema::empty()),
            dfschema: DFSchema::empty(),
            sort_exprs: vec![],
            ordering_req: vec![],
            ignore_nulls: false,
            is_distinct: false,
            is_reversed: false,
        }
    }

    pub fn build(self) -> Result<Arc<dyn AggregateExpr>> {
        let Self {
            fun,
            args,
            logical_args,
            name,
            schema,
            dfschema,
            sort_exprs,
            ordering_req,
            ignore_nulls,
            is_distinct,
            is_reversed,
        } = self;
        if args.is_empty() {
            return internal_err!("args should not be empty");
        }

        let mut ordering_fields = vec![];

        debug_assert_eq!(sort_exprs.len(), ordering_req.len());
        if !ordering_req.is_empty() {
            let ordering_types = ordering_req
                .iter()
                .map(|e| e.expr.data_type(&schema))
                .collect::<Result<Vec<_>>>()?;

            ordering_fields = utils::ordering_fields(&ordering_req, &ordering_types);
        }

        let input_exprs_types = args
            .iter()
            .map(|arg| arg.data_type(&schema))
            .collect::<Result<Vec<_>>>()?;

        check_arg_count(
            fun.name(),
            &input_exprs_types,
            &fun.signature().type_signature,
        )?;

        let data_type = fun.return_type(&input_exprs_types)?;

        Ok(Arc::new(AggregateFunctionExpr {
            fun: Arc::unwrap_or_clone(fun),
            args,
            logical_args,
            data_type,
            name,
            schema: Arc::unwrap_or_clone(schema),
            dfschema,
            sort_exprs,
            ordering_req,
            ignore_nulls,
            ordering_fields,
            is_distinct,
            input_types: input_exprs_types,
            is_reversed,
        }))
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    pub fn dfschema(mut self, dfschema: DFSchema) -> Self {
        self.dfschema = dfschema;
        self
    }

    pub fn order_by(mut self, order_by: LexOrdering) -> Self {
        self.ordering_req = order_by;
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

    /// This method will be deprecated in <https://github.com/apache/datafusion/issues/11359>
    pub fn sort_exprs(mut self, sort_exprs: Vec<Expr>) -> Self {
        self.sort_exprs = sort_exprs;
        self
    }

    /// This method will be deprecated in <https://github.com/apache/datafusion/issues/11359>
    pub fn logical_exprs(mut self, logical_args: Vec<Expr>) -> Self {
        self.logical_args = logical_args;
        self
    }
}

/// An aggregate expression that:
/// * knows its resulting field
/// * knows how to create its accumulator
/// * knows its accumulator's state's field
/// * knows the expressions from whose its accumulator will receive values
///
/// Any implementation of this trait also needs to implement the
/// `PartialEq<dyn Any>` to allows comparing equality between the
/// trait objects.
pub trait AggregateExpr: Send + Sync + Debug + PartialEq<dyn Any> {
    /// Returns the aggregate expression as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field>;

    /// the accumulator used to accumulate values from the expressions.
    /// the accumulator expects the same number of arguments as `expressions` and must
    /// return states with the same description as `state_fields`
    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>>;

    /// the fields that encapsulate the Accumulator's state
    /// the number of fields here equals the number of states that the accumulator contains
    fn state_fields(&self) -> Result<Vec<Field>>;

    /// expressions that are passed to the Accumulator.
    /// Single-column aggregations such as `sum` return a single value, others (e.g. `cov`) return many.
    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>>;

    /// Order by requirements for the aggregate function
    /// By default it is `None` (there is no requirement)
    /// Order-sensitive aggregators, such as `FIRST_VALUE(x ORDER BY y)` should implement this
    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    /// Indicates whether aggregator can produce the correct result with any
    /// arbitrary input ordering. By default, we assume that aggregate expressions
    /// are order insensitive.
    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
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
    fn with_beneficial_ordering(
        self: Arc<Self>,
        _requirement_satisfied: bool,
    ) -> Result<Option<Arc<dyn AggregateExpr>>> {
        if self.order_bys().is_some() && self.order_sensitivity().is_beneficial() {
            return exec_err!(
                "Should implement with satisfied for aggregator :{:?}",
                self.name()
            );
        }
        Ok(None)
    }

    /// Human readable name such as `"MIN(c2)"`. The default
    /// implementation returns placeholder text.
    fn name(&self) -> &str {
        "AggregateExpr: default name"
    }

    /// If the aggregate expression has a specialized
    /// [`GroupsAccumulator`] implementation. If this returns true,
    /// `[Self::create_groups_accumulator`] will be called.
    fn groups_accumulator_supported(&self) -> bool {
        false
    }

    /// Return a specialized [`GroupsAccumulator`] that manages state
    /// for all groups.
    ///
    /// For maximum performance, a [`GroupsAccumulator`] should be
    /// implemented in addition to [`Accumulator`].
    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        not_impl_err!("GroupsAccumulator hasn't been implemented for {self:?} yet")
    }

    /// Construct an expression that calculates the aggregate in reverse.
    /// Typically the "reverse" expression is itself (e.g. SUM, COUNT).
    /// For aggregates that do not support calculation in reverse,
    /// returns None (which is the default value).
    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        None
    }

    /// Creates accumulator implementation that supports retract
    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        not_impl_err!("Retractable Accumulator hasn't been implemented for {self:?} yet")
    }

    /// Returns all expressions used in the [`AggregateExpr`].
    /// These expressions are  (1)function arguments, (2) order by expressions.
    fn all_expressions(&self) -> AggregatePhysicalExpressions {
        let args = self.expressions();
        let order_bys = self.order_bys().unwrap_or(&[]);
        let order_by_exprs = order_bys
            .iter()
            .map(|sort_expr| sort_expr.expr.clone())
            .collect::<Vec<_>>();
        AggregatePhysicalExpressions {
            args,
            order_by_exprs,
        }
    }

    /// Rewrites [`AggregateExpr`], with new expressions given. The argument should be consistent
    /// with the return value of the [`AggregateExpr::all_expressions`] method.
    /// Returns `Some(Arc<dyn AggregateExpr>)` if re-write is supported, otherwise returns `None`.
    /// TODO: This method only rewrites the [`PhysicalExpr`]s and does not handle [`Expr`]s.
    /// This can cause silent bugs and should be fixed in the future (possibly with physical-to-logical
    /// conversions).
    fn with_new_expressions(
        &self,
        _args: Vec<Arc<dyn PhysicalExpr>>,
        _order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn AggregateExpr>> {
        None
    }

    /// If this function is max, return (output_field, true)
    /// if the function is min, return (output_field, false)
    /// otherwise return None (the default)
    ///
    /// output_field is the name of the column produced by this aggregate
    ///
    /// Note: this is used to use special aggregate implementations in certain conditions
    fn get_minmax_desc(&self) -> Option<(Field, bool)> {
        None
    }
}

/// Stores the physical expressions used inside the `AggregateExpr`.
pub struct AggregatePhysicalExpressions {
    /// Aggregate function arguments
    pub args: Vec<Arc<dyn PhysicalExpr>>,
    /// Order by expressions
    pub order_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug, Clone)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    logical_args: Vec<Expr>,
    /// Output / return type of this aggregate
    data_type: DataType,
    name: String,
    schema: Schema,
    dfschema: DFSchema,
    // The logical order by expressions
    sort_exprs: Vec<Expr>,
    // The physical order by expressions
    ordering_req: LexOrdering,
    // Whether to ignore null values
    ignore_nulls: bool,
    // fields used for order sensitive aggregation functions
    ordering_fields: Vec<Field>,
    is_distinct: bool,
    is_reversed: bool,
    input_types: Vec<DataType>,
}

impl AggregateFunctionExpr {
    /// Return the `AggregateUDF` used by this `AggregateFunctionExpr`
    pub fn fun(&self) -> &AggregateUDF {
        &self.fun
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
}

impl AggregateExpr for AggregateFunctionExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.args.clone()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let args = StateFieldsArgs {
            name: &self.name,
            input_types: &self.input_types,
            return_type: &self.data_type,
            ordering_fields: &self.ordering_fields,
            is_distinct: self.is_distinct,
        };

        self.fun.state_fields(args)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let acc_args = AccumulatorArgs {
            data_type: &self.data_type,
            schema: &self.schema,
            dfschema: &self.dfschema,
            ignore_nulls: self.ignore_nulls,
            sort_exprs: &self.sort_exprs,
            is_distinct: self.is_distinct,
            input_types: &self.input_types,
            input_exprs: &self.logical_args,
            name: &self.name,
            is_reversed: self.is_reversed,
        };

        self.fun.accumulator(acc_args)
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let args = AccumulatorArgs {
            data_type: &self.data_type,
            schema: &self.schema,
            dfschema: &self.dfschema,
            ignore_nulls: self.ignore_nulls,
            sort_exprs: &self.sort_exprs,
            is_distinct: self.is_distinct,
            input_types: &self.input_types,
            input_exprs: &self.logical_args,
            name: &self.name,
            is_reversed: self.is_reversed,
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

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        let args = AccumulatorArgs {
            data_type: &self.data_type,
            schema: &self.schema,
            dfschema: &self.dfschema,
            ignore_nulls: self.ignore_nulls,
            sort_exprs: &self.sort_exprs,
            is_distinct: self.is_distinct,
            input_types: &self.input_types,
            input_exprs: &self.logical_args,
            name: &self.name,
            is_reversed: self.is_reversed,
        };
        self.fun.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        let args = AccumulatorArgs {
            data_type: &self.data_type,
            schema: &self.schema,
            dfschema: &self.dfschema,
            ignore_nulls: self.ignore_nulls,
            sort_exprs: &self.sort_exprs,
            is_distinct: self.is_distinct,
            input_types: &self.input_types,
            input_exprs: &self.logical_args,
            name: &self.name,
            is_reversed: self.is_reversed,
        };
        self.fun.create_groups_accumulator(args)
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        if self.ordering_req.is_empty() {
            return None;
        }

        if !self.order_sensitivity().is_insensitive() {
            return Some(&self.ordering_req);
        }

        None
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        if !self.ordering_req.is_empty() {
            // If there is requirement, use the sensitivity of the implementation
            self.fun.order_sensitivity()
        } else {
            // If no requirement, aggregator is order insensitive
            AggregateOrderSensitivity::Insensitive
        }
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateExpr>>> {
        let Some(updated_fn) = self
            .fun
            .clone()
            .with_beneficial_ordering(beneficial_ordering)?
        else {
            return Ok(None);
        };
        create_aggregate_expr_with_dfschema(
            &updated_fn,
            &self.args,
            &self.logical_args,
            &self.sort_exprs,
            &self.ordering_req,
            &self.dfschema,
            self.name(),
            self.ignore_nulls,
            self.is_distinct,
            self.is_reversed,
        )
        .map(Some)
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        match self.fun.reverse_udf() {
            ReversedUDAF::NotSupported => None,
            ReversedUDAF::Identical => Some(Arc::new(self.clone())),
            ReversedUDAF::Reversed(reverse_udf) => {
                let reverse_ordering_req = reverse_order_bys(&self.ordering_req);
                let reverse_sort_exprs = self
                    .sort_exprs
                    .iter()
                    .map(|e| {
                        if let Expr::Sort(s) = e {
                            Expr::Sort(s.reverse())
                        } else {
                            // Expects to receive `Expr::Sort`.
                            unreachable!()
                        }
                    })
                    .collect::<Vec<_>>();
                let mut name = self.name().to_string();
                // If the function is changed, we need to reverse order_by clause as well
                // i.e. First(a order by b asc null first) -> Last(a order by b desc null last)
                if self.fun().name() == reverse_udf.name() {
                } else {
                    replace_order_by_clause(&mut name);
                }
                replace_fn_name_clause(&mut name, self.fun.name(), reverse_udf.name());
                let reverse_aggr = create_aggregate_expr_with_dfschema(
                    &reverse_udf,
                    &self.args,
                    &self.logical_args,
                    &reverse_sort_exprs,
                    &reverse_ordering_req,
                    &self.dfschema,
                    name,
                    self.ignore_nulls,
                    self.is_distinct,
                    !self.is_reversed,
                )
                .unwrap();

                Some(reverse_aggr)
            }
        }
    }

    fn get_minmax_desc(&self) -> Option<(Field, bool)> {
        self.fun
            .is_descending()
            .and_then(|flag| self.field().ok().map(|f| (f, flag)))
    }
}

impl PartialEq<dyn Any> for AggregateFunctionExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.fun == x.fun
                    && self.args.len() == x.args.len()
                    && self
                        .args
                        .iter()
                        .zip(x.args.iter())
                        .all(|(this_arg, other_arg)| this_arg.eq(other_arg))
            })
            .unwrap_or(false)
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
