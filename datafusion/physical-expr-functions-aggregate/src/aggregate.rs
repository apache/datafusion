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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{internal_err, not_impl_err, Result};
use datafusion_expr::expr::create_function_physical_name;
use datafusion_expr::AggregateUDF;
use datafusion_expr::ReversedUDAF;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_expr_common::type_coercion::aggregates::check_arg_count;
use datafusion_functions_aggregate_common::accumulator::AccumulatorArgs;
use datafusion_functions_aggregate_common::accumulator::StateFieldsArgs;
use datafusion_functions_aggregate_common::aggregate::AggregateExpr;
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_functions_aggregate_common::utils::{self, down_cast_any_ref};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_expr_common::utils::reverse_order_bys;

use std::fmt::Debug;
use std::{any::Any, sync::Arc};

/// Builder for physical [`AggregateExpr`]
///
/// `AggregateExpr` contains the information necessary to call
/// an aggregate expression.
#[derive(Debug, Clone)]
pub struct AggregateExprBuilder {
    fun: Arc<AggregateUDF>,
    /// Physical expressions of the aggregate function
    args: Vec<Arc<dyn PhysicalExpr>>,
    alias: Option<String>,
    /// Arrow Schema for the aggregate function
    schema: SchemaRef,
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
            alias: None,
            schema: Arc::new(Schema::empty()),
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
            alias,
            schema,
            ordering_req,
            ignore_nulls,
            is_distinct,
            is_reversed,
        } = self;
        if args.is_empty() {
            return internal_err!("args should not be empty");
        }

        let mut ordering_fields = vec![];

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
        let name = match alias {
            // TODO: Ideally, we should build the name from physical expressions
            None => create_function_physical_name(fun.name(), is_distinct, &[], None)?,
            Some(alias) => alias,
        };

        Ok(Arc::new(AggregateFunctionExpr {
            fun: Arc::unwrap_or_clone(fun),
            args,
            data_type,
            name,
            schema: Arc::unwrap_or_clone(schema),
            ordering_req,
            ignore_nulls,
            ordering_fields,
            is_distinct,
            input_types: input_exprs_types,
            is_reversed,
        }))
    }

    pub fn alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
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
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug, Clone)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Output / return type of this aggregate
    data_type: DataType,
    name: String,
    schema: Schema,
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
            return_type: &self.data_type,
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            ordering_req: &self.ordering_req,
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };

        self.fun.accumulator(acc_args)
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let args = AccumulatorArgs {
            return_type: &self.data_type,
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            ordering_req: &self.ordering_req,
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

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        let args = AccumulatorArgs {
            return_type: &self.data_type,
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            ordering_req: &self.ordering_req,
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
        };
        self.fun.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        let args = AccumulatorArgs {
            return_type: &self.data_type,
            schema: &self.schema,
            ignore_nulls: self.ignore_nulls,
            ordering_req: &self.ordering_req,
            is_distinct: self.is_distinct,
            name: &self.name,
            is_reversed: self.is_reversed,
            exprs: &self.args,
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

        AggregateExprBuilder::new(Arc::new(updated_fn), self.args.to_vec())
            .order_by(self.ordering_req.to_vec())
            .schema(Arc::new(self.schema.clone()))
            .alias(self.name().to_string())
            .with_ignore_nulls(self.ignore_nulls)
            .with_distinct(self.is_distinct)
            .with_reversed(self.is_reversed)
            .build()
            .map(Some)
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        match self.fun.reverse_udf() {
            ReversedUDAF::NotSupported => None,
            ReversedUDAF::Identical => Some(Arc::new(self.clone())),
            ReversedUDAF::Reversed(reverse_udf) => {
                let reverse_ordering_req = reverse_order_bys(&self.ordering_req);
                let mut name = self.name().to_string();
                // If the function is changed, we need to reverse order_by clause as well
                // i.e. First(a order by b asc null first) -> Last(a order by b desc null last)
                if self.fun().name() == reverse_udf.name() {
                } else {
                    replace_order_by_clause(&mut name);
                }
                replace_fn_name_clause(&mut name, self.fun.name(), reverse_udf.name());

                AggregateExprBuilder::new(reverse_udf, self.args.to_vec())
                    .order_by(reverse_ordering_req.to_vec())
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
