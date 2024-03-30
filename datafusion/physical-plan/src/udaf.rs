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

//! This module contains functions and structs supporting user-defined aggregate functions.

use arrow_schema::SortOptions;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{expr, Expr, GroupsAccumulator};
use fmt::Debug;
use std::any::Any;
use std::fmt;

use arrow::datatypes::{DataType, Field, Schema};

use super::{expressions::format_state_name, Accumulator, AggregateExpr};
use datafusion_common::{internal_err, not_impl_err, DFSchema, Result};
pub use datafusion_expr::AggregateUDF;
use datafusion_physical_expr::{
    create_physical_expr, expressions, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};

use datafusion_physical_expr::aggregate::utils::down_cast_any_ref;
use std::sync::Arc;

/// Creates a physical expression of the UDAF, that includes all necessary type coercion.
/// This function errors when `args`' can't be coerced to a valid argument type of the UDAF.
pub fn create_aggregate_expr(
    fun: &AggregateUDF,
    input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    sort_exprs: &[Expr],
    ordering_req: &[PhysicalSortExpr],
    schema: &Schema,
    name: impl Into<String>,
) -> Result<Arc<dyn AggregateExpr>> {
    let input_exprs_types = input_phy_exprs
        .iter()
        .map(|arg| arg.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(AggregateFunctionExpr {
        fun: fun.clone(),
        args: input_phy_exprs.to_vec(),
        data_type: fun.return_type(&input_exprs_types)?,
        name: name.into(),
        schema: schema.clone(),
        sort_exprs: sort_exprs.to_vec(),
        ordering_req: ordering_req.to_vec(),
    }))
}

// TODO: Duplicated functoin from `datafusion/core/src/physical_planner.rs`, remove one of them
// TODO: Maybe move to physical-expr
/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    if let Expr::Sort(expr::Sort {
        expr,
        asc,
        nulls_first,
    }) = e
    {
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(expr, input_dfschema, execution_props)?,
            options: SortOptions {
                descending: !asc,
                nulls_first: *nulls_first,
            },
        })
    } else {
        internal_err!("Expects a sort expression")
    }
}

pub fn create_aggregate_expr_first_value(
    fun: &AggregateUDF,
    args: &[Expr],
    // input_phy_exprs: &[Arc<dyn PhysicalExpr>],
    sort_exprs: &[Expr],
    dfschema: &DFSchema,
    execution_props: &ExecutionProps,
    // ordering_req: &[PhysicalSortExpr],
    schema: &Schema,
    name: impl Into<String>,
    ignore_nulls: bool,
) -> Result<Arc<dyn AggregateExpr>> {
    let args = args
        .iter()
        .map(|e| create_physical_expr(e, dfschema, execution_props))
        .collect::<Result<Vec<_>>>()?;
    let input_exprs_types = args
        .iter()
        .map(|arg| arg.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let ordering_req = sort_exprs
        .iter()
        .map(|e| create_physical_sort_expr(e, dfschema, execution_props))
        .collect::<Result<Vec<_>>>()?;
    let ordering_types = ordering_req
        .iter()
        .map(|e| e.expr.data_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let name: String = name.into();

    let input_data_type = fun.return_type(&input_exprs_types)?;

    let value_field = Field::new(
        format_state_name(&name, "first_value"),
        input_data_type.clone(),
        true,
    );
    let ordering_fields = ordering_fields(&ordering_req, &ordering_types);

    let state_fields = fun.state_fields(value_field, ordering_fields)?;

    let first_value = expressions::FirstValueUDF::new(
        args[0].clone(),
        // input_phy_exprs[0].clone(),
        name,
        input_data_type,
        ordering_req.to_vec(),
        ordering_types,
        state_fields,
    )
    .with_ignore_nulls(ignore_nulls);
    return Ok(Arc::new(first_value));
}

// TODO: Duplicated functoin.
fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<Field> {
    ordering_req
        .iter()
        .zip(data_types.iter())
        .map(|(sort_expr, dtype)| {
            Field::new(
                sort_expr.expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .collect()
}

/// Physical aggregate expression of a UDAF.
#[derive(Debug)]
pub struct AggregateFunctionExpr {
    fun: AggregateUDF,
    args: Vec<Arc<dyn PhysicalExpr>>,
    /// Output / return type of this aggregate
    data_type: DataType,
    name: String,
    schema: Schema,
    // The logical order by expressions
    sort_exprs: Vec<Expr>,
    // The physical order by expressions
    ordering_req: LexOrdering,
}

impl AggregateFunctionExpr {
    /// Return the `AggregateUDF` used by this `AggregateFunctionExpr`
    pub fn fun(&self) -> &AggregateUDF {
        &self.fun
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
        let fields = self
            .fun
            .state_type(&self.data_type)?
            .iter()
            .enumerate()
            .map(|(i, data_type)| {
                Field::new(
                    format_state_name(&self.name, &format!("{i}")),
                    data_type.clone(),
                    true,
                )
            })
            .collect::<Vec<Field>>();

        Ok(fields)
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.fun
            .accumulator(&self.data_type, self.sort_exprs.as_slice(), &self.schema)
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator =
            self.fun
                .accumulator(&self.data_type, &self.sort_exprs, &self.schema)?;

        // Accumulators that have window frame startings different
        // than `UNBOUNDED PRECEDING`, such as `1 PRECEEDING`, need to
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
        // between [1, 3) which is indeed the apropriate range.
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
        self.fun.groups_accumulator_supported()
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        self.fun.create_groups_accumulator()
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        (!self.ordering_req.is_empty()).then_some(&self.ordering_req)
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
