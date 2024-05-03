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

//! Optimizer rule for type validation and coercion

use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit};

use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeIterator, TreeNodeRewriter,
};
use datafusion_common::{
    exec_err, internal_err, not_impl_err, plan_datafusion_err, plan_err, DFSchema,
    DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{
    self, AggregateFunctionDefinition, Between, BinaryExpr, Case, Exists, InList,
    InSubquery, Like, ScalarFunction, WindowFunction,
};
use datafusion_expr::expr_schema::cast_subquery;
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{
    comparison_coercion, get_input_types, like_coercion,
};
use datafusion_expr::type_coercion::functions::data_types;
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_expression, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{is_datetime, is_utf8_or_large_utf8};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    is_false, is_not_false, is_not_true, is_not_unknown, is_true, is_unknown, not,
    type_coercion, AggregateFunction, Expr, ExprSchemable, Join, LogicalPlan, Operator,
    ScalarFunctionDefinition, ScalarUDF, Signature, WindowFrame, WindowFrameBound,
    WindowFrameUnits,
};

use crate::analyzer::AnalyzerRule;
use crate::utils::NamePreserver;

#[derive(Default)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        let empty = DFSchema::empty();
        let mut plan_rewriter = TypeCoercionPlanRewriter::new(&empty);

        let transformed_plan = plan.rewrite_with_subqueries(&mut plan_rewriter)?.data;

        Ok(transformed_plan)
    }
}

/// Rewrites plans to ensure that all expressions have valid types
/// tracking if any input plans have been transformed
pub struct TypeCoercionPlanRewriter<'a> {
    /// were any child plans transformed? If so, we need to recompute the schema
    /// of the parent plan as some plan outputs are dependent on the schema of
    /// the children
    any_plan_transformed: bool,
    /// The outer query schema, if any
    external_schema: &'a DFSchema,
}

impl<'a> TypeCoercionPlanRewriter<'a> {
    fn new(external_schema: &'a DFSchema) -> Self {
        Self {
            any_plan_transformed: false,
            external_schema,
        }
    }
}

impl<'a> TreeNodeRewriter for TypeCoercionPlanRewriter<'a> {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        // get schema representing all available input fields. This is used for data type
        // resolution only, so order does not matter here
        let mut schema = merge_schema(plan.inputs());

        if let LogicalPlan::TableScan(ts) = &plan {
            let source_schema = DFSchema::try_from_qualified_schema(
                ts.table_name.clone(),
                &ts.source.schema(),
            )?;
            schema.merge(&source_schema);
        }

        // merge the outer schema for correlated subqueries
        // like case:
        // select t2.c2 from t1 where t1.c1 in (select t2.c1 from t2 where t2.c2=t1.c3)
        schema.merge(self.external_schema);

        let mut expr_rewrite = TypeCoercionExprRewriter::new(&schema);
        let name_preserver = NamePreserver::new(&plan);
        // apply coercion rewrite to all expressions in the plan individually
        let transformed_plan = plan
            .map_expressions(|expr| {
                let original_name = name_preserver.save(&expr)?;
                expr.rewrite(&mut expr_rewrite)?
                    .map_data(|expr| original_name.restore(expr))
            })?
            // coerce join expressions specially
            .transform_data(|plan| expr_rewrite.coerce_joins(plan))?;

        // Note: We must recompute the schema if any of expressions (or inputs)
        // have been rewritten, as the types may have changed.

        if transformed_plan.transformed {
            self.any_plan_transformed = true;
        }

        // Hack: For subquery alias need to recompute the schema for unknown reasons
        if matches!(transformed_plan.data, LogicalPlan::SubqueryAlias(_)) {
            self.any_plan_transformed = true;
        }

        if self.any_plan_transformed {
            transformed_plan
                .transform_data(|plan| plan.recompute_schema().map(Transformed::yes))
        } else {
            Ok(transformed_plan)
        }
    }
}

pub(crate) struct TypeCoercionExprRewriter<'a> {
    pub(crate) schema: &'a DFSchema,
}

impl<'a> TypeCoercionExprRewriter<'a> {
    fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }
}

impl<'a> TreeNodeRewriter for TypeCoercionExprRewriter<'a> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Unnest(_) => not_impl_err!(
                "Unnest should be rewritten to LogicalPlan::Unnest before type coercion"
            ),
            Expr::ScalarSubquery(subsquery) => self
                .coerce_in_subquery(subsquery)?
                .map_data(|subquery| Ok(Expr::ScalarSubquery(subquery))),
            Expr::Exists(Exists { subquery, negated }) => self
                .coerce_in_subquery(subquery)?
                .map_data(|subquery| Ok(Expr::Exists(Exists { subquery, negated }))),
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => {
                self.coerce_in_subquery(subquery)?
                    .transform_data(|subquery| {
                        let new_plan = &subquery.subquery;
                        let expr_type = expr.get_type(self.schema)?;
                        let subquery_type = new_plan.schema().field(0).data_type();
                        let common_type = comparison_coercion(&expr_type, subquery_type).ok_or(plan_datafusion_err!(
                            "expr type {expr_type:?} can't cast to {subquery_type:?} in InSubquery"
                        ))?;
                        // coerce the expr and subquery to the common type if needed
                        let expr = self.maybe_cast_from(*expr, &expr_type, &common_type)?;
                        let subquery = if subquery.data_type() != &common_type {
                            Transformed::yes(cast_subquery(subquery, &common_type)?)
                        } else {
                            Transformed::no(subquery)
                        };
                        // transformation was applied if either the expr or the
                        // subquery transformed
                        let transformed = expr.transformed || subquery.transformed;
                        // create output
                        let new_expr = Expr::InSubquery(InSubquery::new(
                            Box::new(expr.data),
                            subquery.data,
                            negated,
                        ));
                        if transformed {
                            Ok(Transformed::yes(new_expr))
                        } else {
                            Ok(Transformed::no(new_expr))
                        }
                    })
            }
            Expr::Not(expr) => self.coerce_bool_op(*expr)?.map_data(|expr| Ok(not(expr))),
            Expr::IsTrue(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_true(expr))),
            Expr::IsNotTrue(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_not_true(expr))),
            Expr::IsFalse(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_false(expr))),
            Expr::IsNotFalse(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_not_false(expr))),
            Expr::IsUnknown(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_unknown(expr))),
            Expr::IsNotUnknown(expr) => self
                .coerce_bool_op(*expr)?
                .map_data(|expr| Ok(is_not_unknown(expr))),
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
                case_insensitive,
            }) => {
                let left_type = expr.get_type(self.schema)?;
                let right_type = pattern.get_type(self.schema)?;
                let coerced_type = like_coercion(&left_type,  &right_type).ok_or_else(|| {
                    let op_name = if case_insensitive {
                        "ILIKE"
                    } else {
                        "LIKE"
                    };
                    plan_datafusion_err!(
                        "There isn't a common type to coerce {left_type} and {right_type} in {op_name} expression"
                    )
                })?;
                let mut caster = self.caster();

                let expr =
                    Box::new(caster.maybe_cast_from(*expr, &left_type, &coerced_type)?);
                let pattern = Box::new(caster.maybe_cast_from(
                    *pattern,
                    &right_type,
                    &coerced_type,
                )?);
                caster.build(Expr::Like(Like::new(
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                )))
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => self
                .coerce_binary_op(*left, op, *right)?
                .map_data(|(left, right)| {
                    Ok(Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(left),
                        op,
                        Box::new(right),
                    )))
                }),
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr_type = expr.get_type(self.schema)?;
                let low_type = low.get_type(self.schema)?;
                let low_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {expr_type} and {low_type} in BETWEEN expression"
                        ))
                    })?;
                let high_type = high.get_type(self.schema)?;
                let high_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                        ))
                    })?;
                let coercion_type =
                    comparison_coercion(&low_coerced_type, &high_coerced_type)
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                            ))
                        })?;
                let mut caster = self.caster();
                let expr = caster.maybe_cast_from(*expr, &expr_type, &coercion_type)?;
                let low = caster.maybe_cast_from(*low, &low_type, &coercion_type)?;
                let high = caster.maybe_cast_from(*high, &high_type, &coercion_type)?;
                caster.build(Expr::Between(Between::new(
                    Box::new(expr),
                    negated,
                    Box::new(low),
                    Box::new(high),
                )))
            }
            Expr::InList(InList {
                expr,
                list,
                negated,
            }) => {
                let expr_data_type = expr.get_type(self.schema)?;
                let list_data_types = list
                    .iter()
                    .map(|list_expr| list_expr.get_type(self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let result_type =
                    get_coerce_type_for_list(&expr_data_type, &list_data_types);
                let Some(coerced_type) = result_type else {
                    return plan_err!(
                        "Can not find compatible types to compare {expr_data_type:?} with {list_data_types:?}"
                    );
                };
                // find the coerced type
                let mut caster = self.caster();
                let cast_expr =
                    caster.maybe_cast_from(*expr, &expr_data_type, &coerced_type)?;
                let cast_list_expr = list
                    .into_iter()
                    .map(|list_expr| caster.maybe_cast(list_expr, &coerced_type))
                    .collect::<Result<Vec<_>>>()?;
                caster.build(Expr::InList(InList::new(
                    Box::new(cast_expr),
                    cast_list_expr,
                    negated,
                )))
            }
            Expr::Case(case) => self
                .coerce_case_expression(case)?
                .map_data(|case| Ok(Expr::Case(case))),
            Expr::ScalarFunction(ScalarFunction { func_def, args }) => match func_def {
                ScalarFunctionDefinition::UDF(fun) => self
                    .coerce_arguments_for_signature(args, fun.signature())?
                    .transform_data(|new_expr| {
                        self.coerce_arguments_for_fun(new_expr, &fun)
                    })?
                    .map_data(|new_expr| {
                        Ok(Expr::ScalarFunction(ScalarFunction::new_udf(fun, new_expr)))
                    }),
            },
            Expr::AggregateFunction(expr::AggregateFunction {
                func_def,
                args,
                distinct,
                filter,
                order_by,
                null_treatment,
            }) => match func_def {
                AggregateFunctionDefinition::BuiltIn(fun) => self
                    .coerce_agg_exprs_for_signature(&fun, args, &fun.signature())?
                    .map_data(|new_expr| {
                        Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                            fun,
                            new_expr,
                            distinct,
                            filter,
                            order_by,
                            null_treatment,
                        )))
                    }),
                AggregateFunctionDefinition::UDF(fun) => self
                    .coerce_arguments_for_signature(args, fun.signature())?
                    .map_data(|new_expr| {
                        Ok(Expr::AggregateFunction(expr::AggregateFunction::new_udf(
                            fun,
                            new_expr,
                            false,
                            filter,
                            order_by,
                            null_treatment,
                        )))
                    }),
                AggregateFunctionDefinition::Name(_) => {
                    internal_err!("Function `Expr` with name should be resolved.")
                }
            },
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment,
            }) => {
                let window_frame = self.coerce_window_frame(window_frame, &order_by)?;

                let args = match &fun {
                    expr::WindowFunctionDefinition::AggregateFunction(fun) => {
                        self.coerce_agg_exprs_for_signature(fun, args, &fun.signature())?
                    }
                    _ => Transformed::no(args),
                };
                let transformed = window_frame.transformed | args.transformed;
                let window_func = Expr::WindowFunction(WindowFunction::new(
                    fun,
                    args.data,
                    partition_by,
                    order_by,
                    window_frame.data,
                    null_treatment,
                ));

                if transformed {
                    Ok(Transformed::yes(window_func))
                } else {
                    Ok(Transformed::no(window_func))
                }
            }
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::SimilarTo(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::GetIndexedField(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::Sort(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _) => Ok(Transformed::no(expr)),
        }
    }
}
impl<'a> TypeCoercionExprRewriter<'a> {
    /// Casts expr to new_type if it is different from the current type
    ///
    /// Note: current_type is passed in as an optimization to avoid recomputing the type
    fn maybe_cast_from(
        &self,
        expr: Expr,
        cur_type: &DataType,
        new_type: &DataType,
    ) -> Result<Transformed<Expr>> {
        let mut caster = self.caster();
        let expr = caster.maybe_cast_from(expr, cur_type, new_type)?;
        caster.build(expr)
    }

    /// Create a [`Caster`] for tracking when expressions have been changed
    fn caster(&self) -> Caster {
        Caster::new(self.schema)
    }

    /// Coerce the subquery recursively
    fn coerce_in_subquery(&self, subquery: Subquery) -> Result<Transformed<Subquery>> {
        let Subquery {
            subquery,
            outer_ref_columns,
        } = subquery;

        // use TypeCoercionPlanRewriter rather than calling analyze_internal
        // directly to pass along info if any of the subquery inputs were
        // transformed
        let mut subquery_rewriter = TypeCoercionPlanRewriter::new(self.schema);
        unwrap_arc(subquery)
            .rewrite_with_subqueries(&mut subquery_rewriter)?
            .map_data(|new_plan| {
                Ok(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                })
            })
    }

    /// Coerce join equality expressions
    ///
    /// Joins must be treated specially as their equality expressions are stored
    /// as a parallel list of left and right expressions, rather than a single
    /// equality expression
    ///
    /// For example, on_exprs like `t1.a = t2.b AND t1.x = t2.y` will be stored
    /// as a list of `(t1.a, t2.b), (t1.x, t2.y)`
    fn coerce_joins(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(Join {
            left,
            right,
            on,
            filter,
            join_type,
            join_constraint,
            schema,
            null_equals_null,
        }) = plan
        else {
            return Ok(Transformed::no(plan));
        };

        // apply the coercion to each equality expression
        let on = on.into_iter().map_until_stop_and_collect(|(lhs, rhs)| {
            // coerce the arguments as though they were a single binary equality
            // expression
            self.coerce_binary_op(lhs, Operator::Eq, rhs)
        })?;

        // pass the transformed flag back up
        on.map_data(|on| {
            Ok(LogicalPlan::Join(Join {
                left,
                right,
                on,
                filter,
                join_type,
                join_constraint,
                schema,
                null_equals_null,
            }))
        })
    }
    fn coerce_binary_op(
        &self,
        left: Expr,
        op: Operator,
        right: Expr,
    ) -> Result<Transformed<(Expr, Expr)>> {
        let initial_left_type = left.get_type(self.schema)?;
        let initial_right_type = right.get_type(self.schema)?;

        let (left_type, right_type) =
            get_input_types(&initial_left_type, &op, &initial_right_type)?;

        let mut caster = Caster::new(self.schema);
        let result = (
            caster.maybe_cast_from(left, &initial_left_type, &left_type)?,
            caster.maybe_cast_from(right, &initial_right_type, &right_type)?,
        );
        caster.build(result)
    }

    /// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion by coercing
    /// the input expr to `Boolean` type.
    /// The above Exprs will be rewrite to the binary op when creating the physical op.
    fn coerce_bool_op(&self, expr: Expr) -> Result<Transformed<Expr>> {
        let left_type = expr.get_type(self.schema)?;
        // error check
        get_input_types(&left_type, &Operator::IsDistinctFrom, &DataType::Boolean)?;
        self.maybe_cast_from(expr, &left_type, &DataType::Boolean)
    }

    /// Casts the given `value` to `target_type`. Note that this function
    /// only considers `Null` or `Utf8` values.
    fn coerce_scalar(
        &self,
        target_type: &DataType,
        value: &ScalarValue,
    ) -> Result<Transformed<ScalarValue>> {
        match value {
            // Coerce Utf8 values:
            ScalarValue::Utf8(Some(val)) => {
                ScalarValue::try_from_string(val.clone(), target_type)
                    .map(Transformed::yes)
            }
            s => {
                if s.is_null() {
                    // Coerce `Null` values:
                    ScalarValue::try_from(target_type).map(Transformed::yes)
                } else {
                    // Values except `Utf8`/`Null` variants already have the right type
                    // (casted before) since we convert `sqlparser` outputs to `Utf8`
                    // for all possible cases. Therefore, we return a clone here.
                    Ok(Transformed::no(s.clone()))
                }
            }
        }
    }

    /// This function coerces `value` to `target_type` in a range-aware fashion.
    /// If the coercion is successful, we return an `Ok` value with the result.
    /// If the coercion fails because `target_type` is not wide enough (i.e. we
    /// can not coerce to `target_type`, but we can to a wider type in the same
    /// family), we return a `Null` value of this type to signal this situation.
    /// Downstream code uses this signal to treat these values as *unbounded*.
    fn coerce_scalar_range_aware(
        &self,
        target_type: &DataType,
        value: ScalarValue,
    ) -> Result<Transformed<ScalarValue>> {
        self.coerce_scalar(target_type, &value).or_else(|err| {
            // If type coercion fails, check if the largest type in family works:
            if let Some(largest_type) = self.get_widest_type_in_family(target_type) {
                self.coerce_scalar(largest_type, &value).map_or_else(
                    |_| exec_err!("Cannot cast {value:?} to {target_type:?}"),
                    |_| ScalarValue::try_from(target_type).map(Transformed::yes),
                )
            } else {
                Err(err)
            }
        })
    }

    /// This function returns the widest type in the family of `given_type`.
    /// If the given type is already the widest type, it returns `None`.
    /// For example, if `given_type` is `Int8`, it returns `Int64`.
    fn get_widest_type_in_family(&self, given_type: &DataType) -> Option<&DataType> {
        match given_type {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
                Some(&DataType::UInt64)
            }
            DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(&DataType::Int64),
            DataType::Float16 | DataType::Float32 => Some(&DataType::Float64),
            _ => None,
        }
    }

    /// Coerces the given (window frame) `bound` to `target_type`.
    fn coerce_frame_bound(
        &self,
        target_type: &DataType,
        bound: WindowFrameBound,
    ) -> Result<Transformed<WindowFrameBound>> {
        Ok(match bound {
            WindowFrameBound::Preceding(v) => self
                .coerce_scalar_range_aware(target_type, v)?
                .update_data(WindowFrameBound::Preceding),
            WindowFrameBound::CurrentRow => Transformed::no(WindowFrameBound::CurrentRow),
            WindowFrameBound::Following(v) => self
                .coerce_scalar_range_aware(target_type, v)?
                .update_data(WindowFrameBound::Following),
        })
    }

    // Coerces the given `window_frame` to use appropriate natural types.
    // For example, ROWS and GROUPS frames use `UInt64` during calculations.
    fn coerce_window_frame(
        &self,
        mut window_frame: WindowFrame,
        expressions: &[Expr],
    ) -> Result<Transformed<WindowFrame>> {
        let current_types = expressions
            .iter()
            .map(|e| e.get_type(self.schema))
            .collect::<Result<Vec<_>>>()?;
        let target_type = match window_frame.units {
            WindowFrameUnits::Range => {
                if let Some(col_type) = current_types.first() {
                    if col_type.is_numeric()
                        || is_utf8_or_large_utf8(col_type)
                        || matches!(col_type, DataType::Null)
                    {
                        col_type
                    } else if is_datetime(col_type) {
                        &DataType::Interval(IntervalUnit::MonthDayNano)
                    } else {
                        return internal_err!(
                            "Cannot run range queries on datatype: {col_type:?}"
                        );
                    }
                } else {
                    return internal_err!("ORDER BY column cannot be empty");
                }
            }
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => &DataType::UInt64,
        };

        let start_bound =
            self.coerce_frame_bound(target_type, window_frame.start_bound)?;
        let end_bound = self.coerce_frame_bound(target_type, window_frame.end_bound)?;
        let transformed = start_bound.transformed | end_bound.transformed;

        window_frame.start_bound = start_bound.data;
        window_frame.end_bound = end_bound.data;

        if transformed {
            Ok(Transformed::yes(window_frame))
        } else {
            Ok(Transformed::no(window_frame))
        }
    }

    /// Returns `expressions` coerced to types compatible with
    /// `signature`, if possible.
    ///
    /// See the module level documentation for more detail on coercion.
    fn coerce_arguments_for_signature(
        &self,
        expressions: Vec<Expr>,
        signature: &Signature,
    ) -> Result<Transformed<Vec<Expr>>> {
        if expressions.is_empty() {
            return Ok(Transformed::no(expressions));
        }

        let current_types = expressions
            .iter()
            .map(|e| e.get_type(self.schema))
            .collect::<Result<Vec<_>>>()?;

        let new_types = data_types(&current_types, signature)?;

        let mut caster = self.caster();
        let new_expressions = expressions
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                caster.maybe_cast_from(expr, &current_types[i], &new_types[i])
            })
            .collect::<Result<Vec<_>>>()?;

        caster.build(new_expressions)
    }

    fn coerce_arguments_for_fun(
        &self,
        expressions: Vec<Expr>,
        fun: &Arc<ScalarUDF>,
    ) -> Result<Transformed<Vec<Expr>>> {
        // Cast Fixedsizelist to List for array functions
        if fun.name() == "make_array" {
            let mut caster = self.caster();
            let new_expressions = expressions
                .into_iter()
                .map(|expr| {
                    let data_type = expr.get_type(self.schema)?;
                    if let DataType::FixedSizeList(field, _) = &data_type {
                        let to_type = DataType::List(field.clone());
                        caster.maybe_cast_from(expr, &data_type, &to_type)
                    } else {
                        Ok(expr)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            // Hack: force the schema to be computed again even if seemingly
            // nothing changed
            // caster.build(new_expressions)
            Ok(Transformed::yes(new_expressions))
        } else {
            Ok(Transformed::no(expressions))
        }
    }

    /// Returns the coerced exprs for each `input_exprs`.
    /// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
    /// data type of `input_exprs` need to be coerced.
    fn coerce_agg_exprs_for_signature(
        &self,
        agg_fun: &AggregateFunction,
        input_exprs: Vec<Expr>,
        signature: &Signature,
    ) -> Result<Transformed<Vec<Expr>>> {
        if input_exprs.is_empty() {
            return Ok(Transformed::no(input_exprs));
        }
        let current_types = input_exprs
            .iter()
            .map(|e| e.get_type(self.schema))
            .collect::<Result<Vec<_>>>()?;

        let coerced_types =
            type_coercion::aggregates::coerce_types(agg_fun, &current_types, signature)?;

        let mut caster = Caster::new(self.schema);
        let new_exprs = input_exprs
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                caster.maybe_cast_from(expr, &current_types[i], &coerced_types[i])
            })
            .collect::<Result<Vec<_>>>()?;
        caster.build(new_exprs)
    }

    /// Given expressions like:
    ///
    /// ```sql
    /// CASE a1
    ///   WHEN a2 THEN b1
    ///   WHEN a3 THEN b2
    ///   ELSE b3
    /// END
    /// ```
    ///
    /// or:
    ///
    /// ```sql
    /// CASE
    ///   WHEN x1 THEN b1
    ///   WHEN x2 THEN b2
    ///   ELSE b3
    /// END
    /// ```
    ///
    /// Then all aN (a1, a2, a3) must be converted to a common data type in the first example
    /// (case-when expression coercion)
    ///
    /// All xN (x1, x2) must be converted to a boolean data type in the second example
    /// (when-boolean expression coercion)
    ///
    /// And all bN (b1, b2, b3) must be converted to a common data type in both examples
    /// (then-else expression coercion)
    ///
    /// If any fail to find and cast to a common/specific data type, will return error
    ///
    /// Note that case-when and when-boolean expression coercions are mutually
    /// exclusive Only one or the other can occur for a case expression, whilst
    /// then-else expression coercion will always occur
    fn coerce_case_expression(&self, case: Case) -> Result<Transformed<Case>> {
        // prepare types
        let case_type = case
            .expr
            .as_ref()
            .map(|expr| expr.get_type(self.schema))
            .transpose()?;
        let then_types = case
            .when_then_expr
            .iter()
            .map(|(_when, then)| then.get_type(self.schema))
            .collect::<Result<Vec<_>>>()?;
        let else_type = case
            .else_expr
            .as_ref()
            .map(|expr| expr.get_type(self.schema))
            .transpose()?;

        // find common coercible types
        let case_when_coerce_type = case_type
            .as_ref()
            .map(|case_type| {
                let when_types = case
                    .when_then_expr
                    .iter()
                    .map(|(when, _then)| when.get_type(self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let coerced_type =
                    get_coerce_type_for_case_expression(&when_types, Some(case_type));
                coerced_type.ok_or_else(|| {
                    plan_datafusion_err!(
                    "Failed to coerce case ({case_type:?}) and when ({when_types:?}) \
                     to common types in CASE WHEN expression"
                )
                })
            })
            .transpose()?;
        let then_else_coerce_type =
            get_coerce_type_for_case_expression(&then_types, else_type.as_ref())
                .ok_or_else(|| {
                    plan_datafusion_err!(
                    "Failed to coerce then ({then_types:?}) and else ({else_type:?}) \
                     to common types in CASE WHEN expression"
                )
                })?;

        // do cast if found common coercible types
        let mut caster = Caster::new(self.schema);
        let case_expr = case
            .expr
            .zip(case_when_coerce_type.as_ref())
            .map(|(case_expr, coercible_type)| {
                caster.maybe_cast(*case_expr, coercible_type)
            })
            .transpose()?
            .map(Box::new);
        let when_then = case
            .when_then_expr
            .into_iter()
            .map(|(when, then)| {
                let when_type =
                    case_when_coerce_type.as_ref().unwrap_or(&DataType::Boolean);
                let when = caster.maybe_cast(*when, when_type).map_err(|e| {
                    DataFusionError::Context(
                        format!(
                            "WHEN expressions in CASE couldn't be \
                         converted to common type ({when_type})"
                        ),
                        Box::new(e),
                    )
                })?;
                let then = caster.maybe_cast(*then, &then_else_coerce_type)?;
                Ok((Box::new(when), Box::new(then)))
            })
            .collect::<Result<Vec<_>>>()?;
        let else_expr = case
            .else_expr
            .map(|expr| caster.maybe_cast(*expr, &then_else_coerce_type))
            .transpose()?
            .map(Box::new);

        caster.build(Case::new(case_expr, when_then, else_expr))
    }
}

/// Casts exprs to new types, tracking if any types have been changed
///
/// This is used  when casting multiple expressions to track if any have been
/// transformed to set the transformed flag correctly
#[derive(Debug)]
struct Caster<'a> {
    schema: &'a DFSchema,
    transformed: bool,
}

impl<'a> Caster<'a> {
    fn new(schema: &'a DFSchema) -> Self {
        Self {
            schema,
            transformed: false,
        }
    }

    /// cast expr to new_type if it is different from the current type
    fn maybe_cast(&mut self, expr: Expr, new_type: &DataType) -> Result<Expr> {
        let original_type = expr.get_type(self.schema)?;
        self.maybe_cast_from(expr, &original_type, new_type)
    }

    /// Casts expr to new_type if it is different from the current type
    ///
    /// Note: current_type is passed in as an optimziation to avoid recomputing the type
    fn maybe_cast_from(
        &mut self,
        expr: Expr,
        cur_type: &DataType,
        new_type: &DataType,
    ) -> Result<Expr> {
        if cur_type == new_type {
            Ok(expr)
        } else {
            self.transformed = true;
            expr.cast_to(new_type, self.schema)
        }
    }

    /// Returns a Transformed::yes/Transformed::no based on whether any expr has
    /// been transformed
    fn build<T>(self, arg: T) -> Result<Transformed<T>> {
        Ok(if self.transformed {
            Transformed::yes(arg)
        } else {
            Transformed::no(arg)
        })
    }
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::DataType::Utf8;
    use arrow::datatypes::{DataType, Field, TimeUnit};

    use datafusion_common::tree_node::{TransformedResult, TreeNode};
    use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
    use datafusion_expr::expr::{self, InSubquery, Like, ScalarFunction};
    use datafusion_expr::logical_plan::{EmptyRelation, Projection};
    use datafusion_expr::{
        cast, col, create_udaf, is_true, lit, AccumulatorFactoryFunction,
        AggregateFunction, AggregateUDF, BinaryExpr, Case, ColumnarValue, Expr,
        ExprSchemable, Filter, LogicalPlan, Operator, ScalarUDF, ScalarUDFImpl,
        Signature, SimpleAggregateUDF, Subquery, Volatility,
    };
    use datafusion_physical_expr::expressions::AvgAccumulator;

    use crate::analyzer::type_coercion::{
        coerce_case_expression, TypeCoercion, TypeCoercionExprRewriter,
    };
    use crate::test::assert_analyzed_plan_eq;

    fn empty() -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }))
    }

    fn empty_with_type(data_type: DataType) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::from_unqualifed_fields(
                    vec![Field::new("a", data_type, true)].into(),
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }))
    }

    #[test]
    fn simple_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64)\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);

        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
        )?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64) OR a < CAST(UInt32(2) AS Float64)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[derive(Debug, Clone)]
    struct TestScalarUDF {
        signature: Signature,
    }

    impl ScalarUDFImpl for TestScalarUDF {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "TestScalarUDF"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }

        fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::from("a")))
        }
    }

    #[test]
    fn scalar_udf() -> Result<()> {
        let empty = empty();

        let udf = ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        })
        .call(vec![lit(123_i32)]);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty)?);
        let expected =
            "Projection: TestScalarUDF(CAST(Int32(123) AS Float32))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn scalar_udf_invalid_input() -> Result<()> {
        let empty = empty();
        let udf = ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        })
        .call(vec![lit("Apple")]);
        let plan_err = Projection::try_new(vec![udf], empty)
            .expect_err("Expected an error due to incorrect function input");

        let expected_error = "Error during planning: No function matches the given name and argument types 'TestScalarUDF(Utf8)'. You might need to add explicit type casts.";

        assert!(plan_err.to_string().starts_with(expected_error));
        Ok(())
    }

    #[test]
    fn scalar_function() -> Result<()> {
        // test that automatic argument type coercion for scalar functions work
        let empty = empty();
        let lit_expr = lit(10i64);
        let fun = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        });
        let scalar_function_expr =
            Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(fun), vec![lit_expr]));
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![scalar_function_expr],
            empty,
        )?);
        let expected =
            "Projection: TestScalarUDF(CAST(Int64(10) AS Float32))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn agg_udaf() -> Result<()> {
        let empty = empty();
        let my_avg = create_udaf(
            "MY_AVG",
            vec![DataType::Float64],
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::<AvgAccumulator>::default())),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );
        let udaf = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            Arc::new(my_avg),
            vec![lit(10i64)],
            false,
            None,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let expected = "Projection: MY_AVG(CAST(Int64(10) AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn agg_udaf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type = DataType::Float64;
        let accumulator: AccumulatorFactoryFunction =
            Arc::new(|_| Ok(Box::<AvgAccumulator>::default()));
        let my_avg = AggregateUDF::from(SimpleAggregateUDF::new_with_signature(
            "MY_AVG",
            Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
            return_type,
            accumulator,
            vec![
                Field::new("count", DataType::UInt64, true),
                Field::new("avg", DataType::Float64, true),
            ],
        ));
        let udaf = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            Arc::new(my_avg),
            vec![lit("10")],
            false,
            None,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, "")
            .err()
            .unwrap();
        assert_eq!(
            "type_coercion\ncaused by\nError during planning: Coercion from [Utf8] to the signature Uniform(1, [Float64]) failed.",
            err.strip_backtrace()
        );
        Ok(())
    }

    #[test]
    fn agg_function_case() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit(12i64)],
            false,
            None,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(CAST(Int64(12) AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Int32);
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![col("a")],
            false,
            None,
            None,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(CAST(a AS Float64))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_function_invalid_input_avg() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit("1")],
            false,
            None,
            None,
            None,
        ));
        let err = Projection::try_new(vec![agg_expr], empty)
            .err()
            .unwrap()
            .strip_backtrace();
        assert_eq!(
            "Error during planning: No function matches the given name and argument types 'AVG(Utf8)'. You might need to add explicit type casts.\n\tCandidate functions:\n\tAVG(Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64/Float32/Float64)",
            err
        );
        Ok(())
    }

    #[test]
    fn agg_function_invalid_input_percentile() {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::ApproxPercentileCont;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit(0.95), lit(42.0), lit(100.0)],
            false,
            None,
            None,
            None,
        ));

        let err = Projection::try_new(vec![agg_expr], empty)
            .err()
            .unwrap()
            .strip_backtrace();

        let prefix = "Error during planning: No function matches the given name and argument types 'APPROX_PERCENTILE_CONT(Float64, Float64, Float64)'. You might need to add explicit type casts.\n\tCandidate functions:";
        assert!(!err
            .strip_prefix(prefix)
            .unwrap()
            .contains("APPROX_PERCENTILE_CONT(Float64, Float64, Float64)"));
    }

    #[test]
    fn binary_op_date32_op_interval() -> Result<()> {
        //CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("386547056640")
        let expr = cast(lit("1998-03-18"), DataType::Date32)
            + lit(ScalarValue::IntervalDayTime(Some(386547056640)));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Date32) + IntervalDayTime(\"386547056640\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn inlist_case() -> Result<()> {
        // a in (1,4,8), a is int64
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: a IN ([CAST(Int32(1) AS Int64), CAST(Int8(4) AS Int64), Int64(8)]) AS a IN (Map { iter: Iter([Literal(Int32(1)), Literal(Int8(4)), Literal(Int64(8))]) })\
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // a in (1,4,8), a is decimal
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::from_unqualifed_fields(
                vec![Field::new("a", DataType::Decimal128(12, 4), true)].into(),
                std::collections::HashMap::new(),
            )?),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(a AS Decimal128(24, 4)) IN ([CAST(Int32(1) AS Decimal128(24, 4)), CAST(Int8(4) AS Decimal128(24, 4)), CAST(Int64(8) AS Decimal128(24, 4))]) AS a IN (Map { iter: Iter([Literal(Int32(1)), Literal(Int8(4)), Literal(Int64(8))]) })\
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn between_case() -> Result<()> {
        let expr = col("a").between(
            lit("2002-05-08"),
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
        );
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty)?);
        let expected =
            "Filter: a BETWEEN Utf8(\"2002-05-08\") AND CAST(CAST(Utf8(\"2002-05-08\") AS Date32) + IntervalYearMonth(\"1\") AS Utf8)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn between_infer_cheap_type() -> Result<()> {
        let expr = col("a").between(
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
            lit("2002-12-08"),
        );
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty)?);
        // TODO: we should cast col(a).
        let expected =
            "Filter: CAST(a AS Date32) BETWEEN CAST(Utf8(\"2002-05-08\") AS Date32) + IntervalYearMonth(\"1\") AND CAST(Utf8(\"2002-12-08\") AS Date32)\
            \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)
    }

    #[test]
    fn is_bool_for_type_coercion() -> Result<()> {
        // is true
        let expr = col("a").is_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);
        let expected = "Projection: a IS TRUE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let ret = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, "");
        let err = ret.unwrap_err().to_string();
        assert!(err.contains("Cannot infer common argument type for comparison operation Int64 IS DISTINCT FROM Boolean"), "{err}");

        // is not true
        let expr = col("a").is_not_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT TRUE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // is false
        let expr = col("a").is_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS FALSE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        // is not false
        let expr = col("a").is_not_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT FALSE\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        Ok(())
    }

    #[test]
    fn like_for_type_coercion() -> Result<()> {
        // like : utf8 like "abc"
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE CAST(NULL AS Utf8) AS a LIKE NULL \
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains(
            "There isn't a common type to coerce Int64 and Utf8 in LIKE expression"
        ));

        // ilike
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE CAST(NULL AS Utf8) AS a ILIKE NULL \
             \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let err = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains(
            "There isn't a common type to coerce Int64 and Utf8 in ILIKE expression"
        ));
        Ok(())
    }

    #[test]
    fn unknown_for_type_coercion() -> Result<()> {
        // unknown
        let expr = col("a").is_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);
        let expected = "Projection: a IS UNKNOWN\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let ret = assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected);
        let err = ret.unwrap_err().to_string();
        assert!(err.contains("Cannot infer common argument type for comparison operation Utf8 IS DISTINCT FROM Boolean"), "{err}");

        // is not unknown
        let expr = col("a").is_not_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT UNKNOWN\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;

        Ok(())
    }

    #[test]
    fn concat_for_type_coercion() -> Result<()> {
        let empty = empty_with_type(DataType::Utf8);
        let args = [col("a"), lit("b"), lit(true), lit(false), lit(13)];

        // concat-type signature
        {
            let expr = ScalarUDF::new_from_impl(TestScalarUDF {
                signature: Signature::variadic(vec![Utf8], Volatility::Immutable),
            })
            .call(args.to_vec());
            let plan =
                LogicalPlan::Projection(Projection::try_new(vec![expr], empty.clone())?);
            let expected =
                "Projection: TestScalarUDF(a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation";
            assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        }

        Ok(())
    }

    #[test]
    fn test_type_coercion_rewrite() -> Result<()> {
        // gt
        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionExprRewriter { schema: &schema };
        let expr = is_true(lit(12i32).gt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).gt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter).data()?;
        assert_eq!(expected, result);

        // eq
        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionExprRewriter { schema: &schema };
        let expr = is_true(lit(12i32).eq(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).eq(lit(13i64)));
        let result = expr.rewrite(&mut rewriter).data()?;
        assert_eq!(expected, result);

        // lt
        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionExprRewriter { schema: &schema };
        let expr = is_true(lit(12i32).lt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).lt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter).data()?;
        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn binary_op_date32_eq_ts() -> Result<()> {
        let expr = cast(
            lit("1998-03-18"),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .eq(cast(lit("1998-03-18"), DataType::Date32));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        dbg!(&plan);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None)) = CAST(CAST(Utf8(\"1998-03-18\") AS Date32) AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    fn cast_if_not_same_type(
        expr: Box<Expr>,
        data_type: &DataType,
        schema: &DFSchemaRef,
    ) -> Box<Expr> {
        if &expr.get_type(schema).unwrap() != data_type {
            Box::new(cast(*expr, data_type.clone()))
        } else {
            expr
        }
    }

    fn cast_helper(
        case: Case,
        case_when_type: DataType,
        then_else_type: DataType,
        schema: &DFSchemaRef,
    ) -> Case {
        let expr = case
            .expr
            .map(|e| cast_if_not_same_type(e, &case_when_type, schema));
        let when_then_expr = case
            .when_then_expr
            .into_iter()
            .map(|(when, then)| {
                (
                    cast_if_not_same_type(when, &case_when_type, schema),
                    cast_if_not_same_type(then, &then_else_type, schema),
                )
            })
            .collect::<Vec<_>>();
        let else_expr = case
            .else_expr
            .map(|e| cast_if_not_same_type(e, &then_else_type, schema));

        Case {
            expr,
            when_then_expr,
            else_expr,
        }
    }

    #[test]
    fn test_case_expression_coercion() -> Result<()> {
        let schema = Arc::new(DFSchema::from_unqualifed_fields(
            vec![
                Field::new("boolean", DataType::Boolean, true),
                Field::new("integer", DataType::Int32, true),
                Field::new("float", DataType::Float32, true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    true,
                ),
                Field::new("date", DataType::Date32, true),
                Field::new(
                    "interval",
                    DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
                    true,
                ),
                Field::new("binary", DataType::Binary, true),
                Field::new("string", DataType::Utf8, true),
                Field::new("decimal", DataType::Decimal128(10, 10), true),
            ]
            .into(),
            std::collections::HashMap::new(),
        )?);

        let case = Case {
            expr: None,
            when_then_expr: vec![
                (Box::new(col("boolean")), Box::new(col("integer"))),
                (Box::new(col("integer")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: None,
        };
        let case_when_common_type = DataType::Boolean;
        let then_else_common_type = DataType::Utf8;
        let expected = cast_helper(
            case.clone(),
            case_when_common_type,
            then_else_common_type,
            &schema,
        );
        let actual = coerce_case_expression(case, &schema)?;
        assert_eq!(expected, actual);

        let case = Case {
            expr: Some(Box::new(col("string"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("integer"))),
                (Box::new(col("integer")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: Some(Box::new(col("string"))),
        };
        let case_when_common_type = DataType::Utf8;
        let then_else_common_type = DataType::Utf8;
        let expected = cast_helper(
            case.clone(),
            case_when_common_type,
            then_else_common_type,
            &schema,
        );
        let actual = coerce_case_expression(case, &schema)?;
        assert_eq!(expected, actual);

        let case = Case {
            expr: Some(Box::new(col("interval"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("integer"))),
                (Box::new(col("binary")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("string"))),
            ],
            else_expr: Some(Box::new(col("string"))),
        };
        let err = coerce_case_expression(case, &schema).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: \
            Failed to coerce case (Interval(MonthDayNano)) and \
            when ([Float32, Binary, Utf8]) to common types in \
            CASE WHEN expression"
        );

        let case = Case {
            expr: Some(Box::new(col("string"))),
            when_then_expr: vec![
                (Box::new(col("float")), Box::new(col("date"))),
                (Box::new(col("string")), Box::new(col("float"))),
                (Box::new(col("string")), Box::new(col("binary"))),
            ],
            else_expr: Some(Box::new(col("timestamp"))),
        };
        let err = coerce_case_expression(case, &schema).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: \
            Failed to coerce then ([Date32, Float32, Binary]) and \
            else (Some(Timestamp(Nanosecond, None))) to common types \
            in CASE WHEN expression"
        );

        Ok(())
    }

    #[test]
    fn interval_plus_timestamp() -> Result<()> {
        // SELECT INTERVAL '1' YEAR + '2000-01-01T00:00:00'::timestamp;
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(lit(ScalarValue::IntervalYearMonth(Some(12)))),
            Operator::Plus,
            Box::new(cast(
                lit("2000-01-01T00:00:00"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
        ));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: IntervalYearMonth(\"12\") + CAST(Utf8(\"2000-01-01T00:00:00\") AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn timestamp_subtract_timestamp() -> Result<()> {
        let expr = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(cast(
                lit("1998-03-18"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
            Operator::Minus,
            Box::new(cast(
                lit("1998-03-18"),
                DataType::Timestamp(TimeUnit::Nanosecond, None),
            )),
        ));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        dbg!(&plan);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None)) - CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_subquery() -> Result<()> {
        let empty_int32 = empty_with_type(DataType::Int32);
        let empty_int64 = empty_with_type(DataType::Int64);

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_int32,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int64)?);
        // add cast for subquery
        let expected = "\
        Filter: a IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: CAST(a AS Int64)\
        \n      EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_expr() -> Result<()> {
        let empty_int32 = empty_with_type(DataType::Int32);
        let empty_int64 = empty_with_type(DataType::Int64);

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_int64,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int32)?);
        // add cast for subquery
        let expected = "\
        Filter: CAST(a AS Int64) IN (<subquery>)\
        \n  Subquery:\
        \n    EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }

    #[test]
    fn in_subquery_cast_all() -> Result<()> {
        let empty_inside = empty_with_type(DataType::Decimal128(10, 5));
        let empty_outside = empty_with_type(DataType::Decimal128(8, 8));

        let in_subquery_expr = Expr::InSubquery(InSubquery::new(
            Box::new(col("a")),
            Subquery {
                subquery: empty_inside,
                outer_ref_columns: vec![],
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_outside)?);
        // add cast for subquery
        let expected = "Filter: CAST(a AS Decimal128(13, 8)) IN (<subquery>)\
        \n  Subquery:\
        \n    Projection: CAST(a AS Decimal128(13, 8))\
        \n      EmptyRelation\
        \n  EmptyRelation";
        assert_analyzed_plan_eq(Arc::new(TypeCoercion::new()), &plan, expected)?;
        Ok(())
    }
}
