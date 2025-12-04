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

use datafusion_expr::binary::BinaryTypeCoercer;
use itertools::{izip, Itertools as _};

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema};

use crate::analyzer::AnalyzerRule;
use crate::utils::NamePreserver;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_common::{
    exec_err, internal_datafusion_err, internal_err, not_impl_err, plan_datafusion_err,
    plan_err, Column, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
    TableReference,
};
use datafusion_expr::expr::{
    self, AggregateFunctionParams, Alias, Between, BinaryExpr, Case, Exists, InList,
    InSubquery, Like, ScalarFunction, Sort, WindowFunction,
};
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::expr_schema::cast_subquery;
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{comparison_coercion, like_coercion};
use datafusion_expr::type_coercion::functions::{
    data_types_with_scalar_udf, fields_with_aggregate_udf,
};
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_expression, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{is_datetime, is_utf8_or_utf8view_or_large_utf8};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    is_false, is_not_false, is_not_true, is_not_unknown, is_true, is_unknown, not,
    AggregateUDF, Expr, ExprSchemable, Join, Limit, LogicalPlan, Operator, Projection,
    ScalarUDF, Union, WindowFrame, WindowFrameBound, WindowFrameUnits,
};

/// Performs type coercion by determining the schema
/// and performing the expression rewrites.
#[derive(Default, Debug)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

/// Coerce output schema based upon optimizer config.
fn coerce_output(plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
    if !config.optimizer.expand_views_at_output {
        return Ok(plan);
    }

    let outer_refs = plan.expressions();
    if outer_refs.is_empty() {
        return Ok(plan);
    }

    if let Some(dfschema) = transform_schema_to_nonview(plan.schema()) {
        coerce_plan_expr_for_schema(plan, &dfschema?)
    } else {
        Ok(plan)
    }
}

impl AnalyzerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let empty_schema = DFSchema::empty();

        // recurse
        let transformed_plan = plan
            .transform_up_with_subqueries(|plan| analyze_internal(&empty_schema, plan))?
            .data;

        // finish
        coerce_output(transformed_plan, config)
    }
}

/// use the external schema to handle the correlated subqueries case
///
/// Assumes that children have already been optimized
fn analyze_internal(
    external_schema: &DFSchema,
    plan: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = merge_schema(&plan.inputs());

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
    schema.merge(external_schema);

    // Coerce filter predicates to boolean (handles `WHERE NULL`)
    let plan = if let LogicalPlan::Filter(mut filter) = plan {
        filter.predicate = filter.predicate.cast_to(&DataType::Boolean, &schema)?;
        LogicalPlan::Filter(filter)
    } else {
        plan
    };

    let mut expr_rewrite = TypeCoercionRewriter::new(&schema);

    let name_preserver = NamePreserver::new(&plan);
    // apply coercion rewrite all expressions in the plan individually
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);
        expr.rewrite(&mut expr_rewrite)
            .map(|transformed| transformed.update_data(|e| original_name.restore(e)))
    })?
    // some plans need extra coercion after their expressions are coerced
    .map_data(|plan| expr_rewrite.coerce_plan(plan))?
    // recompute the schema after the expressions have been rewritten as the types may have changed
    .map_data(|plan| plan.recompute_schema())
}

/// Rewrite expressions to apply type coercion.
pub struct TypeCoercionRewriter<'a> {
    pub(crate) schema: &'a DFSchema,
}

impl<'a> TypeCoercionRewriter<'a> {
    /// Create a new [`TypeCoercionRewriter`] with a provided schema
    /// representing both the inputs and output of the [`LogicalPlan`] node.
    pub fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }

    /// Coerce the [`LogicalPlan`].
    ///
    /// Refer to [`TypeCoercionRewriter::coerce_join`] and [`TypeCoercionRewriter::coerce_union`]
    /// for type-coercion approach.
    pub fn coerce_plan(&mut self, plan: LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join(join) => self.coerce_join(join),
            LogicalPlan::Union(union) => Self::coerce_union(union),
            LogicalPlan::Limit(limit) => Self::coerce_limit(limit),
            _ => Ok(plan),
        }
    }

    /// Coerce join equality expressions and join filter
    ///
    /// Joins must be treated specially as their equality expressions are stored
    /// as a parallel list of left and right expressions, rather than a single
    /// equality expression
    ///
    /// For example, on_exprs like `t1.a = t2.b AND t1.x = t2.y` will be stored
    /// as a list of `(t1.a, t2.b), (t1.x, t2.y)`
    pub fn coerce_join(&mut self, mut join: Join) -> Result<LogicalPlan> {
        join.on = join
            .on
            .into_iter()
            .map(|(lhs, rhs)| {
                // coerce the arguments as though they were a single binary equality
                // expression
                let left_schema = join.left.schema();
                let right_schema = join.right.schema();
                let (lhs, rhs) = self.coerce_binary_op(
                    lhs,
                    left_schema,
                    Operator::Eq,
                    rhs,
                    right_schema,
                )?;
                Ok((lhs, rhs))
            })
            .collect::<Result<Vec<_>>>()?;

        // Join filter must be boolean
        join.filter = join
            .filter
            .map(|expr| self.coerce_join_filter(expr))
            .transpose()?;

        Ok(LogicalPlan::Join(join))
    }

    /// Coerce the union’s inputs to a common schema compatible with all inputs.
    /// This occurs after wildcard expansion and the coercion of the input expressions.
    pub fn coerce_union(union_plan: Union) -> Result<LogicalPlan> {
        let union_schema = Arc::new(coerce_union_schema_with_schema(
            &union_plan.inputs,
            &union_plan.schema,
        )?);
        let new_inputs = union_plan
            .inputs
            .into_iter()
            .map(|p| {
                let plan =
                    coerce_plan_expr_for_schema(Arc::unwrap_or_clone(p), &union_schema)?;
                match plan {
                    LogicalPlan::Projection(Projection { expr, input, .. }) => {
                        Ok(Arc::new(project_with_column_index(
                            expr,
                            input,
                            Arc::clone(&union_schema),
                        )?))
                    }
                    other_plan => Ok(Arc::new(other_plan)),
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(LogicalPlan::Union(Union {
            inputs: new_inputs,
            schema: union_schema,
        }))
    }

    /// Coerce the fetch and skip expression to Int64 type.
    fn coerce_limit(limit: Limit) -> Result<LogicalPlan> {
        fn coerce_limit_expr(
            expr: Expr,
            schema: &DFSchema,
            expr_name: &str,
        ) -> Result<Expr> {
            let dt = expr.get_type(schema)?;
            if dt.is_integer() || dt.is_null() {
                expr.cast_to(&DataType::Int64, schema)
            } else {
                plan_err!("Expected {expr_name} to be an integer or null, but got {dt}")
            }
        }

        let empty_schema = DFSchema::empty();
        let new_fetch = limit
            .fetch
            .map(|expr| coerce_limit_expr(*expr, &empty_schema, "LIMIT"))
            .transpose()?;
        let new_skip = limit
            .skip
            .map(|expr| coerce_limit_expr(*expr, &empty_schema, "OFFSET"))
            .transpose()?;
        Ok(LogicalPlan::Limit(Limit {
            input: limit.input,
            fetch: new_fetch.map(Box::new),
            skip: new_skip.map(Box::new),
        }))
    }

    fn coerce_join_filter(&self, expr: Expr) -> Result<Expr> {
        let expr_type = expr.get_type(self.schema)?;
        match expr_type {
            DataType::Boolean => Ok(expr),
            DataType::Null => expr.cast_to(&DataType::Boolean, self.schema),
            other => plan_err!("Join condition must be boolean type, but got {other:?}"),
        }
    }

    fn coerce_binary_op(
        &self,
        left: Expr,
        left_schema: &DFSchema,
        op: Operator,
        right: Expr,
        right_schema: &DFSchema,
    ) -> Result<(Expr, Expr)> {
        let (left_type, right_type) = BinaryTypeCoercer::new(
            &left.get_type(left_schema)?,
            &op,
            &right.get_type(right_schema)?,
        )
        .get_input_types()?;

        Ok((
            left.cast_to(&left_type, left_schema)?,
            right.cast_to(&right_type, right_schema)?,
        ))
    }
}

impl TreeNodeRewriter for TypeCoercionRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::Unnest(_) => not_impl_err!(
                "Unnest should be rewritten to LogicalPlan::Unnest before type coercion"
            ),
            Expr::ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns,
                spans,
            }) => {
                let new_plan =
                    analyze_internal(self.schema, Arc::unwrap_or_clone(subquery))?.data;
                Ok(Transformed::yes(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                    spans,
                })))
            }
            Expr::Exists(Exists { subquery, negated }) => {
                let new_plan = analyze_internal(
                    self.schema,
                    Arc::unwrap_or_clone(subquery.subquery),
                )?
                .data;
                Ok(Transformed::yes(Expr::Exists(Exists {
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                        spans: subquery.spans,
                    },
                    negated,
                })))
            }
            Expr::InSubquery(InSubquery {
                expr,
                subquery,
                negated,
            }) => {
                let new_plan = analyze_internal(
                    self.schema,
                    Arc::unwrap_or_clone(subquery.subquery),
                )?
                .data;
                let expr_type = expr.get_type(self.schema)?;
                let subquery_type = new_plan.schema().field(0).data_type();
                let common_type = comparison_coercion(&expr_type, subquery_type).ok_or(
                    plan_datafusion_err!(
                    "expr type {expr_type} can't cast to {subquery_type} in InSubquery"
                ),
                )?;
                let new_subquery = Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns: subquery.outer_ref_columns,
                    spans: subquery.spans,
                };
                Ok(Transformed::yes(Expr::InSubquery(InSubquery::new(
                    Box::new(expr.cast_to(&common_type, self.schema)?),
                    cast_subquery(new_subquery, &common_type)?,
                    negated,
                ))))
            }
            Expr::Not(expr) => Ok(Transformed::yes(not(get_casted_expr_for_bool_op(
                *expr,
                self.schema,
            )?))),
            Expr::IsTrue(expr) => Ok(Transformed::yes(is_true(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsNotTrue(expr) => Ok(Transformed::yes(is_not_true(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsFalse(expr) => Ok(Transformed::yes(is_false(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsNotFalse(expr) => Ok(Transformed::yes(is_not_false(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsUnknown(expr) => Ok(Transformed::yes(is_unknown(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
            Expr::IsNotUnknown(expr) => Ok(Transformed::yes(is_not_unknown(
                get_casted_expr_for_bool_op(*expr, self.schema)?,
            ))),
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
                let expr = match left_type {
                    DataType::Dictionary(_, inner) if *inner == DataType::Utf8 => expr,
                    _ => Box::new(expr.cast_to(&coerced_type, self.schema)?),
                };
                let pattern = Box::new(pattern.cast_to(&coerced_type, self.schema)?);
                Ok(Transformed::yes(Expr::Like(Like::new(
                    negated,
                    expr,
                    pattern,
                    escape_char,
                    case_insensitive,
                ))))
            }
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                let (left, right) =
                    self.coerce_binary_op(*left, self.schema, op, *right, self.schema)?;
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(left),
                    op,
                    Box::new(right),
                ))))
            }
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
                        internal_datafusion_err!(
                            "Failed to coerce types {expr_type} and {low_type} in BETWEEN expression"
                        )
                    })?;
                let high_type = high.get_type(self.schema)?;
                let high_coerced_type = comparison_coercion(&expr_type, &high_type)
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                        )
                    })?;
                let coercion_type =
                    comparison_coercion(&low_coerced_type, &high_coerced_type)
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "Failed to coerce types {expr_type} and {high_type} in BETWEEN expression"
                            )
                        })?;
                Ok(Transformed::yes(Expr::Between(Between::new(
                    Box::new(expr.cast_to(&coercion_type, self.schema)?),
                    negated,
                    Box::new(low.cast_to(&coercion_type, self.schema)?),
                    Box::new(high.cast_to(&coercion_type, self.schema)?),
                ))))
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
                match result_type {
                    None => plan_err!(
                        "Can not find compatible types to compare {expr_data_type} with [{}]", list_data_types.iter().join(", ")
                    ),
                    Some(coerced_type) => {
                        // find the coerced type
                        let cast_expr = expr.cast_to(&coerced_type, self.schema)?;
                        let cast_list_expr = list
                            .into_iter()
                            .map(|list_expr| {
                                list_expr.cast_to(&coerced_type, self.schema)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Transformed::yes(Expr::InList(InList ::new(
                             Box::new(cast_expr),
                             cast_list_expr,
                            negated,
                        ))))
                    }
                }
            }
            Expr::Case(case) => {
                let case = coerce_case_expression(case, self.schema)?;
                Ok(Transformed::yes(Expr::Case(case)))
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let new_expr = coerce_arguments_for_signature_with_scalar_udf(
                    args,
                    self.schema,
                    &func,
                )?;
                Ok(Transformed::yes(Expr::ScalarFunction(
                    ScalarFunction::new_udf(func, new_expr),
                )))
            }
            Expr::AggregateFunction(expr::AggregateFunction {
                func,
                params:
                    AggregateFunctionParams {
                        args,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    },
            }) => {
                let new_expr = coerce_arguments_for_signature_with_aggregate_udf(
                    args,
                    self.schema,
                    &func,
                )?;
                Ok(Transformed::yes(Expr::AggregateFunction(
                    expr::AggregateFunction::new_udf(
                        func,
                        new_expr,
                        distinct,
                        filter,
                        order_by,
                        null_treatment,
                    ),
                )))
            }
            Expr::WindowFunction(window_fun) => {
                let WindowFunction {
                    fun,
                    params:
                        expr::WindowFunctionParams {
                            args,
                            partition_by,
                            order_by,
                            window_frame,
                            filter,
                            null_treatment,
                            distinct,
                        },
                } = *window_fun;
                let window_frame =
                    coerce_window_frame(window_frame, self.schema, &order_by)?;

                let args = match &fun {
                    expr::WindowFunctionDefinition::AggregateUDF(udf) => {
                        coerce_arguments_for_signature_with_aggregate_udf(
                            args,
                            self.schema,
                            udf,
                        )?
                    }
                    _ => args,
                };

                let new_expr = Expr::from(WindowFunction {
                    fun,
                    params: expr::WindowFunctionParams {
                        args,
                        partition_by,
                        order_by,
                        window_frame,
                        filter,
                        null_treatment,
                        distinct,
                    },
                });
                Ok(Transformed::yes(new_expr))
            }
            // TODO: remove the next line after `Expr::Wildcard` is removed
            #[expect(deprecated)]
            Expr::Alias(_)
            | Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::SimilarTo(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _) => Ok(Transformed::no(expr)),
        }
    }
}

/// Transform a schema to use non-view types for Utf8View and BinaryView
fn transform_schema_to_nonview(dfschema: &DFSchemaRef) -> Option<Result<DFSchema>> {
    let metadata = dfschema.as_arrow().metadata.clone();
    let mut transformed = false;

    let (qualifiers, transformed_fields): (Vec<Option<TableReference>>, Vec<Arc<Field>>) =
        dfschema
            .iter()
            .map(|(qualifier, field)| match field.data_type() {
                DataType::Utf8View => {
                    transformed = true;
                    (
                        qualifier.cloned() as Option<TableReference>,
                        Arc::new(Field::new(
                            field.name(),
                            DataType::LargeUtf8,
                            field.is_nullable(),
                        )),
                    )
                }
                DataType::BinaryView => {
                    transformed = true;
                    (
                        qualifier.cloned() as Option<TableReference>,
                        Arc::new(Field::new(
                            field.name(),
                            DataType::LargeBinary,
                            field.is_nullable(),
                        )),
                    )
                }
                _ => (
                    qualifier.cloned() as Option<TableReference>,
                    Arc::clone(field),
                ),
            })
            .unzip();

    if !transformed {
        return None;
    }

    let schema = Schema::new_with_metadata(transformed_fields, metadata);
    Some(DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(schema),
    ))
}

/// Casts the given `value` to `target_type`. Note that this function
/// only considers `Null` or `Utf8` values.
fn coerce_scalar(target_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        // Coerce Utf8 values:
        ScalarValue::Utf8(Some(val)) => {
            ScalarValue::try_from_string(val.clone(), target_type)
        }
        s => {
            if s.is_null() {
                // Coerce `Null` values:
                ScalarValue::try_from(target_type)
            } else {
                // Values except `Utf8`/`Null` variants already have the right type
                // (casted before) since we convert `sqlparser` outputs to `Utf8`
                // for all possible cases. Therefore, we return a clone here.
                Ok(s.clone())
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
    target_type: &DataType,
    value: &ScalarValue,
) -> Result<ScalarValue> {
    coerce_scalar(target_type, value).or_else(|err| {
        // If type coercion fails, check if the largest type in family works:
        if let Some(largest_type) = get_widest_type_in_family(target_type) {
            coerce_scalar(largest_type, value).map_or_else(
                |_| exec_err!("Cannot cast {value:?} to {target_type}"),
                |_| ScalarValue::try_from(target_type),
            )
        } else {
            Err(err)
        }
    })
}

/// This function returns the widest type in the family of `given_type`.
/// If the given type is already the widest type, it returns `None`.
/// For example, if `given_type` is `Int8`, it returns `Int64`.
fn get_widest_type_in_family(given_type: &DataType) -> Option<&DataType> {
    match given_type {
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Some(&DataType::UInt64),
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Some(&DataType::Int64),
        DataType::Float16 | DataType::Float32 => Some(&DataType::Float64),
        _ => None,
    }
}

/// Coerces the given (window frame) `bound` to `target_type`.
fn coerce_frame_bound(
    target_type: &DataType,
    bound: WindowFrameBound,
) -> Result<WindowFrameBound> {
    match bound {
        WindowFrameBound::Preceding(v) => {
            coerce_scalar_range_aware(target_type, &v).map(WindowFrameBound::Preceding)
        }
        WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        WindowFrameBound::Following(v) => {
            coerce_scalar_range_aware(target_type, &v).map(WindowFrameBound::Following)
        }
    }
}

fn extract_window_frame_target_type(col_type: &DataType) -> Result<DataType> {
    if col_type.is_numeric()
        || is_utf8_or_utf8view_or_large_utf8(col_type)
        || matches!(col_type, DataType::List(_))
        || matches!(col_type, DataType::LargeList(_))
        || matches!(col_type, DataType::FixedSizeList(_, _))
        || matches!(col_type, DataType::Null)
        || matches!(col_type, DataType::Boolean)
    {
        Ok(col_type.clone())
    } else if is_datetime(col_type) {
        Ok(DataType::Interval(IntervalUnit::MonthDayNano))
    } else if let DataType::Dictionary(_, value_type) = col_type {
        extract_window_frame_target_type(value_type)
    } else {
        internal_err!("Cannot run range queries on datatype: {col_type}")
    }
}

// Coerces the given `window_frame` to use appropriate natural types.
// For example, ROWS and GROUPS frames use `UInt64` during calculations.
fn coerce_window_frame(
    window_frame: WindowFrame,
    schema: &DFSchema,
    expressions: &[Sort],
) -> Result<WindowFrame> {
    let mut window_frame = window_frame;
    let target_type = match window_frame.units {
        WindowFrameUnits::Range => {
            let current_types = expressions
                .first()
                .map(|s| s.expr.get_type(schema))
                .transpose()?;
            if let Some(col_type) = current_types {
                extract_window_frame_target_type(&col_type)?
            } else {
                return internal_err!("ORDER BY column cannot be empty");
            }
        }
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => DataType::UInt64,
    };
    window_frame.start_bound =
        coerce_frame_bound(&target_type, window_frame.start_bound)?;
    window_frame.end_bound = coerce_frame_bound(&target_type, window_frame.end_bound)?;
    Ok(window_frame)
}

// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion.
// The above op will be rewrite to the binary op when creating the physical op.
fn get_casted_expr_for_bool_op(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let left_type = expr.get_type(schema)?;
    BinaryTypeCoercer::new(&left_type, &Operator::IsDistinctFrom, &DataType::Boolean)
        .get_input_types()?;
    expr.cast_to(&DataType::Boolean, schema)
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature_with_scalar_udf(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    func: &ScalarUDF,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(expressions);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types_with_scalar_udf(&current_types, func)?;

    expressions
        .into_iter()
        .enumerate()
        .map(|(i, expr)| expr.cast_to(&new_types[i], schema))
        .collect()
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature_with_aggregate_udf(
    expressions: Vec<Expr>,
    schema: &DFSchema,
    func: &AggregateUDF,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(expressions);
    }

    let current_fields = expressions
        .iter()
        .map(|e| e.to_field(schema).map(|(_, f)| f))
        .collect::<Result<Vec<_>>>()?;

    let new_types = fields_with_aggregate_udf(&current_fields, func)?
        .into_iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();

    expressions
        .into_iter()
        .enumerate()
        .map(|(i, expr)| expr.cast_to(&new_types[i], schema))
        .collect()
}

fn coerce_case_expression(case: Case, schema: &DFSchema) -> Result<Case> {
    // Given expressions like:
    //
    // CASE a1
    //   WHEN a2 THEN b1
    //   WHEN a3 THEN b2
    //   ELSE b3
    // END
    //
    // or:
    //
    // CASE
    //   WHEN x1 THEN b1
    //   WHEN x2 THEN b2
    //   ELSE b3
    // END
    //
    // Then all aN (a1, a2, a3) must be converted to a common data type in the first example
    // (case-when expression coercion)
    //
    // All xN (x1, x2) must be converted to a boolean data type in the second example
    // (when-boolean expression coercion)
    //
    // And all bN (b1, b2, b3) must be converted to a common data type in both examples
    // (then-else expression coercion)
    //
    // If any fail to find and cast to a common/specific data type, will return error
    //
    // Note that case-when and when-boolean expression coercions are mutually exclusive
    // Only one or the other can occur for a case expression, whilst then-else expression coercion will always occur

    // prepare types
    let case_type = case
        .expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;
    let then_types = case
        .when_then_expr
        .iter()
        .map(|(_when, then)| then.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let else_type = case
        .else_expr
        .as_ref()
        .map(|expr| expr.get_type(schema))
        .transpose()?;

    // find common coercible types
    let case_when_coerce_type = case_type
        .as_ref()
        .map(|case_type| {
            let when_types = case
                .when_then_expr
                .iter()
                .map(|(when, _then)| when.get_type(schema))
                .collect::<Result<Vec<_>>>()?;
            let coerced_type =
                get_coerce_type_for_case_expression(&when_types, Some(case_type));
            coerced_type.ok_or_else(|| {
                plan_datafusion_err!(
                    "Failed to coerce case ({case_type}) and when ({}) \
                     to common types in CASE WHEN expression",
                    when_types.iter().join(", ")
                )
            })
        })
        .transpose()?;
    let then_else_coerce_type =
        get_coerce_type_for_case_expression(&then_types, else_type.as_ref()).ok_or_else(
            || {
                if let Some(else_type) = else_type {
                    plan_datafusion_err!(
                        "Failed to coerce then ({}) and else ({else_type}) \
                         to common types in CASE WHEN expression",
                        then_types.iter().join(", ")
                    )
                } else {
                    plan_datafusion_err!(
                        "Failed to coerce then ({}) and else (None) \
                         to common types in CASE WHEN expression",
                        then_types.iter().join(", ")
                    )
                }
            },
        )?;

    // do cast if found common coercible types
    let case_expr = case
        .expr
        .zip(case_when_coerce_type.as_ref())
        .map(|(case_expr, coercible_type)| case_expr.cast_to(coercible_type, schema))
        .transpose()?
        .map(Box::new);
    let when_then = case
        .when_then_expr
        .into_iter()
        .map(|(when, then)| {
            let when_type = case_when_coerce_type.as_ref().unwrap_or(&DataType::Boolean);
            let when = when.cast_to(when_type, schema).map_err(|e| {
                DataFusionError::Context(
                    format!(
                        "WHEN expressions in CASE couldn't be \
                         converted to common type ({when_type})"
                    ),
                    Box::new(e),
                )
            })?;
            let then = then.cast_to(&then_else_coerce_type, schema)?;
            Ok((Box::new(when), Box::new(then)))
        })
        .collect::<Result<Vec<_>>>()?;
    let else_expr = case
        .else_expr
        .map(|expr| expr.cast_to(&then_else_coerce_type, schema))
        .transpose()?
        .map(Box::new);

    Ok(Case::new(case_expr, when_then, else_expr))
}

/// Get a common schema that is compatible with all inputs of UNION.
///
/// This method presumes that the wildcard expansion is unneeded, or has already
/// been applied.
///
/// ## Schema and Field Handling in Union Coercion
///
/// **Processing order**: The function starts with the base schema (first input) and then
/// processes remaining inputs sequentially, with later inputs taking precedence in merging.
///
/// **Schema-level metadata merging**: Later schemas take precedence for duplicate keys.
///
/// **Field-level metadata merging**: Later fields take precedence for duplicate metadata keys.
///
/// **Type coercion precedence**: The coerced type is determined by iteratively applying
/// `comparison_coercion()` between the accumulated type and each new input's type. The
/// result depends on type coercion rules, not input order.
///
/// **Nullability merging**: Nullability is accumulated using logical OR (`||`).
/// Once any input field is nullable, the result field becomes nullable permanently.
/// Later inputs can make a field nullable but cannot make it non-nullable.
///
/// **Field precedence**: Field names come from the first (base) schema, but the field properties
/// (nullability and field-level metadata) have later schemas taking precedence.
///
/// **Example**:
/// ```sql
/// SELECT a, b FROM table1  -- a: Int32, metadata {"source": "t1"}, nullable=false
/// UNION
/// SELECT a, b FROM table2  -- a: Int64, metadata {"source": "t2"}, nullable=true
/// UNION
/// SELECT a, b FROM table3  -- a: Int32, metadata {"encoding": "utf8"}, nullable=false
/// -- Result:
/// -- a: Int64 (from type coercion), nullable=true (from table2),
/// -- metadata: {"source": "t2", "encoding": "utf8"} (later inputs take precedence)
/// ```
///
/// **Precedence Summary**:
/// - **Datatypes**: Determined by `comparison_coercion()` rules, not input order
/// - **Nullability**: Later inputs can add nullability but cannot remove it (logical OR)
/// - **Metadata**: Later inputs take precedence for same keys (HashMap::extend semantics)
pub fn coerce_union_schema(inputs: &[Arc<LogicalPlan>]) -> Result<DFSchema> {
    coerce_union_schema_with_schema(&inputs[1..], inputs[0].schema())
}
fn coerce_union_schema_with_schema(
    inputs: &[Arc<LogicalPlan>],
    base_schema: &DFSchemaRef,
) -> Result<DFSchema> {
    let mut union_datatypes = base_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut union_nullabilities = base_schema
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();
    let mut union_field_meta = base_schema
        .fields()
        .iter()
        .map(|f| f.metadata().clone())
        .collect::<Vec<_>>();

    let mut metadata = base_schema.metadata().clone();

    for (i, plan) in inputs.iter().enumerate() {
        let plan_schema = plan.schema();
        metadata.extend(plan_schema.metadata().clone());

        if plan_schema.fields().len() != base_schema.fields().len() {
            return plan_err!(
                "Union schemas have different number of fields: \
                query 1 has {} fields whereas query {} has {} fields",
                base_schema.fields().len(),
                i + 1,
                plan_schema.fields().len()
            );
        }

        // coerce data type and nullability for each field
        for (union_datatype, union_nullable, union_field_map, plan_field) in izip!(
            union_datatypes.iter_mut(),
            union_nullabilities.iter_mut(),
            union_field_meta.iter_mut(),
            plan_schema.fields().iter()
        ) {
            let coerced_type =
                comparison_coercion(union_datatype, plan_field.data_type()).ok_or_else(
                    || {
                        plan_datafusion_err!(
                            "Incompatible inputs for Union: Previous inputs were \
                            of type {}, but got incompatible type {} on column '{}'",
                            union_datatype,
                            plan_field.data_type(),
                            plan_field.name()
                        )
                    },
                )?;

            *union_datatype = coerced_type;
            *union_nullable = *union_nullable || plan_field.is_nullable();
            union_field_map.extend(plan_field.metadata().clone());
        }
    }
    let union_qualified_fields = izip!(
        base_schema.fields(),
        union_datatypes.into_iter(),
        union_nullabilities,
        union_field_meta.into_iter()
    )
    .map(|(field, datatype, nullable, metadata)| {
        let mut field = Field::new(field.name().clone(), datatype, nullable);
        field.set_metadata(metadata);
        (None, field.into())
    })
    .collect::<Vec<_>>();

    DFSchema::new_with_metadata(union_qualified_fields, metadata)
}

/// See `<https://github.com/apache/datafusion/pull/2108>`
fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            Expr::Alias(Alias { ref name, .. }) if name != schema.field(i).name() => {
                Ok(e.unalias().alias(schema.field(i).name()))
            }
            Expr::Column(Column {
                relation: _,
                ref name,
                spans: _,
            }) if name != schema.field(i).name() => Ok(e.alias(schema.field(i).name())),
            Expr::Alias { .. } | Expr::Column { .. } => Ok(e),
            #[expect(deprecated)]
            Expr::Wildcard { .. } => {
                plan_err!("Wildcard should be expanded before type coercion")
            }
            _ => Ok(e.alias(schema.field(i).name())),
        })
        .collect::<Result<Vec<_>>>()?;

    Projection::try_new_with_schema(alias_expr, input, schema)
        .map(LogicalPlan::Projection)
}

#[cfg(test)]
mod test {
    use std::any::Any;
    use std::sync::Arc;

    use arrow::datatypes::DataType::Utf8;
    use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, TimeUnit};
    use insta::assert_snapshot;

    use crate::analyzer::type_coercion::{
        coerce_case_expression, TypeCoercion, TypeCoercionRewriter,
    };
    use crate::analyzer::Analyzer;
    use crate::assert_analyzed_plan_with_config_eq_snapshot;
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::tree_node::{TransformedResult, TreeNode};
    use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue, Spans};
    use datafusion_expr::expr::{self, InSubquery, Like, ScalarFunction};
    use datafusion_expr::logical_plan::{EmptyRelation, Projection, Sort};
    use datafusion_expr::test::function_stub::avg_udaf;
    use datafusion_expr::{
        cast, col, create_udaf, is_true, lit, AccumulatorFactoryFunction, AggregateUDF,
        BinaryExpr, Case, ColumnarValue, Expr, ExprSchemable, Filter, LogicalPlan,
        Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        SimpleAggregateUDF, Subquery, Union, Volatility,
    };
    use datafusion_functions_aggregate::average::AvgAccumulator;
    use datafusion_sql::TableReference;

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
                DFSchema::from_unqualified_fields(
                    vec![Field::new("a", data_type, true)].into(),
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }))
    }

    macro_rules! assert_analyzed_plan_eq {
        (
            $plan: expr,
            @ $expected: literal $(,)?
        ) => {{
            let options = ConfigOptions::default();
            let rule = Arc::new(TypeCoercion::new());
            assert_analyzed_plan_with_config_eq_snapshot!(
                options,
                rule,
                $plan,
                @ $expected,
            )
            }};
    }

    macro_rules! coerce_on_output_if_viewtype {
        (
            $is_viewtype: expr,
            $plan: expr,
            @ $expected: literal $(,)?
        ) => {{
            let mut options = ConfigOptions::default();
            // coerce on output
            if $is_viewtype {options.optimizer.expand_views_at_output = true;}
            let rule = Arc::new(TypeCoercion::new());

            assert_analyzed_plan_with_config_eq_snapshot!(
                options,
                rule,
                $plan,
                @ $expected,
            )
        }};
    }

    fn assert_type_coercion_error(
        plan: LogicalPlan,
        expected_substr: &str,
    ) -> Result<()> {
        let options = ConfigOptions::default();
        let analyzer = Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())]);

        match analyzer.execute_and_check(plan, &options, |_, _| {}) {
            Ok(succeeded_plan) => {
                panic!(
                    "Expected a type coercion error, but analysis succeeded: \n{succeeded_plan:#?}"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains(expected_substr),
                    "Error did not contain expected substring.\n  expected to find: `{expected_substr}`\n  actual error: `{msg}`"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn simple_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a < CAST(UInt32(2) AS Float64)
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn test_coerce_union() -> Result<()> {
        let left_plan = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::try_from_qualified_schema(
                    TableReference::full("datafusion", "test", "foo"),
                    &Schema::new(vec![Field::new("a", DataType::Int32, false)]),
                )
                .unwrap(),
            ),
        }));
        let right_plan = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::try_from_qualified_schema(
                    TableReference::full("datafusion", "test", "foo"),
                    &Schema::new(vec![Field::new("a", DataType::Int64, false)]),
                )
                .unwrap(),
            ),
        }));
        let union = LogicalPlan::Union(Union::try_new_with_loose_types(vec![
            left_plan, right_plan,
        ])?);
        let analyzed_union = Analyzer::with_rules(vec![Arc::new(TypeCoercion::new())])
            .execute_and_check(union, &ConfigOptions::default(), |_, _| {})?;
        let top_level_plan = LogicalPlan::Projection(Projection::try_new(
            vec![col("a")],
            Arc::new(analyzed_union),
        )?);

        assert_analyzed_plan_eq!(
            top_level_plan,
            @r"
        Projection: a
          Union
            Projection: CAST(datafusion.test.foo.a AS Int64) AS a
              EmptyRelation: rows=0
            EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn coerce_utf8view_output() -> Result<()> {
        // Plan A
        // scenario: outermost utf8view projection
        let expr = col("a");
        let empty = empty_with_type(DataType::Utf8View);
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone()],
            Arc::clone(&empty),
        )?);

        // Plan A: no coerce
        coerce_on_output_if_viewtype!(
            false,
            plan.clone(),
            @r"
        Projection: a
          EmptyRelation: rows=0
        "
        )?;

        // Plan A: coerce requested: Utf8View => LargeUtf8
        coerce_on_output_if_viewtype!(
            true,
            plan.clone(),
            @r"
        Projection: CAST(a AS LargeUtf8)
          EmptyRelation: rows=0
        "
        )?;

        // Plan B
        // scenario: outermost bool projection
        let bool_expr = col("a").lt(lit("foo"));
        let bool_plan = LogicalPlan::Projection(Projection::try_new(
            vec![bool_expr],
            Arc::clone(&empty),
        )?);
        // Plan B: no coerce
        coerce_on_output_if_viewtype!(
            false,
            bool_plan.clone(),
            @r#"
        Projection: a < CAST(Utf8("foo") AS Utf8View)
          EmptyRelation: rows=0
        "#
        )?;

        coerce_on_output_if_viewtype!(
            false,
            plan.clone(),
            @r"
        Projection: a
          EmptyRelation: rows=0
        "
        )?;

        // Plan B: coerce requested: no coercion applied
        coerce_on_output_if_viewtype!(
            true,
            plan.clone(),
            @r"
        Projection: CAST(a AS LargeUtf8)
          EmptyRelation: rows=0
        "
        )?;

        // Plan C
        // scenario: with a non-projection root logical plan node
        let sort_expr = expr.sort(true, true);
        let sort_plan = LogicalPlan::Sort(Sort {
            expr: vec![sort_expr],
            input: Arc::new(plan),
            fetch: None,
        });

        // Plan C: no coerce
        coerce_on_output_if_viewtype!(
            false,
            sort_plan.clone(),
            @r"
        Sort: a ASC NULLS FIRST
          Projection: a
            EmptyRelation: rows=0
        "
        )?;

        // Plan C: coerce requested: Utf8View => LargeUtf8
        coerce_on_output_if_viewtype!(
            true,
            sort_plan.clone(),
            @r"
        Projection: CAST(a AS LargeUtf8)
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;

        // Plan D
        // scenario: two layers of projections with view types
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![col("a")],
            Arc::new(sort_plan),
        )?);
        // Plan D: no coerce
        coerce_on_output_if_viewtype!(
            false,
            plan.clone(),
            @r"
        Projection: a
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;
        // Plan B: coerce requested: Utf8View => LargeUtf8 only on outermost
        coerce_on_output_if_viewtype!(
            true,
            plan.clone(),
            @r"
        Projection: CAST(a AS LargeUtf8)
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;

        Ok(())
    }

    #[test]
    fn coerce_binaryview_output() -> Result<()> {
        // Plan A
        // scenario: outermost binaryview projection
        let expr = col("a");
        let empty = empty_with_type(DataType::BinaryView);
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone()],
            Arc::clone(&empty),
        )?);

        // Plan A: no coerce
        coerce_on_output_if_viewtype!(
            false,
            plan.clone(),
            @r"
        Projection: a
          EmptyRelation: rows=0
        "
        )?;

        // Plan A: coerce requested: BinaryView => LargeBinary
        coerce_on_output_if_viewtype!(
            true,
            plan.clone(),
            @r"
        Projection: CAST(a AS LargeBinary)
          EmptyRelation: rows=0
        "
        )?;

        // Plan B
        // scenario: outermost bool projection
        let bool_expr = col("a").lt(lit(vec![8, 1, 8, 1]));
        let bool_plan = LogicalPlan::Projection(Projection::try_new(
            vec![bool_expr],
            Arc::clone(&empty),
        )?);

        // Plan B: no coerce
        coerce_on_output_if_viewtype!(
            false,
            bool_plan.clone(),
            @r#"
        Projection: a < CAST(Binary("8,1,8,1") AS BinaryView)
          EmptyRelation: rows=0
        "#
        )?;

        // Plan B: coerce requested: no coercion applied
        coerce_on_output_if_viewtype!(
            true,
            bool_plan.clone(),
            @r#"
        Projection: a < CAST(Binary("8,1,8,1") AS BinaryView)
          EmptyRelation: rows=0
        "#
        )?;

        // Plan C
        // scenario: with a non-projection root logical plan node
        let sort_expr = expr.sort(true, true);
        let sort_plan = LogicalPlan::Sort(Sort {
            expr: vec![sort_expr],
            input: Arc::new(plan),
            fetch: None,
        });

        // Plan C: no coerce
        coerce_on_output_if_viewtype!(
            false,
            sort_plan.clone(),
            @r"
        Sort: a ASC NULLS FIRST
          Projection: a
            EmptyRelation: rows=0
        "
        )?;
        // Plan C: coerce requested: BinaryView => LargeBinary
        coerce_on_output_if_viewtype!(
            true,
            sort_plan.clone(),
            @r"
        Projection: CAST(a AS LargeBinary)
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;

        // Plan D
        // scenario: two layers of projections with view types
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![col("a")],
            Arc::new(sort_plan),
        )?);

        // Plan D: no coerce
        coerce_on_output_if_viewtype!(
            false,
            plan.clone(),
            @r"
        Projection: a
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;

        // Plan B: coerce requested: BinaryView => LargeBinary only on outermost
        coerce_on_output_if_viewtype!(
            true,
            plan.clone(),
            @r"
        Projection: CAST(a AS LargeBinary)
          Sort: a ASC NULLS FIRST
            Projection: a
              EmptyRelation: rows=0
        "
        )?;

        Ok(())
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = empty_with_type(DataType::Float64);

        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
        )?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a < CAST(UInt32(2) AS Float64) OR a < CAST(UInt32(2) AS Float64)
          EmptyRelation: rows=0
        "
        )
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
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
            Ok(Utf8)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
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

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: TestScalarUDF(CAST(Int32(123) AS Float32))
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn scalar_udf_invalid_input() -> Result<()> {
        let empty = empty();
        let udf = ScalarUDF::from(TestScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        })
        .call(vec![lit("Apple")]);
        Projection::try_new(vec![udf], empty)
            .expect_err("Expected an error due to incorrect function input");

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

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: TestScalarUDF(CAST(Int64(10) AS Float32))
          EmptyRelation: rows=0
        "
        )
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
            vec![],
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: MY_AVG(CAST(Int64(10) AS Float64))
          EmptyRelation: rows=0
        "
        )
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
                Field::new("count", DataType::UInt64, true).into(),
                Field::new("avg", DataType::Float64, true).into(),
            ],
        ));
        let udaf = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            Arc::new(my_avg),
            vec![lit("10")],
            false,
            None,
            vec![],
            None,
        ));

        let err = Projection::try_new(vec![udaf], empty).err().unwrap();
        assert!(
            err.strip_backtrace().starts_with("Error during planning: Failed to coerce arguments to satisfy a call to 'MY_AVG' function: coercion from Utf8 to the signature Uniform(1, [Float64]) failed")
        );
        Ok(())
    }

    #[test]
    fn agg_function_case() -> Result<()> {
        let empty = empty();
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            avg_udaf(),
            vec![lit(12f64)],
            false,
            None,
            vec![],
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: avg(Float64(12))
          EmptyRelation: rows=0
        "
        )?;

        let empty = empty_with_type(DataType::Int32);
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            avg_udaf(),
            vec![cast(col("a"), DataType::Float64)],
            false,
            None,
            vec![],
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: avg(CAST(a AS Float64))
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn agg_function_invalid_input_avg() -> Result<()> {
        let empty = empty();
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new_udf(
            avg_udaf(),
            vec![lit("1")],
            false,
            None,
            vec![],
            None,
        ));
        let err = Projection::try_new(vec![agg_expr], empty)
            .err()
            .unwrap()
            .strip_backtrace();
        assert!(err.starts_with("Error during planning: Failed to coerce arguments to satisfy a call to 'avg' function: coercion from Utf8 to the signature Uniform(1, [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, Float32, Float64]) failed"));
        Ok(())
    }

    #[test]
    fn binary_op_date32_op_interval() -> Result<()> {
        // CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("...")
        let expr = cast(lit("1998-03-18"), DataType::Date32)
            + lit(ScalarValue::new_interval_dt(123, 456));
        let empty = empty();
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("IntervalDayTime { days: 123, milliseconds: 456 }")
          EmptyRelation: rows=0
        "#
        )
    }

    #[test]
    fn inlist_case() -> Result<()> {
        // a in (1,4,8), a is int64
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IN ([CAST(Int32(1) AS Int64), CAST(Int8(4) AS Int64), Int64(8)])
          EmptyRelation: rows=0
        ")?;

        // a in (1,4,8), a is decimal
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::from_unqualified_fields(
                vec![Field::new("a", DataType::Decimal128(12, 4), true)].into(),
                std::collections::HashMap::new(),
            )?),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: CAST(a AS Decimal128(24, 4)) IN ([CAST(Int32(1) AS Decimal128(24, 4)), CAST(Int8(4) AS Decimal128(24, 4)), CAST(Int64(8) AS Decimal128(24, 4))])
          EmptyRelation: rows=0
        ")
    }

    #[test]
    fn between_case() -> Result<()> {
        let expr = col("a").between(
            lit("2002-05-08"),
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
        );
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Filter: CAST(a AS Date32) BETWEEN CAST(Utf8("2002-05-08") AS Date32) AND CAST(Utf8("2002-05-08") AS Date32) + IntervalYearMonth("1")
          EmptyRelation: rows=0
        "#
        )
    }

    #[test]
    fn between_infer_cheap_type() -> Result<()> {
        let expr = col("a").between(
            // (cast('2002-05-08' as date) + interval '1 months')
            cast(lit("2002-05-08"), DataType::Date32)
                + lit(ScalarValue::new_interval_ym(0, 1)),
            lit("2002-12-08"),
        );
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty)?);

        // TODO: we should cast col(a).
        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Filter: CAST(a AS Date32) BETWEEN CAST(Utf8("2002-05-08") AS Date32) + IntervalYearMonth("1") AND CAST(Utf8("2002-12-08") AS Date32)
          EmptyRelation: rows=0
        "#
        )
    }

    #[test]
    fn between_null() -> Result<()> {
        let expr = lit(ScalarValue::Null).between(lit(ScalarValue::Null), lit(2i64));
        let empty = empty();
        let plan = LogicalPlan::Filter(Filter::try_new(expr, empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Filter: CAST(NULL AS Int64) BETWEEN CAST(NULL AS Int64) AND Int64(2)
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn is_bool_for_type_coercion() -> Result<()> {
        // is true
        let expr = col("a").is_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS TRUE
          EmptyRelation: rows=0
        "
        )?;

        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        assert_type_coercion_error(
            plan,
            "Cannot infer common argument type for comparison operation Int64 IS DISTINCT FROM Boolean"
        )?;

        // is not true
        let expr = col("a").is_not_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS NOT TRUE
          EmptyRelation: rows=0
        "
        )?;

        // is false
        let expr = col("a").is_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS FALSE
          EmptyRelation: rows=0
        "
        )?;

        // is not false
        let expr = col("a").is_not_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS NOT FALSE
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn like_for_type_coercion() -> Result<()> {
        // like : utf8 like "abc"
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: a LIKE Utf8("abc")
          EmptyRelation: rows=0
        "#
        )?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a LIKE CAST(NULL AS Utf8)
          EmptyRelation: rows=0
        "
        )?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None, false));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        assert_type_coercion_error(
            plan,
            "There isn't a common type to coerce Int64 and Utf8 in LIKE expression",
        )?;

        // ilike
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: a ILIKE Utf8("abc")
          EmptyRelation: rows=0
        "#
        )?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a ILIKE CAST(NULL AS Utf8)
          EmptyRelation: rows=0
        "
        )?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::Like(Like::new(false, expr, pattern, None, true));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        assert_type_coercion_error(
            plan,
            "There isn't a common type to coerce Int64 and Utf8 in ILIKE expression",
        )?;

        Ok(())
    }

    #[test]
    fn unknown_for_type_coercion() -> Result<()> {
        // unknown
        let expr = col("a").is_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS UNKNOWN
          EmptyRelation: rows=0
        "
        )?;

        let empty = empty_with_type(Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        assert_type_coercion_error(
            plan,
            "Cannot infer common argument type for comparison operation Utf8 IS DISTINCT FROM Boolean"
        )?;

        // is not unknown
        let expr = col("a").is_not_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Projection: a IS NOT UNKNOWN
          EmptyRelation: rows=0
        "
        )
    }

    #[test]
    fn concat_for_type_coercion() -> Result<()> {
        let empty = empty_with_type(Utf8);
        let args = [col("a"), lit("b"), lit(true), lit(false), lit(13)];

        // concat-type signature
        let expr = ScalarUDF::new_from_impl(TestScalarUDF {
            signature: Signature::variadic(vec![Utf8], Volatility::Immutable),
        })
        .call(args.to_vec());
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr], Arc::clone(&empty))?);
        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: TestScalarUDF(a, Utf8("b"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))
          EmptyRelation: rows=0
        "#
        )
    }

    #[test]
    fn test_type_coercion_rewrite() -> Result<()> {
        // gt
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema: &schema };
        let expr = is_true(lit(12i32).gt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).gt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter).data()?;
        assert_eq!(expected, result);

        // eq
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema: &schema };
        let expr = is_true(lit(12i32).eq(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).eq(lit(13i64)));
        let result = expr.rewrite(&mut rewriter).data()?;
        assert_eq!(expected, result);

        // lt
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![Field::new("a", DataType::Int64, true)].into(),
            std::collections::HashMap::new(),
        )?);
        let mut rewriter = TypeCoercionRewriter { schema: &schema };
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

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: CAST(Utf8("1998-03-18") AS Timestamp(ns)) = CAST(CAST(Utf8("1998-03-18") AS Date32) AS Timestamp(ns))
          EmptyRelation: rows=0
        "#
        )
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
        case_when_type: &DataType,
        then_else_type: &DataType,
        schema: &DFSchemaRef,
    ) -> Case {
        let expr = case
            .expr
            .map(|e| cast_if_not_same_type(e, case_when_type, schema));
        let when_then_expr = case
            .when_then_expr
            .into_iter()
            .map(|(when, then)| {
                (
                    cast_if_not_same_type(when, case_when_type, schema),
                    cast_if_not_same_type(then, then_else_type, schema),
                )
            })
            .collect::<Vec<_>>();
        let else_expr = case
            .else_expr
            .map(|e| cast_if_not_same_type(e, then_else_type, schema));

        Case {
            expr,
            when_then_expr,
            else_expr,
        }
    }

    #[test]
    fn test_case_expression_coercion() -> Result<()> {
        let schema = Arc::new(DFSchema::from_unqualified_fields(
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
                Field::new("string", Utf8, true),
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
        let then_else_common_type = Utf8;
        let expected = cast_helper(
            case.clone(),
            &case_when_common_type,
            &then_else_common_type,
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
        let case_when_common_type = Utf8;
        let then_else_common_type = Utf8;
        let expected = cast_helper(
            case.clone(),
            &case_when_common_type,
            &then_else_common_type,
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
        assert_snapshot!(
            err.strip_backtrace(),
            @"Error during planning: Failed to coerce case (Interval(MonthDayNano)) and when (Float32, Binary, Utf8) to common types in CASE WHEN expression"
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
        assert_snapshot!(
            err.strip_backtrace(),
            @"Error during planning: Failed to coerce then (Date32, Float32, Binary) and else (Timestamp(ns)) to common types in CASE WHEN expression"
        );

        Ok(())
    }

    macro_rules! test_case_expression {
        ($expr:expr, $when_then:expr, $case_when_type:expr, $then_else_type:expr, $schema:expr) => {
            let case = Case {
                expr: $expr.map(|e| Box::new(col(e))),
                when_then_expr: $when_then,
                else_expr: None,
            };

            let expected =
                cast_helper(case.clone(), &$case_when_type, &$then_else_type, &$schema);

            let actual = coerce_case_expression(case, &$schema)?;
            assert_eq!(expected, actual);
        };
    }

    #[test]
    fn tes_case_when_list() -> Result<()> {
        let inner_field = Arc::new(Field::new_list_field(DataType::Int64, true));
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![
                Field::new(
                    "large_list",
                    DataType::LargeList(Arc::clone(&inner_field)),
                    true,
                ),
                Field::new(
                    "fixed_list",
                    DataType::FixedSizeList(Arc::clone(&inner_field), 3),
                    true,
                ),
                Field::new("list", DataType::List(inner_field), true),
            ]
            .into(),
            std::collections::HashMap::new(),
        )?);

        test_case_expression!(
            Some("list"),
            vec![(Box::new(col("large_list")), Box::new(lit("1")))],
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );

        test_case_expression!(
            Some("large_list"),
            vec![(Box::new(col("list")), Box::new(lit("1")))],
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );

        test_case_expression!(
            Some("list"),
            vec![(Box::new(col("fixed_list")), Box::new(lit("1")))],
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );

        test_case_expression!(
            Some("fixed_list"),
            vec![(Box::new(col("list")), Box::new(lit("1")))],
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );

        test_case_expression!(
            Some("fixed_list"),
            vec![(Box::new(col("large_list")), Box::new(lit("1")))],
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );

        test_case_expression!(
            Some("large_list"),
            vec![(Box::new(col("fixed_list")), Box::new(lit("1")))],
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            Utf8,
            schema
        );
        Ok(())
    }

    #[test]
    fn test_then_else_list() -> Result<()> {
        let inner_field = Arc::new(Field::new_list_field(DataType::Int64, true));
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![
                Field::new("boolean", DataType::Boolean, true),
                Field::new(
                    "large_list",
                    DataType::LargeList(Arc::clone(&inner_field)),
                    true,
                ),
                Field::new(
                    "fixed_list",
                    DataType::FixedSizeList(Arc::clone(&inner_field), 3),
                    true,
                ),
                Field::new("list", DataType::List(inner_field), true),
            ]
            .into(),
            std::collections::HashMap::new(),
        )?);

        // large list and list
        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("large_list"))),
                (Box::new(col("boolean")), Box::new(col("list")))
            ],
            DataType::Boolean,
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );

        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("list"))),
                (Box::new(col("boolean")), Box::new(col("large_list")))
            ],
            DataType::Boolean,
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );

        // fixed list and list
        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("fixed_list"))),
                (Box::new(col("boolean")), Box::new(col("list")))
            ],
            DataType::Boolean,
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );

        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("list"))),
                (Box::new(col("boolean")), Box::new(col("fixed_list")))
            ],
            DataType::Boolean,
            DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );

        // fixed list and large list
        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("fixed_list"))),
                (Box::new(col("boolean")), Box::new(col("large_list")))
            ],
            DataType::Boolean,
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );

        test_case_expression!(
            None::<String>,
            vec![
                (Box::new(col("boolean")), Box::new(col("large_list"))),
                (Box::new(col("boolean")), Box::new(col("fixed_list")))
            ],
            DataType::Boolean,
            DataType::LargeList(Arc::new(Field::new_list_field(DataType::Int64, true))),
            schema
        );
        Ok(())
    }

    #[test]
    fn test_map_with_diff_name() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new("key", Utf8, false));
        builder.push(Field::new("value", DataType::Float64, true));
        let struct_fields = builder.finish().fields;

        let fields =
            Field::new("entries", DataType::Struct(struct_fields.clone()), false);
        let map_type_entries = DataType::Map(Arc::new(fields), false);

        let fields = Field::new("key_value", DataType::Struct(struct_fields), false);
        let may_type_custom = DataType::Map(Arc::new(fields), false);

        let expr = col("a").eq(cast(col("a"), may_type_custom));
        let empty = empty_with_type(map_type_entries);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: a = CAST(CAST(a AS Map("key_value": non-null Struct("key": non-null Utf8, "value": Float64), unsorted)) AS Map("entries": non-null Struct("key": non-null Utf8, "value": Float64), unsorted))
          EmptyRelation: rows=0
        "#
        )
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

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: IntervalYearMonth("12") + CAST(Utf8("2000-01-01T00:00:00") AS Timestamp(ns))
          EmptyRelation: rows=0
        "#
        )
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

        assert_analyzed_plan_eq!(
            plan,
            @r#"
        Projection: CAST(Utf8("1998-03-18") AS Timestamp(ns)) - CAST(Utf8("1998-03-18") AS Timestamp(ns))
          EmptyRelation: rows=0
        "#
        )
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
                spans: Spans::new(),
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int64)?);
        // add cast for subquery

        assert_analyzed_plan_eq!(
            plan,
            @r"
        Filter: a IN (<subquery>)
          Subquery:
            Projection: CAST(a AS Int64)
              EmptyRelation: rows=0
          EmptyRelation: rows=0
        "
        )
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
                spans: Spans::new(),
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_int32)?);

        // add cast for subquery
        assert_analyzed_plan_eq!(
            plan,
            @r"
        Filter: CAST(a AS Int64) IN (<subquery>)
          Subquery:
            EmptyRelation: rows=0
          EmptyRelation: rows=0
        "
        )
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
                spans: Spans::new(),
            },
            false,
        ));
        let plan = LogicalPlan::Filter(Filter::try_new(in_subquery_expr, empty_outside)?);

        // add cast for subquery
        assert_analyzed_plan_eq!(
            plan,
            @r"
        Filter: CAST(a AS Decimal128(13, 8)) IN (<subquery>)
          Subquery:
            Projection: CAST(a AS Decimal128(13, 8))
              EmptyRelation: rows=0
          EmptyRelation: rows=0
        "
        )
    }
}
