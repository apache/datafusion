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

use datafusion_common::{
    parse_interval, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{self, Between, BinaryExpr, Case, Like, WindowFunction};
use datafusion_expr::expr_rewriter::{ExprRewriter, RewriteRecursion};
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{
    coerce_types, comparison_coercion, like_coercion,
};
use datafusion_expr::type_coercion::functions::data_types;
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_when, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{
    is_date, is_numeric, is_timestamp, is_utf8_or_large_utf8,
};
use datafusion_expr::utils::from_plan;
use datafusion_expr::{
    aggregate_function, function, is_false, is_not_false, is_not_true, is_not_unknown,
    is_true, is_unknown, type_coercion, AggregateFunction, Expr, LogicalPlan, Operator,
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion_expr::{ExprSchemable, Signature};

use crate::utils::{merge_schema, rewrite_preserving_name};
use crate::{OptimizerConfig, OptimizerRule};

#[derive(Default)]
pub struct TypeCoercion {}

impl TypeCoercion {
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for TypeCoercion {
    fn name(&self) -> &str {
        "type_coercion"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        Ok(Some(optimize_internal(&DFSchema::empty(), plan)?))
    }
}

fn optimize_internal(
    // use the external schema to handle the correlated subqueries case
    external_schema: &DFSchema,
    plan: &LogicalPlan,
) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| optimize_internal(external_schema, p))
        .collect::<Result<Vec<_>>>()?;
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = merge_schema(new_inputs.iter().collect());

    if let LogicalPlan::TableScan(ts) = plan {
        let source_schema =
            DFSchema::try_from_qualified_schema(&ts.table_name, &ts.source.schema())?;
        schema.merge(&source_schema);
    }

    // merge the outer schema for correlated subqueries
    // like case:
    // select t2.c2 from t1 where t1.c1 in (select t2.c1 from t2 where t2.c2=t1.c3)
    schema.merge(external_schema);

    let mut expr_rewrite = TypeCoercionRewriter {
        schema: Arc::new(schema),
    };

    let new_expr = plan
        .expressions()
        .into_iter()
        .map(|expr| {
            // ensure aggregate names don't change:
            // https://github.com/apache/arrow-datafusion/issues/3555
            rewrite_preserving_name(expr, &mut expr_rewrite)
        })
        .collect::<Result<Vec<_>>>()?;

    from_plan(plan, &new_expr, &new_inputs)
}

pub(crate) struct TypeCoercionRewriter {
    pub(crate) schema: DFSchemaRef,
}

impl ExprRewriter for TypeCoercionRewriter {
    fn pre_visit(&mut self, _expr: &Expr) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarSubquery(Subquery {
                subquery,
                outer_ref_columns,
            }) => {
                let new_plan = optimize_internal(&self.schema, &subquery)?;
                Ok(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(new_plan),
                    outer_ref_columns,
                }))
            }
            Expr::Exists { subquery, negated } => {
                let new_plan = optimize_internal(&self.schema, &subquery.subquery)?;
                Ok(Expr::Exists {
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                    },
                    negated,
                })
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let new_plan = optimize_internal(&self.schema, &subquery.subquery)?;
                Ok(Expr::InSubquery {
                    expr,
                    subquery: Subquery {
                        subquery: Arc::new(new_plan),
                        outer_ref_columns: subquery.outer_ref_columns,
                    },
                    negated,
                })
            }
            Expr::IsTrue(expr) => {
                let expr = is_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotTrue(expr) => {
                let expr = is_not_true(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsFalse(expr) => {
                let expr = is_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotFalse(expr) => {
                let expr =
                    is_not_false(get_casted_expr_for_bool_op(&expr, &self.schema)?);
                Ok(expr)
            }
            Expr::Like(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                let left_type = expr.get_type(&self.schema)?;
                let right_type = pattern.get_type(&self.schema)?;
                let coerced_type = like_coercion(&left_type,  &right_type).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "There isn't a common type to coerce {left_type} and {right_type} in LIKE expression"
                    ))
                })?;
                let expr = Box::new(expr.cast_to(&coerced_type, &self.schema)?);
                let pattern = Box::new(pattern.cast_to(&coerced_type, &self.schema)?);
                let expr = Expr::Like(Like::new(negated, expr, pattern, escape_char));
                Ok(expr)
            }
            Expr::ILike(Like {
                negated,
                expr,
                pattern,
                escape_char,
            }) => {
                let left_type = expr.get_type(&self.schema)?;
                let right_type = pattern.get_type(&self.schema)?;
                let coerced_type = like_coercion(&left_type,  &right_type).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "There isn't a common type to coerce {left_type} and {right_type} in ILIKE expression"
                    ))
                })?;
                let expr = Box::new(expr.cast_to(&coerced_type, &self.schema)?);
                let pattern = Box::new(pattern.cast_to(&coerced_type, &self.schema)?);
                let expr = Expr::ILike(Like::new(negated, expr, pattern, escape_char));
                Ok(expr)
            }
            Expr::IsUnknown(expr) => {
                // will convert the binary(expr,IsNotDistinctFrom,lit(Boolean(None));
                let left_type = expr.get_type(&self.schema)?;
                let right_type = DataType::Boolean;
                let coerced_type =
                    coerce_types(&left_type, &Operator::IsNotDistinctFrom, &right_type)?;
                let expr = is_unknown(expr.cast_to(&coerced_type, &self.schema)?);
                Ok(expr)
            }
            Expr::IsNotUnknown(expr) => {
                // will convert the binary(expr,IsDistinctFrom,lit(Boolean(None));
                let left_type = expr.get_type(&self.schema)?;
                let right_type = DataType::Boolean;
                let coerced_type =
                    coerce_types(&left_type, &Operator::IsDistinctFrom, &right_type)?;
                let expr = is_not_unknown(expr.cast_to(&coerced_type, &self.schema)?);
                Ok(expr)
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                op,
                ref right,
            }) => {
                let left_type = left.get_type(&self.schema)?;
                let right_type = right.get_type(&self.schema)?;
                match (&left_type, &right_type) {
                    (
                        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _),
                        &DataType::Interval(_),
                    ) => {
                        // this is a workaround for https://github.com/apache/arrow-datafusion/issues/3419
                        Ok(expr.clone())
                    }
                    _ => {
                        let coerced_type = coerce_types(&left_type, &op, &right_type)?;
                        let expr = Expr::BinaryExpr(BinaryExpr::new(
                            Box::new(left.clone().cast_to(&coerced_type, &self.schema)?),
                            op,
                            Box::new(right.clone().cast_to(&coerced_type, &self.schema)?),
                        ));
                        Ok(expr)
                    }
                }
            }
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr_type = expr.get_type(&self.schema)?;
                let low_type = low.get_type(&self.schema)?;
                let low_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {expr_type} and {low_type} in BETWEEN expression"
                        ))
                    })?;
                let high_type = high.get_type(&self.schema)?;
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
                let expr = Expr::Between(Between::new(
                    Box::new(expr.cast_to(&coercion_type, &self.schema)?),
                    negated,
                    Box::new(low.cast_to(&coercion_type, &self.schema)?),
                    Box::new(high.cast_to(&coercion_type, &self.schema)?),
                ));
                Ok(expr)
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr_data_type = expr.get_type(&self.schema)?;
                let list_data_types = list
                    .iter()
                    .map(|list_expr| list_expr.get_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let result_type =
                    get_coerce_type_for_list(&expr_data_type, &list_data_types);
                match result_type {
                    None => Err(DataFusionError::Plan(format!(
                        "Can not find compatible types to compare {expr_data_type:?} with {list_data_types:?}"
                    ))),
                    Some(coerced_type) => {
                        // find the coerced type
                        let cast_expr = expr.cast_to(&coerced_type, &self.schema)?;
                        let cast_list_expr = list
                            .into_iter()
                            .map(|list_expr| {
                                list_expr.cast_to(&coerced_type, &self.schema)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let expr = Expr::InList {
                            expr: Box::new(cast_expr),
                            list: cast_list_expr,
                            negated,
                        };
                        Ok(expr)
                    }
                }
            }
            Expr::Case(case) => {
                // all the result of then and else should be convert to a common data type,
                // if they can be coercible to a common data type, return error.
                let then_types = case
                    .when_then_expr
                    .iter()
                    .map(|when_then| when_then.1.get_type(&self.schema))
                    .collect::<Result<Vec<_>>>()?;
                let else_type = match &case.else_expr {
                    None => Ok(None),
                    Some(expr) => expr.get_type(&self.schema).map(Some),
                }?;
                let case_when_coerce_type =
                    get_coerce_type_for_case_when(&then_types, else_type.as_ref());
                match case_when_coerce_type {
                    None => Err(DataFusionError::Internal(format!(
                        "Failed to coerce then ({then_types:?}) and else ({else_type:?}) to common types in CASE WHEN expression"
                    ))),
                    Some(data_type) => {
                        let left = case.when_then_expr
                            .into_iter()
                            .map(|(when, then)| {
                                let then = then.cast_to(&data_type, &self.schema)?;
                                Ok((when, Box::new(then)))
                            })
                            .collect::<Result<Vec<_>>>()?;
                        let right = match &case.else_expr {
                            None => None,
                            Some(expr) => {
                                Some(Box::new(expr.clone().cast_to(&data_type, &self.schema)?))
                            }
                        };
                        Ok(Expr::Case(Case::new(case.expr,left,right)))
                    }
                }
            }
            Expr::ScalarUDF { fun, args } => {
                let new_expr = coerce_arguments_for_signature(
                    args.as_slice(),
                    &self.schema,
                    &fun.signature,
                )?;
                let expr = Expr::ScalarUDF {
                    fun,
                    args: new_expr,
                };
                Ok(expr)
            }
            Expr::ScalarFunction { fun, args } => {
                let nex_expr = coerce_arguments_for_signature(
                    args.as_slice(),
                    &self.schema,
                    &function::signature(&fun),
                )?;
                let expr = Expr::ScalarFunction {
                    fun,
                    args: nex_expr,
                };
                Ok(expr)
            }
            Expr::AggregateFunction(expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter,
            }) => {
                let new_expr = coerce_agg_exprs_for_signature(
                    &fun,
                    &args,
                    &self.schema,
                    &aggregate_function::signature(&fun),
                )?;
                let expr = Expr::AggregateFunction(expr::AggregateFunction::new(
                    fun, new_expr, distinct, filter,
                ));
                Ok(expr)
            }
            Expr::AggregateUDF { fun, args, filter } => {
                let new_expr = coerce_arguments_for_signature(
                    args.as_slice(),
                    &self.schema,
                    &fun.signature,
                )?;
                let expr = Expr::AggregateUDF {
                    fun,
                    args: new_expr,
                    filter,
                };
                Ok(expr)
            }
            Expr::WindowFunction(WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            }) => {
                let window_frame =
                    coerce_window_frame(window_frame, &self.schema, &order_by)?;
                let expr = Expr::WindowFunction(WindowFunction::new(
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                ));
                Ok(expr)
            }
            expr => Ok(expr),
        }
    }
}

/// Casts the given `value` to `target_type`. Note that this function
/// only considers `Null` or `Utf8` values.
fn coerce_scalar(target_type: &DataType, value: &ScalarValue) -> Result<ScalarValue> {
    match value {
        // Coerce Utf8 values:
        ScalarValue::Utf8(Some(val)) => {
            // When `target_type` is `Interval`, we use `parse_interval` since
            // `try_from_string` does not support `String` to `Interval` coercions.
            if let DataType::Interval(..) = target_type {
                parse_interval("millisecond", val)
            } else {
                ScalarValue::try_from_string(val.clone(), target_type)
            }
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
                |_| {
                    Err(DataFusionError::Execution(format!(
                        "Cannot cast {value:?} to {target_type:?}"
                    )))
                },
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
    bound: &WindowFrameBound,
) -> Result<WindowFrameBound> {
    match bound {
        WindowFrameBound::Preceding(v) => {
            coerce_scalar_range_aware(target_type, v).map(WindowFrameBound::Preceding)
        }
        WindowFrameBound::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        WindowFrameBound::Following(v) => {
            coerce_scalar_range_aware(target_type, v).map(WindowFrameBound::Following)
        }
    }
}

// Coerces the given `window_frame` to use appropriate natural types.
// For example, ROWS and GROUPS frames use `UInt64` during calculations.
fn coerce_window_frame(
    window_frame: WindowFrame,
    schema: &DFSchemaRef,
    expressions: &[Expr],
) -> Result<WindowFrame> {
    let mut window_frame = window_frame;
    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;
    let target_type = match window_frame.units {
        WindowFrameUnits::Range => {
            if let Some(col_type) = current_types.first() {
                if is_numeric(col_type) || is_utf8_or_large_utf8(col_type) {
                    col_type
                } else if is_timestamp(col_type) || is_date(col_type) {
                    &DataType::Interval(IntervalUnit::MonthDayNano)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot run range queries on datatype: {col_type:?}"
                    )));
                }
            } else {
                return Err(DataFusionError::Internal(
                    "ORDER BY column cannot be empty".to_string(),
                ));
            }
        }
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => &DataType::UInt64,
    };
    window_frame.start_bound =
        coerce_frame_bound(target_type, &window_frame.start_bound)?;
    window_frame.end_bound = coerce_frame_bound(target_type, &window_frame.end_bound)?;
    Ok(window_frame)
}

// Support the `IsTrue` `IsNotTrue` `IsFalse` `IsNotFalse` type coercion.
// The above op will be rewrite to the binary op when creating the physical op.
fn get_casted_expr_for_bool_op(expr: &Expr, schema: &DFSchemaRef) -> Result<Expr> {
    let left_type = expr.get_type(schema)?;
    let right_type = DataType::Boolean;
    let coerced_type = coerce_types(&left_type, &Operator::IsDistinctFrom, &right_type)?;
    expr.clone().cast_to(&coerced_type, schema)
}

/// Returns `expressions` coerced to types compatible with
/// `signature`, if possible.
///
/// See the module level documentation for more detail on coercion.
fn coerce_arguments_for_signature(
    expressions: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if expressions.is_empty() {
        return Ok(vec![]);
    }

    let current_types = expressions
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let new_types = data_types(&current_types, signature)?;

    expressions
        .iter()
        .enumerate()
        .map(|(i, expr)| cast_expr(expr, &new_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

/// Cast `expr` to the specified type, if possible
fn cast_expr(expr: &Expr, to_type: &DataType, schema: &DFSchema) -> Result<Expr> {
    // Special case until Interval coercion is handled in arrow-rs
    // https://github.com/apache/arrow-rs/issues/3643
    match (expr, to_type) {
        (Expr::Literal(ScalarValue::Utf8(Some(s))), DataType::Interval(_)) => {
            parse_interval("millisecond", s.as_str()).map(Expr::Literal)
        }
        _ => expr.clone().cast_to(to_type, schema),
    }
}

/// Returns the coerced exprs for each `input_exprs`.
/// Get the coerced data type from `aggregate_rule::coerce_types` and add `try_cast` if the
/// data type of `input_exprs` need to be coerced.
fn coerce_agg_exprs_for_signature(
    agg_fun: &AggregateFunction,
    input_exprs: &[Expr],
    schema: &DFSchema,
    signature: &Signature,
) -> Result<Vec<Expr>> {
    if input_exprs.is_empty() {
        return Ok(vec![]);
    }
    let current_types = input_exprs
        .iter()
        .map(|e| e.get_type(schema))
        .collect::<Result<Vec<_>>>()?;

    let coerced_types =
        type_coercion::aggregates::coerce_types(agg_fun, &current_types, signature)?;

    input_exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| expr.clone().cast_to(&coerced_types[i], schema))
        .collect::<Result<Vec<_>>>()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use datafusion_common::{DFField, DFSchema, Result, ScalarValue};
    use datafusion_expr::expr::{self, Like};
    use datafusion_expr::expr_rewriter::ExprRewritable;
    use datafusion_expr::{
        cast, col, concat, concat_ws, create_udaf, is_true,
        AccumulatorFunctionImplementation, AggregateFunction, AggregateUDF,
        BuiltinScalarFunction, ColumnarValue, StateTypeFunction,
    };
    use datafusion_expr::{
        lit,
        logical_plan::{EmptyRelation, Projection},
        Expr, LogicalPlan, ReturnTypeFunction, ScalarFunctionImplementation, ScalarUDF,
        Signature, Volatility,
    };
    use datafusion_physical_expr::expressions::AvgAccumulator;

    use crate::type_coercion::{TypeCoercion, TypeCoercionRewriter};
    use crate::{OptimizerContext, OptimizerRule};

    fn assert_optimized_plan_eq(plan: &LogicalPlan, expected: &str) -> Result<()> {
        let rule = TypeCoercion::new();
        let config = OptimizerContext::default();
        let plan = rule.try_optimize(plan, &config)?.unwrap();
        assert_eq!(expected, &format!("{plan:?}"));
        Ok(())
    }

    #[test]
    fn simple_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified("a", DataType::Float64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64)\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified("a", DataType::Float64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
        )?);
        let expected = "Projection: a < CAST(UInt32(2) AS Float64) OR a < CAST(UInt32(2) AS Float64)\
            \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn scalar_udf() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation =
            Arc::new(move |_| Ok(ColumnarValue::Scalar(ScalarValue::new_utf8("a"))));
        let udf = Expr::ScalarUDF {
            fun: Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            args: vec![lit(123_i32)],
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty)?);
        let expected =
            "Projection: TestScalarUDF(CAST(Int32(123) AS Float32))\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn scalar_udf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Utf8)));
        let fun: ScalarFunctionImplementation = Arc::new(move |_| unimplemented!());
        let udf = Expr::ScalarUDF {
            fun: Arc::new(ScalarUDF::new(
                "TestScalarUDF",
                &Signature::uniform(1, vec![DataType::Int32], Volatility::Stable),
                &return_type,
                &fun,
            )),
            args: vec![lit("Apple")],
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty)?);
        let err = assert_optimized_plan_eq(&plan, "").err().unwrap();
        assert_eq!(
            "Plan(\"Coercion from [Utf8] to the signature Uniform(1, [Int32]) failed.\")",
            &format!("{err:?}")
        );
        Ok(())
    }

    #[test]
    fn scalar_function() -> Result<()> {
        let empty = empty();
        let lit_expr = lit(10i64);
        let fun: BuiltinScalarFunction = BuiltinScalarFunction::Abs;
        let scalar_function_expr = Expr::ScalarFunction {
            fun,
            args: vec![lit_expr],
        };
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![scalar_function_expr],
            empty,
        )?);
        let expected = "Projection: abs(CAST(Int64(10) AS Float64))\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn agg_udaf() -> Result<()> {
        let empty = empty();
        let my_avg = create_udaf(
            "MY_AVG",
            DataType::Float64,
            Arc::new(DataType::Float64),
            Volatility::Immutable,
            Arc::new(|_| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?))),
            Arc::new(vec![DataType::UInt64, DataType::Float64]),
        );
        let udaf = Expr::AggregateUDF {
            fun: Arc::new(my_avg),
            args: vec![lit(10i64)],
            filter: None,
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let expected = "Projection: MY_AVG(CAST(Int64(10) AS Float64))\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)
    }

    #[test]
    fn agg_udaf_invalid_input() -> Result<()> {
        let empty = empty();
        let return_type: ReturnTypeFunction =
            Arc::new(move |_| Ok(Arc::new(DataType::Float64)));
        let state_type: StateTypeFunction =
            Arc::new(move |_| Ok(Arc::new(vec![DataType::UInt64, DataType::Float64])));
        let accumulator: AccumulatorFunctionImplementation =
            Arc::new(|_| Ok(Box::new(AvgAccumulator::try_new(&DataType::Float64)?)));
        let my_avg = AggregateUDF::new(
            "MY_AVG",
            &Signature::uniform(1, vec![DataType::Float64], Volatility::Immutable),
            &return_type,
            &accumulator,
            &state_type,
        );
        let udaf = Expr::AggregateUDF {
            fun: Arc::new(my_avg),
            args: vec![lit("10")],
            filter: None,
        };
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty)?);
        let err = assert_optimized_plan_eq(&plan, "").err().unwrap();
        assert_eq!(
            "Plan(\"Coercion from [Utf8] to the signature Uniform(1, [Float64]) failed.\")",
            &format!("{err:?}")
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
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(Int64(12))\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let empty = empty_with_type(DataType::Int32);
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![col("a")],
            false,
            None,
        ));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty)?);
        let expected = "Projection: AVG(a)\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;
        Ok(())
    }

    #[test]
    fn agg_function_invalid_input() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction(expr::AggregateFunction::new(
            fun,
            vec![lit("1")],
            false,
            None,
        ));
        let err = Projection::try_new(vec![agg_expr], empty).err().unwrap();
        assert_eq!(
            "Plan(\"The function Avg does not support inputs of type Utf8.\")",
            &format!("{err:?}")
        );
        Ok(())
    }

    #[test]
    fn binary_op_date32_add_interval() -> Result<()> {
        //CAST(Utf8("1998-03-18") AS Date32) + IntervalDayTime("386547056640")
        let expr = cast(lit("1998-03-18"), DataType::Date32)
            + lit(ScalarValue::IntervalDayTime(Some(386547056640)));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Date32) + IntervalDayTime(\"386547056640\")\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;
        Ok(())
    }

    #[test]
    fn inlist_case() -> Result<()> {
        // a in (1,4,8), a is int64
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified("a", DataType::Int64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: a IN ([CAST(Int32(1) AS Int64), CAST(Int8(4) AS Int64), Int64(8)]) AS a IN (Map { iter: Iter([Int32(1), Int8(4), Int64(8)]) })\
             \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        // a in (1,4,8), a is decimal
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified(
                        "a",
                        DataType::Decimal128(12, 4),
                        true,
                    )],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected =
            "Projection: CAST(a AS Decimal128(24, 4)) IN ([CAST(Int32(1) AS Decimal128(24, 4)), CAST(Int8(4) AS Decimal128(24, 4)), CAST(Int64(8) AS Decimal128(24, 4))]) AS a IN (Map { iter: Iter([Int32(1), Int8(4), Int64(8)]) })\
             \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;
        Ok(())
    }

    #[test]
    fn is_bool_for_type_coercion() -> Result<()> {
        // is true
        let expr = col("a").is_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![expr.clone()], empty)?);
        let expected = "Projection: a IS TRUE\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let err = assert_optimized_plan_eq(&plan, "");
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("'Int64 IS DISTINCT FROM Boolean' can't be evaluated because there isn't a common type to coerce the types to"));

        // is not true
        let expr = col("a").is_not_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT TRUE\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        // is false
        let expr = col("a").is_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS FALSE\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        // is not false
        let expr = col("a").is_not_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT FALSE\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        Ok(())
    }

    #[test]
    fn like_for_type_coercion() -> Result<()> {
        // like : utf8 like "abc"
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let expected = "Projection: a LIKE CAST(NULL AS Utf8) AS a LIKE NULL \
             \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty)?);
        let err = assert_optimized_plan_eq(&plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains(
            "There isn't a common type to coerce Int64 and Utf8 in LIKE expression"
        ));

        // ilike
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::ILike(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE Utf8(\"abc\")\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let ilike_expr = Expr::ILike(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let expected = "Projection: a ILIKE CAST(NULL AS Utf8) AS a ILIKE NULL \
             \n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let ilike_expr = Expr::ILike(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![ilike_expr], empty)?);
        let err = assert_optimized_plan_eq(&plan, expected);
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
        assert_optimized_plan_eq(&plan, expected)?;

        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let err = assert_optimized_plan_eq(&plan, expected);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("'Utf8 IS NOT DISTINCT FROM Boolean' can't be evaluated because there isn't a common type to coerce the types to"));

        // is not unknown
        let expr = col("a").is_not_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        let expected = "Projection: a IS NOT UNKNOWN\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;

        Ok(())
    }

    #[test]
    fn concat_for_type_coercion() -> Result<()> {
        let empty = empty_with_type(DataType::Utf8);
        let args = [col("a"), lit("b"), lit(true), lit(false), lit(13)];

        // concat
        {
            let expr = concat(&args);

            let plan =
                LogicalPlan::Projection(Projection::try_new(vec![expr], empty.clone())?);
            let expected =
                "Projection: concat(a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation";
            assert_optimized_plan_eq(&plan, expected)?;
        }

        // concat_ws
        {
            let expr = concat_ws(lit("-"), args.to_vec());

            let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
            let expected =
                "Projection: concatwithseparator(Utf8(\"-\"), a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation";
            assert_optimized_plan_eq(&plan, expected)?;
        }

        Ok(())
    }

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
                DFSchema::new_with_metadata(
                    vec![DFField::new_unqualified("a", data_type, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }))
    }

    #[test]
    fn test_type_coercion_rewrite() -> Result<()> {
        // gt
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new_unqualified("a", DataType::Int64, true)],
                std::collections::HashMap::new(),
            )
            .unwrap(),
        );
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).gt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).gt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        // eq
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new_unqualified("a", DataType::Int64, true)],
                std::collections::HashMap::new(),
            )
            .unwrap(),
        );
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).eq(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).eq(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        // lt
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new_unqualified("a", DataType::Int64, true)],
                std::collections::HashMap::new(),
            )
            .unwrap(),
        );
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).lt(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).lt(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);

        Ok(())
    }

    #[test]
    fn binary_op_date32_eq_ts() -> Result<()> {
        let expr = cast(
            lit("1998-03-18"),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        )
        .eq(cast(lit("1998-03-18"), DataType::Date32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty)?);
        dbg!(&plan);
        let expected =
            "Projection: CAST(Utf8(\"1998-03-18\") AS Timestamp(Nanosecond, None)) = CAST(CAST(Utf8(\"1998-03-18\") AS Date32) AS Timestamp(Nanosecond, None))\n  EmptyRelation";
        assert_optimized_plan_eq(&plan, expected)?;
        Ok(())
    }
}
