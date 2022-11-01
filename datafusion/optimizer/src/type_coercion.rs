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

use crate::utils::rewrite_preserving_name;
use crate::{OptimizerConfig, OptimizerRule};
use arrow::datatypes::{DataType, IntervalUnit};
use datafusion_common::{
    parse_interval, DFSchema, DFSchemaRef, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::expr::{Between, BinaryExpr, Case, Like};
use datafusion_expr::expr_rewriter::{ExprRewriter, RewriteRecursion};
use datafusion_expr::logical_plan::Subquery;
use datafusion_expr::type_coercion::binary::{coerce_types, comparison_coercion};
use datafusion_expr::type_coercion::functions::data_types;
use datafusion_expr::type_coercion::other::{
    get_coerce_type_for_case_when, get_coerce_type_for_list,
};
use datafusion_expr::type_coercion::{is_date, is_numeric, is_timestamp};
use datafusion_expr::utils::from_plan;
use datafusion_expr::{
    aggregate_function, function, is_false, is_not_false, is_not_true, is_not_unknown,
    is_true, is_unknown, type_coercion, AggregateFunction, Expr, LogicalPlan, Operator,
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use datafusion_expr::{ExprSchemable, Signature};
use std::sync::Arc;

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

    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        optimize_internal(&DFSchema::empty(), plan, optimizer_config)
    }
}

fn optimize_internal(
    // use the external schema to handle the correlated subqueries case
    external_schema: &DFSchema,
    plan: &LogicalPlan,
    optimizer_config: &mut OptimizerConfig,
) -> Result<LogicalPlan> {
    // optimize child plans first
    let new_inputs = plan
        .inputs()
        .iter()
        .map(|p| optimize_internal(external_schema, p, optimizer_config))
        .collect::<Result<Vec<_>>>()?;
    // get schema representing all available input fields. This is used for data type
    // resolution only, so order does not matter here
    let mut schema = new_inputs.iter().map(|input| input.schema()).fold(
        DFSchema::empty(),
        |mut lhs, rhs| {
            lhs.merge(rhs);
            lhs
        },
    );

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
            Expr::ScalarSubquery(Subquery { subquery }) => {
                let mut optimizer_config = OptimizerConfig::new();
                let new_plan =
                    optimize_internal(&self.schema, &subquery, &mut optimizer_config)?;
                Ok(Expr::ScalarSubquery(Subquery::new(new_plan)))
            }
            Expr::Exists { subquery, negated } => {
                let mut optimizer_config = OptimizerConfig::new();
                let new_plan = optimize_internal(
                    &self.schema,
                    &subquery.subquery,
                    &mut optimizer_config,
                )?;
                Ok(Expr::Exists {
                    subquery: Subquery::new(new_plan),
                    negated,
                })
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let mut optimizer_config = OptimizerConfig::new();
                let new_plan = optimize_internal(
                    &self.schema,
                    &subquery.subquery,
                    &mut optimizer_config,
                )?;
                Ok(Expr::InSubquery {
                    expr,
                    subquery: Subquery::new(new_plan),
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
                let coerced_type =
                    coerce_types(&left_type, &Operator::Like, &right_type)?;
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
                let coerced_type =
                    coerce_types(&left_type, &Operator::Like, &right_type)?;
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
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, low_type
                        ))
                    })?;
                let high_type = high.get_type(&self.schema)?;
                let high_coerced_type = comparison_coercion(&expr_type, &low_type)
                    .ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "Failed to coerce types {} and {} in BETWEEN expression",
                            expr_type, high_type
                        ))
                    })?;
                let coercion_type =
                    comparison_coercion(&low_coerced_type, &high_coerced_type)
                        .ok_or_else(|| {
                            DataFusionError::Internal(format!(
                                "Failed to coerce types {} and {} in BETWEEN expression",
                                expr_type, high_type
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
                        "Can not find compatible types to compare {:?} with {:?}",
                        expr_data_type, list_data_types
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
                    get_coerce_type_for_case_when(&then_types, &else_type);
                match case_when_coerce_type {
                    None => Err(DataFusionError::Internal(format!(
                        "Failed to coerce then ({:?}) and else ({:?}) to common types in CASE WHEN expression",
                        then_types, else_type
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
            Expr::AggregateFunction {
                fun,
                args,
                distinct,
                filter,
            } => {
                let new_expr = coerce_agg_exprs_for_signature(
                    &fun,
                    &args,
                    &self.schema,
                    &aggregate_function::signature(&fun),
                )?;
                let expr = Expr::AggregateFunction {
                    fun,
                    args: new_expr,
                    distinct,
                    filter,
                };
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
            Expr::WindowFunction {
                fun,
                args,
                partition_by,
                order_by,
                window_frame,
            } => {
                let window_frame =
                    get_coerced_window_frame(window_frame, &self.schema, &order_by)?;
                let expr = Expr::WindowFunction {
                    fun,
                    args,
                    partition_by,
                    order_by,
                    window_frame,
                };
                Ok(expr)
            }
            expr => Ok(expr),
        }
    }
}

/// Casts the ScalarValue `value` to coerced type.
// When coerced type is `Interval` we use `parse_interval` since `try_from_string` not
// supports conversion from string to Interval
fn convert_to_coerced_type(
    coerced_type: &DataType,
    value: &ScalarValue,
) -> Result<ScalarValue> {
    match value {
        // In here we do casting either for ScalarValue::Utf8(None) or
        // ScalarValue::Utf8(Some(val)). The other types are already casted.
        // The reason is that we convert the sqlparser result
        // to the Utf8 for all possible cases. Hence the types other than Utf8
        // are already casted to appropriate type. Therefore they can be returned directly.
        ScalarValue::Utf8(None) => ScalarValue::try_from(coerced_type),
        ScalarValue::Utf8(Some(val)) => {
            // we need special handling for Interval types
            if let DataType::Interval(..) = coerced_type {
                parse_interval("millisecond", val)
            } else {
                ScalarValue::try_from_string(val.clone(), coerced_type)
            }
        }
        s => Ok(s.clone()),
    }
}

fn coerce_frame_bound(
    coerced_type: &DataType,
    bound: &WindowFrameBound,
) -> Result<WindowFrameBound> {
    Ok(match bound {
        WindowFrameBound::Preceding(val) => {
            WindowFrameBound::Preceding(convert_to_coerced_type(coerced_type, val)?)
        }
        WindowFrameBound::CurrentRow => WindowFrameBound::CurrentRow,
        WindowFrameBound::Following(val) => {
            WindowFrameBound::Following(convert_to_coerced_type(coerced_type, val)?)
        }
    })
}

fn get_coerced_window_frame(
    window_frame: Option<WindowFrame>,
    schema: &DFSchemaRef,
    expressions: &[Expr],
) -> Result<Option<WindowFrame>> {
    fn get_coerced_type(column_type: &DataType) -> Result<DataType> {
        if is_numeric(column_type) {
            Ok(column_type.clone())
        } else if is_timestamp(column_type) || is_date(column_type) {
            Ok(DataType::Interval(IntervalUnit::MonthDayNano))
        } else {
            Err(DataFusionError::Internal(format!(
                "Cannot run range queries on datatype: {:?}",
                column_type
            )))
        }
    }

    if let Some(window_frame) = window_frame {
        let mut window_frame = window_frame;
        let current_types = expressions
            .iter()
            .map(|e| e.get_type(schema))
            .collect::<Result<Vec<_>>>()?;
        match &mut window_frame.units {
            WindowFrameUnits::Range => {
                let col_type = current_types.first().ok_or_else(|| {
                    DataFusionError::Internal(
                        "ORDER BY column cannot be empty".to_string(),
                    )
                })?;
                let coerced_type = get_coerced_type(col_type)?;
                window_frame.start_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.start_bound)?;
                window_frame.end_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.end_bound)?;
            }
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => {
                let coerced_type = DataType::UInt64;
                window_frame.start_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.start_bound)?;
                window_frame.end_bound =
                    coerce_frame_bound(&coerced_type, &window_frame.end_bound)?;
            }
        }

        Ok(Some(window_frame))
    } else {
        Ok(None)
    }
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
        .map(|(i, expr)| expr.clone().cast_to(&new_types[i], schema))
        .collect::<Result<Vec<_>>>()
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
    use crate::type_coercion::{TypeCoercion, TypeCoercionRewriter};
    use crate::{OptimizerConfig, OptimizerRule};
    use arrow::datatypes::DataType;
    use datafusion_common::{DFField, DFSchema, Result, ScalarValue};
    use datafusion_expr::expr::Like;
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
    use std::sync::Arc;

    #[test]
    fn simple_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new(None, "a", DataType::Float64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: a < CAST(UInt32(2) AS Float64)\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn nested_case() -> Result<()> {
        let expr = col("a").lt(lit(2_u32));
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new(None, "a", DataType::Float64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone().or(expr)],
            empty,
            None,
        )?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: a < CAST(UInt32(2) AS Float64) OR a < CAST(UInt32(2) AS Float64)\
            \n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
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
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: TestScalarUDF(CAST(Int32(123) AS Float32))\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
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
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).err().unwrap();
        assert_eq!(
            "Plan(\"Coercion from [Utf8] to the signature Uniform(1, [Int32]) failed.\")",
            &format!("{:?}", plan)
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
            None,
        )?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: abs(CAST(Int64(10) AS Float64))\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
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
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: MY_AVG(CAST(Int64(10) AS Float64))\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
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
        let plan = LogicalPlan::Projection(Projection::try_new(vec![udaf], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config);
        assert!(plan.is_err());
        assert_eq!(
            "Plan(\"Coercion from [Utf8] to the signature Uniform(1, [Float64]) failed.\")",
            &format!("{:?}", plan.err().unwrap())
        );
        Ok(())
    }

    #[test]
    fn agg_function_case() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction {
            fun,
            args: vec![lit(12i64)],
            distinct: false,
            filter: None,
        };
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: AVG(Int64(12))\n  EmptyRelation",
            &format!("{:?}", plan)
        );

        let empty = empty_with_type(DataType::Int32);
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction {
            fun,
            args: vec![col("a")],
            distinct: false,
            filter: None,
        };
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![agg_expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: AVG(a)\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn agg_function_invalid_input() -> Result<()> {
        let empty = empty();
        let fun: AggregateFunction = AggregateFunction::Avg;
        let agg_expr = Expr::AggregateFunction {
            fun,
            args: vec![lit("1")],
            distinct: false,
            filter: None,
        };
        let expr = Projection::try_new(vec![agg_expr], empty, None);
        assert!(expr.is_err());
        assert_eq!(
            "Plan(\"The function Avg does not support inputs of type Utf8.\")",
            &format!("{:?}", expr.err().unwrap())
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
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: CAST(Utf8(\"1998-03-18\") AS Date32) + IntervalDayTime(\"386547056640\")\n  EmptyRelation",
            &format!("{:?}", plan)
        );
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
                    vec![DFField::new(None, "a", DataType::Int64, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: a IN ([CAST(Int32(1) AS Int64), CAST(Int8(4) AS Int64), Int64(8)]) AS a IN (Map { iter: Iter([Int32(1), Int8(4), Int64(8)]) })\
             \n  EmptyRelation",
            &format!("{:?}", plan)
        );
        // a in (1,4,8), a is decimal
        let expr = col("a").in_list(vec![lit(1_i32), lit(4_i8), lit(8_i64)], false);
        let empty = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(
                DFSchema::new_with_metadata(
                    vec![DFField::new(None, "a", DataType::Decimal128(12, 4), true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }));
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config)?;
        assert_eq!(
            "Projection: CAST(a AS Decimal128(24, 4)) IN ([CAST(Int32(1) AS Decimal128(24, 4)), CAST(Int8(4) AS Decimal128(24, 4)), CAST(Int64(8) AS Decimal128(24, 4))]) AS a IN (Map { iter: Iter([Int32(1), Int8(4), Int64(8)]) })\
             \n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn is_bool_for_type_coercion() -> Result<()> {
        // is true
        let expr = col("a").is_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone()],
            empty,
            None,
        )?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS TRUE\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        let empty = empty_with_type(DataType::Int64);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config);
        assert!(plan.is_err());
        assert!(plan.unwrap_err().to_string().contains("'Int64 IS DISTINCT FROM Boolean' can't be evaluated because there isn't a common type to coerce the types to"));

        // is not true
        let expr = col("a").is_not_true();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS NOT TRUE\n  EmptyRelation",
            &format!("{:?}", plan)
        );

        // is false
        let expr = col("a").is_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS FALSE\n  EmptyRelation",
            &format!("{:?}", plan)
        );

        // is not false
        let expr = col("a").is_not_false();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS NOT FALSE\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn like_for_type_coercion() -> Result<()> {
        // like : utf8 like "abc"
        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a LIKE Utf8(\"abc\")\n  EmptyRelation",
            &format!("{:?}", plan)
        );

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::Null));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Utf8);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a LIKE CAST(NULL AS Utf8) AS a LIKE NULL \
             \n  EmptyRelation",
            &format!("{:?}", plan)
        );

        let expr = Box::new(col("a"));
        let pattern = Box::new(lit(ScalarValue::new_utf8("abc")));
        let like_expr = Expr::Like(Like::new(false, expr, pattern, None));
        let empty = empty_with_type(DataType::Int64);
        let plan =
            LogicalPlan::Projection(Projection::try_new(vec![like_expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config);
        assert!(plan.is_err());
        assert!(plan.unwrap_err().to_string().contains("'Int64 LIKE Utf8' can't be evaluated because there isn't a common type to coerce the types to"));
        Ok(())
    }

    #[test]
    fn unknown_for_type_coercion() -> Result<()> {
        // unknown
        let expr = col("a").is_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(
            vec![expr.clone()],
            empty,
            None,
        )?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS UNKNOWN\n  EmptyRelation",
            &format!("{:?}", plan)
        );

        let empty = empty_with_type(DataType::Utf8);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config);
        assert!(plan.is_err());
        assert!(plan.unwrap_err().to_string().contains("'Utf8 IS NOT DISTINCT FROM Boolean' can't be evaluated because there isn't a common type to coerce the types to"));

        // is not unknown
        let expr = col("a").is_not_unknown();
        let empty = empty_with_type(DataType::Boolean);
        let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
        let rule = TypeCoercion::new();
        let mut config = OptimizerConfig::default();
        let plan = rule.optimize(&plan, &mut config).unwrap();
        assert_eq!(
            "Projection: a IS NOT UNKNOWN\n  EmptyRelation",
            &format!("{:?}", plan)
        );
        Ok(())
    }

    #[test]
    fn concat_for_type_coercion() -> Result<()> {
        let empty = empty_with_type(DataType::Utf8);
        let args = [col("a"), lit("b"), lit(true), lit(false), lit(13)];

        // concat
        {
            let expr = concat(&args);

            let plan = LogicalPlan::Projection(Projection::try_new(
                vec![expr],
                empty.clone(),
                None,
            )?);
            let rule = TypeCoercion::new();
            let mut config = OptimizerConfig::default();
            let plan = rule.optimize(&plan, &mut config).unwrap();
            assert_eq!(
                "Projection: concat(a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation",
                &format!("{:?}", plan)
            );
        }

        // concat_ws
        {
            let expr = concat_ws(lit("-"), args.to_vec());

            let plan =
                LogicalPlan::Projection(Projection::try_new(vec![expr], empty, None)?);
            let rule = TypeCoercion::new();
            let mut config = OptimizerConfig::default();
            let plan = rule.optimize(&plan, &mut config).unwrap();
            assert_eq!(
                "Projection: concatwithseparator(Utf8(\"-\"), a, Utf8(\"b\"), CAST(Boolean(true) AS Utf8), CAST(Boolean(false) AS Utf8), CAST(Int32(13) AS Utf8))\n  EmptyRelation",
                &format!("{:?}", plan)
            );
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
                    vec![DFField::new(None, "a", data_type, true)],
                    std::collections::HashMap::new(),
                )
                .unwrap(),
            ),
        }))
    }

    #[test]
    fn test_type_coercion_rewrite() -> Result<()> {
        let schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new(None, "a", DataType::Int64, true)],
                std::collections::HashMap::new(),
            )
            .unwrap(),
        );
        let mut rewriter = TypeCoercionRewriter { schema };
        let expr = is_true(lit(12i32).eq(lit(13i64)));
        let expected = is_true(cast(lit(12i32), DataType::Int64).eq(lit(13i64)));
        let result = expr.rewrite(&mut rewriter)?;
        assert_eq!(expected, result);
        Ok(())
        // TODO add more test for this
    }
}
