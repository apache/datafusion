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

//! SQL planning extensions like [`NestedFunctionPlanner`] and [`FieldAccessPlanner`]

use arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::{DFSchema, Result, plan_err, utils::list_ndims};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion_expr::planner::RawAnyAllExpr;
#[cfg(feature = "sql")]
use datafusion_expr::sqlparser::ast::BinaryOperator;
use datafusion_expr::{AggregateUDF, lit, when};
use datafusion_expr::{
    Expr, ExprSchemable, GetFieldAccess,
    planner::{ExprPlanner, PlannerResult, RawBinaryExpr, RawFieldAccessExpr},
};
#[cfg(not(feature = "sql"))]
use datafusion_expr_common::operator::Operator as BinaryOperator;
use datafusion_functions::core::get_field as get_field_inner;
use datafusion_functions::expr_fn::get_field;
use datafusion_functions_aggregate::nth_value::nth_value_udaf;
use std::sync::Arc;

use crate::expr_fn::{array_has, array_position, cardinality};
use crate::map::map_udf;
use crate::min_max::{array_max, array_min};
use crate::{
    array_has::array_has_all,
    expr_fn::{array_append, array_concat, array_prepend},
    extract::{array_element, array_slice},
    make_array::make_array,
};

#[derive(Debug)]
pub struct NestedFunctionPlanner;

impl ExprPlanner for NestedFunctionPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let RawBinaryExpr { op, left, right } = expr;

        if op == BinaryOperator::StringConcat {
            let left_type = left.get_type(schema)?;
            let right_type = right.get_type(schema)?;
            let left_list_ndims = list_ndims(&left_type);
            let right_list_ndims = list_ndims(&right_type);

            // Rewrite string concat operator to function based on types
            // if we get list || list then we rewrite it to array_concat()
            // if we get list || non-list then we rewrite it to array_append()
            // if we get non-list || list then we rewrite it to array_prepend()
            // if we get string || string then we rewrite it to concat()

            // We determine the target function to rewrite based on the list n-dimension, the check is not exact but sufficient.
            // The exact validity check is handled in the actual function, so even if there is 3d list appended with 1d list, it is also fine to rewrite.
            if left_list_ndims + right_list_ndims == 0 {
                // TODO: concat function ignore null, but string concat takes null into consideration
                // we can rewrite it to concat if we can configure the behaviour of concat function to the one like `string concat operator`
            } else if left_list_ndims == right_list_ndims {
                return Ok(PlannerResult::Planned(array_concat(vec![left, right])));
            } else if left_list_ndims > right_list_ndims {
                return Ok(PlannerResult::Planned(array_append(left, right)));
            } else if left_list_ndims < right_list_ndims {
                return Ok(PlannerResult::Planned(array_prepend(left, right)));
            }
        } else if matches!(op, BinaryOperator::AtArrow | BinaryOperator::ArrowAt) {
            let left_type = left.get_type(schema)?;
            let right_type = right.get_type(schema)?;
            let left_list_ndims = list_ndims(&left_type);
            let right_list_ndims = list_ndims(&right_type);
            // if both are list
            if left_list_ndims > 0 && right_list_ndims > 0 {
                if op == BinaryOperator::AtArrow {
                    // array1 @> array2 -> array_has_all(array1, array2)
                    return Ok(PlannerResult::Planned(array_has_all(left, right)));
                } else {
                    // array1 <@ array2 -> array_has_all(array2, array1)
                    return Ok(PlannerResult::Planned(array_has_all(right, left)));
                }
            }
        }

        Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
    }

    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<Vec<Expr>>> {
        Ok(PlannerResult::Planned(make_array(exprs)))
    }

    fn plan_make_map(&self, args: Vec<Expr>) -> Result<PlannerResult<Vec<Expr>>> {
        if !args.len().is_multiple_of(2) {
            return plan_err!("make_map requires an even number of arguments");
        }

        let (keys, values): (Vec<_>, Vec<_>) =
            args.into_iter().enumerate().partition(|(i, _)| i % 2 == 0);
        let keys = make_array(keys.into_iter().map(|(_, e)| e).collect());
        let values = make_array(values.into_iter().map(|(_, e)| e).collect());

        Ok(PlannerResult::Planned(Expr::ScalarFunction(
            ScalarFunction::new_udf(map_udf(), vec![keys, values]),
        )))
    }

    fn plan_any(&self, args: RawAnyAllExpr) -> Result<PlannerResult<RawAnyAllExpr>> {
        let RawAnyAllExpr {
            op,
            needle,
            haystack,
        } = args;
        let expr = match op {
            BinaryOperator::Eq => Ok(array_has(haystack, needle)),
            BinaryOperator::NotEq => {
                let min = array_min(haystack.clone());
                let max = array_max(haystack.clone());
                // NOT EQ is true when either bound differs from left
                let comparison =
                    min.not_eq(needle.clone()).or(max.clone().not_eq(needle));
                any_op_with_null_handling(max, comparison, haystack)
            }
            BinaryOperator::Gt => {
                let min = array_min(haystack.clone());
                any_op_with_null_handling(min.clone(), min.lt(needle), haystack)
            }
            BinaryOperator::Lt => {
                let max = array_max(haystack.clone());
                any_op_with_null_handling(max.clone(), max.gt(needle), haystack)
            }
            BinaryOperator::GtEq => {
                let min = array_min(haystack.clone());
                any_op_with_null_handling(min.clone(), min.lt_eq(needle), haystack)
            }
            BinaryOperator::LtEq => {
                let max = array_max(haystack.clone());
                any_op_with_null_handling(max.clone(), max.gt_eq(needle), haystack)
            }
            _ => plan_err!(
                "Unsupported AnyOp: '{op}', only '=', '<>', '>', '<', '>=', '<=' are supported"
            ),
        }?;
        Ok(PlannerResult::Planned(expr))
    }

    fn plan_all(&self, args: RawAnyAllExpr) -> Result<PlannerResult<RawAnyAllExpr>> {
        // CASE/WHEN structure:
        //   WHEN arr IS NULL        → NULL
        //   WHEN empty              → TRUE
        //   WHEN lhs IS NULL        → NULL
        //   WHEN decisive_condition → FALSE
        //   WHEN has_nulls          → NULL
        //   ELSE                    → TRUE
        let RawAnyAllExpr {
            op,
            needle,
            haystack,
        } = args;
        let null_arr_check = haystack.clone().is_null();
        let empty_check = cardinality(haystack.clone()).eq(lit(0u64));
        let null_lhs_check = needle.clone().is_null();
        // DataFusion's array_position uses is_null() checks internally (not equality),
        // so it can locate NULL elements even though NULL = NULL is NULL in standard SQL.
        let has_nulls =
            array_position(haystack.clone(), lit(ScalarValue::Null), lit(1i64))
                .is_not_null();

        let decisive_condition = match op {
            BinaryOperator::NotEq => array_has(haystack.clone(), needle.clone()),
            BinaryOperator::Eq => {
                let all_equal = array_min(haystack.clone())
                    .eq(needle.clone())
                    .and(array_max(haystack.clone()).eq(needle.clone()));
                Expr::Not(Box::new(all_equal))
            }
            BinaryOperator::Gt => {
                Expr::Not(Box::new(needle.clone().gt(array_max(haystack.clone()))))
            }
            BinaryOperator::Lt => {
                Expr::Not(Box::new(needle.clone().lt(array_min(haystack.clone()))))
            }
            BinaryOperator::GtEq => {
                Expr::Not(Box::new(needle.clone().gt_eq(array_max(haystack.clone()))))
            }
            BinaryOperator::LtEq => {
                Expr::Not(Box::new(needle.clone().lt_eq(array_min(haystack.clone()))))
            }
            _ => {
                return plan_err!(
                    "Unsupported AllOp: '{op}', only '=', '<>', '>', '<', '>=', '<=' are supported"
                );
            }
        };

        let null_bool = lit(ScalarValue::Boolean(None));
        let expr = when(null_arr_check, null_bool.clone())
            .when(empty_check, lit(true))
            .when(null_lhs_check, null_bool.clone())
            .when(decisive_condition, lit(false))
            .when(has_nulls, null_bool)
            .otherwise(lit(true))?;
        Ok(PlannerResult::Planned(expr))
    }
}

/// Builds a CASE expression that handles NULL semantics for `x <op> ANY(arr)`:
///
/// ```text
/// CASE
///   WHEN <min_or_max>(arr) IS NOT NULL THEN <comparison>
///   WHEN arr IS NOT NULL THEN FALSE          -- empty or all-null array
///   ELSE NULL                                -- NULL array
/// END
/// ```
fn any_op_with_null_handling(bound: Expr, comparison: Expr, arr: Expr) -> Result<Expr> {
    when(bound.is_not_null(), comparison)
        .when(arr.is_not_null(), lit(false))
        .otherwise(lit(ScalarValue::Boolean(None)))
}

#[derive(Debug)]
pub struct FieldAccessPlanner;
impl ExprPlanner for FieldAccessPlanner {
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawFieldAccessExpr>> {
        let RawFieldAccessExpr { expr, field_access } = expr;

        match field_access {
            // expr["field"] => get_field(expr, "field")
            // Nested accesses like expr["a"]["b"] create nested get_field calls,
            // which are then merged by the SimplifyExpressions optimizer pass via
            // the GetFieldFunc::simplify() method.
            GetFieldAccess::NamedStructField { name } => {
                Ok(PlannerResult::Planned(get_field(expr, name)))
            }
            // expr[idx] ==> array_element(expr, idx)
            GetFieldAccess::ListIndex { key: index } => {
                match expr {
                    // Special case for array_agg(expr)[index] to NTH_VALUE(expr, index)
                    Expr::AggregateFunction(AggregateFunction {
                        func,
                        params:
                            AggregateFunctionParams {
                                args,
                                distinct,
                                filter,
                                order_by,
                                null_treatment,
                            },
                    }) if is_array_agg(&func) => Ok(PlannerResult::Planned(
                        Expr::AggregateFunction(AggregateFunction::new_udf(
                            nth_value_udaf(),
                            args.into_iter().chain(std::iter::once(*index)).collect(),
                            distinct,
                            filter,
                            order_by,
                            null_treatment,
                        )),
                    )),
                    // special case for map access with
                    _ if matches!(expr.get_type(schema)?, DataType::Map(_, _)) => {
                        Ok(PlannerResult::Planned(Expr::ScalarFunction(
                            ScalarFunction::new_udf(
                                get_field_inner(),
                                vec![expr, *index],
                            ),
                        )))
                    }
                    _ => Ok(PlannerResult::Planned(array_element(expr, *index))),
                }
            }
            // expr[start, stop, stride] ==> array_slice(expr, start, stop, stride)
            GetFieldAccess::ListRange {
                start,
                stop,
                stride,
            } => Ok(PlannerResult::Planned(array_slice(
                expr,
                *start,
                *stop,
                Some(*stride),
            ))),
        }
    }
}

fn is_array_agg(func: &Arc<AggregateUDF>) -> bool {
    func.name() == "array_agg"
}
