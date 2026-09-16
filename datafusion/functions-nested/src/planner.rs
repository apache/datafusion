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
use datafusion_common::{DFSchema, Result, plan_err, utils::list_ndims};
use datafusion_expr::AggregateUDF;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams};
#[cfg(feature = "sql")]
use datafusion_expr::sqlparser::ast::BinaryOperator;
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

use crate::map::map_udf;
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
        } else {
            #[cfg(feature = "sql")]
            let is_colon =
                matches!(&op, BinaryOperator::Custom(operator) if operator == ":");
            #[cfg(not(feature = "sql"))]
            let is_colon = op == BinaryOperator::Colon;

            if is_colon
                && matches!(left.get_type(schema)?, DataType::Struct(_))
                && let Expr::Literal(field_name, _) = &right
                && let Some(field_name) = field_name.try_as_str().flatten()
            {
                return Ok(PlannerResult::Planned(get_field(
                    left,
                    field_name.to_owned(),
                )));
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

#[cfg(all(test, feature = "sql"))]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Fields};
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::planner::ExprPlanner;
    use std::collections::HashMap;

    fn nested_struct_schema() -> DFSchema {
        let country = DataType::Struct(Fields::from(vec![Field::new(
            "name",
            DataType::Utf8,
            true,
        )]));
        let payload =
            DataType::Struct(Fields::from(vec![Field::new("country", country, true)]));
        DFSchema::from_unqualified_fields(
            vec![Field::new("payload", payload, true)].into(),
            HashMap::new(),
        )
        .unwrap()
    }

    fn colon(left: Expr, field_name: &str) -> RawBinaryExpr {
        RawBinaryExpr {
            op: BinaryOperator::Custom(":".to_owned()),
            left,
            right: Expr::Literal(ScalarValue::from(field_name), None),
        }
    }

    fn planned(expr: RawBinaryExpr, schema: &DFSchema) -> Expr {
        let PlannerResult::Planned(expr) =
            NestedFunctionPlanner.plan_binary_op(expr, schema).unwrap()
        else {
            panic!("expected colon access to be planned");
        };
        expr
    }

    #[test]
    fn plans_nested_struct_colon_access_as_get_field() {
        let schema = nested_struct_schema();
        let payload = Expr::Column(Column::new_unqualified("payload"));
        let country = planned(colon(payload.clone(), "country"), &schema);
        let name = planned(colon(country.clone(), "name"), &schema);

        assert_eq!(country, get_field(payload.clone(), "country"));
        assert_eq!(name, get_field(get_field(payload, "country"), "name"));
    }

    #[test]
    fn leaves_non_struct_colon_access_for_other_planners() {
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new("text", DataType::Utf8, true)].into(),
            HashMap::new(),
        )
        .unwrap();
        let original = colon(Expr::Column(Column::new_unqualified("text")), "field");

        assert!(matches!(
            NestedFunctionPlanner
                .plan_binary_op(original, &schema)
                .unwrap(),
            PlannerResult::Original(_)
        ));
    }
}
