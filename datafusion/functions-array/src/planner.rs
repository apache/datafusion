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

use std::sync::Arc;

use datafusion_common::{internal_err, utils::list_ndims, DFSchema, Result};
use datafusion_expr::{
    lit,
    planner::{
        BinaryExpr, ContextProvider, FieldAccessExpr, PlannerSimplifyResult,
        UserDefinedPlanner,
    },
    AggregateFunction, Expr, ExprSchemable, GetFieldAccess, ScalarUDF,
};

pub struct ArrayFunctionPlanner {
    array_concat: Arc<ScalarUDF>,
    array_append: Arc<ScalarUDF>,
    array_prepend: Arc<ScalarUDF>,
    array_has_all: Arc<ScalarUDF>,
    make_array: Arc<ScalarUDF>,
}

impl ArrayFunctionPlanner {
    pub fn try_new(context_provider: &dyn ContextProvider) -> Result<Self> {
        let Some(array_concat) = context_provider.get_function_meta("array_concat")
        else {
            return internal_err!("array_concat not found");
        };
        let Some(array_append) = context_provider.get_function_meta("array_append")
        else {
            return internal_err!("array_append not found");
        };
        let Some(array_prepend) = context_provider.get_function_meta("array_prepend")
        else {
            return internal_err!("array_prepend not found");
        };
        let Some(array_has_all) = context_provider.get_function_meta("array_has_all")
        else {
            return internal_err!("array_has_all not found");
        };
        let Some(make_array) = context_provider.get_function_meta("make_array") else {
            return internal_err!("make_array not found");
        };

        Ok(Self {
            array_concat,
            array_append,
            array_prepend,
            array_has_all,
            make_array,
        })
    }
}

impl UserDefinedPlanner for ArrayFunctionPlanner {
    fn plan_binary_op(
        &self,
        expr: BinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        let BinaryExpr { op, left, right } = expr;

        if op == sqlparser::ast::BinaryOperator::StringConcat {
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
                return Ok(PlannerSimplifyResult::Simplified(
                    self.array_concat.call(vec![left, right]),
                ));
            } else if left_list_ndims > right_list_ndims {
                return Ok(PlannerSimplifyResult::Simplified(
                    self.array_append.call(vec![left, right]),
                ));
            } else if left_list_ndims < right_list_ndims {
                return Ok(PlannerSimplifyResult::Simplified(
                    self.array_prepend.call(vec![left, right]),
                ));
            }
        } else if matches!(
            op,
            sqlparser::ast::BinaryOperator::AtArrow
                | sqlparser::ast::BinaryOperator::ArrowAt
        ) {
            let left_type = left.get_type(schema)?;
            let right_type = right.get_type(schema)?;
            let left_list_ndims = list_ndims(&left_type);
            let right_list_ndims = list_ndims(&right_type);
            // if both are list
            if left_list_ndims > 0 && right_list_ndims > 0 {
                if op == sqlparser::ast::BinaryOperator::AtArrow {
                    // array1 @> array2 -> array_has_all(array1, array2)
                    return Ok(PlannerSimplifyResult::Simplified(
                        self.array_has_all.call(vec![left, right]),
                    ));
                } else {
                    // array1 <@ array2 -> array_has_all(array2, array1)
                    return Ok(PlannerSimplifyResult::Simplified(
                        self.array_has_all.call(vec![right, left]),
                    ));
                }
            }
        }

        Ok(PlannerSimplifyResult::OriginalBinaryExpr(BinaryExpr {
            op,
            left,
            right,
        }))
    }

    fn plan_array_literal(
        &self,
        exprs: Vec<Expr>,
        _schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        Ok(PlannerSimplifyResult::Simplified(
            self.make_array.call(exprs),
        ))
    }
}

pub struct FieldAccessPlanner {
    get_field: Arc<ScalarUDF>,
    array_element: Arc<ScalarUDF>,
    array_slice: Arc<ScalarUDF>,
}

impl FieldAccessPlanner {
    pub fn try_new(context_provider: &dyn ContextProvider) -> Result<Self> {
        let Some(get_field) = context_provider.get_function_meta("get_field") else {
            return internal_err!("get_feild not found");
        };
        let Some(array_element) = context_provider.get_function_meta("array_element")
        else {
            return internal_err!("array_element not found");
        };
        let Some(array_slice) = context_provider.get_function_meta("array_slice") else {
            return internal_err!("array_slice not found");
        };

        Ok(Self {
            get_field,
            array_element,
            array_slice,
        })
    }
}

impl UserDefinedPlanner for FieldAccessPlanner {
    fn plan_field_access(
        &self,
        expr: FieldAccessExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerSimplifyResult> {
        let FieldAccessExpr { expr, field_access } = expr;

        match field_access {
            // expr["field"] => get_field(expr, "field")
            GetFieldAccess::NamedStructField { name } => {
                Ok(PlannerSimplifyResult::Simplified(
                    self.get_field.call(vec![expr, lit(name)]),
                ))
            }
            // expr[idx] ==> array_element(expr, idx)
            GetFieldAccess::ListIndex { key: index } => {
                match expr {
                    // Special case for array_agg(expr)[index] to NTH_VALUE(expr, index)
                    Expr::AggregateFunction(agg_func) if is_array_agg(&agg_func) => {
                        Ok(PlannerSimplifyResult::Simplified(Expr::AggregateFunction(
                            datafusion_expr::expr::AggregateFunction::new(
                                AggregateFunction::NthValue,
                                agg_func
                                    .args
                                    .into_iter()
                                    .chain(std::iter::once(*index))
                                    .collect(),
                                agg_func.distinct,
                                agg_func.filter,
                                agg_func.order_by,
                                agg_func.null_treatment,
                            ),
                        )))
                    }
                    _ => Ok(PlannerSimplifyResult::Simplified(
                        self.array_element.call(vec![expr, *index]),
                    )),
                }
            }
            // expr[start, stop, stride] ==> array_slice(expr, start, stop, stride)
            GetFieldAccess::ListRange {
                start,
                stop,
                stride,
            } => Ok(PlannerSimplifyResult::Simplified(
                self.array_slice.call(vec![expr, *start, *stop, *stride]),
            )),
        }
    }
}

fn is_array_agg(agg_func: &datafusion_expr::expr::AggregateFunction) -> bool {
    agg_func.func_def
        == datafusion_expr::expr::AggregateFunctionDefinition::BuiltIn(
            AggregateFunction::ArrayAgg,
        )
}
