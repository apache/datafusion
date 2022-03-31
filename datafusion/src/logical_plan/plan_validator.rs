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

use arrow::compute::can_cast_types;
use arrow::datatypes::DataType;

use datafusion_common::{Column, DFSchemaRef, DataFusionError, ScalarValue, ToDFSchema};
use datafusion_expr::{Expr, Operator};

use crate::error::Result;
use crate::logical_plan::plan::{
    Aggregate, Analyze, Explain, Filter, Join, Projection, Sort, Window,
};
use crate::logical_plan::{
    unalias, unnormalize_cols, CrossJoin, ExprSchemable, ExprVisitable,
    ExpressionVisitor, Limit, LogicalPlan, Recursion, Repartition, TableScan, Union,
};

/// check whether the optimized logical plan is valid
pub(crate) fn check_plan_invalid(plan: &LogicalPlan) -> Result<()> {
    match plan {
        LogicalPlan::Projection(Projection { expr, input, .. })
        | LogicalPlan::Sort(Sort { expr, input })
        | LogicalPlan::Window(Window {
            window_expr: expr,
            input,
            ..
        }) => check_plan_invalid(input)
            .and_then(|_| check_any_invalid_expr(expr, input.schema())),

        LogicalPlan::Filter(Filter {
            predicate: expr,
            input,
        }) => check_plan_invalid(input).and_then(|_| {
            check_any_invalid_expr(std::slice::from_ref(expr), input.schema())
        }),

        LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        }) => {
            let schema = input.schema();
            check_plan_invalid(input)
                .and_then(|_| check_any_invalid_expr(group_expr, schema))
                .and_then(|_| check_any_invalid_expr(aggr_expr, schema))
        }

        LogicalPlan::Join(Join { left, right, .. })
        | LogicalPlan::CrossJoin(CrossJoin { left, right, .. }) => {
            check_plan_invalid(left).and_then(|_| check_plan_invalid(right))
        }

        LogicalPlan::Repartition(Repartition { input, .. })
        | LogicalPlan::Limit(Limit { input, .. })
        | LogicalPlan::Explain(Explain { plan: input, .. })
        | LogicalPlan::Analyze(Analyze { input, .. }) => check_plan_invalid(input),

        LogicalPlan::Union(Union { inputs, .. }) => {
            inputs.iter().try_for_each(check_plan_invalid)
        }

        LogicalPlan::TableScan(TableScan {
            table_name: _,
            source,
            projection,
            projected_schema,
            filters,
            limit: _,
        }) => {
            if let Some(projection) = projection {
                if projection.len() > projected_schema.fields().len() {
                    return Err(DataFusionError::Plan(
                        "projection contains columns that doesnt belong to projected schema"
                            .to_owned(),
                    ));
                }
            }
            let schema = &source.schema().to_dfschema_ref()?;
            let filters = unnormalize_cols(filters.iter().cloned());
            let unaliased: Vec<Expr> = filters.into_iter().map(unalias).collect();
            check_any_invalid_expr(unaliased.as_slice(), schema)
        }
        _ => Ok(()),
    }
}

/// find first error in the exprs
pub(crate) fn check_any_invalid_expr(exprs: &[Expr], schema: &DFSchemaRef) -> Result<()> {
    exprs
        .iter()
        .try_for_each(|e| e.accept(ExprValidateVisitor { schema }).map(|_| ()))
}

/// do some checks for exprs in a logical plan
pub(crate) struct ExprValidateVisitor<'a> {
    pub(crate) schema: &'a DFSchemaRef,
}

impl ExpressionVisitor for ExprValidateVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>>
    where
        Self: ExpressionVisitor,
    {
        let rec = match expr {
            Expr::Alias(..) => Recursion::Continue(self),
            Expr::Column(Column { relation, name }) => {
                self.schema.field_with_name(relation.as_deref(), name)?;
                Recursion::Stop(self)
            }
            Expr::ScalarVariable(..) => Recursion::Stop(self),
            Expr::Literal(..) => Recursion::Stop(self),
            Expr::BinaryExpr { left, op, right } => match op {
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq
                | Operator::Plus
                | Operator::Minus
                | Operator::Multiply
                | Operator::Divide
                | Operator::Modulo => Recursion::Continue(self), // todo!("need compatible types"),
                Operator::And | Operator::Or => {
                    if left.get_type(self.schema)? != DataType::Boolean
                        || right.get_type(self.schema)? != DataType::Boolean
                    {
                        return Err(DataFusionError::Plan(format!(
                            "Invalid Expression({}) -- Arguments of operator({}) must be of boolean types",
                            expr,
                            op
                        )));
                    }
                    Recursion::Continue(self)
                }
                _ => Recursion::Continue(self),
            },
            Expr::Not(bool_expr) => {
                // Negation of an expression. The expression's type must be a boolean to make sense.
                if bool_expr.get_type(self.schema)? != DataType::Boolean {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid Not Expression({}) -- Type({}) of expression({}) must be boolean",
                        expr,
                        bool_expr.get_type(self.schema)?,
                        bool_expr,
                    )));
                }
                Recursion::Continue(self)
            }
            Expr::IsNotNull(..) => Recursion::Continue(self),
            Expr::IsNull(..) => Recursion::Continue(self),
            Expr::Negative(signed_num_expr) => {
                // arithmetic negation of an expression, the operand must be of a signed numeric data type
                match signed_num_expr.get_type(self.schema)? {
                    DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64 => Recursion::Continue(self),
                    _ => return Err(DataFusionError::Plan(format!(
                        "Invalid Negative Expression({}) -- The operand({}) must be of a signed numeric type({})",
                        expr,
                        signed_num_expr,
                        signed_num_expr.get_type(self.schema)?
                    )))
                }
            }
            Expr::GetIndexedField { expr, key } => {
                // key is null || (expr is list && key is int64 || expr is struct && key is utf8)
                let expr_type = expr.get_type(self.schema)?;
                match (expr_type, key) {
                    (DataType::List(_) | DataType::Struct(_), key) if key.is_null() => {
                        Recursion::Continue(self)
                    }
                    (DataType::List(_), ScalarValue::Int64(Some(_)))
                    | (DataType::Struct(_), ScalarValue::Utf8(Some(_))) => {
                        Recursion::Continue(self)
                    }
                    _ => return Err(DataFusionError::Plan(
                        "GetIndexedField is only possible on lists with int64 indexes or struct with utf8 field name."
                            .to_owned())),
                }
            }
            Expr::Between { .. } => Recursion::Continue(self), // todo!("type compatible"),
            Expr::Case { .. } => Recursion::Continue(self), // todo!("type compatible"),
            Expr::Cast {
                expr: cast_expr,
                data_type,
            } => {
                if !can_cast_types(&cast_expr.get_type(self.schema)?, data_type) {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid Cast Expression({}) -- {} cannot cast to {}",
                        expr,
                        cast_expr.get_type(self.schema)?,
                        data_type
                    )));
                }
                Recursion::Continue(self)
            }
            Expr::TryCast { .. } => Recursion::Continue(self),
            Expr::Sort { .. } => Recursion::Continue(self),
            Expr::ScalarFunction { .. } => Recursion::Continue(self),
            Expr::ScalarUDF { .. } => Recursion::Continue(self),
            Expr::AggregateFunction { .. } => Recursion::Continue(self),
            Expr::WindowFunction { .. } => Recursion::Continue(self),
            Expr::AggregateUDF { .. } => Recursion::Continue(self),
            Expr::InList {
                expr: value, list, ..
            } => {
                // value type eq or can be cast from list value type
                let value_type = value.get_type(self.schema)?;
                for list_value in list {
                    let list_value_type = list_value.get_type(self.schema)?;
                    if value_type != list_value_type
                        && !can_cast_types(&list_value_type, &value_type)
                    {
                        return Err(DataFusionError::Plan(
                            format!("Invalid InList Expression({}) -- Unsupported type cast from {} to {}", 
                                    expr, list_value_type, value_type)
                        ));
                    }
                }
                Recursion::Continue(self)
            }
            Expr::Wildcard | Expr::QualifiedWildcard { .. } => {
                return Err(DataFusionError::Plan(
                    "Wildcard expressions are not valid in a logical query plan"
                        .to_owned(),
                ));
            }
        };
        Ok(rec)
    }
}
