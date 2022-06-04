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

use crate::{
    execution_props::ExecutionProps,
    expressions::{
        self, binary, CaseExpr, Column, DateIntervalExpr, GetIndexedFieldExpr, Literal,
    },
    functions, udf,
    var_provider::VarType,
    PhysicalExpr,
};
use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, Schema},
};
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Expr, Operator};
use std::sync::Arc;

/// Create a physical expression from a logical expression ([Expr])
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    match e {
        Expr::Alias(expr, ..) => Ok(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::ScalarVariable(_, variable_names) => {
            if &variable_names[0][0..2] == "@@" {
                match execution_props.get_var_provider(VarType::System) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => Err(DataFusionError::Plan(
                        "No system variable provider found".to_string(),
                    )),
                }
            } else {
                match execution_props.get_var_provider(VarType::UserDefined) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => Err(DataFusionError::Plan(
                        "No user defined variable provider found".to_string(),
                    )),
                }
            }
        }
        Expr::BinaryExpr { left, op, right } => {
            let lhs = create_physical_expr(
                left,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let rhs = create_physical_expr(
                right,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            match (
                lhs.data_type(input_schema)?,
                op,
                rhs.data_type(input_schema)?,
            ) {
                (
                    DataType::Date32 | DataType::Date64,
                    Operator::Plus | Operator::Minus,
                    DataType::Interval(_),
                ) => Ok(Arc::new(DateIntervalExpr::try_new(
                    lhs,
                    *op,
                    rhs,
                    input_schema,
                )?)),
                _ => {
                    // assume that we can coerce both sides into a common type
                    // and then perform a binary operation
                    binary(lhs, *op, rhs, input_schema)
                }
            }
        }
        Expr::Case {
            expr,
            when_then_expr,
            else_expr,
            ..
        } => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            let when_expr = when_then_expr
                .iter()
                .map(|(w, _)| {
                    create_physical_expr(
                        w.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let then_expr = when_then_expr
                .iter()
                .map(|(_, t)| {
                    create_physical_expr(
                        t.as_ref(),
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                when_expr
                    .iter()
                    .zip(then_expr.iter())
                    .map(|(w, t)| (w.clone(), t.clone()))
                    .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = else_expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            Ok(Arc::new(CaseExpr::try_new(
                expr,
                &when_then_expr,
                else_expr,
            )?))
        }
        Expr::Cast { expr, data_type } => expressions::cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast { expr, data_type } => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::Not(expr) => expressions::not(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::GetIndexedField { expr, key } => Ok(Arc::new(GetIndexedFieldExpr::new(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            key.clone(),
        ))),

        Expr::ScalarFunction { fun, args } => {
            let physical_args = args
                .iter()
                .map(|e| {
                    create_physical_expr(e, input_dfschema, input_schema, execution_props)
                })
                .collect::<Result<Vec<_>>>()?;
            functions::create_physical_expr(
                fun,
                &physical_args,
                input_schema,
                execution_props,
            )
        }
        Expr::ScalarUDF { fun, args } => {
            let mut physical_args = vec![];
            for e in args {
                physical_args.push(create_physical_expr(
                    e,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?);
            }

            udf::create_physical_expr(fun.clone().as_ref(), &physical_args, input_schema)
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value_expr = create_physical_expr(
                expr,
                input_dfschema,
                input_schema,
                execution_props,
            )?;
            let low_expr =
                create_physical_expr(low, input_dfschema, input_schema, execution_props)?;
            let high_expr = create_physical_expr(
                high,
                input_dfschema,
                input_schema,
                execution_props,
            )?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                Operator::And,
                binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                input_schema,
            );

            if *negated {
                expressions::not(binary_expr?)
            } else {
                binary_expr
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => match expr.as_ref() {
            Expr::Literal(ScalarValue::Utf8(None)) => {
                Ok(expressions::lit(ScalarValue::Boolean(None)))
            }
            _ => {
                let value_expr = create_physical_expr(
                    expr,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?;
                let value_expr_data_type = value_expr.data_type(input_schema)?;

                let list_exprs = list
                    .iter()
                    .map(|expr| match expr {
                        Expr::Literal(ScalarValue::Utf8(None)) => create_physical_expr(
                            expr,
                            input_dfschema,
                            input_schema,
                            execution_props,
                        ),
                        _ => {
                            let list_expr = create_physical_expr(
                                expr,
                                input_dfschema,
                                input_schema,
                                execution_props,
                            )?;
                            let list_expr_data_type =
                                list_expr.data_type(input_schema)?;

                            if list_expr_data_type == value_expr_data_type {
                                Ok(list_expr)
                            } else if can_cast_types(
                                &list_expr_data_type,
                                &value_expr_data_type,
                            ) {
                                expressions::cast(
                                    list_expr,
                                    input_schema,
                                    value_expr.data_type(input_schema)?,
                                )
                            } else {
                                Err(DataFusionError::Plan(format!(
                                    "Unsupported CAST from {:?} to {:?}",
                                    list_expr_data_type, value_expr_data_type
                                )))
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                expressions::in_list(value_expr, list_exprs, negated)
            }
        },
        other => Err(DataFusionError::NotImplemented(format!(
            "Physical plan does not support logical expression {:?}",
            other
        ))),
    }
}
