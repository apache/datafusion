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

use std::collections::HashMap;
use std::sync::Arc;

use crate::scalar_subquery::ScalarSubqueryExpr;
use crate::{HigherOrderFunctionExpr, ScalarFunctionExpr};
use crate::{
    PhysicalExpr,
    expressions::{self, Column, Literal, binary, like, similar_to},
};

use arrow::datatypes::Schema;
use datafusion_common::config::ConfigOptions;
use datafusion_common::datatype::FieldExt;
use datafusion_common::metadata::{FieldMetadata, format_type_and_metadata};
use datafusion_common::{
    DFSchema, Result, ScalarValue, ToDFSchema, exec_err, internal_datafusion_err,
    not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::{
    Alias, Cast, HigherOrderFunction, InList, Lambda, LambdaVariable, Placeholder,
    ScalarFunction,
};
use datafusion_expr::var_provider::VarType;
use datafusion_expr::var_provider::is_system_variables;
use datafusion_expr::{
    Between, BinaryExpr, Expr, ExprSchemable, Like, Operator, TryCast, binary_expr, lit,
};

/// [PhysicalExpr] evaluate DataFusion expressions such as `A + 1`, or `CAST(c1
/// AS int)`.
///
/// [PhysicalExpr] are the physical counterpart to [Expr] used in logical
/// planning, and can be evaluated directly on a [RecordBatch]. They are
/// normally created from [Expr] by a [PhysicalPlanner] and can be created
/// directly using [create_physical_expr].
///
/// A Physical expression knows its type, nullability and how to evaluate itself.
///
/// [PhysicalPlanner]: https://docs.rs/datafusion/latest/datafusion/physical_planner/trait.PhysicalPlanner.html
/// [RecordBatch]: https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html
///
/// # Example: Create `PhysicalExpr` from `Expr`
/// ```
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::DFSchema;
/// # use datafusion_expr::{Expr, col, lit};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// // For a logical expression `a = 1`, we can create a physical expression
/// let expr = col("a").eq(lit(1));
/// // To create a PhysicalExpr we need 1. a schema
/// let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// let df_schema = DFSchema::try_from(schema).unwrap();
/// // 2. ExecutionProps
/// let props = ExecutionProps::new();
/// // We can now create a PhysicalExpr:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// ```
///
/// # Example: Executing a PhysicalExpr to obtain [ColumnarValue]
/// ```
/// # use std::sync::Arc;
/// # use arrow::array::{cast::AsArray, BooleanArray, Int32Array, RecordBatch};
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion_common::{assert_batches_eq, DFSchema};
/// # use datafusion_expr::{Expr, col, lit, ColumnarValue};
/// # use datafusion_physical_expr::create_physical_expr;
/// # use datafusion_expr::execution_props::ExecutionProps;
/// # let expr = col("a").eq(lit(1));
/// # let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
/// # let df_schema = DFSchema::try_from(schema.clone()).unwrap();
/// # let props = ExecutionProps::new();
/// // Given a PhysicalExpr, for `a = 1` we can evaluate it against a RecordBatch like this:
/// let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
/// // Input of [1,2,3]
/// let input_batch = RecordBatch::try_from_iter(vec![
///   ("a", Arc::new(Int32Array::from(vec![1, 2, 3])) as _)
/// ]).unwrap();
/// // The result is a ColumnarValue (either an Array or a Scalar)
/// let result = physical_expr.evaluate(&input_batch).unwrap();
/// // In this case, a BooleanArray with the result of the comparison
/// let ColumnarValue::Array(arr) = result else {
///  panic!("Expected an array")
/// };
/// assert_eq!(arr.as_boolean(), &BooleanArray::from(vec![true, false, false]));
/// ```
///
/// [ColumnarValue]: datafusion_expr::ColumnarValue
///
/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///   to qualified or unqualified fields by name.
#[cfg_attr(feature = "recursive_protection", recursive::recursive)]
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    let input_schema = input_dfschema.as_arrow();

    match e {
        Expr::Alias(Alias { expr, metadata, .. }) => {
            if let Expr::Literal(v, prior_metadata) = expr.as_ref() {
                let new_metadata = FieldMetadata::merge_options(
                    prior_metadata.as_ref(),
                    metadata.as_ref(),
                );
                Ok(Arc::new(Literal::new_with_metadata(
                    v.clone(),
                    new_metadata,
                )))
            } else {
                Ok(create_physical_expr(expr, input_dfschema, execution_props)?)
            }
        }
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value, metadata) => Ok(Arc::new(Literal::new_with_metadata(
            value.clone(),
            metadata.clone(),
        ))),
        Expr::ScalarVariable(_, variable_names) => {
            if is_system_variables(variable_names) {
                match execution_props.get_var_provider(VarType::System) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => plan_err!("No system variable provider found"),
                }
            } else {
                match execution_props.get_var_provider(VarType::UserDefined) {
                    Some(provider) => {
                        let scalar_value = provider.get_value(variable_names.clone())?;
                        Ok(Arc::new(Literal::new(scalar_value)))
                    }
                    _ => plan_err!("No user defined variable provider found"),
                }
            }
        }
        Expr::IsTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                lit(true),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotTrue(expr) => {
            let binary_op =
                binary_expr(expr.as_ref().clone(), Operator::IsDistinctFrom, lit(true));
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                lit(false),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotFalse(expr) => {
            let binary_op =
                binary_expr(expr.as_ref().clone(), Operator::IsDistinctFrom, lit(false));
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None), None),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::IsNotUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None), None),
            );
            create_physical_expr(&binary_op, input_dfschema, execution_props)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Create physical expressions for left and right operands
            let lhs = create_physical_expr(left, input_dfschema, execution_props)?;
            let rhs = create_physical_expr(right, input_dfschema, execution_props)?;
            // Note that the logical planner is responsible
            // for type coercion on the arguments (e.g. if one
            // argument was originally Int32 and one was
            // Int64 they will both be coerced to Int64).
            //
            // There should be no coercion during physical
            // planning.
            binary(lhs, *op, rhs, input_schema)
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            // `\` is the implicit escape, see https://github.com/apache/datafusion/issues/13291
            if escape_char.unwrap_or('\\') != '\\' {
                return exec_err!(
                    "LIKE does not support escape_char other than the backslash (\\)"
                );
            }
            let physical_expr =
                create_physical_expr(expr, input_dfschema, execution_props)?;
            let physical_pattern =
                create_physical_expr(pattern, input_dfschema, execution_props)?;
            like(
                *negated,
                *case_insensitive,
                physical_expr,
                physical_pattern,
                input_schema,
            )
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if escape_char.is_some() {
                return exec_err!("SIMILAR TO does not support escape_char yet");
            }
            let physical_expr =
                create_physical_expr(expr, input_dfschema, execution_props)?;
            let physical_pattern =
                create_physical_expr(pattern, input_dfschema, execution_props)?;
            similar_to(*negated, *case_insensitive, physical_expr, physical_pattern)
        }
        Expr::Case(case) => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    execution_props,
                )?)
            } else {
                None
            };
            let (when_expr, then_expr): (Vec<&Expr>, Vec<&Expr>) = case
                .when_then_expr
                .iter()
                .map(|(w, t)| (w.as_ref(), t.as_ref()))
                .unzip();
            let when_expr =
                create_physical_exprs(when_expr, input_dfschema, execution_props)?;
            let then_expr =
                create_physical_exprs(then_expr, input_dfschema, execution_props)?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> =
                when_expr
                    .iter()
                    .zip(then_expr.iter())
                    .map(|(w, t)| (Arc::clone(w), Arc::clone(t)))
                    .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> =
                if let Some(e) = &case.else_expr {
                    Some(create_physical_expr(
                        e.as_ref(),
                        input_dfschema,
                        execution_props,
                    )?)
                } else {
                    None
                };
            Ok(expressions::case(expr, when_then_expr, else_expr)?)
        }
        Expr::Cast(Cast { expr, field }) => expressions::cast_with_target_field(
            create_physical_expr(expr, input_dfschema, execution_props)?,
            input_schema,
            Arc::clone(field),
            None,
        ),
        Expr::TryCast(TryCast { expr, field }) => {
            if !field.metadata().is_empty() {
                let (_, src_field) = expr.to_field(input_dfschema)?;
                return plan_err!(
                    "TryCast from {} to {} is not supported",
                    format_type_and_metadata(
                        src_field.data_type(),
                        Some(src_field.metadata()),
                    ),
                    format_type_and_metadata(field.data_type(), Some(field.metadata()))
                );
            }

            expressions::try_cast(
                create_physical_expr(expr, input_dfschema, execution_props)?,
                input_schema,
                field.data_type().clone(),
            )
        }
        Expr::Not(expr) => {
            expressions::not(create_physical_expr(expr, input_dfschema, execution_props)?)
        }
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            execution_props,
        )?),
        Expr::ScalarFunction(ScalarFunction { func, args }) => {
            let physical_args =
                create_physical_exprs(args, input_dfschema, execution_props)?;
            let config_options = match execution_props.config_options.as_ref() {
                Some(config_options) => Arc::clone(config_options),
                None => Arc::new(ConfigOptions::default()),
            };

            Ok(Arc::new(ScalarFunctionExpr::try_new(
                Arc::clone(func),
                physical_args,
                input_schema,
                config_options,
            )?))
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let value_expr = create_physical_expr(expr, input_dfschema, execution_props)?;
            let low_expr = create_physical_expr(low, input_dfschema, execution_props)?;
            let high_expr = create_physical_expr(high, input_dfschema, execution_props)?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(
                    Arc::clone(&value_expr),
                    Operator::GtEq,
                    low_expr,
                    input_schema,
                )?,
                Operator::And,
                binary(
                    Arc::clone(&value_expr),
                    Operator::LtEq,
                    high_expr,
                    input_schema,
                )?,
                input_schema,
            );

            if *negated {
                expressions::not(binary_expr?)
            } else {
                binary_expr
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => match expr.as_ref() {
            Expr::Literal(ScalarValue::Utf8(None), _) => {
                Ok(expressions::lit(ScalarValue::Boolean(None)))
            }
            _ => {
                let value_expr =
                    create_physical_expr(expr, input_dfschema, execution_props)?;

                let list_exprs =
                    create_physical_exprs(list, input_dfschema, execution_props)?;
                expressions::in_list(value_expr, list_exprs, negated, input_schema)
            }
        },
        Expr::ScalarSubquery(sq) => {
            match execution_props.subquery_indexes.get(sq) {
                Some(&index) => {
                    let schema = sq.subquery.schema();
                    if schema.fields().len() != 1 {
                        return plan_err!(
                            "Scalar subquery must return exactly one column, got {}",
                            schema.fields().len()
                        );
                    }
                    let dt = schema.field(0).data_type().clone();
                    let nullable = schema.field(0).is_nullable();
                    Ok(Arc::new(ScalarSubqueryExpr::new(
                        dt,
                        nullable,
                        index,
                        execution_props.subquery_results.clone(),
                    )))
                }
                None => {
                    // Not found: either a correlated subquery that wasn't
                    // rewritten to a join, or an uncorrelated one that wasn't
                    // registered by the physical planner.
                    not_impl_err!(
                        "Physical plan does not support logical expression {e:?}"
                    )
                }
            }
        }
        Expr::Placeholder(Placeholder { id, .. }) => {
            exec_err!("Placeholder '{id}' was not provided a value for execution.")
        }
        Expr::HigherOrderFunction(invocation @ HigherOrderFunction { func, args }) => {
            let num_lambdas = args
                .iter()
                .filter(|arg| matches!(arg, Expr::Lambda(_)))
                .count();

            let mut lambda_parameters =
                invocation.lambda_parameters(input_dfschema)?.into_iter();

            if num_lambdas > lambda_parameters.len() {
                return plan_err!(
                    "{} lambda_parameters returned only {} values for {num_lambdas} lambdas",
                    func.name(),
                    lambda_parameters.len()
                );
            }

            let physical_args = args
                .iter()
                .map(|arg| match arg {
                    Expr::Lambda(lambda) => {
                        let lambda_parameters = lambda_parameters
                            .next()
                            .ok_or_else(|| {
                                internal_datafusion_err!(
                                    "lambda_parameters len should have been checked above"
                                )
                            })?
                            .into_iter()
                            .zip(&lambda.params)
                            .map(|(field, name)| field.renamed(name.as_str()))
                            .collect();

                        let lambda_schema = DFSchema::from_unqualified_fields(
                            lambda_parameters,
                            HashMap::new(),
                        )?;

                        create_physical_expr(arg, &lambda_schema, execution_props)
                    }
                    _ => create_physical_expr(arg, input_dfschema, execution_props),
                })
                .collect::<Result<_>>()?;

            let config_options = match execution_props.config_options.as_ref() {
                Some(config_options) => Arc::clone(config_options),
                None => Arc::new(ConfigOptions::default()),
            };

            Ok(Arc::new(HigherOrderFunctionExpr::try_new_with_schema(
                Arc::clone(func),
                physical_args,
                input_schema,
                config_options,
            )?))
        }
        Expr::Lambda(Lambda { params, body }) => {
            // tracked at https://github.com/apache/datafusion/issues/21172
            if body.any_column_refs() {
                return plan_err!("lambda doesn't support column capture");
            }

            expressions::lambda(
                params,
                create_physical_expr(body, input_dfschema, execution_props)?,
            )
        }
        Expr::LambdaVariable(LambdaVariable {
            name,
            field,
            spans: _,
        }) => {
            let field = field.as_ref().ok_or_else(|| {
                plan_datafusion_err!("unresolved LambdaVariable {name}")
            })?;

            let index = input_dfschema.inner().index_of(name)?;
            let schema_field = input_dfschema.field(index);

            // LambdaVariable.field will be made optional as in Expr::Placeholder
            // and only LambdaVariable.name used, and field.name ignored,
            // so they're not enforced to match for logical expressions
            // Rename the field to match the schema one and use it's PartialEq impl instead
            // of checking property by property and fail if new properties get's added to it.
            // While not necessary, the sql planner does create lambda vars with matching names,
            // so this shouldn't allocate with a lambda var from it
            let renamed_field = Arc::clone(field).renamed(name);

            if &renamed_field != schema_field {
                return plan_err!(
                    "LambdaVariable field and schema field mismatch {} != {}",
                    renamed_field,
                    schema_field
                );
            }

            Ok(Arc::new(expressions::LambdaVariable::new(
                index,
                Arc::clone(schema_field),
            )))
        }
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}

/// Create vector of Physical Expression from a vector of logical expression
pub fn create_physical_exprs<'a, I>(
    exprs: I,
    input_dfschema: &DFSchema,
    execution_props: &ExecutionProps,
) -> Result<Vec<Arc<dyn PhysicalExpr>>>
where
    I: IntoIterator<Item = &'a Expr>,
{
    exprs
        .into_iter()
        .map(|expr| create_physical_expr(expr, input_dfschema, execution_props))
        .collect()
}

/// Convert a logical expression to a physical expression (without any simplification, etc)
pub fn logical2physical(expr: &Expr, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    // TODO this makes a deep copy of the Schema. Should take SchemaRef instead and avoid deep copy
    let df_schema = schema.clone().to_dfschema().unwrap();
    let execution_props = ExecutionProps::new();
    create_physical_expr(expr, &df_schema, &execution_props).unwrap()
}

#[cfg(test)]
mod tests {
    use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_expr::col;

    use super::*;

    fn test_cast_schema() -> Schema {
        Schema::new(vec![Field::new("a", DataType::Int32, false)])
    }

    fn lower_cast_expr(expr: &Expr, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        let df_schema = DFSchema::try_from(schema.clone())?;
        create_physical_expr(expr, &df_schema, &ExecutionProps::new())
    }

    fn as_planner_cast(physical: &Arc<dyn PhysicalExpr>) -> &expressions::CastExpr {
        physical
            .downcast_ref::<expressions::CastExpr>()
            .expect("planner should lower logical CAST to CastExpr")
    }

    #[test]
    fn test_create_physical_expr_scalar_input_output() -> Result<()> {
        let expr = col("letter").eq(lit("A"));

        let schema = Schema::new(vec![Field::new("letter", DataType::Utf8, false)]);
        let df_schema = DFSchema::try_from_qualified_schema("data", &schema)?;
        let p = create_physical_expr(&expr, &df_schema, &ExecutionProps::new())?;

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(StringArray::from_iter_values(vec![
                "A", "B", "C", "D",
            ]))],
        )?;
        let result = p.evaluate(&batch)?;
        let result = result.into_array(4).expect("Failed to convert to array");

        assert_eq!(
            &result,
            &(Arc::new(BooleanArray::from(vec![true, false, false, false,])) as ArrayRef)
        );

        Ok(())
    }

    #[test]
    fn test_cast_lowering_preserves_target_field_metadata() -> Result<()> {
        let schema = test_cast_schema();
        let target_field = Arc::new(
            Field::new("cast_target", DataType::Int64, true)
                .with_metadata([("target_meta".to_string(), "1".to_string())].into()),
        );
        let cast_expr = Expr::Cast(Cast::new_from_field(
            Box::new(col("a")),
            Arc::clone(&target_field),
        ));

        let physical = lower_cast_expr(&cast_expr, &schema)?;
        let cast = as_planner_cast(&physical);

        assert_eq!(cast.target_field(), &target_field);
        assert_eq!(physical.return_field(&schema)?, target_field);
        assert!(physical.nullable(&schema)?);

        Ok(())
    }

    #[test]
    fn test_cast_lowering_preserves_standard_cast_semantics() -> Result<()> {
        let schema = test_cast_schema();
        let cast_expr = Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int64));

        let physical = lower_cast_expr(&cast_expr, &schema)?;
        let cast = as_planner_cast(&physical);
        let returned_field = physical.return_field(&schema)?;

        assert_eq!(cast.cast_type(), &DataType::Int64);
        assert_eq!(returned_field.name(), "a");
        assert_eq!(returned_field.data_type(), &DataType::Int64);
        assert!(!physical.nullable(&schema)?);

        Ok(())
    }

    #[test]
    fn test_cast_lowering_preserves_same_type_field_semantics() -> Result<()> {
        let schema = test_cast_schema();
        let target_field = Arc::new(
            Field::new("same_type_cast", DataType::Int32, true).with_metadata(
                [("target_meta".to_string(), "same-type".to_string())].into(),
            ),
        );
        let cast_expr = Expr::Cast(Cast::new_from_field(
            Box::new(col("a")),
            Arc::clone(&target_field),
        ));

        let physical = lower_cast_expr(&cast_expr, &schema)?;
        let cast = as_planner_cast(&physical);

        assert_eq!(cast.target_field(), &target_field);
        assert_eq!(physical.return_field(&schema)?, target_field);
        assert!(physical.nullable(&schema)?);

        Ok(())
    }

    /// Test that deeply nested expressions do not cause a stack overflow.
    ///
    /// This test only runs when the `recursive_protection` feature is enabled,
    /// as it would overflow the stack otherwise.
    #[test]
    #[cfg_attr(not(feature = "recursive_protection"), ignore)]
    fn test_deeply_nested_binary_expr() -> Result<()> {
        // Create a deeply nested binary expression tree: ((((a + a) + a) + a) + ... )
        // With 1000 levels of nesting, this would overflow the stack without recursion protection.
        let depth = 1000;

        let mut expr = col("a");
        for _ in 0..depth {
            expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(expr),
                op: Operator::Plus,
                right: Box::new(col("a")),
            });
        }

        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let df_schema = DFSchema::try_from(schema)?;

        // This should not stack overflow
        let _physical_expr =
            create_physical_expr(&expr, &df_schema, &ExecutionProps::new())?;

        Ok(())
    }
}
