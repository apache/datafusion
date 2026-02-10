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

use super::{Between, Expr, predicate_bounds};
use crate::expr::{
    AggregateFunction, AggregateFunctionParams, Alias, BinaryExpr, Cast, InList,
    InSubquery, Placeholder, ScalarFunction, TryCast, Unnest, WindowFunction,
    WindowFunctionParams,
};
use crate::type_coercion::functions::{UDFCoercionExt, fields_with_udf};
use crate::udf::ReturnFieldArgs;
use crate::{LogicalPlan, Projection, Subquery, WindowFunctionDefinition, utils};
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::datatype::FieldExt;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{
    Column, DataFusionError, ExprSchema, Result, ScalarValue, Spans, TableReference,
    not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use std::sync::Arc;

/// Trait to allow expr to typable with respect to a schema
pub trait ExprSchemable {
    /// Given a schema, return the type of the expr
    #[deprecated(since = "53.0.0", note = "use to_field")]
    fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType> {
        Ok(self.to_field(schema)?.1.data_type().clone())
    }

    /// Given a schema, return the nullability of the expr
    #[deprecated(since = "53.0.0", note = "use to_field")]
    fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool> {
        Ok(self.to_field(input_schema)?.1.is_nullable())
    }

    /// Given a schema, return the expr's optional metadata
    #[deprecated(since = "53.0.0", note = "use to_field")]
    fn metadata(&self, schema: &dyn ExprSchema) -> Result<FieldMetadata> {
        Ok(FieldMetadata::from(self.to_field(schema)?.1.metadata()))
    }

    /// Convert to a field with respect to a schema
    fn to_field(
        &self,
        input_schema: &dyn ExprSchema,
    ) -> Result<(Option<TableReference>, Arc<Field>)>;

    /// Cast to a type with respect to a schema
    fn cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> Result<Expr>;

    /// Given a schema, return the type and nullability of the expr
    #[deprecated(
        since = "51.0.0",
        note = "Use `to_field().1.is_nullable` and `to_field().1.data_type()` directly instead"
    )]
    fn data_type_and_nullable(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(DataType, bool)> {
        let field = self.to_field(schema)?.1;

        Ok((field.data_type().clone(), field.is_nullable()))
    }
}

impl ExprSchemable for Expr {
    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    ///
    /// This function converts an expression into a field with appropriate metadata
    /// and nullability based on the expression type and context. It is the primary
    /// mechanism for determining field-level schemas.
    ///
    /// # Field Property Resolution
    ///
    /// For each expression, the following properties are determined:
    ///
    /// ## Data Type Resolution
    /// - **Column references**: Data type from input schema field
    /// - **Literals**: Data type inferred from literal value
    /// - **Aliases**: Data type inherited from the underlying expression (the aliased expression)
    /// - **Binary expressions**: Result type from type coercion rules
    /// - **Boolean expressions**: Always a boolean type
    /// - **Cast expressions**: Target data type from cast operation
    /// - **Function calls**: Return type based on function signature and argument types
    ///
    /// ## Nullability Determination
    /// - **Column references**: Inherit nullability from input schema field
    /// - **Literals**: Nullable only if literal value is NULL
    /// - **Aliases**: Inherit nullability from the underlying expression (the aliased expression)
    /// - **Binary expressions**: Nullable if either operand is nullable
    /// - **Boolean expressions**: Always non-nullable (IS NULL, EXISTS, etc.)
    /// - **Cast expressions**: determined by the input expression's nullability rules
    /// - **Function calls**: Based on function nullability rules and input nullability
    ///
    /// ## Metadata Handling
    /// - **Column references**: Preserve original field metadata from input schema
    /// - **Literals**: Use explicitly provided metadata, otherwise empty
    /// - **Aliases**: Merge underlying expr metadata with alias-specific metadata, preferring the alias metadata
    /// - **Binary expressions**: field metadata is empty
    /// - **Boolean expressions**: field metadata is empty
    /// - **Cast expressions**: determined by the input expression's field metadata handling
    /// - **Scalar functions**: Generate metadata via function's [`return_field_from_args`] method,
    ///   with the default implementation returning empty field metadata
    /// - **Aggregate functions**: Generate metadata via function's [`return_field`] method,
    ///   with the default implementation returning empty field metadata
    /// - **Window functions**: field metadata follows the function's return field
    ///
    /// ## Table Reference Scoping
    /// - Establishes proper qualified field references when columns belong to specific tables
    /// - Maintains table context for accurate field resolution in multi-table scenarios
    ///
    /// So for example, a projected expression `col(c1) + col(c2)` is
    /// placed in an output field **named** col("c1 + c2")
    ///
    /// [`return_field_from_args`]: crate::ScalarUDF::return_field_from_args
    /// [`return_field`]: crate::AggregateUDF::return_field
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn to_field(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(Option<TableReference>, Arc<Field>)> {
        let (relation, schema_name) = self.qualified_name();
        let field = match self {
            Expr::Alias(Alias {
                expr,
                name: _,
                metadata,
                ..
            }) => {
                let field = expr.to_field(schema).map(|(_, f)| f)?;
                let mut combined_metadata = FieldMetadata::from(field.metadata());
                if let Some(metadata) = metadata {
                    combined_metadata.extend(metadata.clone());
                }

                Ok(field.with_field_metadata(&combined_metadata))
            }
            Expr::Negative(expr) => expr.to_field(schema).map(|(_, f)| f),
            Expr::Column(c) => schema.field_from_column(c).map(Arc::clone),
            Expr::OuterReferenceColumn(field, _) => {
                Ok(Arc::clone(field).renamed(&schema_name))
            }
            Expr::ScalarVariable(field, _) => Ok(Arc::clone(field).renamed(&schema_name)),
            Expr::Literal(l, metadata) => Ok(Arc::new(
                Field::new(&schema_name, l.data_type(), l.is_null())
                    .with_field_metadata_opt(metadata.as_ref()),
            )),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Boolean, false)))
            }
            Expr::ScalarSubquery(subquery) => {
                Ok(Arc::clone(&subquery.subquery.schema().fields()[0]))
            }
            Expr::BinaryExpr(BinaryExpr { left, right, op }) => {
                let (left_field, right_field) =
                    (left.to_field(schema)?.1, right.to_field(schema)?.1);

                let (lhs_type, lhs_nullable) =
                    (left_field.data_type(), left_field.is_nullable());
                let (rhs_type, rhs_nullable) =
                    (right_field.data_type(), right_field.is_nullable());
                let mut coercer = BinaryTypeCoercer::new(lhs_type, op, rhs_type);
                coercer.set_lhs_spans(left.spans().cloned().unwrap_or_default());
                coercer.set_rhs_spans(right.spans().cloned().unwrap_or_default());
                Ok(Arc::new(Field::new(
                    &schema_name,
                    coercer.get_result_type()?,
                    lhs_nullable || rhs_nullable,
                )))
            }
            Expr::WindowFunction(window_function) => {
                let WindowFunction {
                    fun,
                    params: WindowFunctionParams { args, .. },
                    ..
                } = window_function.as_ref();

                let fields = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?;
                match fun {
                    WindowFunctionDefinition::AggregateUDF(udaf) => {
                        let new_fields =
                            verify_function_arguments(udaf.as_ref(), &fields)?;
                        let return_field = udaf.return_field(&new_fields)?;
                        Ok(return_field)
                    }
                    WindowFunctionDefinition::WindowUDF(udwf) => {
                        let new_fields =
                            verify_function_arguments(udwf.as_ref(), &fields)?;
                        let return_field = udwf
                            .field(WindowUDFFieldArgs::new(&new_fields, &schema_name))?;
                        Ok(return_field)
                    }
                }
            }
            Expr::AggregateFunction(AggregateFunction {
                func,
                params: AggregateFunctionParams { args, .. },
            }) => {
                let fields = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?;
                let new_fields = verify_function_arguments(func.as_ref(), &fields)?;
                func.return_field(&new_fields)
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let fields = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?;
                let new_fields = verify_function_arguments(func.as_ref(), &fields)?;

                let arguments = args
                    .iter()
                    .map(|e| match e {
                        Expr::Literal(sv, _) => Some(sv),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                let args = ReturnFieldArgs {
                    arg_fields: &new_fields,
                    scalar_arguments: &arguments,
                };

                func.return_field_from_args(args)
            }
            // _ => Ok((self.get_type(schema)?, self.nullable(schema)?)),
            Expr::Cast(Cast { expr, data_type }) => expr
                .to_field(schema)
                .map(|(_, f)| f.retyped(data_type.clone())),
            Expr::Placeholder(Placeholder {
                id: _,
                field: Some(field),
            }) => Ok(Arc::clone(field).renamed(&schema_name)),
            Expr::Like(_) | Expr::SimilarTo(_) => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Boolean, true)))
            }
            Expr::Not(expr) => {
                let field = expr.to_field(schema).map(|(_, f)| f)?;
                Ok(Arc::new(Field::new(
                    &schema_name,
                    DataType::Boolean,
                    field.is_nullable(),
                )))
            }
            Expr::Between(Between {
                expr, low, high, ..
            }) => {
                let expr_field = expr.to_field(schema).map(|(_, f)| f)?;
                let low_field = low.to_field(schema).map(|(_, f)| f)?;
                let high_field = high.to_field(schema).map(|(_, f)| f)?;
                Ok(Arc::new(Field::new(
                    &schema_name,
                    DataType::Boolean,
                    expr_field.is_nullable()
                        || low_field.is_nullable()
                        || high_field.is_nullable(),
                )))
            }
            Expr::Case(case) => {
                let mut data_type = DataType::Null;
                for (_, then_expr) in &case.when_then_expr {
                    let then_field = then_expr.to_field(schema).map(|(_, f)| f)?;
                    if !then_field.data_type().is_null() {
                        data_type = then_field.data_type().clone();
                        break;
                    }
                }
                if data_type.is_null()
                    && let Some(else_expr) = &case.else_expr
                {
                    data_type = else_expr
                        .to_field(schema)
                        .map(|(_, f)| f)?
                        .data_type()
                        .clone();
                }

                // CASE
                //   WHEN condition1 THEN result1
                //   WHEN condition2 THEN result2
                //   ...
                //   ELSE resultN
                // END
                //
                // The result of a CASE expression is nullable if any of the results are nullable
                // or if there is no ELSE clause (in which case the result is NULL if none of
                // the conditions are met)
                let mut is_nullable = case.else_expr.is_none();
                if !is_nullable {
                    for (w, t) in &case.when_then_expr {
                        let t_field = t.to_field(schema).map(|(_, f)| f)?;
                        if !t_field.is_nullable() {
                            continue;
                        }

                        // For case-with-expression assume all 'then' expressions are reachable
                        if case.expr.is_some() {
                            is_nullable = true;
                            break;
                        }

                        // For branches with a nullable 'then' expression, try to determine
                        // if the 'then' expression is ever reachable in the situation where
                        // it would evaluate to null.
                        let bounds = predicate_bounds::evaluate_bounds(
                            w,
                            Some(unwrap_certainly_null_expr(t)),
                            schema,
                        )?;

                        if bounds.contains_value(ScalarValue::Boolean(Some(true)))? {
                            is_nullable = true;
                            break;
                        }
                    }
                    if !is_nullable
                        && let Some(e) = &case.else_expr
                    {
                        is_nullable =
                            e.to_field(schema).map(|(_, f)| f)?.is_nullable();
                    }
                }

                Ok(Arc::new(Field::new(&schema_name, data_type, is_nullable)))
            }
            Expr::TryCast(TryCast { data_type, .. }) => {
                Ok(Arc::new(Field::new(&schema_name, data_type.clone(), true)))
            }
            Expr::InList(InList { expr, list, .. }) => {
                let expr_field = expr.to_field(schema).map(|(_, f)| f)?;
                let mut nullable = expr_field.is_nullable();
                if !nullable {
                    for e in list.iter().take(6) {
                        if e.to_field(schema).map(|(_, f)| f)?.is_nullable() {
                            nullable = true;
                            break;
                        }
                    }
                    if !nullable && list.len() > 6 {
                        nullable = true;
                    }
                }
                Ok(Arc::new(Field::new(
                    &schema_name,
                    DataType::Boolean,
                    nullable,
                )))
            }
            Expr::InSubquery(InSubquery { expr, .. }) => {
                let field = expr.to_field(schema).map(|(_, f)| f)?;
                Ok(Arc::new(Field::new(
                    &schema_name,
                    DataType::Boolean,
                    field.is_nullable(),
                )))
            }
            Expr::SetComparison(_) => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Boolean, true)))
            }
            #[expect(deprecated)]
            Expr::Wildcard { .. } => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Null, false)))
            }
            Expr::GroupingSet(_) => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Null, false)))
            }
            Expr::Placeholder(_) => {
                Ok(Arc::new(Field::new(&schema_name, DataType::Null, true)))
            }
            Expr::Unnest(Unnest { expr }) => {
                let arg_field = expr.to_field(schema).map(|(_, f)| f)?;
                let arg_data_type = arg_field.data_type();
                // Unnest's output type is the inner type of the list
                let data_type = match arg_data_type {
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) => field.data_type().clone(),
                    DataType::Struct(_) => arg_data_type.clone(),
                    DataType::Null => {
                        return not_impl_err!("unnest() does not support null yet");
                    }
                    _ => {
                        return plan_err!(
                            "unnest() can only be applied to array, struct and null"
                        );
                    }
                };
                Ok(Arc::new(Field::new(&schema_name, data_type, true)))
            }
        }?;

        Ok((
            relation,
            // todo avoid this rename / use the name above
            field.renamed(&schema_name),
        ))
    }

    /// Wraps this expression in a cast to a target [arrow::datatypes::DataType].
    ///
    /// # Errors
    ///
    /// This function errors when it is impossible to cast the
    /// expression to the target [arrow::datatypes::DataType].
    fn cast_to(self, cast_to_type: &DataType, schema: &dyn ExprSchema) -> Result<Expr> {
        let this_type = self.to_field(schema)?.1.data_type().clone();
        if this_type == *cast_to_type {
            return Ok(self);
        }

        // TODO(kszucs): Most of the operations do not validate the type correctness
        // like all of the binary expressions below. Perhaps Expr should track the
        // type of the expression?

        // Special handling for struct-to-struct casts with name-based field matching
        let can_cast = match (&this_type, cast_to_type) {
            (DataType::Struct(_), DataType::Struct(_)) => {
                // Always allow struct-to-struct casts; field matching happens at runtime
                true
            }
            _ => can_cast_types(&this_type, cast_to_type),
        };

        if can_cast {
            match self {
                Expr::ScalarSubquery(subquery) => {
                    Ok(Expr::ScalarSubquery(cast_subquery(subquery, cast_to_type)?))
                }
                _ => Ok(Expr::Cast(Cast::new(Box::new(self), cast_to_type.clone()))),
            }
        } else {
            plan_err!("Cannot automatically convert {this_type} to {cast_to_type}")
        }
    }
}

/// Verify that function is invoked with correct number and type of arguments as
/// defined in `TypeSignature`.
fn verify_function_arguments<F: UDFCoercionExt>(
    function: &F,
    input_fields: &[FieldRef],
) -> Result<Vec<FieldRef>> {
    fields_with_udf(input_fields, function).map_err(|err| {
        let data_types = input_fields
            .iter()
            .map(|f| f.data_type())
            .cloned()
            .collect::<Vec<_>>();
        plan_datafusion_err!(
            "{} {}",
            match err {
                DataFusionError::Plan(msg) => msg,
                err => err.to_string(),
            },
            utils::generate_signature_error_message(
                function.name(),
                function.signature(),
                &data_types
            )
        )
    })
}

/// Returns the innermost [Expr] that is provably null if `expr` is null.
fn unwrap_certainly_null_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Not(e) => unwrap_certainly_null_expr(e),
        Expr::Negative(e) => unwrap_certainly_null_expr(e),
        Expr::Cast(e) => unwrap_certainly_null_expr(e.expr.as_ref()),
        _ => expr,
    }
}

/// Cast subquery in InSubquery/ScalarSubquery to a given type.
///
/// 1. **Projection plan**: If the subquery is a projection (i.e. a SELECT statement with specific
///    columns), it casts the first expression in the projection to the target type and creates a
///    new projection with the casted expression.
/// 2. **Non-projection plan**: If the subquery isn't a projection, it adds a projection to the plan
///    with the casted first column.
pub fn cast_subquery(subquery: Subquery, cast_to_type: &DataType) -> Result<Subquery> {
    if subquery.subquery.schema().field(0).data_type() == cast_to_type {
        return Ok(subquery);
    }

    let plan = subquery.subquery.as_ref();
    let new_plan = match plan {
        LogicalPlan::Projection(projection) => {
            let cast_expr = projection.expr[0]
                .clone()
                .cast_to(cast_to_type, projection.input.schema())?;
            LogicalPlan::Projection(Projection::try_new(
                vec![cast_expr],
                Arc::clone(&projection.input),
            )?)
        }
        _ => {
            let cast_expr = Expr::Column(Column::from(plan.schema().qualified_field(0)))
                .cast_to(cast_to_type, subquery.subquery.schema())?;
            LogicalPlan::Projection(Projection::try_new(
                vec![cast_expr],
                subquery.subquery,
            )?)
        }
    };
    Ok(Subquery {
        subquery: Arc::new(new_plan),
        outer_ref_columns: subquery.outer_ref_columns,
        spans: Spans::new(),
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::{and, col, lit, not, or, out_ref_col_with_metadata, when};

    use arrow::datatypes::FieldRef;
    use datafusion_common::{DFSchema, ScalarValue, assert_or_internal_err};

    macro_rules! test_is_expr_nullable {
        ($EXPR_TYPE:ident) => {{
            let expr = lit(ScalarValue::Null).$EXPR_TYPE();
            assert!(
                !expr
                    .to_field(&MockExprSchema::new())
                    .unwrap()
                    .1
                    .is_nullable()
            );
        }};
    }

    #[test]
    fn expr_schema_nullability() {
        let expr = col("foo").eq(lit(1));
        assert!(
            !expr
                .to_field(&MockExprSchema::new())
                .unwrap()
                .1
                .is_nullable()
        );
        assert!(
            expr.to_field(&MockExprSchema::new().with_nullable(true))
                .unwrap()
                .1
                .is_nullable()
        );

        test_is_expr_nullable!(is_null);
        test_is_expr_nullable!(is_not_null);
        test_is_expr_nullable!(is_true);
        test_is_expr_nullable!(is_not_true);
        test_is_expr_nullable!(is_false);
        test_is_expr_nullable!(is_not_false);
        test_is_expr_nullable!(is_unknown);
        test_is_expr_nullable!(is_not_unknown);
    }

    #[test]
    fn test_between_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Int32)
                .with_nullable(nullable)
        };

        let expr = col("foo").between(lit(1), lit(2));
        assert!(!expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
        assert!(expr.to_field(&get_schema(true)).unwrap().1.is_nullable());

        let null = lit(ScalarValue::Int32(None));

        let expr = col("foo").between(null.clone(), lit(2));
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());

        let expr = col("foo").between(lit(1), null.clone());
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());

        let expr = col("foo").between(null.clone(), null);
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
    }

    fn assert_nullability(expr: &Expr, schema: &dyn ExprSchema, expected: bool) {
        assert_eq!(
            expr.to_field(schema).unwrap().1.is_nullable(),
            expected,
            "Nullability of '{expr}' should be {expected}"
        );
    }

    fn assert_not_nullable(expr: &Expr, schema: &dyn ExprSchema) {
        assert_nullability(expr, schema, false);
    }

    fn assert_nullable(expr: &Expr, schema: &dyn ExprSchema) {
        assert_nullability(expr, schema, true);
    }

    #[test]
    fn test_case_expression_nullability() -> Result<()> {
        let nullable_schema = MockExprSchema::new()
            .with_data_type(DataType::Int32)
            .with_nullable(true);

        let not_nullable_schema = MockExprSchema::new()
            .with_data_type(DataType::Int32)
            .with_nullable(false);

        // CASE WHEN x IS NOT NULL THEN x ELSE 0
        let e = when(col("x").is_not_null(), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN NOT x IS NULL THEN x ELSE 0
        let e = when(not(col("x").is_null()), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN X = 5 THEN x ELSE 0
        let e = when(col("x").eq(lit(5)), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS NOT NULL AND x = 5 THEN x ELSE 0
        let e = when(and(col("x").is_not_null(), col("x").eq(lit(5))), col("x"))
            .otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x = 5 AND x IS NOT NULL THEN x ELSE 0
        let e = when(and(col("x").eq(lit(5)), col("x").is_not_null()), col("x"))
            .otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS NOT NULL OR x = 5 THEN x ELSE 0
        let e = when(or(col("x").is_not_null(), col("x").eq(lit(5))), col("x"))
            .otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x = 5 OR x IS NOT NULL THEN x ELSE 0
        let e = when(or(col("x").eq(lit(5)), col("x").is_not_null()), col("x"))
            .otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN (x = 5 AND x IS NOT NULL) OR (x = bar AND x IS NOT NULL) THEN x ELSE 0
        let e = when(
            or(
                and(col("x").eq(lit(5)), col("x").is_not_null()),
                and(col("x").eq(col("bar")), col("x").is_not_null()),
            ),
            col("x"),
        )
        .otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x = 5 OR x IS NULL THEN x ELSE 0
        let e = when(or(col("x").eq(lit(5)), col("x").is_null()), col("x"))
            .otherwise(lit(0))?;
        assert_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS TRUE THEN x ELSE 0
        let e = when(col("x").is_true(), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS NOT TRUE THEN x ELSE 0
        let e = when(col("x").is_not_true(), col("x")).otherwise(lit(0))?;
        assert_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS FALSE THEN x ELSE 0
        let e = when(col("x").is_false(), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS NOT FALSE THEN x ELSE 0
        let e = when(col("x").is_not_false(), col("x")).otherwise(lit(0))?;
        assert_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS UNKNOWN THEN x ELSE 0
        let e = when(col("x").is_unknown(), col("x")).otherwise(lit(0))?;
        assert_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x IS NOT UNKNOWN THEN x ELSE 0
        let e = when(col("x").is_not_unknown(), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN x LIKE 'x' THEN x ELSE 0
        let e = when(col("x").like(lit("x")), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN 0 THEN x ELSE 0
        let e = when(lit(0), col("x")).otherwise(lit(0))?;
        assert_not_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        // CASE WHEN 1 THEN x ELSE 0
        let e = when(lit(1), col("x")).otherwise(lit(0))?;
        assert_nullable(&e, &nullable_schema);
        assert_not_nullable(&e, &not_nullable_schema);

        Ok(())
    }

    #[test]
    fn test_inlist_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Int32)
                .with_nullable(nullable)
        };

        let expr = col("foo").in_list(vec![lit(1); 5], false);
        assert!(!expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
        assert!(expr.to_field(&get_schema(true)).unwrap().1.is_nullable());
        // Testing nullable() returns an error.
        assert!(
            expr.to_field(&get_schema(false).with_error_on_nullable(true))
                .is_err()
        );

        let null = lit(ScalarValue::Int32(None));
        let expr = col("foo").in_list(vec![null, lit(1)], false);
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());

        // Testing on long list (more than 6 elements => conservative nullable)
        let expr = col("foo").in_list(vec![lit(1); 7], false);
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
    }

    #[test]
    fn test_like_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Utf8)
                .with_nullable(nullable)
        };

        let expr = col("foo").like(lit("bar"));
        // Like/SimilarTo currently return nullable=true (conservative)
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
        assert!(expr.to_field(&get_schema(true)).unwrap().1.is_nullable());

        let expr = col("foo").like(lit(ScalarValue::Utf8(None)));
        assert!(expr.to_field(&get_schema(false)).unwrap().1.is_nullable());
    }

    #[test]
    fn expr_schema_data_type() {
        let expr = col("foo");
        assert_eq!(
            DataType::Utf8,
            *expr
                .to_field(&MockExprSchema::new().with_data_type(DataType::Utf8))
                .unwrap()
                .1
                .data_type()
        );
    }

    #[test]
    fn test_expr_metadata() {
        let mut meta = HashMap::new();
        meta.insert("bar".to_string(), "buzz".to_string());
        let meta = FieldMetadata::from(meta);
        let expr = col("foo");
        let schema = MockExprSchema::new()
            .with_data_type(DataType::Int32)
            .with_metadata(meta.clone());

        // col, alias, and cast should be metadata-preserving
        assert_eq!(
            meta,
            FieldMetadata::from(expr.to_field(&schema).unwrap().1.metadata())
        );
        assert_eq!(
            meta,
            FieldMetadata::from(
                expr.clone()
                    .alias("bar")
                    .to_field(&schema)
                    .unwrap()
                    .1
                    .metadata()
            )
        );
        assert_eq!(
            meta,
            FieldMetadata::from(
                expr.clone()
                    .cast_to(&DataType::Int64, &schema)
                    .unwrap()
                    .to_field(&schema)
                    .unwrap()
                    .1
                    .metadata()
            )
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![meta.add_to_field(Field::new("foo", DataType::Int32, true))].into(),
            HashMap::new(),
        )
        .unwrap();

        // verify to_field method populates metadata
        assert_eq!(
            meta,
            FieldMetadata::from(expr.to_field(&schema).unwrap().1.metadata())
        );

        // outer ref constructed by `out_ref_col_with_metadata` should be metadata-preserving
        let outer_ref = out_ref_col_with_metadata(
            DataType::Int32,
            meta.to_hashmap(),
            Column::from_name("foo"),
        );
        assert_eq!(
            meta,
            FieldMetadata::from(outer_ref.to_field(&schema).unwrap().1.metadata())
        );
    }

    #[test]
    fn test_expr_placeholder() {
        let schema = MockExprSchema::new();

        let mut placeholder_meta = HashMap::new();
        placeholder_meta.insert("bar".to_string(), "buzz".to_string());
        let placeholder_meta = FieldMetadata::from(placeholder_meta);

        let expr = Expr::Placeholder(Placeholder::new_with_field(
            "".to_string(),
            Some(
                Field::new("", DataType::Utf8, true)
                    .with_metadata(placeholder_meta.to_hashmap())
                    .into(),
            ),
        ));

        let field = expr.to_field(&schema).unwrap().1;
        assert_eq!(
            (field.data_type(), field.is_nullable()),
            (&DataType::Utf8, true)
        );
        assert_eq!(placeholder_meta, FieldMetadata::from(field.metadata()));

        let expr_alias = expr.alias("a placeholder by any other name");
        let expr_alias_field = expr_alias.to_field(&schema).unwrap().1;
        assert_eq!(
            (expr_alias_field.data_type(), expr_alias_field.is_nullable()),
            (&DataType::Utf8, true)
        );
        assert_eq!(
            placeholder_meta,
            FieldMetadata::from(expr_alias_field.metadata())
        );

        // Non-nullable placeholder field should remain non-nullable
        let expr = Expr::Placeholder(Placeholder::new_with_field(
            "".to_string(),
            Some(Field::new("", DataType::Utf8, false).into()),
        ));
        let expr_field = expr.to_field(&schema).unwrap().1;
        assert_eq!(
            (expr_field.data_type(), expr_field.is_nullable()),
            (&DataType::Utf8, false)
        );

        let expr_alias = expr.alias("a placeholder by any other name");
        let expr_alias_field = expr_alias.to_field(&schema).unwrap().1;
        assert_eq!(
            (expr_alias_field.data_type(), expr_alias_field.is_nullable()),
            (&DataType::Utf8, false)
        );
    }

    #[derive(Debug)]
    struct MockExprSchema {
        field: FieldRef,
        error_on_nullable: bool,
    }

    impl MockExprSchema {
        fn new() -> Self {
            Self {
                field: Arc::new(Field::new("mock_field", DataType::Null, false)),
                error_on_nullable: false,
            }
        }

        fn with_nullable(mut self, nullable: bool) -> Self {
            Arc::make_mut(&mut self.field).set_nullable(nullable);
            self
        }

        fn with_data_type(mut self, data_type: DataType) -> Self {
            Arc::make_mut(&mut self.field).set_data_type(data_type);
            self
        }

        fn with_error_on_nullable(mut self, error_on_nullable: bool) -> Self {
            self.error_on_nullable = error_on_nullable;
            self
        }

        fn with_metadata(mut self, metadata: FieldMetadata) -> Self {
            self.field =
                Arc::new(metadata.add_to_field(Arc::unwrap_or_clone(self.field)));
            self
        }
    }

    impl ExprSchema for MockExprSchema {
        fn field_from_column(&self, _col: &Column) -> Result<&FieldRef> {
            assert_or_internal_err!(!self.error_on_nullable, "nullable error");
            Ok(&self.field)
        }
    }

    #[test]
    fn test_scalar_variable() {
        let mut meta = HashMap::new();
        meta.insert("bar".to_string(), "buzz".to_string());
        let meta = FieldMetadata::from(meta);

        let field = Field::new("foo", DataType::Int32, true);
        let field = meta.add_to_field(field);
        let field = Arc::new(field);

        let expr = Expr::ScalarVariable(field, vec!["foo".to_string()]);

        let schema = MockExprSchema::new();

        assert_eq!(
            meta,
            FieldMetadata::from(expr.to_field(&schema).unwrap().1.metadata())
        );
    }
}
