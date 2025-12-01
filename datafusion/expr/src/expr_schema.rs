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

use super::{predicate_bounds, Between, Expr, Like};
use crate::expr::{
    AggregateFunction, AggregateFunctionParams, Alias, BinaryExpr, Cast, InList,
    InSubquery, Placeholder, ScalarFunction, TryCast, Unnest, WindowFunction,
    WindowFunctionParams,
};
use crate::type_coercion::functions::{
    data_types_with_scalar_udf, fields_with_aggregate_udf, fields_with_window_udf,
};
use crate::udf::ReturnFieldArgs;
use crate::{utils, LogicalPlan, Projection, Subquery, WindowFunctionDefinition};
use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::datatype::FieldExt;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::{
    not_impl_err, plan_datafusion_err, plan_err, Column, DataFusionError, ExprSchema,
    Result, ScalarValue, Spans, TableReference,
};
use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use std::sync::Arc;

/// Trait to allow expr to typable with respect to a schema
pub trait ExprSchemable {
    /// Given a schema, return the type of the expr
    fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType>;

    /// Given a schema, return the nullability of the expr
    fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool>;

    /// Given a schema, return the expr's optional metadata
    fn metadata(&self, schema: &dyn ExprSchema) -> Result<FieldMetadata>;

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
    fn data_type_and_nullable(&self, schema: &dyn ExprSchema)
        -> Result<(DataType, bool)>;
}

impl ExprSchemable for Expr {
    /// Returns the [arrow::datatypes::DataType] of the expression
    /// based on [ExprSchema]
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Examples
    ///
    /// Get the type of an expression that adds 2 columns. Adding an Int32
    /// and Float32 results in Float32 type
    ///
    /// ```
    /// # use arrow::datatypes::{DataType, Field};
    /// # use datafusion_common::DFSchema;
    /// # use datafusion_expr::{col, ExprSchemable};
    /// # use std::collections::HashMap;
    ///
    /// fn main() {
    ///     let expr = col("c1") + col("c2");
    ///     let schema = DFSchema::from_unqualified_fields(
    ///         vec![
    ///             Field::new("c1", DataType::Int32, true),
    ///             Field::new("c2", DataType::Float32, true),
    ///         ]
    ///         .into(),
    ///         HashMap::new(),
    ///     )
    ///     .unwrap();
    ///     assert_eq!("Float32", format!("{}", expr.get_type(&schema).unwrap()));
    /// }
    /// ```
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// [arrow::datatypes::DataType].  This happens when e.g. the
    /// expression refers to a column that does not exist in the
    /// schema, or when the expression is incorrectly typed
    /// (e.g. `[utf8] + [bool]`).
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn get_type(&self, schema: &dyn ExprSchema) -> Result<DataType> {
        match self {
            Expr::Alias(Alias { expr, name, .. }) => match &**expr {
                Expr::Placeholder(Placeholder { field, .. }) => match &field {
                    None => schema.data_type(&Column::from_name(name)).cloned(),
                    Some(field) => Ok(field.data_type().clone()),
                },
                _ => expr.get_type(schema),
            },
            Expr::Negative(expr) => expr.get_type(schema),
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::OuterReferenceColumn(field, _) => Ok(field.data_type().clone()),
            Expr::ScalarVariable(ty, _) => Ok(ty.clone()),
            Expr::Literal(l, _) => Ok(l.data_type()),
            Expr::Case(case) => {
                for (_, then_expr) in &case.when_then_expr {
                    let then_type = then_expr.get_type(schema)?;
                    if !then_type.is_null() {
                        return Ok(then_type);
                    }
                }
                case.else_expr
                    .as_ref()
                    .map_or(Ok(DataType::Null), |e| e.get_type(schema))
            }
            Expr::Cast(Cast { data_type, .. })
            | Expr::TryCast(TryCast { data_type, .. }) => Ok(data_type.clone()),
            Expr::Unnest(Unnest { expr }) => {
                let arg_data_type = expr.get_type(schema)?;
                // Unnest's output type is the inner type of the list
                match arg_data_type {
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _) => Ok(field.data_type().clone()),
                    DataType::Struct(_) => Ok(arg_data_type),
                    DataType::Null => {
                        not_impl_err!("unnest() does not support null yet")
                    }
                    _ => {
                        plan_err!(
                            "unnest() can only be applied to array, struct and null"
                        )
                    }
                }
            }
            Expr::ScalarFunction(_func) => {
                let return_type = self.to_field(schema)?.1.data_type().clone();
                Ok(return_type)
            }
            Expr::WindowFunction(window_function) => self
                .data_type_and_nullable_with_window_function(schema, window_function)
                .map(|(return_type, _)| return_type),
            Expr::AggregateFunction(AggregateFunction {
                func,
                params: AggregateFunctionParams { args, .. },
            }) => {
                let fields = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?;
                let new_fields = fields_with_aggregate_udf(&fields, func)
                    .map_err(|err| {
                        let data_types = fields
                            .iter()
                            .map(|f| f.data_type().clone())
                            .collect::<Vec<_>>();
                        plan_datafusion_err!(
                            "{} {}",
                            match err {
                                DataFusionError::Plan(msg) => msg,
                                err => err.to_string(),
                            },
                            utils::generate_signature_error_msg(
                                func.name(),
                                func.signature().clone(),
                                &data_types
                            )
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();
                Ok(func.return_field(&new_fields)?.data_type().clone())
            }
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_) => Ok(DataType::Boolean),
            Expr::ScalarSubquery(subquery) => {
                Ok(subquery.subquery.schema().field(0).data_type().clone())
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ref op,
            }) => BinaryTypeCoercer::new(
                &left.get_type(schema)?,
                op,
                &right.get_type(schema)?,
            )
            .get_result_type(),
            Expr::Like { .. } | Expr::SimilarTo { .. } => Ok(DataType::Boolean),
            Expr::Placeholder(Placeholder { field, .. }) => {
                if let Some(field) = field {
                    Ok(field.data_type().clone())
                } else {
                    // If the placeholder's type hasn't been specified, treat it as
                    // null (unspecified placeholders generate an error during planning)
                    Ok(DataType::Null)
                }
            }
            #[expect(deprecated)]
            Expr::Wildcard { .. } => Ok(DataType::Null),
            Expr::GroupingSet(_) => {
                // Grouping sets do not really have a type and do not appear in projections
                Ok(DataType::Null)
            }
        }
    }

    /// Returns the nullability of the expression based on [ExprSchema].
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// nullability.  This happens when the expression refers to a
    /// column that does not exist in the schema.
    fn nullable(&self, input_schema: &dyn ExprSchema) -> Result<bool> {
        match self {
            Expr::Alias(Alias { expr, .. }) | Expr::Not(expr) | Expr::Negative(expr) => {
                expr.nullable(input_schema)
            }

            Expr::InList(InList { expr, list, .. }) => {
                // Avoid inspecting too many expressions.
                const MAX_INSPECT_LIMIT: usize = 6;
                // Stop if a nullable expression is found or an error occurs.
                let has_nullable = std::iter::once(expr.as_ref())
                    .chain(list)
                    .take(MAX_INSPECT_LIMIT)
                    .find_map(|e| {
                        e.nullable(input_schema)
                            .map(|nullable| if nullable { Some(()) } else { None })
                            .transpose()
                    })
                    .transpose()?;
                Ok(match has_nullable {
                    // If a nullable subexpression is found, the result may also be nullable.
                    Some(_) => true,
                    // If the list is too long, we assume it is nullable.
                    None if list.len() + 1 > MAX_INSPECT_LIMIT => true,
                    // All the subexpressions are non-nullable, so the result must be non-nullable.
                    _ => false,
                })
            }

            Expr::Between(Between {
                expr, low, high, ..
            }) => Ok(expr.nullable(input_schema)?
                || low.nullable(input_schema)?
                || high.nullable(input_schema)?),

            Expr::Column(c) => input_schema.nullable(c),
            Expr::OuterReferenceColumn(field, _) => Ok(field.is_nullable()),
            Expr::Literal(value, _) => Ok(value.is_null()),
            Expr::Case(case) => {
                let nullable_then = case
                    .when_then_expr
                    .iter()
                    .filter_map(|(w, t)| {
                        let is_nullable = match t.nullable(input_schema) {
                            Err(e) => return Some(Err(e)),
                            Ok(n) => n,
                        };

                        // Branches with a then expression that is not nullable do not impact the
                        // nullability of the case expression.
                        if !is_nullable {
                            return None;
                        }

                        // For case-with-expression assume all 'then' expressions are reachable
                        if case.expr.is_some() {
                            return Some(Ok(()));
                        }

                        // For branches with a nullable 'then' expression, try to determine
                        // if the 'then' expression is ever reachable in the situation where
                        // it would evaluate to null.
                        let bounds = match predicate_bounds::evaluate_bounds(
                            w,
                            Some(unwrap_certainly_null_expr(t)),
                            input_schema,
                        ) {
                            Err(e) => return Some(Err(e)),
                            Ok(b) => b,
                        };

                        let can_be_true = match bounds
                            .contains_value(ScalarValue::Boolean(Some(true)))
                        {
                            Err(e) => return Some(Err(e)),
                            Ok(b) => b,
                        };

                        if !can_be_true {
                            // If the derived 'when' expression can never evaluate to true, the
                            // 'then' expression is not reachable when it would evaluate to NULL.
                            // The most common pattern for this is `WHEN x IS NOT NULL THEN x`.
                            None
                        } else {
                            // The branch might be taken
                            Some(Ok(()))
                        }
                    })
                    .next();

                if let Some(nullable_then) = nullable_then {
                    // There is at least one reachable nullable 'then' expression, so the case
                    // expression itself is nullable.
                    // Use `Result::map` to propagate the error from `nullable_then` if there is one.
                    nullable_then.map(|_| true)
                } else if let Some(e) = &case.else_expr {
                    // There are no reachable nullable 'then' expressions, so all we still need to
                    // check is the 'else' expression's nullability.
                    e.nullable(input_schema)
                } else {
                    // CASE produces NULL if there is no `else` expr
                    // (aka when none of the `when_then_exprs` match)
                    Ok(true)
                }
            }
            Expr::Cast(Cast { expr, .. }) => expr.nullable(input_schema),
            Expr::ScalarFunction(_func) => {
                let field = self.to_field(input_schema)?.1;

                let nullable = field.is_nullable();
                Ok(nullable)
            }
            Expr::AggregateFunction(AggregateFunction { func, .. }) => {
                Ok(func.is_nullable())
            }
            Expr::WindowFunction(window_function) => self
                .data_type_and_nullable_with_window_function(
                    input_schema,
                    window_function,
                )
                .map(|(_, nullable)| nullable),
            Expr::Placeholder(Placeholder { id: _, field }) => {
                Ok(field.as_ref().map(|f| f.is_nullable()).unwrap_or(true))
            }
            Expr::ScalarVariable(_, _) | Expr::TryCast { .. } | Expr::Unnest(_) => {
                Ok(true)
            }
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => Ok(false),
            Expr::InSubquery(InSubquery { expr, .. }) => expr.nullable(input_schema),
            Expr::ScalarSubquery(subquery) => {
                Ok(subquery.subquery.schema().field(0).is_nullable())
            }
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ..
            }) => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            Expr::Like(Like { expr, pattern, .. })
            | Expr::SimilarTo(Like { expr, pattern, .. }) => {
                Ok(expr.nullable(input_schema)? || pattern.nullable(input_schema)?)
            }
            #[expect(deprecated)]
            Expr::Wildcard { .. } => Ok(false),
            Expr::GroupingSet(_) => {
                // Grouping sets do not really have the concept of nullable and do not appear
                // in projections
                Ok(true)
            }
        }
    }

    fn metadata(&self, schema: &dyn ExprSchema) -> Result<FieldMetadata> {
        self.to_field(schema)
            .map(|(_, field)| FieldMetadata::from(field.metadata()))
    }

    /// Returns the datatype and nullability of the expression based on [ExprSchema].
    ///
    /// Note: [`DFSchema`] implements [ExprSchema].
    ///
    /// [`DFSchema`]: datafusion_common::DFSchema
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// datatype or nullability.
    fn data_type_and_nullable(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(DataType, bool)> {
        let field = self.to_field(schema)?.1;

        Ok((field.data_type().clone(), field.is_nullable()))
    }

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
    /// - **Window functions**: field metadata is empty
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
    fn to_field(
        &self,
        schema: &dyn ExprSchema,
    ) -> Result<(Option<TableReference>, Arc<Field>)> {
        let (relation, schema_name) = self.qualified_name();
        #[expect(deprecated)]
        let field = match self {
            Expr::Alias(Alias {
                expr,
                name: _,
                metadata,
                ..
            }) => {
                let mut combined_metadata = expr.metadata(schema)?;
                if let Some(metadata) = metadata {
                    combined_metadata.extend(metadata.clone());
                }

                Ok(expr
                    .to_field(schema)
                    .map(|(_, f)| f)?
                    .with_field_metadata(&combined_metadata))
            }
            Expr::Negative(expr) => expr.to_field(schema).map(|(_, f)| f),
            Expr::Column(c) => schema.field_from_column(c).map(Arc::clone),
            Expr::OuterReferenceColumn(field, _) => {
                Ok(Arc::clone(field).renamed(&schema_name))
            }
            Expr::ScalarVariable(ty, _) => {
                Ok(Arc::new(Field::new(&schema_name, ty.clone(), true)))
            }
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
            Expr::BinaryExpr(BinaryExpr {
                ref left,
                ref right,
                ref op,
            }) => {
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
                let (dt, nullable) = self.data_type_and_nullable_with_window_function(
                    schema,
                    window_function,
                )?;
                Ok(Arc::new(Field::new(&schema_name, dt, nullable)))
            }
            Expr::AggregateFunction(aggregate_function) => {
                let AggregateFunction {
                    func,
                    params: AggregateFunctionParams { args, .. },
                    ..
                } = aggregate_function;

                let fields = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?;
                // Verify that function is invoked with correct number and type of arguments as defined in `TypeSignature`
                let new_fields = fields_with_aggregate_udf(&fields, func)
                    .map_err(|err| {
                        let arg_types = fields
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
                            utils::generate_signature_error_msg(
                                func.name(),
                                func.signature().clone(),
                                &arg_types,
                            )
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();

                func.return_field(&new_fields)
            }
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                let (arg_types, fields): (Vec<DataType>, Vec<Arc<Field>>) = args
                    .iter()
                    .map(|e| e.to_field(schema).map(|(_, f)| f))
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .map(|f| (f.data_type().clone(), f))
                    .unzip();
                // Verify that function is invoked with correct number and type of arguments as defined in `TypeSignature`
                let new_data_types = data_types_with_scalar_udf(&arg_types, func)
                    .map_err(|err| {
                        plan_datafusion_err!(
                            "{} {}",
                            match err {
                                DataFusionError::Plan(msg) => msg,
                                err => err.to_string(),
                            },
                            utils::generate_signature_error_msg(
                                func.name(),
                                func.signature().clone(),
                                &arg_types,
                            )
                        )
                    })?;
                let new_fields = fields
                    .into_iter()
                    .zip(new_data_types)
                    .map(|(f, d)| f.retyped(d))
                    .collect::<Vec<FieldRef>>();

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
            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::TryCast(_)
            | Expr::InList(_)
            | Expr::InSubquery(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::Unnest(_) => Ok(Arc::new(Field::new(
                &schema_name,
                self.get_type(schema)?,
                self.nullable(schema)?,
            ))),
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
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }

        // TODO(kszucs): Most of the operations do not validate the type correctness
        // like all of the binary expressions below. Perhaps Expr should track the
        // type of the expression?

        if can_cast_types(&this_type, cast_to_type) {
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

/// Returns the innermost [Expr] that is provably null if `expr` is null.
fn unwrap_certainly_null_expr(expr: &Expr) -> &Expr {
    match expr {
        Expr::Not(e) => unwrap_certainly_null_expr(e),
        Expr::Negative(e) => unwrap_certainly_null_expr(e),
        Expr::Cast(e) => unwrap_certainly_null_expr(e.expr.as_ref()),
        _ => expr,
    }
}

impl Expr {
    /// Common method for window functions that applies type coercion
    /// to all arguments of the window function to check if it matches
    /// its signature.
    ///
    /// If successful, this method returns the data type and
    /// nullability of the window function's result.
    ///
    /// Otherwise, returns an error if there's a type mismatch between
    /// the window function's signature and the provided arguments.
    fn data_type_and_nullable_with_window_function(
        &self,
        schema: &dyn ExprSchema,
        window_function: &WindowFunction,
    ) -> Result<(DataType, bool)> {
        let WindowFunction {
            fun,
            params: WindowFunctionParams { args, .. },
            ..
        } = window_function;

        let fields = args
            .iter()
            .map(|e| e.to_field(schema).map(|(_, f)| f))
            .collect::<Result<Vec<_>>>()?;
        match fun {
            WindowFunctionDefinition::AggregateUDF(udaf) => {
                let data_types = fields
                    .iter()
                    .map(|f| f.data_type())
                    .cloned()
                    .collect::<Vec<_>>();
                let new_fields = fields_with_aggregate_udf(&fields, udaf)
                    .map_err(|err| {
                        plan_datafusion_err!(
                            "{} {}",
                            match err {
                                DataFusionError::Plan(msg) => msg,
                                err => err.to_string(),
                            },
                            utils::generate_signature_error_msg(
                                fun.name(),
                                fun.signature(),
                                &data_types
                            )
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();

                let return_field = udaf.return_field(&new_fields)?;

                Ok((return_field.data_type().clone(), return_field.is_nullable()))
            }
            WindowFunctionDefinition::WindowUDF(udwf) => {
                let data_types = fields
                    .iter()
                    .map(|f| f.data_type())
                    .cloned()
                    .collect::<Vec<_>>();
                let new_fields = fields_with_window_udf(&fields, udwf)
                    .map_err(|err| {
                        plan_datafusion_err!(
                            "{} {}",
                            match err {
                                DataFusionError::Plan(msg) => msg,
                                err => err.to_string(),
                            },
                            utils::generate_signature_error_msg(
                                fun.name(),
                                fun.signature(),
                                &data_types
                            )
                        )
                    })?
                    .into_iter()
                    .collect::<Vec<_>>();
                let (_, function_name) = self.qualified_name();
                let field_args = WindowUDFFieldArgs::new(&new_fields, &function_name);

                udwf.field(field_args)
                    .map(|field| (field.data_type().clone(), field.is_nullable()))
            }
        }
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

    use datafusion_common::{assert_or_internal_err, DFSchema, ScalarValue};

    macro_rules! test_is_expr_nullable {
        ($EXPR_TYPE:ident) => {{
            let expr = lit(ScalarValue::Null).$EXPR_TYPE();
            assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        }};
    }

    #[test]
    fn expr_schema_nullability() {
        let expr = col("foo").eq(lit(1));
        assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        assert!(expr
            .nullable(&MockExprSchema::new().with_nullable(true))
            .unwrap());

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
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());

        let null = lit(ScalarValue::Int32(None));

        let expr = col("foo").between(null.clone(), lit(2));
        assert!(expr.nullable(&get_schema(false)).unwrap());

        let expr = col("foo").between(lit(1), null.clone());
        assert!(expr.nullable(&get_schema(false)).unwrap());

        let expr = col("foo").between(null.clone(), null);
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    fn assert_nullability(expr: &Expr, schema: &dyn ExprSchema, expected: bool) {
        assert_eq!(
            expr.nullable(schema).unwrap(),
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
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());
        // Testing nullable() returns an error.
        assert!(expr
            .nullable(&get_schema(false).with_error_on_nullable(true))
            .is_err());

        let null = lit(ScalarValue::Int32(None));
        let expr = col("foo").in_list(vec![null, lit(1)], false);
        assert!(expr.nullable(&get_schema(false)).unwrap());

        // Testing on long list
        let expr = col("foo").in_list(vec![lit(1); 6], false);
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    #[test]
    fn test_like_nullability() {
        let get_schema = |nullable| {
            MockExprSchema::new()
                .with_data_type(DataType::Utf8)
                .with_nullable(nullable)
        };

        let expr = col("foo").like(lit("bar"));
        assert!(!expr.nullable(&get_schema(false)).unwrap());
        assert!(expr.nullable(&get_schema(true)).unwrap());

        let expr = col("foo").like(lit(ScalarValue::Utf8(None)));
        assert!(expr.nullable(&get_schema(false)).unwrap());
    }

    #[test]
    fn expr_schema_data_type() {
        let expr = col("foo");
        assert_eq!(
            DataType::Utf8,
            expr.get_type(&MockExprSchema::new().with_data_type(DataType::Utf8))
                .unwrap()
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
        assert_eq!(meta, expr.metadata(&schema).unwrap());
        assert_eq!(meta, expr.clone().alias("bar").metadata(&schema).unwrap());
        assert_eq!(
            meta,
            expr.clone()
                .cast_to(&DataType::Int64, &schema)
                .unwrap()
                .metadata(&schema)
                .unwrap()
        );

        let schema = DFSchema::from_unqualified_fields(
            vec![meta.add_to_field(Field::new("foo", DataType::Int32, true))].into(),
            HashMap::new(),
        )
        .unwrap();

        // verify to_field method populates metadata
        assert_eq!(meta, expr.metadata(&schema).unwrap());

        // outer ref constructed by `out_ref_col_with_metadata` should be metadata-preserving
        let outer_ref = out_ref_col_with_metadata(
            DataType::Int32,
            meta.to_hashmap(),
            Column::from_name("foo"),
        );
        assert_eq!(meta, outer_ref.metadata(&schema).unwrap());
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
        assert_eq!(placeholder_meta, expr.metadata(&schema).unwrap());

        let expr_alias = expr.alias("a placeholder by any other name");
        let expr_alias_field = expr_alias.to_field(&schema).unwrap().1;
        assert_eq!(
            (expr_alias_field.data_type(), expr_alias_field.is_nullable()),
            (&DataType::Utf8, true)
        );
        assert_eq!(placeholder_meta, expr_alias.metadata(&schema).unwrap());

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
        fn nullable(&self, _col: &Column) -> Result<bool> {
            assert_or_internal_err!(!self.error_on_nullable, "nullable error");
            Ok(self.field.is_nullable())
        }

        fn field_from_column(&self, _col: &Column) -> Result<&FieldRef> {
            Ok(&self.field)
        }
    }
}
