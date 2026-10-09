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

use super::{Between, Expr, Like, predicate_bounds};
use crate::ValueOrLambda;
use crate::expr::{
    AggregateFunction, AggregateFunctionParams, Alias, BinaryExpr, Case, Cast, InList,
    InSubquery, Lambda, Placeholder, ScalarFunction, TryCast, Unnest, WindowFunction,
    WindowFunctionParams, in_subquery_tuple_values,
};
use crate::expr::{FieldMetadata, LambdaVariable};
use crate::higher_order_function::HigherOrderReturnFieldArgs;
use crate::type_coercion::functions::value_fields_with_higher_order_udf_and_lambdas;
use crate::type_coercion::functions::{UDFCoercionExt, fields_with_udf};
use crate::udf::ReturnFieldArgs;
use crate::{
    LogicalPlan, Operator, Projection, Subquery, WindowFunctionDefinition, utils,
};
use arrow::compute::can_cast_types;
use arrow::datatypes::FieldRef;
use arrow::datatypes::{DataType, Field};
use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};
use datafusion_common::datatype::FieldExt;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{
    Column, DataFusionError, ExprSchema, Result, ScalarValue, Spans, TableReference,
    internal_err, not_impl_err, plan_datafusion_err, plan_err,
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

/// Derives the output field for a cast expression from the source and target fields.
///
/// Metadata handling:
/// - Type-only casts (i.e., target_field == DataType::SomeDataType.into_nullable_field())
///   propagate non extension-type metadata from the source. This is for backward compatibility
///   (casts have propagated source metadata for many if not all previous versions), recognizing
///   that the return type of `<some extension type>::<some non extension type>` should have the
///   return type of `<some non extension type>` (e.g., casting arrow.json to utf8).
/// - All other casts preserve target metadata exactly. This ensures in particular that output
///   metadata when casting to an extension type contains the extension information in the
///   output field. Callers that wish to have some mix of source and target metadata can use
///   Alias or construct an output field themselves (whose metadata will be used directly).
///
/// For `TryCast`, `force_nullable` is `true` since a failed cast returns NULL.
fn cast_output_field(
    source_field: &FieldRef,
    target_field: &FieldRef,
    force_nullable: bool,
) -> Arc<Field> {
    // Check if this is a "type-only" cast (target_field == DataType::X.into_nullable_field())
    let is_type_only = target_field.name().is_empty()
        && target_field.is_nullable()
        && target_field.metadata().is_empty();

    let metadata = if is_type_only {
        // Type-only cast: propagate source metadata, stripping extension type keys
        let mut meta = source_field.metadata().clone();
        meta.remove(EXTENSION_TYPE_NAME_KEY);
        meta.remove(EXTENSION_TYPE_METADATA_KEY);
        meta
    } else {
        // Explicit target field: use target metadata exactly
        target_field.metadata().clone()
    };

    let mut f = source_field
        .as_ref()
        .clone()
        .with_data_type(target_field.data_type().clone())
        .with_metadata(metadata);
    if force_nullable {
        f = f.with_nullable(true);
    }
    Arc::new(f)
}

fn scalar_arguments_for_fields(
    args: &[Expr],
    arg_fields: &[FieldRef],
) -> Vec<Option<ScalarValue>> {
    args.iter()
        .zip(arg_fields)
        .map(|(expr, field)| scalar_argument_for_field(expr, field))
        .collect()
}

fn scalar_argument_for_field(expr: &Expr, arg_field: &FieldRef) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(sv, _) => Some(
            sv.cast_to(arg_field.data_type())
                .unwrap_or_else(|_| sv.clone()),
        ),
        _ => None,
    }
}

// Resolve metadata bottom-up for branches whose metadata can be preserved.
fn case_field_metadata(case: &Case, schema: &dyn ExprSchema) -> Result<FieldRef> {
    // `to_field` can revisit nested aliases; delegate only shallow branches.
    const MAX_EXACT_BRANCH_DEPTH: usize = 8;

    fn can_infer_exact_branch(expr: &Expr, allow_nested_case: bool) -> Result<bool> {
        let mut work = vec![(expr, 1)];
        while let Some((node, depth)) = work.pop() {
            if (!allow_nested_case && matches!(node, Expr::Case(_)))
                || depth > MAX_EXACT_BRANCH_DEPTH
            {
                return Ok(false);
            }
            node.apply_children(|child| {
                work.push((child, depth + 1));
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(true)
    }

    enum Work<'a> {
        Visit(&'a Expr),
        FinishCase(&'a Case),
        FinishCast(&'a FieldRef, bool),
        FinishAlias(Option<&'a FieldMetadata>),
    }

    struct BranchField {
        field: FieldRef,
        certainly_null: bool,
    }

    fn schedule_case<'a>(case: &'a Case, work: &mut Vec<Work<'a>>) {
        work.push(Work::FinishCase(case));
        work.extend(case.else_expr.iter().map(|expr| Work::Visit(expr)));
        work.extend(
            case.when_then_expr
                .iter()
                .rev()
                .map(|(_, then_expr)| Work::Visit(then_expr)),
        );
    }

    let mut work = Vec::new();
    let mut fields: Vec<BranchField> = Vec::new();
    schedule_case(case, &mut work);
    while let Some(item) = work.pop() {
        match item {
            Work::Visit(expr) => match expr {
                Expr::Case(nested) => schedule_case(nested, &mut work),
                Expr::Cast(cast)
                    if !cast.field.metadata().is_empty()
                        && can_infer_exact_branch(expr, false)? =>
                {
                    fields.push(BranchField {
                        field: expr.to_field(schema)?.1,
                        certainly_null: false,
                    });
                }
                Expr::Cast(cast) => {
                    work.push(Work::FinishCast(&cast.field, false));
                    work.push(Work::Visit(&cast.expr));
                }
                Expr::TryCast(cast)
                    if !cast.field.metadata().is_empty()
                        && can_infer_exact_branch(expr, false)? =>
                {
                    fields.push(BranchField {
                        field: expr.to_field(schema)?.1,
                        certainly_null: false,
                    });
                }
                Expr::TryCast(cast) => {
                    work.push(Work::FinishCast(&cast.field, true));
                    work.push(Work::Visit(&cast.expr));
                }
                Expr::Alias(alias)
                    if alias.metadata.is_some()
                        && can_infer_exact_branch(expr, false)? =>
                {
                    fields.push(BranchField {
                        field: expr.to_field(schema)?.1,
                        certainly_null: false,
                    });
                }
                Expr::Alias(alias) => {
                    work.push(Work::FinishAlias(alias.metadata.as_ref()));
                    work.push(Work::Visit(&alias.expr));
                }
                Expr::Negative(inner) => work.push(Work::Visit(inner)),
                Expr::ScalarFunction(_) | Expr::HigherOrderFunction(_)
                    if can_infer_exact_branch(expr, true)? =>
                {
                    fields.push(BranchField {
                        field: expr.to_field(schema)?.1,
                        certainly_null: false,
                    });
                }
                Expr::Column(_)
                | Expr::Literal(_, _)
                | Expr::OuterReferenceColumn(_, _)
                | Expr::ScalarVariable(_, _)
                | Expr::Placeholder(_)
                | Expr::LambdaVariable(_) => fields.push(BranchField {
                    field: expr.to_field(schema)?.1,
                    certainly_null: matches!(
                        unwrap_certainly_null_expr(expr),
                        Expr::Literal(value, _) if value.is_null()
                    ),
                }),
                _ => return Ok(Arc::new(Field::new("", DataType::Null, true))),
            },
            Work::FinishCast(target, force_nullable) => {
                let Some(source) = fields.pop() else {
                    return internal_err!("Missing CASE cast input field");
                };
                fields.push(BranchField {
                    field: cast_output_field(&source.field, target, force_nullable),
                    certainly_null: source.certainly_null,
                });
            }
            Work::FinishAlias(metadata) => {
                let Some(source) = fields.pop() else {
                    return internal_err!("Missing CASE alias input field");
                };
                let mut combined = source.field.metadata().clone();
                if let Some(metadata) = metadata {
                    combined.extend(metadata.to_hashmap());
                }
                fields.push(BranchField {
                    field: Arc::new(
                        source.field.as_ref().clone().with_metadata(combined),
                    ),
                    certainly_null: source.certainly_null,
                });
            }
            Work::FinishCase(case) => {
                let count =
                    case.when_then_expr.len() + usize::from(case.else_expr.is_some());
                if fields.len() < count {
                    return internal_err!("Missing CASE result fields");
                }
                let start = fields.len() - count;
                let mut then_type = DataType::Null;
                let mut else_type = DataType::Null;
                let mut branch_type = None;
                let mut metadata = None;
                let mut conflict = false;
                let mut certainly_null = true;
                for (index, branch) in fields.drain(start..).enumerate() {
                    let data_type = branch.field.data_type();
                    if index < case.when_then_expr.len() {
                        if then_type.is_null() && !data_type.is_null() {
                            then_type = data_type.clone();
                        }
                    } else {
                        else_type = data_type.clone();
                    }
                    certainly_null &= branch.certainly_null;
                    if data_type.is_null()
                        || (branch.certainly_null && branch.field.metadata().is_empty())
                    {
                        continue;
                    }
                    if branch_type.as_ref().is_some_and(|other| other != data_type)
                        || branch.field.metadata().is_empty()
                        || metadata
                            .as_ref()
                            .is_some_and(|other| other != branch.field.metadata())
                    {
                        conflict = true;
                    }
                    branch_type.get_or_insert_with(|| data_type.clone());
                    metadata.get_or_insert_with(|| branch.field.metadata().clone());
                }
                let data_type = if then_type.is_null() {
                    else_type
                } else {
                    then_type
                };
                if conflict || branch_type.as_ref() != Some(&data_type) {
                    metadata = None;
                }
                fields.push(BranchField {
                    field: Arc::new(
                        Field::new("", data_type, true)
                            .with_metadata(metadata.unwrap_or_default()),
                    ),
                    certainly_null,
                });
            }
        }
    }
    let Some(result) = fields.pop() else {
        return internal_err!("Missing CASE output field");
    };
    Ok(result.field)
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
            Expr::Alias(Alias { expr, .. }) | Expr::Negative(expr) => {
                expr.get_type(schema)
            }
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::OuterReferenceColumn(field, _) => Ok(field.data_type().clone()),
            Expr::ScalarVariable(field, _) => Ok(field.data_type().clone()),
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
            Expr::Cast(Cast { field, .. }) | Expr::TryCast(TryCast { field, .. }) => {
                Ok(field.data_type().clone())
            }
            Expr::Unnest(Unnest { expr, .. }) => {
                let arg_data_type = expr.get_type(schema)?;
                // Unnest's output type is the inner type of the list
                match arg_data_type {
                    DataType::List(field)
                    | DataType::LargeList(field)
                    | DataType::FixedSizeList(field, _)
                    | DataType::ListView(field)
                    | DataType::LargeListView(field) => Ok(field.data_type().clone()),
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
            Expr::ScalarFunction(_)
            | Expr::WindowFunction(_)
            | Expr::AggregateFunction(_) => {
                Ok(self.to_field(schema)?.1.data_type().clone())
            }
            Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::Exists { .. }
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
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
            Expr::BinaryExpr(BinaryExpr { left, right, op }) => BinaryTypeCoercer::new(
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
            Expr::HigherOrderFunction(_func) => {
                Ok(self.to_field(schema)?.1.data_type().clone())
            }
            Expr::Lambda(_lambda) => Ok(DataType::Null),
            Expr::LambdaVariable(LambdaVariable { field, .. }) => match field {
                Some(f) => Ok(f.data_type().clone()),
                // If the lambda variable's field hasn't been specified, treat it as
                // null (unspecified lambda variables generate an error during planning)
                None => Ok(DataType::Null),
            },
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
                let nullable_then = case.when_then_expr.iter().find_map(|(w, t)| {
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

                    let can_be_true =
                        match bounds.contains_value(ScalarValue::Boolean(Some(true))) {
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
                });

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
            Expr::ScalarFunction(_)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_) => Ok(self.to_field(input_schema)?.1.is_nullable()),
            Expr::ScalarVariable(field, _) => Ok(field.is_nullable()),
            Expr::TryCast { .. } | Expr::Unnest(_) | Expr::Placeholder(_) => Ok(true),
            Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Exists { .. } => Ok(false),
            Expr::SetComparison(_) => Ok(true),
            Expr::InSubquery(InSubquery { expr, subquery, .. }) => {
                // A multi-column `(a, b) IN (SELECT x, y ...)` is UNKNOWN when
                // any element or subquery column is NULL. The tuple is a
                // `struct` call, which itself is never NULL, so its elements
                // and every subquery column count.
                if let Some(values) = in_subquery_tuple_values(expr, &subquery.subquery)?
                {
                    let mut nullable = subquery
                        .subquery
                        .schema()
                        .fields()
                        .iter()
                        .any(|field| field.is_nullable());
                    for value in values.iter() {
                        nullable |= value.nullable(input_schema)?;
                    }
                    return Ok(nullable);
                }
                let expr_nullable = expr.nullable(input_schema)?;
                let subquery_nullable = subquery.subquery.schema().fields().first().ok_or_else(|| {
                    plan_datafusion_err!("subquery must return exactly one column of data to compare against")
                })?.is_nullable();

                Ok(expr_nullable | subquery_nullable)
            }
            Expr::ScalarSubquery(subquery) => Ok(scalar_subquery_nullable(subquery)),
            Expr::BinaryExpr(BinaryExpr { left, right, op }) => match op {
                Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => Ok(false),
                _ => Ok(left.nullable(input_schema)? || right.nullable(input_schema)?),
            },
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
            Expr::HigherOrderFunction(_func) => {
                Ok(self.to_field(input_schema)?.1.is_nullable())
            }
            Expr::Lambda(_lambda) => Ok(true),
            Expr::LambdaVariable(LambdaVariable { field, .. }) => match field {
                Some(f) => Ok(f.is_nullable()),
                // If the lambda variable's field hasn't been specified, treat it as
                // null (unspecified lambda variables generate an error during planning)
                None => Ok(true),
            },
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
    /// - **Cast expressions**: Type-only casts pass through source metadata (stripping extension
    ///   type keys); casts with explicit target fields use target metadata exactly
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
                let field = subquery.subquery.schema().field(0);
                Ok(Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_nullable(scalar_subquery_nullable(subquery)),
                ))
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
                let nullable = match op {
                    Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => false,
                    _ => lhs_nullable || rhs_nullable,
                };
                Ok(Arc::new(Field::new(
                    &schema_name,
                    coercer.get_result_type()?,
                    nullable,
                )))
            }
            Expr::WindowFunction(window_function) => {
                let WindowFunction {
                    fun,
                    params: WindowFunctionParams { args, .. },
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

                let arguments = scalar_arguments_for_fields(args, &new_fields);
                let argument_refs =
                    arguments.iter().map(Option::as_ref).collect::<Vec<_>>();
                let args = ReturnFieldArgs {
                    arg_fields: &new_fields,
                    scalar_arguments: &argument_refs,
                };

                func.return_field_from_args(args)
            }
            // _ => Ok((self.get_type(schema)?, self.nullable(schema)?)),
            Expr::Cast(Cast { expr, field }) => expr
                .to_field(schema)
                .map(|(_table_ref, src)| cast_output_field(&src, field, false)),
            Expr::Placeholder(Placeholder {
                id: _,
                field: Some(field),
            }) => Ok(Arc::clone(field).renamed(&schema_name)),
            Expr::TryCast(TryCast { expr, field }) => expr
                .to_field(schema)
                .map(|(_table_ref, src)| cast_output_field(&src, field, true)),
            Expr::LambdaVariable(LambdaVariable {
                field: Some(field), ..
            }) => Ok(Arc::clone(field).renamed(&schema_name)),
            Expr::Case(case) => {
                let data_type = self.get_type(schema)?;
                let nullable = self.nullable(schema)?;
                if let Some((_, first_result)) = case.when_then_expr.first()
                    && matches!(
                        first_result.as_ref(),
                        Expr::Column(_) | Expr::Literal(_, _)
                    )
                {
                    let field = first_result.to_field(schema)?.1;
                    if !field.data_type().is_null()
                        && !matches!(first_result.as_ref(), Expr::Literal(value, _) if value.is_null())
                        && field.metadata().is_empty()
                    {
                        return Ok((
                            relation,
                            Arc::new(Field::new(&schema_name, data_type, nullable)),
                        ));
                    }
                }
                let branch_field = case_field_metadata(case, schema)?;
                let metadata = if branch_field.data_type() == &data_type {
                    branch_field.metadata().clone()
                } else {
                    Default::default()
                };
                Ok(Arc::new(
                    Field::new(&schema_name, data_type, nullable).with_metadata(metadata),
                ))
            }
            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::Between(_)
            | Expr::InList(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_)
            | Expr::Wildcard { .. }
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::Unnest(_)
            | Expr::Lambda(_)
            | Expr::LambdaVariable(_) => Ok(Arc::new(Field::new(
                &schema_name,
                self.get_type(schema)?,
                self.nullable(schema)?,
            ))),
            Expr::HigherOrderFunction(func) => {
                let arg_fields = func
                    .args
                    .iter()
                    .map(|arg| match arg {
                        Expr::Lambda(Lambda { params: _, body }) => {
                            // use the name of the lambda instead of just the body to help with debugging
                            Ok(ValueOrLambda::Lambda(Arc::new(Field::new(
                                arg.qualified_name().1,
                                body.get_type(schema)?,
                                body.nullable(schema)?,
                            ))))
                        }
                        _ => Ok(ValueOrLambda::Value(arg.to_field(schema)?.1)),
                    })
                    .collect::<Result<Vec<_>>>()?;

                let new_fields = value_fields_with_higher_order_udf_and_lambdas(
                    &arg_fields,
                    func.func.as_ref(),
                )?;

                let arguments = func
                    .args
                    .iter()
                    .map(|e| match e {
                        Expr::Literal(sv, _) => Some(sv),
                        _ => None,
                    })
                    .collect::<Vec<_>>();

                let args = HigherOrderReturnFieldArgs {
                    arg_fields: &new_fields,
                    scalar_arguments: &arguments,
                };

                func.func.return_field_from_args(args)
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
        let this_type = self.get_type(schema)?;
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
            "{}. {}",
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

/// Returns whether a scalar subquery may evaluate to NULL.
///
/// This is the case if the subquery's projected field is nullable, or if the
/// subquery may return no rows: a scalar subquery that produces no rows
/// evaluates to NULL regardless of the nullability of its projected field.
fn scalar_subquery_nullable(subquery: &Subquery) -> bool {
    subquery.subquery.schema().field(0).is_nullable() || subquery.subquery.min_rows() == 0
}

/// Cast subquery in InSubquery/ScalarSubquery to a given type.
///
/// 1. **Projection plan**: If the subquery is a projection (i.e. a SELECT statement with specific
///    columns), it casts the first expression in the projection to the target type and creates a
///    new projection with the casted expression.
/// 2. **Non-projection plan**: If the subquery isn't a projection, it adds a projection to the plan
///    with the casted first column.
pub fn cast_subquery(subquery: Subquery, cast_to_type: &DataType) -> Result<Subquery> {
    cast_subquery_columns(subquery, std::slice::from_ref(cast_to_type))
}

/// Cast the leading columns of a subquery to the given types, one per column,
/// like [`cast_subquery`] does for its first column. The result keeps only
/// the `cast_to_types.len()` leading columns.
///
/// Used by a multi-column `(a, b) IN (SELECT x, y ...)`, whose tuple elements
/// are coerced column by column.
pub fn cast_subquery_columns(
    subquery: Subquery,
    cast_to_types: &[DataType],
) -> Result<Subquery> {
    let schema = subquery.subquery.schema();
    if schema
        .fields()
        .iter()
        .zip(cast_to_types)
        .all(|(field, cast_to_type)| field.data_type() == cast_to_type)
    {
        return Ok(subquery);
    }

    let plan = subquery.subquery.as_ref();
    let new_plan = match plan {
        LogicalPlan::Projection(projection) => {
            let cast_exprs = projection
                .expr
                .iter()
                .zip(cast_to_types)
                .map(|(expr, cast_to_type)| {
                    expr.clone()
                        .cast_to(cast_to_type, projection.input.schema())
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalPlan::Projection(Projection::try_new(
                cast_exprs,
                Arc::clone(&projection.input),
            )?)
        }
        _ => {
            let cast_exprs = cast_to_types
                .iter()
                .enumerate()
                .map(|(i, cast_to_type)| {
                    Expr::Column(Column::from(plan.schema().qualified_field(i)))
                        .cast_to(cast_to_type, subquery.subquery.schema())
                })
                .collect::<Result<Vec<_>>>()?;
            LogicalPlan::Projection(Projection::try_new(
                cast_exprs,
                Arc::clone(&subquery.subquery),
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
    use crate::logical_plan::builder::LogicalTableSource;
    use crate::test::function_stub::count;
    use crate::{
        LogicalPlanBuilder, and, col, in_subquery, lit, not, or,
        out_ref_col_with_metadata, scalar_subquery, when,
    };

    use arrow::datatypes::Schema;
    use datafusion_common::{DFSchema, assert_or_internal_err};

    macro_rules! test_is_expr_nullable {
        ($EXPR_TYPE:ident) => {{
            let expr = lit(ScalarValue::Null).$EXPR_TYPE();
            assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        }};
    }

    #[test]
    fn scalar_arguments_match_coerced_fields() {
        let int16_field: FieldRef = Field::new("arg", DataType::Int16, true).into();

        assert_eq!(
            scalar_argument_for_field(&lit(1_i64), &int16_field),
            Some(ScalarValue::Int16(Some(1)))
        );
        assert_eq!(
            scalar_argument_for_field(&lit(ScalarValue::Null), &int16_field),
            Some(ScalarValue::Int16(None))
        );

        let int32_list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int32(Some(1))],
            &DataType::Int32,
            true,
        ));
        let int64_list_type = DataType::new_list(DataType::Int64, true);
        let int64_list_field: FieldRef =
            Field::new("arg", int64_list_type.clone(), true).into();
        assert_eq!(
            scalar_argument_for_field(&lit(int32_list.clone()), &int64_list_field),
            Some(int32_list.cast_to(&int64_list_type).unwrap())
        );
    }

    #[test]
    fn scalar_arguments_exclude_expression_casts_and_preserve_invalid_values() {
        let int16_field: FieldRef = Field::new("arg", DataType::Int16, true).into();
        let int32_field: FieldRef = Field::new("arg", DataType::Int32, true).into();
        let explicit_cast = Expr::Cast(Cast::new(Box::new(lit(1_i64)), DataType::Int16));
        let explicit_try_cast =
            Expr::TryCast(TryCast::new(Box::new(lit(1_i64)), DataType::Int16));
        let out_of_i32_range = i64::from(i32::MAX) + 1;

        assert_eq!(
            scalar_argument_for_field(&explicit_cast, &int16_field),
            None
        );
        assert_eq!(
            scalar_argument_for_field(&explicit_try_cast, &int16_field),
            None
        );
        assert_eq!(
            scalar_argument_for_field(&lit("not an integer"), &int16_field),
            Some(ScalarValue::Utf8(Some("not an integer".to_string())))
        );
        assert_eq!(
            scalar_argument_for_field(&lit(out_of_i32_range), &int32_field),
            Some(ScalarValue::Int64(Some(out_of_i32_range)))
        );
    }

    #[test]
    fn expr_schema_nullability() {
        let cases = [
            (Operator::Eq, false, false),
            (Operator::Eq, true, true),
            (Operator::IsDistinctFrom, true, false),
            (Operator::IsNotDistinctFrom, true, false),
        ];

        for (op, input_nullable, expected) in cases {
            let expr = Expr::BinaryExpr(BinaryExpr::new(
                Box::new(col("foo")),
                op,
                Box::new(col("bar")),
            ));
            let schema = MockExprSchema::new()
                .with_data_type(DataType::Boolean)
                .with_nullable(input_nullable);

            assert_eq!(expr.nullable(&schema).unwrap(), expected, "{op}");
            assert_eq!(
                expr.to_field(&schema).unwrap().1.is_nullable(),
                expected,
                "{op}"
            );
        }

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

        let varchar_schema = MockExprSchema::new()
            .with_data_type(DataType::Utf8)
            .with_nullable(true);
        let try_cast = Expr::TryCast(TryCast::new(Box::new(col("x")), DataType::Int32));
        let e = when(col("x").is_not_null(), try_cast).otherwise(lit(0))?;
        assert_nullable(&e, &varchar_schema);

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
        assert!(
            expr.nullable(&get_schema(false).with_error_on_nullable(true))
                .is_err()
        );

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
    fn test_case_field_metadata() -> Result<()> {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct MarkedIdentity {
            signature: crate::Signature,
        }

        impl crate::ScalarUDFImpl for MarkedIdentity {
            fn name(&self) -> &str {
                "marked_identity"
            }

            fn signature(&self) -> &crate::Signature {
                &self.signature
            }

            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Int32)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
                let input = &args.arg_fields[0];
                Ok(Arc::new(
                    Field::new("marked_identity", DataType::Int32, input.is_nullable())
                        .with_metadata(input.metadata().clone()),
                ))
            }

            fn invoke_with_args(
                &self,
                _args: crate::ScalarFunctionArgs,
            ) -> Result<crate::ColumnarValue> {
                Ok(crate::ColumnarValue::Scalar(ScalarValue::Int32(Some(0))))
            }
        }

        let shared = HashMap::from([("type".to_string(), "structured".to_string())]);
        let different = HashMap::from([("type".to_string(), "other".to_string())]);
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("a", DataType::Int32, false).with_metadata(shared.clone()),
                Field::new("b", DataType::Int32, false).with_metadata(shared.clone()),
                Field::new("c", DataType::Int32, false).with_metadata(different),
                Field::new("d", DataType::Int32, false),
                Field::new("e", DataType::Boolean, false).with_metadata(shared.clone()),
                Field::new("f", DataType::Boolean, false).with_metadata(shared.clone()),
            ]
            .into(),
            HashMap::new(),
        )?;

        let same = when(lit(true), col("a")).otherwise(col("b"))?;
        assert_eq!(same.to_field(&schema)?.1.metadata(), &shared);

        let no_else = when(lit(true), col("a")).end()?;
        assert_eq!(no_else.to_field(&schema)?.1.metadata(), &shared);

        let multiple_when = when(lit(false), col("a"))
            .when(lit(true), col("b"))
            .otherwise(lit(ScalarValue::Null))?;
        assert_eq!(multiple_when.to_field(&schema)?.1.metadata(), &shared);

        let conflicting_when = when(lit(false), col("a"))
            .when(lit(true), col("c"))
            .otherwise(col("b"))?;
        assert!(conflicting_when.to_field(&schema)?.1.metadata().is_empty());

        let marked_literal = Expr::Literal(
            ScalarValue::Int32(Some(1)),
            Some(FieldMetadata::from(shared.clone())),
        );
        let literal_case = when(lit(true), marked_literal).otherwise(col("b"))?;
        assert_eq!(literal_case.to_field(&schema)?.1.metadata(), &shared);

        let unmarked_literal = when(lit(true), col("a")).otherwise(lit(1_i32))?;
        assert!(unmarked_literal.to_field(&schema)?.1.metadata().is_empty());

        let negative_case = when(lit(true), Expr::Negative(Box::new(col("a"))))
            .otherwise(Expr::Negative(Box::new(col("b"))))?;
        assert_eq!(negative_case.to_field(&schema)?.1.metadata(), &shared);

        let unsupported_case =
            when(lit(true), Expr::Not(Box::new(col("e")))).otherwise(col("f"))?;
        assert!(unsupported_case.to_field(&schema)?.1.metadata().is_empty());

        let null_else = when(lit(true), col("a")).otherwise(lit(ScalarValue::Null))?;
        assert_eq!(null_else.to_field(&schema)?.1.metadata(), &shared);

        let coerced_null_else = when(lit(true), col("a"))
            .otherwise(lit(ScalarValue::Null).cast_to(&DataType::Int32, &schema)?)?;
        assert_eq!(coerced_null_else.to_field(&schema)?.1.metadata(), &shared);

        let try_cast_null_else = when(lit(true), col("a")).otherwise(Expr::TryCast(
            TryCast::new(Box::new(lit(ScalarValue::Null)), DataType::Int32),
        ))?;
        assert_eq!(try_cast_null_else.to_field(&schema)?.1.metadata(), &shared);

        let typed_null = Expr::Cast(Cast::new_from_field(
            Box::new(lit(ScalarValue::Null)),
            Arc::new(Field::new("", DataType::Int32, true).with_metadata(shared.clone())),
        ));
        let all_typed_null = when(lit(true), typed_null.clone()).otherwise(typed_null)?;
        assert_eq!(all_typed_null.to_field(&schema)?.1.metadata(), &shared);

        let binary_metadata = HashMap::from([(
            EXTENSION_TYPE_NAME_KEY.to_string(),
            "geoarrow.wkb".to_string(),
        )]);
        let binary_schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("binary", DataType::LargeBinary, false)
                    .with_metadata(binary_metadata.clone()),
            ]
            .into(),
            HashMap::new(),
        )?;
        let target_field: FieldRef = Arc::new(
            Field::new("", DataType::Binary, true).with_metadata(binary_metadata),
        );
        for marked_null in [
            Expr::Cast(Cast::new_from_field(
                Box::new(lit(ScalarValue::Null)),
                Arc::clone(&target_field),
            )),
            Expr::TryCast(TryCast::new_from_field(
                Box::new(lit(ScalarValue::Null)),
                Arc::clone(&target_field),
            )),
        ] {
            let nested_all_null =
                when(lit(true), marked_null).otherwise(lit(ScalarValue::Null))?;
            let type_only_cast =
                Expr::Cast(Cast::new(Box::new(nested_all_null), DataType::LargeBinary));
            let outer = when(lit(true), type_only_cast).otherwise(col("binary"))?;
            assert!(outer.to_field(&binary_schema)?.1.metadata().is_empty());
        }

        let mut nested = col("a");
        for _ in 0..128 {
            nested = when(lit(true), col("a")).otherwise(nested)?;
        }
        let nested = when(lit(true), nested).otherwise(col("b"))?;
        assert_eq!(nested.to_field(&schema)?.1.metadata(), &shared);

        let mut cast_nested = col("a");
        for _ in 0..128 {
            let inner = when(lit(true), col("a")).otherwise(cast_nested)?;
            cast_nested = Expr::Cast(Cast::new(Box::new(inner), DataType::Int32));
        }
        let cast_nested = when(lit(true), cast_nested).otherwise(col("b"))?;
        assert_eq!(cast_nested.to_field(&schema)?.1.metadata(), &shared);

        let mut binary_nested = col("a");
        for _ in 0..128 {
            binary_nested = when(lit(true), binary_nested + lit(1)).otherwise(lit(0))?;
        }
        let binary_nested = when(lit(true), binary_nested).otherwise(col("b"))?;
        let binary_field = binary_nested.to_field(&schema)?.1;
        assert_eq!(binary_field.data_type(), &DataType::Int32);
        assert!(binary_field.metadata().is_empty());

        let identity = Arc::new(crate::expr_fn::create_udf(
            "identity",
            vec![DataType::Int32],
            DataType::Int32,
            crate::Volatility::Immutable,
            Arc::new(|_| {
                Ok(
                    datafusion_expr_common::columnar_value::ColumnarValue::Scalar(
                        ScalarValue::Int32(Some(0)),
                    ),
                )
            }),
        ));
        let scalar_result =
            Expr::ScalarFunction(ScalarFunction::new_udf(identity, vec![col("a")]));
        let scalar_case = when(lit(true), scalar_result).otherwise(col("b"))?;
        assert!(scalar_case.to_field(&schema)?.1.metadata().is_empty());

        let marked = Arc::new(crate::ScalarUDF::from(MarkedIdentity {
            signature: crate::Signature::uniform(
                1,
                vec![DataType::Int32],
                crate::Volatility::Immutable,
            ),
        }));
        let marked_call = |arg| {
            Expr::ScalarFunction(ScalarFunction::new_udf(Arc::clone(&marked), vec![arg]))
        };
        let marked_case =
            when(lit(true), marked_call(col("a"))).otherwise(marked_call(col("b")))?;
        assert_eq!(marked_case.to_field(&schema)?.1.metadata(), &shared);

        let mut deep_function = col("a");
        for _ in 0..9 {
            deep_function = marked_call(deep_function);
        }
        let deep_function_case = when(lit(true), deep_function).otherwise(col("b"))?;
        assert!(
            deep_function_case
                .to_field(&schema)?
                .1
                .metadata()
                .is_empty()
        );

        let nested_arg = when(lit(true), col("a")).otherwise(col("b"))?;
        let nested_call = when(lit(true), marked_call(nested_arg)).otherwise(col("b"))?;
        assert_eq!(nested_call.to_field(&schema)?.1.metadata(), &shared);

        let mut deep_nested_arg = when(lit(true), col("a")).otherwise(col("b"))?;
        for _ in 0..8 {
            deep_nested_arg = deep_nested_arg.alias("nested");
        }
        let deep_nested_call =
            when(lit(true), marked_call(deep_nested_arg)).otherwise(col("b"))?;
        assert!(deep_nested_call.to_field(&schema)?.1.metadata().is_empty());

        let binary = col("d") + lit(1);
        let Expr::Alias(alias) = binary.clone().alias("marked") else {
            unreachable!();
        };
        let marked_alias =
            Expr::Alias(alias.with_metadata(Some(FieldMetadata::from(shared.clone()))));
        let aliased_case = when(lit(true), marked_alias).otherwise(col("b"))?;
        assert_eq!(aliased_case.to_field(&schema)?.1.metadata(), &shared);

        let marked_cast = Expr::Cast(Cast::new_from_field(
            Box::new(binary.clone()),
            Arc::new(Field::new("", DataType::Int32, true).with_metadata(shared.clone())),
        ));
        let cast_case = when(lit(true), marked_cast).otherwise(col("b"))?;
        assert_eq!(cast_case.to_field(&schema)?.1.metadata(), &shared);

        let marked_try_cast = Expr::TryCast(TryCast::new_from_field(
            Box::new(binary),
            Arc::new(Field::new("", DataType::Int32, true).with_metadata(shared.clone())),
        ));
        let try_cast_case = when(lit(true), marked_try_cast).otherwise(col("b"))?;
        assert_eq!(try_cast_case.to_field(&schema)?.1.metadata(), &shared);

        let mut deep_alias = col("a");
        for _ in 0..10 {
            deep_alias = deep_alias.alias("nested");
        }
        let deep_udf_case =
            when(lit(true), marked_call(deep_alias.clone())).otherwise(col("b"))?;
        assert!(deep_udf_case.to_field(&schema)?.1.metadata().is_empty());

        let Expr::Alias(alias) = deep_alias.alias("marked") else {
            unreachable!();
        };
        let deep_marked_alias =
            Expr::Alias(alias.with_metadata(Some(FieldMetadata::from(shared.clone()))));
        let deep_alias_case = when(lit(true), deep_marked_alias).otherwise(col("b"))?;
        assert_eq!(deep_alias_case.to_field(&schema)?.1.metadata(), &shared);

        let mismatched = when(lit(true), col("a")).otherwise(col("c"))?;
        assert!(mismatched.to_field(&schema)?.1.metadata().is_empty());

        let unmarked = when(lit(true), col("a")).otherwise(col("d"))?;
        assert!(unmarked.to_field(&schema)?.1.metadata().is_empty());

        let first_unmarked = when(lit(true), col("d")).otherwise(col("a"))?;
        assert!(first_unmarked.to_field(&schema)?.1.metadata().is_empty());

        Ok(())
    }

    #[test]
    fn test_case_field_metadata_leaf_branches() -> Result<()> {
        let metadata = HashMap::from([("type".to_string(), "structured".to_string())]);
        let field = Arc::new(
            Field::new("value", DataType::Int32, false).with_metadata(metadata.clone()),
        );
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("value", DataType::Int32, false)
                    .with_metadata(metadata.clone()),
            ]
            .into(),
            HashMap::new(),
        )?;

        let branches = [
            Expr::OuterReferenceColumn(Arc::clone(&field), Column::from_name("outer")),
            Expr::ScalarVariable(Arc::clone(&field), vec!["value".to_string()]),
            Expr::Placeholder(Placeholder::new_with_field(
                "$1".to_string(),
                Some(Arc::clone(&field)),
            )),
            Expr::LambdaVariable(LambdaVariable::new("arg".into(), Some(field))),
        ];
        for branch in branches {
            let case = when(lit(true), col("value")).otherwise(branch)?;
            assert_eq!(case.to_field(&schema)?.1.metadata(), &metadata);
        }

        let null_first =
            when(lit(true), lit(ScalarValue::Null)).otherwise(col("value"))?;
        assert_eq!(null_first.to_field(&schema)?.1.metadata(), &metadata);

        let wrong_type = when(lit(true), col("value")).otherwise(Expr::Literal(
            ScalarValue::Boolean(Some(false)),
            Some(FieldMetadata::from(metadata)),
        ))?;
        assert!(wrong_type.to_field(&schema)?.1.metadata().is_empty());
        Ok(())
    }

    #[test]
    fn test_case_field_metadata_higher_order_function() -> Result<()> {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct MetadataPassthrough {
            signature: crate::HigherOrderSignature,
        }

        impl crate::HigherOrderUDFImpl for MetadataPassthrough {
            fn name(&self) -> &str {
                "metadata_passthrough"
            }

            fn signature(&self) -> &crate::HigherOrderSignature {
                &self.signature
            }

            fn lambda_parameters(
                &self,
                _step: usize,
                _fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
            ) -> Result<crate::LambdaParametersProgress> {
                Ok(crate::LambdaParametersProgress::Complete(vec![]))
            }

            fn return_field_from_args(
                &self,
                args: HigherOrderReturnFieldArgs,
            ) -> Result<FieldRef> {
                let ValueOrLambda::Value(field) = &args.arg_fields[0] else {
                    unreachable!();
                };
                Ok(Arc::clone(field))
            }

            fn invoke_with_args(
                &self,
                _args: crate::HigherOrderFunctionArgs,
            ) -> Result<crate::ColumnarValue> {
                unreachable!()
            }
        }

        let metadata = HashMap::from([("kind".to_string(), "marked".to_string())]);
        let schema = DFSchema::from_unqualified_fields(
            vec![
                Field::new("a", DataType::Int32, false).with_metadata(metadata.clone()),
                Field::new("b", DataType::Int32, false).with_metadata(metadata.clone()),
            ]
            .into(),
            HashMap::new(),
        )?;
        let udf = Arc::new(crate::HigherOrderUDF::new_from_impl(MetadataPassthrough {
            signature: crate::HigherOrderSignature::any(1, crate::Volatility::Immutable),
        }));
        let call = |arg| {
            Expr::HigherOrderFunction(crate::expr::HigherOrderFunction::new(
                Arc::clone(&udf),
                vec![arg],
            ))
        };

        let shallow = when(lit(true), call(col("a"))).otherwise(col("b"))?;
        assert_eq!(shallow.to_field(&schema)?.1.metadata(), &metadata);

        let mut deep = col("a");
        for _ in 0..9 {
            deep = call(deep);
        }
        let deep_case = when(lit(true), deep).otherwise(col("b"))?;
        assert!(deep_case.to_field(&schema)?.1.metadata().is_empty());
        Ok(())
    }

    #[test]
    fn test_alias_metadata_is_preserved_in_field_metadata() {
        let schema = MockExprSchema::new().with_data_type(DataType::Int32);
        let alias_metadata = FieldMetadata::from(HashMap::from([(
            "some_key".to_string(),
            "some_value".to_string(),
        )]));

        let Expr::Alias(alias) = col("foo").alias("alias") else {
            unreachable!();
        };
        let expr = Expr::Alias(alias.with_metadata(Some(alias_metadata.clone())));

        let field = expr.to_field(&schema).unwrap().1;
        assert_eq!(
            field.metadata().get("some_key"),
            Some(&"some_value".to_string())
        );
        assert_eq!(expr.metadata(&schema).unwrap(), alias_metadata);
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

    #[test]
    fn test_untyped_aliased_placeholder_in_empty_schema() {
        // A UNION arm computes the type of `$1 AS a` against the input of the
        // projection, which does not have a column `a`.
        let schema = DFSchema::empty();
        let expr = Expr::Placeholder(Placeholder::new_with_field("$1".to_string(), None))
            .alias("a");

        assert_eq!(expr.get_type(&schema).unwrap(), DataType::Null);
        assert_eq!(
            expr.to_field(&schema).unwrap().1.data_type(),
            &DataType::Null
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

    /// A scan of `t`, whose single column `a` has the given nullability.
    fn scan_t(a_nullable: bool) -> LogicalPlanBuilder {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, a_nullable)]);
        let source = Arc::new(LogicalTableSource::new(Arc::new(schema)));
        LogicalPlanBuilder::scan("t", source, None).unwrap()
    }

    #[test]
    fn in_subquery_nullability() {
        // `x IN (SELECT a FROM t)` evaluates to NULL when `x` is NULL, and when `x`
        // matches no row while `a` contains a NULL. So it is nullable exactly when
        // either the compared expression or the subquery's output column is.
        let cases = [
            (false, false, false),
            (false, true, true),
            (true, false, true),
            (true, true, true),
        ];

        for (x_nullable, a_nullable, expected) in cases {
            let subquery = scan_t(a_nullable)
                .project(vec![col("a")])
                .unwrap()
                .build()
                .unwrap();
            let expr = in_subquery(col("x"), Arc::new(subquery));
            let schema = MockExprSchema::new().with_nullable(x_nullable);

            assert_eq!(expr.nullable(&schema).unwrap(), expected);
        }
    }

    #[test]
    fn in_subquery_nullability_uses_subquery_output_schema() {
        // `DISTINCT` carries no expressions of its own, but its output column is still
        // nullable, so the `IN` expression must be nullable too.
        let subquery = scan_t(true)
            .project(vec![col("a")])
            .unwrap()
            .distinct()
            .unwrap()
            .build()
            .unwrap();
        let expr = in_subquery(col("x"), Arc::new(subquery));
        assert!(expr.nullable(&MockExprSchema::new()).unwrap());

        // A computed projection's expressions reference `t.a`, which does not appear in
        // the subquery's output schema, so nullability must be read off that schema's
        // single column rather than by resolving the projection's expressions against it.
        let subquery = scan_t(false)
            .project(vec![col("a") + lit(1)])
            .unwrap()
            .build()
            .unwrap();
        let expr = in_subquery(col("x"), Arc::new(subquery));
        assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
    }

    #[test]
    fn in_subquery_nullability_errors_for_no_subquery_columns() {
        let subquery = LogicalPlanBuilder::empty(false).build().unwrap();
        let expr = in_subquery(col("x"), Arc::new(subquery));

        let err = expr.nullable(&MockExprSchema::new()).unwrap_err();
        assert_eq!(
            err.strip_backtrace(),
            "Error during planning: subquery must return exactly one column of data to compare against"
        );
    }

    #[test]
    fn scalar_subquery_nullability_accounts_for_min_rows() {
        let possibly_empty = LogicalPlanBuilder::empty(false)
            .project(vec![lit(1)])
            .unwrap()
            .build()
            .unwrap();
        assert!(!possibly_empty.schema().field(0).is_nullable());

        let expr = scalar_subquery(Arc::new(possibly_empty));
        assert!(expr.nullable(&MockExprSchema::new()).unwrap());

        let field = expr.to_field(&MockExprSchema::new()).unwrap().1;
        assert_eq!(field.data_type(), &DataType::Int32);
        assert!(field.is_nullable());

        let always_one = LogicalPlanBuilder::empty(false)
            .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])
            .unwrap()
            .build()
            .unwrap();
        assert!(!always_one.schema().field(0).is_nullable());

        let expr = scalar_subquery(Arc::new(always_one));
        assert!(!expr.nullable(&MockExprSchema::new()).unwrap());
        assert!(
            !expr
                .to_field(&MockExprSchema::new())
                .unwrap()
                .1
                .is_nullable()
        );
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

        assert_eq!(meta, expr.metadata(&schema).unwrap());
    }

    #[test]
    fn test_cast_and_try_cast_extension_type_metadata() {
        use crate::expr::{Cast, TryCast};
        use arrow_schema::extension::{
            EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY,
        };

        // Helper to build either Cast or TryCast expression
        fn make_cast_expr(
            expr: Expr,
            target_field: FieldRef,
            use_try_cast: bool,
        ) -> Expr {
            if use_try_cast {
                Expr::TryCast(TryCast {
                    expr: Box::new(expr),
                    field: target_field,
                })
            } else {
                Expr::Cast(Cast {
                    expr: Box::new(expr),
                    field: target_field,
                })
            }
        }

        // Run the same test logic for both Cast and TryCast
        for use_try_cast in [false, true] {
            let cast_name = if use_try_cast { "TryCast" } else { "Cast" };

            // Create a schema with a field that has extension type metadata
            let mut source_meta = HashMap::new();
            source_meta.insert(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "arrow.uuid".to_string(),
            );
            source_meta.insert("custom_key".to_string(), "custom_value".to_string());

            let source_field = Field::new("foo", DataType::FixedSizeBinary(16), false)
                .with_metadata(source_meta);

            let schema = MockExprSchema::new()
                .with_data_type(DataType::FixedSizeBinary(16))
                .with_metadata(FieldMetadata::from(source_field.metadata().clone()));

            // Test 1: Cast to a type without extension metadata strips extension metadata
            // but preserves non-extension metadata
            let cast_expr = make_cast_expr(
                col("foo"),
                Arc::new(Field::new("", DataType::Utf8, true)),
                use_try_cast,
            );

            let (_, result_field) = cast_expr.to_field(&schema).unwrap();
            assert!(
                result_field
                    .metadata()
                    .get(EXTENSION_TYPE_NAME_KEY)
                    .is_none(),
                "{cast_name}: Extension type name should be stripped when target has no extension metadata"
            );
            assert_eq!(
                result_field.metadata().get("custom_key"),
                Some(&"custom_value".to_string()),
                "{cast_name}: Non-extension metadata should be preserved"
            );
            if use_try_cast {
                assert!(
                    result_field.is_nullable(),
                    "TryCast result should be nullable"
                );
            }

            // Test 2: Cast to a field with explicit metadata uses target metadata exactly
            let mut target_meta = HashMap::new();
            target_meta.insert(
                EXTENSION_TYPE_NAME_KEY.to_string(),
                "arrow.json".to_string(),
            );
            target_meta.insert(EXTENSION_TYPE_METADATA_KEY.to_string(), "{}".to_string());

            let target_field =
                Field::new("", DataType::Utf8, true).with_metadata(target_meta);

            let cast_expr =
                make_cast_expr(col("foo"), Arc::new(target_field), use_try_cast);

            let (_, result_field) = cast_expr.to_field(&schema).unwrap();
            assert_eq!(
                result_field.metadata().get(EXTENSION_TYPE_NAME_KEY),
                Some(&"arrow.json".to_string()),
                "{cast_name}: Extension type name should come from target field"
            );
            assert_eq!(
                result_field.metadata().get(EXTENSION_TYPE_METADATA_KEY),
                Some(&"{}".to_string()),
                "{cast_name}: Extension type metadata should come from target field"
            );
            assert!(
                result_field.metadata().get("custom_key").is_none(),
                "{cast_name}: Source metadata should NOT propagate when target has explicit metadata"
            );
            if use_try_cast {
                assert!(
                    result_field.is_nullable(),
                    "TryCast result should be nullable"
                );
            }
        }
    }
}
