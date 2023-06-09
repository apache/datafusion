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

use super::{Between, Expr, Like};
use crate::expr::{
    AggregateFunction, AggregateUDF, BinaryExpr, Cast, GetIndexedField, InList,
    InSubquery, Placeholder, ScalarFunction, ScalarUDF, Sort, TryCast, WindowFunction,
};
use crate::field_util::get_indexed_field;
use crate::type_coercion::binary::get_result_type;
use crate::type_coercion::other::get_coerce_type_for_case_expression;
use crate::{aggregate_function, window_function, LogicalPlan, Projection, Subquery};
use arrow::compute::can_cast_types;
use arrow::datatypes::DataType;
use datafusion_common::{Column, DFField, DFSchema, DataFusionError, ExprSchema, Result};
use std::sync::Arc;

/// trait to allow expr to typable with respect to a schema
pub trait ExprSchemable {
    /// given a schema, return the type of the expr
    fn get_type<S: ExprSchema>(&self, schema: &S) -> Result<DataType>;

    /// given a schema, return the nullability of the expr
    fn nullable<S: ExprSchema>(&self, input_schema: &S) -> Result<bool>;

    /// convert to a field with respect to a schema
    fn to_field(&self, input_schema: &DFSchema) -> Result<DFField>;

    /// cast to a type with respect to a schema
    fn cast_to<S: ExprSchema>(self, cast_to_type: &DataType, schema: &S) -> Result<Expr>;
}

impl ExprSchemable for Expr {
    /// Returns the [arrow::datatypes::DataType] of the expression
    /// based on [ExprSchema]
    ///
    /// Note: [DFSchema] implements [ExprSchema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// [arrow::datatypes::DataType].  This happens when e.g. the
    /// expression refers to a column that does not exist in the
    /// schema, or when the expression is incorrectly typed
    /// (e.g. `[utf8] + [bool]`).
    fn get_type<S: ExprSchema>(&self, schema: &S) -> Result<DataType> {
        match self {
            Expr::Alias(expr, name) => match &**expr {
                Expr::Placeholder(Placeholder { data_type, .. }) => match &data_type {
                    None => schema.data_type(&Column::from_name(name)).cloned(),
                    Some(dt) => Ok(dt.clone()),
                },
                _ => expr.get_type(schema),
            },
            Expr::Sort(Sort { expr, .. }) | Expr::Negative(expr) => expr.get_type(schema),
            Expr::Column(c) => Ok(schema.data_type(c)?.clone()),
            Expr::OuterReferenceColumn(ty, _) => Ok(ty.clone()),
            Expr::ScalarVariable(ty, _) => Ok(ty.clone()),
            Expr::Literal(l) => Ok(l.get_datatype()),
            Expr::Case(case) => {
                // https://github.com/apache/arrow-datafusion/issues/5821
                // when #5681 will be fixed, this code can be reverted to:
                // case.when_then_expr[0].1.get_type(schema)
                let then_types = case
                    .when_then_expr
                    .iter()
                    .map(|when_then| when_then.1.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                let else_type = match &case.else_expr {
                    None => Ok(None),
                    Some(expr) => expr.get_type(schema).map(Some),
                }?;
                get_coerce_type_for_case_expression(&then_types, else_type.as_ref())
                    .ok_or_else(|| {
                        DataFusionError::Internal(String::from(
                            "Cannot infer type for CASE statement",
                        ))
                    })
            }
            Expr::Cast(Cast { data_type, .. })
            | Expr::TryCast(TryCast { data_type, .. }) => Ok(data_type.clone()),
            Expr::ScalarUDF(ScalarUDF { fun, args }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
            }
            Expr::ScalarFunction(ScalarFunction { fun, args }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                fun.return_type(&data_types)
            }
            Expr::WindowFunction(WindowFunction { fun, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                window_function::return_type(fun, &data_types)
            }
            Expr::AggregateFunction(AggregateFunction { fun, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                aggregate_function::return_type(fun, &data_types)
            }
            Expr::AggregateUDF(AggregateUDF { fun, args, .. }) => {
                let data_types = args
                    .iter()
                    .map(|e| e.get_type(schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok((fun.return_type)(&data_types)?.as_ref().clone())
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
            }) => get_result_type(&left.get_type(schema)?, op, &right.get_type(schema)?),
            Expr::Like { .. } | Expr::ILike { .. } | Expr::SimilarTo { .. } => {
                Ok(DataType::Boolean)
            }
            Expr::Placeholder(Placeholder { data_type, .. }) => {
                data_type.clone().ok_or_else(|| {
                    DataFusionError::Plan(
                        "Placeholder type could not be resolved".to_owned(),
                    )
                })
            }
            Expr::Wildcard => {
                // Wildcard do not really have a type and do not appear in projections
                Ok(DataType::Null)
            }
            Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
                "QualifiedWildcard expressions are not valid in a logical query plan"
                    .to_owned(),
            )),
            Expr::GroupingSet(_) => {
                // grouping sets do not really have a type and do not appear in projections
                Ok(DataType::Null)
            }
            Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                let data_type = expr.get_type(schema)?;

                get_indexed_field(&data_type, key).map(|x| x.data_type().clone())
            }
        }
    }

    /// Returns the nullability of the expression based on [ExprSchema].
    ///
    /// Note: [DFSchema] implements [ExprSchema].
    ///
    /// # Errors
    ///
    /// This function errors when it is not possible to compute its
    /// nullability.  This happens when the expression refers to a
    /// column that does not exist in the schema.
    fn nullable<S: ExprSchema>(&self, input_schema: &S) -> Result<bool> {
        match self {
            Expr::Alias(expr, _)
            | Expr::Not(expr)
            | Expr::Negative(expr)
            | Expr::Sort(Sort { expr, .. })
            | Expr::InList(InList { expr, .. }) => expr.nullable(input_schema),
            Expr::Between(Between { expr, .. }) => expr.nullable(input_schema),
            Expr::Column(c) => input_schema.nullable(c),
            Expr::OuterReferenceColumn(_, _) => Ok(true),
            Expr::Literal(value) => Ok(value.is_null()),
            Expr::Case(case) => {
                // this expression is nullable if any of the input expressions are nullable
                let then_nullable = case
                    .when_then_expr
                    .iter()
                    .map(|(_, t)| t.nullable(input_schema))
                    .collect::<Result<Vec<_>>>()?;
                if then_nullable.contains(&true) {
                    Ok(true)
                } else if let Some(e) = &case.else_expr {
                    e.nullable(input_schema)
                } else {
                    // CASE produces NULL if there is no `else` expr
                    // (aka when none of the `when_then_exprs` match)
                    Ok(true)
                }
            }
            Expr::Cast(Cast { expr, .. }) => expr.nullable(input_schema),
            Expr::ScalarVariable(_, _)
            | Expr::TryCast { .. }
            | Expr::ScalarFunction(..)
            | Expr::ScalarUDF(..)
            | Expr::WindowFunction { .. }
            | Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::Placeholder(_) => Ok(true),
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
            Expr::Like(Like { expr, .. }) => expr.nullable(input_schema),
            Expr::ILike(Like { expr, .. }) => expr.nullable(input_schema),
            Expr::SimilarTo(Like { expr, .. }) => expr.nullable(input_schema),
            Expr::Wildcard => Err(DataFusionError::Internal(
                "Wildcard expressions are not valid in a logical query plan".to_owned(),
            )),
            Expr::QualifiedWildcard { .. } => Err(DataFusionError::Internal(
                "QualifiedWildcard expressions are not valid in a logical query plan"
                    .to_owned(),
            )),
            Expr::GetIndexedField(GetIndexedField { key, expr }) => {
                let data_type = expr.get_type(input_schema)?;
                get_indexed_field(&data_type, key).map(|x| x.is_nullable())
            }
            Expr::GroupingSet(_) => {
                // grouping sets do not really have the concept of nullable and do not appear
                // in projections
                Ok(true)
            }
        }
    }

    /// Returns a [arrow::datatypes::Field] compatible with this expression.
    ///
    /// So for example, a projected expression `col(c1) + col(c2)` is
    /// placed in an output field **named** col("c1 + c2")
    fn to_field(&self, input_schema: &DFSchema) -> Result<DFField> {
        match self {
            Expr::Column(c) => Ok(DFField::new(
                c.relation.clone(),
                &c.name,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
            _ => Ok(DFField::new_unqualified(
                &self.display_name()?,
                self.get_type(input_schema)?,
                self.nullable(input_schema)?,
            )),
        }
    }

    /// Wraps this expression in a cast to a target [arrow::datatypes::DataType].
    ///
    /// # Errors
    ///
    /// This function errors when it is impossible to cast the
    /// expression to the target [arrow::datatypes::DataType].
    fn cast_to<S: ExprSchema>(self, cast_to_type: &DataType, schema: &S) -> Result<Expr> {
        let this_type = self.get_type(schema)?;
        if this_type == *cast_to_type {
            return Ok(self);
        }

        // TODO(kszucs): most of the operations do not validate the type correctness
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
            Err(DataFusionError::Plan(format!(
                "Cannot automatically convert {this_type:?} to {cast_to_type:?}"
            )))
        }
    }
}

/// cast subquery in InSubquery/ScalarSubquery to a given type.
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
                projection.input.clone(),
            )?)
        }
        _ => {
            let cast_expr = Expr::Column(plan.schema().field(0).qualified_column())
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, lit};
    use arrow::datatypes::DataType;
    use datafusion_common::{Column, ScalarValue};

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
    fn expr_schema_data_type() {
        let expr = col("foo");
        assert_eq!(
            DataType::Utf8,
            expr.get_type(&MockExprSchema::new().with_data_type(DataType::Utf8))
                .unwrap()
        );
    }

    #[derive(Debug)]
    struct MockExprSchema {
        nullable: bool,
        data_type: DataType,
    }

    impl MockExprSchema {
        fn new() -> Self {
            Self {
                nullable: false,
                data_type: DataType::Null,
            }
        }

        fn with_nullable(mut self, nullable: bool) -> Self {
            self.nullable = nullable;
            self
        }

        fn with_data_type(mut self, data_type: DataType) -> Self {
            self.data_type = data_type;
            self
        }
    }

    impl ExprSchema for MockExprSchema {
        fn nullable(&self, _col: &Column) -> Result<bool> {
            Ok(self.nullable)
        }

        fn data_type(&self, _col: &Column) -> Result<&DataType> {
            Ok(&self.data_type)
        }
    }
}
