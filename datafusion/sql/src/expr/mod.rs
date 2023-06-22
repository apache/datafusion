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

pub(crate) mod arrow_cast;
mod binary_op;
mod function;
mod grouping_set;
mod identifier;
mod order_by;
mod subquery;
mod substring;
mod unary_op;
mod value;

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use arrow_schema::DataType;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr::{InList, Placeholder};
use datafusion_expr::{
    col, expr, lit, AggregateFunction, Between, BinaryExpr, BuiltinScalarFunction, Cast,
    Expr, ExprSchemable, GetIndexedField, Like, Operator, TryCast,
};
use sqlparser::ast::{ArrayAgg, Expr as SQLExpr, TrimWhereField, Value};
use sqlparser::parser::ParserError::ParserError;

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(crate) fn sql_expr_to_logical_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        enum StackEntry {
            SQLExpr(Box<SQLExpr>),
            Operator(Operator),
        }

        // Virtual stack machine to convert SQLExpr to Expr
        // This allows visiting the expr tree in a depth-first manner which
        // produces expressions in postfix notations, i.e. `a + b` => `a b +`.
        // See https://github.com/apache/arrow-datafusion/issues/1444
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sql))];
        let mut eval_stack = vec![];

        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sql_expr) => {
                    match *sql_expr {
                        SQLExpr::BinaryOp { left, op, right } => {
                            // Note the order that we push the entries to the stack
                            // is important. We want to visit the left node first.
                            let op = self.parse_sql_binary_op(op)?;
                            stack.push(StackEntry::Operator(op));
                            stack.push(StackEntry::SQLExpr(right));
                            stack.push(StackEntry::SQLExpr(left));
                        }
                        _ => {
                            let expr = self.sql_expr_to_logical_expr_internal(
                                *sql_expr,
                                schema,
                                planner_context,
                            )?;
                            eval_stack.push(expr);
                        }
                    }
                }
                StackEntry::Operator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let expr = Expr::BinaryExpr(BinaryExpr::new(
                        Box::new(left),
                        op,
                        Box::new(right),
                    ));
                    eval_stack.push(expr);
                }
            }
        }

        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    /// Generate a relational expression from a SQL expression
    pub fn sql_to_expr(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr = self.sql_expr_to_logical_expr(sql, schema, planner_context)?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        let expr = infer_placeholder_types(expr, schema)?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema
                        .fields()
                        .iter()
                        .find(|field| match field.qualifier() {
                            Some(field_q) => {
                                field.name() == &col.name
                                    && field_q.to_string().ends_with(&format!(".{q}"))
                            }
                            _ => false,
                        }) {
                        Some(df_field) => Expr::Column(Column {
                            relation: df_field.qualifier().cloned(),
                            name: df_field.name().clone(),
                        }),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    /// Internal implementation. Use
    /// [`Self::sql_expr_to_logical_expr`] to plan exprs.
    fn sql_expr_to_logical_expr_internal(
        &self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            SQLExpr::Value(value) => {
                self.parse_value(value, planner_context.prepare_param_data_types())
            }
            SQLExpr::Extract { field, expr } => Ok(Expr::ScalarFunction (ScalarFunction::new(
                BuiltinScalarFunction::DatePart,
                 vec![
                    Expr::Literal(ScalarValue::Utf8(Some(format!("{field}")))),
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
                ],
            ))),

            SQLExpr::Array(arr) => self.sql_array_literal(arr.elem, schema),
            SQLExpr::Interval(interval)=> self.sql_interval_to_expr(
                false,
                interval,
                schema,
                planner_context,
            ),
            SQLExpr::Identifier(id) => self.sql_identifier_to_expr(id, schema, planner_context),

            SQLExpr::MapAccess { column, keys } => {
                if let SQLExpr::Identifier(id) = *column {
                    plan_indexed(col(self.normalizer.normalize(id)), keys)
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "map access requires an identifier, found column {column} instead"
                    )))
                }
            }

            SQLExpr::ArrayIndex { obj, indexes } => {
                let expr = self.sql_expr_to_logical_expr(*obj, schema, planner_context)?;
                plan_indexed(expr, indexes)
            }

            SQLExpr::CompoundIdentifier(ids) => self.sql_compound_identifier_to_expr(ids, schema, planner_context),

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.sql_case_identifier_to_expr(operand, conditions, results, else_result, schema, planner_context),

            SQLExpr::Cast {
                expr,
                data_type,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::TryCast {
                expr,
                data_type,
            } => Ok(Expr::TryCast(TryCast::new(
                Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::TypedString {
                data_type,
                value,
            } => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.sql_expr_to_logical_expr(*left, schema, planner_context)?),
                Operator::IsDistinctFrom,
                Box::new(self.sql_expr_to_logical_expr(*right, schema, planner_context)?),
            ))),

            SQLExpr::IsNotDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(self.sql_expr_to_logical_expr(*left, schema, planner_context)?),
                Operator::IsNotDistinctFrom,
                Box::new(self.sql_expr_to_logical_expr(*right, schema, planner_context)?),
            ))),

            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsUnknown(expr) => Ok(Expr::IsUnknown(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::IsNotUnknown(expr) => Ok(Expr::IsNotUnknown(Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?))),

            SQLExpr::UnaryOp { op, expr } => self.parse_sql_unary_op(op, *expr, schema, planner_context),

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(self.sql_expr_to_logical_expr(*expr, schema, planner_context)?),
                negated,
                Box::new(self.sql_expr_to_logical_expr(*low, schema, planner_context)?),
                Box::new(self.sql_expr_to_logical_expr(*high, schema, planner_context)?),
            ))),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => self.sql_in_list_to_expr(*expr, list, negated, schema, planner_context),

            SQLExpr::Like { negated, expr, pattern, escape_char } => self.sql_like_to_expr(negated, *expr, *pattern, escape_char, schema, planner_context),

            SQLExpr::ILike { negated, expr, pattern, escape_char } =>  self.sql_ilike_to_expr(negated, *expr, *pattern, escape_char, schema, planner_context),

            SQLExpr::SimilarTo { negated, expr, pattern, escape_char } => self.sql_similarto_to_expr(negated, *expr, *pattern, escape_char, schema, planner_context),

            SQLExpr::BinaryOp {
                ..
            } => {
                Err(DataFusionError::Internal(
                    "binary_op should be handled by sql_expr_to_logical_expr.".to_string()
                ))
            }

            #[cfg(feature = "unicode_expressions")]
            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
            } => self.sql_substring_to_expr(expr, substring_from, substring_for, schema, planner_context),

            #[cfg(not(feature = "unicode_expressions"))]
            SQLExpr::Substring {
                ..
            } => {
                Err(DataFusionError::Internal(
                    "statement substring requires compilation with feature flag: unicode_expressions.".to_string()
                ))
            }

            SQLExpr::Trim { expr, trim_where, trim_what } => self.sql_trim_to_expr(*expr, trim_where, trim_what, schema, planner_context),

            SQLExpr::AggregateExpressionWithFilter { expr, filter } => self.sql_agg_with_filter_to_expr(*expr, *filter, schema, planner_context),

            SQLExpr::Function(function) => self.sql_function_to_expr(function, schema, planner_context),

            SQLExpr::Rollup(exprs) => self.sql_rollup_to_expr(exprs, schema, planner_context),
            SQLExpr::Cube(exprs) => self.sql_cube_to_expr(exprs,schema, planner_context),
            SQLExpr::GroupingSets(exprs) => self.sql_grouping_sets_to_expr(exprs, schema, planner_context),

            SQLExpr::Floor { expr, field: _field } => self.sql_named_function_to_expr(*expr, BuiltinScalarFunction::Floor, schema, planner_context),
            SQLExpr::Ceil { expr, field: _field } => self.sql_named_function_to_expr(*expr, BuiltinScalarFunction::Ceil, schema, planner_context),

            SQLExpr::Nested(e) => self.sql_expr_to_logical_expr(*e, schema, planner_context),

            SQLExpr::Exists { subquery, negated } => self.parse_exists_subquery(*subquery, negated, schema, planner_context),
            SQLExpr::InSubquery { expr, subquery, negated } => self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context),
            SQLExpr::Subquery(subquery) => self.parse_scalar_subquery(*subquery, schema, planner_context),

            SQLExpr::ArrayAgg(array_agg) => self.parse_array_agg(array_agg, schema, planner_context),

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node in sqltorel: {sql:?}"
            ))),
        }
    }

    fn parse_array_agg(
        &self,
        array_agg: ArrayAgg,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Some dialects have special syntax for array_agg. DataFusion only supports it like a function.
        let ArrayAgg {
            distinct,
            expr,
            order_by,
            limit,
            within_group,
        } = array_agg;

        let order_by = if let Some(order_by) = order_by {
            Some(self.order_by_to_sort_expr(&order_by, input_schema, planner_context)?)
        } else {
            None
        };

        if let Some(limit) = limit {
            return Err(DataFusionError::NotImplemented(format!(
                "LIMIT not supported in ARRAY_AGG: {limit}"
            )));
        }

        if within_group {
            return Err(DataFusionError::NotImplemented(
                "WITHIN GROUP not supported in ARRAY_AGG".to_string(),
            ));
        }

        let args =
            vec![self.sql_expr_to_logical_expr(*expr, input_schema, planner_context)?];

        // next, aggregate built-ins
        let fun = AggregateFunction::ArrayAgg;
        Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
            fun, args, distinct, None, order_by,
        )))
    }

    fn sql_in_list_to_expr(
        &self,
        expr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let list_expr = list
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;

        Ok(Expr::InList(InList::new(
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            list_expr,
            negated,
        )))
    }

    fn sql_like_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<char>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return Err(DataFusionError::Plan(
                "Invalid pattern in LIKE expression".to_string(),
            ));
        }
        Ok(Expr::Like(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
        )))
    }

    fn sql_ilike_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<char>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return Err(DataFusionError::Plan(
                "Invalid pattern in ILIKE expression".to_string(),
            ));
        }
        Ok(Expr::ILike(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
        )))
    }

    fn sql_similarto_to_expr(
        &self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<char>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self.sql_expr_to_logical_expr(pattern, schema, planner_context)?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return Err(DataFusionError::Plan(
                "Invalid pattern in SIMILAR TO expression".to_string(),
            ));
        }
        Ok(Expr::SimilarTo(Like::new(
            negated,
            Box::new(self.sql_expr_to_logical_expr(expr, schema, planner_context)?),
            Box::new(pattern),
            escape_char,
        )))
    }

    fn sql_trim_to_expr(
        &self,
        expr: SQLExpr,
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = match trim_where {
            Some(TrimWhereField::Leading) => BuiltinScalarFunction::Ltrim,
            Some(TrimWhereField::Trailing) => BuiltinScalarFunction::Rtrim,
            Some(TrimWhereField::Both) => BuiltinScalarFunction::Btrim,
            None => BuiltinScalarFunction::Trim,
        };
        let arg = self.sql_expr_to_logical_expr(expr, schema, planner_context)?;
        let args = match trim_what {
            Some(to_trim) => {
                let to_trim =
                    self.sql_expr_to_logical_expr(*to_trim, schema, planner_context)?;
                vec![arg, to_trim]
            }
            None => vec![arg],
        };
        Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)))
    }

    fn sql_agg_with_filter_to_expr(
        &self,
        expr: SQLExpr,
        filter: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match self.sql_expr_to_logical_expr(expr, schema, planner_context)? {
            Expr::AggregateFunction(expr::AggregateFunction {
                fun,
                args,
                distinct,
                order_by,
                ..
            }) => Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun,
                args,
                distinct,
                Some(Box::new(self.sql_expr_to_logical_expr(
                    filter,
                    schema,
                    planner_context,
                )?)),
                order_by,
            ))),
            _ => Err(DataFusionError::Plan(
                "AggregateExpressionWithFilter expression was not an AggregateFunction"
                    .to_string(),
            )),
        }
    }
}

// modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }) = expr {
        if data_type.is_none() {
            let other_dt = other.get_type(schema);
            match other_dt {
                Err(e) => {
                    return Err(e.context(format!(
                        "Can not find type of {other} needed to infer type of {expr}"
                    )))?;
                }
                Ok(dt) => {
                    *data_type = Some(dt);
                }
            }
        };
    }
    Ok(())
}

/// Find all [`Expr::Placeholder`] tokens in a logical plan, and try
/// to infer their [`DataType`] from the context of their use.
fn infer_placeholder_types(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    expr.transform(&|mut expr| {
        // Default to assuming the arguments are the same type
        if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = &mut expr {
            rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
            rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
        };
        Ok(Transformed::Yes(expr))
    })
}

fn plan_key(key: SQLExpr) -> Result<ScalarValue> {
    let scalar = match key {
        SQLExpr::Value(Value::Number(s, _)) => ScalarValue::Int64(Some(
            s.parse()
                .map_err(|_| ParserError(format!("Cannot parse {s} as i64.")))?,
        )),
        SQLExpr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
            ScalarValue::Utf8(Some(s))
        }
        _ => {
            return Err(DataFusionError::SQL(ParserError(format!(
                "Unsupported index key expression: {key:?}"
            ))));
        }
    };

    Ok(scalar)
}

fn plan_indexed(expr: Expr, mut keys: Vec<SQLExpr>) -> Result<Expr> {
    let key = keys.pop().ok_or_else(|| {
        ParserError("Internal error: Missing index key expression".to_string())
    })?;

    let expr = if !keys.is_empty() {
        plan_indexed(expr, keys)?
    } else {
        expr
    };

    Ok(Expr::GetIndexedField(GetIndexedField::new(
        Box::new(expr),
        plan_key(key)?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::logical_plan::builder::LogicalTableSource;
    use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};

    use crate::TableReference;

    struct TestSchemaProvider {
        options: ConfigOptions,
        tables: HashMap<String, Arc<dyn TableSource>>,
    }

    impl TestSchemaProvider {
        pub fn new() -> Self {
            let mut tables = HashMap::new();
            tables.insert(
                "table1".to_string(),
                create_table_source(vec![Field::new(
                    "column1".to_string(),
                    DataType::Utf8,
                    false,
                )]),
            );

            Self {
                options: Default::default(),
                tables,
            }
        }
    }

    impl ContextProvider for TestSchemaProvider {
        fn get_table_provider(
            &self,
            name: TableReference,
        ) -> Result<Arc<dyn TableSource>> {
            match self.tables.get(name.table()) {
                Some(table) => Ok(table.clone()),
                _ => Err(DataFusionError::Plan(format!(
                    "Table not found: {}",
                    name.table()
                ))),
            }
        }

        fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
            None
        }

        fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
            None
        }

        fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
            None
        }

        fn options(&self) -> &ConfigOptions {
            &self.options
        }

        fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
            None
        }
    }

    fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
        Arc::new(LogicalTableSource::new(Arc::new(
            Schema::new_with_metadata(fields, HashMap::new()),
        )))
    }

    macro_rules! test_stack_overflow {
        ($num_expr:expr) => {
            paste::item! {
                #[test]
                fn [<test_stack_overflow_ $num_expr>]() {
                    let schema = DFSchema::empty();
                    let mut planner_context = PlannerContext::default();

                    let expr_str = (0..$num_expr)
                        .map(|i| format!("column1 = 'value{:?}'", i))
                        .collect::<Vec<String>>()
                        .join(" OR ");

                    let dialect = GenericDialect{};
                    let mut parser = Parser::new(&dialect)
                        .try_with_sql(expr_str.as_str())
                        .unwrap();
                    let sql_expr = parser.parse_expr().unwrap();

                    let schema_provider = TestSchemaProvider::new();
                    let sql_to_rel = SqlToRel::new(&schema_provider);

                    // Should not stack overflow
                    sql_to_rel.sql_expr_to_logical_expr(
                        sql_expr,
                        &schema,
                        &mut planner_context,
                    ).unwrap();
                }
            }
        };
    }

    test_stack_overflow!(64);
    test_stack_overflow!(128);
    test_stack_overflow!(256);
    test_stack_overflow!(512);
    test_stack_overflow!(1024);
    test_stack_overflow!(2048);
    test_stack_overflow!(4096);
    test_stack_overflow!(8192);
}
