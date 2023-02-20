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

use crate::planner::{ContextProvider, PlannerContext, SqlToRel};
use crate::utils::normalize_ident;
use datafusion_common::{
    Column, DFSchema, DataFusionError, Result, ScalarValue, TableReference,
};
use datafusion_expr::{Case, Expr, GetIndexedField};
use sqlparser::ast::{Expr as SQLExpr, Ident};

impl<'a, S: ContextProvider> SqlToRel<'a, S> {
    pub(super) fn sql_identifier_to_expr(&self, id: Ident) -> Result<Expr> {
        if id.value.starts_with('@') {
            // TODO: figure out if ScalarVariables should be insensitive.
            let var_names = vec![id.value];
            let ty = self
                .schema_provider
                .get_variable_type(&var_names)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "variable {var_names:?} has no type information"
                    ))
                })?;
            Ok(Expr::ScalarVariable(ty, var_names))
        } else {
            // Don't use `col()` here because it will try to
            // interpret names with '.' as if they were
            // compound identifiers, but this is not a compound
            // identifier. (e.g. it is "foo.bar" not foo.bar)

            Ok(Expr::Column(Column {
                relation: None,
                name: normalize_ident(id),
            }))
        }
    }

    // (relation, column name)
    fn form_identifier(idents: &[String]) -> Result<(Option<TableReference>, &String)> {
        match idents.len() {
            1 => Ok((None, &idents[0])),
            2 => Ok((
                Some(TableReference::Bare {
                    table: (&idents[0]).into(),
                }),
                &idents[1],
            )),
            3 => Ok((
                Some(TableReference::Partial {
                    schema: (&idents[0]).into(),
                    table: (&idents[1]).into(),
                }),
                &idents[2],
            )),
            4 => Ok((
                Some(TableReference::Full {
                    catalog: (&idents[0]).into(),
                    schema: (&idents[1]).into(),
                    table: (&idents[2]).into(),
                }),
                &idents[3],
            )),
            _ => Err(DataFusionError::Internal(format!(
                "Incorrect number of identifiers: {}",
                idents.len()
            ))),
        }
    }

    pub(super) fn sql_compound_identifier_to_expr(
        &self,
        ids: Vec<Ident>,
        schema: &DFSchema,
    ) -> Result<Expr> {
        if ids.len() < 2 {
            return Err(DataFusionError::Internal(format!(
                "Not a compound identifier: {ids:?}"
            )));
        }

        if ids[0].value.starts_with('@') {
            let var_names: Vec<_> = ids.into_iter().map(normalize_ident).collect();
            let ty = self
                .schema_provider
                .get_variable_type(&var_names)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "variable {var_names:?} has no type information"
                    ))
                })?;
            Ok(Expr::ScalarVariable(ty, var_names))
        } else {
            let ids = ids
                .into_iter()
                .map(|id| {
                    if self.options.enable_ident_normalization {
                        normalize_ident(id)
                    } else {
                        id.value
                    }
                })
                .collect::<Vec<_>>();

            // Possibilities we search with, in order from top to bottom for each len:
            //
            // len = 2:
            // 1. (table.column)
            // 2. (column).nested
            //
            // len = 3:
            // 1. (schema.table.column)
            // 2. (table.column).nested
            // 3. (column).nested1.nested2
            //
            // len = 4:
            // 1. (catalog.schema.table.column)
            // 2. (schema.table.column).nested1
            // 3. (table.column).nested1.nested2
            // 4. (column).nested1.nested2.nested3
            //
            // len = 5:
            // 1. (catalog.schema.table.column).nested
            // 2. (schema.table.column).nested1.nested2
            // 3. (table.column).nested1.nested2.nested3
            // 4. (column).nested1.nested2.nested3.nested4
            //
            // len > 5:
            // 1. (catalog.schema.table.column).nested[.nestedN]+
            // 2. (schema.table.column).nested1.nested2[.nestedN]+
            // 3. (table.column).nested1.nested2.nested3[.nestedN]+
            // 4. (column).nested1.nested2.nested3.nested4[.nestedN]+
            //
            // Currently not supporting more than one nested level
            // Though ideally once that support is in place, this code should work with it

            // TODO: remove when can support multiple nested identifiers
            if ids.len() > 5 {
                return Err(DataFusionError::Internal(format!(
                    "Unsupported compound identifier: {ids:?}"
                )));
            }

            // take at most 4 identifiers to form a Column to search with
            // - 1 for the column name
            // - 0 to 3 for the TableReference
            let bound = ids.len().min(4);
            // search from most specific to least specific
            let search_result = (0..bound).rev().find_map(|i| {
                let nested_names_index = i + 1;
                let s = &ids[0..nested_names_index];
                let (relation, column_name) = Self::form_identifier(s).unwrap();
                let field = schema.field_with_name(relation.as_ref(), column_name).ok();
                field.map(|f| (f, nested_names_index))
            });

            match search_result {
                // found matching field with spare identifier(s) for nested field(s) in structure
                Some((field, index)) if index < ids.len() => {
                    // TODO: remove when can support multiple nested identifiers
                    if index < ids.len() - 1 {
                        return Err(DataFusionError::Internal(format!(
                            "Nested identifiers not yet supported for column {}",
                            field.qualified_column().quoted_flat_name()
                        )));
                    }
                    let nested_name = ids[index].to_string();
                    Ok(Expr::GetIndexedField(GetIndexedField::new(
                        Box::new(Expr::Column(field.qualified_column())),
                        ScalarValue::Utf8(Some(nested_name)),
                    )))
                }
                // found matching field with no spare identifier(s)
                Some((field, _index)) => Ok(Expr::Column(field.qualified_column())),
                // found no matching field, will return a default
                None => {
                    // return default where use all identifiers to not have a nested field
                    // this len check is because at 5 identifiers will have to have a nested field
                    if ids.len() == 5 {
                        Err(DataFusionError::Internal(format!(
                            "Unsupported compound identifier: {ids:?}"
                        )))
                    } else {
                        let s = &ids[0..ids.len()];
                        let (relation, column_name) = Self::form_identifier(s).unwrap();
                        let relation = relation.map(|r| r.to_owned_reference());
                        Ok(Expr::Column(Column::new(relation, column_name)))
                    }
                }
            }
        }
    }

    pub(super) fn sql_case_identifier_to_expr(
        &self,
        operand: Option<Box<SQLExpr>>,
        conditions: Vec<SQLExpr>,
        results: Vec<SQLExpr>,
        else_result: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let expr = if let Some(e) = operand {
            Some(Box::new(self.sql_expr_to_logical_expr(
                *e,
                schema,
                planner_context,
            )?))
        } else {
            None
        };
        let when_expr = conditions
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        let then_expr = results
            .into_iter()
            .map(|e| self.sql_expr_to_logical_expr(e, schema, planner_context))
            .collect::<Result<Vec<_>>>()?;
        let else_expr = if let Some(e) = else_result {
            Some(Box::new(self.sql_expr_to_logical_expr(
                *e,
                schema,
                planner_context,
            )?))
        } else {
            None
        };

        Ok(Expr::Case(Case::new(
            expr,
            when_expr
                .iter()
                .zip(then_expr.iter())
                .map(|(w, t)| (Box::new(w.to_owned()), Box::new(t.to_owned())))
                .collect(),
            else_expr,
        )))
    }
}
