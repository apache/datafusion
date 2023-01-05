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

use crate::planner::{
    idents_to_table_reference, ContextProvider, PlannerContext, SqlToRel,
};
use crate::utils::normalize_ident;
use datafusion_common::{
    Column, DFSchema, DataFusionError, OwnedTableReference, Result, ScalarValue,
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

    pub(super) fn sql_compound_identifier_to_expr(
        &self,
        ids: Vec<Ident>,
        schema: &DFSchema,
    ) -> Result<Expr> {
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
            // only support "schema.table" type identifiers here
            let (name, relation) = match idents_to_table_reference(ids)? {
                OwnedTableReference::Partial { schema, table } => (table, schema),
                r @ OwnedTableReference::Bare { .. }
                | r @ OwnedTableReference::Full { .. } => {
                    return Err(DataFusionError::Plan(format!(
                        "Unsupported compound identifier '{r:?}'",
                    )));
                }
            };

            // Try and find the reference in schema
            match schema.field_with_qualified_name(&relation, &name) {
                Ok(_) => {
                    // found an exact match on a qualified name so this is a table.column identifier
                    Ok(Expr::Column(Column {
                        relation: Some(relation),
                        name,
                    }))
                }
                Err(_) => {
                    if let Some(field) =
                        schema.fields().iter().find(|f| f.name().eq(&relation))
                    {
                        // Access to a field of a column which is a structure, example: SELECT my_struct.key
                        Ok(Expr::GetIndexedField(GetIndexedField::new(
                            Box::new(Expr::Column(field.qualified_column())),
                            ScalarValue::Utf8(Some(name)),
                        )))
                    } else {
                        // table.column identifier
                        Ok(Expr::Column(Column {
                            relation: Some(relation),
                            name,
                        }))
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
