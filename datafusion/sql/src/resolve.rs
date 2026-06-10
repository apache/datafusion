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

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion_common::{DataFusionError, Result};

use crate::TableReference;
use crate::parser::{CopyToSource, CopyToStatement, Statement as DFStatement};
use crate::planner::object_name_to_table_reference;
use sqlparser::ast::*;

// following constants are used in `resolve_table_references`
// and should be same as `datafusion/catalog/src/information_schema.rs`
const INFORMATION_SCHEMA: &str = "information_schema";
const TABLES: &str = "tables";
const VIEWS: &str = "views";
const COLUMNS: &str = "columns";
const DF_SETTINGS: &str = "df_settings";
const SCHEMATA: &str = "schemata";
const ROUTINES: &str = "routines";
const PARAMETERS: &str = "parameters";

/// All information schema tables
const INFORMATION_SCHEMA_TABLES: &[&str] = &[
    TABLES,
    VIEWS,
    COLUMNS,
    DF_SETTINGS,
    SCHEMATA,
    ROUTINES,
    PARAMETERS,
];

// Collect table/CTE references as `TableReference`s and normalize them during traversal.
// This avoids a second normalization/conversion pass after visiting the AST.
struct RelationVisitor {
    relations: BTreeSet<TableReference>,
    all_ctes: BTreeSet<TableReference>,
    ctes_in_scope: Vec<TableReference>,
    enable_ident_normalization: bool,
}

impl RelationVisitor {
    /// Record the reference to `relation`, if it's not a CTE reference.
    fn insert_relation(&mut self, relation: &ObjectName) -> ControlFlow<DataFusionError> {
        match object_name_to_table_reference(
            relation.clone(),
            self.enable_ident_normalization,
        ) {
            Ok(relation) => {
                if !self.relations.contains(&relation)
                    && !self.ctes_in_scope.contains(&relation)
                {
                    self.relations.insert(relation);
                }
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    }
}

impl Visitor for RelationVisitor {
    type Break = DataFusionError;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.insert_relation(relation)
    }

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                // The non-recursive CTE name is not in scope when evaluating the CTE itself, so this is valid:
                // `WITH t AS (SELECT * FROM t) SELECT * FROM t`
                // Where the first `t` refers to a predefined table. So we are careful here
                // to visit the CTE first, before putting it in scope.
                if !with.recursive {
                    // This is a bit hackish as the CTE will be visited again as part of visiting `q`,
                    // but thankfully `insert_relation` is idempotent.
                    cte.visit(self)?;
                }
                let cte_name = ObjectName::from(vec![cte.alias.name.clone()]);
                match object_name_to_table_reference(
                    cte_name,
                    self.enable_ident_normalization,
                ) {
                    Ok(cte_ref) => self.ctes_in_scope.push(cte_ref),
                    Err(e) => return ControlFlow::Break(e),
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &q.with {
            for _ in &with.cte_tables {
                // Unwrap: We just pushed these in `pre_visit_query`
                self.all_ctes.insert(self.ctes_in_scope.pop().unwrap());
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if let Statement::ShowCreate {
            obj_type: ShowCreateObject::Table | ShowCreateObject::View,
            obj_name,
        } = statement
        {
            self.insert_relation(obj_name)?;
        }

        // SHOW statements will later be rewritten into a SELECT from the information_schema
        let requires_information_schema = matches!(
            statement,
            Statement::ShowFunctions { .. }
                | Statement::ShowVariable { .. }
                | Statement::ShowStatus { .. }
                | Statement::ShowVariables { .. }
                | Statement::ShowCreate { .. }
                | Statement::ShowColumns { .. }
                | Statement::ShowTables { .. }
                | Statement::ShowCollation { .. }
        );
        if requires_information_schema {
            for s in INFORMATION_SCHEMA_TABLES {
                // Information schema references are synthesized here, so convert directly.
                let obj = ObjectName::from(vec![
                    Ident::new(INFORMATION_SCHEMA),
                    Ident::new(*s),
                ]);
                match object_name_to_table_reference(obj, self.enable_ident_normalization)
                {
                    Ok(tbl_ref) => {
                        self.relations.insert(tbl_ref);
                    }
                    Err(e) => return ControlFlow::Break(e),
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn control_flow_to_result(flow: ControlFlow<DataFusionError>) -> Result<()> {
    match flow {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(err) => Err(err),
    }
}

fn visit_statement(statement: &DFStatement, visitor: &mut RelationVisitor) -> Result<()> {
    match statement {
        DFStatement::Statement(s) => {
            control_flow_to_result(s.as_ref().visit(visitor))?;
        }
        DFStatement::CreateExternalTable(table) => {
            control_flow_to_result(visitor.insert_relation(&table.name))?;
        }
        DFStatement::CopyTo(CopyToStatement { source, .. }) => match source {
            CopyToSource::Relation(table_name) => {
                control_flow_to_result(visitor.insert_relation(table_name))?;
            }
            CopyToSource::Query(query) => {
                control_flow_to_result(query.visit(visitor))?;
            }
        },
        DFStatement::Explain(explain) => {
            visit_statement(&explain.statement, visitor)?;
        }
        DFStatement::Reset(_) => {}
    }
    Ok(())
}

/// Collects all tables and views referenced in the SQL statement. CTEs are collected separately.
/// This can be used to determine which tables need to be in the catalog for a query to be planned.
///
/// # Returns
///
/// A `(table_refs, ctes)` tuple, the first element contains table and view references and the second
/// element contains any CTE aliases that were defined and possibly referenced.
///
/// ## Example
///
/// ```
/// # use datafusion_sql::parser::DFParser;
/// # use datafusion_sql::resolve::resolve_table_references;
/// let query = "SELECT a FROM foo where x IN (SELECT y FROM bar)";
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
/// let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
/// assert_eq!(table_refs.len(), 2);
/// assert_eq!(table_refs[0].to_string(), "bar");
/// assert_eq!(table_refs[1].to_string(), "foo");
/// assert_eq!(ctes.len(), 0);
/// ```
///
/// ## Example with CTEs  
///  
/// ```  
/// # use datafusion_sql::parser::DFParser;
/// # use datafusion_sql::resolve::resolve_table_references;
/// let query = "with my_cte as (values (1), (2)) SELECT * from my_cte;";
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
/// let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
/// assert_eq!(table_refs.len(), 0);
/// assert_eq!(ctes.len(), 1);
/// assert_eq!(ctes[0].to_string(), "my_cte");
/// ```
pub fn resolve_table_references(
    statement: &crate::parser::Statement,
    enable_ident_normalization: bool,
) -> Result<(Vec<TableReference>, Vec<TableReference>)> {
    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
        all_ctes: BTreeSet::new(),
        ctes_in_scope: vec![],
        enable_ident_normalization,
    };

    visit_statement(statement, &mut visitor)?;

    Ok((
        visitor.relations.into_iter().collect(),
        visitor.all_ctes.into_iter().collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_table_references_shadowed_cte() {
        use crate::parser::DFParser;

        // An interesting edge case where the `t` name is used both as an ordinary table reference
        // and as a CTE reference.
        let query = "WITH t AS (SELECT * FROM t) SELECT * FROM t";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(table_refs[0].to_string(), "t");

        // UNION is a special case where the CTE is not in scope for the second branch.
        let query = "(with t as (select 1) select * from t) union (select * from t)";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(table_refs[0].to_string(), "t");

        // Nested CTEs are also handled.
        // Here the first `u` is a CTE, but the second `u` is a table reference.
        // While `t` is always a CTE.
        let query = "(with t as (with u as (select 1) select * from u) select * from u cross join t)";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 1);
        assert_eq!(ctes.len(), 2);
        assert_eq!(ctes[0].to_string(), "t");
        assert_eq!(ctes[1].to_string(), "u");
        assert_eq!(table_refs[0].to_string(), "u");
    }

    #[test]
    fn resolve_table_references_recursive_cte() {
        use crate::parser::DFParser;

        let query = "
            WITH RECURSIVE nodes AS ( 
                SELECT 1 as id
                UNION ALL 
                SELECT id + 1 as id 
                FROM nodes
                WHERE id < 10
            )
            SELECT * FROM nodes
        ";
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(table_refs.len(), 0);
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "nodes");
    }

    #[test]
    fn resolve_table_references_cte_with_quoted_reference() {
        use crate::parser::DFParser;

        let query = r#"with barbaz as (select 1) select * from "barbaz""#;
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "barbaz");
        // Quoted reference should still resolve to the CTE when normalization is on
        assert_eq!(table_refs.len(), 0);
    }

    #[test]
    fn resolve_table_references_cte_with_quoted_reference_normalization_off() {
        use crate::parser::DFParser;

        let query = r#"with barbaz as (select 1) select * from "barbaz""#;
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, false).unwrap();
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "barbaz");
        // Even with normalization off, quoted reference matches same-case CTE name
        assert_eq!(table_refs.len(), 0);
    }

    #[test]
    fn resolve_table_references_cte_with_quoted_reference_uppercase_normalization_on() {
        use crate::parser::DFParser;

        let query = r#"with FOObar as (select 1) select * from "FOObar""#;
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();
        // CTE name is normalized to lowercase, quoted reference preserves case, so they differ
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "foobar");
        assert_eq!(table_refs.len(), 1);
        assert_eq!(table_refs[0].to_string(), "FOObar");
    }

    #[test]
    fn resolve_table_references_cte_with_quoted_reference_uppercase_normalization_off() {
        use crate::parser::DFParser;

        let query = r#"with FOObar as (select 1) select * from "FOObar""#;
        let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
        let (table_refs, ctes) = resolve_table_references(&statement, false).unwrap();
        // Without normalization, cases match exactly, so quoted reference resolves to the CTE
        assert_eq!(ctes.len(), 1);
        assert_eq!(ctes[0].to_string(), "FOObar");
        assert_eq!(table_refs.len(), 0);
    }
}
