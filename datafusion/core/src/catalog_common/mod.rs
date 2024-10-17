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

//! Interfaces and default implementations of catalogs and schemas.
//!
//! Implementations
//! * Simple memory based catalog: [`MemoryCatalogProviderList`], [`MemoryCatalogProvider`], [`MemorySchemaProvider`]
//! * Information schema: [`information_schema`]
//! * Listing schema: [`listing_schema`]

pub mod information_schema;
pub mod listing_schema;
pub mod memory;

pub use crate::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
pub use memory::{
    MemoryCatalogProvider, MemoryCatalogProviderList, MemorySchemaProvider,
};

pub use datafusion_sql::{ResolvedTableReference, TableReference};

use std::collections::BTreeSet;
use std::ops::ControlFlow;

/// See [`CatalogProviderList`]
#[deprecated(since = "35.0.0", note = "use [`CatalogProviderList`] instead")]
pub trait CatalogList: CatalogProviderList {}

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
/// # use datafusion::catalog_common::resolve_table_references;
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
/// # use datafusion::catalog_common::resolve_table_references;
/// let query = "with my_cte as (values (1), (2)) SELECT * from my_cte;";  
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();  
/// let (table_refs, ctes) = resolve_table_references(&statement, true).unwrap();  
/// assert_eq!(table_refs.len(), 0);
/// assert_eq!(ctes.len(), 1);  
/// assert_eq!(ctes[0].to_string(), "my_cte");  
/// ```
pub fn resolve_table_references(
    statement: &datafusion_sql::parser::Statement,
    enable_ident_normalization: bool,
) -> datafusion_common::Result<(Vec<TableReference>, Vec<TableReference>)> {
    use crate::sql::planner::object_name_to_table_reference;
    use datafusion_sql::parser::{
        CopyToSource, CopyToStatement, Statement as DFStatement,
    };
    use information_schema::INFORMATION_SCHEMA;
    use information_schema::INFORMATION_SCHEMA_TABLES;
    use sqlparser::ast::*;

    struct RelationVisitor {
        relations: BTreeSet<ObjectName>,
        all_ctes: BTreeSet<ObjectName>,
        ctes_in_scope: Vec<ObjectName>,
    }

    impl RelationVisitor {
        /// Record the reference to `relation`, if it's not a CTE reference.
        fn insert_relation(&mut self, relation: &ObjectName) {
            if !self.relations.contains(relation)
                && !self.ctes_in_scope.contains(relation)
            {
                self.relations.insert(relation.clone());
            }
        }
    }

    impl Visitor for RelationVisitor {
        type Break = ();

        fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<()> {
            self.insert_relation(relation);
            ControlFlow::Continue(())
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
                        cte.visit(self);
                    }
                    self.ctes_in_scope
                        .push(ObjectName(vec![cte.alias.name.clone()]));
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

        fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<()> {
            if let Statement::ShowCreate {
                obj_type: ShowCreateObject::Table | ShowCreateObject::View,
                obj_name,
            } = statement
            {
                self.insert_relation(obj_name)
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
                    self.relations.insert(ObjectName(vec![
                        Ident::new(INFORMATION_SCHEMA),
                        Ident::new(*s),
                    ]));
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
        all_ctes: BTreeSet::new(),
        ctes_in_scope: vec![],
    };

    fn visit_statement(statement: &DFStatement, visitor: &mut RelationVisitor) {
        match statement {
            DFStatement::Statement(s) => {
                let _ = s.as_ref().visit(visitor);
            }
            DFStatement::CreateExternalTable(table) => {
                visitor.relations.insert(table.name.clone());
            }
            DFStatement::CopyTo(CopyToStatement { source, .. }) => match source {
                CopyToSource::Relation(table_name) => {
                    visitor.insert_relation(table_name);
                }
                CopyToSource::Query(query) => {
                    query.visit(visitor);
                }
            },
            DFStatement::Explain(explain) => visit_statement(&explain.statement, visitor),
        }
    }

    visit_statement(statement, &mut visitor);

    let table_refs = visitor
        .relations
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion_common::Result<_>>()?;
    let ctes = visitor
        .all_ctes
        .into_iter()
        .map(|x| object_name_to_table_reference(x, enable_ident_normalization))
        .collect::<datafusion_common::Result<_>>()?;
    Ok((table_refs, ctes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_table_references_shadowed_cte() {
        use datafusion_sql::parser::DFParser;

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
        use datafusion_sql::parser::DFParser;

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
}
