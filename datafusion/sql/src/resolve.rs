// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use datafusion_common::{DataFusionError, Result};

use crate::parser::{CopyToSource, CopyToStatement, Statement as DFStatement};
use crate::planner::object_name_to_table_reference;
use datafusion_common::TableReference;
use sqlparser::ast::*;

// Collect table/CTE references as `TableReference`s and normalize them during traversal.
// This avoids a second normalization/conversion pass after visiting the AST.
struct RelationVisitor {
    relations: BTreeSet<TableReference>,
    versioned_relations: BTreeSet<(TableReference, TableVersion)>,
    all_ctes: BTreeSet<TableReference>,
    ctes_in_scope: Vec<TableReference>,
    enable_ident_normalization: bool,
    /// The names of the table factors with a time travel clause that are
    /// being visited, innermost last. These names are recorded in
    /// `versioned_relations`, so they are skipped when visited as relations.
    /// Only used for pointer comparison.
    versioned_names: Vec<*const ObjectName>,
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

    /// Record the reference to `relation` with a time travel clause, such as
    /// `VERSION AS OF 3`, if it's not a CTE reference.
    fn insert_versioned_relation(
        &mut self,
        relation: &ObjectName,
        version: &TableVersion,
    ) -> ControlFlow<DataFusionError> {
        match object_name_to_table_reference(
            relation.clone(),
            self.enable_ident_normalization,
        ) {
            Ok(relation) => {
                if !self.ctes_in_scope.contains(&relation) {
                    self.versioned_relations.insert((relation, version.clone()));
                }
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    }
}

/// Returns the name and time travel clause of `table_factor`, if it is a
/// table with a time travel clause, such as `t VERSION AS OF 3`
fn versioned_table(table_factor: &TableFactor) -> Option<(&ObjectName, &TableVersion)> {
    match table_factor {
        TableFactor::Table {
            name,
            args: None,
            version: Some(version),
            ..
        } => Some((name, version)),
        _ => None,
    }
}

impl Visitor for RelationVisitor {
    type Break = DataFusionError;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if self
            .versioned_names
            .iter()
            .any(|name| std::ptr::eq(*name, relation))
        {
            return ControlFlow::Continue(());
        }

        self.insert_relation(relation)
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let Some((name, version)) = versioned_table(table_factor) {
            // The table name is also visited as a relation. Skip exactly that
            // node, so it is not recorded without its version as well. Other
            // relations within this table factor, such as those in a subquery
            // in the time travel clause, are still recorded.
            self.versioned_names.push(name);
            self.insert_versioned_relation(name, version)?;
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &TableFactor,
    ) -> ControlFlow<Self::Break> {
        if versioned_table(table_factor).is_some() {
            self.versioned_names.pop();
        }
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
        // Catalogs are not tables, so there is nothing to resolve here.
        DFStatement::CreateExternalCatalog(_) => {}
    }
    Ok(())
}

/// Tables, views and CTEs referenced in a SQL statement, as collected by
/// [`resolve_table_references_with_versions`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TableReferences {
    /// Tables and views referenced without a time travel clause
    pub tables: Vec<TableReference>,
    /// Tables and views referenced with a time travel clause, such as
    /// `VERSION AS OF 3`, together with that clause
    pub versioned_tables: Vec<(TableReference, TableVersion)>,
    /// CTE aliases that were defined and possibly referenced
    pub ctes: Vec<TableReference>,
}

/// Collects all tables and views referenced in the SQL statement. CTEs are collected separately.
/// This can be used to determine which tables need to be in the catalog for a query to be planned.
///
/// Tables referenced with a time travel clause, such as `VERSION AS OF 3`,
/// are included without their version. Use
/// [`resolve_table_references_with_versions`] to also obtain those versions.
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
    let visitor = visit_relations(statement, enable_ident_normalization)?;
    let mut relations = visitor.relations;
    relations.extend(visitor.versioned_relations.into_iter().map(|(r, _)| r));
    Ok((
        relations.into_iter().collect(),
        visitor.all_ctes.into_iter().collect(),
    ))
}

/// Collects all tables, views and CTEs referenced in the SQL statement, like
/// [`resolve_table_references`], and also the time travel clauses of tables,
/// such as `VERSION AS OF 3`.
///
/// A table referenced with a time travel clause is only included in
/// [`TableReferences::tables`] if it is also referenced without one.
///
/// ## Example
///
/// ```
/// # use datafusion_sql::parser::DFParserBuilder;
/// # use datafusion_sql::resolve::resolve_table_references_with_versions;
/// # use datafusion_sql::sqlparser::dialect::DatabricksDialect;
/// let query = "SELECT * FROM foo JOIN bar VERSION AS OF 3 ON foo.id = bar.id";
/// let statement = DFParserBuilder::new(query)
///     .with_dialect(&DatabricksDialect {})
///     .build()
///     .unwrap()
///     .parse_statements()
///     .unwrap()
///     .pop_back()
///     .unwrap();
/// let references = resolve_table_references_with_versions(&statement, true).unwrap();
/// assert_eq!(references.tables.len(), 1);
/// assert_eq!(references.tables[0].to_string(), "foo");
/// assert_eq!(references.versioned_tables.len(), 1);
/// assert_eq!(references.versioned_tables[0].0.to_string(), "bar");
/// assert_eq!(references.versioned_tables[0].1.to_string(), "VERSION AS OF 3");
/// ```
pub fn resolve_table_references_with_versions(
    statement: &crate::parser::Statement,
    enable_ident_normalization: bool,
) -> Result<TableReferences> {
    let visitor = visit_relations(statement, enable_ident_normalization)?;
    Ok(TableReferences {
        tables: visitor.relations.into_iter().collect(),
        versioned_tables: visitor.versioned_relations.into_iter().collect(),
        ctes: visitor.all_ctes.into_iter().collect(),
    })
}

fn visit_relations(
    statement: &crate::parser::Statement,
    enable_ident_normalization: bool,
) -> Result<RelationVisitor> {
    let mut visitor = RelationVisitor {
        relations: BTreeSet::new(),
        versioned_relations: BTreeSet::new(),
        all_ctes: BTreeSet::new(),
        ctes_in_scope: vec![],
        enable_ident_normalization,
        versioned_names: vec![],
    };
    visit_statement(statement, &mut visitor)?;
    Ok(visitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_snowflake(query: &str) -> crate::parser::Statement {
        use crate::parser::DFParserBuilder;
        use sqlparser::dialect::SnowflakeDialect;

        DFParserBuilder::new(query)
            .with_dialect(&SnowflakeDialect {})
            .build()
            .unwrap()
            .parse_statements()
            .unwrap()
            .pop_back()
            .unwrap()
    }

    #[test]
    fn resolve_table_references_with_versions_collects_versions() {
        let statement = parse_snowflake(
            "SELECT * FROM a \
             JOIN b AT (VERSION => 1) ON true \
             JOIN B AT (VERSION => 1) ON true \
             JOIN b TIMESTAMP AS OF now() ON true \
             WHERE a.x IN (SELECT x FROM c VERSION AS OF 2)",
        );
        let references =
            resolve_table_references_with_versions(&statement, true).unwrap();
        let tables: Vec<_> = references.tables.iter().map(|t| t.to_string()).collect();
        assert_eq!(tables, ["a"]);
        let versioned: Vec<_> = references
            .versioned_tables
            .iter()
            .map(|(t, v)| format!("{t} {v}"))
            .collect();
        assert_eq!(
            versioned,
            [
                "b TIMESTAMP AS OF now()",
                "b AT(VERSION => 1)",
                "c VERSION AS OF 2"
            ]
        );
        assert!(references.ctes.is_empty());

        // `resolve_table_references` returns all tables, without versions
        let (tables, ctes) = resolve_table_references(&statement, true).unwrap();
        let tables: Vec<_> = tables.iter().map(|t| t.to_string()).collect();
        assert_eq!(tables, ["a", "b", "c"]);
        assert_eq!(ctes, references.ctes);
    }

    #[test]
    fn resolve_table_references_with_versions_mixed() {
        // A table referenced both with and without a time travel clause
        let statement = parse_snowflake(
            "SELECT * FROM t AS cur JOIN t VERSION AS OF 1 AS old ON true",
        );
        let references =
            resolve_table_references_with_versions(&statement, true).unwrap();
        let tables: Vec<_> = references.tables.iter().map(|t| t.to_string()).collect();
        assert_eq!(tables, ["t"]);
        let versioned: Vec<_> = references
            .versioned_tables
            .iter()
            .map(|(t, v)| format!("{t} {v}"))
            .collect();
        assert_eq!(versioned, ["t VERSION AS OF 1"]);
    }

    #[test]
    fn resolve_table_references_with_versions_subquery_in_version() {
        // Tables in a subquery in the time travel clause are still recorded.
        // Planning reports an error for the non-constant version.
        let tables_of = |query: &str| {
            let references =
                resolve_table_references_with_versions(&parse_snowflake(query), true)
                    .unwrap();
            let tables: Vec<_> =
                references.tables.iter().map(|t| t.to_string()).collect();
            let versioned: Vec<_> = references
                .versioned_tables
                .iter()
                .map(|(t, _)| t.to_string())
                .collect();
            (tables, versioned)
        };

        let (tables, versioned) =
            tables_of("SELECT * FROM t TIMESTAMP AS OF (SELECT max(ts) FROM log)");
        assert_eq!(tables, ["log"]);
        assert_eq!(versioned, ["t"]);

        let (tables, versioned) = tables_of(
            "SELECT * FROM t AT (VERSION => (SELECT max(v) FROM a JOIN b ON true))",
        );
        assert_eq!(tables, ["a", "b"]);
        assert_eq!(versioned, ["t"]);

        let (tables, versioned) = tables_of(
            "SELECT * FROM t AT (VERSION => \
             (SELECT max(v) FROM log VERSION AS OF 1 JOIN a ON true))",
        );
        assert_eq!(tables, ["a"]);
        assert_eq!(versioned, ["log", "t"]);
    }

    #[test]
    fn resolve_table_references_with_versions_skips_ctes() {
        // A versioned reference to a CTE is not a table reference. Planning
        // reports an error for it.
        let statement = parse_snowflake(
            "WITH t AS (SELECT * FROM t VERSION AS OF 1) SELECT * FROM t VERSION AS OF 2",
        );
        let references =
            resolve_table_references_with_versions(&statement, true).unwrap();
        let versioned: Vec<_> = references
            .versioned_tables
            .iter()
            .map(|(t, v)| format!("{t} {v}"))
            .collect();
        assert_eq!(versioned, ["t VERSION AS OF 1"]);
        assert_eq!(references.ctes.len(), 1);
    }

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
