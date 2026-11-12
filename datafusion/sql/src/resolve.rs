// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::collections::{BTreeSet, HashSet};
use std::ops::ControlFlow;

use datafusion_common::{DataFusionError, Result};

use crate::parser::{CopyToSource, CopyToStatement, Statement as DFStatement};
use crate::planner::object_name_to_table_reference;
use datafusion_common::TableReference;
use sqlparser::ast::*;

// Collect table/CTE references as `TableReference`s and normalize them during traversal.
// This avoids a second normalization/conversion pass after visiting the AST.
//
// # Non-recursive CTEs
//
// The name of a non-recursive CTE is in scope after its definition, but not
// within its own body. The `Visitor` trait has no hooks for individual CTEs,
// so `pre_visit_query` visits these CTE bodies itself, putting each name in
// scope after its body.
// The AST traversal cannot be instructed to skip subtrees so the CTE bodies
// are visited a second time, with all CTEs of the query in scope. During this
// second visit we shouldn't record anything: it's redundant, and its scope is
// wrong.
//
// For example, in this query the inner `b` is a CTE inside the body of CTE
// `a`, and the `c` in `a`'s body is a table because CTE `c` is defined later:
//
// ```sql
// WITH a AS (
//   WITH b AS (SELECT * FROM t)  -- [1]
//   SELECT * FROM b, c, u        -- [2]
// ),
// c AS (SELECT * FROM a)         -- [3]
// SELECT * FROM c                -- [4]
// ```
//
// First pass, by `pre_visit_query` of the outer query, with
// `skipped_cte_body_depth` 0:
//
// * The body of `a`, with no CTEs in scope. Its own `pre_visit_query` visits
//   [1]: `t` is recorded, then `b` is put in scope. The traversal then visits
//   [1] again, as described below, which records nothing. At [2], `b` is a CTE
//   reference, and `c` and `u` are recorded as tables. Then `a` is put in
//   scope.
// * [3], with `a` in scope: `a` is a CTE reference. Then `c` is put in scope.
//
// Second pass, by the traversal of the outer query. Each body visited in the
// first pass increments `skipped_cte_body_depth` on entry and decrements it on
// exit:
//
// * The body of `a`: `skipped_cte_body_depth` becomes 1. `pre_visit_query` no
//   longer visits CTE bodies itself, but still puts `b` in scope.
// * [1]: `skipped_cte_body_depth` becomes 2, and is back to 1 afterwards.
// * [2]: `skipped_cte_body_depth` is still 1, so nothing is recorded. Here `c`
//   is in scope and would wrongly be seen as a CTE reference. With a flag
//   instead of a counter, leaving [1] would have cleared it, and `u` would
//   have been recorded twice.
// * [3]: skipped the same way, after which `skipped_cte_body_depth` is back
//   to 0.
// * [4]: the main body of the query, which was not visited in the first pass.
//   Nothing is skipped, but `c` is a CTE reference, so it is not recorded.
//
// Each table factor, `t`, `c` and `u`, was reported once.
struct RelationVisitor<F> {
    ctes_in_scope: Vec<TableReference>,
    enable_ident_normalization: bool,
    /// The names of the table factors that are being visited, innermost
    /// last. These names are reported with their table factor, so they are
    /// skipped when visited as relations. Only used for pointer comparison.
    table_factor_names: Vec<*const ObjectName>,
    /// The bodies of the non-recursive CTEs that have been visited before
    /// their name was put in scope. They are skipped when visited again as
    /// part of their query. Only used for pointer comparison.
    visited_cte_bodies: HashSet<*const Query>,
    /// The number of CTE bodies in `visited_cte_bodies` that are being
    /// visited again. While positive, nothing is reported.
    skipped_cte_body_depth: usize,
    /// Called for each reference
    on_reference: F,
}

impl<F: FnMut(ResolvedReference<'_>) -> Result<()>> RelationVisitor<F> {
    fn new(enable_ident_normalization: bool, on_reference: F) -> Self {
        Self {
            ctes_in_scope: vec![],
            enable_ident_normalization,
            table_factor_names: vec![],
            visited_cte_bodies: HashSet::new(),
            skipped_cte_body_depth: 0,
            on_reference,
        }
    }

    fn report(
        &mut self,
        reference: ResolvedReference<'_>,
    ) -> ControlFlow<DataFusionError> {
        match (self.on_reference)(reference) {
            Ok(()) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(e),
        }
    }

    /// Normalizes `name`, and returns `None` if it references a CTE
    fn table_reference(
        &self,
        name: &ObjectName,
    ) -> ControlFlow<DataFusionError, Option<TableReference>> {
        match object_name_to_table_reference(
            name.clone(),
            self.enable_ident_normalization,
        ) {
            Ok(table) if self.ctes_in_scope.contains(&table) => {
                ControlFlow::Continue(None)
            }
            Ok(table) => ControlFlow::Continue(Some(table)),
            Err(e) => ControlFlow::Break(e),
        }
    }

    /// Report the reference to the table `relation`, if it's not a CTE
    /// reference
    fn visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<DataFusionError> {
        if let Some(table) = self.table_reference(relation)? {
            self.report(ResolvedReference::Table(table))?;
        }
        ControlFlow::Continue(())
    }

    /// Report `table_factor`, which references the table `relation`, if it's
    /// not a CTE reference
    fn visit_table_factor(
        &mut self,
        relation: &ObjectName,
        table_factor: &TableFactor,
    ) -> ControlFlow<DataFusionError> {
        if let Some(table) = self.table_reference(relation)? {
            self.report(ResolvedReference::TableFactor(table, table_factor))?;
        }
        ControlFlow::Continue(())
    }
}

/// Returns the name of the table referenced by `table_factor`, if it
/// references a table or view, rather than for example a subquery
fn table_factor_name(table_factor: &TableFactor) -> Option<&ObjectName> {
    match table_factor {
        TableFactor::Table {
            name, args: None, ..
        } => Some(name),
        _ => None,
    }
}

impl<F: FnMut(ResolvedReference<'_>) -> Result<()>> Visitor for RelationVisitor<F> {
    type Break = DataFusionError;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        if self.skipped_cte_body_depth > 0
            || self
                .table_factor_names
                .iter()
                .any(|name| std::ptr::eq(*name, relation))
        {
            return ControlFlow::Continue(());
        }

        self.visit_relation(relation)
    }

    fn pre_visit_table_factor(
        &mut self,
        table_factor: &TableFactor,
    ) -> ControlFlow<Self::Break> {
        if let Some(name) = table_factor_name(table_factor) {
            // The table name is also visited as a relation. Skip exactly that
            // node, so it is not reported without its table factor as well.
            // Other relations within this table factor, such as those in a
            // subquery in a time travel clause, are still reported.
            self.table_factor_names.push(name);
            if self.skipped_cte_body_depth == 0 {
                self.visit_table_factor(name, table_factor)?;
            }
        }
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(
        &mut self,
        table_factor: &TableFactor,
    ) -> ControlFlow<Self::Break> {
        if table_factor_name(table_factor).is_some() {
            self.table_factor_names.pop();
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<Self::Break> {
        if self.visited_cte_bodies.contains(&std::ptr::from_ref(q)) {
            self.skipped_cte_body_depth += 1;
        }
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                // The non-recursive CTE name is not in scope when evaluating the CTE itself, so this is valid:
                // `WITH t AS (SELECT * FROM t) SELECT * FROM t`
                // Where the first `t` refers to a predefined table. So we are careful here
                // to visit the CTE first, before putting it in scope. Its body is skipped when
                // visited again as part of `q`; see "Non-recursive CTEs" on `RelationVisitor`.
                if !with.recursive && self.skipped_cte_body_depth == 0 {
                    cte.visit(self)?;
                    self.visited_cte_bodies
                        .insert(std::ptr::from_ref(cte.query.as_ref()));
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
                let cte = self.ctes_in_scope.pop().unwrap();
                // CTEs in skipped bodies were reported when first visited
                if self.skipped_cte_body_depth == 0 {
                    self.report(ResolvedReference::Cte(cte))?;
                }
            }
        }
        if self.visited_cte_bodies.contains(&std::ptr::from_ref(q)) {
            self.skipped_cte_body_depth -= 1;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        if let Statement::ShowCreate {
            obj_type: ShowCreateObject::Table | ShowCreateObject::View,
            obj_name,
        } = statement
        {
            self.visit_relation(obj_name)?;
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

fn visit_statement<F: FnMut(ResolvedReference<'_>) -> Result<()>>(
    statement: &DFStatement,
    visitor: &mut RelationVisitor<F>,
) -> Result<()> {
    match statement {
        DFStatement::Statement(s) => {
            control_flow_to_result(s.as_ref().visit(visitor))?;
        }
        DFStatement::CreateExternalTable(table) => {
            control_flow_to_result(visitor.visit_relation(&table.name))?;
        }
        DFStatement::CopyTo(CopyToStatement { source, .. }) => match source {
            CopyToSource::Relation(table_name) => {
                control_flow_to_result(visitor.visit_relation(table_name))?;
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

/// A reference in a SQL statement, as reported by [`resolve_references`]
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedReference<'a> {
    /// A table or view referenced by a table factor, such as `t` in
    /// `SELECT * FROM t`
    TableFactor(TableReference, &'a TableFactor),
    /// A table or view referenced other than by a table factor, such as the
    /// target of an `INSERT` statement
    Table(TableReference),
    /// A CTE defined in the statement
    Cte(TableReference),
}

/// Collects all tables and views referenced in the SQL statement. CTEs are collected separately.
/// This can be used to determine which tables need to be in the catalog for a query to be planned.
///
/// Only the tables are returned, not the table factors that reference them.
/// Use [`resolve_references`] to also visit the table factors.
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
    let mut tables = BTreeSet::new();
    let mut ctes = BTreeSet::new();
    resolve_references(statement, enable_ident_normalization, |reference| {
        match reference {
            ResolvedReference::TableFactor(table, _)
            | ResolvedReference::Table(table) => {
                tables.insert(table);
            }
            ResolvedReference::Cte(cte) => {
                ctes.insert(cte);
            }
        }
        Ok(())
    })?;
    Ok((tables.into_iter().collect(), ctes.into_iter().collect()))
}

/// Finds the tables, views and CTEs referenced in a SQL statement.
///
/// Unlike [`resolve_table_references`], this also reports the table factor
/// through which a table is referenced, which determines the
/// [`TableSelector`] used to read the table.
///
/// [`TableSelector`]: datafusion_common::TableSelector
///
/// Each reference is passed to `on_reference` as a [`ResolvedReference`].
/// Callers should not make assumptions on the order in which references are passed
/// to the callback. Table factors that reference a CTE are not reported, but the
/// definition of the CTE is.
///
/// References are reported as often as they occur, so the same table or CTE
/// may be reported more than once. For example, in
/// `INSERT INTO t SELECT * FROM t JOIN t AS t2 ON true`, `t` is reported as a
/// [`ResolvedReference::Table`] and twice as a
/// [`ResolvedReference::TableFactor`]. Callers that need the distinct set of tables
/// must remove duplicates themselves, as [`resolve_table_references`] does.
///
/// [`TableReference`]s are reported in the form they are encountered. For instance, `t` will be
/// reported as a [`TableReference::Bare`] and `public.t` as a [`TableReference::Partial`].
///
/// ## Example
///
/// ```
/// # use datafusion_sql::parser::DFParser;
/// # use datafusion_sql::resolve::{ResolvedReference, resolve_references};
/// let query = "INSERT INTO foo SELECT * FROM bar AS b";
/// let statement = DFParser::parse_sql(query).unwrap().pop_back().unwrap();
/// let mut references = vec![];
/// resolve_references(&statement, true, |reference| {
///     references.push(match reference {
///         ResolvedReference::TableFactor(table, table_factor) => {
///             format!("table factor {table}: {table_factor}")
///         }
///         ResolvedReference::Table(table) => format!("table {table}"),
///         ResolvedReference::Cte(cte) => format!("cte {cte}"),
///     });
///     Ok(())
/// })
/// .unwrap();
/// assert_eq!(references, ["table foo", "table factor bar: bar AS b"]);
/// ```
pub fn resolve_references(
    statement: &crate::parser::Statement,
    enable_ident_normalization: bool,
    on_reference: impl FnMut(ResolvedReference<'_>) -> Result<()>,
) -> Result<()> {
    let mut visitor = RelationVisitor::new(enable_ident_normalization, on_reference);
    visit_statement(statement, &mut visitor)
}

/// Like [`resolve_references`], for a SQL statement as parsed by sqlparser
pub(crate) fn resolve_sql_references(
    statement: &Statement,
    enable_ident_normalization: bool,
    on_reference: impl FnMut(ResolvedReference<'_>) -> Result<()>,
) -> Result<()> {
    let mut visitor = RelationVisitor::new(enable_ident_normalization, on_reference);
    control_flow_to_result(statement.visit(&mut visitor))
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

    /// Returns the references reported for `query`, in the Snowflake dialect
    fn references_of(query: &str) -> Vec<String> {
        let mut references = vec![];
        resolve_references(&parse_snowflake(query), true, |reference| {
            references.push(match reference {
                ResolvedReference::TableFactor(table, table_factor) => {
                    format!("{table}: {table_factor}")
                }
                ResolvedReference::Table(table) => format!("table {table}"),
                ResolvedReference::Cte(cte) => format!("cte {cte}"),
            });
            Ok(())
        })
        .unwrap();
        references
    }

    #[test]
    fn resolve_references_reports_table_factors() {
        let query = "SELECT * FROM a \
             JOIN b AT (VERSION => 1) ON true \
             JOIN B AT (VERSION => 1) ON true \
             JOIN b TIMESTAMP AS OF now() ON true \
             WHERE a.x IN (SELECT x FROM c VERSION AS OF 2)";
        assert_eq!(
            references_of(query),
            [
                "a: a",
                "b: b AT(VERSION => 1)",
                "b: B AT(VERSION => 1)",
                "b: b TIMESTAMP AS OF now()",
                "c: c VERSION AS OF 2"
            ]
        );

        // `resolve_table_references` returns all tables, without their table
        // factors
        let (tables, ctes) =
            resolve_table_references(&parse_snowflake(query), true).unwrap();
        let tables: Vec<_> = tables.iter().map(|t| t.to_string()).collect();
        assert_eq!(tables, ["a", "b", "c"]);
        assert!(ctes.is_empty());
    }

    #[test]
    fn resolve_references_other_references() {
        // Tables referenced other than by a table factor
        assert_eq!(
            references_of("INSERT INTO t SELECT * FROM t AS cur JOIN t AS old ON true"),
            ["table t", "t: t AS cur", "t: t AS old"]
        );
    }

    #[test]
    fn resolve_references_subquery_in_version() {
        // Tables in a subquery in a time travel clause are still reported.
        // Planning reports an error for the non-constant version.
        assert_eq!(
            references_of("SELECT * FROM t TIMESTAMP AS OF (SELECT max(ts) FROM log)"),
            ["t: t TIMESTAMP AS OF (SELECT max(ts) FROM log)", "log: log"]
        );
        assert_eq!(
            references_of(
                "SELECT * FROM t AT (VERSION => \
                 (SELECT max(v) FROM log VERSION AS OF 1 JOIN a ON true))"
            ),
            [
                "t: t AT(VERSION => (SELECT max(v) FROM log VERSION AS OF 1 JOIN a ON true))",
                "log: log VERSION AS OF 1",
                "a: a"
            ]
        );
    }

    #[test]
    fn resolve_references_visits_ctes_once() {
        // The body of a non-recursive CTE is visited before the CTE is in
        // scope, and not again afterwards
        assert_eq!(
            references_of("WITH a AS (SELECT * FROM t VERSION AS OF 1) SELECT * FROM a"),
            ["t: t VERSION AS OF 1", "cte a"]
        );

        // The example of "Non-recursive CTEs" on `RelationVisitor`
        assert_eq!(
            references_of(
                "WITH a AS (WITH b AS (SELECT * FROM t) SELECT * FROM b, c, u), \
                 c AS (SELECT * FROM a) \
                 SELECT * FROM c"
            ),
            ["t: t", "c: c", "u: u", "cte b", "cte c", "cte a"]
        );

        // The body of a recursive CTE is only visited with the CTE in scope
        assert_eq!(
            references_of(
                "WITH RECURSIVE a AS (SELECT * FROM t UNION ALL SELECT * FROM a) \
                 SELECT * FROM a"
            ),
            ["t: t", "cte a"]
        );
    }

    #[test]
    fn resolve_references_skips_cte_references() {
        // A reference to a CTE is not a table reference. Planning reports an
        // error for its time travel clause.
        assert_eq!(
            references_of(
                "WITH t AS (SELECT * FROM t VERSION AS OF 1) \
                 SELECT * FROM t VERSION AS OF 2"
            ),
            ["t: t VERSION AS OF 1", "cte t"]
        );
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
