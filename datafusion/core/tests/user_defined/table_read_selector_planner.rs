// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for the TableReadSelectorPlanner extension point

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::catalog::memory::MemTable;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::prelude::*;
use datafusion_common::{Result, TableReadSelector, TableReference, not_impl_err};
use datafusion_expr::planner::{
    TableReadSelectorContext, TableReadSelectorPlanner, TableReadSelectorPlanning,
};
use datafusion_sql::sqlparser::ast::TableFactor;
use insta::assert_snapshot;

/// A schema with a single table `t`, which counts how often the current and
/// a specific version of `t` are looked up.
#[derive(Debug)]
pub(super) struct CountingSchema {
    table: Arc<dyn TableProvider>,
    /// Whether `table_with_read_selector` is implemented, or returns "not
    /// implemented" like catalogs that predate it
    supports_time_travel: bool,
    pub(super) table_calls: AtomicUsize,
    pub(super) table_with_read_selector_calls: AtomicUsize,
}

#[async_trait]
impl SchemaProvider for CountingSchema {
    fn table_names(&self) -> Vec<String> {
        vec!["t".to_string()]
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        self.table_calls.fetch_add(1, Ordering::SeqCst);
        Ok((name == "t").then(|| Arc::clone(&self.table)))
    }

    async fn table_with_read_selector(
        &self,
        name: &str,
        selector: &TableReadSelector,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        self.table_with_read_selector_calls
            .fetch_add(1, Ordering::SeqCst);
        if !self.supports_time_travel {
            return not_impl_err!("Time travel is not supported");
        }
        if name != "t" {
            return Ok(None);
        }
        let table = MemTable::try_new(self.table.schema(), vec![vec![]])?
            .with_read_selector(Some(selector.clone()));
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        name == "t"
    }
}

/// Registers a [`CountingSchema`] named `counting` in `ctx`, with a table
/// `t` that has the schema of table `unused`
pub(super) async fn register_counting_schema(
    ctx: &SessionContext,
    supports_time_travel: bool,
) -> Arc<CountingSchema> {
    let table = ctx.table_provider("unused").await.unwrap();
    let schema = Arc::new(CountingSchema {
        table,
        supports_time_travel,
        table_calls: AtomicUsize::new(0),
        table_with_read_selector_calls: AtomicUsize::new(0),
    });
    ctx.catalog("datafusion")
        .unwrap()
        .register_schema("counting", Arc::clone(&schema) as _)
        .unwrap();
    schema
}

/// Creates a SessionContext with a table `unused`, with an Int64 column `id`
/// and a Utf8 column `name`, and the given table read selector planners
fn create_context(planners: Vec<Arc<dyn TableReadSelectorPlanner>>) -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("unused", Arc::new(table)).unwrap();
    for planner in planners {
        ctx.register_table_read_selector_planner(planner).unwrap();
    }
    ctx
}

/// Example planner that reads a specific version of a table in the `counting`
/// schema, given by a table hint such as `t WITH (1 + 1)`, rather than a time
/// travel clause.
#[derive(Debug)]
struct VersionHintPlanner;

impl TableReadSelectorPlanner for VersionHintPlanner {
    fn plan_read_selector(
        &self,
        table: &TableReference,
        relation: &TableFactor,
        context: &dyn TableReadSelectorContext,
    ) -> Result<TableReadSelectorPlanning> {
        if table.schema() != Some("counting") {
            return Ok(TableReadSelectorPlanning::Original);
        }
        let TableFactor::Table { with_hints, .. } = relation else {
            return Ok(TableReadSelectorPlanning::Original);
        };
        let [hint] = with_hints.as_slice() else {
            return Ok(TableReadSelectorPlanning::Original);
        };
        let version = context.evaluate_const_expr(hint.clone())?;
        Ok(TableReadSelectorPlanning::Planned(Some(
            TableReadSelector::AsOfVersion(version),
        )))
    }
}

/// Example planner that ignores time travel clauses, reading the table as
/// returned by the catalog instead
#[derive(Debug)]
struct IgnoreTimeTravelPlanner;

impl TableReadSelectorPlanner for IgnoreTimeTravelPlanner {
    fn plan_read_selector(
        &self,
        _table: &TableReference,
        _relation: &TableFactor,
        _context: &dyn TableReadSelectorContext,
    ) -> Result<TableReadSelectorPlanning> {
        Ok(TableReadSelectorPlanning::Planned(None))
    }
}

// A TableReadSelectorPlanner can determine the selector of a table from
// syntax other than a time travel clause
#[tokio::test]
async fn planner_determines_read_selector() {
    let ctx = create_context(vec![Arc::new(VersionHintPlanner)]);
    ctx.sql("SET datafusion.sql_parser.dialect = 'MsSQL'")
        .await
        .unwrap();
    let schema = register_counting_schema(&ctx, true).await;

    let plan = ctx
        .sql("SELECT * FROM counting.t WITH (1 + 1)")
        .await
        .unwrap()
        .into_unoptimized_plan();
    assert_snapshot!(plan, @r"
    Projection: counting.t.id, counting.t.name
      TableScan: counting.t@2
    ");
    assert_eq!(
        schema.table_with_read_selector_calls.load(Ordering::SeqCst),
        1
    );
    // Unlike for a `RelationPlanner`, the current version of the table is
    // not fetched as well
    assert_eq!(schema.table_calls.load(Ordering::SeqCst), 0);

    // The planner only handles tables in the `counting` schema, so the
    // hint of other tables is ignored by DataFusion's default logic
    let plan = ctx
        .sql("SELECT * FROM unused WITH (1 + 1)")
        .await
        .unwrap()
        .into_unoptimized_plan();
    assert_snapshot!(plan, @r"
    Projection: unused.id, unused.name
      TableScan: unused
    ");

    // References to CTEs are read without a selector, so the planner is
    // not asked for one
    let plan = ctx
        .sql("WITH c AS (SELECT 1 AS id) SELECT * FROM c WITH (1 + 1)")
        .await
        .unwrap()
        .into_unoptimized_plan();
    assert_snapshot!(plan, @r"
    Projection: c.id
      SubqueryAlias: c
        Projection: Int64(1) AS id
          EmptyRelation: rows=1
    ");

    // Also not in prepared statements, where evaluating `now()` for a
    // selector would otherwise be rejected
    ctx.sql("PREPARE p AS WITH c AS (SELECT 1 AS id) SELECT * FROM c WITH (now())")
        .await
        .unwrap();
}

// A TableReadSelectorPlanner can read a table as returned by the catalog,
// regardless of its time travel clause. The first planner that determines
// the selector of a table takes precedence, and the planner registered
// last is asked first.
#[tokio::test]
async fn planner_ignores_time_travel() {
    let ctx = create_context(vec![
        Arc::new(IgnoreTimeTravelPlanner),
        Arc::new(VersionHintPlanner),
    ]);
    ctx.sql("SET datafusion.sql_parser.dialect = 'MsSQL'")
        .await
        .unwrap();
    let schema = register_counting_schema(&ctx, false).await;

    let plan = ctx
        .sql("SELECT * FROM counting.t FOR SYSTEM_TIME AS OF '2024-01-01'")
        .await
        .unwrap()
        .into_unoptimized_plan();
    assert_snapshot!(plan, @r"
    Projection: counting.t.id, counting.t.name
      TableScan: counting.t
    ");
    assert_eq!(
        schema.table_with_read_selector_calls.load(Ordering::SeqCst),
        0
    );

    // `VersionHintPlanner` takes precedence over `IgnoreTimeTravelPlanner`,
    // so the catalog's lack of support for table read selectors is reported
    let error = ctx
        .sql("SELECT * FROM counting.t WITH (2)")
        .await
        .expect_err("table read selectors should not be supported");
    assert_snapshot!(error.strip_backtrace(), @"This feature is not implemented: Time travel is not supported");
}
