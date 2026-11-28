// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests that catalog and schema lookup failures are reported as errors,
//! and are not confused with a missing schema or table.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::prelude::SessionContext;
use datafusion_catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
use datafusion_common::{Result, exec_err};

const LOOKUP_FAILURE: &str = "remote catalog unavailable";

/// A catalog whose `broken` schema cannot be looked up. Other schemas are
/// served from an in-memory catalog.
#[derive(Debug, Default)]
struct FlakyCatalogProvider {
    inner: MemoryCatalogProvider,
}

impl CatalogProvider for FlakyCatalogProvider {
    fn schema_names(&self) -> Result<Vec<String>> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Result<Option<Arc<dyn SchemaProvider>>> {
        if name == "broken" {
            return exec_err!("{LOOKUP_FAILURE}");
        }
        self.inner.schema(name)
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        self.inner.register_schema(name, schema)
    }
}

/// A schema that cannot list its tables or check if a table exists.
#[derive(Debug)]
struct UnlistableSchemaProvider;

#[async_trait]
impl SchemaProvider for UnlistableSchemaProvider {
    fn table_names(&self) -> Result<Vec<String>> {
        exec_err!("{LOOKUP_FAILURE}")
    }

    async fn table(&self, _name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(None)
    }

    fn table_exist(&self, _name: &str) -> Result<bool> {
        exec_err!("{LOOKUP_FAILURE}")
    }
}

fn ctx_with_flaky_catalog() -> SessionContext {
    let ctx = SessionContext::new();
    let catalog = FlakyCatalogProvider::default();
    catalog
        .register_schema("public", Arc::new(MemorySchemaProvider::new()))
        .unwrap();
    ctx.register_catalog("flaky", Arc::new(catalog));
    ctx
}

async fn sql_error(ctx: &SessionContext, sql: &str) -> String {
    match ctx.sql(sql).await {
        Ok(df) => match df.collect().await {
            Ok(_) => panic!("expected `{sql}` to fail"),
            Err(e) => e.strip_backtrace(),
        },
        Err(e) => e.strip_backtrace(),
    }
}

#[tokio::test]
async fn select_reports_schema_lookup_failure() {
    let ctx = ctx_with_flaky_catalog();
    let err = sql_error(&ctx, "SELECT * FROM flaky.broken.t").await;
    assert!(err.contains(LOOKUP_FAILURE), "unexpected error: {err}");
}

#[tokio::test]
async fn select_reports_missing_schema_and_table() {
    let ctx = ctx_with_flaky_catalog();
    for sql in [
        "SELECT * FROM flaky.missing.t",
        "SELECT * FROM flaky.public.missing",
        "SELECT * FROM missing.public.t",
    ] {
        let err = sql_error(&ctx, sql).await;
        assert!(
            err.contains("not found"),
            "unexpected error for `{sql}`: {err}"
        );
        assert!(!err.contains(LOOKUP_FAILURE), "unexpected error: {err}");
    }
}

#[tokio::test]
async fn drop_reports_schema_lookup_failure() {
    let ctx = ctx_with_flaky_catalog();
    for sql in [
        "DROP TABLE flaky.broken.t",
        "DROP TABLE IF EXISTS flaky.broken.t",
        "DROP VIEW flaky.broken.v",
        "DROP VIEW IF EXISTS flaky.broken.v",
    ] {
        let err = sql_error(&ctx, sql).await;
        assert!(
            err.contains(LOOKUP_FAILURE),
            "unexpected error for `{sql}`: {err}"
        );
    }
}

#[tokio::test]
async fn drop_missing_table_and_view() -> Result<()> {
    let ctx = ctx_with_flaky_catalog();

    for (sql, expected) in [
        (
            "DROP TABLE flaky.missing.t",
            "Table 'flaky.missing.t' doesn't exist.",
        ),
        (
            "DROP TABLE flaky.public.t",
            "Table 'flaky.public.t' doesn't exist.",
        ),
        (
            "DROP VIEW flaky.missing.v",
            "View 'flaky.missing.v' doesn't exist.",
        ),
        (
            "DROP VIEW flaky.public.v",
            "View 'flaky.public.v' doesn't exist.",
        ),
    ] {
        let err = sql_error(&ctx, sql).await;
        assert!(
            err.contains(expected),
            "unexpected error for `{sql}`: {err}"
        );
    }

    for sql in [
        "DROP TABLE IF EXISTS flaky.missing.t",
        "DROP TABLE IF EXISTS flaky.public.t",
        "DROP VIEW IF EXISTS flaky.missing.v",
        "DROP VIEW IF EXISTS flaky.public.v",
    ] {
        ctx.sql(sql).await?.collect().await?;
    }
    Ok(())
}

#[test]
fn memory_catalog_drop_schema_that_cannot_list_tables() {
    let catalog = MemoryCatalogProvider::new();
    catalog
        .register_schema("unlistable", Arc::new(UnlistableSchemaProvider))
        .unwrap();

    let err = catalog
        .deregister_schema("unlistable", false)
        .expect_err("non-cascading drop should need the table list");
    assert!(
        err.to_string().contains(LOOKUP_FAILURE),
        "unexpected error: {err}"
    );
    assert!(catalog.schema("unlistable").unwrap().is_some());

    assert!(
        catalog
            .deregister_schema("unlistable", true)
            .unwrap()
            .is_some()
    );
    assert!(catalog.schema("unlistable").unwrap().is_none());
}
