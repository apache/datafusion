// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! This example demonstrates extending the DataFusion SQL parser to support
//! custom DDL statements, specifically `CREATE FOREIGN CATALOG`.
//!
//! Note: DataFusion supports `CREATE EXTERNAL CATALOG` out-of-the-box making use of
//! [`CatalogProviderFactory`](datafusion::catalog::CatalogProviderFactory). This example
//! is a partial reimplementation of the existing functionality to demonstrate how to extend the
//! SQL parser.
//!
//! DataFusion parses SQL with [`DFParser`], which reads DataFusion's own
//! statements (such as `CREATE EXTERNAL TABLE`) and leaves the rest to
//! `sqlparser` with DataFusion's dialect,
//! [`DataFusion`](datafusion::sql::dialect::DataFusion). This example does the
//! same one level up: it reads `CREATE FOREIGN CATALOG` with `sqlparser`'s
//! [`Parser`] and DataFusion's dialect, and leaves every other statement to
//! [`DFParser`].
//!
//! ### Custom Syntax
//! ```sql
//! CREATE FOREIGN CATALOG my_catalog
//! STORED AS ICEBERG
//! LOCATION 's3://my-bucket/warehouse/'
//! OPTIONS (
//!   'region' = 'us-west-2'
//! );
//! ```
//!
//! Note: For the purpose of this example, we use `local://workspace/` to
//! automatically discover and register files from the project's test data.

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
    TableProviderFactory,
};
use datafusion::common::{DFSchema, TableReference, plan_datafusion_err, plan_err};
use datafusion::datasource::listing_table_factory::ListingTableFactory;
use datafusion::error::Result;
use datafusion::logical_expr::CreateExternalTable;
use datafusion::prelude::SessionContext;
use datafusion::sql::{
    ast::ObjectName,
    dialect::DataFusion,
    parser::{DFParser, Statement},
    sqlparser::{grammar, parser::Parser},
};
use futures::StreamExt;
use insta::assert_snapshot;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

/// Entry point for the example.
pub async fn custom_sql_parser() -> Result<()> {
    // Use standard Parquet testing data as our "foreign" source.
    let base_path = datafusion::common::test_util::parquet_test_data();
    let base_path = std::path::Path::new(&base_path).canonicalize()?;

    // Make the path relative to the workspace root
    let workspace_root = workspace_root();
    let location = base_path
        .strip_prefix(&workspace_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| base_path.to_string_lossy().to_string());

    let create_catalog_sql = format!(
        "CREATE FOREIGN CATALOG parquet_testing
         STORED AS parquet
         LOCATION 'local://workspace/{location}'
         OPTIONS (
           'schema_name' = 'staged_data',
           'format.pruning' = 'true'
         )"
    );

    // =========================================================================
    // Part 1: Standard DataFusion parser rejects the custom DDL
    // =========================================================================
    println!("=== Part 1: Standard DataFusion Parser ===\n");
    println!("Parsing: {}\n", create_catalog_sql.trim());

    let ctx_standard = SessionContext::new();
    let err = ctx_standard
        .sql(&create_catalog_sql)
        .await
        .expect_err("Expected the standard parser to reject CREATE FOREIGN CATALOG (custom DDL syntax)");

    println!("Error: {err}\n");
    assert_snapshot!(err.to_string(), @"SQL error: expected an object type after CREATE, found FOREIGN at Line: 1, Column: 8");

    // =========================================================================
    // Part 2: Custom parser handles the statement
    // =========================================================================
    println!("=== Part 2: Custom Parser ===\n");
    println!("Parsing: {}\n", create_catalog_sql.trim());

    let ctx = SessionContext::new();

    let statement = parse_custom_statement(&create_catalog_sql)?;
    match statement {
        CustomStatement::CreateForeignCatalog(stmt) => {
            handle_create_foreign_catalog(&ctx, stmt).await?;
        }
        CustomStatement::DFStatement(_) => {
            panic!("Expected CreateForeignCatalog statement");
        }
    }

    // Query a table from the registered catalog
    let query_sql = "SELECT id, bool_col, tinyint_col FROM parquet_testing.staged_data.alltypes_plain LIMIT 5";
    println!("Executing: {query_sql}\n");

    let results = execute_sql(&ctx, query_sql).await?;
    println!("{results}");
    assert_snapshot!(results, @r"
    +----+----------+-------------+
    | id | bool_col | tinyint_col |
    +----+----------+-------------+
    | 4  | true     | 0           |
    | 5  | false    | 1           |
    | 6  | true     | 0           |
    | 7  | false    | 1           |
    | 2  | true     | 0           |
    +----+----------+-------------+
    ");

    Ok(())
}

/// Execute SQL and return formatted results.
async fn execute_sql(ctx: &SessionContext, sql: &str) -> Result<String> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(arrow::util::pretty::pretty_format_batches(&batches)?.to_string())
}

/// Custom handler for the `CREATE FOREIGN CATALOG` statement.
async fn handle_create_foreign_catalog(
    ctx: &SessionContext,
    stmt: CreateForeignCatalog<'_>,
) -> Result<()> {
    let factory = ListingTableFactory::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    let schema = Arc::new(MemorySchemaProvider::new());

    // Extract options
    let mut schema_name = "public".to_string();
    let mut table_options = HashMap::new();

    for (k, v) in stmt.options {
        if k == "schema_name" {
            schema_name = v;
        } else {
            table_options.insert(k, v);
        }
    }

    println!("  Target Catalog: {}", stmt.name);
    println!("  Data Location: {}", stmt.location);
    println!("  Resolved Schema: {schema_name}");

    // Register a local object store rooted at the workspace root.
    // We use a specific authority 'workspace' to ensure consistent resolution.
    let store = Arc::new(LocalFileSystem::new_with_prefix(workspace_root())?);
    let store_url = url::Url::parse("local://workspace").unwrap();
    ctx.register_object_store(&store_url, Arc::clone(&store) as _);

    let target_ext = format!(".{}", stmt.catalog_type.to_lowercase());

    // For 'local://workspace/parquet-testing/data', the path is 'parquet-testing/data'.
    let path_str = stmt
        .location
        .strip_prefix("local://workspace/")
        .unwrap_or(&stmt.location);
    let prefix = object_store::path::Path::from(path_str);

    // Discover data files using the ObjectStore API
    let mut table_count = 0;
    let mut list_stream = store.list(Some(&prefix));

    while let Some(meta) = list_stream.next().await {
        let meta = meta?;
        let path = &meta.location;

        if path.as_ref().ends_with(&target_ext) {
            let name = std::path::Path::new(path.as_ref())
                .file_stem()
                .unwrap()
                .to_string_lossy()
                .to_string();

            let table_url = format!("local://workspace/{path}");

            let cmd = CreateExternalTable::builder(
                TableReference::bare(name.clone()),
                table_url,
                stmt.catalog_type.clone(),
                Arc::new(DFSchema::empty()),
            )
            .with_options(table_options.clone())
            .build();

            match factory.create(&ctx.state(), &cmd).await {
                Ok(table) => {
                    schema.register_table(name, table)?;
                    table_count += 1;
                }
                Err(e) => {
                    eprintln!("Failed to create table {name}: {e}");
                }
            }
        }
    }
    println!("  Registered {table_count} tables into schema: {schema_name}");

    catalog.register_schema(&schema_name, schema)?;
    ctx.register_catalog(stmt.name.to_string(), catalog);

    Ok(())
}

/// Possible statements returned by our custom parser.
#[derive(Debug, Clone)]
pub enum CustomStatement<'a> {
    /// Standard DataFusion statement
    DFStatement(Box<Statement<'a>>),
    /// Custom `CREATE FOREIGN CATALOG` statement
    CreateForeignCatalog(CreateForeignCatalog<'a>),
}

/// Data structure for `CREATE FOREIGN CATALOG`.
///
/// Like `sqlparser`'s syntax tree, it borrows from the SQL text.
#[derive(Debug, Clone)]
pub struct CreateForeignCatalog<'a> {
    pub name: ObjectName<'a>,
    pub catalog_type: String,
    pub location: String,
    pub options: Vec<(String, String)>,
}

impl Display for CustomStatement<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DFStatement(s) => write!(f, "{s}"),
            Self::CreateForeignCatalog(s) => write!(f, "{s}"),
        }
    }
}

impl Display for CreateForeignCatalog<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE FOREIGN CATALOG {} STORED AS {} LOCATION '{}'",
            self.name, self.catalog_type, self.location
        )?;
        if !self.options.is_empty() {
            write!(f, " OPTIONS (")?;
            for (i, (k, v)) in self.options.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "'{k}' = '{v}'")?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

/// Parses one statement, which may be `CREATE FOREIGN CATALOG`.
///
/// `CREATE FOREIGN CATALOG` is read with `sqlparser`'s [`Parser`] and
/// DataFusion's dialect, so names and strings follow DataFusion's rules. Any
/// other statement goes to [`DFParser`].
fn parse_custom_statement(sql: &str) -> Result<CustomStatement<'_>> {
    let mut p = Parser::<DataFusion>::new(sql)?;
    if p.at_keyword("CREATE")
        && p.peek_nth(1).is_word("FOREIGN")
        && p.peek_nth(2).is_word("CATALOG")
    {
        p.expect_keywords(&["CREATE", "FOREIGN", "CATALOG"])?;
        let statement = parse_create_foreign_catalog(&mut p)?;
        p.eat_op(";");
        p.expect_eof()?;
        return Ok(CustomStatement::CreateForeignCatalog(statement));
    }

    let mut statements = DFParser::parse_sql(sql)?;
    match (statements.pop_front(), statements.is_empty()) {
        (Some(statement), true) => Ok(CustomStatement::DFStatement(Box::new(statement))),
        _ => plan_err!("Expected exactly one statement"),
    }
}

/// Parses a `CREATE FOREIGN CATALOG` statement, after its keywords.
fn parse_create_foreign_catalog<'a>(
    p: &mut Parser<'a, DataFusion>,
) -> Result<CreateForeignCatalog<'a>> {
    let name = grammar::parse_object_name(p)?;

    let mut catalog_type = None;
    let mut location = None;
    let mut options = vec![];

    loop {
        if p.eat_keyword("STORED") {
            if catalog_type.is_some() {
                return plan_err!("Duplicate STORED AS");
            }
            p.expect_keyword("AS")?;
            catalog_type = Some(p.parse_ident()?.value().into_owned());
        } else if p.eat_keyword("LOCATION") {
            if location.is_some() {
                return plan_err!("Duplicate LOCATION");
            }
            location = Some(grammar::parse_string(p)?.value().into_owned());
        } else if p.eat_keyword("OPTIONS") {
            if !options.is_empty() {
                return plan_err!("Duplicate OPTIONS");
            }
            options = parse_options(p)?;
        } else {
            break;
        }
    }

    Ok(CreateForeignCatalog {
        name,
        catalog_type: catalog_type
            .ok_or_else(|| plan_datafusion_err!("Missing STORED AS"))?,
        location: location.ok_or_else(|| plan_datafusion_err!("Missing LOCATION"))?,
        options,
    })
}

/// Parses options in the form `('key' [=] 'value', ...)`.
fn parse_options(p: &mut Parser<'_, DataFusion>) -> Result<Vec<(String, String)>> {
    p.expect_op("(")?;
    let mut options = vec![];
    loop {
        let key = grammar::parse_string(p)?.value().into_owned();
        // Support optional '=' between key and value
        p.eat_op("=");
        let value = grammar::parse_string(p)?.value().into_owned();
        options.push((key, value));

        let comma = p.eat_op(",");
        if p.eat_op(")") {
            break;
        } else if !comma {
            return Err(p.expected("',' or ')' in OPTIONS").into());
        }
    }
    Ok(options)
}

/// Returns the workspace root directory (parent of datafusion-examples).
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CARGO_MANIFEST_DIR should have a parent")
        .to_path_buf()
}
