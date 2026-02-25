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

//! This example demonstrates extending the DataFusion SQL parser to support
//! custom DDL statements, specifically `CREATE EXTERNAL CATALOG`.
//!
//! ### Custom Syntax
//! ```sql
//! CREATE EXTERNAL CATALOG my_catalog
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
use datafusion::datasource::listing_table_factory::ListingTableFactory;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::{
    parser::{DFParser, DFParserBuilder, Statement},
    sqlparser::{
        ast::{ObjectName, Value},
        keywords::Keyword,
        tokenizer::Token,
    },
};
use datafusion_common::{DFSchema, TableReference, plan_datafusion_err, plan_err};
use datafusion_expr::CreateExternalTable;
use futures::StreamExt;
use insta::assert_snapshot;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;

/// Entry point for the example.
pub async fn custom_sql_parser() -> Result<()> {
    // Use standard Parquet testing data as our "external" source.
    let base_path = datafusion::common::test_util::parquet_test_data();
    let base_path = std::path::Path::new(&base_path).canonicalize()?;

    // Make the path relative to the workspace root
    let workspace_root = workspace_root();
    let location = base_path
        .strip_prefix(&workspace_root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| base_path.to_string_lossy().to_string());

    let create_catalog_sql = format!(
        "CREATE EXTERNAL CATALOG parquet_testing
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
        .expect_err("Expected the standard parser to reject CREATE EXTERNAL CATALOG (custom DDL syntax)");

    println!("Error: {err}\n");
    assert_snapshot!(err.to_string(), @r#"SQL error: ParserError("Expected: TABLE, found: CATALOG at Line: 1, Column: 17")"#);

    // =========================================================================
    // Part 2: Custom parser handles the statement
    // =========================================================================
    println!("=== Part 2: Custom Parser ===\n");
    println!("Parsing: {}\n", create_catalog_sql.trim());

    let ctx = SessionContext::new();

    let mut parser = CustomParser::new(&create_catalog_sql)?;
    let statement = parser.parse_statement()?;
    match statement {
        CustomStatement::CreateExternalCatalog(stmt) => {
            handle_create_external_catalog(&ctx, stmt).await?;
        }
        CustomStatement::DFStatement(_) => {
            panic!("Expected CreateExternalCatalog statement");
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

/// Custom handler for the `CREATE EXTERNAL CATALOG` statement.
async fn handle_create_external_catalog(
    ctx: &SessionContext,
    stmt: CreateExternalCatalog,
) -> Result<()> {
    let factory = ListingTableFactory::new();
    let catalog = Arc::new(MemoryCatalogProvider::new());
    let schema = Arc::new(MemorySchemaProvider::new());

    // Extract options
    let mut schema_name = "public".to_string();
    let mut table_options = HashMap::new();

    for (k, v) in stmt.options {
        let val_str = match v {
            Value::SingleQuotedString(ref s) | Value::DoubleQuotedString(ref s) => {
                s.to_string()
            }
            Value::Number(ref n, _) => n.to_string(),
            Value::Boolean(b) => b.to_string(),
            _ => v.to_string(),
        };

        if k == "schema_name" {
            schema_name = val_str;
        } else {
            table_options.insert(k, val_str);
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
pub enum CustomStatement {
    /// Standard DataFusion statement
    DFStatement(Box<Statement>),
    /// Custom `CREATE EXTERNAL CATALOG` statement
    CreateExternalCatalog(CreateExternalCatalog),
}

/// Data structure for `CREATE EXTERNAL CATALOG`.
#[derive(Debug, Clone)]
pub struct CreateExternalCatalog {
    pub name: ObjectName,
    pub catalog_type: String,
    pub location: String,
    pub options: Vec<(String, Value)>,
}

impl Display for CustomStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DFStatement(s) => write!(f, "{s}"),
            Self::CreateExternalCatalog(s) => write!(f, "{s}"),
        }
    }
}

impl Display for CreateExternalCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE EXTERNAL CATALOG {} STORED AS {} LOCATION '{}'",
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

/// A parser that extends `DFParser` with custom syntax.
struct CustomParser<'a> {
    df_parser: DFParser<'a>,
}

impl<'a> CustomParser<'a> {
    fn new(sql: &'a str) -> Result<Self> {
        Ok(Self {
            df_parser: DFParserBuilder::new(sql).build()?,
        })
    }

    pub fn parse_statement(&mut self) -> Result<CustomStatement> {
        if self.is_create_external_catalog() {
            return self.parse_create_external_catalog();
        }
        Ok(CustomStatement::DFStatement(Box::new(
            self.df_parser.parse_statement()?,
        )))
    }

    fn is_create_external_catalog(&self) -> bool {
        let t1 = &self.df_parser.parser.peek_nth_token(0).token;
        let t2 = &self.df_parser.parser.peek_nth_token(1).token;
        let t3 = &self.df_parser.parser.peek_nth_token(2).token;

        matches!(t1, Token::Word(w) if w.keyword == Keyword::CREATE)
            && matches!(t2, Token::Word(w) if w.keyword == Keyword::EXTERNAL)
            && matches!(t3, Token::Word(w) if w.value.to_uppercase() == "CATALOG")
    }

    fn parse_create_external_catalog(&mut self) -> Result<CustomStatement> {
        // Consume prefix tokens: CREATE EXTERNAL CATALOG
        for _ in 0..3 {
            self.df_parser.parser.next_token();
        }

        let name = self
            .df_parser
            .parser
            .parse_object_name(false)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut catalog_type = None;
        let mut location = None;
        let mut options = vec![];

        while let Some(keyword) = self.df_parser.parser.parse_one_of_keywords(&[
            Keyword::STORED,
            Keyword::LOCATION,
            Keyword::OPTIONS,
        ]) {
            match keyword {
                Keyword::STORED => {
                    if catalog_type.is_some() {
                        return plan_err!("Duplicate STORED AS");
                    }
                    self.df_parser
                        .parser
                        .expect_keyword(Keyword::AS)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    catalog_type = Some(
                        self.df_parser
                            .parser
                            .parse_identifier()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?
                            .value,
                    );
                }
                Keyword::LOCATION => {
                    if location.is_some() {
                        return plan_err!("Duplicate LOCATION");
                    }
                    location = Some(
                        self.df_parser
                            .parser
                            .parse_literal_string()
                            .map_err(|e| DataFusionError::External(Box::new(e)))?,
                    );
                }
                Keyword::OPTIONS => {
                    if !options.is_empty() {
                        return plan_err!("Duplicate OPTIONS");
                    }
                    options = self.parse_value_options()?;
                }
                _ => unreachable!(),
            }
        }

        Ok(CustomStatement::CreateExternalCatalog(
            CreateExternalCatalog {
                name,
                catalog_type: catalog_type
                    .ok_or_else(|| plan_datafusion_err!("Missing STORED AS"))?,
                location: location
                    .ok_or_else(|| plan_datafusion_err!("Missing LOCATION"))?,
                options,
            },
        ))
    }

    /// Parse options in the form: (key [=] value, key [=] value, ...)
    fn parse_value_options(&mut self) -> Result<Vec<(String, Value)>> {
        let mut options = vec![];
        self.df_parser
            .parser
            .expect_token(&Token::LParen)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        loop {
            let key = self.df_parser.parse_option_key()?;
            // Support optional '=' between key and value
            let _ = self.df_parser.parser.consume_token(&Token::Eq);
            let value = self.df_parser.parse_option_value()?;
            options.push((key, value));

            let comma = self.df_parser.parser.consume_token(&Token::Comma);
            if self.df_parser.parser.consume_token(&Token::RParen) {
                break;
            } else if !comma {
                return plan_err!("Expected ',' or ')' in OPTIONS");
            }
        }
        Ok(options)
    }
}

/// Returns the workspace root directory (parent of datafusion-examples).
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CARGO_MANIFEST_DIR should have a parent")
        .to_path_buf()
}
