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

//! SQL unparser tests.
//! These are integration tests that run TPCH and ClickBench queries with and without an extra
//! unparse -> parse step to ensure that the unparsed SQL is valid and produces the same results
//! as the original query.

use std::fs::ReadDir;

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use datafusion::common::Result;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_sql::unparser::Unparser;
use datafusion_sql::unparser::dialect::DefaultDialect;

const BENCHMARKS_PATH_1: &str = "../../benchmarks/";
const BENCHMARKS_PATH_2: &str = "./benchmarks/";

fn iterate_queries(dir: ReadDir) -> Vec<TestQuery> {
    let mut queries = vec![];
    for entry in dir.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "sql" {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        if let Ok(mut contents) = std::fs::read_to_string(entry.path()) {
            // If the query contains ;\n it has DML statements like CREATE VIEW which the unparser doesn't support; skip it
            contents = contents.trim().to_string();
            if contents.contains(";\n") {
                println!("Skipping query with multiple statements: {}", name);
                continue;
            }
            queries.push(TestQuery {
                sql: contents,
                name,
            });
        }
    }
    queries
}

struct TestQuery {
    sql: String,
    name: String,
}

/// Collect SQL for ClickBench queries.
fn clickbench_queries() -> Vec<TestQuery> {
    let paths = [BENCHMARKS_PATH_1, BENCHMARKS_PATH_2];
    let mut queries = vec![];
    for path in paths {
        let dir = format!("{}queries/clickbench/queries/", path);
        println!("Reading ClickBench queries from {dir}");
        if let Ok(dir) = std::fs::read_dir(dir) {
            let read = iterate_queries(dir);
            println!("Found {} ClickBench queries", read.len());
            queries.extend(read);
        }
    }
    queries.sort_unstable_by_key(|q| q.name.clone());
    queries
}

/// Collect SQL for TPC-H queries.
fn tpch_queries() -> Vec<TestQuery> {
    let paths = [BENCHMARKS_PATH_1, BENCHMARKS_PATH_2];
    let mut queries = vec![];
    for path in paths {
        let dir = format!("{}queries/", path);
        println!("Reading TPC-H queries from {dir}");
        if let Ok(dir) = std::fs::read_dir(dir) {
            let read = iterate_queries(dir);
            queries.extend(read);
        }
    }
    println!("Total TPC-H queries found: {}", queries.len());
    queries.sort_unstable_by_key(|q| q.name.clone());
    queries
}

/// Create a new SessionContext for testing that has all ClickBench tables registered.
async fn clickbench_test_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "hits",
        "tests/data/clickbench_hits_10.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    // Sanity check we found the table by querying it's schema, it should not be empty
    // Otherwise if the path is wrong the tests will all fail in confusing ways
    let df = ctx.sql("SELECT * FROM hits LIMIT 1").await?;
    assert!(
        !df.schema().fields().is_empty(),
        "ClickBench 'hits' table not registered correctly"
    );
    Ok(ctx)
}

/// Create a new SessionContext for testing that has all TPC-H tables registered.
async fn tpch_test_context() -> Result<SessionContext> {
    let ctx = SessionContext::new();
    let data_dir = "tests/data/";
    // All tables have the pattern "tpch_<table_name>_small.parquet"
    for table in [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region",
        "supplier",
    ] {
        let path = format!("{data_dir}tpch_{table}_small.parquet");
        ctx.register_parquet(table, &path, ParquetReadOptions::default())
            .await?;
        // Sanity check we found the table by querying it's schema, it should not be empty
        // Otherwise if the path is wrong the tests will all fail in confusing ways
        let df = ctx.sql(&format!("SELECT * FROM {table} LIMIT 1")).await?;
        assert!(
            !df.schema().fields().is_empty(),
            "TPC-H '{table}' table not registered correctly"
        );
    }
    Ok(ctx)
}

async fn sort_batches(
    ctx: &SessionContext,
    batches: Vec<RecordBatch>,
    schema: &Schema,
) -> Result<Vec<RecordBatch>> {
    let df = ctx.read_batches(batches)?.sort(
        schema
            .fields()
            .iter()
            .map(|f| datafusion::prelude::col(f.name()).sort(true, false))
            .collect(),
    )?;
    df.collect().await
}

struct UnparsedTestCaseResult {
    original: String,
    unparsed: String,
    expected: Vec<RecordBatch>,
    actual: Vec<RecordBatch>,
}

async fn collect_results(
    ctx: &SessionContext,
    original: &str,
) -> Result<UnparsedTestCaseResult> {
    let unparser = Unparser::new(&DefaultDialect {});
    let df = ctx.sql(original).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error executing original SQL:\n{}\n\nError: {}",
            original, e,
        ))
    })?;
    let unparsed = format!(
        "{:#}",
        unparser.plan_to_sql(&df.logical_plan()).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Error unparsing SQL:\n{}\n\nError: {}",
                original, e,
            ))
        })?
    );
    let schema = df.schema().as_arrow().clone();
    let is_sorted = ctx
        .state()
        .create_physical_plan(df.logical_plan())
        .await?
        .equivalence_properties()
        .output_ordering()
        .is_some();
    let mut expected = df.collect().await?;
    let actual_df = ctx.sql(&unparsed).await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error executing unparsed SQL:\n{}\n\nError: {}",
            unparsed, e,
        ))
    })?;
    let actual_schema = actual_df.schema().as_arrow().clone();
    assert_eq!(
        schema, actual_schema,
        "Schemas do not match between original and unparsed queries"
    );
    let mut actual = actual_df.collect().await.map_err(|e| {
        datafusion::error::DataFusionError::Execution(format!(
            "Error executing unparsed SQL.Original SQL:\n{original}\n\nUnparsed SQL:\n{unparsed}\n\nError: {e}",
        ))
    })?;
    if !is_sorted {
        expected = sort_batches(ctx, expected, &schema).await?;
        actual = sort_batches(ctx, actual, &schema).await?;
    }
    Ok(UnparsedTestCaseResult {
        original: original.to_string(),
        unparsed: unparsed.to_string(),
        expected,
        actual,
    })
}

#[tokio::test]
async fn test_clickbench_unparser_roundtrip() -> Result<()> {
    let queries = clickbench_queries();
    for sql in queries {
        let ctx = clickbench_test_context().await?;
        println!("Testing ClickBench query: {}", sql.name);
        let result = collect_results(&ctx, &sql.sql).await?;
        assert_eq!(
            result.expected, result.actual,
            "Results do not match for ClickBench query.\nOriginal SQL:\n{}\n\nUnparsed SQL:\n{}\n",
            result.original, result.unparsed
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_tpch_unparser_roundtrip() -> Result<()> {
    let queries = tpch_queries();
    for sql in queries {
        let ctx = tpch_test_context().await?;
        let result = match collect_results(&ctx, &sql.sql).await {
            Ok(res) => res,
            Err(e) => {
                println!(
                    "Error processing TPC-H query {}: {}\nOriginal SQL:\n{}",
                    sql.name, e, sql.sql
                );
                return Err(e);
            }
        };
        assert_eq!(
            result.expected, result.actual,
            "Results do not match for TPC-H query.\nOriginal SQL:\n{}\n\nUnparsed SQL:\n{}\n",
            result.original, result.unparsed
        );
    }
    Ok(())
}
