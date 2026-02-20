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

//! See `main.rs` for how to run it.

use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;

use arrow::csv::ReaderBuilder;
use arrow::csv::reader::Format;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionArgs, TableFunctionImpl};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_examples::utils::datasets::ExampleDataset;

// To define your own table function, you only need to do the following 3 things:
// 1. Implement your own [`TableProvider`]
// 2. Implement your own [`TableFunctionImpl`] and return your [`TableProvider`]
// 3. Register the function using [`SessionContext::register_udtf`]

/// This example demonstrates how to register a TableFunction
pub async fn simple_udtf() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    // register the table function that will be called in SQL statements by `read_csv`
    ctx.register_udtf("read_csv", Arc::new(LocalCsvTableFunc {}));

    let dataset = ExampleDataset::Cars;

    // Pass 2 arguments, read csv with at most 2 rows (simplify logic makes 1+1 --> 2)
    let df = ctx
        .sql(
            format!("SELECT * FROM read_csv('{}', 1 + 1);", dataset.path_str()?).as_str(),
        )
        .await?;
    df.show().await?;

    // just run, return all rows
    let df = ctx
        .sql(format!("SELECT * FROM read_csv('{}');", dataset.path_str()?).as_str())
        .await?;
    df.show().await?;

    Ok(())
}

/// Table Function that mimics the [`read_csv`] function in DuckDB.
///
/// Usage: `read_csv(filename, [limit])`
///
/// [`read_csv`]: https://duckdb.org/docs/data/csv/overview.html
#[derive(Debug)]
struct LocalCsvTable {
    schema: SchemaRef,
    limit: Option<usize>,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl TableProvider for LocalCsvTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let batches = if let Some(max_return_lines) = self.limit {
            // get max return rows from self.batches
            let mut batches = vec![];
            let mut lines = 0;
            for batch in &self.batches {
                let batch_lines = batch.num_rows();
                if lines + batch_lines > max_return_lines {
                    let batch_lines = max_return_lines - lines;
                    batches.push(batch.slice(0, batch_lines));
                    break;
                } else {
                    batches.push(batch.clone());
                    lines += batch_lines;
                }
            }
            batches
        } else {
            self.batches.clone()
        };
        Ok(MemorySourceConfig::try_new_exec(
            &[batches],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

#[derive(Debug)]
struct LocalCsvTableFunc {}

impl TableFunctionImpl for LocalCsvTableFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.args;
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)), _)) = exprs.first() else {
            return plan_err!("read_csv requires at least one string argument");
        };

        let limit = exprs
            .get(1)
            .map(|expr| {
                // try to simplify the expression, so 1+2 becomes 3, for example
                let info = SimplifyContext::default();
                let expr = ExprSimplifier::new(info).simplify(expr.clone())?;

                if let Expr::Literal(ScalarValue::Int64(Some(limit)), _) = expr {
                    Ok(limit as usize)
                } else {
                    plan_err!("Limit must be an integer")
                }
            })
            .transpose()?;

        let (schema, batches) = read_csv_batches(path)?;

        let table = LocalCsvTable {
            schema,
            limit,
            batches,
        };
        Ok(Arc::new(table))
    }
}

fn read_csv_batches(csv_path: impl AsRef<Path>) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let mut file = File::open(csv_path)?;
    let (schema, _) = Format::default().infer_schema(&mut file, None)?;
    file.rewind()?;

    let reader = ReaderBuilder::new(Arc::new(schema.clone()))
        .with_header(true)
        .build(file)?;
    let mut batches = vec![];
    for batch in reader {
        batches.push(batch?);
    }
    let schema = Arc::new(schema);
    Ok((schema, batches))
}
