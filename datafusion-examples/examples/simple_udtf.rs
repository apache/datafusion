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

use arrow::csv::reader::Format;
use arrow::csv::ReaderBuilder;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::function::TableFunctionImpl;
use datafusion::datasource::streaming::StreamingTable;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::{EmptyRelation, Expr, LogicalPlan, Projection, TableType};
use std::fs::File;
use std::io::Seek;
use std::path::Path;
use std::sync::Arc;

// To define your own table function, you only need to do the following 3 things:
// 1. Implement your own TableProvider
// 2. Implement your own TableFunctionImpl and return your TableProvider
// 3. Register the function using ctx.register_udtf

/// This example demonstrates how to register a TableFunction
#[tokio::main]
async fn main() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    ctx.register_udtf("read_csv", Arc::new(LocalCsvTableFunc {}));
    ctx.register_udtf("read_csv_stream", Arc::new(LocalStreamCsvTable {}));

    let testdata = datafusion::test_util::arrow_test_data();
    let csv_file = format!("{testdata}/csv/aggregate_test_100.csv");

    // run it with println now()
    let df = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}', now());").as_str())
        .await?;
    df.show().await?;

    // just run
    let df = ctx
        .sql(format!("SELECT * FROM read_csv('{csv_file}');").as_str())
        .await?;
    df.show().await?;

    // stream csv table
    let df2 = ctx
        .sql(format!("SELECT * FROM read_csv_stream('{csv_file}');").as_str())
        .await?;
    df2.show().await?;

    Ok(())
}

// Option1: (full implmentation of a TableProvider)
struct LocalCsvTable {
    schema: SchemaRef,
    exprs: Vec<Expr>,
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
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.exprs.is_empty() {
            self.interpreter_expr(state).await?;
        }
        Ok(Arc::new(MemoryExec::try_new(
            &[self.batches.clone()],
            TableProvider::schema(self),
            projection.cloned(),
        )?))
    }
}

impl LocalCsvTable {
    // TODO(veeupup): maybe we can make interpreter Expr this more simpler for users
    // TODO(veeupup): maybe we can support more type of exprs
    async fn interpreter_expr(&self, state: &SessionState) -> Result<()> {
        use datafusion::logical_expr::expr_rewriter::normalize_col;
        use datafusion::logical_expr::utils::columnize_expr;
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        });
        let logical_plan = Projection::try_new(
            vec![columnize_expr(
                normalize_col(self.exprs[0].clone(), &plan)?,
                plan.schema(),
            )],
            Arc::new(plan),
        )
        .map(LogicalPlan::Projection)?;
        let rbs = collect(
            state.create_physical_plan(&logical_plan).await?,
            Arc::new(TaskContext::from(state)),
        )
        .await?;
        println!("time now: {:?}", rbs[0].column(0));
        Ok(())
    }
}

struct LocalCsvTableFunc {}

impl TableFunctionImpl for LocalCsvTableFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let mut new_exprs = vec![];
        let mut filepath = String::new();
        for expr in exprs {
            match expr {
                Expr::Literal(ScalarValue::Utf8(Some(ref path))) => {
                    filepath = path.clone()
                }
                expr => new_exprs.push(expr.clone()),
            }
        }
        let (schema, batches) = read_csv_batches(filepath)?;
        let table = LocalCsvTable {
            schema,
            exprs: new_exprs.clone(),
            batches,
        };
        Ok(Arc::new(table))
    }
}

// Option2: (use StreamingTable to make it simpler)
//      Implement PartitionStream and Use StreamTable to return streaming table
impl PartitionStream for LocalCsvTable {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(
        &self,
        _ctx: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::physical_plan::SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            // You can even read data from network or else anywhere, using async is also ok
            // In Fact, you can even implement your own SendableRecordBatchStream
            // by implementing Stream<Item = ArrowResult<RecordBatch>> + Send + Sync + 'static
            futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
        ))
    }
}

struct LocalStreamCsvTable {}

impl TableFunctionImpl for LocalStreamCsvTable {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filepath = match args[0] {
            Expr::Literal(ScalarValue::Utf8(Some(ref path))) => path.clone(),
            _ => unimplemented!(),
        };
        let (schema, batches) = read_csv_batches(filepath)?;
        let stream = LocalCsvTable {
            schema: schema.clone(),
            batches,
            exprs: vec![],
        };
        let table = StreamingTable::try_new(schema, vec![Arc::new(stream)])?;
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
    for bacth in reader {
        batches.push(bacth?);
    }
    let schema = Arc::new(schema);
    Ok((schema, batches))
}
