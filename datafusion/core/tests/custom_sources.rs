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

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Int32Array, Int64Array};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionContext, SessionState, TaskContext};
use datafusion::logical_expr::{
    col, Expr, LogicalPlan, LogicalPlanBuilder, TableScan, UNNAMED_TABLE,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::{
    collect, ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::project_schema;
use datafusion_common::stats::Precision;

use async_trait::async_trait;
use datafusion_physical_plan::memory::MemoryExec;
use futures::stream::Stream;

/// Also run all tests that are found in the `custom_sources_cases` directory
mod custom_sources_cases;

//--- Custom source dataframe tests ---//

struct CustomTableProvider;
#[derive(Debug, Clone)]
struct CustomExecutionPlan {
    projection: Option<Vec<usize>>,
}
struct TestCustomRecordBatchStream {
    /// the nb of batches of TEST_CUSTOM_RECORD_BATCH generated
    nb_batch: i32,
}
macro_rules! TEST_CUSTOM_SCHEMA_REF {
    () => {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]))
    };
}
macro_rules! TEST_CUSTOM_RECORD_BATCH {
    () => {
        RecordBatch::try_new(
            TEST_CUSTOM_SCHEMA_REF!(),
            vec![
                Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
                Arc::new(Int32Array::from(vec![2, 12, 12, 120])),
            ],
        )
    };
}

impl RecordBatchStream for TestCustomRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        TEST_CUSTOM_SCHEMA_REF!()
    }
}

impl Stream for TestCustomRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.nb_batch > 0 {
            self.get_mut().nb_batch -= 1;
            Poll::Ready(Some(TEST_CUSTOM_RECORD_BATCH!().map_err(Into::into)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl DisplayAs for CustomExecutionPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CustomExecutionPlan: projection={:#?}", self.projection)
            }
        }
    }
}

impl ExecutionPlan for CustomExecutionPlan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = TEST_CUSTOM_SCHEMA_REF!();
        project_schema(&schema, self.projection.as_ref()).expect("projected schema")
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TestCustomRecordBatchStream { nb_batch: 1 }))
    }

    fn statistics(&self) -> Result<Statistics> {
        let batch = TEST_CUSTOM_RECORD_BATCH!().unwrap();
        Ok(Statistics {
            num_rows: Precision::Exact(batch.num_rows()),
            total_byte_size: Precision::Absent,
            column_statistics: self
                .projection
                .clone()
                .unwrap_or_else(|| (0..batch.columns().len()).collect())
                .iter()
                .map(|i| ColumnStatistics {
                    null_count: Precision::Exact(batch.column(*i).null_count()),
                    min_value: Precision::Exact(ScalarValue::Int32(aggregate::min(
                        as_primitive_array::<Int32Type>(batch.column(*i)).unwrap(),
                    ))),
                    max_value: Precision::Exact(ScalarValue::Int32(aggregate::max(
                        as_primitive_array::<Int32Type>(batch.column(*i)).unwrap(),
                    ))),
                    ..Default::default()
                })
                .collect(),
        })
    }
}

#[async_trait]
impl TableProvider for CustomTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TEST_CUSTOM_SCHEMA_REF!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExecutionPlan {
            projection: projection.cloned(),
        }))
    }
}

#[tokio::test]
async fn custom_source_dataframe() -> Result<()> {
    let ctx = SessionContext::new();

    let table = ctx.read_table(Arc::new(CustomTableProvider))?;
    let (state, plan) = table.into_parts();
    let logical_plan = LogicalPlanBuilder::from(plan)
        .project(vec![col("c2")])?
        .build()?;

    let optimized_plan = state.optimize(&logical_plan)?;
    match &optimized_plan {
        LogicalPlan::TableScan(TableScan {
            source,
            projected_schema,
            ..
        }) => {
            assert_eq!(source.schema().fields().len(), 2);
            assert_eq!(projected_schema.fields().len(), 1);
        }
        _ => panic!("input to projection should be TableScan"),
    }

    let expected = format!("TableScan: {UNNAMED_TABLE} projection=[c2]");
    assert_eq!(format!("{optimized_plan:?}"), expected);

    let physical_plan = state.create_physical_plan(&optimized_plan).await?;

    assert_eq!(1, physical_plan.schema().fields().len());
    assert_eq!("c2", physical_plan.schema().field(0).name().as_str());

    let batches = collect(physical_plan, state.task_ctx()).await?;
    let origin_rec_batch = TEST_CUSTOM_RECORD_BATCH!()?;
    assert_eq!(1, batches.len());
    assert_eq!(2, batches[0].num_columns());
    assert_eq!(origin_rec_batch.num_rows(), batches[0].num_rows());

    Ok(())
}

#[tokio::test]
async fn optimizers_catch_all_statistics() {
    let ctx = SessionContext::new();
    ctx.register_table("test", Arc::new(CustomTableProvider))
        .unwrap();

    let df = ctx
        .sql("SELECT count(*), min(c1), max(c1) from test")
        .await
        .unwrap();

    let physical_plan = df.create_physical_plan().await.unwrap();

    // when the optimization kicks in, the source is replaced by an EmptyExec
    assert!(
        contains_memory_exec(Arc::clone(&physical_plan)),
        "Expected aggregate_statistics optimizations missing: {physical_plan:?}"
    );

    let expected = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("COUNT(*)", DataType::Int64, false),
            Field::new("MIN(test.c1)", DataType::Int32, false),
            Field::new("MAX(test.c1)", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![4])),
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();

    let task_ctx = ctx.task_ctx();
    let actual = collect(physical_plan, task_ctx).await.unwrap();

    assert_eq!(actual.len(), 1);
    assert_eq!(format!("{:?}", actual[0]), format!("{expected:?}"));
}

fn contains_memory_exec(plan: Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().is::<MemoryExec>() {
        true
    } else if plan.children().len() != 1 {
        false
    } else {
        contains_memory_exec(Arc::clone(&plan.children()[0]))
    }
}
