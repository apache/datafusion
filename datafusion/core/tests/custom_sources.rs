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

use arrow::array::{Int32Array, PrimitiveArray, UInt64Array};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use datafusion::from_slice::FromSlice;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::scalar::ScalarValue;
use datafusion::{datasource::TableProvider, physical_plan::collect};
use datafusion::{error::Result, physical_plan::DisplayFormatType};

use datafusion::execution::context::{SessionContext, TaskContext};
use datafusion::logical_plan::{
    col, Expr, LogicalPlan, LogicalPlanBuilder, TableScan, UNNAMED_TABLE,
};
use datafusion::physical_plan::{
    project_schema, ColumnStatistics, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

use futures::stream::Stream;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use datafusion::logical_plan::plan::Projection;

//// Custom source dataframe tests ////

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
                Arc::new(Int32Array::from_slice(&[1, 10, 10, 100])),
                Arc::new(Int32Array::from_slice(&[2, 12, 12, 120])),
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
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.nb_batch > 0 {
            self.get_mut().nb_batch -= 1;
            Poll::Ready(Some(TEST_CUSTOM_RECORD_BATCH!()))
        } else {
            Poll::Ready(None)
        }
    }
}

#[async_trait]
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
    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(TestCustomRecordBatchStream { nb_batch: 1 }))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "CustomExecutionPlan: projection={:#?}", self.projection)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        let batch = TEST_CUSTOM_RECORD_BATCH!().unwrap();
        Statistics {
            is_exact: true,
            num_rows: Some(batch.num_rows()),
            total_byte_size: None,
            column_statistics: Some(
                self.projection
                    .clone()
                    .unwrap_or_else(|| (0..batch.columns().len()).collect())
                    .iter()
                    .map(|i| ColumnStatistics {
                        null_count: Some(batch.column(*i).null_count()),
                        min_value: Some(ScalarValue::Int32(aggregate::min(
                            batch
                                .column(*i)
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int32Type>>()
                                .unwrap(),
                        ))),
                        max_value: Some(ScalarValue::Int32(aggregate::max(
                            batch
                                .column(*i)
                                .as_any()
                                .downcast_ref::<PrimitiveArray<Int32Type>>()
                                .unwrap(),
                        ))),
                        ..Default::default()
                    })
                    .collect(),
            ),
        }
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

    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(CustomExecutionPlan {
            projection: projection.clone(),
        }))
    }
}

#[tokio::test]
async fn custom_source_dataframe() -> Result<()> {
    let ctx = SessionContext::new();

    let table = ctx.read_table(Arc::new(CustomTableProvider))?;
    let logical_plan = LogicalPlanBuilder::from(table.to_logical_plan()?)
        .project(vec![col("c2")])?
        .build()?;

    let optimized_plan = ctx.optimize(&logical_plan)?;
    match &optimized_plan {
        LogicalPlan::Projection(Projection { input, .. }) => match &**input {
            LogicalPlan::TableScan(TableScan {
                source,
                projected_schema,
                ..
            }) => {
                assert_eq!(source.schema().fields().len(), 2);
                assert_eq!(projected_schema.fields().len(), 1);
            }
            _ => panic!("input to projection should be TableScan"),
        },
        _ => panic!("expect optimized_plan to be projection"),
    }

    let expected = format!(
        "Projection: #{}.c2\
        \n  TableScan: {} projection=Some([1])",
        UNNAMED_TABLE, UNNAMED_TABLE
    );
    assert_eq!(format!("{:?}", optimized_plan), expected);

    let physical_plan = ctx.create_physical_plan(&optimized_plan).await?;

    assert_eq!(1, physical_plan.schema().fields().len());
    assert_eq!("c2", physical_plan.schema().field(0).name().as_str());

    let task_ctx = ctx.task_ctx();
    let batches = collect(physical_plan, task_ctx).await?;
    let origin_rec_batch = TEST_CUSTOM_RECORD_BATCH!()?;
    assert_eq!(1, batches.len());
    assert_eq!(1, batches[0].num_columns());
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

    let physical_plan = ctx
        .create_physical_plan(&df.to_logical_plan().unwrap())
        .await
        .unwrap();

    // when the optimization kicks in, the source is replaced by an EmptyExec
    assert!(
        contains_empty_exec(Arc::clone(&physical_plan)),
        "Expected aggregate_statistics optimizations missing: {:?}",
        physical_plan
    );

    let expected = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("COUNT(UInt8(1))", DataType::UInt64, false),
            Field::new("MIN(test.c1)", DataType::Int32, false),
            Field::new("MAX(test.c1)", DataType::Int32, false),
        ])),
        vec![
            Arc::new(UInt64Array::from_slice(&[4])),
            Arc::new(Int32Array::from_slice(&[1])),
            Arc::new(Int32Array::from_slice(&[100])),
        ],
    )
    .unwrap();

    let task_ctx = ctx.task_ctx();
    let actual = collect(physical_plan, task_ctx).await.unwrap();

    assert_eq!(actual.len(), 1);
    assert_eq!(format!("{:?}", actual[0]), format!("{:?}", expected));
}

fn contains_empty_exec(plan: Arc<dyn ExecutionPlan>) -> bool {
    if plan.as_any().is::<EmptyExec>() {
        true
    } else if plan.children().len() != 1 {
        false
    } else {
        contains_empty_exec(Arc::clone(&plan.children()[0]))
    }
}
