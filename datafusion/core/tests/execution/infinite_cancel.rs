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

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_schema::SortOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::functions_aggregate::sum;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;
use datafusion::{common, physical_plan};
use datafusion_common::config::ConfigOptions;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_optimizer::wrap_leaves_cancellation::WrapLeaves;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::sorts::sort::SortExec;
use futures::{Stream, StreamExt};
use std::any::Any;
use std::error::Error;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::select;

struct InfiniteStream {
    batch: RecordBatch,
    poll_count: usize,
}

impl RecordBatchStream for InfiniteStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

impl Stream for InfiniteStream {
    type Item = common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_count += 1;
        Poll::Ready(Some(Ok(self.batch.clone())))
    }
}

#[derive(Debug)]
struct InfiniteExec {
    batch: RecordBatch,
    properties: PlanProperties,
}

impl InfiniteExec {
    fn new(batch: &RecordBatch) -> Self {
        InfiniteExec {
            batch: batch.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(batch.schema().clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            ),
        }
    }
}

impl DisplayAs for InfiniteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "infinite")
    }
}

impl ExecutionPlan for InfiniteExec {
    fn name(&self) -> &str {
        "infinite"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(InfiniteStream {
            batch: self.batch.clone(),
            poll_count: 0,
        }))
    }
}

#[tokio::test]
async fn test_infinite_agg_cancel() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let session_ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(Fields::from(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )])));
    let mut builder = Int64Array::builder(8192);
    for v in 0..8192 {
        builder.append_value(v);
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])?;

    // 2) set up the infinite source + aggregation
    let inf = Arc::new(InfiniteExec::new(&batch));
    let aggr = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![Arc::new(
            AggregateExprBuilder::new(
                sum::sum_udaf(),
                vec![Arc::new(Column::new_with_schema("value", &schema)?)],
            )
            .schema(inf.schema())
            .alias("sum")
            .build()?,
        )],
        vec![None],
        inf.clone(),
        inf.schema(),
    )?);

    // 3) optimize the plan with WrapLeaves to auto-insert Yield
    let optimized = WrapLeaves::new().optimize(aggr.clone(), &ConfigOptions::new())?;

    // 4) get the stream
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;

    // 5) drive the stream inline in select!
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };

    assert!(result.is_none(), "Expected timeout, but got a result");
    Ok(())
}

#[tokio::test]
async fn test_infinite_sort_cancel() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let session_ctx = SessionContext::new();
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut builder = Int64Array::builder(8192);
    for v in 0..8192 {
        builder.append_value(v);
    }
    let array = builder.finish();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array)])?;

    // 2) set up the infinite source
    let inf = Arc::new(InfiniteExec::new(&batch));

    // 3) set up a SortExec that will never finish because input is infinite
    let sort_options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let sort_expr = PhysicalSortExpr::new(
        Arc::new(Column::new_with_schema("value", &schema)?),
        sort_options,
    );
    // LexOrdering is just Vec<PhysicalSortExpr>
    let lex_ordering: datafusion::physical_expr::LexOrdering = vec![sort_expr].into();
    let sort_exec = Arc::new(SortExec::new(lex_ordering, inf.clone()));

    // 4) optimize the plan with WrapLeaves to auto-insert Yield
    let optimized =
        WrapLeaves::new().optimize(sort_exec.clone(), &ConfigOptions::new())?;

    // 5) get the stream
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;

    // 6) drive the stream inline in select!
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };

    assert!(
        result.is_none(),
        "Expected timeout for sort, but got a result"
    );
    Ok(())
}
