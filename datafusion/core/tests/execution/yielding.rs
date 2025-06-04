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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
use datafusion_common::JoinType;
use datafusion_execution::config::SessionConfig;
use datafusion_expr_common::operator::Operator;
use datafusion_functions_aggregate::min_max;
use datafusion_physical_expr::expressions::{binary, col, lit, Column};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::sorts::sort::SortExec;
use futures::{Stream, StreamExt};
use std::any::Any;
use std::error::Error;
use std::fmt::Formatter;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::runtime::{Handle, Runtime};
use tokio::select;

struct RangeStream {
    schema: SchemaRef,
    value_range: Range<i64>,
    batch_size: usize,
    poll_count: usize,
}

impl RecordBatchStream for RangeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RangeStream {
    type Item = common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_count += 1;

        let mut builder = Int64Array::builder(self.batch_size);
        for _ in 0..self.batch_size {
            match self.value_range.next() {
                None => break,
                Some(v) => builder.append_value(v),
            }
        }
        let array = builder.finish();

        if array.is_empty() {
            return Poll::Ready(None);
        }

        let batch = RecordBatch::try_new(self.schema(), vec![Arc::new(array)])?;
        Poll::Ready(Some(Ok(batch)))
    }
}

#[derive(Debug)]
struct RangeExec {
    value_range: Range<i64>,
    properties: PlanProperties,
}

impl RangeExec {
    fn new(column_name: &str) -> Self {
        Self::new_with_range(column_name, i64::MIN..i64::MAX)
    }

    fn new_with_range(column_name: &str, values: Range<i64>) -> Self {
        let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int64,
            false,
        )]));

        let orderings = vec![LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new(column_name, 0)),
            SortOptions::new(false, true),
        )])];
        RangeExec {
            value_range: values,
            properties: PlanProperties::new(
                EquivalenceProperties::new_with_orderings(schema.clone(), &orderings),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for RangeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "RangeExec")
    }
}

impl ExecutionPlan for RangeExec {
    fn name(&self) -> &str {
        "RangeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        context: Arc<TaskContext>,
    ) -> common::Result<SendableRecordBatchStream> {
        Ok(Box::pin(RangeStream {
            schema: self.schema(),
            value_range: self.value_range.clone(),
            batch_size: context.session_config().batch_size(),
            poll_count: 0,
        }))
    }
}

#[tokio::test]
async fn test_agg_no_grouping_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up an aggregation without grouping
    let inf = Arc::new(RangeExec::new("value"));
    let aggr = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![Arc::new(
            AggregateExprBuilder::new(
                sum::sum_udaf(),
                vec![col("value", &inf.schema())?],
            )
            .schema(inf.schema())
            .alias("sum")
            .build()?,
        )],
        vec![None],
        inf.clone(),
        inf.schema(),
    )?);

    // 3) get the stream
    let stream = physical_plan::execute_stream(aggr, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_agg_grouping_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up an aggregation with grouping
    let inf = Arc::new(RangeExec::new("value"));

    let value_col = col("value", &inf.schema())?;
    let group = binary(
        value_col.clone(),
        Operator::Divide,
        lit(1000000i64),
        &inf.schema(),
    )?;

    let aggr = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![(group, "group".to_string())], vec![], vec![]),
        vec![Arc::new(
            AggregateExprBuilder::new(sum::sum_udaf(), vec![value_col.clone()])
                .schema(inf.schema())
                .alias("sum")
                .build()?,
        )],
        vec![None],
        inf.clone(),
        inf.schema(),
    )?);

    // 3) get the stream
    let stream = physical_plan::execute_stream(aggr, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_agg_grouped_topk_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up a top-k aggregation
    let inf = Arc::new(RangeExec::new("value"));

    let value_col = col("value", &inf.schema())?;
    let group = binary(
        value_col.clone(),
        Operator::Divide,
        lit(1000000i64),
        &inf.schema(),
    )?;

    let aggr = Arc::new(
        AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new(
                vec![(group, "group".to_string())],
                vec![],
                vec![vec![false]],
            ),
            vec![Arc::new(
                AggregateExprBuilder::new(min_max::max_udaf(), vec![value_col.clone()])
                    .schema(inf.schema())
                    .alias("max")
                    .build()?,
            )],
            vec![None],
            inf.clone(),
            inf.schema(),
        )?
        .with_limit(Some(100)),
    );

    // 3) get the stream
    let stream = physical_plan::execute_stream(aggr, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_sort_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up the infinite source
    let inf = Arc::new(RangeExec::new("value"));

    // 3) set up a SortExec that will not be able to finish in time because input is very large
    let sort_expr = PhysicalSortExpr::new(
        col("value", &inf.schema())?,
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    );
    // LexOrdering is just Vec<PhysicalSortExpr>
    let lex_ordering: LexOrdering = vec![sort_expr].into();
    let sort_exec = Arc::new(SortExec::new(lex_ordering, inf.clone()));

    // 4) get the stream
    let stream = physical_plan::execute_stream(sort_exec, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_filter_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up the infinite source
    let inf = Arc::new(RangeExec::new("value"));

    // 3) set up a FilterExec that will filter out entire batches
    let filter_expr = binary(
        col("value", &inf.schema())?,
        Operator::Lt,
        lit(i64::MIN),
        &inf.schema(),
    )?;
    let filter = Arc::new(FilterExec::try_new(filter_expr, inf.clone())?);

    // 4) get the stream
    let stream = physical_plan::execute_stream(filter, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_hash_join_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up the join sources
    let inf1 = Arc::new(RangeExec::new("value1"));
    let inf2 = Arc::new(RangeExec::new("value2"));

    // 3) set up a HashJoinExec that will take a long time in the build phase
    let join = Arc::new(HashJoinExec::try_new(
        inf1.clone(),
        inf2.clone(),
        vec![(
            col("value1", &inf1.schema())?,
            col("value2", &inf2.schema())?,
        )],
        None,
        &JoinType::Left,
        None,
        PartitionMode::CollectLeft,
        true,
    )?);

    // 4) get the stream
    let stream = physical_plan::execute_stream(join, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

#[tokio::test]
async fn test_sort_merge_join_yields() -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let config = SessionConfig::new();
    // config.options_mut().execution.poll_budget = None;
    let session_ctx = SessionContext::new_with_config(config);

    // 2) set up the join sources
    let inf1 = Arc::new(RangeExec::new_with_range("value1", i64::MIN..0));
    let inf2 = Arc::new(RangeExec::new_with_range("value2", 0..i64::MAX));

    // 3) set up a SortMergeJoinExec that will take a long time skipping left side content to find
    // the first right side match
    let join = Arc::new(SortMergeJoinExec::try_new(
        inf1.clone(),
        inf2.clone(),
        vec![(
            col("value1", &inf1.schema())?,
            col("value2", &inf2.schema())?,
        )],
        None,
        JoinType::Inner,
        vec![inf1.properties.eq_properties.output_ordering().unwrap()[0].options],
        true,
    )?);

    // 4) get the stream
    let stream = physical_plan::execute_stream(join, session_ctx.task_ctx())?;

    test_stream_next_yields(stream).await
}

async fn test_stream_next_yields(
    mut stream: SendableRecordBatchStream,
) -> Result<(), Box<dyn Error>> {
    // Create an independent executor pool
    let child_runtime = Runtime::new()?;

    // Spawn a task that tries to poll the stream
    // The task returns Ready when the stream yielded with either Ready or Pending
    let join_handle = child_runtime.spawn(std::future::poll_fn(move |cx| {
        match stream.poll_next_unpin(cx) {
            Poll::Ready(_) => Poll::Ready(Poll::Ready(())),
            Poll::Pending => Poll::Ready(Poll::Pending),
        }
    }));

    let abort_handle = join_handle.abort_handle();

    // Now select on the join handle of the task running in the child executor with a timeout
    let yielded = select! {
        _ = join_handle => true,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => false
    };

    // Try to abort the poll task and shutdown the child runtime
    abort_handle.abort();
    Handle::current().spawn_blocking(move || {
        drop(child_runtime);
    });

    // Finally, check if poll_next yielded
    assert!(yielded, "Task did not yield in a timely fashion");
    Ok(())
}
