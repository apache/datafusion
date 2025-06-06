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
use std::error::Error;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

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
use datafusion_common::{JoinType, ScalarValue};
use datafusion_expr_common::operator::Operator::Gt;
use datafusion_physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_optimizer::wrap_leaves_cancellation::WrapLeaves;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::InterleaveExec;

use futures::{Stream, StreamExt};
use rstest::rstest;
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
    fn new(batch: RecordBatch, pretend_finite: bool) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(batch.schema().clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            if pretend_finite {
                Boundedness::Bounded
            } else {
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                }
            },
        );
        InfiniteExec { batch, properties }
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

#[rstest]
#[tokio::test]
async fn test_infinite_agg_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
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
    let inf = Arc::new(InfiniteExec::new(batch, pretend_finite));
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
        inf,
        schema,
    )?);

    // 3) optimize the plan with WrapLeaves to auto-insert Yield
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(aggr, &config)?;

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

#[rstest]
#[tokio::test]
async fn test_infinite_sort_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) build session & schema & sample batch
    let session_ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
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
    let inf = Arc::new(InfiniteExec::new(batch, pretend_finite));

    // 3) set up a SortExec that will never finish because input is infinite
    let sort_options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let sort_expr = PhysicalSortExpr::new(
        Arc::new(Column::new_with_schema("value", &schema)?),
        sort_options,
    );
    let lex_ordering: datafusion::physical_expr::LexOrdering = vec![sort_expr].into();
    let sort_exec = Arc::new(SortExec::new(lex_ordering, inf));

    // 4) optimize the plan with WrapLeaves to auto-insert Yield
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(sort_exec, &config)?;

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

#[rstest]
#[tokio::test]
#[ignore]
async fn test_infinite_interleave_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Build session, schema, and a sample batch.
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

    // 2) Create N infinite sources, each filtered by a different predicate.
    //    That way, the InterleaveExec will have multiple children.
    let mut infinite_children = vec![];
    // Use 32 distinct thresholds (each >0 and <8 192) to force 32 infinite inputs
    let thresholds = (0..32).map(|i| 8_192 - 1 - (i * 256) as i64);

    for thr in thresholds {
        // 2a) One infinite exec:
        let inf = Arc::new(InfiniteExec::new(batch.clone(), pretend_finite));

        // 2b) Apply a FilterExec: “value > thr”.
        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(thr)))),
        ));
        let filtered = Arc::new(FilterExec::try_new(filter_expr, inf)?);

        // 2c) Wrap in CoalesceBatchesExec so the upstream yields are batched.
        let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8192));

        // 2d) Now repartition so that all children share identical Hash partitioning
        //     on “value” into 1 bucket. This is required for InterleaveExec::try_new.
        let exprs = vec![Arc::new(Column::new_with_schema("value", &schema)?) as _];
        let partitioning = Partitioning::Hash(exprs, 1);
        let hashed = Arc::new(RepartitionExec::try_new(coalesced, partitioning)?);

        infinite_children.push(hashed as _);
    }

    // 3) Build an InterleaveExec over all N children.
    //    Since each child now has Partitioning::Hash([col "value"], 1), InterleaveExec::try_new succeeds.
    let interleave = Arc::new(InterleaveExec::try_new(infinite_children)?);

    // 5) WrapLeaves will automatically insert YieldStreams beneath each “infinite” leaf.
    //    That way, each InfiniteExec (through the FilterExec/CoalesceBatchesExec/RepartitionExec chain)
    //    yields to the runtime periodically instead of spinning CPU.
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(interleave, &config)?;

    // 6) Execute the stream. Because AggregateExec(mode=Single) only emits a final batch
    //    after all inputs finish—and those inputs are infinite—we expect no output
    //    within 1 second (timeout → None).
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };

    assert!(
        result.is_none(),
        "Expected no output for aggregate over infinite interleave, but got some batch"
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_infinite_interleave_agg_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Build session, schema, and a sample batch.
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

    // 2) Create N infinite sources, each filtered by a different predicate.
    //    That way, the InterleaveExec will have multiple children.
    let mut infinite_children = vec![];
    // Use 32 distinct thresholds (each >0 and <8 192) to force 32 infinite inputs
    let thresholds = (0..32).map(|i| 8_192 - 1 - (i * 256) as i64);

    for thr in thresholds {
        // 2a) One infinite exec:
        let inf = Arc::new(InfiniteExec::new(batch.clone(), pretend_finite));

        // 2b) Apply a FilterExec: “value > thr”.
        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(thr)))),
        ));
        let filtered = Arc::new(FilterExec::try_new(filter_expr, inf)?);

        // 2c) Wrap in CoalesceBatchesExec so the upstream yields are batched.
        let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8192));

        // 2d) Now repartition so that all children share identical Hash partitioning
        //     on “value” into 1 bucket. This is required for InterleaveExec::try_new.
        let exprs = vec![Arc::new(Column::new_with_schema("value", &schema)?) as _];
        let partitioning = Partitioning::Hash(exprs, 1);
        let hashed = Arc::new(RepartitionExec::try_new(coalesced, partitioning)?);

        infinite_children.push(hashed as _);
    }

    // 3) Build an InterleaveExec over all N children.
    //    Since each child now has Partitioning::Hash([col "value"], 1), InterleaveExec::try_new succeeds.
    let interleave = Arc::new(InterleaveExec::try_new(infinite_children)?);
    let interleave_schema = interleave.schema();

    // 4) Build a global AggregateExec that sums “value” over all rows.
    //    Because we use `AggregateMode::Single` with no GROUP BY columns, this plan will
    //    only produce one “final” row once all inputs finish. But our inputs never finish,
    //    so we should never get any output.
    let aggregate_expr = AggregateExprBuilder::new(
        sum::sum_udaf(),
        vec![Arc::new(Column::new_with_schema("value", &schema)?)],
    )
    .schema(interleave_schema.clone())
    .alias("total")
    .build()?;

    let aggr = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(
            vec![], // no GROUP BY columns
            vec![], // no GROUP BY expressions
            vec![], // no GROUP BY physical expressions
        ),
        vec![Arc::new(aggregate_expr)],
        vec![None], // no “distinct” flags
        interleave,
        interleave_schema,
    )?);

    // 5) WrapLeaves will automatically insert YieldStreams beneath each “infinite” leaf.
    //    That way, each InfiniteExec (through the FilterExec/CoalesceBatchesExec/RepartitionExec chain)
    //    yields to the runtime periodically instead of spinning CPU.
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(aggr, &config)?;

    // 6) Execute the stream. Because AggregateExec(mode=Single) only emits a final batch
    //    after all inputs finish—and those inputs are infinite—we expect no output
    //    within 1 second (timeout → None).
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };

    assert!(
        result.is_none(),
        "Expected no output for aggregate over infinite interleave, but got some batch"
    );

    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_infinite_join_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Session, schema, and a single 8 K‐row batch for each side
    let session_ctx = SessionContext::new();
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let mut builder_left = Int64Array::builder(8_192);
    let mut builder_right = Int64Array::builder(8_192);
    for v in 0..8_192 {
        builder_left.append_value(v);
        // on the right side, we’ll shift each value by +1 so that not everything joins,
        // but plenty of matching keys exist (e.g. 0 on left matches 1 on right, etc.)
        builder_right.append_value(v + 1);
    }
    let batch_left =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_left.finish())])?;
    let batch_right =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_right.finish())])?;

    // 2a) Build two InfiniteExecs (left and right)
    let infinite_left = Arc::new(InfiniteExec::new(batch_left, pretend_finite));
    let infinite_right = Arc::new(InfiniteExec::new(batch_right, pretend_finite));

    // 2b) Create Join keys → join on “value” = “value”
    let left_keys: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(Column::new_with_schema("value", &schema)?)];
    let right_keys: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(Column::new_with_schema("value", &schema)?)];

    // 2c) Wrap each side in CoalesceBatches + Repartition so they are both hashed into 1 partition
    let coalesced_left = Arc::new(CoalesceBatchesExec::new(infinite_left, 8_192));
    let coalesced_right = Arc::new(CoalesceBatchesExec::new(infinite_right, 8_192));

    let part_left = Partitioning::Hash(left_keys, 1);
    let part_right = Partitioning::Hash(right_keys, 1);

    let hashed_left = Arc::new(RepartitionExec::try_new(coalesced_left, part_left)?);
    let hashed_right = Arc::new(RepartitionExec::try_new(coalesced_right, part_right)?);

    // 2d) Build an Inner HashJoinExec → left.value = right.value
    let join = Arc::new(HashJoinExec::try_new(
        hashed_left,
        hashed_right,
        vec![(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Arc::new(Column::new_with_schema("value", &schema)?),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        true,
    )?);

    // 3) Wrap yields under each infinite leaf
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(join, &config)?;

    // 4) Execute + 1 sec timeout
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };
    assert!(
        result.is_none(),
        "Expected no output for aggregate over infinite + join, but got a batch"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_infinite_join_agg_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Session, schema, and a single 8 K‐row batch for each side
    let session_ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let mut builder_left = Int64Array::builder(8_192);
    let mut builder_right = Int64Array::builder(8_192);
    for v in 0..8_192 {
        builder_left.append_value(v);
        // on the right side, we’ll shift each value by +1 so that not everything joins,
        // but plenty of matching keys exist (e.g. 0 on left matches 1 on right, etc.)
        builder_right.append_value(v + 1);
    }
    let batch_left =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_left.finish())])?;
    let batch_right =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_right.finish())])?;

    // 2a) Build two InfiniteExecs (left and right)
    let infinite_left = Arc::new(InfiniteExec::new(batch_left, pretend_finite));
    let infinite_right = Arc::new(InfiniteExec::new(batch_right, pretend_finite));

    // 2b) Create Join keys → join on “value” = “value”
    let left_keys: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(Column::new_with_schema("value", &schema)?)];
    let right_keys: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(Column::new_with_schema("value", &schema)?)];

    // 2c) Wrap each side in CoalesceBatches + Repartition so they are both hashed into 1 partition
    let coalesced_left = Arc::new(CoalesceBatchesExec::new(infinite_left, 8_192));
    let coalesced_right = Arc::new(CoalesceBatchesExec::new(infinite_right, 8_192));

    let part_left = Partitioning::Hash(left_keys.clone(), 1);
    let part_right = Partitioning::Hash(right_keys.clone(), 1);

    let hashed_left = Arc::new(RepartitionExec::try_new(coalesced_left, part_left)?);
    let hashed_right = Arc::new(RepartitionExec::try_new(coalesced_right, part_right)?);

    // 2d) Build an Inner HashJoinExec → left.value = right.value
    let join = Arc::new(HashJoinExec::try_new(
        hashed_left,
        hashed_right,
        vec![(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Arc::new(Column::new_with_schema("value", &schema)?),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        true,
    )?);

    // 3) Project only one column (“value” from the left side) because we just want to sum that
    let input_schema = join.schema();

    let proj_expr = vec![(
        Arc::new(Column::new_with_schema("value", &input_schema)?) as _,
        "value".to_string(),
    )];

    let projection = Arc::new(ProjectionExec::try_new(proj_expr, join)?);
    let projection_schema = projection.schema();

    let output_fields = vec![Field::new("total", DataType::Int64, true)];
    let output_schema = Arc::new(Schema::new(output_fields));

    // 4) Global aggregate (Single) over “value”
    let aggregate_expr = AggregateExprBuilder::new(
        sum::sum_udaf(),
        vec![Arc::new(Column::new_with_schema(
            "value",
            &projection.schema(),
        )?)],
    )
    .schema(output_schema)
    .alias("total")
    .build()?;

    let aggr = Arc::new(AggregateExec::try_new(
        AggregateMode::Single,
        PhysicalGroupBy::new(vec![], vec![], vec![]),
        vec![Arc::new(aggregate_expr)],
        vec![None],
        projection,
        projection_schema,
    )?);

    // 5) Wrap yields under each infinite leaf
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(aggr, &config)?;

    // 6) Execute + 1 sec timeout
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };
    assert!(
        result.is_none(),
        "Expected no output for aggregate over infinite + join, but got a batch"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
#[ignore]
async fn test_filter_reject_all_batches_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Create a Session, Schema, and an 8K-row RecordBatch
    let session_ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    // Build a batch with values 0..8191
    let mut builder = Int64Array::builder(8_192);
    for v in 0..8_192 {
        builder.append_value(v);
    }
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())])?;

    // 2a) Wrap this batch in an InfiniteExec
    let infinite = Arc::new(InfiniteExec::new(batch, pretend_finite));

    // 2b) Construct a FilterExec that is always false: “value > 10000” (no rows pass)
    let false_predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("value", &schema)?),
        Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(10_000)))),
    ));
    let filtered = Arc::new(FilterExec::try_new(false_predicate, infinite)?);

    // 2c) Use CoalesceBatchesExec to guarantee each Filter pull always yields an 8192-row batch
    let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8_192));

    // 3) WrapLeaves to insert YieldExec—so that the InfiniteExec yields control between batches
    let config = ConfigOptions::new();
    let optimized = WrapLeaves::new().optimize(coalesced, &config)?;

    // 4) Execute with a 1-second timeout. Because Filter discards all 8192 rows each time
    //    without ever producing output, no batch will arrive within 1 second. And since
    //    emission type is not Final, we never see an end‐of‐stream marker.
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => {
            None
        }
    };
    assert!(
        result.is_none(),
        "Expected no output for infinite + filter(all-false) + aggregate, but got a batch"
    );
    Ok(())
}
