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

use std::error::Error;
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow_schema::SortOptions;
use datafusion::functions_aggregate::sum;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{JoinType, ScalarValue};
use datafusion_expr_common::operator::Operator::Gt;
use datafusion_physical_expr::expressions::{col, BinaryExpr, Column, Literal};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
use datafusion_physical_optimizer::insert_yield_exec::InsertYieldExec;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::InterleaveExec;

use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use futures::StreamExt;
use parking_lot::RwLock;
use rstest::rstest;
use tokio::select;

#[derive(Debug)]
/// A batch generator that can produce either bounded or boundless infinite stream of the same RecordBatch.
struct InfiniteGenerator {
    /// The RecordBatch to return on each call.
    batch: RecordBatch,
    /// How many batches have already been generated.
    counter: usize,
}

impl std::fmt::Display for InfiniteGenerator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // Display current counter
        write!(f, "InfiniteGenerator(counter={})", self.counter)
    }
}

impl LazyBatchGenerator for InfiniteGenerator {
    /// Generate the next RecordBatch.
    fn generate_next_batch(&mut self) -> datafusion_common::Result<Option<RecordBatch>> {
        // Increment the counter and return a clone of the batch
        self.counter += 1;
        Ok(Some(self.batch.clone()))
    }
}

/// Build a LazyMemoryExec that yields either a finite or infinite stream depending on `pretend_finite`.
fn make_lazy_exec(
    batch: RecordBatch,
    schema: SchemaRef,
    pretend_finite: bool,
) -> Arc<dyn ExecutionPlan> {
    let boundedness = if pretend_finite {
        Boundedness::Bounded
    } else {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    };

    // Instantiate the generator with the batch and limit
    let gen = InfiniteGenerator { batch, counter: 0 };

    // Wrap the generator in a trait object behind Arc<RwLock<_>>
    let generator: Arc<RwLock<dyn LazyBatchGenerator>> = Arc::new(RwLock::new(gen));

    // Create a LazyMemoryExec with one partition using our generator
    let mut exec = LazyMemoryExec::try_new(schema, vec![generator]).unwrap();
    exec.set_boundedness(boundedness);

    // Erase concrete type into a generic ExecutionPlan handle
    Arc::new(exec) as Arc<dyn ExecutionPlan>
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
    let inf = make_lazy_exec(batch.clone(), schema.clone(), pretend_finite);
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

    // 3) optimize the plan with InsertYieldExec to auto-insert Yield
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(aggr, &config)?;

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
    let inf = make_lazy_exec(batch.clone(), schema.clone(), pretend_finite);

    // 3) set up a SortExec that will never finish because input is infinite
    let sort_options = SortOptions {
        descending: false,
        nulls_first: true,
    };
    let sort_expr = PhysicalSortExpr::new(
        Arc::new(Column::new_with_schema("value", &schema)?),
        sort_options,
    );
    let sort_exec = Arc::new(SortExec::new([sort_expr].into(), inf));

    // 4) optimize the plan with InsertYieldExec to auto-insert Yield
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(sort_exec, &config)?;

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
async fn test_infinite_interleave_cancel(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Build a session and a schema with one i64 column.
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

    // 2) Create multiple infinite sources, each filtered by a different threshold.
    //    This ensures InterleaveExec has many children.
    let mut infinite_children = vec![];
    // Use 32 distinct thresholds (each > 0 and < 8192) for 32 infinite inputs.
    let thresholds = (0..32).map(|i| 8191 - (i * 256) as i64);

    for thr in thresholds {
        // 2a) Set up the infinite source
        let inf = make_lazy_exec(batch.clone(), schema.clone(), pretend_finite);

        // 2b) Apply a FilterExec with predicate "value > thr".
        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(thr)))),
        ));
        let filtered = Arc::new(FilterExec::try_new(filter_expr, inf)?);

        // 2c) Wrap the filtered stream in CoalesceBatchesExec so it emits
        //     one 8192-row batch at a time.
        let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8192));

        // 2d) Repartition each coalesced stream by hashing on "value" into 1 partition.
        //     Required for InterleaveExec::try_new to succeed.
        let exprs = vec![Arc::new(Column::new_with_schema("value", &schema)?) as _];
        let partitioning = Partitioning::Hash(exprs, 1);
        let hashed = Arc::new(RepartitionExec::try_new(coalesced, partitioning)?);

        infinite_children.push(hashed as Arc<dyn ExecutionPlan>);
    }

    // 3) Build an InterleaveExec over all infinite children.
    let interleave = Arc::new(InterleaveExec::try_new(infinite_children)?);

    // 4) Wrap the InterleaveExec in a FilterExec that always returns false,
    //    ensuring that no rows are ever emitted.
    let always_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    let filtered_interleave = Arc::new(FilterExec::try_new(always_false, interleave)?);

    // 5) Coalesce the filtered interleave into 8192-row batches.
    //    This lets InsertYieldExec insert YieldStreamExec at each batch boundary.
    let coalesced_top = Arc::new(CoalesceBatchesExec::new(filtered_interleave, 8192));

    // 6) Apply InsertYieldExec to insert YieldStreamExec under every leaf.
    //    Each InfiniteExec → FilterExec → CoalesceBatchesExec chain will yield periodically.
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(coalesced_top, &config)?;

    // 7) Execute the optimized plan with a 1-second timeout.
    //    Because the top-level FilterExec always discards rows and the inputs are infinite,
    //    no batch will be returned within 1 second, causing result to be None.
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT)) => None,
    };

    assert!(
        result.is_none(),
        "Expected no output for infinite interleave aggregate, but got a batch"
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
        let inf = make_lazy_exec(batch.clone(), schema.clone(), pretend_finite);

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

    // 5) InsertYieldExec will automatically insert YieldStreams beneath each “infinite” leaf.
    //    That way, each InfiniteExec (through the FilterExec/CoalesceBatchesExec/RepartitionExec chain)
    //    yields to the runtime periodically instead of spinning CPU.
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(aggr, &config)?;

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
    let infinite_left =
        make_lazy_exec(batch_left.clone(), schema.clone(), pretend_finite);
    let infinite_right =
        make_lazy_exec(batch_right.clone(), schema.clone(), pretend_finite);

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
    let optimized = InsertYieldExec::new().optimize(join, &config)?;

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
    let infinite_left =
        make_lazy_exec(batch_left.clone(), schema.clone(), pretend_finite);
    let infinite_right =
        make_lazy_exec(batch_right.clone(), schema.clone(), pretend_finite);

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
    let optimized = InsertYieldExec::new().optimize(aggr, &config)?;

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
    let infinite = make_lazy_exec(batch.clone(), schema.clone(), pretend_finite);

    // 2b) Construct a FilterExec that is always false: “value > 10000” (no rows pass)
    let false_predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new_with_schema("value", &schema)?),
        Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(10_000)))),
    ));
    let filtered = Arc::new(FilterExec::try_new(false_predicate, infinite)?);

    // 2c) Use CoalesceBatchesExec to guarantee each Filter pull always yields an 8192-row batch
    let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8_192));

    // 3) InsertYieldExec to insert YieldExec—so that the InfiniteExec yields control between batches
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(coalesced, &config)?;

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

#[rstest]
#[tokio::test]
async fn test_infinite_hash_join_without_repartition_and_no_agg(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Create Session, schema, and an 8K-row RecordBatch for each side
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
        builder_right.append_value(v + 1);
    }
    let batch_left =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_left.finish())])?;
    let batch_right =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(builder_right.finish())])?;

    // 2a) Unlike the test with aggregation, keep this as a pure join—
    //     use InfiniteExec to simulate an infinite stream
    let infinite_left =
        make_lazy_exec(batch_left.clone(), schema.clone(), pretend_finite);
    let infinite_right =
        make_lazy_exec(batch_right.clone(), schema.clone(), pretend_finite);

    // 2b) To feed a single batch into the Join, we can still use CoalesceBatchesExec,
    //     but do NOT wrap it in a RepartitionExec
    let coalesced_left = Arc::new(CoalesceBatchesExec::new(infinite_left, 8_192));
    let coalesced_right = Arc::new(CoalesceBatchesExec::new(infinite_right, 8_192));

    // 2c) Directly feed `coalesced_left` and `coalesced_right` into HashJoinExec.
    //     Do not use aggregation or repartition.
    let join = Arc::new(HashJoinExec::try_new(
        coalesced_left,
        coalesced_right,
        vec![(
            Arc::new(Column::new_with_schema("value", &schema)?),
            Arc::new(Column::new_with_schema("value", &schema)?),
        )],
        /* filter */ None,
        &JoinType::Inner,
        /* output64 */ None,
        // Using CollectLeft is fine—just avoid RepartitionExec’s partitioned channels.
        PartitionMode::CollectLeft,
        /* build_left */ true,
    )?);

    // 3) Do not apply InsertYieldExec—since there is no aggregation, InsertYieldExec would
    //    not insert a 'final' yield wrapper for the Join. If you want to skip InsertYieldExec
    //    entirely, comment out the next line; however, not calling it is equivalent
    //    because there is no aggregation so no wrapper is inserted. Here we simply do
    //    not call InsertYieldExec, ensuring the plan has neither aggregation nor repartition.
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(join, &config)?;

    // 4) Execute with a 1 second timeout
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT_SEC: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT_SEC)) => {
            None
        }
    };

    assert!(
        result.is_none(),
        "Expected no output for infinite + filter(all-false) + aggregate, but got a batch"
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn test_infinite_sort_merge_join_without_repartition_and_no_agg(
    #[values(false, true)] pretend_finite: bool,
) -> Result<(), Box<dyn Error>> {
    // 1) Create Session, schema, and two small RecordBatches that never overlap:
    //    Left = [-3, -2, -1], Right = [0, 1, 2]
    let session_ctx = SessionContext::new();
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let left_array = {
        let mut b = Int64Array::builder(3);
        b.append_value(-3);
        b.append_value(-2);
        b.append_value(-1);
        Arc::new(b.finish()) as Arc<dyn Array>
    };
    let right_array = {
        let mut b = Int64Array::builder(3);
        b.append_value(0);
        b.append_value(1);
        b.append_value(2);
        Arc::new(b.finish()) as Arc<dyn Array>
    };
    let batch_left = RecordBatch::try_new(schema.clone(), vec![left_array])?;
    let batch_right = RecordBatch::try_new(schema.clone(), vec![right_array])?;

    // 2a) Wrap each small batch in an InfiniteExec (pretend_finite toggles finite vs infinite)
    let infinite_left =
        make_lazy_exec(batch_left.clone(), schema.clone(), pretend_finite);
    let infinite_right =
        make_lazy_exec(batch_right.clone(), schema.clone(), pretend_finite);

    // 2b) Coalesce each InfiniteExec into a single 3-row batch at a time.
    //     (Do NOT wrap in RepartitionExec.)
    let coalesced_left = Arc::new(CoalesceBatchesExec::new(infinite_left, 3));
    let coalesced_right = Arc::new(CoalesceBatchesExec::new(infinite_right, 3));

    // 2c) Build a SortMergeJoinExec on “value”. Since left values < 0 and
    //     right values ≥ 0, they never match. No aggregation or repartition.
    //
    //    We need a Vec<SortOptions> for the join key. Any consistent SortOptions works,
    //    because data is already in ascending order on “value.”
    let join = Arc::new(SortMergeJoinExec::try_new(
        coalesced_left,
        coalesced_right,
        vec![(col("value", &schema)?, col("value", &schema)?)],
        /* filter */ None,
        JoinType::Inner,
        vec![SortOptions::new(true, false)], // ascending, nulls last
        /* null_equal */ true,
    )?);

    // 3) Do not apply InsertYieldExec (no aggregation, no repartition → no built-in yields).
    let config = ConfigOptions::new();
    let optimized = InsertYieldExec::new().optimize(join, &config)?;

    // 4) Execute with a 1-second timeout. Because both sides are infinite and never match,
    //    the SortMergeJoin will never produce output within 1s.
    let mut stream = physical_plan::execute_stream(optimized, session_ctx.task_ctx())?;
    const TIMEOUT_SEC: u64 = 1;
    let result = select! {
        batch_opt = stream.next() => batch_opt,
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(TIMEOUT_SEC)) => None,
    };

    assert!(
        result.is_none(),
        "Expected no output for infinite SortMergeJoin (no repartition & no aggregation), but got a batch",
    );
    Ok(())
}
