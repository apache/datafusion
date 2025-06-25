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
use datafusion::common::NullEquality;
use datafusion::functions_aggregate::sum;
use datafusion::physical_expr::aggregate::AggregateExprBuilder;
use datafusion::physical_plan;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::{DataFusionError, JoinType, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr_common::operator::Operator;
use datafusion_expr_common::operator::Operator::{Divide, Eq, Gt, Modulo};
use datafusion_functions_aggregate::min_max;
use datafusion_physical_expr::expressions::{
    binary, col, lit, BinaryExpr, Column, Literal,
};
use datafusion_physical_expr::Partitioning;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_optimizer::ensure_coop::EnsureCooperative;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::InterleaveExec;
use futures::StreamExt;
use parking_lot::RwLock;
use rstest::rstest;
use std::error::Error;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};
use tokio::select;

#[derive(Debug)]
struct RangeBatchGenerator {
    schema: SchemaRef,
    value_range: Range<i64>,
    boundedness: Boundedness,
    batch_size: usize,
    poll_count: usize,
}

impl std::fmt::Display for RangeBatchGenerator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        // Display current counter
        write!(f, "InfiniteGenerator(counter={})", self.poll_count)
    }
}

impl LazyBatchGenerator for RangeBatchGenerator {
    fn boundedness(&self) -> Boundedness {
        self.boundedness
    }

    /// Generate the next RecordBatch.
    fn generate_next_batch(&mut self) -> datafusion_common::Result<Option<RecordBatch>> {
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
            return Ok(None);
        }

        let batch =
            RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(array)])?;
        Ok(Some(batch))
    }
}

fn make_lazy_exec(column_name: &str, pretend_infinite: bool) -> LazyMemoryExec {
    make_lazy_exec_with_range(column_name, i64::MIN..i64::MAX, pretend_infinite)
}

fn make_lazy_exec_with_range(
    column_name: &str,
    range: Range<i64>,
    pretend_infinite: bool,
) -> LazyMemoryExec {
    let schema = Arc::new(Schema::new(vec![Field::new(
        column_name,
        DataType::Int64,
        false,
    )]));

    let boundedness = if pretend_infinite {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    } else {
        Boundedness::Bounded
    };

    // Instantiate the generator with the batch and limit
    let gen = RangeBatchGenerator {
        schema: Arc::clone(&schema),
        boundedness,
        value_range: range,
        batch_size: 8192,
        poll_count: 0,
    };

    // Wrap the generator in a trait object behind Arc<RwLock<_>>
    let generator: Arc<RwLock<dyn LazyBatchGenerator>> = Arc::new(RwLock::new(gen));

    // Create a LazyMemoryExec with one partition using our generator
    let mut exec = LazyMemoryExec::try_new(schema, vec![generator]).unwrap();

    exec.add_ordering(vec![PhysicalSortExpr::new(
        Arc::new(Column::new(column_name, 0)),
        SortOptions::new(false, true),
    )]);

    exec
}

#[rstest]
#[tokio::test]
async fn agg_no_grouping_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up an aggregation without grouping
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));
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

    query_yields(aggr, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn agg_grouping_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up an aggregation with grouping
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));

    let value_col = col("value", &inf.schema())?;
    let group = binary(value_col.clone(), Divide, lit(1000000i64), &inf.schema())?;

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

    query_yields(aggr, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn agg_grouped_topk_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up a top-k aggregation
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));

    let value_col = col("value", &inf.schema())?;
    let group = binary(value_col.clone(), Divide, lit(1000000i64), &inf.schema())?;

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

    query_yields(aggr, session_ctx.task_ctx()).await
}

// Note that this test succeeds, but it's because internal budget is exhausted 
// in early sorting stage (before spill, before SPM), so it's actually testing the same behavior as sort_yields
#[rstest]
#[tokio::test]
async fn spill_sort_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    use datafusion::prelude::SessionConfig;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    let session_config = SessionConfig::new().with_target_partitions(2);
    let sort_spill_reservation_bytes = session_config
        .options()
        .execution
        .sort_spill_reservation_bytes;
    let runtime = RuntimeEnvBuilder::new()
        // .with_memory_limit(sort_spill_reservation_bytes + 12288, 1.05)
        .with_memory_limit(sort_spill_reservation_bytes + 12288, 1.02)
        .build_arc()?;
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(session_config)
            .with_runtime(runtime),
    );

    // set up the infinite source
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));

    // set up a SortExec that will not be able to finish in time because input is very large
    let sort_expr = PhysicalSortExpr::new(
        col("value", &inf.schema())?,
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    );

    let lex_ordering = LexOrdering::new(vec![sort_expr]).unwrap();
    let sort_exec = Arc::new(SortExec::new(lex_ordering, inf.clone()));

    let res = query_yields(sort_exec.clone(), task_ctx).await;

    let metrics = sort_exec.metrics().unwrap();
    // check whether spill actually happened
    let did_it_spill = metrics.spill_count().unwrap_or(0) > 0;
    assert!(did_it_spill);
    println!("spill counts {}", metrics.spill_count().unwrap_or(0));
    res
}

// Manually make a Producer Stream that  
#[rstest]
#[tokio::test]
async fn spill_reader_yield(
) -> Result<(), Box<dyn Error>> {
    use std::{pin::Pin, task::Context};
    use datafusion_physical_plan::common::spawn_buffered;
    use datafusion_common::{Result};
    use datafusion_execution::{RecordBatchStream};
    use datafusion_physical_plan::{coop::cooperative, test::build_table_i32};
    use futures::{Stream};

    let batch = build_table_i32(
        ("a2", &vec![0, 1, 2]),
        ("b2", &vec![3, 4, 5]),
        ("c2", &vec![4, 5, 6]),
    );

    let schema = batch.schema();
    // Set large buffer so that buffer always has free space for the producer/sender 
    let buffer_capacity = 100_000;  
    /// A mock stream that always returns `Poll::Ready(Some(...))` immediately
    struct AlwaysReadyStream {
        batch: RecordBatch,
        schema: Arc<Schema>,
        remaining: usize
    }

    impl AlwaysReadyStream {
        pub fn new(batch: RecordBatch, schema: Arc<Schema>) -> Self {
            Self { batch, schema, remaining: 200 }
        }
    }

    impl Stream for AlwaysReadyStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            if self.remaining == 0 {
                Poll::Ready(None)
            } else {
                self.remaining -= 1;
                Poll::Ready(Some(Ok(self.batch.clone())))
            }
        }
    }

    impl RecordBatchStream for AlwaysReadyStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    struct ConsumerStream {
        inner: SendableRecordBatchStream,
    }

    impl Stream for ConsumerStream {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            use arrow::compute::concat_batches;
            let mut collected = vec![]; 
            // To make sure that inner stream is polled multiple times, loop forever if inner (producer) stream returns Ready
            loop {
                match self.inner.as_mut().poll_next(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        println!("received batch from inner");
                        collected.push(batch);
                    }
                    Poll::Ready(Some(Err(e))) => {
                        println!("error from inner");
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        println!("inner stream ended");
                        break;
                    }
                    Poll::Pending => {
                        // polling inner stream may return Pending only when it reaches budget, since 
                        // we intentionally made ProducerStream always return Ready
                        return Poll::Pending;
                    }
                }
            }

            // This should be unreachable since the stream is canceled
            let combined = concat_batches(&self.inner.schema(), &collected)
                .expect("Failed to concat batches");

            Poll::Ready(Some(Ok(combined)))
        }
    }

    impl RecordBatchStream for ConsumerStream {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }
    }

    // this function makes a consumer stream that resembles how read_stream from spill file is constructed 
    fn mock_read_spill_as_stream(batch: RecordBatch, schema: Arc<Schema>, buffer_capacity: usize) -> Result<SendableRecordBatchStream> {
        let stream = Box::pin(cooperative(AlwaysReadyStream::new(batch, schema)));
        Ok(spawn_buffered(stream, buffer_capacity))
    }

    let mock_stream = mock_read_spill_as_stream(batch, schema.clone(), buffer_capacity).unwrap();
    let consumer_stream = Box::pin(ConsumerStream { inner: mock_stream });
    stream_yields(consumer_stream).await
}

#[rstest]
#[tokio::test]
async fn sort_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up the infinite source
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));

    // set up a SortExec that will not be able to finish in time because input is very large
    let sort_expr = PhysicalSortExpr::new(
        col("value", &inf.schema())?,
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    );

    let lex_ordering = LexOrdering::new(vec![sort_expr]).unwrap();
    let sort_exec = Arc::new(SortExec::new(lex_ordering, inf.clone()));

    query_yields(sort_exec, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn sort_merge_join_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up the join sources
    let inf1 = Arc::new(make_lazy_exec_with_range(
        "value1",
        i64::MIN..0,
        pretend_infinite,
    ));
    let inf2 = Arc::new(make_lazy_exec_with_range(
        "value2",
        0..i64::MAX,
        pretend_infinite,
    ));

    // set up a SortMergeJoinExec that will take a long time skipping left side content to find
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
        vec![inf1.properties().eq_properties.output_ordering().unwrap()[0].options],
        NullEquality::NullEqualsNull,
    )?);

    query_yields(join, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn filter_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up the infinite source
    let inf = Arc::new(make_lazy_exec("value", pretend_infinite));

    // set up a FilterExec that will filter out entire batches
    let filter_expr = binary(
        col("value", &inf.schema())?,
        Operator::Lt,
        lit(i64::MIN),
        &inf.schema(),
    )?;
    let filter = Arc::new(FilterExec::try_new(filter_expr, inf.clone())?);

    query_yields(filter, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn filter_reject_all_batches_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Create a Session, Schema, and an 8K-row RecordBatch
    let session_ctx = SessionContext::new();

    // Wrap this batch in an InfiniteExec
    let infinite = make_lazy_exec_with_range("value", i64::MIN..0, pretend_infinite);

    // 2b) Construct a FilterExec that is always false: “value > 10000” (no rows pass)
    let false_predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("value", 0)),
        Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
    ));
    let filtered = Arc::new(FilterExec::try_new(false_predicate, Arc::new(infinite))?);

    // Use CoalesceBatchesExec to guarantee each Filter pull always yields an 8192-row batch
    let coalesced = Arc::new(CoalesceBatchesExec::new(filtered, 8_192));

    query_yields(coalesced, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn interleave_then_filter_all_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Build a session and a schema with one i64 column.
    let session_ctx = SessionContext::new();

    // Create multiple infinite sources, each filtered by a different threshold.
    // This ensures InterleaveExec has many children.
    let mut infinite_children = vec![];

    // Use 32 distinct thresholds (each >0 and <8 192) to force 32 infinite inputs
    for thr in 1..32 {
        // One infinite exec:
        let mut inf = make_lazy_exec_with_range("value", 0..i64::MAX, pretend_infinite);

        // Now repartition so that all children share identical Hash partitioning
        // on “value” into 1 bucket. This is required for InterleaveExec::try_new.
        let exprs = vec![Arc::new(Column::new("value", 0)) as _];
        let partitioning = Partitioning::Hash(exprs, 1);
        inf.try_set_partitioning(partitioning)?;

        // Apply a FilterExec: “(value / 8192) % thr == 0”.
        let filter_expr = binary(
            binary(
                binary(
                    col("value", &inf.schema())?,
                    Divide,
                    lit(8192i64),
                    &inf.schema(),
                )?,
                Modulo,
                lit(thr as i64),
                &inf.schema(),
            )?,
            Eq,
            lit(0i64),
            &inf.schema(),
        )?;
        let filtered = Arc::new(FilterExec::try_new(filter_expr, Arc::new(inf))?);

        infinite_children.push(filtered as _);
    }

    // Build an InterleaveExec over all infinite children.
    let interleave = Arc::new(InterleaveExec::try_new(infinite_children)?);

    // Wrap the InterleaveExec in a FilterExec that always returns false,
    // ensuring that no rows are ever emitted.
    let always_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
    let filtered_interleave = Arc::new(FilterExec::try_new(always_false, interleave)?);

    query_yields(filtered_interleave, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn interleave_then_aggregate_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Build session, schema, and a sample batch.
    let session_ctx = SessionContext::new();

    // Create N infinite sources, each filtered by a different predicate.
    // That way, the InterleaveExec will have multiple children.
    let mut infinite_children = vec![];

    // Use 32 distinct thresholds (each >0 and <8 192) to force 32 infinite inputs
    for thr in 1..32 {
        // One infinite exec:
        let mut inf = make_lazy_exec_with_range("value", 0..i64::MAX, pretend_infinite);

        // Now repartition so that all children share identical Hash partitioning
        // on “value” into 1 bucket. This is required for InterleaveExec::try_new.
        let exprs = vec![Arc::new(Column::new("value", 0)) as _];
        let partitioning = Partitioning::Hash(exprs, 1);
        inf.try_set_partitioning(partitioning)?;

        // Apply a FilterExec: “(value / 8192) % thr == 0”.
        let filter_expr = binary(
            binary(
                binary(
                    col("value", &inf.schema())?,
                    Divide,
                    lit(8192i64),
                    &inf.schema(),
                )?,
                Modulo,
                lit(thr as i64),
                &inf.schema(),
            )?,
            Eq,
            lit(0i64),
            &inf.schema(),
        )?;
        let filtered = Arc::new(FilterExec::try_new(filter_expr, Arc::new(inf))?);

        infinite_children.push(filtered as _);
    }

    // Build an InterleaveExec over all N children.
    // Since each child now has Partitioning::Hash([col "value"], 1), InterleaveExec::try_new succeeds.
    let interleave = Arc::new(InterleaveExec::try_new(infinite_children)?);
    let interleave_schema = interleave.schema();

    // Build a global AggregateExec that sums “value” over all rows.
    // Because we use `AggregateMode::Single` with no GROUP BY columns, this plan will
    // only produce one “final” row once all inputs finish. But our inputs never finish,
    // so we should never get any output.
    let aggregate_expr = AggregateExprBuilder::new(
        sum::sum_udaf(),
        vec![Arc::new(Column::new("value", 0))],
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

    query_yields(aggr, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn join_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Session, schema, and a single 8 K‐row batch for each side
    let session_ctx = SessionContext::new();

    // on the right side, we’ll shift each value by +1 so that not everything joins,
    // but plenty of matching keys exist (e.g. 0 on left matches 1 on right, etc.)
    let infinite_left = make_lazy_exec_with_range("value", -10..10, false);
    let infinite_right =
        make_lazy_exec_with_range("value", 0..i64::MAX, pretend_infinite);

    // Create Join keys → join on “value” = “value”
    let left_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];
    let right_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];

    // Wrap each side in CoalesceBatches + Repartition so they are both hashed into 1 partition
    let coalesced_left =
        Arc::new(CoalesceBatchesExec::new(Arc::new(infinite_left), 8_192));
    let coalesced_right =
        Arc::new(CoalesceBatchesExec::new(Arc::new(infinite_right), 8_192));

    let part_left = Partitioning::Hash(left_keys, 1);
    let part_right = Partitioning::Hash(right_keys, 1);

    let hashed_left = Arc::new(RepartitionExec::try_new(coalesced_left, part_left)?);
    let hashed_right = Arc::new(RepartitionExec::try_new(coalesced_right, part_right)?);

    // Build an Inner HashJoinExec → left.value = right.value
    let join = Arc::new(HashJoinExec::try_new(
        hashed_left,
        hashed_right,
        vec![(
            Arc::new(Column::new("value", 0)),
            Arc::new(Column::new("value", 0)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
    )?);

    query_yields(join, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn join_agg_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Session, schema, and a single 8 K‐row batch for each side
    let session_ctx = SessionContext::new();

    // on the right side, we’ll shift each value by +1 so that not everything joins,
    // but plenty of matching keys exist (e.g. 0 on left matches 1 on right, etc.)
    let infinite_left = make_lazy_exec_with_range("value", -10..10, false);
    let infinite_right =
        make_lazy_exec_with_range("value", 0..i64::MAX, pretend_infinite);

    // 2b) Create Join keys → join on “value” = “value”
    let left_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];
    let right_keys: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("value", 0))];

    // Wrap each side in CoalesceBatches + Repartition so they are both hashed into 1 partition
    let coalesced_left =
        Arc::new(CoalesceBatchesExec::new(Arc::new(infinite_left), 8_192));
    let coalesced_right =
        Arc::new(CoalesceBatchesExec::new(Arc::new(infinite_right), 8_192));

    let part_left = Partitioning::Hash(left_keys, 1);
    let part_right = Partitioning::Hash(right_keys, 1);

    let hashed_left = Arc::new(RepartitionExec::try_new(coalesced_left, part_left)?);
    let hashed_right = Arc::new(RepartitionExec::try_new(coalesced_right, part_right)?);

    // Build an Inner HashJoinExec → left.value = right.value
    let join = Arc::new(HashJoinExec::try_new(
        hashed_left,
        hashed_right,
        vec![(
            Arc::new(Column::new("value", 0)),
            Arc::new(Column::new("value", 0)),
        )],
        None,
        &JoinType::Inner,
        None,
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
    )?);

    // Project only one column (“value” from the left side) because we just want to sum that
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

    query_yields(aggr, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn hash_join_yields(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // build session
    let session_ctx = SessionContext::new();

    // set up the join sources
    let inf1 = Arc::new(make_lazy_exec("value1", pretend_infinite));
    let inf2 = Arc::new(make_lazy_exec("value2", pretend_infinite));

    // set up a HashJoinExec that will take a long time in the build phase
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
        NullEquality::NullEqualsNull,
    )?);

    query_yields(join, session_ctx.task_ctx()).await
}

#[rstest]
#[tokio::test]
async fn hash_join_without_repartition_and_no_agg(
    #[values(false, true)] pretend_infinite: bool,
) -> Result<(), Box<dyn Error>> {
    // Create Session, schema, and an 8K-row RecordBatch for each side
    let session_ctx = SessionContext::new();

    // on the right side, we’ll shift each value by +1 so that not everything joins,
    // but plenty of matching keys exist (e.g. 0 on left matches 1 on right, etc.)
    let infinite_left = make_lazy_exec_with_range("value", -10..10, false);
    let infinite_right =
        make_lazy_exec_with_range("value", 0..i64::MAX, pretend_infinite);

    // Directly feed `infinite_left` and `infinite_right` into HashJoinExec.
    // Do not use aggregation or repartition.
    let join = Arc::new(HashJoinExec::try_new(
        Arc::new(infinite_left),
        Arc::new(infinite_right),
        vec![(
            Arc::new(Column::new("value", 0)),
            Arc::new(Column::new("value", 0)),
        )],
        /* filter */ None,
        &JoinType::Inner,
        /* output64 */ None,
        // Using CollectLeft is fine—just avoid RepartitionExec’s partitioned channels.
        PartitionMode::CollectLeft,
        NullEquality::NullEqualsNull,
    )?);

    query_yields(join, session_ctx.task_ctx()).await
}

#[derive(Debug)]
enum Yielded {
    ReadyOrPending,
    Err(#[allow(dead_code)] DataFusionError),
    Timeout,
}

async fn stream_yields(
    mut stream: SendableRecordBatchStream,
) -> Result<(), Box<dyn Error>> {
    // Create an independent executor pool
    let child_runtime = Runtime::new()?;

    // Spawn a task that tries to poll the stream
    // The task returns Ready when the stream yielded with either Ready or Pending
    let join_handle = child_runtime.spawn(std::future::poll_fn(move |cx| {
        match stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Poll::Ready(Ok(()))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Poll::Ready(Err(e))),
            Poll::Ready(None) => Poll::Ready(Poll::Ready(Ok(()))),
            Poll::Pending => Poll::Ready(Poll::Pending),
        }
    }));

    let abort_handle = join_handle.abort_handle();

    // Now select on the join handle of the task running in the child executor with a timeout
    let yielded = select! {
        result = join_handle => {
            match result {
                Ok(Pending) => Yielded::ReadyOrPending,
                Ok(Ready(Ok(_))) => Yielded::ReadyOrPending,
                Ok(Ready(Err(e))) => Yielded::Err(e),
                Err(_) => Yielded::Err(DataFusionError::Execution("join error".into())),
            }
        },
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            Yielded::Timeout
        }
    };

    // Try to abort the poll task and shutdown the child runtime
    abort_handle.abort();
    Handle::current().spawn_blocking(move || {
        child_runtime.shutdown_timeout(Duration::from_secs(5));
    });

    // Finally, check if poll_next yielded
    assert!(
        matches!(yielded, Yielded::ReadyOrPending),
        "Result is not Ready or Pending: {yielded:?}"
    );
    Ok(())
}

async fn query_yields(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<(), Box<dyn Error>> {
    // Run plan through EnsureCooperative
    let optimized =
        EnsureCooperative::new().optimize(plan, task_ctx.session_config().options())?;

    // Get the stream
    let mut stream = physical_plan::execute_stream(optimized, task_ctx)?;

    // Create an independent executor pool
    let child_runtime = Runtime::new()?;

    // Spawn a task that tries to poll the stream
    // The task returns Ready when the stream yielded with either Ready or Pending
    let join_handle = child_runtime.spawn(std::future::poll_fn(move |cx| {
        match stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(_))) => Poll::Ready(Poll::Ready(Ok(()))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Poll::Ready(Err(e))),
            Poll::Ready(None) => Poll::Ready(Poll::Ready(Ok(()))),
            Poll::Pending => Poll::Ready(Poll::Pending),
        }
    }));

    let abort_handle = join_handle.abort_handle();

    // Now select on the join handle of the task running in the child executor with a timeout
    let yielded = select! {
        result = join_handle => {
            match result {
                Ok(Pending) => Yielded::ReadyOrPending,
                Ok(Ready(Ok(_))) => Yielded::ReadyOrPending,
                Ok(Ready(Err(e))) => Yielded::Err(e),
                Err(_) => Yielded::Err(DataFusionError::Execution("join error".into())),
            }
        },
        _ = tokio::time::sleep(Duration::from_secs(10)) => {
            Yielded::Timeout
        }
    };

    // Try to abort the poll task and shutdown the child runtime
    abort_handle.abort();
    Handle::current().spawn_blocking(move || {
        child_runtime.shutdown_timeout(Duration::from_secs(5));
    });

    // Finally, check if poll_next yielded
    assert!(
        matches!(yielded, Yielded::ReadyOrPending),
        "Result is not Ready or Pending: {yielded:?}"
    );
    Ok(())
}
