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

//! Snapshot tests for *which input the merge drains, and in what order*.
//!
//! The round-robin tie breaker in [`SortPreservingMergeStream`] never changes
//! the merged output: rows with equal sort keys are interchangeable, so the
//! sorted values are identical whether it is on or off. All it changes is
//! which tied input gets consumed next, and therefore which upstream producers
//! keep making progress. Ordinary correctness tests are blind to that, which
//! makes the behavior easy to regress silently.
//!
//! This module drives a merge over hand-written inputs and records an
//! interleaved trace of everything an outside observer can see:
//!
//! * `poll S<n>` — the merge asked input `n` for another batch
//! * `row  S<n>` — the merge emitted a row that came from input `n`
//!
//! The traces are asserted with inline `insta` snapshots, so a change in poll
//! order shows up as a snapshot diff instead of passing unnoticed. To accept
//! an intended change, run the tests with `INSTA_FORCE_UPDATE=1` and apply the
//! result with `cargo insta accept`, rather than hand-editing the expected
//! text -- the point is that changing it is a deliberate act.
//!
//! [`SortPreservingMergeStream`]: crate::sorts::merge::SortPreservingMergeStream

use std::fmt::Write as _;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::sorts::streaming_merge::StreamingMergeBuilder;
use crate::stream::RecordBatchStreamAdapter;
use crate::{RecordBatchStream, SendableRecordBatchStream};

use arrow::array::{AsArray, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion_common::Result;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};

use futures::{Stream, StreamExt};
use insta::assert_snapshot;

/// One event observed while driving a merge.
#[derive(Debug)]
enum TraceEvent {
    /// The merge polled input `partition`, which returned a batch of `keys`.
    Batch { partition: usize, keys: Vec<i32> },
    /// The merge polled input `partition`, which reported end of stream.
    Eof { partition: usize },
    /// The merge emitted an output row that originated in `partition`.
    Row { partition: usize, key: i32 },
}

/// Shared, append-only log of [`TraceEvent`]s.
///
/// The input streams append to it from inside the merge's `poll_next`, and the
/// driver appends emitted rows in between, which is what makes the two kinds
/// of event interleave in issue order.
type TraceLog = Arc<Mutex<Vec<TraceEvent>>>;

/// Wraps one input of the merge and logs every poll that resolves.
///
/// `Poll::Pending` is not logged: these inputs are always immediately ready,
/// so a pending poll never happens.
struct TracingStream {
    partition: usize,
    inner: SendableRecordBatchStream,
    log: TraceLog,
}

impl Stream for TracingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let partition = self.partition;
        let polled = self.inner.poll_next_unpin(cx);
        match &polled {
            Poll::Ready(Some(Ok(batch))) => {
                let keys = batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .values()
                    .to_vec();
                self.log
                    .lock()
                    .unwrap()
                    .push(TraceEvent::Batch { partition, keys });
            }
            Poll::Ready(None) => {
                self.log.lock().unwrap().push(TraceEvent::Eof { partition });
            }
            Poll::Ready(Some(Err(_))) | Poll::Pending => {}
        }
        polled
    }
}

impl RecordBatchStream for TracingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

/// `(key, source partition)` pairs; `key` is the sort key, the partition index
/// rides along so emitted rows can be attributed back to their input.
fn trace_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("src", DataType::Int32, false),
    ]))
}

/// Builds one input stream: `batches` is a list of batches, each a list of
/// sort keys. All rows are tagged with `partition`.
fn input_stream(partition: usize, batches: &[&[i32]], log: &TraceLog) -> TracingStream {
    let schema = trace_schema();
    let batches: Vec<Result<RecordBatch>> = batches
        .iter()
        .map(|keys| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(keys.to_vec())),
                    Arc::new(Int32Array::from(vec![partition as i32; keys.len()])),
                ],
            )
            .map_err(Into::into)
        })
        .collect();

    TracingStream {
        partition,
        inner: Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches),
        )),
        log: Arc::clone(log),
    }
}

/// Merges `inputs` (partition -> batch -> sort keys) and returns the rendered
/// trace of polls and emitted rows.
///
/// The merge runs with `batch_size = 1` so every output row is handed back
/// individually and can be slotted into the log at the point it was produced.
/// Output batching is independent of the loser tree and the tie breaker, so
/// this makes the trace finer-grained without changing what is being traced.
async fn trace_merge(inputs: &[&[&[i32]]], round_robin_tie_breaker: bool) -> String {
    let schema = trace_schema();
    let log: TraceLog = Arc::new(Mutex::new(Vec::new()));

    let streams: Vec<SendableRecordBatchStream> = inputs
        .iter()
        .enumerate()
        .map(|(partition, batches)| {
            Box::pin(input_stream(partition, batches, &log)) as SendableRecordBatchStream
        })
        .collect();

    let ordering: LexOrdering =
        [PhysicalSortExpr::new_default(col("key", &schema).unwrap())].into();

    let mut merged = StreamingMergeBuilder::new()
        .with_streams(streams)
        .with_schema(Arc::clone(&schema))
        .with_expressions(&ordering)
        .with_metrics(BaselineMetrics::new(&ExecutionPlanMetricsSet::new(), 0))
        .with_batch_size(1)
        .with_bypass_mempool()
        .with_round_robin_tie_breaker(round_robin_tie_breaker)
        .build()
        .unwrap();

    while let Some(batch) = merged.next().await {
        let batch = batch.unwrap();
        let keys = batch.column(0).as_primitive::<Int32Type>();
        let sources = batch.column(1).as_primitive::<Int32Type>();
        let mut log = log.lock().unwrap();
        for row in 0..batch.num_rows() {
            log.push(TraceEvent::Row {
                partition: sources.value(row) as usize,
                key: keys.value(row),
            });
        }
    }

    render(inputs, round_robin_tie_breaker, &log.lock().unwrap())
}

/// Renders the inputs and the recorded events as the snapshot text.
fn render(
    inputs: &[&[&[i32]]],
    round_robin_tie_breaker: bool,
    events: &[TraceEvent],
) -> String {
    let mut out = String::new();
    writeln!(out, "round_robin_tie_breaker = {round_robin_tie_breaker}").unwrap();
    writeln!(out, "inputs:").unwrap();
    for (partition, batches) in inputs.iter().enumerate() {
        let batches = batches
            .iter()
            .map(|keys| format!("[{}]", join(keys)))
            .collect::<Vec<_>>()
            .join(" ");
        writeln!(out, "  S{partition}: {batches}").unwrap();
    }
    writeln!(out, "trace:").unwrap();
    for event in events {
        match event {
            TraceEvent::Batch { partition, keys } => {
                writeln!(out, "  poll S{partition} -> [{}]", join(keys)).unwrap()
            }
            TraceEvent::Eof { partition } => {
                writeln!(out, "  poll S{partition} -> done").unwrap()
            }
            TraceEvent::Row { partition, key } => {
                writeln!(out, "  row  S{partition} key={key}").unwrap()
            }
        }
    }
    out
}

fn join(keys: &[i32]) -> String {
    keys.iter()
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Traces the same inputs with the tie breaker enabled and disabled, so a
/// single snapshot shows what the tie breaker changes.
async fn trace_both(inputs: &[&[&[i32]]]) -> String {
    format!(
        "{}\n{}",
        trace_merge(inputs, true).await,
        trace_merge(inputs, false).await
    )
}

/// With every key tied, the tie breaker must hand rows out in strict
/// round-robin order; with it off, the lowest-index input is drained first and
/// the other producer is left idle.
#[tokio::test]
async fn test_tied_keys_alternate_between_inputs() {
    let inputs: &[&[&[i32]]] = &[&[&[1, 1, 1]], &[&[1, 1, 1]]];

    assert_snapshot!(trace_both(inputs).await, @r"
    round_robin_tie_breaker = true
    inputs:
      S0: [1,1,1]
      S1: [1,1,1]
    trace:
      poll S0 -> [1,1,1]
      poll S1 -> [1,1,1]
      row  S0 key=1
      row  S1 key=1
      row  S0 key=1
      row  S1 key=1
      row  S0 key=1
      poll S0 -> done
      row  S1 key=1
      poll S1 -> done

    round_robin_tie_breaker = false
    inputs:
      S0: [1,1,1]
      S1: [1,1,1]
    trace:
      poll S0 -> [1,1,1]
      poll S1 -> [1,1,1]
      row  S0 key=1
      row  S0 key=1
      row  S0 key=1
      poll S0 -> done
      row  S1 key=1
      row  S1 key=1
      row  S1 key=1
      poll S1 -> done
    ");
}

/// Regression test for stale poll counts leaking across runs of ties.
///
/// `S0` runs out of `1`s first, so the first run of ties ends with `S1` still
/// holding several rows that it drains on its own. The second run (key `2`)
/// is a fresh tie-breaker round and must alternate from its very first row.
///
/// Before the fix, `is_poll_count_gt` compared the challenger's *raw* counter,
/// which still held the count `S1` accumulated during the key `1` run, and the
/// key `2` run started `S1 S0 S0 S1` instead of alternating.
#[tokio::test]
async fn test_poll_counts_reset_between_runs_of_ties() {
    let inputs: &[&[&[i32]]] = &[&[&[1, 1], &[2, 2]], &[&[1, 1, 1, 1], &[2, 2]]];

    assert_snapshot!(trace_both(inputs).await, @r"
    round_robin_tie_breaker = true
    inputs:
      S0: [1,1] [2,2]
      S1: [1,1,1,1] [2,2]
    trace:
      poll S0 -> [1,1]
      poll S1 -> [1,1,1,1]
      row  S0 key=1
      row  S1 key=1
      row  S0 key=1
      poll S0 -> [2,2]
      row  S1 key=1
      row  S1 key=1
      row  S1 key=1
      poll S1 -> [2,2]
      row  S0 key=2
      row  S1 key=2
      row  S0 key=2
      poll S0 -> done
      row  S1 key=2
      poll S1 -> done

    round_robin_tie_breaker = false
    inputs:
      S0: [1,1] [2,2]
      S1: [1,1,1,1] [2,2]
    trace:
      poll S0 -> [1,1]
      poll S1 -> [1,1,1,1]
      row  S0 key=1
      row  S0 key=1
      poll S0 -> [2,2]
      row  S1 key=1
      row  S1 key=1
      row  S1 key=1
      row  S1 key=1
      poll S1 -> [2,2]
      row  S0 key=2
      row  S0 key=2
      poll S0 -> done
      row  S1 key=2
      row  S1 key=2
      poll S1 -> done
    ");
}

/// Without ties there is nothing to break: both settings must produce the same
/// poll order. A diff here means the tie breaker started interfering with the
/// ordinary loser-tree path.
#[tokio::test]
async fn test_distinct_keys_are_unaffected_by_the_tie_breaker() {
    let inputs: &[&[&[i32]]] = &[&[&[1, 3, 5]], &[&[2, 4, 6]]];

    assert_snapshot!(trace_both(inputs).await, @r"
    round_robin_tie_breaker = true
    inputs:
      S0: [1,3,5]
      S1: [2,4,6]
    trace:
      poll S0 -> [1,3,5]
      poll S1 -> [2,4,6]
      row  S0 key=1
      row  S1 key=2
      row  S0 key=3
      row  S1 key=4
      row  S0 key=5
      poll S0 -> done
      row  S1 key=6
      poll S1 -> done

    round_robin_tie_breaker = false
    inputs:
      S0: [1,3,5]
      S1: [2,4,6]
    trace:
      poll S0 -> [1,3,5]
      poll S1 -> [2,4,6]
      row  S0 key=1
      row  S1 key=2
      row  S0 key=3
      row  S1 key=4
      row  S0 key=5
      poll S0 -> done
      row  S1 key=6
      poll S1 -> done
    ");
}

/// The tie breaker only runs at the root of the loser tree (`cmp_node == 1`),
/// so with four tied inputs it balances the two sub-tree winners rather than
/// all four producers: `S1` and `S3` still drain in one go.
///
/// This is a known limitation, recorded here so that widening the tie breaker
/// to inner nodes shows up as a deliberate snapshot change. Note also that the
/// initial polls are not in partition order — `initialize_all_partitions` uses
/// `swap_remove`, which reorders the pending list.
#[tokio::test]
async fn test_tie_breaker_only_balances_the_root_comparison() {
    let inputs: &[&[&[i32]]] = &[&[&[1, 1]], &[&[1, 1]], &[&[1, 1]], &[&[1, 1]]];

    assert_snapshot!(trace_merge(inputs, true).await, @r"
    round_robin_tie_breaker = true
    inputs:
      S0: [1,1]
      S1: [1,1]
      S2: [1,1]
      S3: [1,1]
    trace:
      poll S0 -> [1,1]
      poll S3 -> [1,1]
      poll S2 -> [1,1]
      poll S1 -> [1,1]
      row  S0 key=1
      row  S2 key=1
      row  S0 key=1
      poll S0 -> done
      row  S1 key=1
      row  S1 key=1
      poll S1 -> done
      row  S2 key=1
      poll S2 -> done
      row  S3 key=1
      row  S3 key=1
      poll S3 -> done
    ");
}
