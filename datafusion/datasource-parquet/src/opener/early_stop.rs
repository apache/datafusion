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

//! [`EarlyStoppingStream`] terminates a Parquet file scan when a dynamic
//! filter narrows after the scan has already started.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_plan::metrics::PruningMetrics;
use datafusion_pruning::FilePruner;
use futures::{Stream, StreamExt, ready};

/// Wraps an inner RecordBatchStream and a [`FilePruner`]
///
/// This can terminate the scan early when some dynamic filters is updated after
/// the scan starts, so we discover after the scan starts that the file can be
/// pruned (can't have matching rows).
pub(super) struct EarlyStoppingStream<S> {
    /// Has the stream finished processing? All subsequent polls will return
    /// None
    done: bool,
    file_pruner: FilePruner,
    files_ranges_pruned_statistics: PruningMetrics,
    /// The inner stream, dropped as soon as this stream is done with it.
    ///
    /// Held as an `Option` so finishing releases the decoder — and the buffers
    /// and per-file metric state it owns — at the moment we stop reading, not
    /// whenever the caller gets around to dropping this wrapper. Notably the
    /// scan's byte-progress accounting is completed by that drop, so deferring
    /// it would leave the file reading as partially scanned after the scan had
    /// demonstrably finished with it.
    inner: Option<S>,
}

impl<S> EarlyStoppingStream<S> {
    pub(super) fn new(
        stream: S,
        file_pruner: FilePruner,
        files_ranges_pruned_statistics: PruningMetrics,
    ) -> Self {
        Self {
            done: false,
            inner: Some(stream),
            file_pruner,
            files_ranges_pruned_statistics,
        }
    }

    /// Mark the stream finished and release the inner stream.
    fn finish(&mut self) {
        self.done = true;
        self.inner = None;
    }
}

impl<S> EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    fn check_prune(&mut self, input: Result<RecordBatch>) -> Result<Option<RecordBatch>> {
        let batch = input?;

        // Since dynamic filters may have been updated, see if we can stop
        // reading this stream entirely.
        if self.file_pruner.should_prune()? {
            self.files_ranges_pruned_statistics.add_pruned(1);
            // Previously this file range has been counted as matched
            self.files_ranges_pruned_statistics.subtract_matched(1);
            self.finish();
            Ok(None)
        } else {
            // Return the adapted batch
            Ok(Some(batch))
        }
    }
}

impl<S> Stream for EarlyStoppingStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match ready!(inner.poll_next_unpin(cx)) {
            None => {
                // input done
                self.finish();
                Poll::Ready(None)
            }
            Some(input_batch) => {
                let output = self.check_prune(input_batch);
                Poll::Ready(output.transpose())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{
        ColumnStatistics, ScalarValue, Statistics, stats::Precision,
    };
    use datafusion_datasource::PartitionedFile;
    use datafusion_physical_expr::PhysicalExpr;
    use datafusion_physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, Literal,
    };
    use datafusion_physical_plan::metrics::Count;
    use futures::stream;

    /// An inner stream that records when it is dropped, standing in for the
    /// decoder whose drop completes the scan's byte accounting.
    struct DropRecordingStream<S> {
        inner: S,
        dropped: Arc<AtomicBool>,
    }

    impl<S: Stream + Unpin> Stream for DropRecordingStream<S> {
        type Item = S::Item;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.inner.poll_next_unpin(cx)
        }
    }

    impl<S> Drop for DropRecordingStream<S> {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
    }

    /// A file whose only column holds values 1..=9, so a predicate demanding
    /// larger values prunes it outright.
    fn file_with_stats() -> PartitionedFile {
        // Built field by field rather than from `Statistics::new_unknown`, which
        // already seeds one entry per column, so that column 0 carries these
        // bounds rather than an unknown placeholder.
        let statistics = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::new_unknown()
                    .with_min_value(Precision::Exact(ScalarValue::Int32(Some(1))))
                    .with_max_value(Precision::Exact(ScalarValue::Int32(Some(9))))
                    .with_null_count(Precision::Exact(0)),
            ],
        };
        PartitionedFile::new("test.parquet".to_string(), 1_000)
            .with_statistics(Arc::new(statistics))
    }

    fn pruning_filter(schema: &SchemaRef) -> FilePruner {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            datafusion_expr::Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(100)))),
        ));
        let dynamic: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            expr.children().into_iter().map(Arc::clone).collect(),
            expr,
        ));
        FilePruner::try_new(dynamic, schema, &file_with_stats(), Count::new())
            .expect("file has statistics, so a pruner can be built")
    }

    fn batch(schema: &SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap()
    }

    /// Stopping early must release the inner stream there and then. The decoder's
    /// drop is what completes this file's byte-progress accounting, so holding it
    /// until the caller drops the wrapper would leave the scan reporting a file it
    /// has finished with as still partly unread.
    #[tokio::test]
    async fn stopping_early_releases_the_inner_stream() {
        let schema = schema();
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropRecordingStream {
            inner: stream::iter(vec![Ok(batch(&schema)), Ok(batch(&schema))]),
            dropped: Arc::clone(&dropped),
        };

        let mut early_stopping = EarlyStoppingStream::new(
            inner,
            pruning_filter(&schema),
            PruningMetrics::new(),
        );

        assert!(
            early_stopping.next().await.is_none(),
            "the filter prunes every row, so the first batch must end the stream",
        );
        assert!(
            dropped.load(Ordering::Relaxed),
            "the inner stream must be released when the scan stops, not when the \
             wrapper is eventually dropped",
        );
    }

    /// The same must hold when the inner stream simply runs out.
    #[tokio::test]
    async fn exhausting_the_inner_stream_releases_it() {
        let schema = schema();
        let dropped = Arc::new(AtomicBool::new(false));
        let inner = DropRecordingStream {
            inner: stream::iter(Vec::<Result<RecordBatch>>::new()),
            dropped: Arc::clone(&dropped),
        };

        let mut early_stopping = EarlyStoppingStream::new(
            inner,
            pruning_filter(&schema),
            PruningMetrics::new(),
        );

        assert!(early_stopping.next().await.is_none());
        assert!(
            dropped.load(Ordering::Relaxed),
            "an exhausted inner stream must be released too",
        );
    }
}
