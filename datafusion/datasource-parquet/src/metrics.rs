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

use std::sync::Arc;

use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Gauge, Label, MetricBuilder, MetricCategory,
    MetricType, PruningMetrics, RatioMergeStrategy, RatioMetrics, Time,
};

/// Stores metrics about the parquet execution for a particular parquet file.
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
///
/// # The `bytes_processed` metric
///
/// Not every metric the parquet scan reports is a field on this struct. `bytes_processed` — the
/// number of bytes the scan is finished with, whether it read them or proved by pruning that it
/// did not need them — is registered straight onto the plan's metrics set, because only the
/// internal progress guard ever touches it and a public field here would make every future
/// metric a breaking change for anyone building this struct with a literal.
///
/// Read it the way `EXPLAIN ANALYZE` does, by name off the plan's metrics:
///
/// ```no_run
/// # use datafusion_physical_plan::ExecutionPlan;
/// # fn completion_fraction(plan: &dyn ExecutionPlan, total_file_bytes: u64) -> Option<f64> {
/// let processed = plan.metrics()?.sum_by_name("bytes_processed")?.as_usize();
/// Some(processed as f64 / total_file_bytes as f64)
/// # }
/// ```
///
/// Over the life of a file — or of one byte range of a file split for parallelism — it advances
/// by exactly that file's size, which is what makes the ratio above a completion fraction rather
/// than just another counter. Note that it measures work *resolved*, not time spent: bytes that
/// pruning removes are credited the moment they are proved unnecessary, and proving that is
/// nearly free. It is the right numerator for a progress bar, and only a rough one for
/// predicting how much longer a query will take.
///
/// [`ParquetFileReaderFactory`]: super::ParquetFileReaderFactory
#[derive(Debug, Clone)]
pub struct ParquetFileMetrics {
    /// Number of file **ranges** pruned or matched by partition or file level statistics.
    /// Pruning of files often happens at planning time but may happen at execution time
    /// if dynamic filters (e.g. from a join) result in additional pruning.
    ///
    /// This does **not** necessarily equal the number of files pruned:
    /// files may be scanned in sub-ranges to increase parallelism,
    /// in which case this will represent the number of sub-ranges pruned, not the number of files.
    /// The number of files pruned will always be less than or equal to this number.
    ///
    /// A single file may have some ranges that are not pruned and some that are pruned.
    /// For example, with a query like `ORDER BY col LIMIT 10`, the TopK dynamic filter
    /// pushdown optimization may fill up the TopK heap when reading the first part of a file,
    /// then skip the second part if file statistics indicate it cannot contain rows
    /// that would be in the TopK.
    pub files_ranges_pruned_statistics: PruningMetrics,
    /// Number of times the predicate could not be evaluated
    pub predicate_evaluation_errors: Count,
    /// Number of row groups pruned by bloom filters
    pub row_groups_pruned_bloom_filter: PruningMetrics,
    /// Number of row groups pruned due to limit pruning.
    pub limit_pruned_row_groups: PruningMetrics,
    /// Number of row groups pruned by statistics
    pub row_groups_pruned_statistics: PruningMetrics,
    /// Number of row groups pruned at runtime by a dynamic predicate
    /// (e.g. the threshold expression a TopK `SortExec` pushes down).
    ///
    /// Unlike [`Self::row_groups_pruned_statistics`], which is decided once
    /// at access-plan time, this counter reflects row groups that survived
    /// the initial pruning but were proved unreachable mid-scan after the
    /// dynamic filter tightened.
    pub row_groups_pruned_dynamic_filter: Count,
    /// Total number of bytes scanned
    pub bytes_scanned: Count,
    /// Total rows filtered out by predicates pushed into parquet scan
    pub pushdown_rows_pruned: Count,
    /// Total rows passed predicates pushed into parquet scan
    pub pushdown_rows_matched: Count,
    /// Total time spent evaluating row-level pushdown filters
    pub row_pushdown_eval_time: Time,
    /// Total time spent evaluating row group-level statistics filters
    pub statistics_eval_time: Time,
    /// Total time spent evaluating row group Bloom Filters
    pub bloom_filter_eval_time: Time,
    /// Total rows filtered or matched by parquet page index
    pub page_index_rows_pruned: PruningMetrics,
    /// Total pages filtered or matched by parquet page index
    pub page_index_pages_pruned: PruningMetrics,
    /// Total time spent evaluating parquet page index filters
    pub page_index_eval_time: Time,
    /// Total time spent reading and parsing metadata from the footer
    pub metadata_load_time: Time,
    /// Scan Efficiency Ratio, calculated as bytes_scanned / total_file_size
    pub scan_efficiency_ratio: RatioMetrics,
    /// Predicate Cache: Total number of rows physically read and decoded from the Parquet file.
    ///
    /// This metric tracks "cache misses" in the predicate pushdown optimization.
    /// When the specialized predicate reader cannot find the requested data in its cache,
    /// it must fall back to the "inner reader" to physically decode the data from the
    /// Parquet.
    ///
    /// This is the expensive path (IO + Decompression + Decoding).
    ///
    /// We use a Gauge here as arrow-rs reports absolute numbers rather
    /// than incremental readings, we want a `set` operation here rather
    /// than `add`. Earlier it was `Count`, which led to this issue:
    /// github.com/apache/datafusion/issues/19334
    pub predicate_cache_inner_records: Gauge,
    /// Predicate Cache: number of records read from the cache. This is the
    /// number of rows that were stored in the cache after evaluating predicates
    /// reused for the output.
    pub predicate_cache_records: Gauge,
}

/// Tracks how much of one file — or one byte range of a file — a scan has
/// finished with, crediting [`ParquetFileMetrics`]'s `bytes_processed` as it goes.
///
/// Every credit is clamped to the bytes left in the budget, and whatever is
/// left over is credited on drop. The counter therefore advances by exactly the
/// size of the range being scanned however the scan ends: normally, at a
/// `LIMIT`, when a dynamic filter proves the rest of the file irrelevant, or on
/// an error — including one that stops the file being opened at all, which is
/// why the guard is created before the fallible stages of opening rather than
/// alongside the decoder. That total is what makes the metric usable as a
/// completion fraction rather than just another counter.
///
/// The clamp and the final top-up also absorb two small inexactnesses in
/// crediting by row group: a file is slightly larger than the sum of its row
/// groups (the footer, the page index and any padding belong to no row group),
/// and a row group is assigned to a byte range by the offset of its first page,
/// so a range's row groups do not add up to precisely its length.
///
/// The budget is held as a `usize` because [`Count`] is, so the two cannot
/// disagree: on a 32-bit target a range longer than `usize::MAX` saturates once,
/// here, rather than letting the remaining-byte arithmetic run ahead of what the
/// counter can record. [`ParquetFileMetrics::bytes_scanned`] has the same
/// ceiling.
#[derive(Debug)]
pub(crate) struct ByteProgress {
    /// Bytes of the scanned range not yet credited.
    remaining: usize,
    bytes_processed: Count,
}

impl ByteProgress {
    /// Start tracking a range of `total` bytes.
    pub(crate) fn new(total: u64, bytes_processed: Count) -> Self {
        Self {
            remaining: saturating_usize(total),
            bytes_processed,
        }
    }

    /// Record that the scan is finished with `bytes` more of the range.
    pub(crate) fn credit(&mut self, bytes: u64) {
        let bytes = saturating_usize(bytes).min(self.remaining);
        self.remaining -= bytes;
        self.bytes_processed.add(bytes);
    }
}

/// Narrow a byte count to the width [`Count`] stores, saturating rather than
/// wrapping. Lossless on 64-bit targets.
fn saturating_usize(bytes: u64) -> usize {
    usize::try_from(bytes).unwrap_or(usize::MAX)
}

impl Drop for ByteProgress {
    fn drop(&mut self) {
        let remaining = self.remaining;
        self.remaining = 0;
        self.bytes_processed.add(remaining);
    }
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // Share the filename label across all per-file metrics to avoid
        // allocating the same filename string for each metric.
        let filename_label = Label::new("filename", Arc::<str>::from(filename));
        let builder = MetricBuilder::new(metrics).with_label(filename_label);

        // -----------------------
        // 'summary' level metrics
        // -----------------------
        let row_groups_pruned_bloom_filter = builder
            .clone()
            .with_type(MetricType::Summary)
            .pruning_metrics("row_groups_pruned_bloom_filter", partition);

        let limit_pruned_row_groups = builder
            .clone()
            .with_type(MetricType::Summary)
            .pruning_metrics("limit_pruned_row_groups", partition);

        let row_groups_pruned_statistics = builder
            .clone()
            .with_type(MetricType::Summary)
            .pruning_metrics("row_groups_pruned_statistics", partition);

        let page_index_pages_pruned = builder
            .clone()
            .with_type(MetricType::Summary)
            .pruning_metrics("page_index_pages_pruned", partition);

        let bytes_scanned = builder
            .clone()
            .with_type(MetricType::Summary)
            .bytes_counter("bytes_scanned", partition);

        let metadata_load_time = builder
            .clone()
            .with_type(MetricType::Summary)
            .subset_time("metadata_load_time", partition);

        let files_ranges_pruned_statistics = MetricBuilder::new(metrics)
            .with_type(MetricType::Summary)
            .pruning_metrics("files_ranges_pruned_statistics", partition);

        let scan_efficiency_ratio = builder
            .clone()
            .with_type(MetricType::Summary)
            .ratio_metrics_with_strategy(
                "scan_efficiency_ratio",
                partition,
                RatioMergeStrategy::AddPartSetTotal,
            );

        // -----------------------
        // 'dev' level metrics
        // -----------------------
        let predicate_evaluation_errors = builder
            .clone()
            .with_category(MetricCategory::Rows)
            .counter("predicate_evaluation_errors", partition);

        let pushdown_rows_pruned = builder
            .clone()
            .with_category(MetricCategory::Rows)
            .counter("pushdown_rows_pruned", partition);
        let pushdown_rows_matched = builder
            .clone()
            .with_category(MetricCategory::Rows)
            .counter("pushdown_rows_matched", partition);

        let row_pushdown_eval_time = builder
            .clone()
            .subset_time("row_pushdown_eval_time", partition);
        let statistics_eval_time = builder
            .clone()
            .subset_time("statistics_eval_time", partition);
        let bloom_filter_eval_time = builder
            .clone()
            .subset_time("bloom_filter_eval_time", partition);

        let page_index_eval_time = builder
            .clone()
            .subset_time("page_index_eval_time", partition);

        let page_index_rows_pruned = builder
            .clone()
            .pruning_metrics("page_index_rows_pruned", partition);

        let predicate_cache_inner_records = builder
            .clone()
            .with_category(MetricCategory::Rows)
            .gauge("predicate_cache_inner_records", partition);

        let predicate_cache_records = builder
            .with_category(MetricCategory::Rows)
            .gauge("predicate_cache_records", partition);

        let row_groups_pruned_dynamic_filter = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::Summary)
            .counter("row_groups_pruned_dynamic_filter", partition);

        Self {
            files_ranges_pruned_statistics,
            predicate_evaluation_errors,
            row_groups_pruned_bloom_filter,
            limit_pruned_row_groups,
            row_groups_pruned_statistics,
            row_groups_pruned_dynamic_filter,
            bytes_scanned,
            pushdown_rows_pruned,
            pushdown_rows_matched,
            row_pushdown_eval_time,
            statistics_eval_time,
            bloom_filter_eval_time,
            page_index_rows_pruned,
            page_index_pages_pruned,
            page_index_eval_time,
            metadata_load_time,
            scan_efficiency_ratio,
            predicate_cache_inner_records,
            predicate_cache_records,
        }
    }

    /// The `bytes_processed` counter for one file: the total number of bytes the
    /// scan is finished with, whether they were read or skipped.
    ///
    /// Where [`Self::bytes_scanned`] counts only the bytes fetched from the
    /// object store, this counts every byte the scan has resolved: the bytes it
    /// read, plus the bytes of the row groups (and whole files) that pruning
    /// proved cannot contribute. Over the lifetime of a file it therefore sums
    /// to that file's size — or, for a file split into byte ranges for
    /// parallelism, to the size of the range — so
    /// `bytes_processed / total file bytes` is a scan completion fraction,
    /// which `bytes_scanned` on its own is not: it understates progress by
    /// however much pruning and projection pushdown saved.
    ///
    /// Credited at row-group granularity: a row group's bytes land when the
    /// scan is done with it. Crediting a row group's bytes progressively as its
    /// rows are decoded is left to a follow-up.
    ///
    /// Built on demand rather than held on [`ParquetFileMetrics`] because only
    /// [`ByteProgress`] ever touches it, and a public field would make every
    /// future metric added here a breaking change for anyone constructing the
    /// struct with a literal.
    pub(crate) fn bytes_processed_counter(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        filename: &str,
    ) -> Count {
        MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::Summary)
            .bytes_counter("bytes_processed", partition)
    }

    /// Record pages whose page-index pruning was skipped because the containing
    /// row group was fully matched by row-group statistics.
    ///
    /// The counter is only registered when there is a non-zero value. This keeps
    /// [`ParquetFileMetrics::new`] from cloning the filename and metrics set for
    /// files that never use this metric.
    pub(crate) fn add_page_index_pages_skipped_by_fully_matched(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        filename: &str,
        n: usize,
    ) {
        if n == 0 {
            return;
        }

        let count = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::Summary)
            .with_category(MetricCategory::Rows)
            .counter("page_index_pages_skipped_by_fully_matched", partition);
        count.add(n);
    }

    /// Record that page index I/O was skipped because row-group statistics
    /// already proved page index could not prune further.
    pub(crate) fn add_page_index_load_skipped(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        filename: &str,
        n: usize,
    ) {
        if n == 0 {
            return;
        }

        let count = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::Summary)
            .counter("page_index_load_skipped", partition);
        count.add(n);
    }
}

/// Lazily-registered counter for `row_filter_skipped_fully_matched`: the
/// number of times the per-row
/// [`RowFilter`](parquet::arrow::arrow_reader::RowFilter) was suppressed
/// because static stats proved every row of the upcoming row group(s)
/// satisfies the predicate.
///
/// Like [`ParquetFileMetrics::add_page_index_pages_skipped_by_fully_matched`],
/// the counter is only registered when it first fires, so scans that never
/// suppress a row filter don't carry a zero-valued counter in
/// `EXPLAIN ANALYZE` (and `ParquetFileMetrics` keeps no public field for it).
/// Unlike that fire-once helper, the decode stream records suppressions as
/// they happen, so this holder keeps a live [`Count`] handle after the first
/// registration.
///
/// Note this counts *suppression events*, not row groups: a run of
/// consecutive fully-matched row groups shares a single toggle (the filter
/// stays off across the run with no further rebuilds).
pub(crate) struct RowFilterSkippedFullyMatchedMetric {
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    filename: String,
    count: Option<Count>,
}

impl RowFilterSkippedFullyMatchedMetric {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
        filename: &str,
    ) -> Self {
        Self {
            metrics: metrics.clone(),
            partition,
            filename: filename.to_string(),
            count: None,
        }
    }

    /// Record one suppression, registering the counter on first use.
    pub(crate) fn add_one(&mut self) {
        let count = self.count.get_or_insert_with(|| {
            MetricBuilder::new(&self.metrics)
                .with_new_label("filename", self.filename.clone())
                .with_type(MetricType::Summary)
                .with_category(MetricCategory::Rows)
                .counter("row_filter_skipped_fully_matched", self.partition)
        });
        count.add(1);
    }
}
