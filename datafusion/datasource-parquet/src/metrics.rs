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
    Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricCategory, MetricType,
    MetricValue, PruningMetrics, RatioMergeStrategy, RatioMetrics, Time,
};

/// Stores metrics about the parquet execution for a particular parquet file.
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
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
    /// Keeps the lazy registration guard alive. The guard's [`Drop`] impl is
    /// what registers the non-zero metric handles into the plan metrics set.
    _registration: Arc<ParquetFileMetricRegistration>,
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        // -----------------------
        // 'summary' level metrics
        // -----------------------
        let row_groups_pruned_bloom_filter = PruningMetrics::new();
        let limit_pruned_row_groups = PruningMetrics::new();
        let row_groups_pruned_statistics = PruningMetrics::new();
        let page_index_pages_pruned = PruningMetrics::new();
        let bytes_scanned = Count::new();
        let metadata_load_time = Time::new();
        let files_ranges_pruned_statistics = PruningMetrics::new();
        let scan_efficiency_ratio =
            RatioMetrics::new().with_merge_strategy(RatioMergeStrategy::AddPartSetTotal);

        // -----------------------
        // 'dev' level metrics
        // -----------------------
        let predicate_evaluation_errors = Count::new();
        let pushdown_rows_pruned = Count::new();
        let pushdown_rows_matched = Count::new();
        let row_pushdown_eval_time = Time::new();
        let statistics_eval_time = Time::new();
        let bloom_filter_eval_time = Time::new();
        let page_index_eval_time = Time::new();
        let page_index_rows_pruned = PruningMetrics::new();
        let predicate_cache_inner_records = Gauge::new();
        let predicate_cache_records = Gauge::new();

        // The returned metrics and the registration guard both need handles to
        // the same shared metric state: scanners update the returned handles,
        // and the guard reads those values on drop to register non-zero metrics.
        Self {
            files_ranges_pruned_statistics: files_ranges_pruned_statistics.clone(),
            predicate_evaluation_errors: predicate_evaluation_errors.clone(),
            row_groups_pruned_bloom_filter: row_groups_pruned_bloom_filter.clone(),
            row_groups_pruned_statistics: row_groups_pruned_statistics.clone(),
            limit_pruned_row_groups: limit_pruned_row_groups.clone(),
            bytes_scanned: bytes_scanned.clone(),
            pushdown_rows_pruned: pushdown_rows_pruned.clone(),
            pushdown_rows_matched: pushdown_rows_matched.clone(),
            row_pushdown_eval_time: row_pushdown_eval_time.clone(),
            page_index_rows_pruned: page_index_rows_pruned.clone(),
            page_index_pages_pruned: page_index_pages_pruned.clone(),
            statistics_eval_time: statistics_eval_time.clone(),
            bloom_filter_eval_time: bloom_filter_eval_time.clone(),
            page_index_eval_time: page_index_eval_time.clone(),
            metadata_load_time: metadata_load_time.clone(),
            scan_efficiency_ratio: scan_efficiency_ratio.clone(),
            predicate_cache_inner_records: predicate_cache_inner_records.clone(),
            predicate_cache_records: predicate_cache_records.clone(),
            _registration: Arc::new(ParquetFileMetricRegistration {
                metrics: metrics.clone(),
                partition,
                filename: filename.to_string(),
                row_groups_pruned_bloom_filter,
                limit_pruned_row_groups,
                row_groups_pruned_statistics,
                page_index_pages_pruned,
                bytes_scanned,
                metadata_load_time,
                files_ranges_pruned_statistics,
                scan_efficiency_ratio,
                predicate_evaluation_errors,
                pushdown_rows_pruned,
                pushdown_rows_matched,
                row_pushdown_eval_time,
                statistics_eval_time,
                bloom_filter_eval_time,
                page_index_eval_time,
                page_index_rows_pruned,
                predicate_cache_inner_records,
                predicate_cache_records,
            }),
        }
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
}

#[derive(Debug)]
struct ParquetFileMetricRegistration {
    metrics: ExecutionPlanMetricsSet,
    partition: usize,
    filename: String,
    row_groups_pruned_bloom_filter: PruningMetrics,
    limit_pruned_row_groups: PruningMetrics,
    row_groups_pruned_statistics: PruningMetrics,
    page_index_pages_pruned: PruningMetrics,
    bytes_scanned: Count,
    metadata_load_time: Time,
    files_ranges_pruned_statistics: PruningMetrics,
    scan_efficiency_ratio: RatioMetrics,
    predicate_evaluation_errors: Count,
    pushdown_rows_pruned: Count,
    pushdown_rows_matched: Count,
    row_pushdown_eval_time: Time,
    statistics_eval_time: Time,
    bloom_filter_eval_time: Time,
    page_index_eval_time: Time,
    page_index_rows_pruned: PruningMetrics,
    predicate_cache_inner_records: Gauge,
    predicate_cache_records: Gauge,
}

impl Drop for ParquetFileMetricRegistration {
    fn drop(&mut self) {
        // -----------------------
        // 'summary' level metrics
        // -----------------------
        self.register_pruning(
            "row_groups_pruned_bloom_filter",
            &self.row_groups_pruned_bloom_filter,
            MetricType::Summary,
            true,
        );
        self.register_pruning(
            "limit_pruned_row_groups",
            &self.limit_pruned_row_groups,
            MetricType::Summary,
            true,
        );
        self.register_pruning(
            "row_groups_pruned_statistics",
            &self.row_groups_pruned_statistics,
            MetricType::Summary,
            true,
        );
        self.register_pruning(
            "page_index_pages_pruned",
            &self.page_index_pages_pruned,
            MetricType::Summary,
            true,
        );
        self.register_count(
            "bytes_scanned",
            &self.bytes_scanned,
            MetricType::Summary,
            MetricCategory::Bytes,
            true,
        );
        self.register_time(
            "metadata_load_time",
            &self.metadata_load_time,
            MetricType::Summary,
            true,
        );
        self.register_pruning(
            "files_ranges_pruned_statistics",
            &self.files_ranges_pruned_statistics,
            MetricType::Summary,
            false,
        );
        self.register_ratio(
            "scan_efficiency_ratio",
            &self.scan_efficiency_ratio,
            MetricType::Summary,
            true,
        );

        // -----------------------
        // 'dev' level metrics
        // -----------------------
        self.register_count(
            "predicate_evaluation_errors",
            &self.predicate_evaluation_errors,
            MetricType::Dev,
            MetricCategory::Rows,
            true,
        );
        self.register_count(
            "pushdown_rows_pruned",
            &self.pushdown_rows_pruned,
            MetricType::Dev,
            MetricCategory::Rows,
            true,
        );
        self.register_count(
            "pushdown_rows_matched",
            &self.pushdown_rows_matched,
            MetricType::Dev,
            MetricCategory::Rows,
            true,
        );
        self.register_time(
            "row_pushdown_eval_time",
            &self.row_pushdown_eval_time,
            MetricType::Dev,
            true,
        );
        self.register_time(
            "statistics_eval_time",
            &self.statistics_eval_time,
            MetricType::Dev,
            true,
        );
        self.register_time(
            "bloom_filter_eval_time",
            &self.bloom_filter_eval_time,
            MetricType::Dev,
            true,
        );
        self.register_time(
            "page_index_eval_time",
            &self.page_index_eval_time,
            MetricType::Dev,
            true,
        );
        self.register_pruning(
            "page_index_rows_pruned",
            &self.page_index_rows_pruned,
            MetricType::Dev,
            true,
        );
        self.register_gauge(
            "predicate_cache_inner_records",
            &self.predicate_cache_inner_records,
            MetricType::Dev,
            MetricCategory::Rows,
            true,
        );
        self.register_gauge(
            "predicate_cache_records",
            &self.predicate_cache_records,
            MetricType::Dev,
            MetricCategory::Rows,
            true,
        );
    }
}

impl ParquetFileMetricRegistration {
    fn builder(
        &self,
        metric_type: MetricType,
        metric_category: MetricCategory,
        filename_label: bool,
    ) -> MetricBuilder<'_> {
        let builder = MetricBuilder::new(&self.metrics)
            .with_type(metric_type)
            .with_category(metric_category);
        if filename_label {
            builder.with_new_label("filename", self.filename.clone())
        } else {
            builder
        }
    }

    fn register_count(
        &self,
        name: &'static str,
        count: &Count,
        metric_type: MetricType,
        metric_category: MetricCategory,
        filename_label: bool,
    ) {
        if count.value() == 0 {
            return;
        }

        self.builder(metric_type, metric_category, filename_label)
            .with_partition(self.partition)
            .build(MetricValue::Count {
                name: name.into(),
                count: count.clone(),
            });
    }

    fn register_gauge(
        &self,
        name: &'static str,
        gauge: &Gauge,
        metric_type: MetricType,
        metric_category: MetricCategory,
        filename_label: bool,
    ) {
        if gauge.value() == 0 {
            return;
        }

        self.builder(metric_type, metric_category, filename_label)
            .with_partition(self.partition)
            .build(MetricValue::Gauge {
                name: name.into(),
                gauge: gauge.clone(),
            });
    }

    fn register_time(
        &self,
        name: &'static str,
        time: &Time,
        metric_type: MetricType,
        filename_label: bool,
    ) {
        if time.value() == 0 {
            return;
        }

        self.builder(metric_type, MetricCategory::Timing, filename_label)
            .with_partition(self.partition)
            .build(MetricValue::Time {
                name: name.into(),
                time: time.clone(),
            });
    }

    fn register_pruning(
        &self,
        name: &'static str,
        pruning_metrics: &PruningMetrics,
        metric_type: MetricType,
        filename_label: bool,
    ) {
        if pruning_metrics.pruned() == 0
            && pruning_metrics.matched() == 0
            && pruning_metrics.fully_matched() == 0
        {
            return;
        }

        self.builder(metric_type, MetricCategory::Rows, filename_label)
            .with_partition(self.partition)
            .build(MetricValue::PruningMetrics {
                name: name.into(),
                pruning_metrics: pruning_metrics.clone(),
            });
    }

    fn register_ratio(
        &self,
        name: &'static str,
        ratio_metrics: &RatioMetrics,
        metric_type: MetricType,
        filename_label: bool,
    ) {
        if ratio_metrics.part() == 0 && ratio_metrics.total() == 0 {
            return;
        }

        self.builder(metric_type, MetricCategory::Rows, filename_label)
            .with_partition(self.partition)
            .build(MetricValue::Ratio {
                name: name.into(),
                ratio_metrics: ratio_metrics.clone(),
            });
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn parquet_file_metrics_skip_zero_metrics_on_drop() {
        let metrics = ExecutionPlanMetricsSet::new();

        {
            let _file_metrics = ParquetFileMetrics::new(0, "file.parquet", &metrics);
        }

        assert!(
            metrics.clone_inner().iter().next().is_none(),
            "zero-value parquet file metrics should not be registered"
        );
    }

    #[test]
    fn parquet_file_metrics_register_non_zero_metrics_on_drop() {
        let metrics = ExecutionPlanMetricsSet::new();

        {
            let file_metrics = ParquetFileMetrics::new(0, "file.parquet", &metrics);
            assert!(metrics.clone_inner().sum_by_name("bytes_scanned").is_none());

            file_metrics.bytes_scanned.add(42);
            file_metrics.predicate_cache_records.set(9);
            file_metrics
                .metadata_load_time
                .add_duration(Duration::from_nanos(7));
            file_metrics.row_groups_pruned_statistics.add_pruned(2);
            file_metrics.row_groups_pruned_statistics.add_matched(5);
            file_metrics.page_index_rows_pruned.add_fully_matched(3);
            file_metrics.scan_efficiency_ratio.add_total(100);
        }

        let metrics = metrics.clone_inner();
        assert_eq!(
            metrics.sum_by_name("bytes_scanned").map(|v| v.as_usize()),
            Some(42)
        );
        assert_eq!(
            metrics
                .sum_by_name("predicate_cache_records")
                .map(|v| v.as_usize()),
            Some(9)
        );
        assert_eq!(
            metrics
                .sum_by_name("metadata_load_time")
                .map(|v| v.as_usize()),
            Some(7)
        );

        match metrics
            .sum_by_name("row_groups_pruned_statistics")
            .expect("row_groups_pruned_statistics registered")
        {
            MetricValue::PruningMetrics {
                pruning_metrics, ..
            } => {
                assert_eq!(pruning_metrics.pruned(), 2);
                assert_eq!(pruning_metrics.matched(), 5);
                assert_eq!(pruning_metrics.fully_matched(), 0);
            }
            metric => panic!("unexpected metric type: {metric:?}"),
        }

        match metrics
            .sum_by_name("page_index_rows_pruned")
            .expect("page_index_rows_pruned registered")
        {
            MetricValue::PruningMetrics {
                pruning_metrics, ..
            } => {
                assert_eq!(pruning_metrics.pruned(), 0);
                assert_eq!(pruning_metrics.matched(), 0);
                assert_eq!(pruning_metrics.fully_matched(), 3);
            }
            metric => panic!("unexpected metric type: {metric:?}"),
        }

        match metrics
            .sum_by_name("scan_efficiency_ratio")
            .expect("scan_efficiency_ratio registered")
        {
            MetricValue::Ratio { ratio_metrics, .. } => {
                assert_eq!(ratio_metrics.part(), 0);
                assert_eq!(ratio_metrics.total(), 100);
            }
            metric => panic!("unexpected metric type: {metric:?}"),
        }

        assert!(metrics.sum_by_name("predicate_evaluation_errors").is_none());
    }
}
