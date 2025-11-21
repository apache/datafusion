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

use datafusion_physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricType, PruningMetrics,
    RatioMergeStrategy, RatioMetrics, Time,
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
    pub predicate_cache_inner_records: Count,
    /// Predicate Cache: number of records read from the cache. This is the
    /// number of rows that were stored in the cache after evaluating predicates
    /// reused for the output.
    pub predicate_cache_records: Count,
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
        let row_groups_pruned_bloom_filter = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("row_groups_pruned_bloom_filter", partition);

        let limit_pruned_row_groups = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("limit_pruned_row_groups", partition);

        let row_groups_pruned_statistics = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("row_groups_pruned_statistics", partition);

        let page_index_rows_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("page_index_rows_pruned", partition);

        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .counter("bytes_scanned", partition);

        let metadata_load_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .subset_time("metadata_load_time", partition);

        let files_ranges_pruned_statistics = MetricBuilder::new(metrics)
            .with_type(MetricType::SUMMARY)
            .pruning_metrics("files_ranges_pruned_statistics", partition);

        let scan_efficiency_ratio = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .with_type(MetricType::SUMMARY)
            .ratio_metrics_with_strategy(
                "scan_efficiency_ratio",
                partition,
                RatioMergeStrategy::AddPartSetTotal,
            );

        // -----------------------
        // 'dev' level metrics
        // -----------------------
        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let pushdown_rows_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("pushdown_rows_pruned", partition);
        let pushdown_rows_matched = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("pushdown_rows_matched", partition);

        let row_pushdown_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("row_pushdown_eval_time", partition);
        let statistics_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("statistics_eval_time", partition);
        let bloom_filter_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("bloom_filter_eval_time", partition);

        let page_index_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("page_index_eval_time", partition);

        let predicate_cache_inner_records = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_cache_inner_records", partition);

        let predicate_cache_records = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_cache_records", partition);

        Self {
            files_ranges_pruned_statistics,
            predicate_evaluation_errors,
            row_groups_pruned_bloom_filter,
            row_groups_pruned_statistics,
            limit_pruned_row_groups,
            bytes_scanned,
            pushdown_rows_pruned,
            pushdown_rows_matched,
            row_pushdown_eval_time,
            page_index_rows_pruned,
            statistics_eval_time,
            bloom_filter_eval_time,
            page_index_eval_time,
            metadata_load_time,
            scan_efficiency_ratio,
            predicate_cache_inner_records,
            predicate_cache_records,
        }
    }
}
