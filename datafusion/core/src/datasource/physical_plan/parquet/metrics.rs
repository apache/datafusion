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

use crate::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, Time,
};

/// Stores metrics about the parquet execution for a particular parquet file.
///
/// This component is a subject to **change** in near future and is exposed for low level integrations
/// through [`ParquetFileReaderFactory`].
///
/// [`ParquetFileReaderFactory`]: super::ParquetFileReaderFactory
#[derive(Debug, Clone)]
pub struct ParquetFileMetrics {
    /// Number of times the predicate could not be evaluated
    pub predicate_evaluation_errors: Count,
    /// Number of row groups whose bloom filters were checked and matched (not pruned)
    pub row_groups_matched_bloom_filter: Count,
    /// Number of row groups pruned by bloom filters
    pub row_groups_pruned_bloom_filter: Count,
    /// Number of row groups whose statistics were checked and matched (not pruned)
    pub row_groups_matched_statistics: Count,
    /// Number of row groups pruned by statistics
    pub row_groups_pruned_statistics: Count,
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
    /// Total rows filtered out by parquet page index
    pub page_index_rows_pruned: Count,
    /// Total rows passed through the parquet page index
    pub page_index_rows_matched: Count,
    /// Total time spent evaluating parquet page index filters
    pub page_index_eval_time: Time,
    /// Total time spent reading and parsing metadata from the footer
    pub metadata_load_time: Time,
}

impl ParquetFileMetrics {
    /// Create new metrics
    pub fn new(
        partition: usize,
        filename: &str,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let predicate_evaluation_errors = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("predicate_evaluation_errors", partition);

        let row_groups_matched_bloom_filter = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_matched_bloom_filter", partition);

        let row_groups_pruned_bloom_filter = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_pruned_bloom_filter", partition);

        let row_groups_matched_statistics = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_matched_statistics", partition);

        let row_groups_pruned_statistics = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("row_groups_pruned_statistics", partition);

        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("bytes_scanned", partition);

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

        let page_index_rows_pruned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("page_index_rows_pruned", partition);
        let page_index_rows_matched = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("page_index_rows_matched", partition);

        let page_index_eval_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("page_index_eval_time", partition);

        let metadata_load_time = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .subset_time("metadata_load_time", partition);

        Self {
            predicate_evaluation_errors,
            row_groups_matched_bloom_filter,
            row_groups_pruned_bloom_filter,
            row_groups_matched_statistics,
            row_groups_pruned_statistics,
            bytes_scanned,
            pushdown_rows_pruned,
            pushdown_rows_matched,
            row_pushdown_eval_time,
            page_index_rows_pruned,
            page_index_rows_matched,
            statistics_eval_time,
            bloom_filter_eval_time,
            page_index_eval_time,
            metadata_load_time,
        }
    }
}
