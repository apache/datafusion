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

//! Eager pruning: pruning parquet files while a scan is planned.
//!
//! Pruning normally happens only when a parquet scan executes, so during
//! planning the scan reports statistics for entire files, even when a filter
//! can skip most of their data. Eager pruning (see [`EagerParquetPruning`])
//! runs the same row group, page index and bloom filter pruning while the scan
//! is planned, and uses the results to:
//!
//! * drop files in which no row can match the filters,
//! * attach the resulting [`ParquetAccessPlan`] to each file, so the scan
//!   starts from the already pruned plan, and
//! * tighten the (always inexact) statistics of the files and of the scan.
//!
//! The access plan attached to a file never marks row groups as fully
//! matched: the scan may execute with a different (e.g. dynamic) predicate,
//! so it re-evaluates its own predicate starting from the attached plan.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Schema, TimeUnit};
use datafusion_common::config::{EagerParquetPruning, TableParquetOptions};
use datafusion_common::stats::{NdvFallback, Precision};
use datafusion_common::{ColumnStatistics, Result, ScalarValue, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_physical_expr::conjunction;
use datafusion_physical_expr::simplifier::PhysicalExprSimplifier;
use datafusion_physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory, PhysicalExprAdapterFactory,
    replace_columns_with_literals,
};
use datafusion_physical_plan::PhysicalExpr;
use datafusion_physical_plan::metrics::{Count, ExecutionPlanMetricsSet};
use futures::StreamExt;
use log::debug;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::{PageIndexPolicy, RowGroupMetaData};

use crate::access_plan::RowGroupAccess;
use crate::bloom_filter::load_row_group_bloom_filters;
use crate::metadata::DFParquetMetadata;
use crate::opener::{
    build_page_pruning_predicate, build_pruning_predicates, constant_columns_from_stats,
    create_initial_plan, load_page_index,
};
use crate::row_group_filter::RowGroupAccessPlanFilter;
use crate::schema_coercion::coerce_physical_file_schema;
use crate::source::{parse_coerce_int96_string, parse_coerce_int96_tz_string};
use crate::{
    ParquetAccessPlan, ParquetFileMetrics, ParquetFileReaderFactory, ParquetRowSelection,
};

/// The outcome of eager pruning for a parquet scan.
///
/// See [`EagerParquetPruning`]. Shown in the `EXPLAIN` output of the scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EagerPruningSummary {
    /// Eager pruning was enabled but not performed
    Skipped {
        /// The configured eager pruning level
        level: EagerParquetPruning,
        /// Why eager pruning was not performed
        reason: String,
    },
    /// Eager pruning was performed
    Pruned(EagerPruningStats),
}

/// Counts describing the work done by eager pruning.
///
/// Row group and row counts only include files that were evaluated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EagerPruningStats {
    /// The configured eager pruning level
    pub level: EagerParquetPruning,
    /// Number of files in the scan
    pub files: usize,
    /// Number of files that were not evaluated and left unchanged, for
    /// example because their metadata could not be read
    pub files_not_evaluated: usize,
    /// Number of files removed from the scan because no row can match
    pub files_pruned: usize,
    /// Number of row groups in the evaluated files
    pub row_groups: usize,
    /// Number of row groups that will not be read
    pub row_groups_pruned: usize,
    /// Number of rows in the evaluated files
    pub rows: usize,
    /// Number of rows that will not be read
    pub rows_pruned: usize,
}

impl fmt::Display for EagerPruningSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Skipped { level, reason } => {
                write!(f, "[level={level}, skipped={reason}]")
            }
            Self::Pruned(stats) => {
                let EagerPruningStats {
                    level,
                    files,
                    files_not_evaluated,
                    files_pruned,
                    row_groups,
                    row_groups_pruned,
                    rows,
                    rows_pruned,
                } = stats;
                write!(
                    f,
                    "[level={level}, files_pruned={files_pruned}/{files}, \
                     row_groups_pruned={row_groups_pruned}/{row_groups}, \
                     rows_pruned={rows_pruned}/{rows}"
                )?;
                if *files_not_evaluated > 0 {
                    write!(f, ", files_not_evaluated={files_not_evaluated}")?;
                }
                write!(f, "]")
            }
        }
    }
}

/// Performs eager pruning for a parquet scan.
pub(crate) struct EagerPruner {
    level: EagerParquetPruning,
    /// The conjunction of the scan's filters, against the table schema
    predicate: Arc<dyn PhysicalExpr>,
    options: TableParquetOptions,
    reader_factory: Arc<dyn ParquetFileReaderFactory>,
    concurrency: usize,
}

/// The result of eager pruning a single file
enum FileOutcome {
    /// The file was not evaluated and is kept unchanged
    NotEvaluated(PartitionedFile),
    /// The file was evaluated
    Evaluated {
        /// The file to scan, or `None` if no row in the file can match
        file: Option<PartitionedFile>,
        /// Whether the file changed, either what is read from it or its
        /// statistics
        changed: bool,
        row_groups: usize,
        row_groups_pruned: usize,
        rows: usize,
        rows_pruned: usize,
    },
}

/// Per scan state needed to prune individual files
struct ScanContext<'a> {
    table_schema: &'a TableSchema,
    expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
    coerce_int96: Option<TimeUnit>,
    coerce_int96_tz: Option<Arc<str>>,
}

impl EagerPruner {
    /// Create a pruner for a scan with the given `filters`.
    ///
    /// Returns `None` if eager pruning is disabled or there is nothing to prune
    /// with.
    pub(crate) fn try_new(
        options: &TableParquetOptions,
        filters: &[Arc<dyn PhysicalExpr>],
        reader_factory: Arc<dyn ParquetFileReaderFactory>,
        concurrency: usize,
    ) -> Option<Self> {
        let level = options.global.eager_pruning;
        if level == EagerParquetPruning::Disabled || filters.is_empty() {
            return None;
        }
        Some(Self {
            level,
            predicate: conjunction(filters.iter().cloned()),
            options: options.clone(),
            reader_factory,
            concurrency: concurrency.max(1),
        })
    }

    /// Eagerly prune the files of `conf`.
    ///
    /// Returns the updated configuration and a summary of the work done.
    /// Errors pruning individual files are not returned: such files are left
    /// unchanged.
    pub(crate) async fn prune(
        &self,
        table_schema: &TableSchema,
        conf: FileScanConfig,
    ) -> Result<(FileScanConfig, EagerPruningSummary)> {
        let num_files: usize = conf.file_groups.iter().map(FileGroup::len).sum();
        if let Some(reason) = self.skip_reason(table_schema, num_files) {
            debug!("Skipping eager parquet pruning: {reason}");
            let summary = EagerPruningSummary::Skipped {
                level: self.level,
                reason,
            };
            return Ok((conf, summary));
        }

        let global = &self.options.global;
        let context = ScanContext {
            table_schema,
            expr_adapter_factory: conf
                .expr_adapter_factory
                .clone()
                .unwrap_or_else(|| Arc::new(DefaultPhysicalExprAdapterFactory) as _),
            coerce_int96: global
                .coerce_int96
                .as_deref()
                .map(parse_coerce_int96_string)
                .transpose()?,
            coerce_int96_tz: global
                .coerce_int96_tz
                .as_deref()
                .map(parse_coerce_int96_tz_string)
                .transpose()?,
        };

        let group_lens: Vec<usize> =
            conf.file_groups.iter().map(FileGroup::len).collect();
        let files = conf
            .file_groups
            .iter()
            .flat_map(|group| group.iter().cloned())
            .collect::<Vec<_>>();
        let outcomes: Vec<FileOutcome> = futures::stream::iter(files)
            .map(|file| self.prune_file_or_keep(&context, file))
            .buffered(self.concurrency)
            .collect()
            .await;

        let mut stats = EagerPruningStats {
            level: self.level,
            files: num_files,
            ..Default::default()
        };
        let mut changed = false;
        let mut outcomes = outcomes.into_iter();
        let mut file_groups = Vec::with_capacity(group_lens.len());
        for len in group_lens {
            let mut files = Vec::with_capacity(len);
            for outcome in outcomes.by_ref().take(len) {
                match outcome {
                    FileOutcome::NotEvaluated(file) => {
                        stats.files_not_evaluated += 1;
                        files.push(file);
                    }
                    FileOutcome::Evaluated {
                        file,
                        changed: file_changed,
                        row_groups,
                        row_groups_pruned,
                        rows,
                        rows_pruned,
                    } => {
                        changed |= file_changed;
                        stats.row_groups += row_groups;
                        stats.row_groups_pruned += row_groups_pruned;
                        stats.rows += rows;
                        stats.rows_pruned += rows_pruned;
                        match file {
                            Some(file) => files.push(file),
                            None => stats.files_pruned += 1,
                        }
                    }
                }
            }
            file_groups.push(files);
        }

        let summary = EagerPruningSummary::Pruned(stats);
        if !changed {
            // Nothing is pruned: keep the original files and (possibly exact)
            // statistics
            return Ok((conf, summary));
        }

        let table_schema = table_schema.table_schema();
        let all_files_have_statistics = file_groups
            .iter()
            .flatten()
            .all(|file| file.statistics.is_some());
        let file_groups = file_groups
            .into_iter()
            .zip(&conf.file_groups)
            .map(|(files, original_group)| {
                let statistics = if all_files_have_statistics {
                    Some(group_statistics(&files, table_schema)?)
                } else {
                    // Files may have been removed from the group, so its
                    // original statistics are an upper bound
                    original_group
                        .file_statistics(None)
                        .map(|statistics| statistics.clone().to_inexact())
                };
                let group = FileGroup::new(files);
                Ok(match statistics {
                    Some(statistics) => group.with_statistics(Arc::new(statistics)),
                    None => group,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let statistics = if all_files_have_statistics {
            let mut non_empty_groups = file_groups
                .iter()
                .filter(|group| !group.is_empty())
                .filter_map(|group| group.file_statistics(None))
                .peekable();
            if non_empty_groups.peek().is_none() {
                empty_statistics(table_schema)
            } else {
                Statistics::try_merge_iter_with_ndv_fallback(
                    non_empty_groups,
                    table_schema,
                    NdvFallback::Max,
                )?
                .to_inexact()
            }
        } else {
            // The number of rows is unknown for some files: keep the statistics
            // of the scan, which are now an upper bound
            conf.statistics().to_inexact()
        };

        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_statistics(statistics)
            .build();
        Ok((conf, summary))
    }

    /// Returns why eager pruning cannot be performed for a scan, if it cannot
    fn skip_reason(
        &self,
        table_schema: &TableSchema,
        num_files: usize,
    ) -> Option<String> {
        let file_limit = self.options.global.eager_pruning_file_limit;
        if num_files > file_limit {
            return Some(format!(
                "{num_files} files exceed eager_pruning_file_limit {file_limit}"
            ));
        }
        let crypto = &self.options.crypto;
        if crypto.file_decryption.is_some() || crypto.factory_id.is_some() {
            return Some("encrypted files are not supported".to_string());
        }
        if !table_schema.virtual_columns().is_empty() {
            return Some("virtual columns are not supported".to_string());
        }
        None
    }

    /// Eagerly prune `file`, keeping the file unchanged if that fails.
    async fn prune_file_or_keep(
        &self,
        context: &ScanContext<'_>,
        file: PartitionedFile,
    ) -> FileOutcome {
        match self.prune_file(context, &file).await {
            Ok(outcome) => outcome,
            Err(e) => {
                debug!(
                    "Eager parquet pruning of {} failed, leaving it unchanged: {e}",
                    file.object_meta.location
                );
                FileOutcome::NotEvaluated(file)
            }
        }
    }

    /// Eagerly prune a single file.
    ///
    /// This mirrors the pruning the parquet opener performs when the file is
    /// scanned.
    async fn prune_file(
        &self,
        context: &ScanContext<'_>,
        file: &PartitionedFile,
    ) -> Result<FileOutcome> {
        if file.extensions.contains::<ParquetRowSelection>() {
            // A ParquetAccessPlan cannot be attached next to a row selection
            return Ok(FileOutcome::NotEvaluated(file.clone()));
        }

        let global = &self.options.global;
        let table_schema = context.table_schema;
        let logical_file_schema = table_schema.file_schema();
        let file_name = file.object_meta.location.to_string();

        // Replace partition columns and columns that are constant in the file
        // with their values, as the opener does
        let mut literal_columns: HashMap<String, ScalarValue> = table_schema
            .table_partition_cols()
            .iter()
            .zip(file.partition_values.iter())
            .map(|(field, value)| (field.name().clone(), value.clone()))
            .collect();
        literal_columns.extend(constant_columns_from_stats(
            file.statistics.as_deref(),
            logical_file_schema,
        ));
        let mut predicate = Arc::clone(&self.predicate);
        if !literal_columns.is_empty() {
            predicate = replace_columns_with_literals(predicate, &literal_columns)?;
        }

        // Metrics are required by the pruning code but are not reported: the
        // scan reports the pruning it performs itself when it executes
        let metrics = ExecutionPlanMetricsSet::new();
        let file_metrics = ParquetFileMetrics::new(0, &file_name, &metrics);
        let mut reader: Box<dyn AsyncFileReader> = self.reader_factory.create_reader(
            0,
            file.clone(),
            file.metadata_size_hint.or(global.metadata_size_hint),
            &metrics,
        )?;

        // Like the opener, defer loading the page index until it is needed
        let mut options =
            ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Skip);
        if let Some(schema) = file.arrow_schema.as_ref() {
            options = options.with_schema(Arc::clone(schema));
        }
        let mut reader_metadata =
            ArrowReaderMetadata::load_async(&mut reader, options.clone()).await?;

        let physical_file_schema = coerce_physical_file_schema(
            logical_file_schema,
            reader_metadata.schema(),
            reader_metadata.parquet_schema(),
            context.coerce_int96.as_ref(),
            context.coerce_int96_tz.clone(),
        )
        .unwrap_or_else(|| Arc::clone(reader_metadata.schema()));

        // Adapt the predicate to the physical file schema
        let rewriter = context.expr_adapter_factory.create(
            Arc::clone(logical_file_schema),
            Arc::clone(&physical_file_schema),
        )?;
        let predicate = PhysicalExprSimplifier::new(&physical_file_schema)
            .simplify(rewriter.rewrite(predicate)?)?;

        let parquet_metadata = Arc::clone(reader_metadata.metadata());
        let rg_metadata = parquet_metadata.row_groups();
        let mut row_groups = RowGroupAccessPlanFilter::new(create_initial_plan(
            &file_name,
            &file.extensions,
            rg_metadata,
        )?);
        if let Some(range) = file.range.as_ref() {
            row_groups.prune_by_range(rg_metadata, range);
        }
        let initial_plan = row_groups.access_plan().clone();
        let initial_row_groups = read_row_group_count(&initial_plan, rg_metadata);
        let initial_rows = initial_plan.selected_row_count(rg_metadata)?;

        let predicate_creation_errors = Count::new();
        let pruning_predicate = build_pruning_predicates(
            Some(&predicate),
            &physical_file_schema,
            &predicate_creation_errors,
            global.max_in_list_size,
        );
        let page_pruning_predicate =
            (self.level.includes(EagerParquetPruning::PageIndex)
                && global.enable_page_index)
                .then(|| {
                    build_page_pruning_predicate(
                        &predicate,
                        &physical_file_schema,
                        global.max_in_list_size,
                    )
                })
                .filter(|p| p.filter_number() > 0);

        // Row group statistics
        if let Some(pruning_predicate) = pruning_predicate.as_deref()
            && global.pruning
            && !row_groups.is_empty()
        {
            row_groups.prune_by_statistics_with_metadata(
                &physical_file_schema,
                &parquet_metadata,
                pruning_predicate,
                &file_metrics,
            );
        }

        // Load the page index if it can prune any remaining row group. This
        // must happen before reading bloom filters, which consumes the reader.
        if page_pruning_predicate.is_some() && !row_groups.is_empty() {
            let fully_matched = row_groups.is_fully_matched();
            if !row_groups.row_group_indexes().all(|idx| fully_matched[idx]) {
                reader_metadata = load_page_index(
                    reader_metadata,
                    &mut reader,
                    options.with_page_index_policy(PageIndexPolicy::Optional),
                )
                .await?;
            }
        }

        // Bloom filters
        if let Some(pruning_predicate) = pruning_predicate.as_deref()
            && self.level.includes(EagerParquetPruning::BloomFilters)
            && global.bloom_filter_on_read
            && !row_groups.is_empty()
        {
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(
                reader,
                reader_metadata.clone(),
            );
            let row_group_indexes: Vec<usize> = row_groups.row_group_indexes().collect();
            let bloom_filters = load_row_group_bloom_filters(
                &mut builder,
                pruning_predicate,
                &physical_file_schema,
                &row_group_indexes,
                &file_metrics.predicate_evaluation_errors,
            )
            .await;
            row_groups.prune_by_bloom_filters(
                pruning_predicate,
                &file_metrics,
                &bloom_filters,
            );
        }

        let mut access_plan = row_groups.build();

        // Page index
        if let Some(page_pruning_predicate) = page_pruning_predicate
            && access_plan.row_group_index_iter().next().is_some()
        {
            access_plan = page_pruning_predicate.prune_plan_with_page_index(
                access_plan,
                &physical_file_schema,
                reader_metadata.parquet_schema(),
                reader_metadata.metadata(),
                &file_metrics,
            );
        }

        // Fully matched flags only hold for this predicate, see module docs
        access_plan.clear_fully_matched();

        let rows = access_plan.selected_row_count(rg_metadata)?;
        let row_groups_read = read_row_group_count(&access_plan, rg_metadata);
        let outcome = |file, changed| FileOutcome::Evaluated {
            file,
            changed,
            row_groups: initial_row_groups,
            row_groups_pruned: initial_row_groups.saturating_sub(row_groups_read),
            rows: initial_rows,
            rows_pruned: initial_rows.saturating_sub(rows),
        };

        if rows == 0 {
            return Ok(outcome(None, true));
        }
        if rows == initial_rows {
            // Nothing is pruned, so the file is kept unchanged. The access plan
            // can still differ from the initial plan, as page index pruning
            // selects all rows of a row group in which no page is pruned.
            if file
                .statistics
                .as_ref()
                .is_some_and(|statistics| statistics.num_rows.get_value().is_some())
            {
                return Ok(outcome(Some(file.clone()), false));
            }
            // The row count of the file is unknown, e.g. because statistics are
            // not collected. Use the metadata that was read to fill it in, so
            // the row count of the scan does not become unknown.
            let file_statistics = DFParquetMetadata::statistics_from_parquet_metadata(
                &parquet_metadata,
                logical_file_schema,
            )?;
            let file = file.clone().with_statistics(Arc::new(file_statistics));
            return Ok(outcome(Some(file), true));
        }

        // Summarize only the row groups that are read, so that column
        // statistics such as min/max values describe the rows that are read
        // rather than the entire file. Otherwise estimates derived from them,
        // such as filter selectivity, would account for the pruning twice.
        let read_row_groups: Vec<RowGroupMetaData> = access_plan
            .row_group_index_iter()
            .map(|idx| rg_metadata[idx].clone())
            .collect();
        let read_row_groups_rows = read_row_groups
            .iter()
            .map(|row_group| row_group.num_rows() as usize)
            .sum();
        let row_group_statistics = DFParquetMetadata::statistics_from_row_groups(
            &parquet_metadata,
            &read_row_groups,
            logical_file_schema,
        )?;
        // `with_statistics` also adds statistics for partition columns
        let mut file = file.clone().with_statistics(Arc::new(row_group_statistics));
        if let Some(statistics) = file.statistics.as_deref() {
            file.statistics = Some(Arc::new(pruned_statistics(
                statistics,
                read_row_groups_rows,
                rows,
            )));
        }
        file.extensions.insert(access_plan);

        Ok(outcome(Some(file), true))
    }
}

/// Returns the number of row groups from which `access_plan` reads any rows
fn read_row_group_count(
    access_plan: &ParquetAccessPlan,
    rg_metadata: &[RowGroupMetaData],
) -> usize {
    access_plan
        .inner()
        .iter()
        .zip(rg_metadata)
        .filter(|(access, row_group)| match access {
            RowGroupAccess::Skip => false,
            RowGroupAccess::Scan => row_group.num_rows() > 0,
            RowGroupAccess::Selection(selection) => selection.selects_any(),
        })
        .count()
}

/// Returns statistics describing the `selected_rows` rows read from row
/// groups with `row_group_rows` rows, given the `statistics` of these row
/// groups.
///
/// The number of rows is the (upper bound) estimate from pruning, and byte
/// sizes are scaled accordingly. All values are inexact, except a null count
/// of zero.
///
/// The statistics must stay correct for the entire file if the attached
/// access plan is not applied, for example after the plan is serialized (file
/// statistics are serialized, file extensions are not). Min and max values
/// only bound the rows that are read, so they must not be exact: the scan
/// replaces a column whose exact min and max are equal with that value, which
/// is wrong for row groups the statistics do not cover. A null count of zero
/// stays exact because every row that is not read fails the filters, which are
/// always applied to the output of the scan.
fn pruned_statistics(
    statistics: &Statistics,
    row_group_rows: usize,
    selected_rows: usize,
) -> Statistics {
    let scale = |value: &Precision<usize>| match value.get_value() {
        Some(value) if row_group_rows > 0 => {
            let scaled =
                (*value as u128 * selected_rows as u128).div_ceil(row_group_rows as u128);
            Precision::Inexact(usize::try_from(scaled).unwrap_or(usize::MAX))
        }
        _ => value.to_inexact(),
    };

    let column_statistics = statistics
        .column_statistics
        .iter()
        .map(|column| {
            ColumnStatistics {
                null_count: if column.null_count == Precision::Exact(0) {
                    Precision::Exact(0)
                } else {
                    column.null_count.to_inexact()
                },
                min_value: column.min_value.clone().to_inexact(),
                max_value: column.max_value.clone().to_inexact(),
                // The sum of the entire file says little about the sum of the
                // selected rows
                sum_value: Precision::Absent,
                distinct_count: column.distinct_count.to_inexact(),
                byte_size: scale(&column.byte_size),
            }
        })
        .collect();

    Statistics {
        num_rows: Precision::Inexact(selected_rows),
        total_byte_size: scale(&statistics.total_byte_size),
        column_statistics,
    }
}

/// Merges the statistics of the files in a file group
fn group_statistics(
    files: &[PartitionedFile],
    table_schema: &Schema,
) -> Result<Statistics> {
    if files.is_empty() {
        return Ok(empty_statistics(table_schema));
    }
    Statistics::try_merge_iter_with_ndv_fallback(
        files.iter().filter_map(|file| file.statistics.as_deref()),
        table_schema,
        NdvFallback::Max,
    )
}

/// Statistics for a scan, or file group, from which no rows are read
fn empty_statistics(table_schema: &Schema) -> Statistics {
    Statistics::new_unknown(table_schema)
        .with_num_rows(Precision::Inexact(0))
        .with_total_byte_size(Precision::Inexact(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultParquetFileReaderFactory;
    use crate::source::ParquetSource;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field};
    use bytes::{BufMut, BytesMut};
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::{Expr, col, lit};
    use datafusion_physical_expr::planner::logical2physical;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Prunes a file whose column `a` holds `0..400` in 4 row groups of 100
    /// rows, using `predicate`
    async fn prune(
        level: EagerParquetPruning,
        file_limit: usize,
        predicate: Expr,
    ) -> (FileScanConfig, EagerPruningSummary) {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from_iter_values(0..400))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(100))
            .build();
        let mut out = BytesMut::new().writer();
        {
            let mut writer =
                ArrowWriter::try_new(&mut out, Arc::clone(&schema), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }
        let data = out.into_inner().freeze();
        let size = data.len() as u64;
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from("test.parquet"), data.into())
            .await
            .unwrap();

        let table_schema = TableSchema::from(&schema);
        let conf = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(table_schema.clone())),
        )
        .with_file(PartitionedFile::new("test.parquet", size))
        .build();

        let mut options = TableParquetOptions::default();
        options.global.eager_pruning = level;
        options.global.eager_pruning_file_limit = file_limit;
        let filters = [logical2physical(&predicate, &schema)];
        let pruner = EagerPruner::try_new(
            &options,
            &filters,
            Arc::new(DefaultParquetFileReaderFactory::new(store)),
            4,
        )
        .unwrap();
        pruner.prune(&table_schema, conf).await.unwrap()
    }

    #[tokio::test]
    async fn prunes_row_groups_without_fully_matched_row_groups() {
        // Row groups 0 and 1 fully match `a < 250`, row group 2 partially matches
        let (conf, summary) =
            prune(EagerParquetPruning::RowGroups, 1, col("a").lt(lit(250i64))).await;

        assert_eq!(
            summary,
            EagerPruningSummary::Pruned(EagerPruningStats {
                level: EagerParquetPruning::RowGroups,
                files: 1,
                files_not_evaluated: 0,
                files_pruned: 0,
                row_groups: 4,
                row_groups_pruned: 1,
                rows: 400,
                rows_pruned: 100,
            })
        );

        let file = &conf.file_groups[0].files()[0];
        let access_plan = file.extensions.get::<ParquetAccessPlan>().unwrap();
        assert_eq!(access_plan.row_group_indexes(), vec![0, 1, 2]);
        // The scan may execute with a different predicate, so no row group
        // may be marked as fully matched
        assert!(
            access_plan.fully_matched().iter().all(|matched| !matched),
            "{access_plan:?}"
        );

        // Statistics describe the row groups that are read
        let file_statistics = file.statistics.as_ref().unwrap();
        assert_eq!(file_statistics.num_rows, Precision::Inexact(300));
        let column_statistics = &file_statistics.column_statistics[0];
        assert_eq!(
            column_statistics.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(0)))
        );
        assert_eq!(
            column_statistics.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(299)))
        );
        assert_eq!(column_statistics.null_count, Precision::Exact(0));
        assert_eq!(conf.statistics().num_rows, Precision::Inexact(300));
    }

    #[tokio::test]
    async fn prunes_entire_file() {
        let (conf, summary) =
            prune(EagerParquetPruning::RowGroups, 1, col("a").gt(lit(1000i64))).await;

        let EagerPruningSummary::Pruned(stats) = summary else {
            panic!("expected eager pruning");
        };
        assert_eq!(stats.files_pruned, 1);
        assert_eq!(conf.file_groups.len(), 1);
        assert!(conf.file_groups[0].is_empty());
        assert_eq!(conf.statistics().num_rows, Precision::Inexact(0));
        assert_eq!(
            conf.file_groups[0].file_statistics(None).unwrap().num_rows,
            Precision::Inexact(0)
        );
    }

    #[tokio::test]
    async fn skips_scans_above_file_limit() {
        let (conf, summary) =
            prune(EagerParquetPruning::RowGroups, 0, col("a").lt(lit(250i64))).await;

        assert_eq!(
            summary,
            EagerPruningSummary::Skipped {
                level: EagerParquetPruning::RowGroups,
                reason: "1 files exceed eager_pruning_file_limit 0".to_string(),
            }
        );
        let file = &conf.file_groups[0].files()[0];
        assert!(!file.extensions.contains::<ParquetAccessPlan>());
    }

    #[test]
    fn disabled_or_without_filters() {
        let reader_factory: Arc<dyn ParquetFileReaderFactory> = Arc::new(
            DefaultParquetFileReaderFactory::new(Arc::new(InMemory::new())),
        );
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let filters = [logical2physical(&col("a").lt(lit(1i64)), &schema)];

        let mut options = TableParquetOptions::default();
        assert!(
            EagerPruner::try_new(&options, &filters, Arc::clone(&reader_factory), 1)
                .is_none()
        );

        options.global.eager_pruning = EagerParquetPruning::BloomFilters;
        assert!(
            EagerPruner::try_new(&options, &[], Arc::clone(&reader_factory), 1).is_none()
        );
        assert!(EagerPruner::try_new(&options, &filters, reader_factory, 1).is_some());
    }

    #[test]
    fn statistics_of_pruned_file() {
        let column = |min: i64, max: i64, null_count: usize| ColumnStatistics {
            null_count: Precision::Exact(null_count),
            min_value: Precision::Exact(ScalarValue::Int64(Some(min))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(max))),
            sum_value: Precision::Exact(ScalarValue::Int64(Some(1000))),
            distinct_count: Precision::Exact(10),
            byte_size: Precision::Exact(800),
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(100),
            total_byte_size: Precision::Exact(1000),
            column_statistics: vec![column(5, 5, 0), column(1, 10, 0), column(1, 10, 3)],
        };

        let pruned = pruned_statistics(&statistics, 100, 25);

        assert_eq!(pruned.num_rows, Precision::Inexact(25));
        assert_eq!(pruned.total_byte_size, Precision::Inexact(250));
        let [single_value, no_nulls, nulls] = pruned.column_statistics.as_slice() else {
            panic!("expected 3 columns");
        };

        // Even a single value is inexact: it only describes the rows that are
        // read, and an exact single value would let the scan replace the
        // column with it in rows it reads without the attached access plan
        assert_eq!(single_value.null_count, Precision::Exact(0));
        assert_eq!(
            single_value.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(5)))
        );
        assert_eq!(
            single_value.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(5)))
        );

        assert_eq!(no_nulls.null_count, Precision::Exact(0));
        assert_eq!(
            no_nulls.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            no_nulls.max_value,
            Precision::Inexact(ScalarValue::Int64(Some(10)))
        );

        assert_eq!(nulls.null_count, Precision::Inexact(3));
        assert_eq!(
            nulls.min_value,
            Precision::Inexact(ScalarValue::Int64(Some(1)))
        );

        for column in &pruned.column_statistics {
            assert_eq!(column.sum_value, Precision::Absent);
            assert_eq!(column.distinct_count, Precision::Inexact(10));
            assert_eq!(column.byte_size, Precision::Inexact(200));
        }
    }

    #[test]
    fn display() {
        let stats = EagerPruningStats {
            level: EagerParquetPruning::PageIndex,
            files: 4,
            files_not_evaluated: 0,
            files_pruned: 1,
            row_groups: 120,
            row_groups_pruned: 117,
            rows: 1000,
            rows_pruned: 990,
        };
        assert_eq!(
            EagerPruningSummary::Pruned(stats.clone()).to_string(),
            "[level=page_index, files_pruned=1/4, row_groups_pruned=117/120, \
             rows_pruned=990/1000]"
        );
        assert_eq!(
            EagerPruningSummary::Pruned(EagerPruningStats {
                files_not_evaluated: 2,
                ..stats
            })
            .to_string(),
            "[level=page_index, files_pruned=1/4, row_groups_pruned=117/120, \
             rows_pruned=990/1000, files_not_evaluated=2]"
        );
        assert_eq!(
            EagerPruningSummary::Skipped {
                level: EagerParquetPruning::RowGroups,
                reason: "some reason".to_string(),
            }
            .to_string(),
            "[level=row_groups, skipped=some reason]"
        );
    }
}
