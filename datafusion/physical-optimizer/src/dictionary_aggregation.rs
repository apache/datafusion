// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Preserve profitable file-native dictionary encodings through supported operators.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, PhysicalFileStatistics, Result, Statistics};
use datafusion_datasource::file::try_rewrite_dictionary_filter_predicate;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::Operator;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CastExpr, Column, InListExpr, Literal,
};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, aggregate_expressions,
};
use datafusion_physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::statistics::StatisticsArgs;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
};

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

// Every Utf8View row hashes a 16-byte view, even when its value is inline and
// much shorter. Dictionary grouping hashes each distinct value and then uses
// the 4-byte keys, so cost short strings by their view slot rather than only
// their logical payload width.
const UTF8_VIEW_SLOT_BYTES: usize = size_of::<u128>();
// Require the decoded values to be substantially larger than the uncompressed
// Parquet dictionary representation.
const MIN_VALUE_BYTES_PER_PARQUET_BYTE: usize = 4;
// Filtering needs a larger storage win than grouping because it does not also
// replace repeated hash-table keys. The TPC-H column sweep found no filter win
// below this ratio, while both Q19 candidates clear it comfortably.
const MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE: usize = 8;
// Avoid changing representation when too little string data reaches the
// aggregate for cheaper key hashing to repay the scan and output conversions.
const MIN_AGGREGATE_VALUE_BYTES: usize = 8 * 1024 * 1024;
// A predicate on another column makes the aggregate row estimate inexact.
// Require substantially more estimated post-filter work for this less certain
// path than for an aggregate with an exact input size.
const MIN_SOURCE_FILTERED_AGGREGATE_VALUE_BYTES: usize = 64 * 1024 * 1024;
// Footer rows are only a reliable scale proxy when the post-filter estimate
// says nearly all rows still reach the aggregate. Q1 is estimated at 96.4%.
const MIN_SOURCE_FILTERED_INPUT_PERCENT: usize = 90;
// A filter-only rewrite only saves the difference between a 16-byte Utf8View
// slot and a 4-byte dictionary key. Require enough rows for those savings to
// matter before paying for a different scan representation.
const MIN_FILTER_SAVED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
enum DictionaryUse {
    Aggregate,
    SourceFilteredAggregate,
    Filter,
}

impl DictionaryUse {
    fn min_value_bytes_per_parquet_byte(self) -> usize {
        match self {
            Self::Aggregate | Self::SourceFilteredAggregate => {
                MIN_VALUE_BYTES_PER_PARQUET_BYTE
            }
            Self::Filter => MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE,
        }
    }

    fn min_work_bytes(self) -> usize {
        match self {
            Self::Aggregate => MIN_AGGREGATE_VALUE_BYTES,
            Self::SourceFilteredAggregate => MIN_SOURCE_FILTERED_AGGREGATE_VALUE_BYTES,
            Self::Filter => MIN_FILTER_SAVED_BYTES,
        }
    }

    fn work_bytes_per_row(self, value_bytes_per_row: usize) -> usize {
        match self {
            Self::Aggregate | Self::SourceFilteredAggregate => {
                value_bytes_per_row.max(UTF8_VIEW_SLOT_BYTES)
            }
            Self::Filter => UTF8_VIEW_SLOT_BYTES - size_of::<i32>(),
        }
    }
}

/// Rewrites eligible string columns to use dictionaries emitted natively by
/// their file source when grouping or filter-only use can profit from them.
#[derive(Debug, Default)]
pub struct DictionaryPreservation {
    phase: DictionaryPhase,
}

#[derive(Debug, Default)]
enum DictionaryPhase {
    #[default]
    Aggregation,
    Filter,
}

#[derive(Debug, Clone)]
struct GroupColumn {
    group_index: usize,
    aggregate_input_index: usize,
    scan: Arc<dyn ExecutionPlan>,
    table_index: usize,
    scan_path: Vec<usize>,
    /// Whether the scan-to-aggregate path contains a `FilterExec`.
    has_filter_exec: bool,
    /// Whether this path contains only a scan, projections, and filters.
    simple_filter_path: bool,
}

#[derive(Debug, Clone)]
struct ScanTarget {
    scan: Arc<dyn ExecutionPlan>,
    table_index: usize,
    scan_path: Vec<usize>,
    /// Whether the traced path contains a `FilterExec`.
    has_filter_exec: bool,
    /// Whether this path contains only a scan, projections, and filters.
    simple_filter_path: bool,
}

impl ScanTarget {
    fn prepend_child(mut self, child_index: usize) -> Self {
        self.scan_path.insert(0, child_index);
        self
    }
}

#[derive(Debug)]
struct ScanRewrite {
    scan_path: Vec<usize>,
    table_indices: Vec<usize>,
}

impl DictionaryPreservation {
    /// Create the grouped-aggregation pass of [`DictionaryPreservation`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Create the filter pass that runs after projection pushdown has made
    /// filter-only columns explicit.
    pub fn new_filter_pass() -> Self {
        Self {
            phase: DictionaryPhase::Filter,
        }
    }

    fn try_rewrite_aggregate(
        plan: &Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(final_agg) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };

        match final_agg.mode() {
            AggregateMode::Single | AggregateMode::SinglePartitioned => {
                Self::rewrite_aggregate_chain(final_agg, None, context)
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                let Some(partial_agg) = final_agg.input().downcast_ref::<AggregateExec>()
                else {
                    return Ok(None);
                };
                if partial_agg.mode() != &AggregateMode::Partial {
                    return Ok(None);
                }
                Self::rewrite_aggregate_chain(final_agg, Some(partial_agg), context)
            }
            AggregateMode::Partial | AggregateMode::PartialReduce => Ok(None),
        }
    }

    fn try_rewrite_filter(
        plan: &Arc<dyn ExecutionPlan>,
        _context: &dyn PhysicalOptimizerContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(filter) = plan.downcast_ref::<FilterExec>() else {
            return Ok(None);
        };
        let Some(projection) = filter.projection() else {
            return Ok(None);
        };
        if filter.fetch().is_some() {
            return Ok(None);
        }

        let Some(scan) = filter.input().downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };
        let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
            return Ok(None);
        };
        if config.limit.is_some() || config.file_source.exact_filter().is_some() {
            return Ok(None);
        }

        let mut input_indices: Vec<_> = collect_columns(filter.predicate())
            .into_iter()
            .map(|column| column.index())
            .collect();
        input_indices.sort_unstable();
        input_indices.dedup();

        let input_schema = filter.input().schema();
        let source_predicate = config.file_source.filter();
        let mut table_indices = vec![];
        for input_index in input_indices {
            // ProjectionPushdown embeds the output projection into FilterExec.
            // A candidate is safe only when the filter is its last consumer.
            if projection.contains(&input_index)
                || input_schema
                    .fields()
                    .get(input_index)
                    .is_none_or(|field| field.data_type() != &DataType::Utf8View)
                || !Self::candidate_has_supported_filter_use(
                    filter.predicate(),
                    input_index,
                )
            {
                continue;
            }
            let Some(table_index) = Self::scan_table_index(config, input_index) else {
                continue;
            };
            if source_predicate.as_ref().is_some_and(|predicate| {
                !Self::candidate_has_supported_filter_use(predicate, table_index)
            }) || !Self::is_profitable(
                config,
                table_index,
                None,
                DictionaryUse::Filter,
            ) {
                continue;
            }
            table_indices.push(table_index);
        }
        if table_indices.is_empty() {
            return Ok(None);
        }

        let rewrites = [ScanRewrite {
            scan_path: vec![0],
            table_indices,
        }];
        let Some(rewritten) = Self::rewrite_scans(plan, &rewrites, &mut vec![])? else {
            return Ok(None);
        };
        if rewritten.schema() != plan.schema() {
            return Ok(None);
        }
        Ok(Some(rewritten))
    }

    fn scan_table_index(config: &FileScanConfig, output_index: usize) -> Option<usize> {
        let Some(projection) = config.file_source.projection() else {
            return Some(output_index);
        };
        let target = projection.as_ref().get(output_index)?;
        let column = target.expr.downcast_ref::<Column>()?;
        let table_index = column.index();
        (projection
            .iter()
            .filter(|expr| {
                expr.expr
                    .downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == table_index)
            })
            .count()
            == 1)
            .then_some(table_index)
    }

    fn candidate_has_supported_filter_use(
        expr: &Arc<dyn PhysicalExpr>,
        candidate_index: usize,
    ) -> bool {
        let references_candidate = collect_columns(expr)
            .iter()
            .any(|column| column.index() == candidate_index);
        if !references_candidate {
            return true;
        }

        if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
            return match binary.op() {
                Operator::And | Operator::Or => {
                    Self::candidate_has_supported_filter_use(
                        binary.left(),
                        candidate_index,
                    ) && Self::candidate_has_supported_filter_use(
                        binary.right(),
                        candidate_index,
                    )
                }
                Operator::Eq => {
                    let is_candidate = |expr: &Arc<dyn PhysicalExpr>| {
                        expr.downcast_ref::<Column>()
                            .is_some_and(|column| column.index() == candidate_index)
                    };
                    (is_candidate(binary.left())
                        && binary.right().downcast_ref::<Literal>().is_some())
                        || (is_candidate(binary.right())
                            && binary.left().downcast_ref::<Literal>().is_some())
                }
                _ => false,
            };
        }

        expr.downcast_ref::<InListExpr>().is_some_and(|in_list| {
            !in_list.negated()
                && !in_list.list().is_empty()
                && in_list
                    .expr()
                    .downcast_ref::<Column>()
                    .is_some_and(|column| column.index() == candidate_index)
                && in_list
                    .list()
                    .iter()
                    .all(|expr| expr.downcast_ref::<Literal>().is_some())
        })
    }

    fn rewrite_aggregate_chain(
        final_agg: &AggregateExec,
        partial_agg: Option<&AggregateExec>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let raw_agg = partial_agg.unwrap_or(final_agg);
        if raw_agg.group_expr().is_empty()
            || raw_agg.group_expr().has_grouping_set()
            || !raw_agg.group_expr().null_expr().is_empty()
            || raw_agg.limit_options().is_some()
            || final_agg.limit_options().is_some()
        {
            return Ok(None);
        }

        let group_columns = Self::group_columns(raw_agg)?;
        if group_columns.is_empty() {
            return Ok(None);
        }

        // A dictionary that is cheap at the scan can still be a loss when an
        // intervening join or projection leaves too little work at the
        // aggregate. Cost the representation change where it is consumed.
        let statistics =
            context.compute_statistics(raw_agg.input(), &StatisticsArgs::new())?;
        if statistics.num_rows == Precision::Absent {
            return Ok(None);
        }

        let group_columns: Vec<_> = group_columns
            .into_iter()
            .filter(|column| {
                let table_index = column.table_index;
                let scan = column
                    .scan
                    .downcast_ref::<DataSourceExec>()
                    .expect("group column lineage ends at a data source");
                let config = scan
                    .data_source()
                    .downcast_ref::<FileScanConfig>()
                    .expect("group column lineage ends at a file scan");
                let source_filter_supported =
                    config.file_source.filter().is_none_or(|predicate| {
                        let references_candidate = collect_columns(&predicate)
                            .iter()
                            .any(|column| column.index() == table_index);
                        !references_candidate
                    });
                let has_source_filter = config.file_source.filter().is_some();
                let (input_rows, dictionary_use) = match statistics.num_rows {
                    Precision::Exact(rows)
                        if !has_source_filter || column.has_filter_exec =>
                    {
                        (Some(rows), DictionaryUse::Aggregate)
                    }
                    Precision::Inexact(rows)
                        if column.has_filter_exec && column.simple_filter_path =>
                    {
                        (Some(rows), DictionaryUse::SourceFilteredAggregate)
                    }
                    Precision::Exact(_) | Precision::Inexact(_) | Precision::Absent => {
                        return false;
                    }
                };
                config.limit.is_none()
                    && config.file_source.exact_filter().is_none()
                    && source_filter_supported
                    && Self::is_profitable(
                        config,
                        column.table_index,
                        input_rows,
                        dictionary_use,
                    )
            })
            .collect();
        if group_columns.is_empty() {
            return Ok(None);
        }

        let encoded_input_indices: HashSet<_> = group_columns
            .iter()
            .map(|column| column.aggregate_input_index)
            .collect();
        let raw_references_candidate =
            Self::aggregate_references_columns(raw_agg, &encoded_input_indices)?;
        if raw_references_candidate {
            return Ok(None);
        }

        if let Some(partial_agg) = partial_agg {
            if final_agg.group_expr().expr().len() != raw_agg.group_expr().expr().len()
                || !final_agg.group_expr().expr().iter().enumerate().all(
                    |(index, (expr, _))| {
                        expr.downcast_ref::<Column>()
                            .is_some_and(|column| column.index() == index)
                    },
                )
            {
                return Ok(None);
            }
            let final_group_indices: HashSet<_> = group_columns
                .iter()
                .map(|column| column.group_index)
                .collect();
            let final_references_candidate =
                Self::aggregate_references_columns(final_agg, &final_group_indices)?;
            if final_references_candidate
                || partial_agg.group_expr().expr().len()
                    != final_agg.group_expr().expr().len()
            {
                return Ok(None);
            }
        }

        let scan_rewrites = Self::scan_rewrites(&group_columns);
        let Some(new_raw_input) =
            Self::rewrite_scans(raw_agg.input(), &scan_rewrites, &mut vec![])?
        else {
            return Ok(None);
        };
        let raw_input_schema = new_raw_input.schema();

        let new_raw_agg = Arc::new(AggregateExec::try_new(
            *raw_agg.mode(),
            raw_agg.group_expr().clone(),
            raw_agg.aggr_expr().to_vec(),
            raw_agg.filter_expr().to_vec(),
            new_raw_input,
            Arc::clone(&raw_input_schema),
        )?);

        let new_final_agg: Arc<dyn ExecutionPlan> = if partial_agg.is_some() {
            Arc::new(AggregateExec::try_new(
                *final_agg.mode(),
                final_agg.group_expr().clone(),
                final_agg.aggr_expr().to_vec(),
                final_agg.filter_expr().to_vec(),
                new_raw_agg,
                raw_input_schema,
            )?)
        } else {
            new_raw_agg
        };

        Self::restore_output_schema(&final_agg.schema(), new_final_agg, &group_columns)
            .map(Some)
    }

    fn group_columns(aggregate: &AggregateExec) -> Result<Vec<GroupColumn>> {
        let input_schema = aggregate.input().schema();
        let mut columns = vec![];
        for (group_index, (expr, _)) in aggregate.group_expr().expr().iter().enumerate() {
            let Some(column) = expr.downcast_ref::<Column>() else {
                // A grouping expression could itself depend on a candidate
                // column and therefore cannot safely retain its old input type.
                return Ok(vec![]);
            };
            if input_schema
                .fields()
                .get(column.index())
                .is_none_or(|field| field.data_type() != &DataType::Utf8View)
            {
                continue;
            }
            if let Some(target) =
                Self::trace_group_column(aggregate.input(), column.index())?
            {
                columns.push(GroupColumn {
                    group_index,
                    aggregate_input_index: column.index(),
                    scan: target.scan,
                    table_index: target.table_index,
                    scan_path: target.scan_path,
                    has_filter_exec: target.has_filter_exec,
                    simple_filter_path: target.simple_filter_path,
                });
            }
        }
        Ok(columns)
    }

    /// Trace a direct grouping column to one file scan while proving every
    /// intervening operator treats it as payload rather than evaluating it.
    fn trace_group_column(
        plan: &Arc<dyn ExecutionPlan>,
        output_index: usize,
    ) -> Result<Option<ScanTarget>> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>() else {
                return Ok(None);
            };
            let table_index = match config.file_source.projection() {
                Some(projection) => {
                    // Re-typing below an expression would leave that expression
                    // with the old input type. Ordinary scans have only direct
                    // column projections, so fail closed on any other shape.
                    let mut projected_columns =
                        Vec::with_capacity(projection.as_ref().len());
                    for expr in projection.as_ref() {
                        let Some(column) = expr.expr.downcast_ref::<Column>() else {
                            return Ok(None);
                        };
                        projected_columns.push(column.index());
                    }
                    let Some(&table_index) = projected_columns.get(output_index) else {
                        return Ok(None);
                    };
                    if projected_columns
                        .iter()
                        .filter(|&&index| index == table_index)
                        .count()
                        != 1
                    {
                        return Ok(None);
                    }
                    table_index
                }
                None => output_index,
            };
            return Ok(Some(ScanTarget {
                scan: Arc::clone(plan),
                table_index,
                scan_path: vec![],
                has_filter_exec: false,
                simple_filter_path: true,
            }));
        }

        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            let Some(target) = projection.expr().get(output_index) else {
                return Ok(None);
            };
            let Some(column) = target.expr.downcast_ref::<Column>() else {
                return Ok(None);
            };
            let input_index = column.index();
            if projection.expr().iter().enumerate().any(|(index, expr)| {
                index != output_index
                    && collect_columns(&expr.expr)
                        .iter()
                        .any(|column| column.index() == input_index)
            }) {
                return Ok(None);
            }
            let target = Self::trace_group_column(projection.input(), input_index)?;
            return Ok(target.map(|target| target.prepend_child(0)));
        }

        if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            if filter.fetch().is_some() {
                return Ok(None);
            }
            let input_index = match filter.projection() {
                Some(projection) => {
                    let Some(&input_index) = projection.get(output_index) else {
                        return Ok(None);
                    };
                    if projection
                        .iter()
                        .filter(|&&index| index == input_index)
                        .count()
                        != 1
                    {
                        return Ok(None);
                    }
                    input_index
                }
                None => output_index,
            };
            if collect_columns(filter.predicate())
                .iter()
                .any(|column| column.index() == input_index)
            {
                return Ok(None);
            }
            let target = Self::trace_group_column(filter.input(), input_index)?;
            return Ok(target.map(|mut target| {
                target.has_filter_exec = true;
                target.prepend_child(0)
            }));
        }

        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            if join.join_type() != &JoinType::Inner
                || join.filter().is_some()
                || join.fetch().is_some()
            {
                return Ok(None);
            }
            let join_output_index = match &join.projection {
                Some(projection) => {
                    let Some(&join_output_index) = projection.get(output_index) else {
                        return Ok(None);
                    };
                    if projection
                        .iter()
                        .filter(|&&index| index == join_output_index)
                        .count()
                        != 1
                    {
                        return Ok(None);
                    }
                    join_output_index
                }
                None => output_index,
            };
            let (_, column_indices) = build_join_schema(
                join.left().schema().as_ref(),
                join.right().schema().as_ref(),
                join.join_type(),
            );
            let Some(column_index) = column_indices.get(join_output_index) else {
                return Ok(None);
            };
            let (input, child_index, is_join_key) = match column_index.side {
                JoinSide::Left => (
                    join.left(),
                    0,
                    join.on().iter().any(|(left, _)| {
                        collect_columns(left)
                            .iter()
                            .any(|column| column.index() == column_index.index)
                    }),
                ),
                JoinSide::Right => (
                    join.right(),
                    1,
                    join.on().iter().any(|(_, right)| {
                        collect_columns(right)
                            .iter()
                            .any(|column| column.index() == column_index.index)
                    }),
                ),
                JoinSide::None => return Ok(None),
            };
            if is_join_key {
                return Ok(None);
            }
            let target = Self::trace_group_column(input, column_index.index)?;
            return Ok(target.map(|mut target| {
                target.simple_filter_path = false;
                target.prepend_child(child_index)
            }));
        }

        // Repartitions and every other unrecognized node are rejected until
        // their expression and reconstruction contracts are covered explicitly.
        Ok(None)
    }

    fn scan_rewrites(group_columns: &[GroupColumn]) -> Vec<ScanRewrite> {
        let mut rewrites: Vec<ScanRewrite> = vec![];
        for column in group_columns {
            if let Some(rewrite) = rewrites
                .iter_mut()
                .find(|rewrite| rewrite.scan_path == column.scan_path)
            {
                if !rewrite.table_indices.contains(&column.table_index) {
                    rewrite.table_indices.push(column.table_index);
                }
            } else {
                rewrites.push(ScanRewrite {
                    scan_path: column.scan_path.clone(),
                    table_indices: vec![column.table_index],
                });
            }
        }
        rewrites
    }

    fn rewrite_scans(
        plan: &Arc<dyn ExecutionPlan>,
        rewrites: &[ScanRewrite],
        path: &mut Vec<usize>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(rewrite) = rewrites.iter().find(|rewrite| rewrite.scan_path == *path)
        {
            let scan = plan
                .downcast_ref::<DataSourceExec>()
                .expect("scan rewrite target is a data source");
            let config = scan
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .expect("scan rewrite target is a file scan");
            let Some(file_source) = config
                .file_source
                .try_pushdown_dictionary_encoding(&rewrite.table_indices)?
            else {
                return Ok(None);
            };
            let statistics =
                Self::retype_statistics(config.statistics(), &rewrite.table_indices);
            let file_groups = config
                .file_groups
                .iter()
                .map(|group| {
                    let files = group
                        .iter()
                        .cloned()
                        .map(|mut file| {
                            if let Some(statistics) = &mut file.statistics {
                                let retyped = Self::retype_statistics(
                                    statistics.as_ref().clone(),
                                    &rewrite.table_indices,
                                );
                                *statistics = Arc::new(retyped);
                            }
                            file
                        })
                        .collect();
                    let mut new_group = FileGroup::new(files);
                    if let Some(statistics) = group.file_statistics(None) {
                        new_group =
                            new_group.with_statistics(Arc::new(Self::retype_statistics(
                                statistics.clone(),
                                &rewrite.table_indices,
                            )));
                    }
                    new_group
                })
                .collect();
            let new_config = FileScanConfigBuilder::from(config.clone())
                .with_source(file_source)
                .with_file_groups(file_groups)
                .with_statistics(statistics)
                .build();
            return Ok(Some(DataSourceExec::from_data_source(new_config)));
        }

        let children = plan.children();
        if children.is_empty() {
            return Ok(Some(Arc::clone(plan)));
        }
        let mut changed = false;
        let mut new_children = Vec::with_capacity(children.len());
        for (child_index, child) in children.into_iter().enumerate() {
            path.push(child_index);
            let new_child = if rewrites
                .iter()
                .any(|rewrite| rewrite.scan_path.starts_with(path.as_slice()))
            {
                let Some(new_child) = Self::rewrite_scans(child, rewrites, path)? else {
                    return Ok(None);
                };
                new_child
            } else {
                Arc::clone(child)
            };
            path.pop();
            changed |= !Arc::ptr_eq(&new_child, child);
            new_children.push(new_child);
        }
        if !changed {
            return Ok(Some(Arc::clone(plan)));
        }

        let rewritten = if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            Arc::new(ProjectionExec::try_new_with_schema_metadata(
                projection.expr().iter().cloned(),
                new_children.swap_remove(0),
                projection.schema().as_ref(),
            )?) as Arc<dyn ExecutionPlan>
        } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            let new_input = new_children.swap_remove(0);
            let old_schema = filter.input().schema();
            let new_schema = new_input.schema();
            let changed_column_indices = old_schema
                .fields()
                .iter()
                .zip(new_schema.fields())
                .enumerate()
                .filter_map(|(index, (old, new))| {
                    (old.data_type() != new.data_type()).then_some(index)
                })
                .collect::<Vec<_>>();
            let Some(predicate) = try_rewrite_dictionary_filter_predicate(
                filter.predicate(),
                new_schema.as_ref(),
                &changed_column_indices,
            )?
            else {
                return Ok(None);
            };
            Arc::new(
                FilterExecBuilder::from(filter)
                    .with_input(new_input)
                    .with_predicate(predicate)
                    .build()?,
            ) as Arc<dyn ExecutionPlan>
        } else {
            let result = Arc::clone(plan).replace_children(
                new_children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            );
            match result {
                Ok(rewritten) => rewritten,
                // A prepared hash join intentionally rejects replacing its
                // build child. Treat that and any other unsupported hash-join
                // reconstruction as an inapplicable rewrite, not a query error.
                Err(_) if plan.downcast_ref::<HashJoinExec>().is_some() => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            }
        };
        Ok(Some(rewritten))
    }

    fn retype_statistics(
        mut statistics: Statistics,
        table_indices: &[usize],
    ) -> Statistics {
        statistics.total_byte_size = Precision::Absent;
        for &index in table_indices {
            let Some(column) = statistics.column_statistics.get_mut(index) else {
                continue;
            };
            column.min_value = column
                .min_value
                .cast_to(&DataType::Utf8)
                .unwrap_or(Precision::Absent);
            column.max_value = column
                .max_value
                .cast_to(&DataType::Utf8)
                .unwrap_or(Precision::Absent);
            column.sum_value = Precision::Absent;
            column.byte_size = Precision::Absent;
        }
        statistics
    }

    fn aggregate_references_columns(
        aggregate: &AggregateExec,
        column_indices: &HashSet<usize>,
    ) -> Result<bool> {
        let references_candidate = |expr: &Arc<dyn PhysicalExpr>| {
            collect_columns(expr)
                .iter()
                .any(|column| column_indices.contains(&column.index()))
        };
        let aggregate_expressions = aggregate_expressions(
            aggregate.aggr_expr(),
            aggregate.mode(),
            aggregate.group_expr().expr().len(),
        )?;
        Ok(aggregate_expressions
            .iter()
            .flatten()
            .any(references_candidate)
            || aggregate
                .filter_expr()
                .iter()
                .flatten()
                .any(references_candidate))
    }

    /// File scan repartitioning splits one Parquet file into byte ranges and
    /// clones the file-level encoding metadata onto every range. Accept those
    /// ranges only when they form one exact, non-overlapping cover of the full
    /// object. This rejects caller-supplied partial or duplicated scans, whose
    /// row count may not match the file-level cost evidence.
    fn ranges_cover_full_files(config: &FileScanConfig) -> bool {
        let mut files_by_path = HashMap::new();
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            let Ok(file_size) = i64::try_from(file.object_meta.size) else {
                return false;
            };
            let (object_meta, ranges) = files_by_path
                .entry(file.object_meta.location.to_string())
                .or_insert_with(|| (file.object_meta.clone(), Vec::new()));
            if object_meta != &file.object_meta {
                return false;
            }
            ranges.push(
                file.range
                    .clone()
                    .unwrap_or(datafusion_datasource::FileRange {
                        start: 0,
                        end: file_size,
                    }),
            );
        }

        files_by_path
            .into_values()
            .all(|(object_meta, mut ranges)| {
                let Ok(file_size) = i64::try_from(object_meta.size) else {
                    return false;
                };
                ranges.sort_unstable();
                let mut next_start = 0;
                for range in ranges {
                    if range.start != next_start
                        || range.end <= range.start
                        || range.end > file_size
                    {
                        return false;
                    }
                    next_start = range.end;
                }
                next_start == file_size
            })
    }

    fn is_profitable(
        config: &FileScanConfig,
        table_index: usize,
        input_rows: Option<usize>,
        dictionary_use: DictionaryUse,
    ) -> bool {
        // Custom expression adapters can change values relative to the file
        // statistics used by this cost gate.
        if config.expr_adapter_factory.is_some() || !Self::ranges_cover_full_files(config)
        {
            return false;
        }
        let mut files_by_path = HashMap::new();
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            if file.arrow_schema.is_some() || file.extensions.len() != 1 {
                return false;
            }
            let Some(physical) = file.extension::<PhysicalFileStatistics>() else {
                return false;
            };
            match files_by_path.entry(file.object_meta.location.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(physical.clone());
                }
                std::collections::hash_map::Entry::Occupied(entry)
                    if entry.get() != physical =>
                {
                    return false;
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }

        let footer_rows = || {
            files_by_path.values().try_fold(0usize, |total, physical| {
                total.checked_add(physical.num_rows?)
            })
        };
        let input_rows = match dictionary_use {
            DictionaryUse::Aggregate => {
                let Some(input_rows) = input_rows else {
                    return false;
                };
                input_rows
            }
            DictionaryUse::SourceFilteredAggregate => {
                let (Some(input_rows), Some(footer_rows)) = (input_rows, footer_rows())
                else {
                    return false;
                };
                let Some(estimated_percent) = input_rows.checked_mul(100) else {
                    return false;
                };
                let Some(minimum_percent) =
                    footer_rows.checked_mul(MIN_SOURCE_FILTERED_INPUT_PERCENT)
                else {
                    return false;
                };
                if input_rows > footer_rows || estimated_percent < minimum_percent {
                    return false;
                }
                input_rows
            }
            DictionaryUse::Filter => {
                if input_rows.is_some() {
                    return false;
                }
                // Footer rows provide a stable file-size heuristic for the
                // filter-only path, while hosted benchmarks remain the gate.
                let Some(input_rows) = footer_rows() else {
                    return false;
                };
                input_rows
            }
        };

        let mut saw_file = false;
        let mut min_value_bytes_per_row = usize::MAX;
        for physical in files_by_path.values() {
            saw_file = true;
            let Some(column) = physical.column_statistics.get(table_index) else {
                return false;
            };
            if !column.all_dictionary_encoded {
                return false;
            }
            let Some(value_bytes_per_row) = column.min_row_group_value_bytes_per_row
            else {
                return false;
            };
            if column.native_dictionary_value_type != Some(DataType::Utf8)
                || column
                    .min_row_group_value_bytes_per_encoded_byte
                    .is_none_or(|ratio| {
                        ratio < dictionary_use.min_value_bytes_per_parquet_byte()
                    })
            {
                return false;
            }
            min_value_bytes_per_row = min_value_bytes_per_row.min(value_bytes_per_row);
        }
        let work_bytes_per_row =
            dictionary_use.work_bytes_per_row(min_value_bytes_per_row);
        saw_file
            && input_rows
                .checked_mul(work_bytes_per_row)
                .is_some_and(|bytes| bytes >= dictionary_use.min_work_bytes())
    }

    fn restore_output_schema(
        output_schema: &SchemaRef,
        input: Arc<dyn ExecutionPlan>,
        group_columns: &[GroupColumn],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let encoded_groups: HashSet<_> = group_columns
            .iter()
            .map(|column| column.group_index)
            .collect();
        let expressions =
            output_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let column = Arc::new(Column::new(field.name(), index))
                        as Arc<dyn PhysicalExpr>;
                    let expr = if encoded_groups.contains(&index) {
                        Arc::new(CastExpr::new_with_target_field(
                            column,
                            Arc::clone(field),
                            None,
                        )) as Arc<dyn PhysicalExpr>
                    } else {
                        column
                    };
                    (expr, field.name().clone())
                });
        Ok(Arc::new(ProjectionExec::try_new_with_schema_metadata(
            expressions,
            input,
            output_schema.as_ref(),
        )?))
    }
}

impl PhysicalOptimizerRule for DictionaryPreservation {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.optimize_with_context(plan, &ConfigOnlyContext::new(config))
    }

    fn optimize_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !context
            .config_options()
            .optimizer
            .enable_dictionary_preservation
        {
            return Ok(plan);
        }
        plan.transform_down(|plan| {
            let rewritten = match self.phase {
                DictionaryPhase::Aggregation => {
                    Self::try_rewrite_aggregate(&plan, context)?
                }
                DictionaryPhase::Filter => Self::try_rewrite_filter(&plan, context)?,
            };
            Ok(match rewritten {
                Some(rewritten) => Transformed::yes(rewritten),
                None => Transformed::no(plan),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        match self.phase {
            DictionaryPhase::Aggregation => "DictionaryPreservation(Aggregation)",
            DictionaryPhase::Filter => "DictionaryPreservation(Filter)",
        }
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, BooleanArray, DictionaryArray, Int64Array};
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{
        ColumnStatistics, NullEquality, PhysicalColumnStatistics, ScalarValue, Statistics,
    };
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::test::function_stub::count_udaf;
    use datafusion_functions_aggregate::count::count_udaf as real_count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{IsNotNullExpr, LikeExpr};
    use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion_physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};

    fn aggregate_plan(
        value_bytes: &[usize],
        parquet_uncompressed_bytes: &[Option<usize>],
        all_dictionary_encoded: bool,
        include_physical_statistics: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(value_bytes.len(), parquet_uncompressed_bytes.len());
        let fields = value_bytes
            .iter()
            .enumerate()
            .map(|(index, _)| {
                Field::new(format!("key_{index}"), DataType::Utf8View, false)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let physical_statistics = PhysicalFileStatistics {
            num_rows: Some(10_000_000),
            column_statistics: value_bytes
                .iter()
                .zip(parquet_uncompressed_bytes)
                .map(
                    |(bytes, parquet_uncompressed_bytes)| PhysicalColumnStatistics {
                        all_dictionary_encoded,
                        native_dictionary_value_type: Some(DataType::Utf8),
                        min_row_group_value_bytes_per_row: Some(*bytes / 10_000_000),
                        min_row_group_value_bytes_per_encoded_byte:
                            parquet_uncompressed_bytes
                                .filter(|bytes| *bytes != 0)
                                .map(|encoded_bytes| *bytes / encoded_bytes),
                    },
                )
                .collect(),
        };
        let mut file = PartitionedFile::new("data.parquet", 1024);
        if include_physical_statistics {
            file = file.with_extension(physical_statistics);
        }
        let statistics = Statistics {
            num_rows: Precision::Exact(10_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: value_bytes
                .iter()
                .map(|_| ColumnStatistics {
                    null_count: Precision::Exact(0),
                    ..Default::default()
                })
                .collect(),
        };
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(Arc::clone(&schema))),
        )
        .with_file(file)
        .with_statistics(statistics)
        .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        let raw_input_schema = scan.schema();
        let raw_group_by = PhysicalGroupBy::new_single(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    (
                        Arc::new(Column::new(field.name(), index))
                            as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    )
                })
                .collect(),
        );
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            raw_group_by,
            vec![],
            vec![],
            scan,
            Arc::clone(&raw_input_schema),
        )?);
        let final_group_by = PhysicalGroupBy::new_single(
            schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    (
                        Arc::new(Column::new(field.name(), index))
                            as Arc<dyn PhysicalExpr>,
                        field.name().clone(),
                    )
                })
                .collect(),
        );
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            final_group_by,
            vec![],
            vec![],
            partial,
            raw_input_schema,
        )?))
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::new();
        config.optimizer.enable_dictionary_preservation = true;
        DictionaryPreservation::new().optimize(plan, &config)
    }

    fn optimize_filter(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut config = ConfigOptions::new();
        config.optimizer.enable_dictionary_preservation = true;
        DictionaryPreservation::new_filter_pass().optimize(plan, &config)
    }

    fn candidate_physical_statistics(
        column_count: usize,
        candidate_index: usize,
        native_value_type: DataType,
        value_bytes_per_row: usize,
        value_bytes_per_parquet_byte: usize,
    ) -> PhysicalFileStatistics {
        PhysicalFileStatistics {
            num_rows: Some(10_000_000),
            column_statistics: (0..column_count)
                .map(|index| {
                    if index == candidate_index {
                        PhysicalColumnStatistics {
                            all_dictionary_encoded: true,
                            native_dictionary_value_type: Some(native_value_type.clone()),
                            min_row_group_value_bytes_per_row: Some(value_bytes_per_row),
                            min_row_group_value_bytes_per_encoded_byte: Some(
                                value_bytes_per_parquet_byte,
                            ),
                        }
                    } else {
                        PhysicalColumnStatistics::default()
                    }
                })
                .collect(),
        }
    }

    fn candidate_file(
        path: &str,
        column_count: usize,
        candidate_index: usize,
        native_value_type: DataType,
        value_bytes_per_row: usize,
        value_bytes_per_parquet_byte: usize,
    ) -> PartitionedFile {
        PartitionedFile::new(path, 1024).with_extension(candidate_physical_statistics(
            column_count,
            candidate_index,
            native_value_type,
            value_bytes_per_row,
            value_bytes_per_parquet_byte,
        ))
    }

    fn scan_plan(
        schema: SchemaRef,
        files: Vec<PartitionedFile>,
        projection: Option<Vec<usize>>,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut source = ParquetSource::new(Arc::clone(&schema));
        if let Some(predicate) = predicate {
            source = source.with_predicate(predicate);
        }
        let statistics = Statistics {
            num_rows: Precision::Exact(10_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: schema
                .fields()
                .iter()
                .map(|_| ColumnStatistics {
                    null_count: Precision::Exact(0),
                    ..Default::default()
                })
                .collect(),
        };
        let mut builder = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source),
        )
        .with_statistics(statistics)
        .with_limit(limit);
        for file in files {
            builder = builder.with_file(file);
        }
        builder = builder.with_projection_indices(projection)?;
        Ok(DataSourceExec::from_data_source(builder.build()))
    }

    fn equality_filter_plan(
        project_candidate: bool,
        value_bytes_per_parquet_byte: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("measure", DataType::Int64, false),
            Field::new("filter_key", DataType::Utf8View, true),
        ]));
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("filter_key", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                "target".to_string(),
            )))),
        )) as Arc<dyn PhysicalExpr>;
        let file = candidate_file(
            "data.parquet",
            2,
            1,
            DataType::Utf8,
            12,
            value_bytes_per_parquet_byte,
        );
        let scan =
            scan_plan(schema, vec![file], None, Some(Arc::clone(&predicate)), None)?;
        let projection = if project_candidate {
            vec![0, 1]
        } else {
            vec![0]
        };
        Ok(Arc::new(
            FilterExecBuilder::new(predicate, scan)
                .apply_projection(Some(projection))?
                .build()?,
        ))
    }

    fn assert_predicate_result(
        predicate: &Arc<dyn PhysicalExpr>,
        batch: &RecordBatch,
        expected: Vec<Option<bool>>,
    ) -> Result<()> {
        let ColumnarValue::Array(actual) = predicate.evaluate(batch)? else {
            panic!("filter predicate should evaluate to an array");
        };
        let actual = actual
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("filter predicate should return a BooleanArray");
        assert_eq!(actual, &BooleanArray::from(expected));
        Ok(())
    }

    fn q19_filter_predicate(
        schema: &Schema,
        shipmode_index: usize,
        shipinstruct_index: usize,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let literal = |value: Option<&str>| {
            Arc::new(Literal::new(match value {
                Some(value) => ScalarValue::Utf8View(Some(value.to_string())),
                None => ScalarValue::Null,
            })) as Arc<dyn PhysicalExpr>
        };
        let in_list = |values: &[Option<&str>]| -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(InListExpr::try_new(
                Arc::new(Column::new("shipmode", shipmode_index)),
                values.iter().map(|value| literal(*value)).collect(),
                false,
                schema,
            )?))
        };
        let instruct_eq = |value: &str| {
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("shipinstruct", shipinstruct_index)),
                Operator::Eq,
                literal(Some(value)),
            )) as Arc<dyn PhysicalExpr>
        };
        let branch = |modes: &[Option<&str>],
                      instruction: &str|
         -> Result<Arc<dyn PhysicalExpr>> {
            Ok(Arc::new(BinaryExpr::new(
                in_list(modes)?,
                Operator::And,
                instruct_eq(instruction),
            )))
        };
        Ok(Arc::new(BinaryExpr::new(
            branch(&[Some("AIR"), Some("AIR REG")], "DELIVER IN PERSON")?,
            Operator::Or,
            branch(&[Some("SHIP"), None], "TAKE BACK RETURN")?,
        )))
    }

    fn single_group_aggregate(
        input: Arc<dyn ExecutionPlan>,
        group_index: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let field = input_schema.field(group_index);
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new(field.name(), group_index)) as Arc<dyn PhysicalExpr>,
            field.name().clone(),
        )]);
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            input,
            input_schema,
        )?))
    }

    fn profitable_scan(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        let physical_statistics = PhysicalFileStatistics {
            num_rows: Some(10_000_000),
            column_statistics: schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, _)| {
                    if index == 1 {
                        PhysicalColumnStatistics {
                            all_dictionary_encoded: true,
                            native_dictionary_value_type: Some(DataType::Utf8),
                            min_row_group_value_bytes_per_row: Some(24),
                            min_row_group_value_bytes_per_encoded_byte: Some(6),
                        }
                    } else {
                        PhysicalColumnStatistics::default()
                    }
                })
                .collect(),
        };
        let file = PartitionedFile::new("data.parquet", 1024)
            .with_extension(physical_statistics);
        let statistics = Statistics {
            num_rows: Precision::Exact(10_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: schema
                .fields()
                .iter()
                .map(|_| ColumnStatistics {
                    null_count: Precision::Exact(0),
                    ..Default::default()
                })
                .collect(),
        };
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema)),
        )
        .with_file(file)
        .with_statistics(statistics)
        .build();
        DataSourceExec::from_data_source(config)
    }

    #[derive(Clone, Copy)]
    enum JoinPlanShape {
        Safe,
        CandidateIsJoinKey,
        InputFilter,
        DuplicateProjection,
        FilteredJoin,
        FetchedJoin,
        LeftJoin,
        DuplicateJoinProjection,
    }

    fn join_aggregate_plan(shape: JoinPlanShape) -> Result<Arc<dyn ExecutionPlan>> {
        let candidate_is_join_key = matches!(shape, JoinPlanShape::CandidateIsJoinKey);
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let mut left = profitable_scan(left_schema);
        if matches!(shape, JoinPlanShape::InputFilter) {
            let predicate =
                Arc::new(IsNotNullExpr::new(Arc::new(Column::new("group_key", 1))));
            left = Arc::new(FilterExec::try_new(predicate, left)?);
        }

        let right_type = if candidate_is_join_key {
            DataType::Utf8View
        } else {
            DataType::Int64
        };
        let right_schema = Arc::new(Schema::new(vec![Field::new(
            "right_key",
            right_type,
            false,
        )]));
        let right = scan_plan(
            right_schema,
            vec![PartitionedFile::new("right.parquet", 1024)],
            None,
            None,
            None,
        )?;
        let left_join_index = usize::from(candidate_is_join_key);
        let join_filter = matches!(shape, JoinPlanShape::FilteredJoin).then(|| {
            JoinFilter::new(
                Arc::new(IsNotNullExpr::new(Arc::new(Column::new("group_key", 0)))),
                vec![ColumnIndex {
                    index: 1,
                    side: JoinSide::Left,
                }],
                Arc::new(Schema::new(vec![Field::new(
                    "group_key",
                    DataType::Utf8View,
                    false,
                )])),
            )
        });
        let join_type = if matches!(shape, JoinPlanShape::LeftJoin) {
            JoinType::Left
        } else {
            JoinType::Inner
        };
        let join_projection = if matches!(shape, JoinPlanShape::DuplicateJoinProjection) {
            vec![1, 1]
        } else {
            vec![1]
        };
        let join = HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new(
                    if candidate_is_join_key {
                        "group_key"
                    } else {
                        "join_key"
                    },
                    left_join_index,
                )),
                Arc::new(Column::new("right_key", 0)),
            )],
            join_filter,
            &join_type,
            Some(join_projection),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?;
        let join = if matches!(shape, JoinPlanShape::FetchedJoin) {
            join.builder().with_fetch(Some(1)).build()?
        } else {
            join
        };
        let join = Arc::new(join) as Arc<dyn ExecutionPlan>;

        let mut projection_expr = vec![(
            Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
            "group_key".to_string(),
        )];
        if matches!(shape, JoinPlanShape::DuplicateProjection) {
            projection_expr.push((
                Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
                "duplicate_group_key".to_string(),
            ));
        }
        let projection = Arc::new(ProjectionExec::try_new(projection_expr, join)?)
            as Arc<dyn ExecutionPlan>;
        let input_schema = projection.schema();
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
            "group_key".to_string(),
        )]);
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            projection,
            input_schema,
        )?))
    }

    fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            return scan.data_source().downcast_ref::<FileScanConfig>();
        }
        plan.children()
            .into_iter()
            .find_map(|child| find_scan(child))
    }

    fn rewritten_scan(plan: &Arc<dyn ExecutionPlan>) -> &FileScanConfig {
        let projection = plan
            .downcast_ref::<ProjectionExec>()
            .expect("eligible aggregation should have a schema-restoring projection");
        let final_agg = projection
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("projection input should be final aggregate");
        let partial_agg = final_agg
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("final aggregate input should be partial aggregate");
        let scan = partial_agg
            .input()
            .downcast_ref::<DataSourceExec>()
            .expect("partial aggregate input should be scan");
        scan.data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("scan should use FileScanConfig")
    }

    #[test]
    fn rewrites_profitable_group_key_and_preserves_output_schema() -> Result<()> {
        let plan = aggregate_plan(&[240_000_000], &[Some(12_000_000)], true, true)?;
        let original_schema = plan.schema();
        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let scan = rewritten_scan(&optimized);
        assert_eq!(
            scan.file_schema().field(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        let optimized_again = optimize(Arc::clone(&optimized))?;
        assert!(
            Arc::ptr_eq(&optimized_again, &optimized),
            "dictionary rewrite should be idempotent"
        );
        Ok(())
    }

    #[test]
    fn rewrites_only_columns_that_pass_the_cost_gate() -> Result<()> {
        let plan = aggregate_plan(
            &[240_000_000, 80_000_000],
            &[Some(12_000_000), Some(40_000_000)],
            true,
            true,
        )?;
        let original_schema = plan.schema();
        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let scan = rewritten_scan(&optimized);
        assert!(matches!(
            scan.file_schema().field(0).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(scan.file_schema().field(1).data_type(), &DataType::Utf8View);
        Ok(())
    }

    #[test]
    fn rewrites_multiple_short_group_keys() -> Result<()> {
        let plan = aggregate_plan(
            &[10_000_000, 10_000_000],
            &[Some(2_000_000), Some(2_000_000)],
            true,
            true,
        )?;
        let optimized = optimize(plan)?;
        let scan = rewritten_scan(&optimized);
        for index in 0..2 {
            assert!(matches!(
                scan.file_schema().field(index).data_type(),
                DataType::Dictionary(_, _)
            ));
        }
        Ok(())
    }

    #[test]
    fn final_state_columns_do_not_alias_raw_aggregate_arguments() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let file = candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6);
        let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
        let raw_input_schema = scan.schema();
        let aggregate = Arc::new(
            AggregateExprBuilder::new(
                real_count_udaf(),
                vec![Arc::new(Column::new("value", 0))],
            )
            .schema(Arc::clone(&raw_input_schema))
            .alias("count_value")
            .build()?,
        );
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group_key", 1)) as Arc<dyn PhysicalExpr>,
                "group_key".to_string(),
            )]),
            vec![Arc::clone(&aggregate)],
            vec![None],
            scan,
            Arc::clone(&raw_input_schema),
        )?) as Arc<dyn ExecutionPlan>;
        let final_aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
                "group_key".to_string(),
            )]),
            vec![aggregate],
            vec![None],
            partial,
            raw_input_schema,
        )?) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(final_aggregate)?;
        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        Ok(())
    }

    #[test]
    fn skips_when_cost_or_storage_evidence_is_incomplete() -> Result<()> {
        let cases = [
            // The dictionary representation does not avoid enough value bytes.
            (240_000_000, Some(80_000_000), true, true),
            // The writer did not report decoded byte-array sizes.
            (240_000_000, None, true, true),
            // At least one data page is not dictionary encoded.
            (240_000_000, Some(12_000_000), false, true),
            // The file format supplied no physical encoding metadata.
            (240_000_000, Some(12_000_000), true, false),
        ];
        for (bytes, parquet_bytes, encoded, include_physical) in cases {
            let plan =
                aggregate_plan(&[bytes], &[parquet_bytes], encoded, include_physical)?;
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "ineligible plan should be returned unchanged"
            );
        }
        Ok(())
    }

    #[test]
    fn disabled_rule_returns_plan_unchanged() -> Result<()> {
        let plan = aggregate_plan(&[240_000_000], &[Some(12_000_000)], true, true)?;
        let config = ConfigOptions::new();
        let optimized =
            DictionaryPreservation::new().optimize(Arc::clone(&plan), &config)?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn aggregate_workload_cost_gate_boundaries() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 16, 4);
        let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
        let config = find_scan(&scan).expect("test scan should use FileScanConfig");
        let rows_at_threshold = MIN_AGGREGATE_VALUE_BYTES / UTF8_VIEW_SLOT_BYTES;

        assert!(!DictionaryPreservation::is_profitable(
            config,
            0,
            Some(rows_at_threshold - 1),
            DictionaryUse::Aggregate,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            0,
            Some(rows_at_threshold),
            DictionaryUse::Aggregate,
        ));
        assert!(!DictionaryPreservation::is_profitable(
            config,
            0,
            Some(usize::MAX),
            DictionaryUse::Aggregate,
        ));

        let narrow = candidate_file(
            "narrow.parquet",
            1,
            0,
            DataType::Utf8,
            1,
            MIN_VALUE_BYTES_PER_PARQUET_BYTE,
        );
        let scan = scan_plan(schema, vec![narrow], None, None, None)?;
        let config = find_scan(&scan).expect("test scan should use FileScanConfig");
        assert!(!DictionaryPreservation::is_profitable(
            config,
            0,
            Some(rows_at_threshold - 1),
            DictionaryUse::Aggregate,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            0,
            Some(rows_at_threshold),
            DictionaryUse::Aggregate,
        ));
        Ok(())
    }

    #[test]
    fn source_filtered_aggregate_cost_gate_boundaries() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let rows_at_threshold =
            MIN_SOURCE_FILTERED_AGGREGATE_VALUE_BYTES.div_ceil(UTF8_VIEW_SLOT_BYTES);
        for (footer_rows, input_rows, expected) in [
            (rows_at_threshold, Some(rows_at_threshold - 1), false),
            (rows_at_threshold, Some(rows_at_threshold), true),
            (rows_at_threshold, None, false),
            (10_000_000, Some(8_999_999), false),
            (10_000_000, Some(9_000_000), true),
            (10_000_000, Some(10_000_001), false),
        ] {
            let physical_statistics = PhysicalFileStatistics {
                num_rows: Some(footer_rows),
                column_statistics: vec![PhysicalColumnStatistics {
                    all_dictionary_encoded: true,
                    native_dictionary_value_type: Some(DataType::Utf8),
                    min_row_group_value_bytes_per_row: Some(16),
                    min_row_group_value_bytes_per_encoded_byte: Some(
                        MIN_VALUE_BYTES_PER_PARQUET_BYTE,
                    ),
                }],
            };
            let file = PartitionedFile::new("data.parquet", 1024)
                .with_extension(physical_statistics);
            let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
            let config = find_scan(&scan).expect("test scan should use FileScanConfig");
            assert_eq!(
                DictionaryPreservation::is_profitable(
                    config,
                    0,
                    input_rows,
                    DictionaryUse::SourceFilteredAggregate,
                ),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn filter_workload_cost_gate_boundaries() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "filter_key",
            DataType::Utf8View,
            false,
        )]));
        let saved_bytes_per_row = UTF8_VIEW_SLOT_BYTES - size_of::<i32>();
        let rows_at_threshold = MIN_FILTER_SAVED_BYTES.div_ceil(saved_bytes_per_row);
        for (rows, expected) in [
            (Some(rows_at_threshold - 1), false),
            (Some(rows_at_threshold), true),
            (None, false),
        ] {
            let physical_statistics = PhysicalFileStatistics {
                num_rows: rows,
                column_statistics: vec![PhysicalColumnStatistics {
                    all_dictionary_encoded: true,
                    native_dictionary_value_type: Some(DataType::Utf8),
                    min_row_group_value_bytes_per_row: Some(16),
                    min_row_group_value_bytes_per_encoded_byte: Some(
                        MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE,
                    ),
                }],
            };
            let file = PartitionedFile::new("data.parquet", 1024)
                .with_extension(physical_statistics);
            let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
            let config = find_scan(&scan).expect("test scan should use FileScanConfig");
            assert_eq!(
                DictionaryPreservation::is_profitable(
                    config,
                    0,
                    None,
                    DictionaryUse::Filter,
                ),
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn skips_missing_aggregate_input_rows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 24, 6);
        let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
        let config = find_scan(&scan)
            .expect("test scan should use FileScanConfig")
            .clone();
        let config = FileScanConfigBuilder::from(config)
            .with_statistics(Statistics::new_unknown(schema.as_ref()))
            .build();
        let plan = single_group_aggregate(DataSourceExec::from_data_source(config), 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_if_any_file_fails_the_cost_gate() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let good = candidate_file("good.parquet", 1, 0, DataType::Utf8, 24, 6);
        let bad = candidate_file("bad.parquet", 1, 0, DataType::Utf8, 8, 2);
        let scan = scan_plan(Arc::clone(&schema), vec![good, bad], None, None, None)?;
        let plan = single_group_aggregate(scan, 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(
            Arc::ptr_eq(&optimized, &plan),
            "a profitable first file must not mask an unprofitable later file"
        );
        Ok(())
    }

    #[test]
    fn skips_logical_utf8_view_backed_by_native_binary() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file("data.parquet", 1, 0, DataType::Binary, 24, 6);
        let scan = scan_plan(schema, vec![file], None, None, None)?;
        let plan = single_group_aggregate(scan, 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn handles_file_ranges_and_skips_reader_overrides() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let with_extra_extension =
            candidate_file("extra-extension.parquet", 1, 0, DataType::Utf8, 24, 6)
                .with_extension(String::from("reader-specific metadata"));
        let with_arrow_schema =
            candidate_file("arrow-schema.parquet", 1, 0, DataType::Utf8, 24, 6)
                .with_arrow_schema(Arc::clone(&schema));

        for file in [with_extra_extension, with_arrow_schema] {
            let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
            let plan = single_group_aggregate(scan, 0)?;
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "reader-specific file behavior must fail closed"
            );
        }

        for files in [
            vec![
                candidate_file("range.parquet", 1, 0, DataType::Utf8, 24, 6)
                    .with_range(0, 1024),
            ],
            vec![
                candidate_file("split.parquet", 1, 0, DataType::Utf8, 24, 6)
                    .with_range(0, 512),
                candidate_file("split.parquet", 1, 0, DataType::Utf8, 24, 6)
                    .with_range(512, 1024),
            ],
        ] {
            let scan = scan_plan(Arc::clone(&schema), files, None, None, None)?;
            let optimized = optimize(single_group_aggregate(scan, 0)?)?;
            let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
            assert!(matches!(
                scan.file_schema().field(0).data_type(),
                DataType::Dictionary(_, _)
            ));
        }

        let partial_range =
            candidate_file("partial.parquet", 1, 0, DataType::Utf8, 24, 6)
                .with_range(0, 512);
        let scan = scan_plan(schema, vec![partial_range], None, None, None)?;
        let plan = single_group_aggregate(scan, 0)?;
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_custom_expression_adapter() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 24, 6);
        let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
        let config = find_scan(&scan)
            .expect("test scan should use FileScanConfig")
            .clone();
        let config = FileScanConfigBuilder::from(config)
            .with_expr_adapter(Some(Arc::new(DefaultPhysicalExprAdapterFactory)))
            .build();
        let plan = single_group_aggregate(DataSourceExec::from_data_source(config), 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn retypes_scan_group_and_file_statistics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("untouched", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let untouched = ColumnStatistics {
            null_count: Precision::Exact(0),
            min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
            max_value: Precision::Exact(ScalarValue::Int64(Some(9))),
            sum_value: Precision::Exact(ScalarValue::Int64(Some(10))),
            distinct_count: Precision::Exact(9),
            byte_size: Precision::Exact(80_000_000),
        };
        let statistics = Statistics {
            num_rows: Precision::Exact(10_000_000),
            total_byte_size: Precision::Exact(320_000_000),
            column_statistics: vec![
                untouched.clone(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(ScalarValue::Utf8View(Some(
                        "alpha".to_string(),
                    ))),
                    max_value: Precision::Exact(ScalarValue::Utf8View(Some(
                        "omega".to_string(),
                    ))),
                    sum_value: Precision::Exact(ScalarValue::Utf8View(Some(
                        "not meaningful".to_string(),
                    ))),
                    distinct_count: Precision::Inexact(10),
                    byte_size: Precision::Exact(240_000_000),
                },
            ],
        };
        let file = candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6)
            .with_statistics(Arc::new(statistics.clone()));
        let group =
            FileGroup::new(vec![file]).with_statistics(Arc::new(statistics.clone()));
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(Arc::clone(&schema))),
        )
        .with_file_group(group)
        .with_statistics(statistics)
        .with_projection_indices(Some(vec![1, 0]))?
        .build();
        let plan = single_group_aggregate(DataSourceExec::from_data_source(config), 0)?;

        let optimized = optimize(plan)?;
        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        let assert_retyped = |statistics: &Statistics| {
            assert_eq!(statistics.total_byte_size, Precision::Absent);
            assert_eq!(statistics.column_statistics[0], untouched);
            let column = &statistics.column_statistics[1];
            assert_eq!(
                column.min_value,
                Precision::Exact(ScalarValue::Utf8(Some("alpha".to_string())))
            );
            assert_eq!(
                column.max_value,
                Precision::Exact(ScalarValue::Utf8(Some("omega".to_string())))
            );
            assert_eq!(column.sum_value, Precision::Absent);
            assert_eq!(column.byte_size, Precision::Absent);
            assert_eq!(column.distinct_count, Precision::Inexact(10));
        };
        assert_retyped(&scan.statistics());
        let group = &scan.file_groups[0];
        assert_retyped(
            group
                .file_statistics(None)
                .expect("group statistics should be retained"),
        );
        assert_retyped(
            group
                .iter()
                .next()
                .and_then(|file| file.statistics.as_deref())
                .expect("file statistics should be retained"),
        );
        Ok(())
    }

    #[test]
    fn skips_source_predicate_without_filter_exec() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("filter_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let predicate =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("filter_key", 0))))
                as Arc<dyn PhysicalExpr>;
        let file = candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6);
        let scan = scan_plan(schema, vec![file], None, Some(predicate), None)?;
        let plan = single_group_aggregate(scan, 1)?;
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn source_filtered_aggregate_requires_near_passthrough_estimate() -> Result<()> {
        for (upper_bound, pushdown_filters, expected_rewrite) in [
            (94_i64, false, true),
            (19_i64, false, false),
            (94_i64, true, false),
        ] {
            let schema = Arc::new(Schema::new(vec![
                Field::new("filter_key", DataType::Int64, false),
                Field::new("group_key", DataType::Utf8View, false),
            ]));
            let predicate = Arc::new(BinaryExpr::new(
                Arc::new(Column::new("filter_key", 0)),
                Operator::LtEq,
                Arc::new(Literal::new(ScalarValue::Int64(Some(upper_bound)))),
            )) as Arc<dyn PhysicalExpr>;
            let source = ParquetSource::new(Arc::clone(&schema))
                .with_predicate(Arc::clone(&predicate))
                .with_pushdown_filters(pushdown_filters);
            let statistics = Statistics {
                num_rows: Precision::Exact(10_000_000),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    ColumnStatistics {
                        null_count: Precision::Exact(0),
                        min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                        max_value: Precision::Exact(ScalarValue::Int64(Some(99))),
                        ..Default::default()
                    },
                    ColumnStatistics {
                        null_count: Precision::Exact(0),
                        ..Default::default()
                    },
                ],
            };
            let scan = DataSourceExec::from_data_source(
                FileScanConfigBuilder::new(
                    ObjectStoreUrl::local_filesystem(),
                    Arc::new(source),
                )
                .with_file(candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6))
                .with_statistics(statistics)
                .build(),
            );
            let filter =
                Arc::new(FilterExec::try_new(predicate, scan)?) as Arc<dyn ExecutionPlan>;
            let config = ConfigOptions::new();
            let context = ConfigOnlyContext::new(&config);
            let filtered_rows = context
                .compute_statistics(&filter, &StatisticsArgs::new())?
                .num_rows;
            assert!(matches!(filtered_rows, Precision::Inexact(_)));

            let plan = single_group_aggregate(filter, 1)?;
            let optimized = optimize(Arc::clone(&plan))?;
            assert_eq!(!Arc::ptr_eq(&optimized, &plan), expected_rewrite);
        }
        Ok(())
    }

    #[test]
    fn skips_scan_predicate_on_candidate_and_limit() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let candidate_predicate =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("group_key", 0))))
                as Arc<dyn PhysicalExpr>;
        let cases = [(Some(candidate_predicate), None), (None, Some(100))];

        for (predicate, limit) in cases {
            let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 24, 6);
            let scan =
                scan_plan(Arc::clone(&schema), vec![file], None, predicate, limit)?;
            let plan = single_group_aggregate(scan, 0)?;
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(Arc::ptr_eq(&optimized, &plan));
        }
        Ok(())
    }

    #[test]
    fn traverses_filter_that_does_not_reference_candidate() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("filter_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let scan = profitable_scan(schema);
        let predicate =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("filter_key", 0))));
        let filter =
            Arc::new(FilterExec::try_new(predicate, scan)?) as Arc<dyn ExecutionPlan>;
        let optimized = optimize(single_group_aggregate(filter, 1)?)?;
        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        Ok(())
    }

    #[test]
    fn rewrites_equality_filter_only_column() -> Result<()> {
        let plan = equality_filter_plan(false, MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE)?;
        let output_schema = plan.schema();
        let optimized = optimize_filter(plan)?;
        assert_eq!(optimized.schema(), output_schema);
        let filter = optimized
            .downcast_ref::<FilterExec>()
            .expect("optimized plan should remain a filter");
        let arrays = vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(DictionaryArray::<Int32Type>::from_iter([
                Some("target"),
                Some("other"),
                None,
            ])) as ArrayRef,
        ];
        let batch = RecordBatch::try_new(filter.input().schema(), arrays.clone())?;
        assert_predicate_result(
            filter.predicate(),
            &batch,
            vec![Some(true), Some(false), None],
        )?;

        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        let source_predicate = scan
            .file_source
            .filter()
            .expect("rewritten Parquet source should retain its predicate");
        let batch = RecordBatch::try_new(
            Arc::clone(scan.file_source.table_schema().table_schema()),
            arrays,
        )?;
        assert_predicate_result(
            &source_predicate,
            &batch,
            vec![Some(true), Some(false), None],
        )?;
        Ok(())
    }

    #[test]
    fn rewrites_q19_filter_with_reordered_scan_projection() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("measure", DataType::Int64, false),
            Field::new("shipmode", DataType::Utf8View, true),
            Field::new("shipinstruct", DataType::Utf8View, true),
        ]));
        let source_predicate = q19_filter_predicate(&table_schema, 1, 2)?;
        let source = ParquetSource::new(Arc::clone(&table_schema))
            .with_predicate(source_predicate);
        let candidate = || PhysicalColumnStatistics {
            all_dictionary_encoded: true,
            native_dictionary_value_type: Some(DataType::Utf8),
            min_row_group_value_bytes_per_row: Some(12),
            min_row_group_value_bytes_per_encoded_byte: Some(
                MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE,
            ),
        };
        let file = PartitionedFile::new("lineitem.parquet", 1024).with_extension(
            PhysicalFileStatistics {
                num_rows: Some(10_000_000),
                column_statistics: vec![
                    PhysicalColumnStatistics::default(),
                    candidate(),
                    candidate(),
                ],
            },
        );
        let statistics = Statistics {
            num_rows: Precision::Exact(10_000_000),
            total_byte_size: Precision::Absent,
            column_statistics: table_schema
                .fields()
                .iter()
                .map(|_| ColumnStatistics::default())
                .collect(),
        };
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source),
        )
        .with_file(file)
        .with_statistics(statistics)
        .with_projection_indices(Some(vec![2, 0, 1]))?
        .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        let scan_predicate = q19_filter_predicate(scan.schema().as_ref(), 2, 0)?;
        let plan = Arc::new(
            FilterExecBuilder::new(scan_predicate, scan)
                .apply_projection(Some(vec![1]))?
                .build()?,
        ) as Arc<dyn ExecutionPlan>;
        let output_schema = plan.schema();

        let optimized = optimize_filter(plan)?;
        assert_eq!(optimized.schema(), output_schema);
        let filter = optimized
            .downcast_ref::<FilterExec>()
            .expect("optimized plan should remain a filter");
        let measure = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let shipmode = Arc::new(DictionaryArray::<Int32Type>::from_iter([
            Some("AIR"),
            Some("SHIP"),
            Some("SHIP"),
            Some("AIR"),
            None,
            Some("TRUCK"),
        ])) as ArrayRef;
        let shipinstruct = Arc::new(DictionaryArray::<Int32Type>::from_iter([
            Some("DELIVER IN PERSON"),
            Some("DELIVER IN PERSON"),
            Some("TAKE BACK RETURN"),
            None,
            Some("OTHER"),
            Some("TAKE BACK RETURN"),
        ])) as ArrayRef;
        let expected = vec![Some(true), Some(false), Some(true), None, Some(false), None];
        let batch = RecordBatch::try_new(
            filter.input().schema(),
            vec![
                Arc::clone(&shipinstruct),
                Arc::clone(&measure),
                Arc::clone(&shipmode),
            ],
        )?;
        assert_predicate_result(filter.predicate(), &batch, expected.clone())?;

        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert!(matches!(
            scan.file_schema().field(2).data_type(),
            DataType::Dictionary(_, _)
        ));
        let source_predicate = scan
            .file_source
            .filter()
            .expect("rewritten Parquet source should retain its predicate");
        let batch = RecordBatch::try_new(
            Arc::clone(scan.file_source.table_schema().table_schema()),
            vec![measure, shipmode, shipinstruct],
        )?;
        assert_predicate_result(&source_predicate, &batch, expected)?;
        Ok(())
    }

    #[test]
    fn skips_group_key_rewrite_when_filter_has_fetch() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group_key", DataType::Utf8View, false),
            Field::new("filter_key", DataType::Int64, false),
        ]));
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("filter_key", 1)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
        )) as Arc<dyn PhysicalExpr>;
        let file = candidate_file("data.parquet", 2, 0, DataType::Utf8, 24, 6);
        let scan =
            scan_plan(schema, vec![file], None, Some(Arc::clone(&predicate)), None)?;
        let filtered = Arc::new(
            FilterExecBuilder::new(predicate, scan)
                .with_fetch(Some(1))
                .build()?,
        ) as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(filtered, 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_filter_on_group_key() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        // This is the filter shape from TPC-H Q16. It regressed in the hosted
        // benchmark and must not opt a surviving grouping column into a dictionary.
        let predicate = Arc::new(LikeExpr::new(
            true,
            false,
            Arc::new(Column::new("group_key", 0)),
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                "MEDIUM POLISHED%".to_string(),
            )))),
        )) as Arc<dyn PhysicalExpr>;
        let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 16, 8);
        let scan = scan_plan(
            Arc::clone(&schema),
            vec![file],
            None,
            Some(Arc::clone(&predicate)),
            None,
        )?;
        let filtered =
            Arc::new(FilterExec::try_new(predicate, scan)?) as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(filtered, 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_filter_column_that_survives_or_misses_cost_gate() -> Result<()> {
        let cases = [
            equality_filter_plan(true, MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE)?,
            equality_filter_plan(false, MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE - 1)?,
        ];
        for plan in cases {
            let optimized = optimize_filter(Arc::clone(&plan))?;
            assert!(Arc::ptr_eq(&optimized, &plan));
        }
        Ok(())
    }

    #[test]
    fn skips_filter_rewrite_with_fetch_scan_limit_or_exact_filter() -> Result<()> {
        let base = equality_filter_plan(false, MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE)?;
        let filter = base
            .downcast_ref::<FilterExec>()
            .expect("test plan should be a filter");
        let fetched = Arc::new(
            FilterExecBuilder::from(filter)
                .with_fetch(Some(1))
                .build()?,
        ) as Arc<dyn ExecutionPlan>;

        let config = find_scan(&base)
            .expect("test plan should contain a file scan")
            .clone();
        let limited_scan = DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config.clone())
                .with_limit(Some(1))
                .build(),
        );
        let limited = Arc::new(
            FilterExecBuilder::from(filter)
                .with_input(limited_scan)
                .build()?,
        ) as Arc<dyn ExecutionPlan>;

        let parquet = config
            .file_source
            .downcast_ref::<ParquetSource>()
            .expect("test scan should use ParquetSource")
            .clone()
            .with_pushdown_filters(true);
        assert!(parquet.exact_filter().is_some());
        let exact_scan = DataSourceExec::from_data_source(
            FileScanConfigBuilder::from(config)
                .with_source(Arc::new(parquet))
                .build(),
        );
        let exact = Arc::new(
            FilterExecBuilder::from(filter)
                .with_input(exact_scan)
                .build()?,
        ) as Arc<dyn ExecutionPlan>;

        for plan in [fetched, limited, exact] {
            let optimized = optimize_filter(Arc::clone(&plan))?;
            assert!(Arc::ptr_eq(&optimized, &plan));
        }
        Ok(())
    }

    #[test]
    fn skips_non_equality_filter_use() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "filter_key",
            DataType::Utf8View,
            false,
        )]));
        let predicates = [
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("filter_key", 0))))
                as Arc<dyn PhysicalExpr>,
            Arc::new(LikeExpr::new(
                false,
                false,
                Arc::new(Column::new("filter_key", 0)),
                Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                    "prefix%".to_string(),
                )))),
            )) as Arc<dyn PhysicalExpr>,
        ];
        for predicate in predicates {
            let file = candidate_file(
                "data.parquet",
                1,
                0,
                DataType::Utf8,
                12,
                MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE,
            );
            let scan = scan_plan(
                Arc::clone(&schema),
                vec![file],
                None,
                Some(Arc::clone(&predicate)),
                None,
            )?;
            let plan = Arc::new(
                FilterExecBuilder::new(predicate, scan)
                    .apply_projection(Some(vec![]))?
                    .build()?,
            ) as Arc<dyn ExecutionPlan>;

            let optimized = optimize_filter(Arc::clone(&plan))?;
            assert!(Arc::ptr_eq(&optimized, &plan));
        }
        Ok(())
    }

    #[test]
    fn skips_when_aggregate_reads_candidate_column() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file("data.parquet", 1, 0, DataType::Utf8, 24, 6);
        let scan = scan_plan(schema, vec![file], None, None, None)?;
        let input_schema = scan.schema();
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
            "group_key".to_string(),
        )]);
        let count = AggregateExprBuilder::new(
            count_udaf(),
            vec![Arc::new(Column::new("group_key", 0))],
        )
        .schema(Arc::clone(&input_schema))
        .alias("count_group_key")
        .build()?;
        let plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![Arc::new(count)],
            vec![None],
            scan,
            input_schema,
        )?) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn rewrites_reordered_scan_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let file = candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6);
        let scan = scan_plan(
            Arc::clone(&schema),
            vec![file],
            Some(vec![1, 0]),
            None,
            None,
        )?;
        let plan = single_group_aggregate(scan, 0)?;
        let optimized = optimize(plan)?;

        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert_eq!(
            scan.file_schema().field(1).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(
            scan.projected_schema()?.field(0).data_type(),
            scan.file_schema().field(1).data_type()
        );
        Ok(())
    }

    #[test]
    fn skips_duplicated_scan_projection() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let file = candidate_file("data.parquet", 2, 1, DataType::Utf8, 24, 6);
        let scan = scan_plan(schema, vec![file], Some(vec![1, 1]), None, None)?;
        let plan = single_group_aggregate(scan, 0)?;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_unfiltered_inexact_join_payload_through_projection() -> Result<()> {
        let plan = join_aggregate_plan(JoinPlanShape::Safe)?;
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_unfiltered_inexact_right_join_payload() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "join_key",
            DataType::Int64,
            false,
        )]));
        let left = scan_plan(
            left_schema,
            vec![PartitionedFile::new("left.parquet", 1024)],
            None,
            None,
            None,
        )?;
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let right = profitable_scan(right_schema);
        let join = Arc::new(HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("join_key", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![2]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(join, 0)?;
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_small_post_join_payload() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![Field::new(
            "join_key",
            DataType::Int64,
            false,
        )]));
        let left: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(left_schema));
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let right = profitable_scan(right_schema);
        let join = Arc::new(HashJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("join_key", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![2]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(join, 0)?;

        // The source is otherwise profitable, but the empty build side proves
        // that no rows reach the aggregate.
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn skips_unsafe_join_lineage() -> Result<()> {
        for plan in [
            join_aggregate_plan(JoinPlanShape::CandidateIsJoinKey)?,
            join_aggregate_plan(JoinPlanShape::InputFilter)?,
            join_aggregate_plan(JoinPlanShape::DuplicateProjection)?,
            join_aggregate_plan(JoinPlanShape::FilteredJoin)?,
            join_aggregate_plan(JoinPlanShape::FetchedJoin)?,
            join_aggregate_plan(JoinPlanShape::LeftJoin)?,
            join_aggregate_plan(JoinPlanShape::DuplicateJoinProjection)?,
        ] {
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "unsafe lineage should be returned unchanged"
            );
        }
        Ok(())
    }

    #[test]
    fn skips_unfiltered_inexact_shared_scan() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let shared_scan = profitable_scan(schema);
        let join = Arc::new(HashJoinExec::try_new(
            Arc::clone(&shared_scan),
            shared_scan,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("join_key", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![1]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let input_schema = join.schema();
        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
            "group_key".to_string(),
        )]);
        let plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            join,
            input_schema,
        )?) as Arc<dyn ExecutionPlan>;

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }
}
