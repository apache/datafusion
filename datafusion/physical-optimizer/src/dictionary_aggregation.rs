// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Preserve profitable file-native dictionary encodings through supported operators.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Constraint, JoinSide, JoinType, NullEquality, PhysicalFileStatistics, Result,
    ScalarValue, Statistics,
};
use datafusion_datasource::file::try_rewrite_dictionary_filter_predicate;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_expr::Operator;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_functions::regex::regexpreplace::RegexpReplaceFunc;
use datafusion_functions::string::octet_length::OctetLengthFunc;
use datafusion_functions_aggregate::average::Avg;
use datafusion_functions_aggregate::count::Count;
use datafusion_functions_aggregate::min_max::Min;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, CastExpr, Column, InListExpr, Literal, cast,
};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{Partitioning, PhysicalExpr, ScalarFunctionExpr};
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateInputMode, AggregateMode, AggregateOutputMode,
    PhysicalGroupBy,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
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
// Join cardinalities are estimates, but the representation still reaches the
// aggregate without being evaluated when it is carried as an ordinary payload
// column. Require substantially more estimated work than an exact input.
const MIN_JOINED_AGGREGATE_VALUE_BYTES: usize = 64 * 1024;
// The Q21-shaped join path repeats a small, unique build-side payload far
// beyond its source cardinality. Require both strong amplification and a
// material reduction in the repeated string representation.
const MIN_AMPLIFIED_JOIN_ROWS_PER_SOURCE_ROW: usize = 64;
const MIN_AMPLIFIED_JOIN_SAVED_BYTES: usize = 32 * 1024 * 1024;
// A dictionary used only as join payload can be restored before aggregation,
// so a smaller proven join-side saving is sufficient.
const MIN_JOIN_PAYLOAD_ROWS_PER_SOURCE_ROW: usize = 4;
const MIN_JOIN_PAYLOAD_SAVED_BYTES: usize = 1024 * 1024;
const MAX_JOIN_PAYLOAD_SOURCE_ROWS: usize = 64 * 1024;
// Very small dimensions can save meaningful work across a unique-build star
// join without clearing the general per-column byte threshold.
const MAX_TINY_DIMENSION_ROWS: usize = 64;
const MIN_TINY_DIMENSION_SAVED_BYTES: usize = 4 * 1024 * 1024;
// A scan-local derived transform can amortize dictionary construction across
// both the transform and aggregate expressions. Only consider this fallback
// for large inputs; unlike the ordinary aggregate path, some file formats do
// not provide decoded value-byte statistics for these columns.
const MIN_DERIVED_AGGREGATE_ROWS: usize = 64 * 1024 * 1024;
// A tiny dictionary page followed by predominantly plain pages is not useful
// evidence for the derived scan/aggregate path. Require every non-empty row
// group to start with a material dictionary page before changing the scan
// representation for this otherwise storage-statistics-poor shape.
const MIN_DERIVED_DICTIONARY_PAGE_BYTES: usize = 16 * 1024;
// The filtered two-key DISTINCT rollup has no decoded-value byte statistics in
// some Parquet files. Require a large source and prove that every data page is
// dictionary encoded before using its narrower structural cost gate.
const MIN_FILTERED_DISTINCT_ROLLUP_ROWS: usize = 64 * 1024 * 1024;
// A filter-only rewrite only saves the difference between a 16-byte Utf8View
// slot and a 4-byte dictionary key. Require enough rows for those savings to
// matter before paying for a different scan representation.
const MIN_FILTER_SAVED_BYTES: usize = 64 * 1024 * 1024;
// Wide joined rollups retain every dictionary's values in aggregate output
// batches. Keep their group width bounded until that cost is modeled directly.
const MAX_INEXACT_JOINED_GROUP_EXPRESSIONS: usize = 8;
// Restoring a grouping key can invalidate the aggregate's hash partitioning.
// If that key feeds another hash join, require enough row reduction to repay
// a replacement repartition after restoration.
const MIN_JOIN_PAYLOAD_AGGREGATE_REDUCTION: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DictionaryUse {
    Aggregate,
    FilteredDistinctRollup,
    JoinedAggregate,
    SemiJoinedAggregate,
    JoinedAggregateWithCompanion,
    TransformedJoinedAggregate,
    AmplifiedJoinPayload,
    JoinPayload,
    DerivedAggregate,
    Filter,
}

impl DictionaryUse {
    fn min_value_bytes_per_parquet_byte(self) -> usize {
        match self {
            Self::Aggregate
            | Self::FilteredDistinctRollup
            | Self::JoinedAggregate
            | Self::SemiJoinedAggregate
            | Self::JoinedAggregateWithCompanion
            | Self::TransformedJoinedAggregate
            | Self::AmplifiedJoinPayload
            | Self::JoinPayload
            | Self::DerivedAggregate => MIN_VALUE_BYTES_PER_PARQUET_BYTE,
            Self::Filter => MIN_FILTER_VALUE_BYTES_PER_PARQUET_BYTE,
        }
    }

    fn min_work_bytes(self) -> usize {
        match self {
            Self::Aggregate => MIN_AGGREGATE_VALUE_BYTES,
            Self::FilteredDistinctRollup => 0,
            Self::JoinedAggregate
            | Self::SemiJoinedAggregate
            | Self::JoinedAggregateWithCompanion
            | Self::TransformedJoinedAggregate => MIN_JOINED_AGGREGATE_VALUE_BYTES,
            Self::AmplifiedJoinPayload => MIN_AMPLIFIED_JOIN_SAVED_BYTES,
            Self::JoinPayload => MIN_JOIN_PAYLOAD_SAVED_BYTES,
            Self::DerivedAggregate => 0,
            Self::Filter => MIN_FILTER_SAVED_BYTES,
        }
    }

    fn work_bytes_per_row(self, value_bytes_per_row: usize) -> usize {
        match self {
            Self::Aggregate
            | Self::FilteredDistinctRollup
            | Self::JoinedAggregate
            | Self::SemiJoinedAggregate
            | Self::JoinedAggregateWithCompanion
            | Self::TransformedJoinedAggregate
            | Self::AmplifiedJoinPayload
            | Self::JoinPayload
            | Self::DerivedAggregate => value_bytes_per_row.max(UTF8_VIEW_SLOT_BYTES),
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
    group_indices: Vec<usize>,
    aggregate_input_index: usize,
    scan: Arc<dyn ExecutionPlan>,
    table_index: usize,
    scan_path: Vec<usize>,
    /// Whether the scan-to-aggregate path contains a `FilterExec`.
    has_filter_exec: bool,
    /// Whether the candidate is carried through a supported join as payload.
    has_join: bool,
    /// Whether a derived grouping expression justifies rebuilding a supported
    /// comparison filter on this candidate.
    allows_candidate_filter: bool,
    /// Whether the candidate is transformed by a grouping expression rather
    /// than used as a direct key.
    has_transformed_group: bool,
    /// Whether this is a scan-local transform that also feeds aggregate
    /// expressions and filters on the same source column.
    derived_scan_aggregate: bool,
    /// Whether this is the filtered two-key deduplication chain produced for a
    /// grouped single-column `COUNT(DISTINCT ...)`.
    filtered_distinct_rollup: bool,
    /// Preserved-side input captured at a semi join. This marks the narrower
    /// semi-join cost path but is not used to inflate aggregate input rows.
    preserved_input_rows: Option<usize>,
    /// Repeated rows proven by the narrow unique-build-side join shape.
    amplified_join_payload_rows: Option<usize>,
    /// Whether to decode this candidate after its join payload work and before
    /// the first aggregate.
    restore_before_aggregate: bool,
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
    /// Whether the candidate is carried through a supported join as payload.
    has_join: bool,
    /// Whether a filter on the path evaluates the candidate.
    candidate_filter: bool,
    /// Whether the path has exactly one direct `!= literal` candidate filter.
    derived_candidate_filter: bool,
    /// Whether the path has exactly one direct `!= ''` candidate filter.
    empty_string_candidate_filter: bool,
    /// Preserved-side input captured below a semi join.
    preserved_input_rows: Option<usize>,
    /// Whether the first join above the source is the audited unique
    /// `CollectLeft` inner-join build-side payload shape.
    amplified_join_anchor: bool,
    /// Whether every join after the anchor remains in the audited path.
    amplified_join_path_valid: bool,
    /// Whether the first join above the source has the Q21 anchor's broad
    /// inner-join, left-payload shape, even if it misses a required proof.
    amplified_join_candidate: bool,
    /// Whether a Q21-like anchor candidate is followed by a semi/anti join.
    amplified_join_has_downstream_join: bool,
    /// Largest preserved-side input observed at a downstream semi/anti join.
    amplified_join_payload_rows: Option<usize>,
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

#[derive(Debug)]
struct DownstreamJoinAggregate {
    aggregate: Arc<dyn ExecutionPlan>,
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
        feeds_partitioned_join: bool,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(final_agg) = plan.downcast_ref::<AggregateExec>() else {
            return Ok(None);
        };

        match final_agg.mode() {
            AggregateMode::Single | AggregateMode::SinglePartitioned => {
                Self::rewrite_aggregate_chain(
                    final_agg,
                    None,
                    context,
                    feeds_partitioned_join,
                )
            }
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                let Some(partial_agg) = final_agg.input().downcast_ref::<AggregateExec>()
                else {
                    return Ok(None);
                };
                if partial_agg.mode() != &AggregateMode::Partial {
                    return Ok(None);
                }
                Self::rewrite_aggregate_chain(
                    final_agg,
                    Some(partial_agg),
                    context,
                    feeds_partitioned_join,
                )
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
            {
                continue;
            }
            let Some(table_index) = Self::scan_table_index(config, input_index) else {
                continue;
            };
            if !Self::candidate_has_supported_filter_use(filter.predicate(), input_index)
                || source_predicate.as_ref().is_some_and(|predicate| {
                    !Self::candidate_has_supported_filter_use(predicate, table_index)
                })
                || !Self::is_profitable(config, table_index, None, DictionaryUse::Filter)
            {
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

    fn candidate_has_supported_group_filter_use(
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
                    Self::candidate_has_supported_group_filter_use(
                        binary.left(),
                        candidate_index,
                    ) && Self::candidate_has_supported_group_filter_use(
                        binary.right(),
                        candidate_index,
                    )
                }
                Operator::Eq | Operator::NotEq => {
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

    fn is_direct_not_equal_literal(
        expr: &Arc<dyn PhysicalExpr>,
        candidate_index: usize,
    ) -> bool {
        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            return false;
        };
        if binary.op() != &Operator::NotEq {
            return false;
        }
        let is_candidate = |expr: &Arc<dyn PhysicalExpr>| {
            expr.downcast_ref::<Column>()
                .is_some_and(|column| column.index() == candidate_index)
        };
        let is_non_null_literal = |expr: &Arc<dyn PhysicalExpr>| {
            expr.downcast_ref::<Literal>()
                .is_some_and(|literal| !literal.value().is_null())
        };
        (is_candidate(binary.left()) && is_non_null_literal(binary.right()))
            || (is_candidate(binary.right()) && is_non_null_literal(binary.left()))
    }

    fn is_direct_not_equal_empty_string(
        expr: &Arc<dyn PhysicalExpr>,
        candidate_index: usize,
    ) -> bool {
        let Some(binary) = expr.downcast_ref::<BinaryExpr>() else {
            return false;
        };
        if binary.op() != &Operator::NotEq {
            return false;
        }
        let is_candidate = |expr: &Arc<dyn PhysicalExpr>| {
            expr.downcast_ref::<Column>()
                .is_some_and(|column| column.index() == candidate_index)
        };
        let is_empty_string = |expr: &Arc<dyn PhysicalExpr>| {
            expr.downcast_ref::<Literal>()
                .is_some_and(|literal| literal.value().try_as_str() == Some(Some("")))
        };
        (is_candidate(binary.left()) && is_empty_string(binary.right()))
            || (is_candidate(binary.right()) && is_empty_string(binary.left()))
    }

    /// Map one grouping output of the two-stage deduplication aggregate created
    /// by `SingleDistinctToGroupBy` back to its raw input column.
    fn filtered_distinct_inner_input_index(
        final_agg: &AggregateExec,
        output_index: usize,
    ) -> Option<usize> {
        if !matches!(
            final_agg.mode(),
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ) || final_agg.group_expr().expr().len() != 2
            || final_agg.group_expr().has_grouping_set()
            || !final_agg.group_expr().null_expr().is_empty()
            || !final_agg.aggr_expr().is_empty()
            || !final_agg.filter_expr().is_empty()
            || final_agg.limit_options().is_some()
        {
            return None;
        }
        let partial_agg = final_agg.input().downcast_ref::<AggregateExec>()?;
        if partial_agg.mode() != &AggregateMode::Partial
            || partial_agg.group_expr().expr().len() != 2
            || partial_agg.group_expr().has_grouping_set()
            || !partial_agg.group_expr().null_expr().is_empty()
            || !partial_agg.aggr_expr().is_empty()
            || !partial_agg.filter_expr().is_empty()
            || partial_agg.limit_options().is_some()
            || final_agg.group_expr() != &partial_agg.group_expr().as_final()
        {
            return None;
        }
        // Both stages must be a direct two-column deduplication. This also
        // rejects duplicated or reordered state columns.
        let final_columns = final_agg
            .group_expr()
            .expr()
            .iter()
            .map(|(expr, _)| expr.downcast_ref::<Column>().map(Column::index))
            .collect::<Option<Vec<_>>>()?;
        if final_columns != [0, 1] {
            return None;
        }
        let partial_columns = partial_agg
            .group_expr()
            .expr()
            .iter()
            .map(|(expr, _)| expr.downcast_ref::<Column>().map(Column::index))
            .collect::<Option<Vec<_>>>()?;
        if partial_columns[0] == partial_columns[1] {
            return None;
        }
        partial_columns.get(output_index).copied()
    }

    fn is_supported_filtered_distinct_rollup(
        aggregate: &AggregateExec,
        candidate_index: usize,
        group_indices: &[usize],
        aggregate_indices: &[usize],
    ) -> bool {
        if aggregate.mode() != &AggregateMode::Partial
            || aggregate.group_expr().expr().len() != 1
            || aggregate.group_expr().has_grouping_set()
            || !aggregate.group_expr().null_expr().is_empty()
            || aggregate.group_expr().groups() != [vec![false]]
            || group_indices != [0]
            || !aggregate_indices.is_empty()
            || aggregate.aggr_expr().len() != 1
            || aggregate.filter_expr().len() != 1
            || aggregate.filter_expr()[0].is_some()
            || aggregate.limit_options().is_some()
        {
            return false;
        }
        let Some(group_column) =
            aggregate.group_expr().expr()[0].0.downcast_ref::<Column>()
        else {
            return false;
        };
        if group_column.index() != candidate_index {
            return false;
        }
        let count = &aggregate.aggr_expr()[0];
        if !count.fun().inner().is::<Count>()
            || count.is_distinct()
            || count.ignore_nulls()
            || count.is_reversed()
            || !count.order_bys().is_empty()
        {
            return false;
        }
        let arguments = count.expressions();
        let [argument] = arguments.as_slice() else {
            return false;
        };
        let Some(argument) = argument.downcast_ref::<Column>() else {
            return false;
        };
        if candidate_index == argument.index() {
            return false;
        }
        let Some(inner_final) = aggregate.input().downcast_ref::<AggregateExec>() else {
            return false;
        };
        let Some(candidate_input) =
            Self::filtered_distinct_inner_input_index(inner_final, candidate_index)
        else {
            return false;
        };
        let Some(argument_input) =
            Self::filtered_distinct_inner_input_index(inner_final, argument.index())
        else {
            return false;
        };
        if candidate_input == argument_input {
            return false;
        }
        let inner_partial = inner_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("validated filtered DISTINCT chain has a partial stage");
        inner_partial
            .input()
            .schema()
            .fields()
            .get(argument_input)
            .is_some_and(|field| field.data_type().is_integer())
    }

    fn rewrite_aggregate_chain(
        final_agg: &AggregateExec,
        partial_agg: Option<&AggregateExec>,
        context: &dyn PhysicalOptimizerContext,
        feeds_partitioned_join: bool,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let raw_agg = partial_agg.unwrap_or(final_agg);
        if raw_agg.group_expr().is_empty()
            || raw_agg.limit_options().is_some()
            || final_agg.limit_options().is_some()
        {
            return Ok(None);
        }

        let group_columns = Self::group_columns(raw_agg, context)?;
        if group_columns.is_empty() {
            return Ok(None);
        }

        // A dictionary that is cheap at the scan can still be a loss when an
        // intervening join or projection leaves too little work at the
        // aggregate. Cost the representation change where it is consumed.
        let statistics =
            context.compute_statistics(raw_agg.input(), &StatisticsArgs::new())?;
        // An inexact zero can be estimator underflow for a non-empty join
        // chain (TPC-H Q21). Only trust it when the plan also contains an
        // explicit empty input, as in the eliminated TPC-DS Q36 branch.
        if matches!(statistics.num_rows, Precision::Absent | Precision::Exact(0))
            || (matches!(statistics.num_rows, Precision::Inexact(0))
                && Self::contains_empty_exec(raw_agg.input()))
        {
            return Ok(None);
        }
        let downstream_join_reduction_sufficient = if feeds_partitioned_join
            && group_columns
                .iter()
                .any(|column| !column.group_indices.is_empty())
        {
            let final_agg_plan: Arc<dyn ExecutionPlan> = Arc::new(final_agg.clone());
            let output_statistics =
                context.compute_statistics(&final_agg_plan, &StatisticsArgs::new())?;
            Self::has_minimum_aggregate_reduction(
                &statistics.num_rows,
                &output_statistics.num_rows,
            )
        } else {
            true
        };
        let raw_aggregate_statistics = if group_columns
            .iter()
            .any(|column| column.amplified_join_payload_rows.is_some())
        {
            let raw_agg_plan: Arc<dyn ExecutionPlan> = Arc::new(raw_agg.clone());
            Some(context.compute_statistics(&raw_agg_plan, &StatisticsArgs::new())?)
        } else {
            None
        };

        let joined_scans_with_strong_candidate: HashSet<_> = group_columns
            .iter()
            .filter(|column| column.has_join && !column.group_indices.is_empty())
            .filter_map(|column| {
                let scan = column.scan.downcast_ref::<DataSourceExec>()?;
                let config = scan.data_source().downcast_ref::<FileScanConfig>()?;
                Self::has_strong_storage_evidence(config, column.table_index)
                    .then(|| column.scan_path.clone())
            })
            .collect();
        let mut group_columns: Vec<_> = group_columns
            .into_iter()
            .filter_map(|mut column| {
                if !downstream_join_reduction_sufficient
                    && !column.group_indices.is_empty()
                {
                    return None;
                }
                // Joined estimates are especially unreliable for wide rollups
                // with many aggregate expressions. Fail closed for direct
                // group keys in that shape; aggregate-only CASE candidates use
                // a separately audited path.
                if column.has_join
                    && !column.group_indices.is_empty()
                    && matches!(statistics.num_rows, Precision::Inexact(_))
                    && (raw_agg.aggr_expr().len() > 5
                        || raw_agg.group_expr().expr().len()
                            > MAX_INEXACT_JOINED_GROUP_EXPRESSIONS)
                {
                    return None;
                }
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
                            || (column.allows_candidate_filter
                                && Self::candidate_has_supported_group_filter_use(
                                    &predicate,
                                    table_index,
                                ))
                    });
                let derived_source_filter_supported = !column.derived_scan_aggregate
                    || config.file_source.filter().is_none_or(|predicate| {
                        Self::is_direct_not_equal_literal(&predicate, table_index)
                    });
                let filtered_distinct_source_filter_supported = !column
                    .filtered_distinct_rollup
                    || config.file_source.filter().is_none_or(|predicate| {
                        Self::is_direct_not_equal_empty_string(&predicate, table_index)
                    });
                let has_source_filter = config.file_source.filter().is_some();
                let (input_rows, dictionary_use) = match statistics.num_rows {
                    Precision::Exact(_) | Precision::Inexact(_)
                        if column.filtered_distinct_rollup =>
                    {
                        // This path deliberately uses source/footer rows: the
                        // post-dedup aggregate estimate is not the amount of
                        // work saved by preserving the scan dictionary.
                        (None, DictionaryUse::FilteredDistinctRollup)
                    }
                    Precision::Exact(rows) | Precision::Inexact(rows)
                        if column.derived_scan_aggregate =>
                    {
                        (Some(rows), DictionaryUse::DerivedAggregate)
                    }
                    Precision::Exact(rows) | Precision::Inexact(rows)
                        if column.has_join =>
                    {
                        let (aggregate_rows, dictionary_use) =
                            if let Some(rows) = column.amplified_join_payload_rows {
                                (rows, DictionaryUse::AmplifiedJoinPayload)
                            } else if column.has_transformed_group {
                                (rows, DictionaryUse::TransformedJoinedAggregate)
                            } else if joined_scans_with_strong_candidate
                                .contains(&column.scan_path)
                                && !column.group_indices.is_empty()
                            {
                                (rows, DictionaryUse::JoinedAggregateWithCompanion)
                            } else if column.preserved_input_rows.is_some() {
                                // Semi joins cannot fan out their preserved side,
                                // so weaker storage evidence is less risky here
                                // than for an ordinary join payload.
                                (rows, DictionaryUse::SemiJoinedAggregate)
                            } else {
                                (rows, DictionaryUse::JoinedAggregate)
                            };
                        (Some(aggregate_rows), dictionary_use)
                    }
                    Precision::Exact(rows)
                        if !has_source_filter || column.has_filter_exec =>
                    {
                        (Some(rows), DictionaryUse::Aggregate)
                    }
                    Precision::Exact(_) | Precision::Inexact(_) | Precision::Absent => {
                        return None;
                    }
                };
                let eligible = config.limit.is_none()
                    && config.file_source.exact_filter().is_none()
                    && source_filter_supported
                    && derived_source_filter_supported
                    && filtered_distinct_source_filter_supported;
                if !eligible {
                    return None;
                }
                if Self::is_profitable(
                    config,
                    column.table_index,
                    input_rows,
                    dictionary_use,
                ) {
                    return Some(column);
                }
                if dictionary_use == DictionaryUse::AmplifiedJoinPayload
                    && Self::is_profitable(
                        config,
                        column.table_index,
                        input_rows,
                        DictionaryUse::JoinPayload,
                    )
                {
                    // Restoring one wide join payload before aggregation still
                    // needs enough row reduction to repay its conversion.
                    if raw_agg.group_expr().expr().len() == 1
                        && Self::has_wide_file_values(config, table_index)
                        && !raw_aggregate_statistics.as_ref().is_some_and(|output| {
                            Self::has_minimum_aggregate_reduction(
                                &statistics.num_rows,
                                &output.num_rows,
                            )
                        })
                    {
                        return None;
                    }
                    column.restore_before_aggregate = true;
                    return Some(column);
                }
                None
            })
            .collect();
        // Join-payload-only candidates restore strings before a pure DISTINCT
        // aggregate. Leave this empirically losing shape unchanged until its
        // pre-aggregate savings are modeled directly.
        if raw_agg.aggr_expr().is_empty() {
            group_columns.retain(|column| !column.restore_before_aggregate);
        }
        if group_columns.is_empty() {
            return Ok(None);
        }

        if partial_agg.is_some() {
            let aggregates_are_shared = final_agg.aggr_expr().len()
                == raw_agg.aggr_expr().len()
                && final_agg
                    .aggr_expr()
                    .iter()
                    .zip(raw_agg.aggr_expr())
                    .all(|(final_expr, raw_expr)| Arc::ptr_eq(final_expr, raw_expr));
            if final_agg.group_expr() != &raw_agg.group_expr().as_final()
                || !aggregates_are_shared
            {
                return Ok(None);
            }
        }

        // Replanning multiple related expressions is atomic: if any rebuilt
        // expression rejects the dictionary input, keep the original valid
        // plan instead of turning an optional optimization into a query error.
        let rewrite = (|| -> Result<Option<Arc<dyn ExecutionPlan>>> {
            let scan_rewrites = Self::scan_rewrites(&group_columns);
            let Some(mut new_raw_input) =
                Self::rewrite_scans(raw_agg.input(), &scan_rewrites, &mut vec![])?
            else {
                return Ok(None);
            };
            let restore_input_indices: HashSet<_> = group_columns
                .iter()
                .filter(|column| column.restore_before_aggregate)
                .map(|column| column.aggregate_input_index)
                .collect();
            if !restore_input_indices.is_empty() {
                let original_schema = raw_agg.input().schema();
                let rewritten_schema = new_raw_input.schema();
                let fields = rewritten_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        if restore_input_indices.contains(&index) {
                            Arc::clone(&original_schema.fields()[index])
                        } else {
                            Arc::clone(field)
                        }
                    })
                    .collect::<Vec<_>>();
                let restored_schema = Arc::new(Schema::new_with_metadata(
                    fields,
                    rewritten_schema.metadata().clone(),
                ));
                let Some(restored_input) =
                    Self::restore_output_schema(&restored_schema, new_raw_input)?
                else {
                    return Ok(None);
                };
                new_raw_input = restored_input;
            }
            let raw_input_schema = new_raw_input.schema();
            let aggregate_group_columns = group_columns
                .iter()
                .filter(|column| !column.restore_before_aggregate)
                .cloned()
                .collect::<Vec<_>>();
            let Some(new_raw_group) = Self::retype_group_by(
                raw_agg.group_expr(),
                raw_input_schema.as_ref(),
                &aggregate_group_columns,
            )?
            else {
                return Ok(None);
            };
            let changed_input_indices: HashSet<_> = aggregate_group_columns
                .iter()
                .map(|column| column.aggregate_input_index)
                .collect();
            let mut new_aggregate_expr = Vec::with_capacity(raw_agg.aggr_expr().len());
            for old in raw_agg.aggr_expr() {
                // AggregateFunctionExpr caches the complete input schema for UDAF
                // planning and accumulator construction. Rebuild every aggregate,
                // including those whose arguments do not reference the rewritten
                // column, so none retain the pre-rewrite schema.
                let Some(new) = Self::replan_aggregate_expr(
                    old,
                    Arc::clone(&raw_input_schema),
                    &changed_input_indices,
                )?
                else {
                    return Ok(None);
                };
                if !Self::aggregate_fields_compatible(old, &new)?.0 {
                    return Ok(None);
                }
                new_aggregate_expr.push(Arc::new(new));
            }

            let new_raw_agg = Arc::new(AggregateExec::try_new(
                *raw_agg.mode(),
                new_raw_group.clone(),
                new_aggregate_expr.clone(),
                raw_agg.filter_expr().to_vec(),
                new_raw_input,
                Arc::clone(&raw_input_schema),
            )?);

            let new_final_agg: Arc<dyn ExecutionPlan> = if partial_agg.is_some() {
                // AggregateExec may adjust aggregate expressions while it is
                // initialized. Carry those exact expressions into the final stage,
                // as the physical planner does for an ordinary aggregate chain.
                let final_aggregate_expr = new_raw_agg.aggr_expr().to_vec();
                Arc::new(AggregateExec::try_new(
                    *final_agg.mode(),
                    new_raw_group.as_final(),
                    final_aggregate_expr,
                    final_agg.filter_expr().to_vec(),
                    new_raw_agg,
                    raw_input_schema,
                )?)
            } else {
                new_raw_agg
            };

            Self::restore_output_schema(&final_agg.schema(), new_final_agg)
        })();
        match rewrite {
            Ok(plan) => Ok(plan),
            Err(_) => Ok(None),
        }
    }

    fn contains_empty_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<EmptyExec>().is_some()
            || plan.children().into_iter().any(Self::contains_empty_exec)
    }

    fn has_minimum_aggregate_reduction(
        input_rows: &Precision<usize>,
        output_rows: &Precision<usize>,
    ) -> bool {
        input_rows
            .get_value()
            .copied()
            .zip(output_rows.get_value().copied())
            .is_some_and(|(input_rows, output_rows)| {
                input_rows > 0
                    && output_rows > 0
                    && output_rows
                        .checked_mul(MIN_JOIN_PAYLOAD_AGGREGATE_REDUCTION)
                        .is_some_and(|reduced_rows| reduced_rows <= input_rows)
            })
    }

    fn has_wide_file_values(config: &FileScanConfig, table_index: usize) -> bool {
        let mut saw_file = false;
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            let Some(physical) = file.extension::<PhysicalFileStatistics>() else {
                return false;
            };
            let Some(column) = physical.column_statistics.get(table_index) else {
                return false;
            };
            if column
                .min_row_group_value_bytes_per_row
                .is_none_or(|bytes| bytes < UTF8_VIEW_SLOT_BYTES)
            {
                return false;
            }
            saw_file = true;
        }
        saw_file
    }

    fn downstream_join_aggregates(
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Vec<DownstreamJoinAggregate> {
        let mut aggregates = vec![];
        Self::collect_downstream_join_aggregates(plan, &mut aggregates);
        aggregates
    }

    fn collect_downstream_join_aggregates(
        plan: &Arc<dyn ExecutionPlan>,
        aggregates: &mut Vec<DownstreamJoinAggregate>,
    ) {
        if let Some(join) = plan
            .downcast_ref::<HashJoinExec>()
            .filter(|join| join.partition_mode() == &PartitionMode::Partitioned)
        {
            let left = join
                .on()
                .iter()
                .map(|(left, _)| Arc::clone(left))
                .collect::<Vec<_>>();
            let right = join
                .on()
                .iter()
                .map(|(_, right)| Arc::clone(right))
                .collect::<Vec<_>>();
            Self::record_downstream_join_aggregate(join.left(), &left, aggregates);
            Self::record_downstream_join_aggregate(join.right(), &right, aggregates);
        }
        for child in plan.children() {
            Self::collect_downstream_join_aggregates(child, aggregates);
        }
    }

    fn record_downstream_join_aggregate(
        input: &Arc<dyn ExecutionPlan>,
        join_exprs: &[Arc<dyn PhysicalExpr>],
        aggregates: &mut Vec<DownstreamJoinAggregate>,
    ) {
        let mut aggregate = None;
        let mut group_indices = Vec::with_capacity(join_exprs.len());
        for join_expr in join_exprs {
            let Some(column) = join_expr.downcast_ref::<Column>() else {
                return;
            };
            let Some((current_aggregate, group_index)) =
                Self::trace_aggregate_group_output(input, column.index())
            else {
                return;
            };
            if aggregate
                .as_ref()
                .is_some_and(|aggregate| !Arc::ptr_eq(aggregate, &current_aggregate))
            {
                return;
            }
            aggregate = Some(current_aggregate);
            group_indices.push(group_index);
        }
        let Some(aggregate) = aggregate else {
            return;
        };
        let aggregate_exec = aggregate
            .downcast_ref::<AggregateExec>()
            .expect("join-key lineage should end at an aggregate");
        if group_indices.len() != aggregate_exec.group_expr().expr().len()
            || !group_indices.iter().copied().eq(0..group_indices.len())
        {
            return;
        }
        if !aggregates
            .iter()
            .any(|existing| Arc::ptr_eq(&existing.aggregate, &aggregate))
        {
            aggregates.push(DownstreamJoinAggregate { aggregate });
        }
    }

    fn trace_aggregate_group_output(
        plan: &Arc<dyn ExecutionPlan>,
        output_index: usize,
    ) -> Option<(Arc<dyn ExecutionPlan>, usize)> {
        if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            return matches!(
                aggregate.mode(),
                AggregateMode::SinglePartitioned | AggregateMode::FinalPartitioned
            )
            .then(|| aggregate.group_expr().expr().get(output_index))
            .flatten()
            .map(|_| (Arc::clone(plan), output_index));
        }
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            let projected = projection.expr().get(output_index)?;
            let column = projected.expr.downcast_ref::<Column>()?;
            return Self::trace_aggregate_group_output(
                projection.input(),
                column.index(),
            );
        }
        if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            let input_index = match filter.projection() {
                Some(projection) => *projection.get(output_index)?,
                None => output_index,
            };
            return Self::trace_aggregate_group_output(filter.input(), input_index);
        }
        None
    }

    fn group_columns(
        aggregate: &AggregateExec,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Vec<GroupColumn>> {
        let input_schema = aggregate.input().schema();
        let mut columns = vec![];
        let mut group_indices_by_input: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut aggregate_indices_by_input: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut unsupported_aggregate_inputs = HashSet::new();
        for (group_index, (expr, _)) in aggregate.group_expr().expr().iter().enumerate() {
            for column in collect_columns(expr) {
                group_indices_by_input
                    .entry(column.index())
                    .or_default()
                    .push(group_index);
            }
        }
        for (aggregate_index, aggregate_expr) in aggregate.aggr_expr().iter().enumerate()
        {
            for expr in aggregate_expr.expressions() {
                for column in collect_columns(&expr) {
                    aggregate_indices_by_input
                        .entry(column.index())
                        .or_default()
                        .push(aggregate_index);
                }
            }
            for order_by in aggregate_expr.order_bys() {
                unsupported_aggregate_inputs.extend(
                    collect_columns(&order_by.expr)
                        .into_iter()
                        .map(|column| column.index()),
                );
            }
            if let Some(filter) = &aggregate.filter_expr()[aggregate_index] {
                unsupported_aggregate_inputs.extend(
                    collect_columns(filter)
                        .into_iter()
                        .map(|column| column.index()),
                );
            }
        }

        let candidate_inputs: HashSet<_> = group_indices_by_input
            .keys()
            .chain(aggregate_indices_by_input.keys())
            .copied()
            .collect();
        for input_index in candidate_inputs {
            if unsupported_aggregate_inputs.contains(&input_index) {
                continue;
            }
            if input_schema
                .fields()
                .get(input_index)
                .is_none_or(|field| field.data_type() != &DataType::Utf8View)
            {
                continue;
            }
            let mut group_indices = group_indices_by_input
                .remove(&input_index)
                .unwrap_or_default();
            group_indices.sort_unstable();
            group_indices.dedup();
            let mut aggregate_indices = aggregate_indices_by_input
                .remove(&input_index)
                .unwrap_or_default();
            aggregate_indices.sort_unstable();
            aggregate_indices.dedup();
            let hypothetical_schema =
                Self::dictionary_input_schema(input_schema.as_ref(), input_index)?;
            let expressions_preserve_dictionary = group_indices.iter().all(|&index| {
                let expr = &aggregate.group_expr().expr()[index].0;
                Self::replan_dictionary_expr(
                    expr,
                    hypothetical_schema.as_ref(),
                    &[input_index].into(),
                )
                .ok()
                .flatten()
                .and_then(|expr| expr.data_type(hypothetical_schema.as_ref()).ok())
                .is_some_and(|data_type| {
                    matches!(
                        data_type,
                        DataType::Dictionary(key, value)
                            if key.as_ref() == &DataType::Int32
                                && value.as_ref() == &DataType::Utf8
                    )
                })
            });
            let supported_derived_shape = Self::is_supported_derived_scan_aggregate(
                aggregate,
                input_index,
                &group_indices,
                &aggregate_indices,
            );
            let supported_filtered_distinct_rollup =
                Self::is_supported_filtered_distinct_rollup(
                    aggregate,
                    input_index,
                    &group_indices,
                    &aggregate_indices,
                );
            let aggregate_uses_supported = supported_derived_shape
                || aggregate_indices.iter().all(|&index| {
                    aggregate.aggr_expr()[index]
                        .expressions()
                        .iter()
                        .all(|expr| {
                            Self::is_supported_case_aggregate_argument(expr, input_index)
                        })
                });
            if !aggregate_uses_supported {
                continue;
            }
            if !expressions_preserve_dictionary && !supported_derived_shape {
                continue;
            }
            let mut aggregate_types_changed = false;
            let aggregates_replan = aggregate_indices.iter().all(|&index| {
                let old = &aggregate.aggr_expr()[index];
                let Ok(Some(new)) = Self::replan_aggregate_expr(
                    old,
                    Arc::clone(&hypothetical_schema),
                    &[input_index].into(),
                ) else {
                    return false;
                };
                let Ok((compatible, changed)) =
                    Self::aggregate_fields_compatible(old, &new)
                else {
                    return false;
                };
                aggregate_types_changed |= changed;
                compatible
            });
            if !aggregates_replan || (group_indices.is_empty() && aggregate_types_changed)
            {
                continue;
            }
            let has_transformed_group = group_indices.iter().any(|&index| {
                aggregate.group_expr().expr()[index]
                    .0
                    .downcast_ref::<Column>()
                    .is_none()
            });
            let target = Self::trace_group_column(
                aggregate.input(),
                input_index,
                supported_derived_shape || supported_filtered_distinct_rollup,
                supported_filtered_distinct_rollup,
                false,
                context,
            )?;
            if let Some(target) = target {
                let derived_scan_aggregate = target.simple_filter_path
                    && target.derived_candidate_filter
                    && supported_derived_shape;
                let filtered_distinct_rollup = supported_filtered_distinct_rollup
                    && target.empty_string_candidate_filter
                    && target.candidate_filter
                    && !target.has_join;
                if target.candidate_filter
                    && !derived_scan_aggregate
                    && !filtered_distinct_rollup
                {
                    continue;
                }
                // A derived group expression is useful even when its output is
                // not dictionary typed, but only for the fully matched
                // scan/filter/aggregate shape above.
                if !expressions_preserve_dictionary && !derived_scan_aggregate {
                    continue;
                }
                let amplified_join_payload_rows = (target.amplified_join_anchor
                    && target.amplified_join_path_valid
                    && !target.candidate_filter
                    && aggregate_indices.is_empty()
                    && group_indices.len() == 1)
                    .then_some(target.amplified_join_payload_rows)
                    .flatten();
                // A multi-join plan that starts like Q21 must satisfy the full
                // audited shape. Do not let a failed anchor or downstream proof
                // fall through to one of the broader joined-aggregate gates.
                if target.amplified_join_candidate
                    && target.amplified_join_has_downstream_join
                    && amplified_join_payload_rows.is_none()
                {
                    continue;
                }
                columns.push(GroupColumn {
                    group_indices,
                    aggregate_input_index: input_index,
                    scan: target.scan,
                    table_index: target.table_index,
                    scan_path: target.scan_path,
                    has_filter_exec: target.has_filter_exec,
                    has_join: target.has_join,
                    allows_candidate_filter: supported_derived_shape
                        || filtered_distinct_rollup,
                    has_transformed_group,
                    derived_scan_aggregate,
                    filtered_distinct_rollup,
                    preserved_input_rows: target.preserved_input_rows,
                    amplified_join_payload_rows,
                    restore_before_aggregate: false,
                });
            }
        }
        Ok(columns)
    }

    fn dictionary_input_schema(schema: &Schema, input_index: usize) -> Result<SchemaRef> {
        let fields: Vec<FieldRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index == input_index {
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Utf8),
                    ))) as FieldRef
                } else {
                    Arc::clone(field)
                }
            })
            .collect();
        Ok(Arc::new(Schema::new_with_metadata(
            fields,
            schema.metadata().clone(),
        )))
    }

    /// Return whether `expr` either does not use `candidate_index`, or uses it
    /// only in searched `CASE WHEN` predicates whose result type is independent
    /// of the candidate. This keeps the aggregate's argument type unchanged
    /// while allowing dictionary-aware equality evaluation in the predicate.
    fn is_supported_case_aggregate_argument(
        expr: &Arc<dyn PhysicalExpr>,
        candidate_index: usize,
    ) -> bool {
        let references_candidate = |expr: &Arc<dyn PhysicalExpr>| {
            collect_columns(expr)
                .iter()
                .any(|column| column.index() == candidate_index)
        };
        if !references_candidate(expr) {
            return true;
        }

        let Some(case) = expr.downcast_ref::<CaseExpr>() else {
            return false;
        };
        if case.expr().is_some() || case.else_expr().is_some_and(references_candidate) {
            return false;
        }
        case.when_then_expr().iter().all(|(when, then)| {
            Self::candidate_has_supported_group_filter_use(when, candidate_index)
                && !references_candidate(then)
        })
    }

    fn is_supported_derived_scan_aggregate(
        aggregate: &AggregateExec,
        candidate_index: usize,
        group_indices: &[usize],
        aggregate_indices: &[usize],
    ) -> bool {
        if group_indices.len() != 1 || aggregate_indices.is_empty() {
            return false;
        }
        let group = &aggregate.group_expr().expr()[group_indices[0]].0;
        let Some(function) =
            ScalarFunctionExpr::try_downcast_func::<RegexpReplaceFunc>(group.as_ref())
        else {
            return false;
        };
        if !(3..=4).contains(&function.args().len())
            || !Self::is_candidate_column(&function.args()[0], candidate_index)
            || !function.args()[1..]
                .iter()
                .all(|arg| arg.downcast_ref::<Literal>().is_some())
        {
            return false;
        }

        aggregate_indices.iter().all(|&index| {
            let aggregate = &aggregate.aggr_expr()[index];
            if aggregate.is_distinct()
                || aggregate.ignore_nulls()
                || aggregate.is_reversed()
                || !aggregate.order_bys().is_empty()
            {
                return false;
            }
            let args = aggregate.expressions();
            if aggregate.fun().inner().is::<Min>() {
                args.len() == 1
                    && Self::is_candidate_column(&args[0], candidate_index)
            } else if aggregate.fun().inner().is::<Avg>() {
                    let [arg] = args.as_slice() else {
                        return false;
                    };
                    let Some(cast) = arg.downcast_ref::<CastExpr>() else {
                        return false;
                    };
                    let Some(length) = ScalarFunctionExpr::try_downcast_func::<
                        OctetLengthFunc,
                    >(cast.expr().as_ref())
                    else {
                        return false;
                    };
                    cast.cast_type() == &DataType::Float64
                        && length.args().len() == 1
                        && Self::is_candidate_column(&length.args()[0], candidate_index)
            } else {
                false
            }
        })
    }

    fn is_candidate_column(expr: &Arc<dyn PhysicalExpr>, candidate_index: usize) -> bool {
        expr.downcast_ref::<Column>()
            .is_some_and(|column| column.index() == candidate_index)
    }

    fn replan_dictionary_expr(
        expr: &Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
        changed_input_indices: &HashSet<usize>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let references_candidate = collect_columns(expr)
            .iter()
            .any(|column| changed_input_indices.contains(&column.index()));
        if !references_candidate {
            return Ok(Some(Arc::clone(expr)));
        }
        if let Some(column) = expr.downcast_ref::<Column>() {
            return Ok(changed_input_indices
                .contains(&column.index())
                .then(|| Arc::clone(expr)));
        }
        if expr.downcast_ref::<CastExpr>().is_some() {
            let Some(child) = expr.children().into_iter().next() else {
                return Ok(None);
            };
            let Some(child) =
                Self::replan_dictionary_expr(child, input_schema, changed_input_indices)?
            else {
                return Ok(None);
            };
            return Arc::clone(expr).with_new_children(vec![child]).map(Some);
        }
        if expr.downcast_ref::<BinaryExpr>().is_some()
            || expr.downcast_ref::<InListExpr>().is_some()
        {
            return try_rewrite_dictionary_filter_predicate(
                expr,
                input_schema,
                &changed_input_indices.iter().copied().collect::<Vec<_>>(),
            );
        }
        if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
            let mut args = Vec::with_capacity(function.args().len());
            for arg in function.args() {
                let Some(arg) = Self::replan_dictionary_expr(
                    arg,
                    input_schema,
                    changed_input_indices,
                )?
                else {
                    return Ok(None);
                };
                args.push(arg);
            }
            let function = ScalarFunctionExpr::try_new(
                Arc::new(function.fun().clone()),
                args,
                input_schema,
                Arc::new(function.config_options().clone()),
            )?;
            return Ok(Some(Arc::new(function)));
        }
        if expr.downcast_ref::<CaseExpr>().is_some() {
            let mut children = Vec::with_capacity(expr.children().len());
            for child in expr.children() {
                let Some(child) = Self::replan_dictionary_expr(
                    child,
                    input_schema,
                    changed_input_indices,
                )?
                else {
                    return Ok(None);
                };
                children.push(child);
            }
            return Arc::clone(expr).with_new_children(children).map(Some);
        }
        Ok(None)
    }

    fn replan_aggregate_expr(
        aggregate: &AggregateFunctionExpr,
        input_schema: SchemaRef,
        changed_input_indices: &HashSet<usize>,
    ) -> Result<Option<AggregateFunctionExpr>> {
        let mut args = Vec::with_capacity(aggregate.expressions().len());
        for arg in aggregate.expressions() {
            let Some(arg) = Self::replan_dictionary_expr(
                &arg,
                input_schema.as_ref(),
                changed_input_indices,
            )?
            else {
                return Ok(None);
            };
            args.push(arg);
        }
        // A changed physical input type must go through the same UDAF coercion
        // as ordinary logical planning. For example, MIN unwraps dictionary
        // input to its value type so it can use the grouped byte accumulator.
        let current_fields = args
            .iter()
            .map(|arg| arg.return_field(input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;
        let coerced_fields = fields_with_udf(&current_fields, aggregate.fun())?;
        let args = args
            .into_iter()
            .zip(coerced_fields)
            .map(|(arg, field)| {
                cast(arg, input_schema.as_ref(), field.data_type().clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let order_by_exprs = aggregate
            .order_bys()
            .iter()
            .map(|order_by| Arc::clone(&order_by.expr))
            .collect();
        aggregate.with_new_expressions_and_schema(args, order_by_exprs, input_schema)
    }

    fn aggregate_fields_compatible(
        old: &AggregateFunctionExpr,
        new: &AggregateFunctionExpr,
    ) -> Result<(bool, bool)> {
        let old_fields = std::iter::once(old.field())
            .chain(old.state_fields()?)
            .collect::<Vec<_>>();
        let new_fields = std::iter::once(new.field())
            .chain(new.state_fields()?)
            .collect::<Vec<_>>();
        if old_fields.len() != new_fields.len() {
            return Ok((false, false));
        }
        let mut changed = false;
        for (old, new) in old_fields.iter().zip(new_fields) {
            if old.name() != new.name()
                || old.is_nullable() != new.is_nullable()
                || old.metadata() != new.metadata()
            {
                return Ok((false, false));
            }
            if old.data_type() == new.data_type() {
                continue;
            }
            changed = true;
            if !Self::is_supported_string_replacement(old.data_type(), new.data_type()) {
                return Ok((false, false));
            }
        }
        Ok((true, changed))
    }

    fn is_supported_string_replacement(old: &DataType, new: &DataType) -> bool {
        old == &DataType::Utf8View
            && (new == &DataType::Utf8
                || matches!(
                    new,
                    DataType::Dictionary(key, value)
                        if key.as_ref() == &DataType::Int32
                            && value.as_ref() == &DataType::Utf8
                ))
    }

    fn retype_group_by(
        group_by: &PhysicalGroupBy,
        input_schema: &Schema,
        group_columns: &[GroupColumn],
    ) -> Result<Option<PhysicalGroupBy>> {
        let encoded_groups: HashSet<_> = group_columns
            .iter()
            .flat_map(|column| column.group_indices.iter().copied())
            .collect();
        let changed_input_indices: HashSet<_> = group_columns
            .iter()
            .map(|column| column.aggregate_input_index)
            .collect();
        let mut group_expr = Vec::with_capacity(group_by.expr().len());
        for (index, (expr, alias)) in group_by.expr().iter().enumerate() {
            let expr = if encoded_groups.contains(&index) {
                let Some(expr) = Self::replan_dictionary_expr(
                    expr,
                    input_schema,
                    &changed_input_indices,
                )?
                else {
                    return Ok(None);
                };
                expr
            } else {
                Arc::clone(expr)
            };
            group_expr.push((expr, alias.clone()));
        }
        if group_by.has_grouping_set()
            && group_by.null_expr().len() != group_by.expr().len()
        {
            return Ok(None);
        }

        let mut null_expr = Vec::with_capacity(group_by.null_expr().len());
        for (index, (expr, alias)) in group_by.null_expr().iter().enumerate() {
            if !encoded_groups.contains(&index) {
                null_expr.push((Arc::clone(expr), alias.clone()));
                continue;
            }
            let Some(literal) = expr.downcast_ref::<Literal>() else {
                return Ok(None);
            };
            if !literal.value().is_null() {
                return Ok(None);
            }
            let data_type = group_expr[index].0.data_type(input_schema)?;
            null_expr.push((
                Arc::new(Literal::new(ScalarValue::try_new_null(&data_type)?))
                    as Arc<dyn PhysicalExpr>,
                alias.clone(),
            ));
        }
        Ok(Some(PhysicalGroupBy::new(
            group_expr,
            null_expr,
            group_by.groups().to_vec(),
            group_by.has_grouping_set(),
        )))
    }

    /// Prove output columns remain unique through schema-only operators and a
    /// unique-build enrichment join that cannot duplicate right-side rows.
    fn has_unique_columns(
        plan: &Arc<dyn ExecutionPlan>,
        column_indices: &[usize],
        null_equality: &NullEquality,
    ) -> bool {
        if column_indices.is_empty() {
            return false;
        }

        let mut column_indices = column_indices.to_vec();
        column_indices.sort_unstable();
        let column_count = column_indices.len();
        column_indices.dedup();
        if column_indices.len() != column_count {
            return false;
        }
        let nullable_unique_keys_match = null_equality == &NullEquality::NullEqualsNull
            && column_indices.iter().any(|&index| {
                plan.schema()
                    .fields()
                    .get(index)
                    .is_some_and(|field| field.is_nullable())
            });
        if plan
            .properties()
            .equivalence_properties()
            .constraints()
            .iter()
            .any(|constraint| {
                let (mut constraint_indices, unique) = match constraint {
                    Constraint::PrimaryKey(indices) => (indices.clone(), false),
                    Constraint::Unique(indices) => (indices.clone(), true),
                };
                constraint_indices.sort_unstable();
                constraint_indices == column_indices
                    && !(unique && nullable_unique_keys_match)
            })
        {
            return true;
        }

        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            let mut input_indices = Vec::with_capacity(column_indices.len());
            for &output_index in &column_indices {
                let Some(expr) = projection.expr().get(output_index) else {
                    return false;
                };
                let Some(column) = expr.expr.downcast_ref::<Column>() else {
                    return false;
                };
                input_indices.push(column.index());
            }
            return Self::has_unique_columns(
                projection.input(),
                &input_indices,
                null_equality,
            );
        }

        let schema_only_exchange = plan
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
            || plan
                .downcast_ref::<RepartitionExec>()
                .is_some_and(|repartition| {
                    matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
                });
        if schema_only_exchange {
            let children = plan.children();
            let Some(input) = children.first() else {
                return false;
            };
            return Self::has_unique_columns(input, &column_indices, null_equality);
        }

        let Some(join) = plan.downcast_ref::<HashJoinExec>() else {
            return false;
        };
        if join.join_type() != &JoinType::Inner
            || join.partition_mode() != &PartitionMode::CollectLeft
            || !Self::has_unique_left_join_key(join)
        {
            return false;
        }

        let (_, output_columns) = build_join_schema(
            join.left().schema().as_ref(),
            join.right().schema().as_ref(),
            join.join_type(),
        );
        let mut right_indices = Vec::with_capacity(column_indices.len());
        for output_index in column_indices {
            let unprojected_index = match &join.projection {
                Some(projection) => {
                    let Some(&index) = projection.get(output_index) else {
                        return false;
                    };
                    index
                }
                None => output_index,
            };
            let Some(column) = output_columns.get(unprojected_index) else {
                return false;
            };
            if column.side != JoinSide::Right {
                return false;
            }
            right_indices.push(column.index);
        }
        Self::has_unique_columns(join.right(), &right_indices, null_equality)
    }

    fn has_unique_left_join_key(join: &HashJoinExec) -> bool {
        if join.partition_mode() != &PartitionMode::CollectLeft || join.on().is_empty() {
            return false;
        }
        let mut join_key_indices = Vec::with_capacity(join.on().len());
        for (left, _) in join.on() {
            let Some(column) = left.downcast_ref::<Column>() else {
                return false;
            };
            join_key_indices.push(column.index());
        }
        Self::has_unique_columns(join.left(), &join_key_indices, &join.null_equality())
    }

    fn trace_group_column(
        plan: &Arc<dyn ExecutionPlan>,
        output_index: usize,
        allow_candidate_filter: bool,
        allow_filtered_distinct_rollup: bool,
        scan_filter_only: bool,
        context: &dyn PhysicalOptimizerContext,
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
                has_join: false,
                candidate_filter: false,
                derived_candidate_filter: false,
                empty_string_candidate_filter: false,
                preserved_input_rows: None,
                amplified_join_anchor: false,
                amplified_join_path_valid: true,
                amplified_join_candidate: false,
                amplified_join_has_downstream_join: false,
                amplified_join_payload_rows: None,
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
            let target = Self::trace_group_column(
                projection.input(),
                input_index,
                allow_candidate_filter,
                allow_filtered_distinct_rollup,
                scan_filter_only,
                context,
            )?;
            return Ok(target.map(|target| target.prepend_child(0)));
        }

        let schema_only_exchange = plan
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
            || plan
                .downcast_ref::<RepartitionExec>()
                .is_some_and(|repartition| {
                    matches!(repartition.partitioning(), Partitioning::RoundRobinBatch(_))
                });
        if schema_only_exchange {
            let children = plan.children();
            let Some(input) = children.first() else {
                return Ok(None);
            };
            let target = Self::trace_group_column(
                input,
                output_index,
                allow_candidate_filter,
                allow_filtered_distinct_rollup,
                scan_filter_only,
                context,
            )?;
            return Ok(target.map(|mut target| {
                target.simple_filter_path = false;
                target.prepend_child(0)
            }));
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
            let predicate_references_candidate = collect_columns(filter.predicate())
                .iter()
                .any(|column| column.index() == input_index);
            if scan_filter_only {
                // The filtered DISTINCT rollup admits exactly one FilterExec:
                // the direct candidate `!= ''` predicate. The same predicate
                // may also be stored on the Parquet source without counting as
                // another execution operator.
                if !Self::is_direct_not_equal_empty_string(
                    filter.predicate(),
                    input_index,
                ) {
                    return Ok(None);
                }
            } else if predicate_references_candidate
                && (!allow_candidate_filter
                    || !Self::candidate_has_supported_group_filter_use(
                        filter.predicate(),
                        input_index,
                    ))
            {
                return Ok(None);
            }
            let target = Self::trace_group_column(
                filter.input(),
                input_index,
                allow_candidate_filter,
                allow_filtered_distinct_rollup,
                scan_filter_only,
                context,
            )?;
            let Some(mut target) = target else {
                return Ok(None);
            };
            if scan_filter_only && target.candidate_filter {
                return Ok(None);
            }
            target.has_filter_exec = true;
            if predicate_references_candidate {
                target.derived_candidate_filter = !target.candidate_filter
                    && Self::is_direct_not_equal_literal(filter.predicate(), input_index);
                target.empty_string_candidate_filter = !target.candidate_filter
                    && Self::is_direct_not_equal_empty_string(
                        filter.predicate(),
                        input_index,
                    );
                target.candidate_filter = true;
            }
            return Ok(Some(target.prepend_child(0)));
        }

        if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            if !allow_filtered_distinct_rollup {
                return Ok(None);
            }
            let Some(input_index) =
                Self::filtered_distinct_inner_input_index(aggregate, output_index)
            else {
                return Ok(None);
            };
            let partial = aggregate
                .input()
                .downcast_ref::<AggregateExec>()
                .expect("validated filtered DISTINCT chain has a partial stage");
            let target = Self::trace_group_column(
                partial.input(),
                input_index,
                true,
                false,
                true,
                context,
            )?;
            return Ok(target.map(|mut target| {
                target.simple_filter_path = false;
                target.prepend_child(0).prepend_child(0)
            }));
        }

        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            if scan_filter_only || join.fetch().is_some() {
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
            let carries_unmodified_payload = matches!(
                (join.join_type(), column_index.side),
                (JoinType::Inner, JoinSide::Left | JoinSide::Right)
                    | (
                        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti,
                        JoinSide::Left
                    )
                    | (
                        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti,
                        JoinSide::Right
                    )
            );
            if !carries_unmodified_payload
                || join.filter().is_some_and(|filter| {
                    filter.column_indices().iter().any(|filter_column| {
                        filter_column.side == column_index.side
                            && filter_column.index == column_index.index
                    })
                })
            {
                return Ok(None);
            }
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
            let target = Self::trace_group_column(
                input,
                column_index.index,
                allow_candidate_filter,
                allow_filtered_distinct_rollup,
                scan_filter_only,
                context,
            )?;
            let preserved_input_rows =
                if matches!(join.join_type(), JoinType::LeftSemi | JoinType::RightSemi) {
                    match context
                        .compute_statistics(input, &StatisticsArgs::new())?
                        .num_rows
                    {
                        Precision::Exact(rows) | Precision::Inexact(rows) => Some(rows),
                        Precision::Absent => None,
                    }
                } else {
                    None
                };
            let is_downstream_amplified_join = matches!(
                (join.join_type(), column_index.side),
                (JoinType::LeftSemi | JoinType::LeftAnti, JoinSide::Left)
                    | (JoinType::RightSemi | JoinType::RightAnti, JoinSide::Right)
            );
            let amplified_join_payload_rows = if is_downstream_amplified_join {
                match context
                    .compute_statistics(input, &StatisticsArgs::new())?
                    .num_rows
                {
                    Precision::Exact(rows) | Precision::Inexact(rows) => Some(rows),
                    Precision::Absent => None,
                }
            } else {
                None
            };
            let is_amplified_join_anchor = join.join_type() == &JoinType::Inner
                && column_index.side == JoinSide::Left
                && join.filter().is_none()
                && Self::has_unique_left_join_key(join);
            let is_amplified_join_candidate = join.join_type() == &JoinType::Inner
                && column_index.side == JoinSide::Left;
            let is_unique_build_enrichment = join.join_type() == &JoinType::Inner
                && column_index.side == JoinSide::Right
                && join.filter().is_none()
                && Self::has_unique_left_join_key(join);
            return Ok(target.map(|mut target| {
                let had_join = target.has_join;
                target.simple_filter_path = false;
                target.has_join = true;
                if had_join
                    && target.amplified_join_candidate
                    && matches!(
                        join.join_type(),
                        JoinType::LeftSemi
                            | JoinType::LeftAnti
                            | JoinType::RightSemi
                            | JoinType::RightAnti
                    )
                {
                    target.amplified_join_has_downstream_join = true;
                } else if !had_join {
                    target.amplified_join_candidate = is_amplified_join_candidate;
                }
                target.preserved_input_rows =
                    target.preserved_input_rows.max(preserved_input_rows);
                if target.amplified_join_path_valid {
                    if target.amplified_join_anchor {
                        if is_downstream_amplified_join {
                            target.amplified_join_payload_rows = target
                                .amplified_join_payload_rows
                                .max(amplified_join_payload_rows);
                        } else if !is_unique_build_enrichment && !is_amplified_join_anchor
                        {
                            target.amplified_join_path_valid = false;
                        }
                    } else if is_amplified_join_anchor {
                        target.amplified_join_anchor = true;
                    } else {
                        target.amplified_join_path_valid = false;
                    }
                }
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
        } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            // The only aggregate path admitted by `trace_group_column` is the
            // exact two-column, aggregate-free deduplication chain above. Keep
            // its grouping column dictionary encoded through both deduplication
            // stages so the outer aggregate can reuse the same representation.
            if aggregate.group_expr().expr().len() != 2
                || aggregate.group_expr().has_grouping_set()
                || !aggregate.group_expr().null_expr().is_empty()
                || !aggregate.aggr_expr().is_empty()
                || !aggregate.filter_expr().is_empty()
                || aggregate.limit_options().is_some()
                || !matches!(
                    aggregate.mode(),
                    AggregateMode::Partial
                        | AggregateMode::Final
                        | AggregateMode::FinalPartitioned
                )
            {
                return Ok(None);
            }
            let new_input = new_children.swap_remove(0);
            let input_schema = match aggregate.mode().input_mode() {
                AggregateInputMode::Raw => new_input.schema(),
                AggregateInputMode::Partial => {
                    let Some(state_producer) = new_input.downcast_ref::<AggregateExec>()
                    else {
                        return Ok(None);
                    };
                    if state_producer.mode().output_mode() != AggregateOutputMode::Partial
                    {
                        return Ok(None);
                    }
                    state_producer.input_schema()
                }
            };
            Arc::new(AggregateExec::try_new(
                *aggregate.mode(),
                aggregate.group_expr().clone(),
                vec![],
                vec![],
                new_input,
                input_schema,
            )?) as Arc<dyn ExecutionPlan>
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

    fn has_strong_storage_evidence(config: &FileScanConfig, table_index: usize) -> bool {
        let mut saw_file = false;
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            let Some(physical) = file.extension::<PhysicalFileStatistics>() else {
                return false;
            };
            let Some(column) = physical.column_statistics.get(table_index) else {
                return false;
            };
            saw_file = true;
            if column.native_dictionary_value_type != Some(DataType::Utf8)
                || !column.all_dictionary_encoded
                || column
                    .min_row_group_value_bytes_per_encoded_byte
                    .is_none_or(|ratio| ratio < MIN_VALUE_BYTES_PER_PARQUET_BYTE)
            {
                return false;
            }
        }
        saw_file
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
            DictionaryUse::Aggregate
            | DictionaryUse::JoinedAggregate
            | DictionaryUse::SemiJoinedAggregate
            | DictionaryUse::JoinedAggregateWithCompanion
            | DictionaryUse::TransformedJoinedAggregate
            | DictionaryUse::AmplifiedJoinPayload
            | DictionaryUse::JoinPayload
            | DictionaryUse::DerivedAggregate => {
                let Some(input_rows) = input_rows else {
                    return false;
                };
                if dictionary_use == DictionaryUse::DerivedAggregate
                    && input_rows < MIN_DERIVED_AGGREGATE_ROWS
                {
                    return false;
                }
                input_rows
            }
            DictionaryUse::FilteredDistinctRollup => {
                if input_rows.is_some() {
                    return false;
                }
                let Some(input_rows) = footer_rows() else {
                    return false;
                };
                if input_rows < MIN_FILTERED_DISTINCT_ROLLUP_ROWS {
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
        if matches!(
            dictionary_use,
            DictionaryUse::JoinedAggregate
                | DictionaryUse::SemiJoinedAggregate
                | DictionaryUse::JoinedAggregateWithCompanion
                | DictionaryUse::TransformedJoinedAggregate
        ) && footer_rows().is_none_or(|source_rows| input_rows < source_rows)
        {
            return false;
        }
        let tiny_amplified_dimension = dictionary_use
            == DictionaryUse::AmplifiedJoinPayload
            && footer_rows().is_some_and(|rows| rows <= MAX_TINY_DIMENSION_ROWS);
        let small_join_payload_source = dictionary_use == DictionaryUse::JoinPayload
            && footer_rows().is_some_and(|rows| rows <= MAX_JOIN_PAYLOAD_SOURCE_ROWS);

        let mut saw_file = false;
        let mut min_value_bytes_per_row = usize::MAX;
        let mut storage_profitable = true;
        let mut weak_storage_profitable = true;
        let mut dictionary_page_storage_profitable = true;
        let mut all_wide_values = true;
        for physical in files_by_path.values() {
            saw_file = true;
            let Some(column) = physical.column_statistics.get(table_index) else {
                return false;
            };
            let native_type = column.native_dictionary_value_type.as_ref();
            if dictionary_use == DictionaryUse::FilteredDistinctRollup {
                if !matches!(native_type, Some(DataType::Utf8 | DataType::Binary))
                    || !column.all_dictionary_encoded
                {
                    return false;
                }
                continue;
            }
            if native_type != Some(&DataType::Utf8)
                && !(dictionary_use == DictionaryUse::DerivedAggregate
                    && native_type == Some(&DataType::Binary))
            {
                return false;
            }
            if dictionary_use == DictionaryUse::DerivedAggregate {
                if column
                    .min_dictionary_page_bytes
                    .is_none_or(|bytes| bytes < MIN_DERIVED_DICTIONARY_PAGE_BYTES)
                {
                    return false;
                }
                continue;
            }
            let Some(value_bytes_per_row) = column.min_row_group_value_bytes_per_row
            else {
                return false;
            };
            let file_storage_profitable = column.all_dictionary_encoded
                && (dictionary_use == DictionaryUse::AmplifiedJoinPayload
                    || column
                        .min_row_group_value_bytes_per_encoded_byte
                        .is_some_and(|ratio| {
                            ratio >= dictionary_use.min_value_bytes_per_parquet_byte()
                        }));
            let file_weak_storage_profitable = column.all_dictionary_encoded
                && column
                    .min_row_group_value_bytes_per_encoded_byte
                    .is_some_and(|ratio| ratio >= 2);
            let file_dictionary_page_storage_profitable =
                column.min_dictionary_page_bytes.is_some();
            if !matches!(
                dictionary_use,
                DictionaryUse::JoinedAggregate
                    | DictionaryUse::SemiJoinedAggregate
                    | DictionaryUse::JoinedAggregateWithCompanion
                    | DictionaryUse::TransformedJoinedAggregate
            ) && !file_storage_profitable
                && !((tiny_amplified_dimension || small_join_payload_source)
                    && file_dictionary_page_storage_profitable)
            {
                return false;
            }
            storage_profitable &= file_storage_profitable;
            weak_storage_profitable &= file_weak_storage_profitable;
            dictionary_page_storage_profitable &= file_dictionary_page_storage_profitable;
            all_wide_values &= value_bytes_per_row >= 64;
            min_value_bytes_per_row = min_value_bytes_per_row.min(value_bytes_per_row);
        }
        if dictionary_use == DictionaryUse::DerivedAggregate {
            return saw_file;
        }
        if dictionary_use == DictionaryUse::FilteredDistinctRollup {
            return saw_file;
        }
        if dictionary_use == DictionaryUse::JoinPayload {
            let Some(source_rows) = footer_rows() else {
                return false;
            };
            if source_rows == 0
                || source_rows > MAX_JOIN_PAYLOAD_SOURCE_ROWS
                || source_rows
                    .checked_mul(MIN_JOIN_PAYLOAD_ROWS_PER_SOURCE_ROW)
                    .is_none_or(|minimum_rows| input_rows < minimum_rows)
                || source_rows
                    .checked_mul(MIN_AMPLIFIED_JOIN_ROWS_PER_SOURCE_ROW)
                    .is_some_and(|amplified_rows| input_rows >= amplified_rows)
            {
                return false;
            }
            let saved_bytes_per_row = UTF8_VIEW_SLOT_BYTES - size_of::<i32>();
            return saw_file
                && (storage_profitable
                    || (small_join_payload_source
                        && dictionary_page_storage_profitable))
                && input_rows
                    .checked_mul(saved_bytes_per_row)
                    .is_some_and(|bytes| bytes >= MIN_JOIN_PAYLOAD_SAVED_BYTES);
        }
        if dictionary_use == DictionaryUse::AmplifiedJoinPayload {
            let Some(source_rows) = footer_rows() else {
                return false;
            };
            if source_rows == 0
                || source_rows
                    .checked_mul(MIN_AMPLIFIED_JOIN_ROWS_PER_SOURCE_ROW)
                    .is_none_or(|minimum_rows| input_rows < minimum_rows)
            {
                return false;
            }
            // Native Utf8 needs one i32 offset per row, while a dictionary
            // needs one i32 key. Their difference is therefore the repeated
            // value bytes conservatively reported by the file metadata.
            let Some(saved_bytes_per_row) = size_of::<i32>()
                .checked_add(min_value_bytes_per_row)
                .and_then(|bytes| bytes.checked_sub(size_of::<i32>()))
            else {
                return false;
            };
            let min_saved_bytes = if source_rows <= MAX_TINY_DIMENSION_ROWS {
                MIN_TINY_DIMENSION_SAVED_BYTES
            } else {
                MIN_AMPLIFIED_JOIN_SAVED_BYTES
            };
            return saw_file
                && (storage_profitable
                    || (tiny_amplified_dimension && dictionary_page_storage_profitable))
                && input_rows
                    .checked_mul(saved_bytes_per_row)
                    .is_some_and(|bytes| bytes >= min_saved_bytes);
        }
        let is_joined_aggregate = matches!(
            dictionary_use,
            DictionaryUse::JoinedAggregate
                | DictionaryUse::JoinedAggregateWithCompanion
                | DictionaryUse::SemiJoinedAggregate
                | DictionaryUse::TransformedJoinedAggregate
        );
        let joined_storage_profitable = storage_profitable
            || (dictionary_use == DictionaryUse::SemiJoinedAggregate
                && weak_storage_profitable)
            || (dictionary_use == DictionaryUse::JoinedAggregateWithCompanion
                && all_wide_values);
        if is_joined_aggregate && !joined_storage_profitable {
            let Some(source_rows) = footer_rows() else {
                return false;
            };
            let transformed_tiny_dimension = dictionary_use
                == DictionaryUse::TransformedJoinedAggregate
                && source_rows <= 5;
            if !transformed_tiny_dimension {
                return false;
            }
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
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let input_schema = input.schema();
        if output_schema.fields().len() != input_schema.fields().len() {
            return Ok(None);
        }
        let mut expressions = Vec::with_capacity(output_schema.fields().len());
        for (index, (field, input_field)) in output_schema
            .fields()
            .iter()
            .zip(input_schema.fields())
            .enumerate()
        {
            let column =
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>;
            let expr = if field == input_field {
                column
            } else if field.name() == input_field.name()
                && field.is_nullable() == input_field.is_nullable()
                && field.metadata() == input_field.metadata()
                && Self::is_supported_string_replacement(
                    field.data_type(),
                    input_field.data_type(),
                )
            {
                Arc::new(CastExpr::new_with_target_field(
                    column,
                    Arc::clone(field),
                    None,
                )) as Arc<dyn PhysicalExpr>
            } else {
                return Ok(None);
            };
            expressions.push((expr, field.name().clone()));
        }
        Ok(Some(Arc::new(
            ProjectionExec::try_new_with_schema_metadata(
                expressions,
                input,
                output_schema.as_ref(),
            )?,
        )))
    }

    /// Whether this node has the shape handled by the restoration pass.
    fn has_restoration_filter_shape(plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<FilterExec>().is_some_and(|filter| {
            filter.fetch().is_none()
                && filter.projection().is_none()
                && filter.input().downcast_ref::<ProjectionExec>().is_some_and(
                    |projection| {
                        projection.input().downcast_ref::<AggregateExec>().is_some()
                    },
                )
        })
    }

    /// Move a schema-restoring projection above a filter that does not read
    /// any retyped field. This avoids decoding every aggregate group before a
    /// selective `HAVING` predicate while preserving the rule's output schema.
    fn push_restoration_above_filter(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(filter) = plan.downcast_ref::<FilterExec>() else {
            return Ok(Transformed::no(plan));
        };
        if filter.fetch().is_some() || filter.projection().is_some() {
            return Ok(Transformed::no(plan));
        }
        let Some(projection) = filter.input().downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(plan));
        };
        if projection.input().downcast_ref::<AggregateExec>().is_none() {
            return Ok(Transformed::no(plan));
        }

        let input_schema = projection.input().schema();
        let output_schema = projection.schema();
        if projection.expr().len() != input_schema.fields().len()
            || output_schema.fields().len() != input_schema.fields().len()
        {
            return Ok(Transformed::no(plan));
        }
        let mut changed_indices = HashSet::new();
        for (index, projection_expr) in projection.expr().iter().enumerate() {
            let input_field = input_schema.field(index);
            let output_field = output_schema.field(index);
            if projection_expr.alias.as_str() != output_field.name() {
                return Ok(Transformed::no(plan));
            }
            if projection_expr
                .expr
                .downcast_ref::<Column>()
                .is_some_and(|column| column.index() == index)
                && input_field == output_field
            {
                continue;
            }
            let Some(cast) = projection_expr.expr.downcast_ref::<CastExpr>() else {
                return Ok(Transformed::no(plan));
            };
            if cast
                .expr()
                .downcast_ref::<Column>()
                .is_none_or(|column| column.index() != index)
                || cast.cast_type() != output_field.data_type()
                || input_field.name() != output_field.name()
                || input_field.is_nullable() != output_field.is_nullable()
                || input_field.metadata() != output_field.metadata()
                || !Self::is_supported_string_replacement(
                    output_field.data_type(),
                    input_field.data_type(),
                )
            {
                return Ok(Transformed::no(plan));
            }
            changed_indices.insert(index);
        }
        if changed_indices.is_empty()
            || collect_columns(filter.predicate())
                .iter()
                .any(|column| changed_indices.contains(&column.index()))
        {
            return Ok(Transformed::no(plan));
        }

        let new_filter = Arc::new(
            FilterExecBuilder::from(filter)
                .with_input(Arc::clone(projection.input()))
                .build()?,
        ) as Arc<dyn ExecutionPlan>;
        let restored = Arc::new(ProjectionExec::try_new_with_schema_metadata(
            projection.expr().iter().cloned(),
            new_filter,
            output_schema.as_ref(),
        )?) as Arc<dyn ExecutionPlan>;
        Ok(Transformed::yes(restored))
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
        let downstream_join_aggregates = match self.phase {
            DictionaryPhase::Aggregation => Self::downstream_join_aggregates(&plan),
            DictionaryPhase::Filter => vec![],
        };
        let mut saw_restoration_filter_shape = false;
        let transformed = plan.transform_down(|plan| {
            saw_restoration_filter_shape |=
                matches!(self.phase, DictionaryPhase::Aggregation)
                    && Self::has_restoration_filter_shape(&plan);
            let rewritten = match self.phase {
                DictionaryPhase::Aggregation => Self::try_rewrite_aggregate(
                    &plan,
                    context,
                    downstream_join_aggregates
                        .iter()
                        .any(|aggregate| Arc::ptr_eq(&aggregate.aggregate, &plan)),
                )?,
                DictionaryPhase::Filter => Self::try_rewrite_filter(&plan, context)?,
            };
            Ok(match rewritten {
                Some(rewritten) => Transformed::yes(rewritten),
                None => Transformed::no(plan),
            })
        })?;
        let run_restoration_pass =
            transformed.transformed || saw_restoration_filter_shape;
        let plan = transformed.data;
        match self.phase {
            DictionaryPhase::Aggregation if run_restoration_pass => plan
                .transform_up(Self::push_restoration_above_filter)
                .data(),
            DictionaryPhase::Aggregation => Ok(plan),
            DictionaryPhase::Filter => Ok(plan),
        }
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
    use arrow::array::{
        ArrayRef, BooleanArray, DictionaryArray, Int64Array, StringViewArray, UInt64Array,
    };
    use arrow::datatypes::{Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::{
        ColumnStatistics, Constraints, NullEquality, PhysicalColumnStatistics,
        ScalarValue, Statistics,
    };
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_execution::{TaskContext, object_store::ObjectStoreUrl};
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::test::function_stub::count_udaf;
    use datafusion_functions_aggregate::count::count_udaf as real_count_udaf;
    use datafusion_functions_aggregate::min_max::min_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{IsNotNullExpr, LikeExpr};
    use datafusion_physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use datafusion_physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_physical_plan::collect;
    use datafusion_physical_plan::filter::FilterExec;
    use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
    use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion_physical_plan::test::TestMemoryExec;

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
                        min_dictionary_page_bytes: all_dictionary_encoded
                            .then_some(MIN_DERIVED_DICTIONARY_PAGE_BYTES),
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
                            min_dictionary_page_bytes: Some(
                                MIN_DERIVED_DICTIONARY_PAGE_BYTES,
                            ),
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

    fn filtered_distinct_aggregate_chain(
        input: Arc<dyn ExecutionPlan>,
        inner_group_indices: &[usize],
        outer_group_indices: &[usize],
        count_index: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        filtered_distinct_aggregate_chain_with_final_mode(
            input,
            inner_group_indices,
            outer_group_indices,
            count_index,
            AggregateMode::FinalPartitioned,
        )
    }

    fn filtered_distinct_aggregate_chain_with_final_mode(
        input: Arc<dyn ExecutionPlan>,
        inner_group_indices: &[usize],
        outer_group_indices: &[usize],
        count_index: usize,
        final_mode: AggregateMode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(matches!(
            final_mode,
            AggregateMode::Final | AggregateMode::FinalPartitioned
        ));
        let raw_input_schema = input.schema();
        let inner_groups = inner_group_indices
            .iter()
            .map(|&index| {
                let field = raw_input_schema.field(index);
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();
        let inner_partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(inner_groups),
            vec![],
            vec![],
            input,
            Arc::clone(&raw_input_schema),
        )?);
        let inner_final = Arc::new(AggregateExec::try_new(
            final_mode,
            inner_partial.group_expr().as_final(),
            vec![],
            vec![],
            inner_partial,
            raw_input_schema,
        )?);

        let outer_input_schema = inner_final.schema();
        let count_field = outer_input_schema.field(count_index);
        let count = Arc::new(
            AggregateExprBuilder::new(
                real_count_udaf(),
                vec![Arc::new(Column::new(count_field.name(), count_index))],
            )
            .schema(Arc::clone(&outer_input_schema))
            .alias("count_distinct_value")
            .build()?,
        );
        let outer_groups = outer_group_indices
            .iter()
            .map(|&index| {
                let field = outer_input_schema.field(index);
                (
                    Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                    field.name().to_string(),
                )
            })
            .collect();
        let outer_partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(outer_groups),
            vec![count],
            vec![None],
            inner_final,
            Arc::clone(&outer_input_schema),
        )?);
        let outer_final = AggregateExec::try_new(
            final_mode,
            outer_partial.group_expr().as_final(),
            outer_partial.aggr_expr().to_vec(),
            vec![None],
            outer_partial,
            outer_input_schema,
        )?;
        Ok(Arc::new(outer_final))
    }

    #[derive(Clone, Copy)]
    enum FilteredDistinctExtraFilter {
        None,
        Unrelated,
        DuplicateCandidate,
    }

    fn filtered_distinct_rollup_plan_with(
        footer_rows: usize,
        all_dictionary_encoded: bool,
        native_value_type: DataType,
        filter_literal: &str,
        extra_group_key: bool,
        distinct_type: DataType,
        extra_filter: FilteredDistinctExtraFilter,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        filtered_distinct_rollup_plan_with_final_mode(
            footer_rows,
            all_dictionary_encoded,
            native_value_type,
            filter_literal,
            extra_group_key,
            distinct_type,
            extra_filter,
            AggregateMode::FinalPartitioned,
        )
    }

    #[expect(clippy::too_many_arguments)]
    fn filtered_distinct_rollup_plan_with_final_mode(
        footer_rows: usize,
        all_dictionary_encoded: bool,
        native_value_type: DataType,
        filter_literal: &str,
        extra_group_key: bool,
        distinct_type: DataType,
        extra_filter: FilteredDistinctExtraFilter,
        final_mode: AggregateMode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut fields = vec![
            Field::new("candidate", DataType::Utf8View, true),
            Field::new("distinct_value", distinct_type, false),
        ];
        if extra_group_key {
            fields.push(Field::new("extra_group", DataType::Int64, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let physical_statistics = PhysicalFileStatistics {
            num_rows: Some(footer_rows),
            column_statistics: (0..schema.fields().len())
                .map(|index| {
                    if index == 0 {
                        PhysicalColumnStatistics {
                            all_dictionary_encoded,
                            native_dictionary_value_type: Some(native_value_type.clone()),
                            // Deliberately omit decoded-value byte statistics:
                            // the ClickBench files for this shape do not have
                            // them, so the exact structural/all-dictionary proof
                            // must stand on its own.
                            ..Default::default()
                        }
                    } else {
                        PhysicalColumnStatistics::default()
                    }
                })
                .collect(),
        };
        let file = PartitionedFile::new("data.parquet", 1024)
            .with_extension(physical_statistics);
        let candidate_predicate = || {
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("candidate", 0)),
                Operator::NotEq,
                Arc::new(Literal::new(ScalarValue::Utf8View(Some(
                    filter_literal.to_string(),
                )))),
            )) as Arc<dyn PhysicalExpr>
        };
        let scan = scan_plan(
            Arc::clone(&schema),
            vec![file],
            None,
            Some(candidate_predicate()),
            None,
        )?;
        let mut filtered = Arc::new(FilterExec::try_new(candidate_predicate(), scan)?)
            as Arc<dyn ExecutionPlan>;
        filtered = match extra_filter {
            FilteredDistinctExtraFilter::None => filtered,
            FilteredDistinctExtraFilter::Unrelated => Arc::new(FilterExec::try_new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("distinct_value", 1)),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::UInt64(Some(0)))),
                )),
                filtered,
            )?),
            FilteredDistinctExtraFilter::DuplicateCandidate => {
                Arc::new(FilterExec::try_new(candidate_predicate(), filtered)?)
            }
        };

        let mut inner_group_indices = vec![0, 1];
        let mut outer_group_indices = vec![0];
        if extra_group_key {
            inner_group_indices.push(2);
            outer_group_indices.push(2);
        }
        filtered_distinct_aggregate_chain_with_final_mode(
            filtered,
            &inner_group_indices,
            &outer_group_indices,
            1,
            final_mode,
        )
    }

    fn filtered_distinct_rollup_plan(
        footer_rows: usize,
        all_dictionary_encoded: bool,
        native_value_type: DataType,
        filter_literal: &str,
        extra_group_key: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        filtered_distinct_rollup_plan_with(
            footer_rows,
            all_dictionary_encoded,
            native_value_type,
            filter_literal,
            extra_group_key,
            DataType::UInt64,
            FilteredDistinctExtraFilter::None,
        )
    }

    fn reordered_projected_filtered_distinct_rollup_plan()
    -> Result<Arc<dyn ExecutionPlan>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("payload", DataType::Int64, false),
            Field::new("candidate", DataType::Utf8View, true),
            Field::new("distinct_value", DataType::UInt32, false),
        ]));
        let physical_statistics = PhysicalFileStatistics {
            num_rows: Some(100_000_000),
            column_statistics: vec![
                PhysicalColumnStatistics::default(),
                PhysicalColumnStatistics {
                    all_dictionary_encoded: true,
                    native_dictionary_value_type: Some(DataType::Binary),
                    ..Default::default()
                },
                PhysicalColumnStatistics::default(),
            ],
        };
        let source_predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("candidate", 1)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(String::new())))),
        )) as Arc<dyn PhysicalExpr>;
        // The scan emits [distinct, payload, candidate], proving that footer
        // metadata is still looked up by the candidate's nonzero table index.
        let scan = scan_plan(
            Arc::clone(&schema),
            vec![
                PartitionedFile::new("data.parquet", 1024)
                    .with_extension(physical_statistics),
            ],
            Some(vec![2, 0, 1]),
            Some(source_predicate),
            None,
        )?;
        let filter_predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("candidate", 2)),
            Operator::NotEq,
            Arc::new(Literal::new(ScalarValue::Utf8View(Some(String::new())))),
        )) as Arc<dyn PhysicalExpr>;
        let filtered = Arc::new(FilterExec::try_new(filter_predicate, scan)?)
            as Arc<dyn ExecutionPlan>;
        // Project [candidate, distinct], then deliberately deduplicate in the
        // opposite order so COUNT reads inner output 0 and the candidate is 1.
        let projected = Arc::new(ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("candidate", 2)) as Arc<dyn PhysicalExpr>,
                    "candidate".to_string(),
                ),
                (
                    Arc::new(Column::new("distinct_value", 0)) as Arc<dyn PhysicalExpr>,
                    "distinct_value".to_string(),
                ),
            ],
            filtered,
        )?) as Arc<dyn ExecutionPlan>;
        filtered_distinct_aggregate_chain(projected, &[1, 0], &[1], 0)
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

    fn partial_final_group_count_aggregate(
        input: Arc<dyn ExecutionPlan>,
        group_index: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input_schema = input.schema();
        let field = input_schema.field(group_index);
        let count = Arc::new(
            AggregateExprBuilder::new(
                real_count_udaf(),
                vec![Arc::new(Literal::new(ScalarValue::Int64(Some(1))))],
            )
            .schema(Arc::clone(&input_schema))
            .alias("count_rows")
            .build()?,
        );
        let partial = Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new(field.name(), group_index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )]),
            vec![count],
            vec![None],
            input,
            Arc::clone(&input_schema),
        )?);
        Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::FinalPartitioned,
            partial.group_expr().as_final(),
            partial.aggr_expr().to_vec(),
            vec![None],
            partial,
            input_schema,
        )?))
    }

    fn join_on_reordered_aggregate_output(
        aggregate: Arc<dyn ExecutionPlan>,
        aggregate_output_index: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let aggregate_schema = aggregate.schema();
        assert_eq!(aggregate_schema.fields().len(), 2);
        let projection = Arc::new(ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new(aggregate_schema.field(1).name(), 1))
                        as Arc<dyn PhysicalExpr>,
                    "measure".to_string(),
                ),
                (
                    Arc::new(Column::new(aggregate_schema.field(0).name(), 0))
                        as Arc<dyn PhysicalExpr>,
                    "group_key".to_string(),
                ),
            ],
            aggregate,
        )?) as Arc<dyn ExecutionPlan>;
        let projection = Arc::new(FilterExec::try_new(
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            projection,
        )?) as Arc<dyn ExecutionPlan>;
        let projected_join_index = 1 - aggregate_output_index;
        let join_type = projection
            .schema()
            .field(projected_join_index)
            .data_type()
            .clone();
        let peer = statistics_scan(
            Arc::new(Schema::new(vec![Field::new("join_key", join_type, false)])),
            10_000,
            &[None],
            None,
            Constraints::default(),
        );
        Ok(Arc::new(HashJoinExec::try_new(
            peer,
            projection,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new(
                    if projected_join_index == 0 {
                        "measure"
                    } else {
                        "group_key"
                    },
                    projected_join_index,
                )),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?))
    }

    fn dictionary_aggregate_with_restoration() -> Result<Arc<dyn ExecutionPlan>> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("group_key", dictionary_type, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input =
            Arc::new(EmptyExec::new(Arc::clone(&input_schema))) as Arc<dyn ExecutionPlan>;
        let count = AggregateExprBuilder::new(
            real_count_udaf(),
            vec![Arc::new(Column::new("value", 1))],
        )
        .schema(Arc::clone(&input_schema))
        .alias("count_value")
        .build()?;
        let aggregate = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                Arc::new(Column::new("group_key", 0)) as Arc<dyn PhysicalExpr>,
                "group_key".to_string(),
            )]),
            vec![Arc::new(count)],
            vec![None],
            input,
            input_schema,
        )?) as Arc<dyn ExecutionPlan>;

        let aggregate_schema = aggregate.schema();
        let restored_fields = aggregate_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                if index == 0 {
                    Arc::new(field.as_ref().clone().with_data_type(DataType::Utf8View))
                        as FieldRef
                } else {
                    Arc::clone(field)
                }
            })
            .collect::<Vec<_>>();
        let restored_schema = Arc::new(Schema::new_with_metadata(
            restored_fields,
            aggregate_schema.metadata().clone(),
        ));
        Ok(
            DictionaryPreservation::restore_output_schema(&restored_schema, aggregate)?
                .expect("dictionary aggregate should support Utf8View restoration"),
        )
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
                            min_dictionary_page_bytes: Some(
                                MIN_DERIVED_DICTIONARY_PAGE_BYTES,
                            ),
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

    fn statistics_scan(
        schema: SchemaRef,
        rows: usize,
        distinct_counts: &[Option<usize>],
        physical_statistics: Option<PhysicalFileStatistics>,
        constraints: Constraints,
    ) -> Arc<dyn ExecutionPlan> {
        assert_eq!(schema.fields().len(), distinct_counts.len());
        let mut file = PartitionedFile::new("data.parquet", 1024);
        if let Some(physical_statistics) = physical_statistics {
            file = file.with_extension(physical_statistics);
        }
        let statistics = Statistics {
            num_rows: Precision::Exact(rows),
            total_byte_size: Precision::Absent,
            column_statistics: distinct_counts
                .iter()
                .map(|distinct_count| ColumnStatistics {
                    null_count: Precision::Exact(0),
                    distinct_count: distinct_count
                        .map(Precision::Exact)
                        .unwrap_or(Precision::Absent),
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
        .with_constraints(constraints)
        .build();
        DataSourceExec::from_data_source(config)
    }

    fn amplified_join_plan(
        unique_build_key: bool,
        all_dictionary_encoded: bool,
        anchor_mode: PartitionMode,
        downstream_join_type: JoinType,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        amplified_join_plan_with_anchor(
            unique_build_key.then(|| Constraint::PrimaryKey(vec![0])),
            false,
            NullEquality::NullEqualsNothing,
            all_dictionary_encoded,
            anchor_mode,
            downstream_join_type,
        )
    }

    fn amplified_join_plan_with_anchor(
        build_constraint: Option<Constraint>,
        build_key_nullable: bool,
        anchor_null_equality: NullEquality,
        all_dictionary_encoded: bool,
        anchor_mode: PartitionMode,
        downstream_join_type: JoinType,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let downstream = amplified_join_input(
            build_constraint,
            build_key_nullable,
            anchor_null_equality,
            all_dictionary_encoded,
            anchor_mode,
            downstream_join_type,
            3_790_000,
            (12, Some(10_000)),
        )?;
        single_group_aggregate(downstream, 0)
    }

    #[expect(clippy::too_many_arguments)]
    fn amplified_join_input(
        build_constraint: Option<Constraint>,
        build_key_nullable: bool,
        anchor_null_equality: NullEquality,
        all_dictionary_encoded: bool,
        anchor_mode: PartitionMode,
        downstream_join_type: JoinType,
        carried_rows: usize,
        payload_statistics: (usize, Option<usize>),
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (value_bytes_per_row, payload_distinct_count) = payload_statistics;
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, build_key_nullable),
            Field::new("s_name", DataType::Utf8View, false),
        ]));
        let supplier_physical = PhysicalFileStatistics {
            num_rows: Some(10_000),
            column_statistics: vec![
                PhysicalColumnStatistics::default(),
                PhysicalColumnStatistics {
                    min_dictionary_page_bytes: Some(MIN_DERIVED_DICTIONARY_PAGE_BYTES),
                    all_dictionary_encoded,
                    native_dictionary_value_type: Some(DataType::Utf8),
                    min_row_group_value_bytes_per_row: Some(value_bytes_per_row),
                    min_row_group_value_bytes_per_encoded_byte: Some(2),
                },
            ],
        };
        let constraints = build_constraint
            .map(|constraint| Constraints::new_unverified(vec![constraint]))
            .unwrap_or_default();
        let supplier = statistics_scan(
            supplier_schema,
            10_000,
            &[Some(10_000), payload_distinct_count],
            Some(supplier_physical),
            constraints,
        );

        let lineitem_schema = Arc::new(Schema::new(vec![
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_orderkey", DataType::Int64, false),
        ]));
        let lineitem = statistics_scan(
            lineitem_schema,
            carried_rows,
            &[Some(10_000), Some(carried_rows.min(1_000_000))],
            None,
            Constraints::default(),
        );
        let amplified = Arc::new(HashJoinExec::try_new(
            supplier,
            lineitem,
            vec![(
                Arc::new(Column::new("s_suppkey", 0)),
                Arc::new(Column::new("l_suppkey", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![1, 3]),
            anchor_mode,
            anchor_null_equality,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let orders_schema = Arc::new(Schema::new(vec![Field::new(
            "o_orderkey",
            DataType::Int64,
            false,
        )]));
        let orders = statistics_scan(
            orders_schema,
            1_000_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let projection = match downstream_join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => Some(vec![0]),
            JoinType::Inner => Some(vec![0]),
            _ => panic!("test helper only supports left-preserving join shapes"),
        };
        let downstream = Arc::new(HashJoinExec::try_new(
            amplified,
            orders,
            vec![(
                Arc::new(Column::new("l_orderkey", 1)),
                Arc::new(Column::new("o_orderkey", 0)),
            )],
            None,
            &downstream_join_type,
            projection,
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        Ok(downstream)
    }

    fn q6_star_join_plan(propagate_future_key: bool) -> Result<Arc<dyn ExecutionPlan>> {
        let dimension = statistics_scan(
            Arc::new(Schema::new(vec![
                Field::new("d_key", DataType::Int64, false),
                Field::new("d_name", DataType::Utf8View, false),
            ])),
            10_000,
            &[Some(10_000), Some(10_000)],
            Some(PhysicalFileStatistics {
                num_rows: Some(10_000),
                column_statistics: vec![
                    PhysicalColumnStatistics::default(),
                    PhysicalColumnStatistics {
                        min_dictionary_page_bytes: Some(
                            MIN_DERIVED_DICTIONARY_PAGE_BYTES,
                        ),
                        all_dictionary_encoded: true,
                        native_dictionary_value_type: Some(DataType::Utf8),
                        min_row_group_value_bytes_per_row: Some(12),
                        min_row_group_value_bytes_per_encoded_byte: Some(2),
                    },
                ],
            }),
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        );
        let bridge_constraints = if propagate_future_key {
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![1])])
        } else {
            Constraints::default()
        };
        let bridge = statistics_scan(
            Arc::new(Schema::new(vec![
                Field::new("b_d_key", DataType::Int64, false),
                Field::new("b_future_key", DataType::Int64, false),
            ])),
            10_000,
            &[Some(10_000), Some(10_000)],
            None,
            bridge_constraints,
        );
        let first_anchor = Arc::new(HashJoinExec::try_new(
            dimension,
            bridge,
            vec![(
                Arc::new(Column::new("d_key", 0)),
                Arc::new(Column::new("b_d_key", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![1, 3]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let coalesced =
            Arc::new(CoalescePartitionsExec::new(first_anchor)) as Arc<dyn ExecutionPlan>;

        let fact = statistics_scan(
            Arc::new(Schema::new(vec![
                Field::new("f_future_key", DataType::Int64, false),
                Field::new("f_filter_key", DataType::Int64, false),
            ])),
            3_790_000,
            &[Some(10_000), Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let repeated_anchor = Arc::new(HashJoinExec::try_new(
            coalesced,
            fact,
            vec![(
                Arc::new(Column::new("b_future_key", 1)),
                Arc::new(Column::new("f_future_key", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![0, 3]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let enrichment = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "e_filter_key",
                DataType::Int64,
                false,
            )])),
            1_000_000,
            &[Some(1_000_000)],
            None,
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        );
        let enriched = Arc::new(HashJoinExec::try_new(
            enrichment,
            repeated_anchor,
            vec![(
                Arc::new(Column::new("e_filter_key", 0)),
                Arc::new(Column::new("f_filter_key", 1)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![1, 2]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let filter_keys = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "filter_key",
                DataType::Int64,
                false,
            )])),
            1_000_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let filtered = Arc::new(HashJoinExec::try_new(
            enriched,
            filter_keys,
            vec![(
                Arc::new(Column::new("f_filter_key", 1)),
                Arc::new(Column::new("filter_key", 0)),
            )],
            None,
            &JoinType::LeftSemi,
            Some(vec![0]),
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        single_group_aggregate(filtered, 0)
    }

    fn q21_join_plan() -> Result<Arc<dyn ExecutionPlan>> {
        let supplier_schema = Arc::new(Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8View, false),
            Field::new("s_nationkey", DataType::Int64, false),
        ]));
        let supplier_physical = PhysicalFileStatistics {
            num_rows: Some(10_000),
            column_statistics: vec![
                PhysicalColumnStatistics::default(),
                PhysicalColumnStatistics {
                    min_dictionary_page_bytes: Some(MIN_DERIVED_DICTIONARY_PAGE_BYTES),
                    all_dictionary_encoded: true,
                    native_dictionary_value_type: Some(DataType::Utf8),
                    min_row_group_value_bytes_per_row: Some(12),
                    min_row_group_value_bytes_per_encoded_byte: Some(2),
                },
                PhysicalColumnStatistics::default(),
            ],
        };
        let supplier = statistics_scan(
            supplier_schema,
            10_000,
            &[Some(10_000), Some(10_000), Some(25)],
            Some(supplier_physical),
            Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])]),
        );
        let lineitem = statistics_scan(
            Arc::new(Schema::new(vec![
                Field::new("l_suppkey", DataType::Int64, false),
                Field::new("l_orderkey", DataType::Int64, false),
            ])),
            3_790_000,
            &[Some(10_000), Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let amplified = Arc::new(HashJoinExec::try_new(
            supplier,
            lineitem,
            vec![(
                Arc::new(Column::new("s_suppkey", 0)),
                Arc::new(Column::new("l_suppkey", 0)),
            )],
            None,
            &JoinType::Inner,
            Some(vec![1, 2, 4]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let orders = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "o_orderkey",
                DataType::Int64,
                false,
            )])),
            1_000_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let orders_filtered = Arc::new(HashJoinExec::try_new(
            orders,
            amplified,
            vec![(
                Arc::new(Column::new("o_orderkey", 0)),
                Arc::new(Column::new("l_orderkey", 2)),
            )],
            None,
            &JoinType::RightSemi,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let nation = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "n_nationkey",
                DataType::Int64,
                false,
            )])),
            25,
            &[Some(25)],
            None,
            Constraints::default(),
        );
        let nation_filtered = Arc::new(HashJoinExec::try_new(
            nation,
            orders_filtered,
            vec![(
                Arc::new(Column::new("n_nationkey", 0)),
                Arc::new(Column::new("s_nationkey", 1)),
            )],
            None,
            &JoinType::RightSemi,
            Some(vec![0, 2]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let exists_lineitem = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "l_orderkey",
                DataType::Int64,
                false,
            )])),
            3_790_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let exists_filtered = Arc::new(HashJoinExec::try_new(
            nation_filtered,
            exists_lineitem,
            vec![(
                Arc::new(Column::new("l_orderkey", 1)),
                Arc::new(Column::new("l_orderkey", 0)),
            )],
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;

        let late_lineitem = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "l_orderkey",
                DataType::Int64,
                false,
            )])),
            3_790_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let late_filtered = Arc::new(HashJoinExec::try_new(
            exists_filtered,
            late_lineitem,
            vec![(
                Arc::new(Column::new("l_orderkey", 1)),
                Arc::new(Column::new("l_orderkey", 0)),
            )],
            None,
            &JoinType::LeftAnti,
            Some(vec![0]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        // TPC-H Q21 groups by supplier name and counts the matching rows.
        partial_final_group_count_aggregate(late_filtered, 0)
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

    fn amplified_payload_rows(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<usize>> {
        let aggregate = plan
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an aggregate");
        let aggregate = match aggregate.mode() {
            AggregateMode::Final | AggregateMode::FinalPartitioned => aggregate
                .input()
                .downcast_ref::<AggregateExec>()
                .expect("final aggregate should consume a partial aggregate"),
            _ => aggregate,
        };
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);
        let columns = DictionaryPreservation::group_columns(aggregate, &context)?;
        Ok(columns
            .first()
            .and_then(|column| column.amplified_join_payload_rows))
    }

    fn group_column_count(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
        let aggregate = plan
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an aggregate");
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);
        Ok(DictionaryPreservation::group_columns(aggregate, &context)?.len())
    }

    fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            return scan.data_source().downcast_ref::<FileScanConfig>();
        }
        plan.children()
            .into_iter()
            .find_map(|child| find_scan(child))
    }

    fn replace_scan_with_memory(
        plan: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        partitions: &[Vec<RecordBatch>],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let memory = TestMemoryExec::try_new_exec(partitions, schema, None)?
            as Arc<dyn ExecutionPlan>;
        let mut replaced = 0;
        let plan = plan
            .transform_up(|plan| {
                if plan.downcast_ref::<DataSourceExec>().is_some() {
                    replaced += 1;
                    Ok(Transformed::yes(Arc::clone(&memory)))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data;
        assert_eq!(replaced, 1, "test plan should contain exactly one scan");
        Ok(plan)
    }

    async fn collect_filtered_distinct_result(
        plan: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        partitions: &[Vec<RecordBatch>],
    ) -> Result<Vec<(String, i64)>> {
        let plan = replace_scan_with_memory(plan, schema, partitions)?;
        assert_eq!(plan.schema().field(0).data_type(), &DataType::Utf8View);

        let mut rows = vec![];
        for batch in collect(plan, Arc::new(TaskContext::default())).await? {
            let candidates = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("restored group key should be Utf8View");
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT result should be Int64");
            rows.extend(candidates.iter().zip(counts.iter()).map(
                |(candidate, count)| {
                    (
                        candidate
                            .expect("filtered group key should be non-null")
                            .to_string(),
                        count.expect("COUNT result should be non-null"),
                    )
                },
            ));
        }
        rows.sort_unstable();
        Ok(rows)
    }

    fn collect_scan_field_types(
        plan: &Arc<dyn ExecutionPlan>,
        field_name: &str,
        data_types: &mut Vec<DataType>,
    ) {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>() {
            let config = scan
                .data_source()
                .downcast_ref::<FileScanConfig>()
                .expect("test scan should use FileScanConfig");
            if let Ok(index) = config.file_schema().index_of(field_name) {
                data_types.push(config.file_schema().field(index).data_type().clone());
            }
            return;
        }
        for child in plan.children() {
            collect_scan_field_types(child, field_name, data_types);
        }
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
    fn rewrites_filtered_distinct_rollup_without_decoded_value_statistics() -> Result<()>
    {
        let plan = filtered_distinct_rollup_plan(
            100_000_000,
            true,
            DataType::Binary,
            "",
            false,
        )?;
        let original_schema = plan.schema();
        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("outer result should restore Utf8View once");
        let outer_final = projection
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("projection should wrap the outer final aggregate");
        let outer_partial = outer_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("outer final should consume the outer partial aggregate");
        let inner_final = outer_partial
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("outer partial should consume the inner final aggregate directly");
        let inner_partial = inner_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("inner final should consume the inner partial aggregate");
        let filter = inner_partial
            .input()
            .downcast_ref::<FilterExec>()
            .expect("inner partial should consume the rewritten filter");
        let scan = filter
            .input()
            .downcast_ref::<DataSourceExec>()
            .expect("filter should consume the rewritten scan");

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let scan_config = scan
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("test scan should use FileScanConfig");
        assert_eq!(
            scan_config.file_schema().field(0).data_type(),
            &dictionary_type
        );
        assert_eq!(filter.schema().field(0).data_type(), &dictionary_type);
        for schema in [
            inner_partial.input_schema(),
            inner_final.input_schema(),
            outer_partial.input_schema(),
            outer_final.input_schema(),
        ] {
            assert_eq!(schema.field(0).data_type(), &dictionary_type);
        }

        let batch = RecordBatch::try_new(
            filter.input().schema(),
            vec![
                Arc::new(DictionaryArray::<Int32Type>::from_iter([
                    Some(""),
                    Some("phone"),
                    None,
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
            ],
        )?;
        assert_predicate_result(
            filter.predicate(),
            &batch,
            vec![Some(false), Some(true), None],
        )?;
        Ok(())
    }

    #[test]
    fn rewrites_reordered_projected_filtered_distinct_rollup() -> Result<()> {
        let plan = reordered_projected_filtered_distinct_rollup_plan()?;
        let original_schema = plan.schema();
        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let restoration = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("outer result should restore Utf8View once");
        let outer_final = restoration
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("restoration should wrap the outer final aggregate");
        let outer_partial = outer_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("outer final should consume outer partial");
        let inner_final = outer_partial
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("outer partial should consume inner final");
        let inner_partial = inner_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("inner final should consume inner partial");
        let projected = inner_partial
            .input()
            .downcast_ref::<ProjectionExec>()
            .expect("inner partial should consume the reordered projection");
        let filter = projected
            .input()
            .downcast_ref::<FilterExec>()
            .expect("projection should consume the candidate filter");
        let scan = filter
            .input()
            .downcast_ref::<DataSourceExec>()
            .expect("filter should consume the projected scan");

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let scan_config = scan
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("test scan should use FileScanConfig");
        assert_eq!(
            scan_config.file_schema().field(1).data_type(),
            &dictionary_type
        );
        assert_eq!(projected.schema().field(0).data_type(), &dictionary_type);
        assert_eq!(
            inner_partial.input_schema().field(0).data_type(),
            &dictionary_type
        );
        assert_eq!(
            inner_final.input_schema().field(0).data_type(),
            &dictionary_type
        );
        assert_eq!(
            outer_partial.input_schema().field(1).data_type(),
            &dictionary_type
        );
        assert_eq!(
            outer_final.input_schema().field(1).data_type(),
            &dictionary_type
        );
        let count_arguments = outer_partial.aggr_expr()[0].expressions();
        let [count_argument] = count_arguments.as_slice() else {
            panic!("COUNT should have one argument");
        };
        assert_eq!(
            count_argument
                .downcast_ref::<Column>()
                .expect("COUNT argument should remain a direct column")
                .index(),
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn executes_filtered_distinct_rollup_with_different_dictionaries() -> Result<()>
    {
        let utf8_schema = Arc::new(Schema::new(vec![
            Field::new("candidate", DataType::Utf8View, true),
            Field::new("distinct_value", DataType::UInt64, false),
        ]));
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dictionary_schema = Arc::new(Schema::new(vec![
            Field::new("candidate", dictionary_type, true),
            Field::new("distinct_value", DataType::UInt64, false),
        ]));
        let first_values = vec![
            Some(""),
            Some("phone"),
            Some("tablet"),
            Some("phone"),
            None,
            Some("phone"),
        ];
        let first_ids = vec![0, 1, 3, 2, 9, 1];
        let second_values = vec![
            Some("tablet"),
            Some(""),
            Some("phone"),
            Some("tablet"),
            None,
            Some("phone"),
        ];
        let second_ids = vec![3, 8, 2, 4, 9, 5];

        let utf8_partitions = vec![vec![
            RecordBatch::try_new(
                Arc::clone(&utf8_schema),
                vec![
                    Arc::new(StringViewArray::from(first_values.clone())) as ArrayRef,
                    Arc::new(UInt64Array::from(first_ids.clone())) as ArrayRef,
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&utf8_schema),
                vec![
                    Arc::new(StringViewArray::from(second_values.clone())) as ArrayRef,
                    Arc::new(UInt64Array::from(second_ids.clone())) as ArrayRef,
                ],
            )?,
        ]];
        let dictionary_partitions = vec![vec![
            RecordBatch::try_new(
                Arc::clone(&dictionary_schema),
                vec![
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(first_values))
                        as ArrayRef,
                    Arc::new(UInt64Array::from(first_ids)) as ArrayRef,
                ],
            )?,
            RecordBatch::try_new(
                Arc::clone(&dictionary_schema),
                vec![
                    // First occurrence order deliberately gives this batch a
                    // different key-to-value mapping from the first batch.
                    Arc::new(DictionaryArray::<Int32Type>::from_iter(second_values))
                        as ArrayRef,
                    Arc::new(UInt64Array::from(second_ids)) as ArrayRef,
                ],
            )?,
        ]];

        for final_mode in [AggregateMode::Final, AggregateMode::FinalPartitioned] {
            let original = filtered_distinct_rollup_plan_with_final_mode(
                100_000_000,
                true,
                DataType::Binary,
                "",
                false,
                DataType::UInt64,
                FilteredDistinctExtraFilter::None,
                final_mode,
            )?;
            let optimized = optimize(Arc::clone(&original))?;

            let original_rows = collect_filtered_distinct_result(
                original,
                Arc::clone(&utf8_schema),
                &utf8_partitions,
            )
            .await?;
            let optimized_rows = collect_filtered_distinct_result(
                optimized,
                Arc::clone(&dictionary_schema),
                &dictionary_partitions,
            )
            .await?;

            assert_eq!(
                original_rows,
                vec![("phone".to_string(), 3), ("tablet".to_string(), 2)]
            );
            assert_eq!(optimized_rows, original_rows);
        }
        Ok(())
    }

    #[test]
    fn skips_filtered_distinct_rollup_near_misses() -> Result<()> {
        let near_misses = [
            // Q11-like: two output grouping keys become a three-key dedup.
            filtered_distinct_rollup_plan(100_000_000, true, DataType::Binary, "", true)?,
            // Q13-like: at least one source file is not fully dictionary encoded.
            filtered_distinct_rollup_plan(
                100_000_000,
                false,
                DataType::Binary,
                "",
                false,
            )?,
            filtered_distinct_rollup_plan(
                MIN_FILTERED_DISTINCT_ROLLUP_ROWS - 1,
                true,
                DataType::Binary,
                "",
                false,
            )?,
            filtered_distinct_rollup_plan(
                100_000_000,
                true,
                DataType::Binary,
                "unknown",
                false,
            )?,
            filtered_distinct_rollup_plan(
                100_000_000,
                true,
                DataType::LargeBinary,
                "",
                false,
            )?,
            // COUNT's inner companion must be an integral identifier.
            filtered_distinct_rollup_plan_with(
                100_000_000,
                true,
                DataType::Binary,
                "",
                false,
                DataType::Utf8View,
                FilteredDistinctExtraFilter::None,
            )?,
            // No additional FilterExec is admitted on the scan path.
            filtered_distinct_rollup_plan_with(
                100_000_000,
                true,
                DataType::Binary,
                "",
                false,
                DataType::UInt64,
                FilteredDistinctExtraFilter::Unrelated,
            )?,
            // A second candidate FilterExec is rejected; the Parquet source's
            // copy of the pushed predicate does not count as another operator.
            filtered_distinct_rollup_plan_with(
                100_000_000,
                true,
                DataType::Binary,
                "",
                false,
                DataType::UInt64,
                FilteredDistinctExtraFilter::DuplicateCandidate,
            )?,
        ];
        for plan in near_misses {
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "a near miss must keep the original physical plan"
            );
        }
        Ok(())
    }

    #[test]
    fn filtered_distinct_rollup_requires_direct_single_group() -> Result<()> {
        let plan = filtered_distinct_rollup_plan(
            100_000_000,
            true,
            DataType::Binary,
            "",
            false,
        )?;
        let outer_final = plan
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an outer final aggregate");
        let outer_partial = outer_final
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("outer final should consume outer partial");
        assert!(
            DictionaryPreservation::is_supported_filtered_distinct_rollup(
                outer_partial,
                0,
                &[0],
                &[],
            )
        );

        let rebuild = |group_by: PhysicalGroupBy| {
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by,
                outer_partial.aggr_expr().to_vec(),
                outer_partial.filter_expr().to_vec(),
                Arc::clone(outer_partial.input()),
                outer_partial.input_schema(),
            )
        };
        let non_column = rebuild(PhysicalGroupBy::new_single(vec![(
            Arc::new(CastExpr::new(
                Arc::new(Column::new("candidate", 0)),
                DataType::Utf8View,
                None,
            )) as Arc<dyn PhysicalExpr>,
            "candidate".to_string(),
        )]))?;
        let grouping_set = rebuild(PhysicalGroupBy::new(
            vec![(
                Arc::new(Column::new("candidate", 0)) as Arc<dyn PhysicalExpr>,
                "candidate".to_string(),
            )],
            vec![(
                Arc::new(Literal::new(ScalarValue::Utf8View(None)))
                    as Arc<dyn PhysicalExpr>,
                "candidate".to_string(),
            )],
            vec![vec![false], vec![true]],
            true,
        ))?;
        let stray_null_expr = rebuild(PhysicalGroupBy::new(
            vec![(
                Arc::new(Column::new("candidate", 0)) as Arc<dyn PhysicalExpr>,
                "candidate".to_string(),
            )],
            vec![(
                Arc::new(Literal::new(ScalarValue::Utf8View(None)))
                    as Arc<dyn PhysicalExpr>,
                "candidate".to_string(),
            )],
            vec![vec![false]],
            false,
        ))?;

        for aggregate in [&non_column, &grouping_set, &stray_null_expr] {
            assert!(
                !DictionaryPreservation::is_supported_filtered_distinct_rollup(
                    aggregate,
                    0,
                    &[0],
                    &[],
                )
            );
        }
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
    fn replans_min_dictionary_argument_with_one_value_cast() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("candidate", DataType::Utf8View, true),
            Field::new("other", DataType::Int64, false),
        ]));
        let candidate: Arc<dyn PhysicalExpr> = Arc::new(Column::new("candidate", 0));
        let other: Arc<dyn PhysicalExpr> = Arc::new(Column::new("other", 1));
        let min = AggregateExprBuilder::new(min_udaf(), vec![Arc::clone(&candidate)])
            .schema(Arc::clone(&input_schema))
            .alias("min_candidate")
            .build()?;
        let count =
            AggregateExprBuilder::new(real_count_udaf(), vec![Arc::clone(&other)])
                .schema(Arc::clone(&input_schema))
                .alias("count_other")
                .build()?;

        let dictionary_schema =
            DictionaryPreservation::dictionary_input_schema(&input_schema, 0)?;
        let changed_input_indices = HashSet::from([0]);
        let replanned_min = DictionaryPreservation::replan_aggregate_expr(
            &min,
            Arc::clone(&dictionary_schema),
            &changed_input_indices,
        )?
        .expect("MIN should support dictionary input coercion");
        let replanned_count = DictionaryPreservation::replan_aggregate_expr(
            &count,
            dictionary_schema,
            &changed_input_indices,
        )?
        .expect("unrelated COUNT should remain valid");

        let min_args = replanned_min.expressions();
        let [min_arg] = min_args.as_slice() else {
            panic!("MIN should have exactly one argument");
        };
        let min_cast = min_arg
            .downcast_ref::<CastExpr>()
            .expect("dictionary MIN argument should be cast to its value type");
        assert_eq!(min_cast.cast_type(), &DataType::Utf8);
        assert!(min_cast.expr().downcast_ref::<Column>().is_some());
        assert_eq!(replanned_min.field().data_type(), &DataType::Utf8);
        assert!(replanned_min.groups_accumulator_supported());
        let _groups_accumulator = replanned_min.create_groups_accumulator()?;

        let count_args = replanned_count.expressions();
        let [count_arg] = count_args.as_slice() else {
            panic!("COUNT should have exactly one argument");
        };
        assert!(count_arg.downcast_ref::<Column>().is_some());
        assert!(Arc::ptr_eq(count_arg, &other));
        Ok(())
    }

    #[test]
    fn moves_restoration_projection_above_unrelated_having() -> Result<()> {
        let restoration = dictionary_aggregate_with_restoration()?;
        let output_schema = restoration.schema();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("count_value", 1)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(100_000)))),
        ));
        let plan = Arc::new(FilterExec::try_new(predicate, restoration)?)
            as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan)?;
        assert_eq!(optimized.schema(), output_schema);
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("restoration projection should move above HAVING");
        let filter = projection
            .input()
            .downcast_ref::<FilterExec>()
            .expect("HAVING should be below the restoration projection");
        assert!(filter.input().downcast_ref::<AggregateExec>().is_some());
        Ok(())
    }

    #[test]
    fn descendant_rewrite_triggers_restoration_pass() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let aggregate = partial_final_group_count_aggregate(profitable_scan(schema), 1)?;
        let output_schema = aggregate.schema();
        let predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("count_rows", 1)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(100_000)))),
        ));
        let plan = Arc::new(FilterExec::try_new(predicate, aggregate)?)
            as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan)?;
        assert_eq!(optimized.schema(), output_schema);
        let projection = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("restoration projection should move above HAVING");
        let filter = projection
            .input()
            .downcast_ref::<FilterExec>()
            .expect("HAVING should be below the restoration projection");
        assert!(filter.input().downcast_ref::<AggregateExec>().is_some());

        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        Ok(())
    }

    #[test]
    fn keeps_restoration_below_having_that_reads_restored_field() -> Result<()> {
        let restoration = dictionary_aggregate_with_restoration()?;
        let predicate =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("group_key", 0))));
        let plan = Arc::new(FilterExec::try_new(predicate, restoration)?)
            as Arc<dyn ExecutionPlan>;

        let optimized = optimize(plan)?;
        let filter = optimized
            .downcast_ref::<FilterExec>()
            .expect("HAVING should remain above restoration");
        let projection = filter
            .input()
            .downcast_ref::<ProjectionExec>()
            .expect("restoration should remain below HAVING");
        assert!(projection.input().downcast_ref::<AggregateExec>().is_some());
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
    fn joined_aggregate_requires_at_least_source_rows() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let file = candidate_file(
            "data.parquet",
            1,
            0,
            DataType::Utf8,
            16,
            MIN_VALUE_BYTES_PER_PARQUET_BYTE,
        );
        let scan = scan_plan(schema, vec![file], None, None, None)?;
        let config = find_scan(&scan).expect("test scan should use FileScanConfig");

        for dictionary_use in [
            DictionaryUse::JoinedAggregate,
            DictionaryUse::SemiJoinedAggregate,
            DictionaryUse::JoinedAggregateWithCompanion,
            DictionaryUse::TransformedJoinedAggregate,
        ] {
            assert!(!DictionaryPreservation::is_profitable(
                config,
                0,
                Some(9_999_999),
                dictionary_use,
            ));
            assert!(DictionaryPreservation::is_profitable(
                config,
                0,
                Some(10_000_000),
                dictionary_use,
            ));
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
                    min_dictionary_page_bytes: Some(MIN_DERIVED_DICTIONARY_PAGE_BYTES),
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
    fn skips_source_filtered_aggregate_with_inexact_rows() -> Result<()> {
        for (upper_bound, pushdown_filters) in
            [(94_i64, false), (19_i64, false), (94_i64, true)]
        {
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
            assert!(Arc::ptr_eq(&optimized, &plan));
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
            min_dictionary_page_bytes: Some(MIN_DERIVED_DICTIONARY_PAGE_BYTES),
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
    fn rewrites_amplified_unique_build_payload() -> Result<()> {
        let plan = amplified_join_plan(
            true,
            true,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
        )?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(3_790_000));
        let config = find_scan(&plan).expect("Q21-shaped plan should contain its scan");
        let rows_at_saved_bytes_threshold = MIN_AMPLIFIED_JOIN_SAVED_BYTES.div_ceil(12);
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(10_000 * MIN_AMPLIFIED_JOIN_ROWS_PER_SOURCE_ROW - 1),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(rows_at_saved_bytes_threshold - 1),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            1,
            Some(rows_at_saved_bytes_threshold),
            DictionaryUse::AmplifiedJoinPayload,
        ));

        let optimized = optimize(plan)?;
        let scan = find_scan(&optimized).expect("optimized plan should retain its scan");
        assert_eq!(
            scan.file_schema().field(1).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        Ok(())
    }

    #[test]
    fn skips_exact_zero_and_structurally_empty_inputs() -> Result<()> {
        let input = amplified_join_input(
            Some(Constraint::PrimaryKey(vec![0])),
            false,
            NullEquality::NullEqualsNothing,
            true,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
            3_790_000,
            (12, Some(10_000)),
        )?;
        let projected = Arc::new(ProjectionExec::try_new(
            vec![
                (
                    Arc::new(Column::new("s_name", 0)) as Arc<dyn PhysicalExpr>,
                    "s_name".to_string(),
                ),
                (
                    Arc::new(Literal::new(ScalarValue::Int64(Some(1))))
                        as Arc<dyn PhysicalExpr>,
                    "marker".to_string(),
                ),
            ],
            input,
        )?) as Arc<dyn ExecutionPlan>;
        let marker_eq = |value| {
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("marker", 1)),
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Int64(Some(value)))),
            )) as Arc<dyn PhysicalExpr>
        };
        let exact_zero: Arc<dyn PhysicalExpr> =
            Arc::new(BinaryExpr::new(marker_eq(1), Operator::And, marker_eq(2)));
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);

        let filtered = Arc::new(FilterExec::try_new(exact_zero, projected)?)
            as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(filtered, 0)?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(3_790_000));
        let aggregate = plan
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an aggregate");
        assert_eq!(
            context
                .compute_statistics(aggregate.input(), &StatisticsArgs::new())?
                .num_rows,
            Precision::Exact(0)
        );
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));

        // Model the Q36 branch eliminated to an empty build input. The join
        // estimate is inexact, but the explicit EmptyExec makes zero rows
        // trustworthy.
        let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(
            Schema::new(vec![Field::new("join_key", DataType::Int64, false)]),
        )));
        let candidate = profitable_scan(Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ])));
        let empty_semi_join = Arc::new(HashJoinExec::try_new(
            empty,
            candidate,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("join_key", 0)),
            )],
            None,
            &JoinType::RightSemi,
            Some(vec![1]),
            PartitionMode::CollectLeft,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let plan = single_group_aggregate(empty_semi_join, 0)?;
        let aggregate = plan
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an aggregate");
        assert_eq!(
            context
                .compute_statistics(aggregate.input(), &StatisticsArgs::new())?
                .num_rows,
            Precision::Inexact(0)
        );
        assert!(DictionaryPreservation::contains_empty_exec(
            aggregate.input()
        ));
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn inexact_joined_group_width_is_bounded() -> Result<()> {
        let plan = |width| -> Result<Arc<dyn ExecutionPlan>> {
            let base = join_aggregate_plan(JoinPlanShape::Safe)?;
            let input = Arc::clone(
                base.downcast_ref::<AggregateExec>()
                    .expect("test plan should end in an aggregate")
                    .input(),
            );
            let input_schema = input.schema();
            let group_name = input_schema.field(0).name();
            let groups = (0..width)
                .map(|index| {
                    (
                        Arc::new(Column::new(group_name, 0)) as Arc<dyn PhysicalExpr>,
                        format!("group_key_{index}"),
                    )
                })
                .collect();
            Ok(Arc::new(AggregateExec::try_new(
                AggregateMode::Single,
                PhysicalGroupBy::new_single(groups),
                vec![],
                vec![],
                input,
                input_schema,
            )?) as Arc<dyn ExecutionPlan>)
        };
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);

        let at_limit = plan(MAX_INEXACT_JOINED_GROUP_EXPRESSIONS)?;
        let aggregate = at_limit
            .downcast_ref::<AggregateExec>()
            .expect("test plan should end in an aggregate");
        assert!(matches!(
            context
                .compute_statistics(aggregate.input(), &StatisticsArgs::new())?
                .num_rows,
            Precision::Inexact(_)
        ));
        let optimized = optimize(Arc::clone(&at_limit))?;
        assert!(!Arc::ptr_eq(&optimized, &at_limit));
        let scan = find_scan(&optimized).expect("eligible plan should retain its scan");
        assert!(matches!(
            scan.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));

        let over_limit = plan(MAX_INEXACT_JOINED_GROUP_EXPRESSIONS + 1)?;
        let optimized = optimize(Arc::clone(&over_limit))?;
        assert!(Arc::ptr_eq(&optimized, &over_limit));
        Ok(())
    }

    #[test]
    fn rewrites_q21_join_chain_with_mixed_partition_modes() -> Result<()> {
        let plan = q21_join_plan()?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(3_790_000));
        let original_schema = plan.schema();

        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let mut s_name_types = vec![];
        collect_scan_field_types(&optimized, "s_name", &mut s_name_types);
        assert_eq!(
            s_name_types,
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            )]
        );
        Ok(())
    }

    #[test]
    fn q6_star_chain_requires_propagated_future_key() -> Result<()> {
        let plan = q6_star_join_plan(true)?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(3_790_000));
        let original_schema = plan.schema();

        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let mut d_name_types = vec![];
        collect_scan_field_types(&optimized, "d_name", &mut d_name_types);
        assert_eq!(
            d_name_types,
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8),
            )]
        );

        let missing_right_primary_key = q6_star_join_plan(false)?;
        assert_eq!(amplified_payload_rows(&missing_right_primary_key)?, None);
        let optimized = optimize(Arc::clone(&missing_right_primary_key))?;
        assert!(Arc::ptr_eq(&optimized, &missing_right_primary_key));
        Ok(())
    }

    #[test]
    fn restores_mixed_join_payload_before_partial_aggregate() -> Result<()> {
        let input = amplified_join_input(
            Some(Constraint::PrimaryKey(vec![0])),
            false,
            NullEquality::NullEqualsNothing,
            false,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
            100_000,
            (12, Some(10_000)),
        )?;
        let plan = partial_final_group_count_aggregate(input, 0)?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(100_000));
        let config = find_scan(&plan).expect("plan should contain its dimension scan");
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::JoinPayload,
        ));
        let original_schema = plan.schema();

        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let output = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("rewritten aggregate should preserve its output schema");
        let final_aggregate = output
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("output projection should consume the final aggregate");
        let partial_aggregate = final_aggregate
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("final aggregate should consume the partial aggregate");
        let restoration = partial_aggregate
            .input()
            .downcast_ref::<ProjectionExec>()
            .expect("join payload should be restored immediately before aggregation");
        let downstream_join = restoration
            .input()
            .downcast_ref::<HashJoinExec>()
            .expect("restoration should consume the downstream join");
        let anchor_join = downstream_join
            .left()
            .downcast_ref::<HashJoinExec>()
            .expect("downstream join should preserve the amplified join");
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        assert_eq!(
            downstream_join.schema().field(0).data_type(),
            &dictionary_type
        );
        assert_eq!(anchor_join.schema().field(0).data_type(), &dictionary_type);
        assert_eq!(
            restoration.schema().field(0).data_type(),
            &DataType::Utf8View
        );
        assert_eq!(
            partial_aggregate.input_schema().field(0).data_type(),
            &DataType::Utf8View
        );
        assert_eq!(
            final_aggregate.input_schema().field(0).data_type(),
            &DataType::Utf8View
        );
        let mut s_name_types = vec![];
        collect_scan_field_types(&optimized, "s_name", &mut s_name_types);
        assert_eq!(s_name_types, vec![dictionary_type]);
        Ok(())
    }

    #[test]
    fn skips_pure_distinct_join_payload_restoration() -> Result<()> {
        let input = amplified_join_input(
            Some(Constraint::PrimaryKey(vec![0])),
            false,
            NullEquality::NullEqualsNothing,
            false,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
            100_000,
            (12, Some(10_000)),
        )?;
        let plan = single_group_aggregate(input, 0)?;
        assert_eq!(amplified_payload_rows(&plan)?, Some(100_000));
        let config = find_scan(&plan).expect("plan should contain its dimension scan");
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::JoinPayload,
        ));

        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn nullable_unique_anchor_requires_nulls_not_equal() -> Result<()> {
        let nulls_equal = amplified_join_plan_with_anchor(
            Some(Constraint::Unique(vec![0])),
            true,
            NullEquality::NullEqualsNull,
            true,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
        )?;
        assert_eq!(amplified_payload_rows(&nulls_equal)?, None);
        let optimized = optimize(Arc::clone(&nulls_equal))?;
        assert!(Arc::ptr_eq(&optimized, &nulls_equal));

        let nulls_not_equal = amplified_join_plan_with_anchor(
            Some(Constraint::Unique(vec![0])),
            true,
            NullEquality::NullEqualsNothing,
            true,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
        )?;
        assert_eq!(amplified_payload_rows(&nulls_not_equal)?, Some(3_790_000));
        let optimized = optimize(nulls_not_equal)?;
        let scan = find_scan(&optimized).expect("optimized plan should retain its scan");
        assert_eq!(
            scan.file_schema().field(1).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        Ok(())
    }

    #[test]
    fn amplified_join_payload_gate_rejects_near_misses() -> Result<()> {
        for plan in [
            // The first join does not prove that its build key is unique.
            amplified_join_plan(
                false,
                true,
                PartitionMode::CollectLeft,
                JoinType::LeftSemi,
            )?,
            // The build side is partitioned instead of collected once.
            amplified_join_plan(
                true,
                true,
                PartitionMode::Partitioned,
                JoinType::LeftSemi,
            )?,
        ] {
            assert_eq!(amplified_payload_rows(&plan)?, None);
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "Q21-shaped near miss must not fall through to a broader join gate"
            );
        }

        // An ordinary downstream inner join invalidates the amplified path,
        // but must not activate the Q21-specific fail-closed branch. It can
        // still be considered by the generic joined-aggregate cost gate.
        let downstream_inner =
            amplified_join_plan(true, true, PartitionMode::CollectLeft, JoinType::Inner)?;
        assert_eq!(amplified_payload_rows(&downstream_inner)?, None);
        assert_eq!(group_column_count(&downstream_inner)?, 1);

        let not_fully_dictionary_encoded = amplified_join_plan(
            true,
            false,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
        )?;
        assert_eq!(
            amplified_payload_rows(&not_fully_dictionary_encoded)?,
            Some(3_790_000)
        );
        let config = find_scan(&not_fully_dictionary_encoded)
            .expect("Q21-shaped plan should contain its scan");
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(3_790_000),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        let optimized = optimize(Arc::clone(&not_fully_dictionary_encoded))?;
        assert!(Arc::ptr_eq(&optimized, &not_fully_dictionary_encoded));
        Ok(())
    }

    #[test]
    fn rewrites_unfiltered_inexact_join_payload_through_projection() -> Result<()> {
        let plan = join_aggregate_plan(JoinPlanShape::Safe)?;
        let original_schema = plan.schema();
        let optimized = optimize(Arc::clone(&plan))?;
        assert_eq!(optimized.schema(), original_schema);
        let mut data_types = vec![];
        collect_scan_field_types(&optimized, "group_key", &mut data_types);
        assert_eq!(
            data_types,
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )]
        );
        Ok(())
    }

    #[test]
    fn skips_fact_side_semi_join_payload() -> Result<()> {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("join_key", DataType::Int64, false),
            Field::new("group_key", DataType::Utf8View, false),
        ]));
        let fact = statistics_scan(
            fact_schema,
            10_000_000,
            &[Some(10_000_000), Some(100)],
            Some(candidate_physical_statistics(
                2,
                1,
                DataType::Utf8,
                24,
                MIN_VALUE_BYTES_PER_PARQUET_BYTE,
            )),
            Constraints::default(),
        );
        let dimension = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "join_key",
                DataType::Int64,
                false,
            )])),
            1_000_000,
            &[Some(1_000_000)],
            None,
            Constraints::default(),
        );
        let semi_join = Arc::new(HashJoinExec::try_new(
            fact,
            dimension,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("join_key", 0)),
            )],
            None,
            &JoinType::LeftSemi,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let config = ConfigOptions::new();
        let context = ConfigOnlyContext::new(&config);
        assert_eq!(
            context
                .compute_statistics(&semi_join, &StatisticsArgs::new())?
                .num_rows,
            Precision::Inexact(1_000_000)
        );

        let plan = single_group_aggregate(semi_join, 1)?;
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn downstream_join_key_requires_fourfold_aggregate_reduction() -> Result<()> {
        assert!(DictionaryPreservation::has_minimum_aggregate_reduction(
            &Precision::Inexact(100),
            &Precision::Inexact(25),
        ));
        assert!(!DictionaryPreservation::has_minimum_aggregate_reduction(
            &Precision::Inexact(100),
            &Precision::Inexact(26),
        ));
        assert!(!DictionaryPreservation::has_minimum_aggregate_reduction(
            &Precision::Inexact(0),
            &Precision::Inexact(0),
        ));
        assert!(!DictionaryPreservation::has_minimum_aggregate_reduction(
            &Precision::Inexact(100),
            &Precision::Inexact(0),
        ));

        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for (payload_distinct_count, join_output_index, expected_type) in [
            (None, 0, DataType::Utf8View),
            (Some(10_000), 0, dictionary_type.clone()),
            (None, 1, dictionary_type),
        ] {
            let input = amplified_join_input(
                Some(Constraint::PrimaryKey(vec![0])),
                false,
                NullEquality::NullEqualsNothing,
                true,
                PartitionMode::CollectLeft,
                JoinType::LeftSemi,
                3_790_000,
                (12, payload_distinct_count),
            )?;
            let aggregate = partial_final_group_count_aggregate(input, 0)?;
            assert_eq!(amplified_payload_rows(&aggregate)?, Some(3_790_000));
            let plan = join_on_reordered_aggregate_output(aggregate, join_output_index)?;
            let optimized = optimize(plan)?;
            let mut s_name_types = vec![];
            collect_scan_field_types(&optimized, "s_name", &mut s_name_types);
            assert_eq!(s_name_types, vec![expected_type]);
        }
        Ok(())
    }

    #[test]
    fn wide_join_payload_restoration_requires_fourfold_reduction() -> Result<()> {
        let input = amplified_join_input(
            Some(Constraint::PrimaryKey(vec![0])),
            false,
            NullEquality::NullEqualsNothing,
            false,
            PartitionMode::CollectLeft,
            JoinType::LeftSemi,
            100_000,
            (UTF8_VIEW_SLOT_BYTES, None),
        )?;
        let plan = partial_final_group_count_aggregate(input, 0)?;
        let config = find_scan(&plan).expect("plan should contain its dimension scan");
        assert!(DictionaryPreservation::has_wide_file_values(config, 1));
        assert!(!DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::AmplifiedJoinPayload,
        ));
        assert!(DictionaryPreservation::is_profitable(
            config,
            1,
            Some(100_000),
            DictionaryUse::JoinPayload,
        ));
        let optimized = optimize(Arc::clone(&plan))?;
        assert!(Arc::ptr_eq(&optimized, &plan));
        Ok(())
    }

    #[test]
    fn partial_downstream_join_key_does_not_gate_aggregate() -> Result<()> {
        let aggregate = aggregate_plan(
            &[240_000_000, 240_000_000],
            &[Some(12_000_000), Some(12_000_000)],
            true,
            true,
        )?;
        let peer = statistics_scan(
            Arc::new(Schema::new(vec![Field::new(
                "join_key",
                DataType::Utf8View,
                false,
            )])),
            10_000,
            &[None],
            None,
            Constraints::default(),
        );
        let plan = Arc::new(HashJoinExec::try_new(
            peer,
            aggregate,
            vec![(
                Arc::new(Column::new("join_key", 0)),
                Arc::new(Column::new("key_0", 0)),
            )],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?) as Arc<dyn ExecutionPlan>;
        let optimized = optimize(plan)?;
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        for field_name in ["key_0", "key_1"] {
            let mut data_types = vec![];
            collect_scan_field_types(&optimized, field_name, &mut data_types);
            assert_eq!(data_types, vec![dictionary_type.clone()]);
        }
        Ok(())
    }

    #[test]
    fn rewrites_unfiltered_inexact_right_join_payload() -> Result<()> {
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
        let original_schema = plan.schema();
        let optimized = optimize(Arc::clone(&plan))?;
        assert_eq!(optimized.schema(), original_schema);
        let mut data_types = vec![];
        collect_scan_field_types(&optimized, "group_key", &mut data_types);
        assert_eq!(
            data_types,
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )]
        );
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
    fn rewrites_left_join_preserved_payload() -> Result<()> {
        let plan = join_aggregate_plan(JoinPlanShape::LeftJoin)?;
        let original_schema = plan.schema();

        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let mut data_types = vec![];
        collect_scan_field_types(&optimized, "group_key", &mut data_types);
        assert_eq!(
            data_types,
            vec![DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )]
        );
        Ok(())
    }

    #[test]
    fn rewrites_only_selected_occurrence_of_shared_scan() -> Result<()> {
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

        let original_schema = plan.schema();
        let optimized = optimize(plan)?;
        assert_eq!(optimized.schema(), original_schema);
        let mut data_types = vec![];
        collect_scan_field_types(&optimized, "group_key", &mut data_types);
        assert_eq!(
            data_types,
            vec![
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                DataType::Utf8View,
            ]
        );
        Ok(())
    }
}
