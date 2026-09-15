// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Preserve profitable file-native dictionary encodings through hash aggregation.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{JoinSide, JoinType, PhysicalFileStatistics, Result, Statistics};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::statistics::StatisticsArgs;
use datafusion_physical_plan::{
    ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions,
};

use crate::PhysicalOptimizerRule;
use crate::optimizer::{ConfigOnlyContext, PhysicalOptimizerContext};

// Utf8View stores values up to 12 bytes inline. Require values to clear that
// fast path with enough headroom for length variance and array metadata.
const MIN_ROW_GROUP_VALUE_BYTES_PER_ROW: usize = 16;
// Require the decoded values to be substantially larger than the uncompressed
// Parquet dictionary representation.
const MIN_VALUE_BYTES_PER_PARQUET_BYTE: usize = 4;
// Avoid changing representation when too little string data reaches the
// aggregate for cheaper key hashing to repay the scan and output conversions.
const MIN_AGGREGATE_VALUE_BYTES: usize = 8 * 1024 * 1024;

/// Rewrites eligible string GROUP BY columns to use dictionaries emitted
/// natively by their file source.
#[derive(Debug, Default)]
pub struct DictionaryAggregation {}

#[derive(Debug, Clone)]
struct GroupColumn {
    group_index: usize,
    aggregate_input_index: usize,
    scan: Arc<dyn ExecutionPlan>,
    table_index: usize,
    scan_path: Vec<usize>,
}

#[derive(Debug, Clone)]
struct ScanTarget {
    scan: Arc<dyn ExecutionPlan>,
    table_index: usize,
    scan_path: Vec<usize>,
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

impl DictionaryAggregation {
    /// Create a new [`DictionaryAggregation`] optimizer rule.
    pub fn new() -> Self {
        Self {}
    }

    fn try_rewrite(
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
        let Some(&aggregate_input_rows) = statistics.num_rows.get_value() else {
            return Ok(None);
        };

        let group_columns: Vec<_> = group_columns
            .into_iter()
            .filter(|column| {
                let scan = column
                    .scan
                    .downcast_ref::<DataSourceExec>()
                    .expect("group column lineage ends at a data source");
                let config = scan
                    .data_source()
                    .downcast_ref::<FileScanConfig>()
                    .expect("group column lineage ends at a file scan");
                config.file_source.filter().is_none()
                    && config.limit.is_none()
                    && Self::is_profitable(
                        config,
                        column.table_index,
                        aggregate_input_rows,
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
        if Self::aggregate_references_columns(raw_agg, &encoded_input_indices) {
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
            if Self::aggregate_references_columns(final_agg, &final_group_indices)
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

        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            // Keep the initial policy to pure inner equi-joins, which are the
            // only join shape covered by the benchmark evidence.
            if join.join_type() != &JoinType::Inner || join.filter().is_some() {
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
            return Ok(target.map(|target| target.prepend_child(child_index)));
        }

        // Filters change how many rows reach the aggregate, so scan-wide file
        // statistics are not sufficient to prove that dictionary grouping is
        // profitable. Repartitions and every other unrecognized node are also
        // rejected until their expression and reconstruction contracts are
        // covered explicitly.
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
        } else {
            Arc::clone(plan).replace_children(
                new_children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?
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
    ) -> bool {
        let references_candidate = |expr: &Arc<dyn PhysicalExpr>| {
            collect_columns(expr)
                .iter()
                .any(|column| column_indices.contains(&column.index()))
        };
        aggregate.aggr_expr().iter().any(|aggregate| {
            aggregate.expressions().iter().any(references_candidate)
                || aggregate
                    .order_bys()
                    .iter()
                    .any(|order| references_candidate(&order.expr))
        }) || aggregate
            .filter_expr()
            .iter()
            .flatten()
            .any(references_candidate)
    }

    fn is_profitable(
        config: &FileScanConfig,
        table_index: usize,
        aggregate_input_rows: usize,
    ) -> bool {
        // Custom expression adapters can change values relative to the file
        // statistics used by this cost gate.
        if config.expr_adapter_factory.is_some() {
            return false;
        }
        let mut saw_file = false;
        let mut min_value_bytes_per_row = usize::MAX;
        for file in config.file_groups.iter().flat_map(|group| group.iter()) {
            if file.range.is_some()
                || file.arrow_schema.is_some()
                || file.extensions.len() != 1
            {
                return false;
            }
            saw_file = true;
            let Some(physical) = file.extension::<PhysicalFileStatistics>() else {
                return false;
            };
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
                || value_bytes_per_row < MIN_ROW_GROUP_VALUE_BYTES_PER_ROW
                || column
                    .min_row_group_value_bytes_per_encoded_byte
                    .is_none_or(|ratio| ratio < MIN_VALUE_BYTES_PER_PARQUET_BYTE)
            {
                return false;
            }
            min_value_bytes_per_row = min_value_bytes_per_row.min(value_bytes_per_row);
        }
        saw_file
            && aggregate_input_rows
                .checked_mul(min_value_bytes_per_row)
                .is_some_and(|bytes| bytes >= MIN_AGGREGATE_VALUE_BYTES)
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

impl PhysicalOptimizerRule for DictionaryAggregation {
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
            .enable_dictionary_aggregation
        {
            return Ok(plan);
        }
        plan.transform_down(|plan| {
            Ok(match Self::try_rewrite(&plan, context)? {
                Some(rewritten) => Transformed::yes(rewritten),
                None => Transformed::no(plan),
            })
        })
        .data()
    }

    fn name(&self) -> &str {
        "DictionaryAggregation"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion_common::{
        ColumnStatistics, NullEquality, PhysicalColumnStatistics, ScalarValue, Statistics,
    };
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_execution::object_store::ObjectStoreUrl;
    use datafusion_expr::test::function_stub::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::IsNotNullExpr;
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
        config.optimizer.enable_dictionary_aggregation = true;
        DictionaryAggregation::new().optimize(plan, &config)
    }

    fn candidate_physical_statistics(
        column_count: usize,
        candidate_index: usize,
        native_value_type: DataType,
        value_bytes_per_row: usize,
        value_bytes_per_parquet_byte: usize,
    ) -> PhysicalFileStatistics {
        PhysicalFileStatistics {
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
        let join = Arc::new(HashJoinExec::try_new(
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
        )?) as Arc<dyn ExecutionPlan>;

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

    fn find_scan_for_file<'a>(
        plan: &'a Arc<dyn ExecutionPlan>,
        path: &str,
    ) -> Option<&'a FileScanConfig> {
        if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
            && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
            && config
                .file_groups
                .iter()
                .flat_map(|group| group.iter())
                .any(|file| file.path().as_ref() == path)
        {
            return Some(config);
        }
        plan.children()
            .into_iter()
            .find_map(|child| find_scan_for_file(child, path))
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
            &[Some(12_000_000), Some(4_000_000)],
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
    fn skips_when_cost_or_storage_evidence_is_incomplete() -> Result<()> {
        let cases = [
            // Utf8View's short-value inline representation is faster.
            (80_000_000, Some(4_000_000), true, true),
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
            DictionaryAggregation::new().optimize(Arc::clone(&plan), &config)?;
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
        let rows_at_threshold =
            MIN_AGGREGATE_VALUE_BYTES / MIN_ROW_GROUP_VALUE_BYTES_PER_ROW;

        assert!(!DictionaryAggregation::is_profitable(
            config,
            0,
            rows_at_threshold - 1
        ));
        assert!(DictionaryAggregation::is_profitable(
            config,
            0,
            rows_at_threshold
        ));
        assert!(!DictionaryAggregation::is_profitable(config, 0, usize::MAX));

        let narrow = candidate_file(
            "narrow.parquet",
            1,
            0,
            DataType::Utf8,
            MIN_ROW_GROUP_VALUE_BYTES_PER_ROW - 1,
            MIN_VALUE_BYTES_PER_PARQUET_BYTE,
        );
        let scan = scan_plan(schema, vec![narrow], None, None, None)?;
        assert!(!DictionaryAggregation::is_profitable(
            find_scan(&scan).expect("test scan should use FileScanConfig"),
            0,
            rows_at_threshold
        ));
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
    fn skips_files_with_reader_overrides_ranges_or_extra_extensions() -> Result<()> {
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
        let with_range = candidate_file("range.parquet", 1, 0, DataType::Utf8, 24, 6)
            .with_range(0, 1024);

        for file in [with_extra_extension, with_arrow_schema, with_range] {
            let scan = scan_plan(Arc::clone(&schema), vec![file], None, None, None)?;
            let plan = single_group_aggregate(scan, 0)?;
            let optimized = optimize(Arc::clone(&plan))?;
            assert!(
                Arc::ptr_eq(&optimized, &plan),
                "reader-specific file behavior must fail closed"
            );
        }
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
    fn skips_scan_predicate_and_limit() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Utf8View,
            false,
        )]));
        let predicate =
            Arc::new(IsNotNullExpr::new(Arc::new(Column::new("group_key", 0))))
                as Arc<dyn PhysicalExpr>;
        let cases = [(Some(predicate), None), (None, Some(100))];

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
    fn rewrites_join_payload_through_projection() -> Result<()> {
        let plan = join_aggregate_plan(JoinPlanShape::Safe)?;
        let original_schema = plan.schema();
        let optimized = optimize(plan)?;

        assert_eq!(optimized.schema(), original_schema);
        let scan = find_scan(&optimized).expect("rewritten plan should contain scan");
        assert_eq!(
            scan.file_schema().field(1).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        let optimized_again = optimize(Arc::clone(&optimized))?;
        assert!(Arc::ptr_eq(&optimized_again, &optimized));
        Ok(())
    }

    #[test]
    fn rewrites_right_join_payload() -> Result<()> {
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
        let optimized = optimize(plan)?;

        let left = find_scan_for_file(&optimized, "left.parquet")
            .expect("left input should remain a scan");
        assert_eq!(left.file_schema().field(0).data_type(), &DataType::Int64);
        let right = find_scan_for_file(&optimized, "data.parquet")
            .expect("right input should remain a scan");
        assert_eq!(
            right.file_schema().field(1).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
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
    fn rewrites_only_traced_use_of_shared_scan() -> Result<()> {
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

        let optimized = optimize(plan)?;
        let restore = optimized
            .downcast_ref::<ProjectionExec>()
            .expect("rewrite should restore the output schema");
        let aggregate = restore
            .input()
            .downcast_ref::<AggregateExec>()
            .expect("projection input should be aggregate");
        let join = aggregate
            .input()
            .downcast_ref::<HashJoinExec>()
            .expect("aggregate input should be join");
        let left = join
            .left()
            .downcast_ref::<DataSourceExec>()
            .expect("left input should be scan");
        let right = join
            .right()
            .downcast_ref::<DataSourceExec>()
            .expect("right input should be scan");
        let left = left
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("left input should be file scan");
        let right = right
            .data_source()
            .downcast_ref::<FileScanConfig>()
            .expect("right input should be file scan");
        assert!(matches!(
            left.file_schema().field(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(
            right.file_schema().field(1).data_type(),
            &DataType::Utf8View
        );
        Ok(())
    }
}
