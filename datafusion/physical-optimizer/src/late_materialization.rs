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

//! Row-number based late materialization for simple TopK plans.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt32Array, UInt64Array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{FileRowsSelection, PartitionedFile};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::execution_plan::{
    Boundedness, EmissionType, collect, reset_plan_states,
};
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    Statistics,
};
use futures::{StreamExt, TryStreamExt, stream};

use crate::PhysicalOptimizerRule;

const PARTITION_COLUMN: &str = "__datafusion_late_materialization_partition";
const ROW_NUMBER_COLUMN: &str = "__datafusion_late_materialization_row_number";

/// Rewrites simple TopK plans to sort a narrow key-only stream and materialize
/// the full rows after the winning row numbers are known.
#[derive(Default, Debug)]
pub struct LateMaterialization {}

impl LateMaterialization {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for LateMaterialization {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !config.optimizer.enable_row_number_topk_late_materialization {
            return Ok(plan);
        }

        plan.transform_down(|plan| {
            if let Some(spm) = plan.downcast_ref::<SortPreservingMergeExec>()
                && let Some(fetch) = spm.fetch()
                && let Some(sort_child) = spm.input().downcast_ref::<SortExec>()
                && sort_child.preserve_partitioning()
                && let Some(exec) = LateTopKMaterializationExec::try_new(
                    sort_child.input(),
                    spm.expr().clone(),
                    fetch,
                )?
            {
                return Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>));
            }

            let Some(sort) = plan.downcast_ref::<SortExec>() else {
                return Ok(Transformed::no(plan));
            };
            let Some(fetch) = sort.fetch() else {
                return Ok(Transformed::no(plan));
            };
            if sort.preserve_partitioning() {
                return Ok(Transformed::no(plan));
            }

            if let Some(exec) = LateTopKMaterializationExec::try_new(
                sort.input(),
                sort.expr().clone(),
                fetch,
            )? {
                Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "LateMaterialization"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
struct LateTopKMaterializationExec {
    key_input: Arc<dyn ExecutionPlan>,
    full_input: Arc<dyn ExecutionPlan>,
    sort_exprs: LexOrdering,
    key_sort_exprs: LexOrdering,
    fetch: usize,
    key_width: usize,
    cache: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl LateTopKMaterializationExec {
    fn try_new(
        input: &Arc<dyn ExecutionPlan>,
        sort_exprs: LexOrdering,
        fetch: usize,
    ) -> Result<Option<Self>> {
        if fetch == 0 {
            return Ok(None);
        }

        let Some(full_input) = input.with_preserve_order(true) else {
            return Ok(None);
        };

        let key_columns = simple_key_columns(&sort_exprs)?;
        if key_columns.is_empty() {
            return Ok(None);
        }
        let input_schema = full_input.schema();
        if key_columns.len() >= input_schema.fields().len() {
            return Ok(None);
        }

        let projection_exprs = key_columns
            .iter()
            .map(|column| {
                (
                    Arc::new(Column::new(column.name(), column.index()))
                        as Arc<dyn PhysicalExpr>,
                    column.name().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let projection =
            ProjectionExec::try_new(projection_exprs, Arc::clone(&full_input))?;

        let Some(key_input) = full_input.try_swapping_with_projection(&projection)?
        else {
            return Ok(None);
        };
        let Some(key_input) = key_input.with_preserve_order(true) else {
            return Ok(None);
        };

        let key_sort_exprs = remap_sort_exprs(&sort_exprs, &key_columns)?;
        let cache = Self::compute_properties(&full_input, &sort_exprs);

        Ok(Some(Self {
            key_input,
            full_input,
            sort_exprs,
            key_sort_exprs,
            fetch,
            key_width: key_columns.len(),
            cache: Arc::new(cache),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn compute_properties(
        full_input: &Arc<dyn ExecutionPlan>,
        sort_exprs: &LexOrdering,
    ) -> PlanProperties {
        let schema = full_input.schema();
        let eq_properties =
            EquivalenceProperties::new_with_orderings(schema, vec![sort_exprs.to_vec()]);
        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        )
    }

    fn key_topk_plan(&self) -> Arc<dyn ExecutionPlan> {
        let key_input = Arc::new(RowNumberExec::new(Arc::clone(&self.key_input)))
            as Arc<dyn ExecutionPlan>;
        let key_input = if key_input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(key_input)) as Arc<dyn ExecutionPlan>
        } else {
            key_input
        };
        Arc::new(
            SortExec::new(self.key_sort_exprs.clone(), key_input)
                .with_fetch(Some(self.fetch)),
        )
    }
}

impl DisplayAs for LateTopKMaterializationExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LateTopKMaterializationExec: fetch={}, expr=[{}]",
                    self.fetch, self.sort_exprs
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "fetch={}", self.fetch)?;
                write!(f, "expr=[{}]", self.sort_exprs)
            }
        }
    }
}

impl ExecutionPlan for LateTopKMaterializationExec {
    fn name(&self) -> &str {
        "LateTopKMaterializationExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.key_input, &self.full_input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return internal_err!(
                "LateTopKMaterializationExec requires exactly two children"
            );
        }
        let mut new_exec = Arc::unwrap_or_clone(self);
        new_exec.key_input = Arc::clone(&children[0]);
        new_exec.full_input = Arc::clone(&children[1]);
        new_exec.cache = Arc::new(Self::compute_properties(
            &new_exec.full_input,
            &new_exec.sort_exprs,
        ));
        Ok(Arc::new(new_exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "LateTopKMaterializationExec invalid partition {partition}"
            );
        }

        let key_topk_plan = reset_plan_states(self.key_topk_plan())?;
        let full_input = reset_plan_states(Arc::clone(&self.full_input))?;
        let output_schema = self.schema();
        let key_width = self.key_width;
        let context = Arc::clone(&context);

        let batches = stream::once(async move {
            let selected_rows =
                collect_selected_rows(key_topk_plan, Arc::clone(&context), key_width)
                    .await?;
            if let Some(batches) = materialize_with_pushed_down_file_rows(
                &full_input,
                Arc::clone(&context),
                Arc::clone(&output_schema),
                &selected_rows,
            )
            .await?
            {
                return Ok(batches);
            }
            materialize_selected_rows(full_input, context, output_schema, &selected_rows)
                .await
        })
        .map_ok(|batches| stream::iter(batches.into_iter().map(Ok)))
        .try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            batches,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_some() {
            return Ok(Arc::new(Statistics::new_unknown(&self.schema())));
        }
        Ok(Arc::new(
            self.full_input
                .partition_statistics(None)?
                .as_ref()
                .clone()
                .with_fetch(Some(self.fetch), 0, 1)?,
        ))
    }
}

#[derive(Debug, Clone)]
struct RowNumberExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl RowNumberExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let input_schema = input.schema();
        let mut fields = input_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            PARTITION_COLUMN,
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(
            ROW_NUMBER_COLUMN,
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        );
        Self {
            input,
            cache: Arc::new(cache),
        }
    }
}

impl DisplayAs for RowNumberExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RowNumberExec")
            }
            DisplayFormatType::TreeRender => write!(f, "RowNumberExec"),
        }
    }
}

impl ExecutionPlan for RowNumberExec {
    fn name(&self) -> &str {
        "RowNumberExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("RowNumberExec requires exactly one child");
        }
        Ok(Arc::new(Self::new(Arc::clone(&children[0]))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let mut next_row_number = 0_u64;
        let stream = input.map(move |batch| {
            let batch = batch?;
            let row_count = batch.num_rows();
            let end_row_number = next_row_number + row_count as u64;
            let partition_values = UInt64Array::from_value(partition as u64, row_count);
            let row_numbers =
                UInt64Array::from_iter_values(next_row_number..end_row_number);
            next_row_number = end_row_number;

            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(partition_values) as ArrayRef);
            columns.push(Arc::new(row_numbers) as ArrayRef);
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        if partition.is_some() {
            return Ok(Arc::new(Statistics::new_unknown(&self.schema())));
        }
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}

#[derive(Debug)]
struct SelectedRows {
    by_partition: HashMap<usize, BTreeMap<u64, usize>>,
    row_count: usize,
}

async fn collect_selected_rows(
    key_topk_plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    key_width: usize,
) -> Result<SelectedRows> {
    let batches = collect(key_topk_plan, context).await?;
    let partition_index = key_width;
    let row_number_index = key_width + 1;

    let mut by_partition: HashMap<usize, BTreeMap<u64, usize>> = HashMap::new();
    let mut rank = 0_usize;
    for batch in batches {
        let partition_array = batch
            .column(partition_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "late materialization partition column had wrong type".to_string(),
                )
            })?;
        let row_number_array = batch
            .column(row_number_index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "late materialization row number column had wrong type".to_string(),
                )
            })?;

        for row in 0..batch.num_rows() {
            let partition =
                usize::try_from(partition_array.value(row)).map_err(|_| {
                    DataFusionError::Internal(
                        "late materialization partition value overflowed usize"
                            .to_string(),
                    )
                })?;
            let row_number = row_number_array.value(row);
            by_partition
                .entry(partition)
                .or_default()
                .insert(row_number, rank);
            rank += 1;
        }
    }

    Ok(SelectedRows {
        by_partition,
        row_count: rank,
    })
}

async fn materialize_selected_rows(
    full_input: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
    selected_rows: &SelectedRows,
) -> Result<Vec<RecordBatch>> {
    if selected_rows.row_count == 0 {
        return Ok(vec![]);
    }

    let partition_count = full_input.output_partitioning().partition_count();
    let mut selected_batches = Vec::new();
    let mut selected_ranks = Vec::with_capacity(selected_rows.row_count);

    for (partition, rows) in &selected_rows.by_partition {
        if *partition >= partition_count {
            return Err(DataFusionError::Internal(format!(
                "late materialization selected partition {partition}, but full input has {partition_count} partitions"
            )));
        }
        let mut stream = full_input.execute(*partition, Arc::clone(&context))?;
        let mut next_row_number = 0_u64;
        let mut found_in_partition = 0_usize;
        let rows_to_find = rows.len();
        let max_row_number = rows.keys().next_back().copied();

        while let Some(batch) = stream.next().await {
            let batch = batch?;
            let batch_start = next_row_number;
            let batch_end = batch_start + batch.num_rows() as u64;

            let mut indices = Vec::new();
            let mut ranks = Vec::new();
            for (row_number, rank) in rows.range(batch_start..batch_end) {
                indices.push(u32::try_from(*row_number - batch_start).map_err(|_| {
                    DataFusionError::Internal(
                        "late materialization batch row index overflowed u32".to_string(),
                    )
                })?);
                ranks.push(*rank);
            }

            if !indices.is_empty() {
                let indices = UInt32Array::from(indices);
                let columns = batch
                    .columns()
                    .iter()
                    .map(|column| take(column.as_ref(), &indices, None))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                selected_batches
                    .push(RecordBatch::try_new(Arc::clone(&schema), columns)?);
                found_in_partition += ranks.len();
                selected_ranks.extend(ranks);
            }

            next_row_number = batch_end;
            if found_in_partition >= rows_to_find
                || max_row_number.is_some_and(|max| next_row_number > max)
            {
                break;
            }
        }
    }

    if selected_ranks.len() != selected_rows.row_count {
        return Err(DataFusionError::Internal(format!(
            "late materialization found {} of {} selected rows",
            selected_ranks.len(),
            selected_rows.row_count
        )));
    }

    reorder_selected_batches(
        schema,
        &selected_batches,
        selected_ranks,
        selected_rows.row_count,
    )
}

async fn materialize_with_pushed_down_file_rows(
    full_input: &Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    schema: SchemaRef,
    selected_rows: &SelectedRows,
) -> Result<Option<Vec<RecordBatch>>> {
    if selected_rows.row_count == 0 {
        return Ok(Some(vec![]));
    }

    let Some(selected_input) = selected_file_scan(full_input, selected_rows)? else {
        return Ok(None);
    };

    let partition_count = selected_input.output_partitioning().partition_count();
    let mut selected_batches = Vec::new();
    let mut selected_ranks = Vec::with_capacity(selected_rows.row_count);

    for partition in 0..partition_count {
        let Some(rows) = selected_rows.by_partition.get(&partition) else {
            continue;
        };

        let mut stream = selected_input.execute(partition, Arc::clone(&context))?;
        let mut partition_row_count = 0_usize;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            partition_row_count += batch.num_rows();
            selected_batches.push(batch);
        }

        if partition_row_count != rows.len() {
            return Err(DataFusionError::Internal(format!(
                "late materialization pushed-down file row selection returned {partition_row_count} rows for partition {partition}, expected {}",
                rows.len()
            )));
        }
        selected_ranks.extend(rows.values().copied());
    }

    reorder_selected_batches(
        schema,
        &selected_batches,
        selected_ranks,
        selected_rows.row_count,
    )
    .map(Some)
}

fn reorder_selected_batches(
    schema: SchemaRef,
    selected_batches: &[RecordBatch],
    selected_ranks: Vec<usize>,
    row_count: usize,
) -> Result<Vec<RecordBatch>> {
    if row_count == 0 {
        return Ok(vec![]);
    }

    let concatenated = concat_batches(&schema, selected_batches)?;
    let mut rank_to_row = selected_ranks
        .into_iter()
        .enumerate()
        .map(|(row_index, rank)| (rank, row_index))
        .collect::<Vec<_>>();
    rank_to_row.sort_by_key(|(rank, _)| *rank);
    let take_indices = rank_to_row
        .into_iter()
        .map(|(_, row_index)| {
            u32::try_from(row_index).map_err(|_| {
                DataFusionError::Internal(
                    "late materialization output row index overflowed u32".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let take_indices = UInt32Array::from(take_indices);
    let columns = concatenated
        .columns()
        .iter()
        .map(|column| take(column.as_ref(), &take_indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(vec![RecordBatch::try_new(schema, columns)?])
}

fn selected_file_scan(
    full_input: &Arc<dyn ExecutionPlan>,
    selected_rows: &SelectedRows,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(data_source_exec) = full_input.downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(file_scan_config) = data_source_exec
        .data_source()
        .downcast_ref::<FileScanConfig>()
    else {
        return Ok(None);
    };

    if file_scan_config.file_source().file_type() != "parquet"
        || file_scan_config.file_source().filter().is_some()
        || file_scan_config.limit.is_some()
    {
        return Ok(None);
    }

    let mut selected_file_groups = Vec::with_capacity(file_scan_config.file_groups.len());
    for (partition, file_group) in file_scan_config.file_groups.iter().enumerate() {
        let selected_files = match selected_rows.by_partition.get(&partition) {
            Some(rows) => {
                let Some(selected_files) =
                    selected_files_for_partition(file_group, rows)?
                else {
                    return Ok(None);
                };
                selected_files
            }
            None => Vec::new(),
        };
        selected_file_groups.push(FileGroup::new(selected_files));
    }

    let selected_config = FileScanConfigBuilder::from(file_scan_config.clone())
        .with_file_groups(selected_file_groups)
        .build();
    Ok(Some(Arc::new(DataSourceExec::new(Arc::new(
        selected_config,
    )))))
}

fn selected_files_for_partition(
    file_group: &FileGroup,
    rows: &BTreeMap<u64, usize>,
) -> Result<Option<Vec<PartitionedFile>>> {
    if rows.is_empty() {
        return Ok(Some(vec![]));
    }

    let mut selected_files = Vec::new();
    let mut file_start = 0_u64;
    let max_row_number = *rows.keys().next_back().expect("rows is not empty");

    for file in file_group.iter() {
        if file_start > max_row_number {
            break;
        }

        if file.range.is_some() || !file.extensions.is_empty() {
            return Ok(None);
        }

        let Some(file_row_count) = exact_file_row_count(file)? else {
            return Ok(None);
        };
        let file_end = file_start.checked_add(file_row_count).ok_or_else(|| {
            DataFusionError::Internal(
                "late materialization file row count overflowed u64".to_string(),
            )
        })?;

        let selected_file_rows = rows
            .range(file_start..file_end)
            .map(|(row_number, _)| row_number - file_start)
            .collect::<Vec<_>>();
        if !selected_file_rows.is_empty() {
            selected_files.push(
                file.clone()
                    .with_extension(FileRowsSelection::new(selected_file_rows)),
            );
        }

        file_start = file_end;
    }

    if max_row_number >= file_start {
        return Err(DataFusionError::Internal(format!(
            "late materialization selected row {max_row_number}, but partition has {file_start} known file rows"
        )));
    }

    Ok(Some(selected_files))
}

fn exact_file_row_count(file: &PartitionedFile) -> Result<Option<u64>> {
    let Some(statistics) = &file.statistics else {
        return Ok(None);
    };
    let Precision::Exact(row_count) = &statistics.num_rows else {
        return Ok(None);
    };
    u64::try_from(*row_count).map(Some).map_err(|_| {
        DataFusionError::Internal(
            "late materialization exact file row count overflowed u64".to_string(),
        )
    })
}

fn simple_key_columns(sort_exprs: &LexOrdering) -> Result<Vec<Column>> {
    let mut seen = HashSet::new();
    let mut columns = Vec::new();
    for sort_expr in sort_exprs {
        let Some(column) = sort_expr.expr.downcast_ref::<Column>() else {
            return Ok(vec![]);
        };
        if seen.insert(column.index()) {
            columns.push(column.clone());
        }
    }
    Ok(columns)
}

fn remap_sort_exprs(
    sort_exprs: &LexOrdering,
    key_columns: &[Column],
) -> Result<LexOrdering> {
    let index_map = key_columns
        .iter()
        .enumerate()
        .map(|(key_index, column)| {
            (column.index(), (key_index, column.name().to_string()))
        })
        .collect::<HashMap<_, _>>();

    let sort_exprs = sort_exprs
        .iter()
        .map(|sort_expr| {
            let column = sort_expr
                .expr
                .downcast_ref::<Column>()
                .expect("validated by simple_key_columns");
            let (key_index, name) = index_map.get(&column.index()).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "sort column {column} was missing from late materialization key projection"
                ))
            })?;
            Ok(PhysicalSortExpr {
                expr: Arc::new(Column::new(name, *key_index)),
                options: sort_expr.options,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LexOrdering::new(sort_exprs).expect("sort exprs are not empty"))
}
