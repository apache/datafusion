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

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::fmt::Debug;
use std::ops::Deref;
use std::slice::from_ref;
use std::sync::Arc;

use crate::sink::DataSink;
use crate::source::{DataSource, DataSourceExec};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{
    assert_or_internal_err, plan_err, project_schema, Result, ScalarValue,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::project_orderings;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::projection::{
    all_alias_free_columns, new_projections_for_columns,
};
use datafusion_physical_plan::{
    common, ColumnarValue, DisplayAs, DisplayFormatType, Partitioning, PhysicalExpr,
    SendableRecordBatchStream, Statistics,
};

use async_trait::async_trait;
use datafusion_physical_plan::coop::cooperative;
use datafusion_physical_plan::execution_plan::SchedulingType;
use futures::StreamExt;
use itertools::Itertools;
use tokio::sync::RwLock;

/// Data source configuration for reading in-memory batches of data
#[derive(Clone, Debug)]
pub struct MemorySourceConfig {
    /// The partitions to query.
    ///
    /// Each partition is a `Vec<RecordBatch>`.
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    /// Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    /// if partition sizes should be displayed
    show_sizes: bool,
    /// The maximum number of records to read from this plan. If `None`,
    /// all records after filtering are returned.
    fetch: Option<usize>,
}

impl DataSource for MemorySourceConfig {
    fn open(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(cooperative(
            MemoryStream::try_new(
                self.partitions[partition].clone(),
                Arc::clone(&self.projected_schema),
                self.projection.clone(),
            )?
            .with_fetch(self.fetch),
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_sizes: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();

                let output_ordering = self
                    .sort_information
                    .first()
                    .map(|output_ordering| format!(", output_ordering={output_ordering}"))
                    .unwrap_or_default();

                let eq_properties = self.eq_properties();
                let constraints = eq_properties.constraints();
                let constraints = if constraints.is_empty() {
                    String::new()
                } else {
                    format!(", {constraints}")
                };

                let limit = self
                    .fetch
                    .map_or(String::new(), |limit| format!(", fetch={limit}"));
                if self.show_sizes {
                    write!(
                                f,
                                "partitions={}, partition_sizes={partition_sizes:?}{limit}{output_ordering}{constraints}",
                                partition_sizes.len(),
                            )
                } else {
                    write!(
                        f,
                        "partitions={}{limit}{output_ordering}{constraints}",
                        partition_sizes.len(),
                    )
                }
            }
            DisplayFormatType::TreeRender => {
                let total_rows = self.partitions.iter().map(|b| b.len()).sum::<usize>();
                let total_bytes: usize = self
                    .partitions
                    .iter()
                    .flatten()
                    .map(|batch| batch.get_array_memory_size())
                    .sum();
                writeln!(f, "format=memory")?;
                writeln!(f, "rows={total_rows}")?;
                writeln!(f, "bytes={total_bytes}")?;
                Ok(())
            }
        }
    }

    /// If possible, redistribute batches across partitions according to their size.
    ///
    /// Returns `Ok(None)` if unable to repartition. Preserve output ordering if exists.
    /// Refer to [`DataSource::repartitioned`] for further details.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        if self.partitions.is_empty() || self.partitions.len() >= target_partitions
        // if have no partitions, or already have more partitions than desired, do not repartition
        {
            return Ok(None);
        }

        let maybe_repartitioned = if let Some(output_ordering) = output_ordering {
            self.repartition_preserving_order(target_partitions, output_ordering)?
        } else {
            self.repartition_evenly_by_size(target_partitions)?
        };

        if let Some(repartitioned) = maybe_repartitioned {
            Ok(Some(Arc::new(Self::try_new(
                &repartitioned,
                self.original_schema(),
                self.projection.clone(),
            )?)))
        } else {
            Ok(None)
        }
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(
            Arc::clone(&self.projected_schema),
            self.sort_information.clone(),
        )
    }

    fn scheduling_type(&self) -> SchedulingType {
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        if let Some(partition) = partition {
            // Compute statistics for a specific partition
            if let Some(batches) = self.partitions.get(partition) {
                Ok(common::compute_record_batch_statistics(
                    from_ref(batches),
                    &self.schema,
                    self.projection.clone(),
                ))
            } else {
                // Invalid partition index
                Ok(Statistics::new_unknown(&self.projected_schema))
            }
        } else {
            // Compute statistics across all partitions
            Ok(common::compute_record_batch_statistics(
                &self.partitions,
                &self.schema,
                self.projection.clone(),
            ))
        }
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let source = self.clone();
        Some(Arc::new(source.with_limit(limit)))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        // If there is any non-column or alias-carrier expression, Projection should not be removed.
        // This process can be moved into MemoryExec, but it would be an overlap of their responsibility.
        let exprs = projection.iter().cloned().collect_vec();
        all_alias_free_columns(exprs.as_slice())
            .then(|| {
                let all_projections = (0..self.schema.fields().len()).collect();
                let new_projections = new_projections_for_columns(
                    &exprs,
                    self.projection().as_ref().unwrap_or(&all_projections),
                );

                MemorySourceConfig::try_new(
                    self.partitions(),
                    self.original_schema(),
                    Some(new_projections),
                )
                .map(|s| Arc::new(s) as Arc<dyn DataSource>)
            })
            .transpose()
    }
}

impl MemorySourceConfig {
    /// Create a new `MemorySourceConfig` for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        })
    }

    /// Create a new `DataSourceExec` plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new_exec(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Arc<DataSourceExec>> {
        let source = Self::try_new(partitions, schema, projection)?;
        Ok(DataSourceExec::from_data_source(source))
    }

    /// Create a new execution plan from a list of constant values (`ValuesExec`)
    #[expect(clippy::needless_pass_by_value)]
    pub fn try_new_as_values(
        schema: SchemaRef,
        data: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Arc<DataSourceExec>> {
        if data.is_empty() {
            return plan_err!("Values list cannot be empty");
        }

        let n_row = data.len();
        let n_col = schema.fields().len();

        // We have this single row batch as a placeholder to satisfy evaluation argument
        // and generate a single output row
        let placeholder_schema = Arc::new(Schema::empty());
        let placeholder_batch = RecordBatch::try_new_with_options(
            Arc::clone(&placeholder_schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;

        // Evaluate each column
        let arrays = (0..n_col)
            .map(|j| {
                (0..n_row)
                    .map(|i| {
                        let expr = &data[i][j];
                        let result = expr.evaluate(&placeholder_batch)?;

                        match result {
                            ColumnarValue::Scalar(scalar) => Ok(scalar),
                            ColumnarValue::Array(array) if array.len() == 1 => {
                                ScalarValue::try_from_array(&array, 0)
                            }
                            ColumnarValue::Array(_) => {
                                plan_err!("Cannot have array values in a values list")
                            }
                        }
                    })
                    .collect::<Result<Vec<_>>>()
                    .and_then(ScalarValue::iter_to_array)
            })
            .collect::<Result<Vec<_>>>()?;

        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(n_row)),
        )?;

        let partitions = vec![batch];
        Self::try_new_from_batches(Arc::clone(&schema), partitions)
    }

    /// Create a new plan using the provided schema and batches.
    ///
    /// Errors if any of the batches don't match the provided schema, or if no
    /// batches are provided.
    #[expect(clippy::needless_pass_by_value)]
    pub fn try_new_from_batches(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Arc<DataSourceExec>> {
        if batches.is_empty() {
            return plan_err!("Values list cannot be empty");
        }

        for batch in &batches {
            let batch_schema = batch.schema();
            if batch_schema != schema {
                return plan_err!(
                    "Batch has invalid schema. Expected: {}, got: {}",
                    schema,
                    batch_schema
                );
            }
        }

        let partitions = vec![batches];
        let source = Self {
            partitions,
            schema: Arc::clone(&schema),
            projected_schema: Arc::clone(&schema),
            projection: None,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        };
        Ok(DataSourceExec::from_data_source(source))
    }

    /// Set the limit of the files
    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.fetch = limit;
        self
    }

    /// Set `show_sizes` to determine whether to display partition sizes
    pub fn with_show_sizes(mut self, show_sizes: bool) -> Self {
        self.show_sizes = show_sizes;
        self
    }

    /// Ref to partitions
    pub fn partitions(&self) -> &[Vec<RecordBatch>] {
        &self.partitions
    }

    /// Ref to projection
    pub fn projection(&self) -> &Option<Vec<usize>> {
        &self.projection
    }

    /// Show sizes
    pub fn show_sizes(&self) -> bool {
        self.show_sizes
    }

    /// Ref to sort information
    pub fn sort_information(&self) -> &[LexOrdering] {
        &self.sort_information
    }

    /// A memory table can be ordered by multiple expressions simultaneously.
    /// [`EquivalenceProperties`] keeps track of expressions that describe the
    /// global ordering of the schema. These columns are not necessarily same; e.g.
    /// ```text
    /// ┌-------┐
    /// | a | b |
    /// |---|---|
    /// | 1 | 9 |
    /// | 2 | 8 |
    /// | 3 | 7 |
    /// | 5 | 5 |
    /// └---┴---┘
    /// ```
    /// where both `a ASC` and `b DESC` can describe the table ordering. With
    /// [`EquivalenceProperties`], we can keep track of these equivalences
    /// and treat `a ASC` and `b DESC` as the same ordering requirement.
    ///
    /// Note that if there is an internal projection, that projection will be
    /// also applied to the given `sort_information`.
    pub fn try_with_sort_information(
        mut self,
        mut sort_information: Vec<LexOrdering>,
    ) -> Result<Self> {
        // All sort expressions must refer to the original schema
        let fields = self.schema.fields();
        let ambiguous_column = sort_information
            .iter()
            .flat_map(|ordering| ordering.clone())
            .flat_map(|expr| collect_columns(&expr.expr))
            .find(|col| {
                fields
                    .get(col.index())
                    .map(|field| field.name() != col.name())
                    .unwrap_or(true)
            });
        assert_or_internal_err!(
            ambiguous_column.is_none(),
            "Column {:?} is not found in the original schema of the MemorySourceConfig",
            ambiguous_column.as_ref().unwrap()
        );

        // If there is a projection on the source, we also need to project orderings
        if self.projection.is_some() {
            sort_information =
                project_orderings(&sort_information, &self.projected_schema);
        }

        self.sort_information = sort_information;
        Ok(self)
    }

    /// Arc clone of ref to original schema
    pub fn original_schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Repartition while preserving order.
    ///
    /// Returns `Ok(None)` if cannot fulfill the requested repartitioning, such
    /// as having too few batches to fulfill the `target_partitions` or if unable
    /// to preserve output ordering.
    fn repartition_preserving_order(
        &self,
        target_partitions: usize,
        output_ordering: LexOrdering,
    ) -> Result<Option<Vec<Vec<RecordBatch>>>> {
        if !self.eq_properties().ordering_satisfy(output_ordering)? {
            Ok(None)
        } else {
            let total_num_batches =
                self.partitions.iter().map(|b| b.len()).sum::<usize>();
            if total_num_batches < target_partitions {
                // no way to create the desired repartitioning
                return Ok(None);
            }

            let cnt_to_repartition = target_partitions - self.partitions.len();

            // Label the current partitions and their order.
            // Such that when we later split up the partitions into smaller sizes, we are maintaining the order.
            let to_repartition = self
                .partitions
                .iter()
                .enumerate()
                .map(|(idx, batches)| RePartition {
                    idx: idx + (cnt_to_repartition * idx), // make space in ordering for split partitions
                    row_count: batches.iter().map(|batch| batch.num_rows()).sum(),
                    batches: batches.clone(),
                })
                .collect_vec();

            // Put all of the partitions into a heap ordered by `RePartition::partial_cmp`, which sizes
            // by count of rows.
            let mut max_heap = BinaryHeap::with_capacity(target_partitions);
            for rep in to_repartition {
                max_heap.push(CompareByRowCount(rep));
            }

            // Split the largest partitions into smaller partitions. Maintaining the output
            // order of the partitions & newly created partitions.
            let mut cannot_split_further = Vec::with_capacity(target_partitions);
            for _ in 0..cnt_to_repartition {
                // triggers loop for the cnt_to_repartition. So if need another 4 partitions, it attempts to split 4 times.
                loop {
                    // Take the largest item off the heap, and attempt to split.
                    let Some(to_split) = max_heap.pop() else {
                        // Nothing left to attempt repartition. Break inner loop.
                        break;
                    };

                    // Split the partition. The new partitions will be ordered with idx and idx+1.
                    let mut new_partitions = to_split.into_inner().split();
                    if new_partitions.len() > 1 {
                        for new_partition in new_partitions {
                            max_heap.push(CompareByRowCount(new_partition));
                        }
                        // Successful repartition. Break inner loop, and return to outer `cnt_to_repartition` loop.
                        break;
                    } else {
                        cannot_split_further.push(new_partitions.remove(0));
                    }
                }
            }
            let mut partitions = max_heap
                .drain()
                .map(CompareByRowCount::into_inner)
                .collect_vec();
            partitions.extend(cannot_split_further);

            // Finally, sort all partitions by the output ordering.
            // This was the original ordering of the batches within the partition. We are maintaining this ordering.
            partitions.sort_by_key(|p| p.idx);
            let partitions = partitions.into_iter().map(|rep| rep.batches).collect_vec();

            Ok(Some(partitions))
        }
    }

    /// Repartition into evenly sized chunks (as much as possible without batch splitting),
    /// disregarding any ordering.
    ///
    /// Current implementation uses a first-fit-decreasing bin packing, modified to enable
    /// us to still return the desired count of `target_partitions`.
    ///
    /// Returns `Ok(None)` if cannot fulfill the requested repartitioning, such
    /// as having too few batches to fulfill the `target_partitions`.
    fn repartition_evenly_by_size(
        &self,
        target_partitions: usize,
    ) -> Result<Option<Vec<Vec<RecordBatch>>>> {
        // determine if we have enough total batches to fulfill request
        let mut flatten_batches =
            self.partitions.clone().into_iter().flatten().collect_vec();
        if flatten_batches.len() < target_partitions {
            return Ok(None);
        }

        // Take all flattened batches (all in 1 partititon/vec) and divide evenly into the desired number of `target_partitions`.
        let total_num_rows = flatten_batches.iter().map(|b| b.num_rows()).sum::<usize>();
        // sort by size, so we pack multiple smaller batches into the same partition
        flatten_batches.sort_by_key(|b| std::cmp::Reverse(b.num_rows()));

        // Divide.
        let mut partitions =
            vec![Vec::with_capacity(flatten_batches.len()); target_partitions];
        let mut target_partition_size = total_num_rows.div_ceil(target_partitions);
        let mut total_rows_seen = 0;
        let mut curr_bin_row_count = 0;
        let mut idx = 0;
        for batch in flatten_batches {
            let row_cnt = batch.num_rows();
            idx = std::cmp::min(idx, target_partitions - 1);

            partitions[idx].push(batch);
            curr_bin_row_count += row_cnt;
            total_rows_seen += row_cnt;

            if curr_bin_row_count >= target_partition_size {
                idx += 1;
                curr_bin_row_count = 0;

                // update target_partition_size, to handle very lopsided batch distributions
                // while still returning the count of `target_partitions`
                if total_rows_seen < total_num_rows {
                    target_partition_size = (total_num_rows - total_rows_seen)
                        .div_ceil(target_partitions - idx);
                }
            }
        }

        Ok(Some(partitions))
    }
}

/// For use in repartitioning, track the total size and original partition index.
///
/// Do not implement clone, in order to avoid unnecessary copying during repartitioning.
struct RePartition {
    /// Original output ordering for the partition.
    idx: usize,
    /// Total size of the partition, for use in heap ordering
    /// (a.k.a. splitting up the largest partitions).
    row_count: usize,
    /// A partition containing record batches.
    batches: Vec<RecordBatch>,
}

impl RePartition {
    /// Split [`RePartition`] into 2 pieces, consuming self.
    ///
    /// Returns only 1 partition if cannot be split further.
    fn split(self) -> Vec<Self> {
        if self.batches.len() == 1 {
            return vec![self];
        }

        let new_0 = RePartition {
            idx: self.idx, // output ordering
            row_count: 0,
            batches: vec![],
        };
        let new_1 = RePartition {
            idx: self.idx + 1, // output ordering +1
            row_count: 0,
            batches: vec![],
        };
        let split_pt = self.row_count / 2;

        let [new_0, new_1] = self.batches.into_iter().fold(
            [new_0, new_1],
            |[mut new0, mut new1], batch| {
                if new0.row_count < split_pt {
                    new0.add_batch(batch);
                } else {
                    new1.add_batch(batch);
                }
                [new0, new1]
            },
        );
        vec![new_0, new_1]
    }

    fn add_batch(&mut self, batch: RecordBatch) {
        self.row_count += batch.num_rows();
        self.batches.push(batch);
    }
}

impl fmt::Display for RePartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}rows-in-{}batches@{}",
            self.row_count,
            self.batches.len(),
            self.idx
        )
    }
}

struct CompareByRowCount(RePartition);
impl CompareByRowCount {
    fn into_inner(self) -> RePartition {
        self.0
    }
}
impl Ord for CompareByRowCount {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.row_count.cmp(&other.0.row_count)
    }
}
impl PartialOrd for CompareByRowCount {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for CompareByRowCount {
    fn eq(&self, other: &Self) -> bool {
        // PartialEq must be consistent with PartialOrd
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for CompareByRowCount {}
impl Deref for CompareByRowCount {
    type Target = RePartition;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Type alias for partition data
pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

/// Implements for writing to a [`MemTable`]
///
/// [`MemTable`]: <https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.MemTable.html>
pub struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
    schema: SchemaRef,
}

impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl DisplayAs for MemSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_count = self.batches.len();
                write!(f, "MemoryTable (partitions={partition_count})")
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl MemSink {
    /// Creates a new [`MemSink`].
    ///
    /// The caller is responsible for ensuring that there is at least one partition to insert into.
    pub fn try_new(batches: Vec<PartitionData>, schema: SchemaRef) -> Result<Self> {
        if batches.is_empty() {
            return plan_err!("Cannot insert into MemTable with zero partitions");
        }
        Ok(Self { batches, schema })
    }
}

#[async_trait]
impl DataSink for MemSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.batches.len();

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        // write the outputs into the batches
        for (target, mut batches) in self.batches.iter().zip(new_batches.into_iter()) {
            // Append all the new batches in one go to minimize locking overhead
            target.write().await.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}

#[cfg(test)]
mod memory_source_tests {
    use std::sync::Arc;

    use crate::memory::MemorySourceConfig;
    use crate::source::DataSourceExec;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::Result;
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion_physical_plan::ExecutionPlan;

    #[test]
    fn test_memory_order_eq() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let sort1: LexOrdering = [
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions::default(),
            },
        ]
        .into();
        let sort2: LexOrdering = [PhysicalSortExpr {
            expr: col("c", &schema)?,
            options: SortOptions::default(),
        }]
        .into();
        let mut expected_output_order = sort1.clone();
        expected_output_order.extend(sort2.clone());

        let sort_information = vec![sort1.clone(), sort2.clone()];
        let mem_exec = DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&[vec![]], schema, None)?
                .try_with_sort_information(sort_information)?,
        );

        assert_eq!(
            mem_exec.properties().output_ordering().unwrap(),
            &expected_output_order
        );
        let eq_properties = mem_exec.properties().equivalence_properties();
        assert!(eq_properties.oeq_class().contains(&sort1));
        assert!(eq_properties.oeq_class().contains(&sort2));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::col;
    use crate::tests::{aggr_test_schema, make_partition};

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::assert_batches_eq;
    use datafusion_common::stats::{ColumnStatistics, Precision};
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::expressions::lit;

    use datafusion_physical_plan::ExecutionPlan;
    use futures::StreamExt;

    #[tokio::test]
    async fn exec_with_limit() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let batch = make_partition(7);
        let schema = batch.schema();
        let batches = vec![batch.clone(), batch];

        let exec = MemorySourceConfig::try_new_from_batches(schema, batches).unwrap();
        assert_eq!(exec.fetch(), None);

        let exec = exec.with_fetch(Some(4)).unwrap();
        assert_eq!(exec.fetch(), Some(4));

        let mut it = exec.execute(0, task_ctx)?;
        let mut results = vec![];
        while let Some(batch) = it.next().await {
            results.push(batch?);
        }

        let expected = [
            "+---+", "| i |", "+---+", "| 0 |", "| 1 |", "| 2 |", "| 3 |", "+---+",
        ];
        assert_batches_eq!(expected, &results);
        Ok(())
    }

    #[tokio::test]
    async fn values_empty_case() -> Result<()> {
        let schema = aggr_test_schema();
        let empty = MemorySourceConfig::try_new_as_values(schema, vec![]);
        assert!(empty.is_err());
        Ok(())
    }

    #[test]
    fn new_exec_with_batches() {
        let batch = make_partition(7);
        let schema = batch.schema();
        let batches = vec![batch.clone(), batch];
        let _exec = MemorySourceConfig::try_new_from_batches(schema, batches).unwrap();
    }

    #[test]
    fn new_exec_with_batches_empty() {
        let batch = make_partition(7);
        let schema = batch.schema();
        let _ = MemorySourceConfig::try_new_from_batches(schema, Vec::new()).unwrap_err();
    }

    #[test]
    fn new_exec_with_batches_invalid_schema() {
        let batch = make_partition(7);
        let batches = vec![batch.clone(), batch];

        let invalid_schema = Arc::new(Schema::new(vec![
            Field::new("col0", DataType::UInt32, false),
            Field::new("col1", DataType::Utf8, false),
        ]));
        let _ = MemorySourceConfig::try_new_from_batches(invalid_schema, batches)
            .unwrap_err();
    }

    // Test issue: https://github.com/apache/datafusion/issues/8763
    #[test]
    fn new_exec_with_non_nullable_schema() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col0",
            DataType::UInt32,
            false,
        )]));
        let _ = MemorySourceConfig::try_new_as_values(
            Arc::clone(&schema),
            vec![vec![lit(1u32)]],
        )
        .unwrap();
        // Test that a null value is rejected
        let _ = MemorySourceConfig::try_new_as_values(
            schema,
            vec![vec![lit(ScalarValue::UInt32(None))]],
        )
        .unwrap_err();
    }

    #[test]
    fn values_stats_with_nulls_only() -> Result<()> {
        let data = vec![
            vec![lit(ScalarValue::Null)],
            vec![lit(ScalarValue::Null)],
            vec![lit(ScalarValue::Null)],
        ];
        let rows = data.len();
        let schema =
            Arc::new(Schema::new(vec![Field::new("col0", DataType::Null, true)]));
        let values = MemorySourceConfig::try_new_as_values(schema, data)?;

        assert_eq!(
            values.partition_statistics(None)?,
            Statistics {
                num_rows: Precision::Exact(rows),
                total_byte_size: Precision::Exact(8), // not important
                column_statistics: vec![ColumnStatistics {
                    null_count: Precision::Exact(rows), // there are only nulls
                    distinct_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    sum_value: Precision::Absent,
                },],
            }
        );

        Ok(())
    }

    fn batch(row_size: usize) -> RecordBatch {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1; row_size]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("foo"); row_size]));
        let c: ArrayRef = Arc::new(Int64Array::from_iter(vec![1; row_size]));
        RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap()
    }

    fn schema() -> SchemaRef {
        batch(1).schema()
    }

    fn memorysrcconfig_no_partitions(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    fn memorysrcconfig_1_partition_1_batch(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![vec![batch(100)]];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    fn memorysrcconfig_3_partitions_1_batch_each(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![vec![batch(100)], vec![batch(100)], vec![batch(100)]];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    fn memorysrcconfig_3_partitions_with_2_batches_each(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![
            vec![batch(100), batch(100)],
            vec![batch(100), batch(100)],
            vec![batch(100), batch(100)],
        ];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    /// Batches of different sizes, with batches ordered by size (100_000, 10_000, 100, 1)
    /// in the Memtable partition (a.k.a. vector of batches).
    fn memorysrcconfig_1_partition_with_different_sized_batches(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![vec![batch(100_000), batch(10_000), batch(100), batch(1)]];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    /// Same as [`memorysrcconfig_1_partition_with_different_sized_batches`],
    /// but the batches are ordered differently (not by size)
    /// in the Memtable partition (a.k.a. vector of batches).
    fn memorysrcconfig_1_partition_with_ordering_not_matching_size(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![vec![batch(100_000), batch(1), batch(100), batch(10_000)]];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    fn memorysrcconfig_2_partition_with_different_sized_batches(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![
            vec![batch(100_000), batch(10_000), batch(1_000)],
            vec![batch(2_000), batch(20)],
        ];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    fn memorysrcconfig_2_partition_with_extreme_sized_batches(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![
            vec![
                batch(100_000),
                batch(1),
                batch(1),
                batch(1),
                batch(1),
                batch(0),
            ],
            vec![batch(1), batch(1), batch(1), batch(1), batch(0), batch(100)],
        ];
        MemorySourceConfig::try_new(&partitions, schema(), None)?
            .try_with_sort_information(sort_information)
    }

    /// Assert that we get the expected count of partitions after repartitioning.
    ///
    /// If None, then we expected the [`DataSource::repartitioned`] to return None.
    fn assert_partitioning(
        partitioned_datasrc: Option<Arc<dyn DataSource>>,
        partition_cnt: Option<usize>,
    ) {
        let should_exist = if let Some(partition_cnt) = partition_cnt {
            format!("new datasource should exist and have {partition_cnt:?} partitions")
        } else {
            "new datasource should not exist".into()
        };

        let actual = partitioned_datasrc
            .map(|datasrc| datasrc.output_partitioning().partition_count());
        assert_eq!(
            actual,
            partition_cnt,
            "partitioned datasrc does not match expected, we expected {should_exist}, instead found {actual:?}"
        );
    }

    fn run_all_test_scenarios(
        output_ordering: Option<LexOrdering>,
        sort_information_on_config: Vec<LexOrdering>,
    ) -> Result<()> {
        let not_used = usize::MAX;

        // src has no partitions
        let mem_src_config =
            memorysrcconfig_no_partitions(sort_information_on_config.clone())?;
        let partitioned_datasrc =
            mem_src_config.repartitioned(1, not_used, output_ordering.clone())?;
        assert_partitioning(partitioned_datasrc, None);

        // src has partitions == target partitions (=1)
        let target_partitions = 1;
        let mem_src_config =
            memorysrcconfig_1_partition_1_batch(sort_information_on_config.clone())?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, None);

        // src has partitions == target partitions (=3)
        let target_partitions = 3;
        let mem_src_config = memorysrcconfig_3_partitions_1_batch_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, None);

        // src has partitions > target partitions, but we don't merge them
        let target_partitions = 2;
        let mem_src_config = memorysrcconfig_3_partitions_1_batch_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, None);

        // src has partitions < target partitions, but not enough batches (per partition) to split into more partitions
        let target_partitions = 4;
        let mem_src_config = memorysrcconfig_3_partitions_1_batch_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, None);

        // src has partitions < target partitions, and can split to sufficient amount
        // has 6 batches across 3 partitions. Will need to split 2 of it's partitions.
        let target_partitions = 5;
        let mem_src_config = memorysrcconfig_3_partitions_with_2_batches_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, Some(5));

        // src has partitions < target partitions, and can split to sufficient amount
        // has 6 batches across 3 partitions. Will need to split all of it's partitions.
        let target_partitions = 6;
        let mem_src_config = memorysrcconfig_3_partitions_with_2_batches_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, Some(6));

        // src has partitions < target partitions, but not enough total batches to fulfill the split (desired target_partitions)
        let target_partitions = 3 * 2 + 1;
        let mem_src_config = memorysrcconfig_3_partitions_with_2_batches_each(
            sort_information_on_config.clone(),
        )?;
        let partitioned_datasrc = mem_src_config.repartitioned(
            target_partitions,
            not_used,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc, None);

        // src has 1 partition with many batches of lopsided sizes
        // make sure it handles the split properly
        let target_partitions = 2;
        let mem_src_config = memorysrcconfig_1_partition_with_different_sized_batches(
            sort_information_on_config,
        )?;
        let partitioned_datasrc = mem_src_config.clone().repartitioned(
            target_partitions,
            not_used,
            output_ordering,
        )?;
        assert_partitioning(partitioned_datasrc.clone(), Some(2));
        // Starting = batch(100_000), batch(10_000), batch(100), batch(1).
        // It should have split as p1=batch(100_000), p2=[batch(10_000), batch(100), batch(1)]
        let partitioned_datasrc = partitioned_datasrc.unwrap();
        let Some(mem_src_config) = partitioned_datasrc
            .as_any()
            .downcast_ref::<MemorySourceConfig>()
        else {
            unreachable!()
        };
        let repartitioned_raw_batches = mem_src_config.partitions.clone();
        assert_eq!(repartitioned_raw_batches.len(), 2);
        let [ref p1, ref p2] = repartitioned_raw_batches[..] else {
            unreachable!()
        };
        // p1=batch(100_000)
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].num_rows(), 100_000);
        // p2=[batch(10_000), batch(100), batch(1)]
        assert_eq!(p2.len(), 3);
        assert_eq!(p2[0].num_rows(), 10_000);
        assert_eq!(p2[1].num_rows(), 100);
        assert_eq!(p2[2].num_rows(), 1);

        Ok(())
    }

    #[test]
    fn test_repartition_no_sort_information_no_output_ordering() -> Result<()> {
        let no_sort = vec![];
        let no_output_ordering = None;

        // Test: common set of functionality
        run_all_test_scenarios(no_output_ordering.clone(), no_sort.clone())?;

        // Test: how no-sort-order divides differently.
        //    * does not preserve separate partitions (with own internal ordering) on even split,
        //    * nor does it preserve ordering (re-orders batch(2_000) vs batch(1_000)).
        let target_partitions = 3;
        let mem_src_config =
            memorysrcconfig_2_partition_with_different_sized_batches(no_sort)?;
        let partitioned_datasrc = mem_src_config.clone().repartitioned(
            target_partitions,
            usize::MAX,
            no_output_ordering,
        )?;
        assert_partitioning(partitioned_datasrc.clone(), Some(3));
        // Starting = batch(100_000), batch(10_000), batch(1_000), batch(2_000), batch(20)
        // It should have split as p1=batch(100_000), p2=batch(10_000),  p3=rest(mixed across original partitions)
        let repartitioned_raw_batches = mem_src_config
            .repartition_evenly_by_size(target_partitions)?
            .unwrap();
        assert_eq!(repartitioned_raw_batches.len(), 3);
        let [ref p1, ref p2, ref p3] = repartitioned_raw_batches[..] else {
            unreachable!()
        };
        // p1=batch(100_000)
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].num_rows(), 100_000);
        // p2=batch(10_000)
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].num_rows(), 10_000);
        // p3= batch(2_000), batch(1_000), batch(20)
        assert_eq!(p3.len(), 3);
        assert_eq!(p3[0].num_rows(), 2_000);
        assert_eq!(p3[1].num_rows(), 1_000);
        assert_eq!(p3[2].num_rows(), 20);

        Ok(())
    }

    #[test]
    fn test_repartition_no_sort_information_no_output_ordering_lopsized_batches(
    ) -> Result<()> {
        let no_sort = vec![];
        let no_output_ordering = None;

        // Test: case has two input partitions:
        //     b(100_000), b(1), b(1), b(1), b(1), b(0)
        //     b(1), b(1), b(1), b(1), b(0), b(100)
        //
        // We want an output with target_partitions=5, which means the ideal division is:
        //     b(100_000)
        //     b(100)
        //     b(1), b(1), b(1)
        //     b(1), b(1), b(1)
        //     b(1), b(1), b(0)
        let target_partitions = 5;
        let mem_src_config =
            memorysrcconfig_2_partition_with_extreme_sized_batches(no_sort)?;
        let partitioned_datasrc = mem_src_config.clone().repartitioned(
            target_partitions,
            usize::MAX,
            no_output_ordering,
        )?;
        assert_partitioning(partitioned_datasrc.clone(), Some(5));
        // Starting partition 1 = batch(100_000), batch(1), batch(1), batch(1), batch(1), batch(0)
        // Starting partition 1 = batch(1), batch(1), batch(1), batch(1), batch(0), batch(100)
        // It should have split as p1=batch(100_000), p2=batch(100), p3=[batch(1),batch(1)], p4=[batch(1),batch(1)], p5=[batch(1),batch(1),batch(0),batch(0)]
        let repartitioned_raw_batches = mem_src_config
            .repartition_evenly_by_size(target_partitions)?
            .unwrap();
        assert_eq!(repartitioned_raw_batches.len(), 5);
        let [ref p1, ref p2, ref p3, ref p4, ref p5] = repartitioned_raw_batches[..]
        else {
            unreachable!()
        };
        // p1=batch(100_000)
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].num_rows(), 100_000);
        // p2=batch(100)
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].num_rows(), 100);
        // p3=[batch(1),batch(1),batch(1)]
        assert_eq!(p3.len(), 3);
        assert_eq!(p3[0].num_rows(), 1);
        assert_eq!(p3[1].num_rows(), 1);
        assert_eq!(p3[2].num_rows(), 1);
        // p4=[batch(1),batch(1),batch(1)]
        assert_eq!(p4.len(), 3);
        assert_eq!(p4[0].num_rows(), 1);
        assert_eq!(p4[1].num_rows(), 1);
        assert_eq!(p4[2].num_rows(), 1);
        // p5=[batch(1),batch(1),batch(0),batch(0)]
        assert_eq!(p5.len(), 4);
        assert_eq!(p5[0].num_rows(), 1);
        assert_eq!(p5[1].num_rows(), 1);
        assert_eq!(p5[2].num_rows(), 0);
        assert_eq!(p5[3].num_rows(), 0);

        Ok(())
    }

    #[test]
    fn test_repartition_with_sort_information() -> Result<()> {
        let schema = schema();
        let sort_key: LexOrdering =
            [PhysicalSortExpr::new_default(col("c", &schema)?)].into();
        let has_sort = vec![sort_key.clone()];
        let output_ordering = Some(sort_key);

        // Test: common set of functionality
        run_all_test_scenarios(output_ordering.clone(), has_sort.clone())?;

        // Test: DOES preserve separate partitions (with own internal ordering)
        let target_partitions = 3;
        let mem_src_config =
            memorysrcconfig_2_partition_with_different_sized_batches(has_sort)?;
        let partitioned_datasrc = mem_src_config.clone().repartitioned(
            target_partitions,
            usize::MAX,
            output_ordering.clone(),
        )?;
        assert_partitioning(partitioned_datasrc.clone(), Some(3));
        // Starting = batch(100_000), batch(10_000), batch(1_000), batch(2_000), batch(20)
        // It should have split as p1=batch(100_000), p2=[batch(10_000),batch(1_000)],  p3=<other_partition>
        let Some(output_ord) = output_ordering else {
            unreachable!()
        };
        let repartitioned_raw_batches = mem_src_config
            .repartition_preserving_order(target_partitions, output_ord)?
            .unwrap();
        assert_eq!(repartitioned_raw_batches.len(), 3);
        let [ref p1, ref p2, ref p3] = repartitioned_raw_batches[..] else {
            unreachable!()
        };
        // p1=batch(100_000)
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].num_rows(), 100_000);
        // p2=[batch(10_000),batch(1_000)]
        assert_eq!(p2.len(), 2);
        assert_eq!(p2[0].num_rows(), 10_000);
        assert_eq!(p2[1].num_rows(), 1_000);
        // p3=batch(2_000), batch(20)
        assert_eq!(p3.len(), 2);
        assert_eq!(p3[0].num_rows(), 2_000);
        assert_eq!(p3[1].num_rows(), 20);

        Ok(())
    }

    #[test]
    fn test_repartition_with_batch_ordering_not_matching_sizing() -> Result<()> {
        let schema = schema();
        let sort_key: LexOrdering =
            [PhysicalSortExpr::new_default(col("c", &schema)?)].into();
        let has_sort = vec![sort_key.clone()];
        let output_ordering = Some(sort_key);

        // src has 1 partition with many batches of lopsided sizes
        // note that the input vector of batches are not ordered by decreasing size
        let target_partitions = 2;
        let mem_src_config =
            memorysrcconfig_1_partition_with_ordering_not_matching_size(has_sort)?;
        let partitioned_datasrc = mem_src_config.clone().repartitioned(
            target_partitions,
            usize::MAX,
            output_ordering,
        )?;
        assert_partitioning(partitioned_datasrc.clone(), Some(2));
        // Starting = batch(100_000), batch(1), batch(100), batch(10_000).
        // It should have split as p1=batch(100_000), p2=[batch(1), batch(100), batch(10_000)]
        let partitioned_datasrc = partitioned_datasrc.unwrap();
        let Some(mem_src_config) = partitioned_datasrc
            .as_any()
            .downcast_ref::<MemorySourceConfig>()
        else {
            unreachable!()
        };
        let repartitioned_raw_batches = mem_src_config.partitions.clone();
        assert_eq!(repartitioned_raw_batches.len(), 2);
        let [ref p1, ref p2] = repartitioned_raw_batches[..] else {
            unreachable!()
        };
        // p1=batch(100_000)
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].num_rows(), 100_000);
        // p2=[batch(1), batch(100), batch(10_000)] -- **this is preserving the partition order**
        assert_eq!(p2.len(), 3);
        assert_eq!(p2[0].num_rows(), 1);
        assert_eq!(p2[1].num_rows(), 100);
        assert_eq!(p2[2].num_rows(), 10_000);

        Ok(())
    }
}
