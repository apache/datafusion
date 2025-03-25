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

//! Execution plan for reading in-memory batches of data

use std::any::Any;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt;
use std::sync::Arc;

use crate::source::{DataSource, DataSourceExec};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::memory::MemoryStream;
use datafusion_physical_plan::projection::{
    all_alias_free_columns, new_projections_for_columns, ProjectionExec,
};
use datafusion_physical_plan::{
    common, ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PhysicalExpr, PlanProperties, SendableRecordBatchStream, Statistics,
};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::{
    internal_err, plan_err, project_schema, Constraints, Result, ScalarValue,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use itertools::Itertools;

/// Execution plan for reading in-memory batches of data
#[derive(Clone)]
#[deprecated(
    since = "46.0.0",
    note = "use MemorySourceConfig and DataSourceExec instead"
)]
pub struct MemoryExec {
    inner: DataSourceExec,
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Optional projection
    projection: Option<Vec<usize>>,
    // Sort information: one or more equivalent orderings
    sort_information: Vec<LexOrdering>,
    /// if partition sizes should be displayed
    show_sizes: bool,
}

#[allow(unused, deprecated)]
impl fmt::Debug for MemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(DisplayFormatType::Default, f)
    }
}

#[allow(unused, deprecated)]
impl DisplayAs for MemoryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[allow(unused, deprecated)]
impl ExecutionPlan for MemoryExec {
    fn name(&self) -> &'static str {
        "MemoryExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a leaf node and has no children
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // MemoryExec has no children
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    /// We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        self.inner.try_swapping_with_projection(projection)
    }
}

#[allow(unused, deprecated)]
impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let source = MemorySourceConfig::try_new(partitions, schema, projection.clone())?;
        let data_source = DataSourceExec::new(Arc::new(source));
        Ok(Self {
            inner: data_source,
            partitions: partitions.to_vec(),
            projection,
            sort_information: vec![],
            show_sizes: true,
        })
    }

    /// Create a new execution plan from a list of constant values (`ValuesExec`)
    pub fn try_new_as_values(
        schema: SchemaRef,
        data: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
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
    pub fn try_new_from_batches(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Self> {
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
        let source = MemorySourceConfig {
            partitions: partitions.clone(),
            schema: Arc::clone(&schema),
            projected_schema: Arc::clone(&schema),
            projection: None,
            sort_information: vec![],
            show_sizes: true,
            fetch: None,
        };
        let data_source = DataSourceExec::new(Arc::new(source));
        Ok(Self {
            inner: data_source,
            partitions,
            projection: None,
            sort_information: vec![],
            show_sizes: true,
        })
    }

    fn memory_source_config(&self) -> MemorySourceConfig {
        self.inner
            .data_source()
            .as_any()
            .downcast_ref::<MemorySourceConfig>()
            .unwrap()
            .clone()
    }

    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.inner = self.inner.with_constraints(constraints);
        self
    }

    /// Set `show_sizes` to determine whether to display partition sizes
    pub fn with_show_sizes(mut self, show_sizes: bool) -> Self {
        let mut memory_source = self.memory_source_config();
        memory_source.show_sizes = show_sizes;
        self.show_sizes = show_sizes;
        self.inner = DataSourceExec::new(Arc::new(memory_source));
        self
    }

    /// Ref to constraints
    pub fn constraints(&self) -> &Constraints {
        self.properties().equivalence_properties().constraints()
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
        sort_information: Vec<LexOrdering>,
    ) -> Result<Self> {
        self.sort_information = sort_information.clone();
        let mut memory_source = self.memory_source_config();
        memory_source = memory_source.try_with_sort_information(sort_information)?;
        self.inner = DataSourceExec::new(Arc::new(memory_source));
        Ok(self)
    }

    /// Arc clone of ref to original schema
    pub fn original_schema(&self) -> SchemaRef {
        Arc::clone(&self.inner.schema())
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        constraints: Constraints,
        partitions: &[Vec<RecordBatch>],
    ) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new_with_orderings(schema, orderings)
                .with_constraints(constraints),
            Partitioning::UnknownPartitioning(partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

/// Data source configuration for reading in-memory batches of data
#[derive(Clone, Debug)]
pub struct MemorySourceConfig {
    /// The partitions to query
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
        Ok(Box::pin(
            MemoryStream::try_new(
                self.partitions[partition].clone(),
                Arc::clone(&self.projected_schema),
                self.projection.clone(),
            )?
            .with_fetch(self.fetch),
        ))
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
                    .map(|output_ordering| {
                        format!(", output_ordering={}", output_ordering)
                    })
                    .unwrap_or_default();

                let eq_properties = self.eq_properties();
                let constraints = eq_properties.constraints();
                let constraints = if constraints.is_empty() {
                    String::new()
                } else {
                    format!(", {}", constraints)
                };

                let limit = self
                    .fetch
                    .map_or(String::new(), |limit| format!(", fetch={}", limit));
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

    fn repartitioned(
        &self,
        target_partitions: usize,
        _repartition_file_min_size: usize,
        output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        if self.partitions.is_empty() || self.partitions.len() >= target_partitions
        // if already have more partitions than desired, do not merge
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
            self.sort_information.as_slice(),
        )
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        ))
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
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // If there is any non-column or alias-carrier expression, Projection should not be removed.
        // This process can be moved into MemoryExec, but it would be an overlap of their responsibility.
        all_alias_free_columns(projection.expr())
            .then(|| {
                let all_projections = (0..self.schema.fields().len()).collect();
                let new_projections = new_projections_for_columns(
                    projection,
                    self.projection().as_ref().unwrap_or(&all_projections),
                );

                MemorySourceConfig::try_new_exec(
                    self.partitions(),
                    self.original_schema(),
                    Some(new_projections),
                )
                .map(|e| e as _)
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
        Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
    }

    /// Create a new execution plan from a list of constant values (`ValuesExec`)
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
        Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
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
        if let Some(col) = ambiguous_column {
            return internal_err!(
                "Column {:?} is not found in the original schema of the MemorySourceConfig",
                col
            );
        }

        // If there is a projection on the source, we also need to project orderings
        if let Some(projection) = &self.projection {
            let base_eqp = EquivalenceProperties::new_with_orderings(
                self.original_schema(),
                &sort_information,
            );
            let proj_exprs = projection
                .iter()
                .map(|idx| {
                    let base_schema = self.original_schema();
                    let name = base_schema.field(*idx).name();
                    (Arc::new(Column::new(name, *idx)) as _, name.to_string())
                })
                .collect::<Vec<_>>();
            let projection_mapping =
                ProjectionMapping::try_new(&proj_exprs, &self.original_schema())?;
            sort_information = base_eqp
                .project(&projection_mapping, Arc::clone(&self.projected_schema))
                .into_oeq_class()
                .into_inner();
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
    /// Returns None if cannot fulfill requested repartitioning.
    fn repartition_preserving_order(
        &self,
        target_partitions: usize,
        output_ordering: LexOrdering,
    ) -> Result<Option<Vec<Vec<RecordBatch>>>> {
        if !self.eq_properties().ordering_satisfy(&output_ordering) {
            Ok(None)
        } else if self.partitions.len() == 1 {
            self.repartition_evenly_by_size(target_partitions)
        } else {
            let total_num_batches =
                self.partitions.iter().map(|b| b.len()).sum::<usize>();
            if total_num_batches < target_partitions {
                // no way to create the desired repartitioning
                return Ok(None);
            }

            let cnt_to_repartition = target_partitions - self.partitions.len();

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

            // split the largest partitions
            let mut max_heap = BinaryHeap::with_capacity(target_partitions);
            for rep in to_repartition {
                max_heap.push(rep);
            }
            let mut cannot_split_further = Vec::with_capacity(target_partitions);
            for _ in 0..cnt_to_repartition {
                loop {
                    let Some(to_split) = max_heap.pop() else {
                        break;
                    };

                    let mut new_partitions = to_split.split();
                    if new_partitions.len() > 1 {
                        for new_partition in new_partitions {
                            max_heap.push(new_partition);
                        }
                        break;
                    } else {
                        cannot_split_further.push(new_partitions.remove(0));
                    }
                }
            }
            let mut partitions = max_heap.drain().collect_vec();
            partitions.extend(cannot_split_further);
            partitions.sort_by_key(|p| p.idx);
            let partitions = partitions.into_iter().map(|rep| rep.batches).collect_vec();

            Ok(Some(partitions))
        }
    }

    /// Repartition into evenly sized chunks (as much as possible without batch splitting),
    /// disregarding any ordering.
    ///
    /// Returns None if cannot fulfill requested repartitioning.
    fn repartition_evenly_by_size(
        &self,
        target_partitions: usize,
    ) -> Result<Option<Vec<Vec<RecordBatch>>>> {
        // determine if we have enough total batches to fulfill request
        let flatten_batches = self.partitions.clone().into_iter().flatten().collect_vec();
        if flatten_batches.len() < target_partitions {
            return Ok(None);
        }

        // repartition evenly
        let total_num_rows = flatten_batches.iter().map(|b| b.num_rows()).sum::<usize>();
        let target_partition_size = total_num_rows.div_ceil(target_partitions);
        let mut partitions =
            vec![Vec::with_capacity(flatten_batches.len()); target_partitions];
        let mut curr_row_count = 0;
        let mut next_idx = 0;
        for batch in flatten_batches {
            let row_cnt = batch.num_rows();

            // handle very lopsided batch sizing
            if partitions[next_idx].is_empty() {
                partitions[next_idx].push(batch);
            } else {
                // have at least 1 batch per partition
                let idx =
                    std::cmp::min(next_idx + 1, curr_row_count / target_partition_size);
                if let Some(partition) = partitions.get_mut(idx) {
                    partition.push(batch);
                } else {
                    partitions[target_partitions - 1].push(batch);
                }
                next_idx = idx;
            }

            curr_row_count += row_cnt;
        }

        Ok(Some(partitions))
    }
}

/// For use in repartitioning, track the total size and original partition index.
///
/// Do not implement clone, in order to avoid unnecessary copying during repartitioning.
struct RePartition {
    idx: usize,
    row_count: usize,
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
            idx: self.idx,
            row_count: 0,
            batches: vec![],
        };
        let new_1 = RePartition {
            idx: self.idx + 1,
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

impl PartialOrd for RePartition {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.row_count.cmp(&other.row_count))
    }
}

impl Ord for RePartition {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row_count.cmp(&other.row_count)
    }
}

impl PartialEq for RePartition {
    fn eq(&self, other: &Self) -> bool {
        self.row_count.eq(&other.row_count)
    }
}

impl Eq for RePartition {}

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

#[cfg(test)]
mod memory_source_tests {
    use std::sync::Arc;

    use crate::memory::MemorySourceConfig;
    use crate::source::DataSourceExec;
    use datafusion_physical_plan::ExecutionPlan;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_expr::expressions::col;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_expr_common::sort_expr::LexOrdering;

    #[test]
    fn test_memory_order_eq() -> datafusion_common::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]));
        let sort1 = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions::default(),
            },
        ]);
        let sort2 = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c", &schema)?,
            options: SortOptions::default(),
        }]);
        let mut expected_output_order = LexOrdering::default();
        expected_output_order.extend(sort1.clone());
        expected_output_order.extend(sort2.clone());

        let sort_information = vec![sort1.clone(), sort2.clone()];
        let mem_exec = Arc::new(DataSourceExec::new(Arc::new(
            MemorySourceConfig::try_new(&[vec![]], schema, None)?
                .try_with_sort_information(sort_information)?,
        )));

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
    use crate::test_util::col;
    use crate::tests::{aggr_test_schema, make_partition};

    use super::*;

    use arrow::array::{ArrayRef, Int32Array, Int64Array, StringArray};
    use arrow::compute::SortOptions;
    use datafusion_physical_expr::PhysicalSortExpr;
    use datafusion_physical_plan::expressions::lit;

    use arrow::datatypes::{DataType, Field};
    use datafusion_common::assert_batches_eq;
    use datafusion_common::stats::{ColumnStatistics, Precision};
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
        let values = MemorySourceConfig::try_new_as_values(
            Arc::new(Schema::new(vec![Field::new("col0", DataType::Null, true)])),
            data,
        )?;

        assert_eq!(
            values.statistics()?,
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

    fn memorysrcconfig_1_partition_with_different_sized_batches(
        sort_information: Vec<LexOrdering>,
    ) -> Result<MemorySourceConfig> {
        let partitions = vec![vec![batch(100_000), batch(10_000), batch(100), batch(1)]];
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

    /// Assert that we get the expected count of partitions after repartitioning.
    ///
    /// If None, then we expected the [`DataSource::repartitioned`] to return None.
    fn assert_partitioning(
        partitioned_datasrc: Option<Arc<dyn DataSource>>,
        partition_cnt: Option<usize>,
    ) {
        let should_exist = if let Some(partition_cnt) = partition_cnt {
            format!(
                "new datasource should exist and have {:?} partitions",
                partition_cnt
            )
        } else {
            "new datasource should not exist".into()
        };

        let actual = partitioned_datasrc
            .map(|datasrc| datasrc.output_partitioning().partition_count());
        assert_eq!(
            actual,
            partition_cnt,
            "partitioned datasrc does not match expected, we expected {}, instead found {:?}",
            should_exist,
            actual
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
        let repartitioned_raw_batches = mem_src_config
            .repartition_evenly_by_size(target_partitions)?
            .unwrap();
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

        // Test: does not preserve separate partitions (with own internal ordering) on even split
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
        // p3= batch(1_000), batch(2_000), batch(20)
        assert_eq!(p3.len(), 3);
        assert_eq!(p3[0].num_rows(), 1_000);
        assert_eq!(p3[1].num_rows(), 2_000);
        assert_eq!(p3[2].num_rows(), 20);

        Ok(())
    }

    #[test]
    fn test_repartition_with_sort_information() -> Result<()> {
        let schema = schema();
        let sort_key = LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }]);
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
}
