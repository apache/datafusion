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
        Ok(DataSourceExec::from_data_source(source))
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
    use crate::tests::{aggr_test_schema, make_partition};

    use super::*;

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
}
