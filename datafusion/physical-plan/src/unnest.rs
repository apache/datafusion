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

//! Define a plan for unnesting values in columns that contain a list type.

use std::cmp::{self, Ordering};
use std::sync::Arc;
use std::task::{Poll, ready};

use super::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
    MetricsSet, SplitMetrics,
};
use super::{DisplayAs, ExecutionPlanProperties, PlanProperties};
use crate::stream::{BatchSplitStream, EmptyRecordBatchStream, ObservedStream};
use crate::{
    ChildrenPropertiesMode, DisplayFormatType, Distribution, ExecutionPlan,
    RecordBatchStream, ReplaceChildrenOptions, SendableRecordBatchStream,
    validate_child_count,
};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanBufferBuilder, FixedSizeListArray, Int64Array,
    LargeListArray, LargeListViewArray, ListArray, ListViewArray, PrimitiveArray, Scalar,
    StructArray, new_null_array,
};
use arrow::compute::kernels::length::length;
use arrow::compute::kernels::zip::zip;
use arrow::compute::{cast, is_not_null, kernels, sum};
use arrow::datatypes::{DataType, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_ord::cmp::lt;
use async_trait::async_trait;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    Constraints, HashMap, HashSet, Result, UnnestOptions, exec_datafusion_err, exec_err,
    internal_err,
};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::equivalence::ProjectionMapping;
use datafusion_physical_expr::expressions::Column;
use futures::{Stream, StreamExt};
use log::trace;

/// Unnest the given columns (either with type struct or list)
/// For list unnesting, each row is vertically transformed into multiple rows
/// For struct unnesting, each column is horizontally transformed into multiple columns,
/// Thus the original RecordBatch with dimension (n x m) may have new dimension (n' x m')
///
/// See [`UnnestOptions`] for more details and an example.
#[derive(Debug, Clone)]
pub struct UnnestExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// The schema once the unnest is applied
    schema: SchemaRef,
    /// Indices of the list-typed columns in the input schema
    list_column_indices: Vec<ListUnnest>,
    /// Indices of the struct-typed columns in the input schema
    struct_column_indices: Vec<usize>,
    /// Options
    options: UnnestOptions,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

impl UnnestExec {
    /// Create a new [UnnestExec].
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        list_column_indices: Vec<ListUnnest>,
        struct_column_indices: Vec<usize>,
        schema: SchemaRef,
        options: UnnestOptions,
    ) -> Result<Self> {
        let cache = Self::compute_properties(
            &input,
            &list_column_indices,
            &struct_column_indices,
            &schema,
        )?;

        Ok(UnnestExec {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            metrics: Default::default(),
            cache: Arc::new(cache),
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        list_column_indices: &[ListUnnest],
        struct_column_indices: &[usize],
        schema: &SchemaRef,
    ) -> Result<PlanProperties> {
        // Find out which indices are not unnested, such that they can be copied over from the input plan
        let input_schema = input.schema();
        let mut unnested_indices = BooleanBufferBuilder::new(input_schema.fields().len());
        unnested_indices.append_n(input_schema.fields().len(), false);
        for list_unnest in list_column_indices {
            unnested_indices.set_bit(list_unnest.index_in_input_schema, true);
        }
        for struct_unnest in struct_column_indices {
            unnested_indices.set_bit(*struct_unnest, true)
        }
        let unnested_indices = unnested_indices.finish();
        let non_unnested_indices: Vec<usize> = (0..input_schema.fields().len())
            .filter(|idx| !unnested_indices.value(*idx))
            .collect();

        // Manually build projection mapping from non-unnested input columns to their positions in the output
        let input_schema = input.schema();
        let projection_mapping: ProjectionMapping = non_unnested_indices
            .iter()
            .map(|&input_idx| {
                // Find what index the input column has in the output schema
                let input_field = input_schema.field(input_idx);
                let output_idx = schema
                    .fields()
                    .iter()
                    .position(|output_field| output_field.name() == input_field.name())
                    .ok_or_else(|| {
                        exec_datafusion_err!(
                            "Non-unnested column '{}' must exist in output schema",
                            input_field.name()
                        )
                    })?;

                let input_col = Arc::new(Column::new(input_field.name(), input_idx))
                    as Arc<dyn PhysicalExpr>;
                let target_col = Arc::new(Column::new(input_field.name(), output_idx))
                    as Arc<dyn PhysicalExpr>;
                // Use From<Vec<(Arc<dyn PhysicalExpr>, usize)>> for ProjectionTargets
                let targets = vec![(target_col, output_idx)].into();
                Ok((input_col, targets))
            })
            .collect::<Result<ProjectionMapping>>()?;

        // Create the unnest's equivalence properties by copying the input plan's equivalence properties
        // for the unaffected columns. Except for the constraints, which are removed entirely because
        // the unnest operation invalidates any global uniqueness or primary-key constraints.
        let input_eq_properties = input.equivalence_properties();
        let eq_properties = input_eq_properties
            .project(&projection_mapping, Arc::clone(schema))
            .with_constraints(Constraints::default());

        // Output partitioning must use the projection mapping
        let output_partitioning = input
            .output_partitioning()
            .project(&projection_mapping, &eq_properties);

        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            input.pipeline_behavior(),
            input.boundedness(),
        ))
    }

    /// Input execution plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Indices of the list-typed columns in the input schema
    pub fn list_column_indices(&self) -> &[ListUnnest] {
        &self.list_column_indices
    }

    /// Indices of the struct-typed columns in the input schema
    pub fn struct_column_indices(&self) -> &[usize] {
        &self.struct_column_indices
    }

    pub fn options(&self) -> &UnnestOptions {
        &self.options
    }
}

impl DisplayAs for UnnestExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "UnnestExec")
            }
            DisplayFormatType::TreeRender => {
                write!(f, "")
            }
        }
    }
}

impl ExecutionPlan for UnnestExec {
    fn name(&self) -> &'static str {
        "UnnestExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input: children.swap_remove(0),
                metrics: ExecutionPlanMetricsSet::new(),
                ..Self::clone(&*self)
            })),
            ChildrenPropertiesMode::Recompute => Ok(Arc::new(UnnestExec::new(
                children.swap_remove(0),
                self.list_column_indices.clone(),
                self.struct_column_indices.clone(),
                Arc::clone(&self.schema),
                self.options.clone(),
            )?)),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> crate::InputDistributionRequirements {
        crate::InputDistributionRequirements::new(vec![
            Distribution::UnspecifiedDistribution,
        ])
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let input = self.input.execute(partition, context)?;
        let metrics = UnnestMetrics::new(partition, &self.metrics);
        let baseline_metrics = metrics.baseline_metrics.clone();

        let stream = Box::pin(UnnestStream {
            input,
            schema: Arc::clone(&self.schema),
            list_type_columns: self.list_column_indices.clone(),
            struct_column_indices: self.struct_column_indices.iter().copied().collect(),
            options: self.options.clone(),
            metrics,
            batch_size,
            pending_input: None,
        });

        // Chunking the input bounds each build to roughly `batch_size` rows, but two cases
        // can still produce an oversized batch (see `predict_output_lens`), so the output
        // goes through the shared splitter to make the bound unconditional.
        let stream = Box::pin(BatchSplitStream::new(
            stream,
            batch_size,
            SplitMetrics::new(&self.metrics, partition),
        ));
        Ok(Box::pin(ObservedStream::new(
            stream,
            baseline_metrics,
            None,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;

        // Exhaustive destructure: adding a field to `UnnestExec` without
        // deciding how it is serialized is a compile error, not a silent
        // round-trip gap.
        let Self {
            input,
            schema,
            list_column_indices,
            struct_column_indices,
            options,
            // Runtime execution state, rebuilt empty on decode.
            metrics: _,
            // Derived at construction by `UnnestExec::compute_properties`.
            cache: _,
        } = self;

        let input = ctx.encode_child(input)?;
        let schema = schema.as_ref().try_into()?;
        let list_type_columns = list_column_indices
            .iter()
            .map(|column| protobuf::ListUnnest {
                index_in_input_schema: column.index_in_input_schema as _,
                depth: column.depth as _,
            })
            .collect();
        let struct_type_columns = struct_column_indices
            .iter()
            .map(|index| *index as _)
            .collect();
        let null_handling = {
            use datafusion_common::NullHandling;
            use protobuf::unnest_options::NullHandling as ProtoNullHandling;
            match options.null_handling {
                NullHandling::Preserve => ProtoNullHandling::Preserve,
                NullHandling::Drop => ProtoNullHandling::Drop,
                NullHandling::PreserveAndExpandEmpty => {
                    ProtoNullHandling::PreserveAndExpandEmpty
                }
            }
        } as i32;
        let options = protobuf::UnnestOptions {
            null_handling,
            recursions: options
                .recursions
                .iter()
                .map(|recursion| protobuf::RecursionUnnestOption {
                    input_column: Some((&recursion.input_column).into()),
                    output_column: Some((&recursion.output_column).into()),
                    depth: recursion.depth as _,
                })
                .collect(),
        };

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Unnest(Box::new(
                    protobuf::UnnestExecNode {
                        input: Some(Box::new(input)),
                        schema: Some(schema),
                        list_type_columns,
                        struct_type_columns,
                        options: Some(options),
                    },
                )),
            ),
        }))
    }
}

#[cfg(feature = "proto")]
impl UnnestExec {
    /// Reconstruct an [`UnnestExec`] from its protobuf representation.
    ///
    /// The exact inverse of [`ExecutionPlan::try_to_proto`].
    ///
    /// [`ExecutionPlan::try_to_proto`]: crate::ExecutionPlan::try_to_proto
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use datafusion_common::utils::usize_from_wire;
        use datafusion_proto_models::protobuf;

        let unnest = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Unnest,
            "UnnestExec",
        );
        // Exhaustive destructure: a new field on `UnnestExecNode` is a compile
        // error here rather than a silently ignored wire field.
        let protobuf::UnnestExecNode {
            input,
            schema,
            list_type_columns,
            struct_type_columns,
            options,
        } = unnest.as_ref();

        let input = ctx.decode_required_child(input.as_deref(), "UnnestExec", "input")?;
        let schema: Schema = schema
            .as_ref()
            .ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "UnnestExec is missing required field 'schema'"
                )
            })?
            .try_into()?;
        let list_column_indices = list_type_columns
            .iter()
            .map(|column| ListUnnest {
                index_in_input_schema: column.index_in_input_schema as _,
                depth: column.depth as _,
            })
            .collect();
        let struct_column_indices = struct_type_columns
            .iter()
            .map(|index| usize_from_wire(*index, "UnnestExec", "struct_type_columns"))
            .collect::<Result<Vec<_>>>()?;
        let options = options.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "UnnestExec is missing required field 'options'"
            )
        })?;
        let null_handling = {
            use datafusion_common::NullHandling;
            use protobuf::unnest_options::NullHandling as ProtoNullHandling;
            match ProtoNullHandling::try_from(options.null_handling) {
                Ok(ProtoNullHandling::Preserve) => NullHandling::Preserve,
                Ok(ProtoNullHandling::Drop) => NullHandling::Drop,
                Ok(ProtoNullHandling::PreserveAndExpandEmpty) => {
                    NullHandling::PreserveAndExpandEmpty
                }
                // Unknown enum values fall back to the default (Preserve),
                // matching DataFusion's historical behavior.
                Err(_) => NullHandling::Preserve,
            }
        };
        let options = UnnestOptions {
            null_handling,
            recursions: options
                .recursions
                .iter()
                .map(|recursion| datafusion_common::RecursionUnnestOption {
                    input_column: recursion.input_column.as_ref().unwrap().into(),
                    output_column: recursion.output_column.as_ref().unwrap().into(),
                    depth: recursion.depth as _,
                })
                .collect(),
        };

        Ok(Arc::new(UnnestExec::new(
            input,
            list_column_indices,
            struct_column_indices,
            Arc::new(schema),
            options,
        )?))
    }
}

#[derive(Clone, Debug)]
struct UnnestMetrics {
    /// Execution metrics
    baseline_metrics: BaselineMetrics,
    /// Number of batches consumed
    input_batches: metrics::Count,
    /// Number of rows consumed
    input_rows: metrics::Count,
}

impl UnnestMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let input_batches = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics)
            .with_category(MetricCategory::Rows)
            .counter("input_rows", partition);

        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            input_batches,
            input_rows,
        }
    }
}

/// A stream that issues [RecordBatch]es with unnested column data.
struct UnnestStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Unnested schema
    schema: Arc<Schema>,
    /// represents all unnest operations to be applied to the input (input index, depth)
    /// e.g unnest(col1),unnest(unnest(col1)) where col1 has index 1 in original input schema
    /// then list_type_columns = [ListUnnest{1,1},ListUnnest{1,2}]
    list_type_columns: Vec<ListUnnest>,
    struct_column_indices: HashSet<usize>,
    /// Options
    options: UnnestOptions,
    /// Metrics
    metrics: UnnestMetrics,
    /// Target number of rows per output batch, from `datafusion.execution.batch_size`.
    batch_size: usize,
    /// Rows of the current input batch that have not been unnested yet. Unnesting one
    /// input batch can produce arbitrarily many output rows, so the input is consumed in
    /// chunks small enough that each chunk's output stays near `batch_size`.
    ///
    /// Note the scope of the memory bound this buys: chunking removes the input batch size
    /// from the peak, but not the length of an individual list. A single row whose list is
    /// longer than `batch_size`, and recursive unnesting (where the expansion cannot be
    /// predicted up front), both still materialize their full expansion in one build.
    pending_input: Option<PendingInput>,
}

/// An input batch being unnested incrementally, a chunk of rows at a time.
struct PendingInput {
    /// The full input batch. Rows before `row_offset` have already been unnested.
    batch: RecordBatch,
    /// Index of the next input row to unnest.
    row_offset: usize,
    /// How many output rows each input row expands into, indexed by input row.
    ///
    /// `None` when the expansion cannot be predicted from the input alone, in which case
    /// the whole remaining input is unnested in one call and only the output is split.
    /// See [`UnnestStream::predict_output_lens`].
    output_lens: Option<PrimitiveArray<Int64Type>>,
}

impl PendingInput {
    fn remaining_rows(&self) -> usize {
        self.batch.num_rows() - self.row_offset
    }

    /// How many input rows to unnest next so the resulting batch holds at most
    /// `batch_size` rows.
    fn next_chunk_rows(&self, batch_size: usize) -> usize {
        let Some(output_lens) = &self.output_lens else {
            return self.remaining_rows();
        };

        let lens = &output_lens.values()[self.row_offset..];
        let batch_size = batch_size as i64;
        let mut output_rows = 0i64;
        for (rows, len) in lens.iter().enumerate() {
            // The first row is always taken, even if it alone overshoots `batch_size`: an
            // input row is never split across builds, so this is what guarantees progress.
            // An oversized build is sliced down by `BatchSplitStream` on the way out.
            if rows > 0 && output_rows + len > batch_size {
                return rows;
            }
            output_rows += len;
        }
        lens.len()
    }

    /// The per-row output lengths covering the next `rows` input rows, so the unnesting
    /// does not have to recompute what `predict_output_lens` already derived.
    fn chunk_lengths(&self, rows: usize) -> Option<PrimitiveArray<Int64Type>> {
        self.output_lens
            .as_ref()
            .map(|lens| lens.slice(self.row_offset, rows))
    }
}

impl RecordBatchStream for UnnestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait]
impl Stream for UnnestStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl UnnestStream {
    /// Separate implementation function that unpins the [`UnnestStream`] so
    /// that partial borrows work correctly
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            // Unnest the next chunk of the input batch already in hand.
            if let Some(pending) = self.pending_input.as_mut() {
                // `PendingInput` is only built from a non-empty batch and `next_chunk_rows`
                // always consumes at least one row, so it is dropped the moment it drains.
                debug_assert!(pending.remaining_rows() > 0);

                let rows = pending.next_chunk_rows(self.batch_size);
                let chunk = pending.batch.slice(pending.row_offset, rows);
                let chunk_lengths = pending.chunk_lengths(rows);
                pending.row_offset += rows;
                let drained = pending.remaining_rows() == 0;

                let timer = self.metrics.baseline_metrics.elapsed_compute().timer();
                let result = build_batch(
                    &chunk,
                    &self.schema,
                    &self.list_type_columns,
                    &self.struct_column_indices,
                    &self.options,
                    chunk_lengths.as_ref(),
                );
                timer.done();

                if drained {
                    self.pending_input = None;
                }

                // A chunk can legitimately produce no rows at all, for example when every
                // list in it is empty under `NullHandling::Drop`; `build_batch` signals
                // that with `None` rather than an empty batch.
                if let Some(batch) = result? {
                    debug_assert!(batch.num_rows() > 0);
                    return Poll::Ready(Some(Ok(batch)));
                }
                continue;
            }

            // Otherwise pull the next input batch.
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());
                    if batch.num_rows() > 0 {
                        let timer =
                            self.metrics.baseline_metrics.elapsed_compute().timer();
                        let output_lens = self.predict_output_lens(&batch);
                        timer.done();
                        self.pending_input = Some(PendingInput {
                            batch,
                            row_offset: 0,
                            output_lens: output_lens?,
                        });
                    }
                }
                // If the stream is depleted or returned an error, log the finish message:
                other => {
                    trace!(
                        "Processed {} probe-side input batches containing {} rows and \
                        produced {} output batches containing {} rows in {}",
                        self.metrics.input_batches,
                        self.metrics.input_rows,
                        self.metrics.baseline_metrics.output_batches(),
                        self.metrics.baseline_metrics.output_rows(),
                        self.metrics.baseline_metrics.elapsed_compute(),
                    );

                    // In the non-error case, i.e., input is simply depleted:
                    if other.is_none() {
                        // Release the input pipeline's resources.
                        let input_schema = self.input.schema();
                        self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    }

                    return Poll::Ready(other);
                }
            }
        }
    }

    /// Compute how many output rows each input row of `batch` will expand into, so the
    /// input can be chunked to keep each build bounded.
    ///
    /// Returns `None` when the count cannot be derived from the input alone, which is the
    /// signal to unnest the whole batch in one call:
    ///
    /// * With no list columns, unnesting only widens structs and leaves the row count
    ///   alone, so the output is already bounded by the input batch size.
    /// * With recursion (`depth > 1`), a row's expansion depends on the lengths of inner
    ///   lists that only exist after the outer levels have been unnested, so it cannot be
    ///   predicted up front.
    fn predict_output_lens(
        &self,
        batch: &RecordBatch,
    ) -> Result<Option<PrimitiveArray<Int64Type>>> {
        if self.list_type_columns.is_empty()
            || self
                .list_type_columns
                .iter()
                .any(|unnest| unnest.depth != 1)
        {
            return Ok(None);
        }

        let list_arrays: Vec<ArrayRef> = self
            .list_type_columns
            .iter()
            .map(|unnest| Arc::clone(batch.column(unnest.index_in_input_schema)))
            .collect();

        // This is exactly the per-row length that `list_unnest_at_level` derives when it
        // actually unnests, so the chunk boundaries are exact rather than estimated, and
        // each chunk's slice of it is handed back to `build_batch` instead of recomputed.
        let longest_length = find_longest_length(&list_arrays, &self.options)?;
        Ok(Some(longest_length.as_primitive::<Int64Type>().clone()))
    }
}

/// Given a set of struct column indices to flatten
/// try converting the column in input into multiple subfield columns
/// For example
/// struct_col: [a: struct(item: int, name: string), b: int]
/// with a batch
/// {a: {item: 1, name: "a"}, b: 2},
/// {a: {item: 3, name: "b"}, b: 4]
/// will be converted into
/// {a.item: 1, a.name: "a", b: 2},
/// {a.item: 3, a.name: "b", b: 4}
fn flatten_struct_cols(
    input_batch: &[Arc<dyn Array>],
    schema: &SchemaRef,
    struct_column_indices: &HashSet<usize>,
) -> Result<RecordBatch> {
    // horizontal expansion because of struct unnest
    let columns_expanded = input_batch
        .iter()
        .enumerate()
        .map(|(idx, column_data)| match struct_column_indices.get(&idx) {
            Some(_) => match column_data.data_type() {
                DataType::Struct(_) => {
                    let struct_arr =
                        column_data.as_any().downcast_ref::<StructArray>().unwrap();
                    Ok(struct_arr.columns().to_vec())
                }
                data_type => internal_err!(
                    "expecting column {idx} from input plan to be a struct, got {data_type}"
                ),
            },
            None => Ok(vec![Arc::clone(column_data)]),
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(RecordBatch::try_new(Arc::clone(schema), columns_expanded)?)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct ListUnnest {
    pub index_in_input_schema: usize,
    pub depth: usize,
}

/// This function is used to execute the unnesting on multiple columns all at once, but
/// one level at a time, and is called n times, where n is the highest recursion level among
/// the unnest exprs in the query.
///
/// For example giving the following query:
/// ```sql
/// select unnest(colA, max_depth:=3) as P1, unnest(colA,max_depth:=2) as P2, unnest(colB, max_depth:=1) as P3 from temp;
/// ```
/// Then the total times this function being called is 3
///
/// It needs to be aware of which level the current unnesting is, because if there exists
/// multiple unnesting on the same column, but with different recursion levels, say
/// **unnest(colA, max_depth:=3)** and **unnest(colA, max_depth:=2)**, then the unnesting
/// of expr **unnest(colA, max_depth:=3)** will start at level 3, while unnesting for expr
/// **unnest(colA, max_depth:=2)** has to start at level 2
///
/// Set *colA* as a 3-dimension columns and *colB* as an array (1-dimension). As stated,
/// this function is called with the descending order of recursion depth
///
/// Depth = 3
/// - colA(3-dimension) unnest into temp column temp_P1(2_dimension) (unnesting of P1 starts
///   from this level)
/// - colA(3-dimension) having indices repeated by the unnesting operation above
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 2
/// - temp_P1(2-dimension) unnest into temp column temp_P1(1-dimension)
/// - colA(3-dimension) unnest into temp column temp_P2(2-dimension) (unnesting of P2 starts
///   from this level)
/// - colB(1-dimension) having indices repeated by the unnesting operation above
///
/// Depth = 1
/// - temp_P1(1-dimension) unnest into P1
/// - temp_P2(2-dimension) unnest into P2
/// - colB(1-dimension) unnest into P3 (unnesting of P3 starts from this level)
///
/// The returned array will has the same size as the input batch
/// and only contains original columns that are not being unnested.
fn list_unnest_at_level(
    batch: &[ArrayRef],
    list_type_unnests: &[ListUnnest],
    temp_unnested_arrs: &mut HashMap<ListUnnest, ArrayRef>,
    level_to_unnest: usize,
    options: &UnnestOptions,
    precomputed_lengths: Option<&PrimitiveArray<Int64Type>>,
) -> Result<Option<Vec<ArrayRef>>> {
    // Extract unnestable columns at this level
    let (arrs_to_unnest, list_unnest_specs): (Vec<Arc<dyn Array>>, Vec<_>) =
        list_type_unnests
            .iter()
            .filter_map(|unnesting| {
                if level_to_unnest == unnesting.depth {
                    return Some((
                        Arc::clone(&batch[unnesting.index_in_input_schema]),
                        *unnesting,
                    ));
                }
                // This means the unnesting on this item has started at higher level
                // and need to continue until depth reaches 1
                if level_to_unnest < unnesting.depth {
                    return Some((
                        Arc::clone(temp_unnested_arrs.get(unnesting).unwrap()),
                        *unnesting,
                    ));
                }
                None
            })
            .unzip();

    // Filter out so that list_arrays only contain column with the highest depth
    // at the same time, during iteration remove this depth so next time we don't have to unnest them again
    //
    // The caller may already have computed these lengths to decide how many input rows to
    // feed us; reusing them avoids running the kernel chain twice over the same rows.
    // Cloning is an `Arc` bump on the underlying buffer, not a copy.
    let longest_length = match precomputed_lengths {
        Some(lengths) => lengths.clone(),
        None => find_longest_length(&arrs_to_unnest, options)?
            .as_primitive::<Int64Type>()
            .clone(),
    };
    let unnested_length = &longest_length;
    let total_length = if unnested_length.is_empty() {
        0
    } else {
        sum(unnested_length).ok_or_else(|| {
            exec_datafusion_err!("Failed to calculate the total unnested length")
        })? as usize
    };
    if total_length == 0 {
        return Ok(None);
    }

    // Unnest all the list arrays
    let unnested_temp_arrays =
        unnest_list_arrays(arrs_to_unnest.as_ref(), unnested_length, total_length)?;

    // Create the take indices array for other columns
    let take_indices = create_take_indices(unnested_length, total_length);
    unnested_temp_arrays
        .into_iter()
        .zip(list_unnest_specs.iter())
        .for_each(|(flatten_arr, unnesting)| {
            temp_unnested_arrs.insert(*unnesting, flatten_arr);
        });

    let repeat_mask: Vec<bool> = batch
        .iter()
        .enumerate()
        .map(|(i, _)| {
            // Check if the column is needed in future levels (levels below the current one)
            let needed_in_future_levels = list_type_unnests.iter().any(|unnesting| {
                unnesting.index_in_input_schema == i && unnesting.depth < level_to_unnest
            });

            // Check if the column is involved in unnesting at any level
            let is_involved_in_unnesting = list_type_unnests
                .iter()
                .any(|unnesting| unnesting.index_in_input_schema == i);

            // Repeat columns needed in future levels or not unnested.
            needed_in_future_levels || !is_involved_in_unnesting
        })
        .collect();

    // Dimension of arrays in batch is untouched, but the values are repeated
    // as the side effect of unnesting
    let ret = repeat_arrs_from_indices(batch, &take_indices, &repeat_mask)?;

    Ok(Some(ret))
}
struct UnnestingResult {
    arr: ArrayRef,
    depth: usize,
}

/// For each row in a `RecordBatch`, some list/struct columns need to be unnested.
/// - For list columns: We will expand the values in each list into multiple rows,
///   taking the longest length among these lists, and shorter lists are padded with NULLs.
/// - For struct columns: We will expand the struct columns into multiple subfield columns.
///
/// For columns that don't need to be unnested, repeat their values until reaching the longest length.
///
/// Note: unnest has a big difference in behavior between Postgres and DuckDB
///
/// Take this example
///
/// 1. Postgres
/// ```ignored
/// create table temp (
///     i integer[][][], j integer[]
/// )
/// insert into temp values ('{{{1,2},{3,4}},{{5,6},{7,8}}}', '{1,2}');
/// select unnest(i), unnest(j) from temp;
/// ```
///
/// Result
/// ```text
///     1   1
///     2   2
///     3
///     4
///     5
///     6
///     7
///     8
/// ```
/// 2. DuckDB
/// ```ignore
///     create table temp (i integer[][][], j integer[]);
///     insert into temp values ([[[1,2],[3,4]],[[5,6],[7,8]]], [1,2]);
///     select unnest(i,recursive:=true), unnest(j,recursive:=true) from temp;
/// ```
/// Result:
/// ```text
///
///     ┌────────────────────────────────────────────────┬────────────────────────────────────────────────┐
///     │ unnest(i, "recursive" := CAST('t' AS BOOLEAN)) │ unnest(j, "recursive" := CAST('t' AS BOOLEAN)) │
///     │                     int32                      │                     int32                      │
///     ├────────────────────────────────────────────────┼────────────────────────────────────────────────┤
///     │                                              1 │                                              1 │
///     │                                              2 │                                              2 │
///     │                                              3 │                                              1 │
///     │                                              4 │                                              2 │
///     │                                              5 │                                              1 │
///     │                                              6 │                                              2 │
///     │                                              7 │                                              1 │
///     │                                              8 │                                              2 │
///     └────────────────────────────────────────────────┴────────────────────────────────────────────────┘
/// ```
///
/// The following implementation refer to DuckDB's implementation
fn build_batch(
    batch: &RecordBatch,
    schema: &SchemaRef,
    list_type_columns: &[ListUnnest],
    struct_column_indices: &HashSet<usize>,
    options: &UnnestOptions,
    precomputed_lengths: Option<&PrimitiveArray<Int64Type>>,
) -> Result<Option<RecordBatch>> {
    let transformed = match list_type_columns.len() {
        0 => flatten_struct_cols(batch.columns(), schema, struct_column_indices),
        _ => {
            let mut temp_unnested_result = HashMap::new();
            let max_recursion = list_type_columns
                .iter()
                .fold(0, |highest_depth, ListUnnest { depth, .. }| {
                    cmp::max(highest_depth, *depth)
                });

            // This arr always has the same column count with the input batch
            let mut flatten_arrs = vec![];

            // Original batch has the same columns
            // All unnesting results are written to temp_batch
            for depth in (1..=max_recursion).rev() {
                let input = match depth == max_recursion {
                    true => batch.columns(),
                    false => &flatten_arrs,
                };
                // Only sound for a single non-recursive level: with recursion the deeper
                // levels' lengths depend on arrays that do not exist yet, which is also why
                // the caller does not predict lengths in that case.
                let level_lengths = if max_recursion == 1 {
                    precomputed_lengths
                } else {
                    None
                };
                let Some(temp_result) = list_unnest_at_level(
                    input,
                    list_type_columns,
                    &mut temp_unnested_result,
                    depth,
                    options,
                    level_lengths,
                )?
                else {
                    return Ok(None);
                };
                flatten_arrs = temp_result;
            }
            let unnested_array_map: HashMap<usize, Vec<UnnestingResult>> =
                temp_unnested_result.into_iter().fold(
                    HashMap::new(),
                    |mut acc,
                     (
                        ListUnnest {
                            index_in_input_schema,
                            depth,
                        },
                        flattened_array,
                    )| {
                        acc.entry(index_in_input_schema).or_default().push(
                            UnnestingResult {
                                arr: flattened_array,
                                depth,
                            },
                        );
                        acc
                    },
                );
            let output_order: HashMap<ListUnnest, usize> = list_type_columns
                .iter()
                .enumerate()
                .map(|(order, unnest_def)| (*unnest_def, order))
                .collect();

            // One original column may be unnested multiple times into separate columns
            let mut multi_unnested_per_original_index = unnested_array_map
                .into_iter()
                .map(
                    // Each item in unnested_columns is the result of unnesting the same input column
                    // we need to sort them to conform with the original expression order
                    // e.g unnest(unnest(col)) must goes before unnest(col)
                    |(original_index, mut unnested_columns)| {
                        unnested_columns.sort_by(
                            |UnnestingResult { depth: depth1, .. },
                             UnnestingResult { depth: depth2, .. }|
                             -> Ordering {
                                output_order
                                    .get(&ListUnnest {
                                        depth: *depth1,
                                        index_in_input_schema: original_index,
                                    })
                                    .unwrap()
                                    .cmp(
                                        output_order
                                            .get(&ListUnnest {
                                                depth: *depth2,
                                                index_in_input_schema: original_index,
                                            })
                                            .unwrap(),
                                    )
                            },
                        );
                        (
                            original_index,
                            unnested_columns
                                .into_iter()
                                .map(|result| result.arr)
                                .collect::<Vec<_>>(),
                        )
                    },
                )
                .collect::<HashMap<_, _>>();

            let ret = flatten_arrs
                .into_iter()
                .enumerate()
                .flat_map(|(col_idx, arr)| {
                    // Convert original column into its unnested version(s)
                    // Plural because one column can be unnested with different recursion level
                    // and into separate output columns
                    match multi_unnested_per_original_index.remove(&col_idx) {
                        Some(unnested_arrays) => unnested_arrays,
                        None => vec![arr],
                    }
                })
                .collect::<Vec<_>>();

            flatten_struct_cols(&ret, schema, struct_column_indices)
        }
    }?;
    Ok(Some(transformed))
}

/// Find the longest list length among the given list arrays for each row.
///
/// For example if we have the following two list arrays:
///
/// ```ignore
/// l1: [1, 2, 3], null, [], [3]
/// l2: [4,5], [], null, [6, 7]
/// ```
///
/// With [`datafusion_common::NullHandling::Drop`], the longest length array will be:
///
/// ```ignore
/// longest_length: [3, 0, 0, 2]
/// ```
///
/// With [`datafusion_common::NullHandling::Preserve`] (the default), the longest length array
/// will be:
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
///
/// With [`datafusion_common::NullHandling::PreserveAndExpandEmpty`], empty input lists are
/// also bumped to length 1 so they produce a single `NULL` output row:
///
/// ```ignore
/// longest_length: [3, 1, 1, 2]
/// ```
fn find_longest_length(
    list_arrays: &[ArrayRef],
    options: &UnnestOptions,
) -> Result<ArrayRef> {
    // The length to substitute for a NULL input list.
    let null_length = if options.preserve_nulls() {
        Scalar::new(Int64Array::from_value(1, 1))
    } else {
        Scalar::new(Int64Array::from_value(0, 1))
    };
    let expand_empty = options.expand_empty_as_null();
    // Reused scalars for the empty-list rewrite when expand_empty is set.
    let zero = Scalar::new(Int64Array::from_value(0, 1));
    let one = Scalar::new(Int64Array::from_value(1, 1));
    let list_lengths: Vec<ArrayRef> = list_arrays
        .iter()
        .map(|list_array| {
            let mut length_array = length(list_array)?;
            // Make sure length arrays have the same type. Int64 is the most general one.
            length_array = cast(&length_array, &DataType::Int64)?;
            length_array =
                zip(&is_not_null(&length_array)?, &length_array, &null_length)?;
            if expand_empty {
                // Bump empty lists (length 0) to length 1 so they
                // produce a single output row padded with NULL.
                let is_zero = arrow_ord::cmp::eq(&length_array, &zero)?;
                length_array = zip(&is_zero, &one, &length_array)?;
            }
            Ok(length_array)
        })
        .collect::<Result<_>>()?;

    let longest_length = list_lengths.iter().skip(1).try_fold(
        Arc::clone(&list_lengths[0]),
        |longest, current| {
            let is_lt = lt(&longest, &current)?;
            zip(&is_lt, &current, &longest)
        },
    )?;
    Ok(longest_length)
}

/// Trait defining common methods used for unnesting, implemented by list array types.
trait ListArrayType: Array {
    /// Returns a reference to the values of this list.
    fn values(&self) -> &ArrayRef;

    /// Returns the start and end offset of the values for the given row.
    fn value_offsets(&self, row: usize) -> (i64, i64);
}

impl ListArrayType for ListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row].into(), offsets[row + 1].into())
    }
}

impl ListArrayType for LargeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offsets = self.value_offsets();
        (offsets[row], offsets[row + 1])
    }
}

impl ListArrayType for FixedSizeListArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let start = self.value_offset(row) as i64;
        (start, start + self.value_length() as i64)
    }
}

impl ListArrayType for ListViewArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offset = self.value_offsets()[row] as i64;
        let size = self.value_sizes()[row] as i64;
        (offset, offset + size)
    }
}

impl ListArrayType for LargeListViewArray {
    fn values(&self) -> &ArrayRef {
        self.values()
    }

    fn value_offsets(&self, row: usize) -> (i64, i64) {
        let offset = self.value_offsets()[row];
        let size = self.value_sizes()[row];
        (offset, offset + size)
    }
}

/// Unnest multiple list arrays according to the length array.
fn unnest_list_arrays(
    list_arrays: &[ArrayRef],
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<Vec<ArrayRef>> {
    let typed_arrays = list_arrays
        .iter()
        .map(|list_array| match list_array.data_type() {
            DataType::List(_) => Ok(list_array.as_list::<i32>() as &dyn ListArrayType),
            DataType::LargeList(_) => {
                Ok(list_array.as_list::<i64>() as &dyn ListArrayType)
            }
            DataType::FixedSizeList(_, _) => {
                Ok(list_array.as_fixed_size_list() as &dyn ListArrayType)
            }
            DataType::ListView(_) => {
                Ok(list_array.as_list_view::<i32>() as &dyn ListArrayType)
            }
            DataType::LargeListView(_) => {
                Ok(list_array.as_list_view::<i64>() as &dyn ListArrayType)
            }
            other => exec_err!("Invalid unnest datatype {other }"),
        })
        .collect::<Result<Vec<_>>>()?;

    typed_arrays
        .iter()
        .map(|list_array| unnest_list_array(*list_array, length_array, capacity))
        .collect::<Result<_>>()
}

/// Unnest a list array according the target length array.
///
/// Consider a list array like this:
///
/// ```ignore
/// [1], [2, 3, 4], null, [5], [],
/// ```
///
/// and the length array is:
///
/// ```ignore
/// [2, 3, 2, 1, 2]
/// ```
///
/// If the length of a certain list is less than the target length, pad with NULLs.
/// So the unnested array will look like this:
///
/// ```ignore
/// [1, null, 2, 3, 4, null, null, 5, null, null]
/// ```
fn unnest_list_array(
    list_array: &dyn ListArrayType,
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> Result<ArrayRef> {
    let values = list_array.values();
    let mut take_indices_builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for row in 0..list_array.len() {
        let mut value_length = 0;
        if !list_array.is_null(row) {
            let (start, end) = list_array.value_offsets(row);
            value_length = end - start;
            for i in start..end {
                take_indices_builder.append_value(i)
            }
        }
        let target_length = length_array.value(row);
        debug_assert!(
            value_length <= target_length,
            "value length is beyond the longest length"
        );
        // Pad with NULL values
        for _ in value_length..target_length {
            take_indices_builder.append_null();
        }
    }
    Ok(kernels::take::take(
        &values,
        &take_indices_builder.finish(),
        None,
    )?)
}

/// Creates take indices that will be used to expand all columns except for the list type
/// [`columns`](UnnestExec::list_column_indices) that is being unnested.
/// Every column value needs to be repeated multiple times according to the length array.
///
/// If the length array looks like this:
///
/// ```ignore
/// [2, 3, 1]
/// ```
/// Then [`create_take_indices`] will return an array like this
///
/// ```ignore
/// [0, 0, 1, 1, 1, 2]
/// ```
fn create_take_indices(
    length_array: &PrimitiveArray<Int64Type>,
    capacity: usize,
) -> PrimitiveArray<Int64Type> {
    // `find_longest_length()` guarantees this.
    debug_assert!(
        length_array.null_count() == 0,
        "length array should not contain nulls"
    );
    let mut builder = PrimitiveArray::<Int64Type>::builder(capacity);
    for (index, repeat) in length_array.iter().enumerate() {
        // The length array should not contain nulls, so unwrap is safe
        let repeat = repeat.unwrap();
        (0..repeat).for_each(|_| builder.append_value(index as i64));
    }
    builder.finish()
}

/// Create a batch of arrays based on an input `batch` and a `indices` array.
/// The `indices` array is used by the take kernel to repeat values in the arrays
/// that are marked with `true` in the `repeat_mask`. Arrays marked with `false`
/// in the `repeat_mask` will be replaced with arrays filled with nulls of the
/// appropriate length.
///
/// For example if we have the following batch:
///
/// ```ignore
/// c1: [1], null, [2, 3, 4], null, [5, 6]
/// c2: 'a', 'b',  'c', null, 'd'
/// ```
///
/// then the `unnested_list_arrays` contains the unnest column that will replace `c1` in
/// the final batch if `preserve_nulls` is true:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// ```
///
/// And the `indices` array contains the indices that are used by `take` kernel to
/// repeat the values in `c2`:
///
/// ```ignore
/// 0, 1, 2, 2, 2, 3, 4, 4
/// ```
///
/// so that the final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6
/// c2: 'a', 'b', 'c', 'c', 'c', null, 'd', 'd'
/// ```
///
/// The `repeat_mask` determines whether an array's values are repeated or replaced with nulls.
/// For example, if the `repeat_mask` is:
///
/// ```ignore
/// [true, false]
/// ```
///
/// The final batch will look like:
///
/// ```ignore
/// c1: 1, null, 2, 3, 4, null, 5, 6  // Repeated using `indices`
/// c2: null, null, null, null, null, null, null, null  // Replaced with nulls
fn repeat_arrs_from_indices(
    batch: &[ArrayRef],
    indices: &PrimitiveArray<Int64Type>,
    repeat_mask: &[bool],
) -> Result<Vec<Arc<dyn Array>>> {
    batch
        .iter()
        .zip(repeat_mask.iter())
        .map(|(arr, &repeat)| {
            if repeat {
                Ok(kernels::take::take(arr, indices, None)?)
            } else {
                Ok(new_null_array(arr.data_type(), arr.len()))
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        GenericListArray, Int32Array, NullBufferBuilder, OffsetSizeTrait, StringArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_common::NullHandling;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_physical_expr_common::metrics::MetricValue;
    use insta::assert_snapshot;

    // Create a GenericListArray with the following list values:
    //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
    fn make_generic_array<OffsetSize>() -> GenericListArray<OffsetSize>
    where
        OffsetSize: OffsetSizeTrait,
    {
        let mut values = vec![];
        let mut offsets: Vec<OffsetSize> = vec![OffsetSize::zero()];
        let mut valid = NullBufferBuilder::new(6);

        // [A, B, C]
        values.extend_from_slice(&[Some("A"), Some("B"), Some("C")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_non_null();

        // []
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_non_null();

        // NULL with non-zero value length
        // Issue https://github.com/apache/datafusion/issues/9932
        values.push(Some("?"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_null();

        // [D]
        values.push(Some("D"));
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_non_null();

        // Another NULL with zero value length
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_null();

        // [NULL, F]
        values.extend_from_slice(&[None, Some("F")]);
        offsets.push(OffsetSize::from_usize(values.len()).unwrap());
        valid.append_non_null();

        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        GenericListArray::<OffsetSize>::new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(StringArray::from(values)),
            valid.finish(),
        )
    }

    // Create a FixedSizeListArray with the following list values:
    //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
    fn make_fixed_list() -> FixedSizeListArray {
        let values = Arc::new(StringArray::from_iter([
            Some("A"),
            Some("B"),
            None,
            None,
            Some("C"),
            Some("D"),
            None,
            None,
            None,
            Some("F"),
            None,
            None,
        ]));
        let field = Arc::new(Field::new_list_field(DataType::Utf8, true));
        let valid = NullBuffer::from(vec![true, false, true, false, true, true]);
        FixedSizeListArray::new(field, 2, values, Some(valid))
    }

    fn verify_unnest_list_array(
        list_array: &dyn ListArrayType,
        lengths: Vec<i64>,
        expected: Vec<Option<&str>>,
    ) -> Result<()> {
        let length_array = Int64Array::from(lengths);
        let unnested_array = unnest_list_array(list_array, &length_array, 3 * 6)?;
        let strs = unnested_array.as_string::<i32>().iter().collect::<Vec<_>>();
        assert_eq!(strs, expected);
        Ok(())
    }

    #[test]
    fn test_build_batch_list_arr_recursive() -> Result<()> {
        // col1                             | col2
        // [[1,2,3],null,[4,5]]             | ['a','b']
        // [[7,8,9,10], null, [11,12,13]]   | ['c','d']
        // null                             | ['e']
        let list_arr1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(7), Some(8), Some(9), Some(10)]),
            None,
            Some(vec![Some(11), Some(12), Some(13)]),
        ]);

        let list_arr1_ref = Arc::new(list_arr1) as ArrayRef;
        let offsets = OffsetBuffer::from_lengths([3, 3, 0]);
        let mut nulls = NullBufferBuilder::new(3);
        nulls.append_non_null();
        nulls.append_non_null();
        nulls.append_null();
        // list<list<int32>>
        let col1_field = Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            ))),
            true,
        );
        let col1 = ListArray::new(
            Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            )),
            offsets,
            list_arr1_ref,
            nulls.finish(),
        );

        let list_arr2 = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]);

        let offsets = OffsetBuffer::from_lengths([2, 2, 1]);
        let mut nulls = NullBufferBuilder::new(3);
        nulls.append_n_non_nulls(3);
        let col2_field = Field::new(
            "col2",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        );
        let col2 = GenericListArray::<i32>::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(list_arr2),
            nulls.finish(),
        );
        // convert col1 and col2 to a record batch
        let schema = Arc::new(Schema::new(vec![col1_field, col2_field]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new(
                "col1_unnest_placeholder_depth_1",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new("col1_unnest_placeholder_depth_2", DataType::Int32, true),
            Field::new("col2_unnest_placeholder_depth_1", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(col1) as ArrayRef, Arc::new(col2) as ArrayRef],
        )
        .unwrap();
        let list_type_columns = vec![
            ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 0,
                depth: 2,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
        ];
        let ret = build_batch(
            &batch,
            &out_schema,
            list_type_columns.as_ref(),
            &HashSet::default(),
            &UnnestOptions {
                null_handling: NullHandling::Preserve,
                recursions: vec![],
            },
            None,
        )?
        .unwrap();

        assert_snapshot!(batches_to_string(&[ret]),
        @r"
        +---------------------------------+---------------------------------+---------------------------------+
        | col1_unnest_placeholder_depth_1 | col1_unnest_placeholder_depth_2 | col2_unnest_placeholder_depth_1 |
        +---------------------------------+---------------------------------+---------------------------------+
        | [1, 2, 3]                       | 1                               | a                               |
        |                                 | 2                               | b                               |
        | [4, 5]                          | 3                               |                                 |
        | [1, 2, 3]                       |                                 | a                               |
        |                                 |                                 | b                               |
        | [4, 5]                          |                                 |                                 |
        | [1, 2, 3]                       | 4                               | a                               |
        |                                 | 5                               | b                               |
        | [4, 5]                          |                                 |                                 |
        | [7, 8, 9, 10]                   | 7                               | c                               |
        |                                 | 8                               | d                               |
        | [11, 12, 13]                    | 9                               |                                 |
        |                                 | 10                              |                                 |
        | [7, 8, 9, 10]                   |                                 | c                               |
        |                                 |                                 | d                               |
        | [11, 12, 13]                    |                                 |                                 |
        | [7, 8, 9, 10]                   | 11                              | c                               |
        |                                 | 12                              | d                               |
        | [11, 12, 13]                    | 13                              |                                 |
        |                                 |                                 | e                               |
        +---------------------------------+---------------------------------+---------------------------------+
        ");
        Ok(())
    }

    #[test]
    fn test_build_batch_preserve_and_expand_empty() -> Result<()> {
        // c1: [A, B, C], [], NULL, [D], NULL, [NULL, F]   c2: 1, 2, 3, 4, 5, 6
        // Expected for `NullHandling::PreserveAndExpandEmpty`:
        //   [A, B, C] -> three rows with c2 = 1, 1, 1
        //   []        -> one  row  with c2 = 2 and unnested value NULL
        //   NULL      -> one  row  with c2 = 3 and unnested value NULL
        //   [D]       -> one  row  with c2 = 4
        //   NULL      -> one  row  with c2 = 5 and unnested value NULL
        //   [NULL, F] -> two  rows with c2 = 6, 6
        let list_array = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        let other = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let in_schema = Arc::new(Schema::new(vec![
            Field::new(
                "c1",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Field::new("c2", DataType::Int32, true),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("c1_unnested", DataType::Utf8, true),
            Field::new("c2", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&in_schema),
            vec![Arc::clone(&list_array), Arc::clone(&other)],
        )?;
        let list_type_columns = vec![ListUnnest {
            index_in_input_schema: 0,
            depth: 1,
        }];

        let ret = build_batch(
            &batch,
            &out_schema,
            &list_type_columns,
            &HashSet::default(),
            &UnnestOptions {
                null_handling: NullHandling::PreserveAndExpandEmpty,
                recursions: vec![],
            },
            None,
        )?
        .unwrap();

        assert_snapshot!(batches_to_string(&[ret]),
        @r"
        +-------------+----+
        | c1_unnested | c2 |
        +-------------+----+
        | A           | 1  |
        | B           | 1  |
        | C           | 1  |
        |             | 2  |
        |             | 3  |
        | D           | 4  |
        |             | 5  |
        |             | 6  |
        | F           | 6  |
        +-------------+----+
        ");
        Ok(())
    }

    // PreserveAndExpandEmpty must work for LargeListArray (i64 offsets) too,
    // not just the i32-offset ListArray exercised above.
    #[test]
    fn test_build_batch_preserve_and_expand_empty_largelist() -> Result<()> {
        let list_array = Arc::new(make_generic_array::<i64>()) as ArrayRef;
        let other = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])) as ArrayRef;
        let in_schema = Arc::new(Schema::new(vec![
            Field::new(
                "c1",
                DataType::LargeList(Arc::new(Field::new_list_field(
                    DataType::Utf8,
                    true,
                ))),
                true,
            ),
            Field::new("c2", DataType::Int32, true),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("c1_unnested", DataType::Utf8, true),
            Field::new("c2", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&in_schema),
            vec![Arc::clone(&list_array), Arc::clone(&other)],
        )?;
        let list_type_columns = vec![ListUnnest {
            index_in_input_schema: 0,
            depth: 1,
        }];

        let ret = build_batch(
            &batch,
            &out_schema,
            &list_type_columns,
            &HashSet::default(),
            &UnnestOptions {
                null_handling: NullHandling::PreserveAndExpandEmpty,
                recursions: vec![],
            },
            None,
        )?
        .unwrap();

        // Same expected shape as the ListArray case — exercises the LargeList
        // code path in unnest_list_array.
        assert_snapshot!(batches_to_string(&[ret]),
        @r"
        +-------------+----+
        | c1_unnested | c2 |
        +-------------+----+
        | A           | 1  |
        | B           | 1  |
        | C           | 1  |
        |             | 2  |
        |             | 3  |
        | D           | 4  |
        |             | 5  |
        |             | 6  |
        | F           | 6  |
        +-------------+----+
        ");
        Ok(())
    }

    // When two list columns are unnested together, `find_longest_length`
    // takes the per-row max. PreserveAndExpandEmpty must bump zeros to ones
    // in each input column independently, then the row-wise max picks up
    // the right value.
    #[test]
    fn test_build_batch_preserve_and_expand_empty_multi_column() -> Result<()> {
        // col_a: [1, 2], [],   NULL,  [3]
        // col_b: ['x'],  ['y'],['z'], NULL
        let col_a = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
            None,
            Some(vec![Some(3)]),
        ]);
        let col_b = {
            let mut b =
                arrow::array::ListBuilder::new(arrow::array::StringBuilder::new());
            b.values().append_value("x");
            b.append(true);
            b.values().append_value("y");
            b.append(true);
            b.values().append_value("z");
            b.append(true);
            b.append(false);
            b.finish()
        };
        let id = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;

        let in_schema = Arc::new(Schema::new(vec![
            Field::new(
                "a",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new(
                "b",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
            Field::new("id", DataType::Int32, true),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("a_unnested", DataType::Int32, true),
            Field::new("b_unnested", DataType::Utf8, true),
            Field::new("id", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&in_schema),
            vec![
                Arc::new(col_a) as ArrayRef,
                Arc::new(col_b) as ArrayRef,
                Arc::clone(&id),
            ],
        )?;
        let list_type_columns = vec![
            ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
        ];

        let ret = build_batch(
            &batch,
            &out_schema,
            &list_type_columns,
            &HashSet::default(),
            &UnnestOptions {
                null_handling: NullHandling::PreserveAndExpandEmpty,
                recursions: vec![],
            },
            None,
        )?
        .unwrap();

        // Row 0: longest = max(len([1,2])=2, len(['x'])=1) = 2 → a=[1,2], b=['x',NULL]
        // Row 1: a=[] bumped to len 1, b=['y'] len 1 → a=[NULL], b=['y']
        // Row 2: a=NULL bumped to len 1, b=['z'] len 1 → a=[NULL], b=['z']
        // Row 3: a=[3] len 1, b=NULL bumped to len 1 → a=[3], b=[NULL]
        assert_snapshot!(batches_to_string(&[ret]),
        @r"
        +------------+------------+----+
        | a_unnested | b_unnested | id |
        +------------+------------+----+
        | 1          | x          | 10 |
        | 2          |            | 10 |
        |            | y          | 20 |
        |            | z          | 30 |
        | 3          |            | 40 |
        +------------+------------+----+
        ");
        Ok(())
    }

    // PreserveAndExpandEmpty must propagate through recursive depth-2
    // unnesting: an outer NULL or empty produces one NULL output row at
    // each level. Adapted from `test_build_batch_list_arr_recursive`.
    #[test]
    fn test_build_batch_preserve_and_expand_empty_recursive() -> Result<()> {
        // col1                             | col2
        // [[1,2,3],null,[4,5]]             | ['a','b']
        // [[7,8,9,10], null, [11,12,13]]   | ['c','d']
        // null                             | ['e']
        let list_arr1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(7), Some(8), Some(9), Some(10)]),
            None,
            Some(vec![Some(11), Some(12), Some(13)]),
        ]);
        let list_arr1_ref = Arc::new(list_arr1) as ArrayRef;
        let offsets = OffsetBuffer::from_lengths([3, 3, 0]);
        let mut nulls = NullBufferBuilder::new(3);
        nulls.append_non_null();
        nulls.append_non_null();
        nulls.append_null();
        let col1_field = Field::new_list_field(
            DataType::List(Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            ))),
            true,
        );
        let col1 = ListArray::new(
            Arc::new(Field::new_list_field(
                list_arr1_ref.data_type().to_owned(),
                true,
            )),
            offsets,
            list_arr1_ref,
            nulls.finish(),
        );

        let list_arr2 = StringArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]);
        let offsets = OffsetBuffer::from_lengths([2, 2, 1]);
        let mut nulls = NullBufferBuilder::new(3);
        nulls.append_n_non_nulls(3);
        let col2_field = Field::new(
            "col2",
            DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
            true,
        );
        let col2 = GenericListArray::<i32>::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(list_arr2),
            nulls.finish(),
        );
        let schema = Arc::new(Schema::new(vec![col1_field, col2_field]));
        let out_schema = Arc::new(Schema::new(vec![
            Field::new(
                "col1_unnest_placeholder_depth_1",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new("col1_unnest_placeholder_depth_2", DataType::Int32, true),
            Field::new("col2_unnest_placeholder_depth_1", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(col1) as ArrayRef, Arc::new(col2) as ArrayRef],
        )?;
        let list_type_columns = vec![
            ListUnnest {
                index_in_input_schema: 0,
                depth: 1,
            },
            ListUnnest {
                index_in_input_schema: 0,
                depth: 2,
            },
            ListUnnest {
                index_in_input_schema: 1,
                depth: 1,
            },
        ];

        let ret = build_batch(
            &batch,
            &out_schema,
            &list_type_columns,
            &HashSet::default(),
            &UnnestOptions {
                null_handling: NullHandling::PreserveAndExpandEmpty,
                recursions: vec![],
            },
            None,
        )?
        .unwrap();

        // The third input row (col1 = null, col2 = ['e']) now produces a
        // NULL row for the depth-1 col1 placeholder *and* the depth-2 one,
        // instead of being dropped at depth 1 and again at depth 2 the way
        // it would be under `Drop`. Inner NULLs inside [...null...] sub-
        // lists are still padded with NULL as before.
        assert_snapshot!(batches_to_string(&[ret]),
        @r"
        +---------------------------------+---------------------------------+---------------------------------+
        | col1_unnest_placeholder_depth_1 | col1_unnest_placeholder_depth_2 | col2_unnest_placeholder_depth_1 |
        +---------------------------------+---------------------------------+---------------------------------+
        | [1, 2, 3]                       | 1                               | a                               |
        |                                 | 2                               | b                               |
        | [4, 5]                          | 3                               |                                 |
        | [1, 2, 3]                       |                                 | a                               |
        |                                 |                                 | b                               |
        | [4, 5]                          |                                 |                                 |
        | [1, 2, 3]                       | 4                               | a                               |
        |                                 | 5                               | b                               |
        | [4, 5]                          |                                 |                                 |
        | [7, 8, 9, 10]                   | 7                               | c                               |
        |                                 | 8                               | d                               |
        | [11, 12, 13]                    | 9                               |                                 |
        |                                 | 10                              |                                 |
        | [7, 8, 9, 10]                   |                                 | c                               |
        |                                 |                                 | d                               |
        | [11, 12, 13]                    |                                 |                                 |
        | [7, 8, 9, 10]                   | 11                              | c                               |
        |                                 | 12                              | d                               |
        | [11, 12, 13]                    | 13                              |                                 |
        |                                 |                                 | e                               |
        +---------------------------------+---------------------------------+---------------------------------+
        ");
        Ok(())
    }

    #[test]
    fn test_unnest_list_array() -> Result<()> {
        // [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = make_generic_array::<i32>();
        verify_unnest_list_array(
            &list_array,
            vec![3, 2, 1, 2, 0, 3],
            vec![
                Some("A"),
                Some("B"),
                Some("C"),
                None,
                None,
                None,
                Some("D"),
                None,
                None,
                Some("F"),
                None,
            ],
        )?;

        // [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = make_fixed_list();
        verify_unnest_list_array(
            &list_array,
            vec![3, 1, 2, 0, 2, 3],
            vec![
                Some("A"),
                Some("B"),
                None,
                None,
                Some("C"),
                Some("D"),
                None,
                Some("F"),
                None,
                None,
                None,
            ],
        )?;

        Ok(())
    }

    fn verify_longest_length(
        list_arrays: &[ArrayRef],
        null_handling: NullHandling,
        expected: Vec<i64>,
    ) -> Result<()> {
        let options = UnnestOptions {
            null_handling,
            recursions: vec![],
        };
        let longest_length = find_longest_length(list_arrays, &options)?;
        let expected_array = Int64Array::from(expected);
        assert_eq!(
            longest_length
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &expected_array
        );
        Ok(())
    }

    #[test]
    fn test_longest_list_length() -> Result<()> {
        // Test with single ListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Drop,
            vec![3, 0, 0, 1, 0, 2],
        )?;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Preserve,
            vec![3, 0, 1, 1, 1, 2],
        )?;
        // PreserveAndExpandEmpty also treats empty lists as a NULL row.
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::PreserveAndExpandEmpty,
            vec![3, 1, 1, 1, 1, 2],
        )?;

        // Test with single LargeListArray
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        let list_array = Arc::new(make_generic_array::<i64>()) as ArrayRef;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Drop,
            vec![3, 0, 0, 1, 0, 2],
        )?;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Preserve,
            vec![3, 0, 1, 1, 1, 2],
        )?;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::PreserveAndExpandEmpty,
            vec![3, 1, 1, 1, 1, 2],
        )?;

        // Test with single FixedSizeListArray
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list_array = Arc::new(make_fixed_list()) as ArrayRef;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Drop,
            vec![2, 0, 2, 0, 2, 2],
        )?;
        verify_longest_length(
            &[Arc::clone(&list_array)],
            NullHandling::Preserve,
            vec![2, 1, 2, 1, 2, 2],
        )?;

        // Test with multiple list arrays
        //  [A, B, C], [], NULL, [D], NULL, [NULL, F]
        //  [A, B], NULL, [C, D], NULL, [NULL, F], [NULL, NULL]
        let list1 = Arc::new(make_generic_array::<i32>()) as ArrayRef;
        let list2 = Arc::new(make_fixed_list()) as ArrayRef;
        let list_arrays = vec![Arc::clone(&list1), Arc::clone(&list2)];
        verify_longest_length(&list_arrays, NullHandling::Drop, vec![3, 0, 2, 1, 2, 2])?;
        verify_longest_length(
            &list_arrays,
            NullHandling::Preserve,
            vec![3, 1, 2, 1, 2, 2],
        )?;
        verify_longest_length(
            &list_arrays,
            NullHandling::PreserveAndExpandEmpty,
            vec![3, 1, 2, 1, 2, 2],
        )?;

        Ok(())
    }

    #[test]
    fn test_create_take_indices() -> Result<()> {
        let length_array = Int64Array::from(vec![2, 3, 1]);
        let take_indices = create_take_indices(&length_array, 6);
        let expected = Int64Array::from(vec![0, 0, 1, 1, 1, 2]);
        assert_eq!(take_indices, expected);
        Ok(())
    }

    /// Build a single-column `List<Int32>` batch where row `i` holds `lens[i]` elements,
    /// numbered consecutively from 0 across the whole batch. A `None` length is a NULL
    /// list.
    fn list_batch(lens: &[Option<usize>]) -> RecordBatch {
        let mut next = 0i32;
        let rows: Vec<Option<Vec<Option<i32>>>> = lens
            .iter()
            .map(|len| {
                len.map(|len| {
                    (0..len)
                        .map(|_| {
                            next += 1;
                            Some(next - 1)
                        })
                        .collect()
                })
            })
            .collect();
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(rows);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            list.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(list)]).unwrap()
    }

    /// Run a depth-1 unnest of column "l" over `input`, with the given
    /// `datafusion.execution.batch_size`, and return the output batches.
    async fn unnest_with_batch_size(
        input: Vec<RecordBatch>,
        batch_size: usize,
        options: UnnestOptions,
    ) -> Result<Vec<RecordBatch>> {
        unnest_at_depth(input, batch_size, options, 1).await
    }

    /// Unnest column "l" of `input` to `depth`, with the given
    /// `datafusion.execution.batch_size`, and return the output batches.
    async fn unnest_at_depth(
        input: Vec<RecordBatch>,
        batch_size: usize,
        options: UnnestOptions,
        depth: usize,
    ) -> Result<Vec<RecordBatch>> {
        Ok(
            unnest_at_depth_with_metrics(input, batch_size, options, depth)
                .await?
                .0,
        )
    }

    async fn unnest_at_depth_with_metrics(
        input: Vec<RecordBatch>,
        batch_size: usize,
        options: UnnestOptions,
        depth: usize,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let input_schema = input[0].schema();
        let output_schema =
            Arc::new(Schema::new(vec![Field::new("l", DataType::Int32, true)]));
        let source =
            crate::test::TestMemoryExec::try_new_exec(&[input], input_schema, None)?;
        let unnest = UnnestExec::new(
            source,
            vec![ListUnnest {
                index_in_input_schema: 0,
                depth,
            }],
            vec![],
            output_schema,
            options,
        )?;
        let task_ctx = Arc::new(
            TaskContext::default().with_session_config(
                datafusion_execution::config::SessionConfig::new()
                    .with_batch_size(batch_size),
            ),
        );
        let batches = crate::common::collect(unnest.execute(0, task_ctx)?).await?;
        let metrics = unnest.metrics().expect("UnnestExec exposes metrics");
        Ok((batches, metrics))
    }

    /// The values an unnest produces, flattened across all output batches.
    fn output_values(batches: &[RecordBatch]) -> Vec<Option<i32>> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_primitive::<Int32Type>()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Output batch sizes are fully determined by the input lengths and `batch_size`, so
    /// assert the exact shapes rather than just the `<= batch_size` bound. Each case pins a
    /// distinct path through `next_chunk_rows`.
    #[tokio::test]
    async fn test_unnest_stream_output_batch_shapes() -> Result<()> {
        struct Case {
            /// One inner slice per input batch, each of that batch's per-row list lengths.
            lens_per_batch: &'static [&'static [Option<usize>]],
            batch_size: usize,
            expected_sizes: &'static [usize],
        }
        let cases: &[Case] = &[
            // Chunks pack several input rows. This is the case that distinguishes chunking
            // the input from building everything and slicing: slicing a single 30-row build
            // would give [8, 8, 8, 6].
            Case {
                lens_per_batch: &[&[Some(3); 10]],
                batch_size: 8,
                expected_sizes: &[6, 6, 6, 6, 6],
            },
            // Output smaller than batch_size comes back as one batch.
            Case {
                lens_per_batch: &[&[Some(3), Some(2)]],
                batch_size: 1024,
                expected_sizes: &[5],
            },
            // One row expanding past batch_size cannot be chunked on the input side, so the
            // oversized build is sliced on the way out instead.
            Case {
                lens_per_batch: &[&[Some(25)]],
                batch_size: 10,
                expected_sizes: &[10, 10, 5],
            },
            // Chunk boundaries are per input batch, so each batch contributes a short tail.
            Case {
                lens_per_batch: &[&[Some(5), Some(5)], &[Some(1)], &[Some(7), Some(2)]],
                batch_size: 4,
                expected_sizes: &[4, 1, 4, 1, 1, 4, 3, 2],
            },
        ];

        for case in cases {
            let input: Vec<RecordBatch> = case
                .lens_per_batch
                .iter()
                .map(|lens| list_batch(lens))
                .collect();
            let batches =
                unnest_with_batch_size(input, case.batch_size, UnnestOptions::default())
                    .await?;

            let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
            assert_eq!(
                sizes, case.expected_sizes,
                "lens={:?} batch_size={}",
                case.lens_per_batch, case.batch_size
            );

            // `list_batch` numbers each batch's elements from 0, so the expected values are
            // one run per input batch. Splitting must not perturb values or their order.
            let expected_values: Vec<Option<i32>> = case
                .lens_per_batch
                .iter()
                .flat_map(|lens| {
                    (0..lens.iter().flatten().sum::<usize>() as i32).map(Some)
                })
                .collect();
            assert_eq!(
                output_values(&batches),
                expected_values,
                "lens={:?} batch_size={}",
                case.lens_per_batch,
                case.batch_size
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_stream_output_metrics_after_split() -> Result<()> {
        let (batches, metrics) = unnest_at_depth_with_metrics(
            vec![list_batch(&[Some(25)])],
            10,
            UnnestOptions::default(),
            1,
        )
        .await?;

        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        let output_batches = metrics
            .sum(|metric| matches!(metric.value(), MetricValue::OutputBatches(_)))
            .expect("output_batches metric exists")
            .as_usize();
        assert_eq!(output_batches, batches.len());
        let output_rows = metrics
            .sum(|metric| matches!(metric.value(), MetricValue::OutputRows(_)))
            .expect("output_rows metric exists")
            .as_usize();
        assert_eq!(
            output_rows,
            batches.iter().map(RecordBatch::num_rows).sum::<usize>()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_stream_chunking_preserves_null_handling() -> Result<()> {
        // NULL and empty lists each contribute one NULL output row under
        // PreserveAndExpandEmpty, and the per-row output counts that drive chunking must
        // agree with that or chunk boundaries would drift out of step with the unnesting.
        let lens = &[Some(3), Some(0), None, Some(2), None, Some(0)];
        let options =
            UnnestOptions::new().with_null_handling(NullHandling::PreserveAndExpandEmpty);

        let chunked =
            unnest_with_batch_size(vec![list_batch(lens)], 2, options.clone()).await?;
        let whole = unnest_with_batch_size(vec![list_batch(lens)], 1024, options).await?;

        assert!(chunked.iter().all(|b| b.num_rows() <= 2));
        // 3 + 1 + 1 + 2 + 1 + 1
        assert_eq!(chunked.iter().map(|b| b.num_rows()).sum::<usize>(), 9);
        assert_eq!(output_values(&chunked), output_values(&whole));
        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_stream_drop_null_handling() -> Result<()> {
        // Under Drop, NULL and empty lists produce nothing. Chunks made up entirely of
        // such rows yield no batch at all, and must not stall the stream or leak an
        // empty batch into the output.
        let lens = &[None, Some(0), None, Some(4), Some(0), None];
        let options = UnnestOptions::new().with_null_handling(NullHandling::Drop);

        let batches = unnest_with_batch_size(vec![list_batch(lens)], 2, options).await?;

        assert!(batches.iter().all(|b| b.num_rows() > 0));
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_unnest_stream_recursive_respects_batch_size() -> Result<()> {
        // Recursive unnest cannot have its expansion predicted from the input, so it falls
        // back to unnesting a whole input batch and slicing the output. The batch_size
        // guarantee has to hold on that path too.
        let inner = Field::new_list_field(DataType::Int32, true);
        let outer =
            Field::new_list_field(DataType::new_list(DataType::Int32, true), true);
        let values = Int32Array::from((0..24).collect::<Vec<_>>());
        // 12 inner lists of 2 elements each...
        let inner_list = ListArray::new(
            Arc::new(inner),
            OffsetBuffer::new((0..=12).map(|i| i * 2).collect::<Vec<i32>>().into()),
            Arc::new(values),
            None,
        );
        // ...grouped 3 to a row, so 4 input rows expand to 24 output rows at depth 2.
        let outer_list = ListArray::new(
            Arc::new(outer),
            OffsetBuffer::new((0..=4).map(|i| i * 3).collect::<Vec<i32>>().into()),
            Arc::new(inner_list),
            None,
        );
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "l",
            outer_list.data_type().clone(),
            true,
        )]));
        let input = RecordBatch::try_new(input_schema, vec![Arc::new(outer_list)])?;

        let batches =
            unnest_at_depth(vec![input], 7, UnnestOptions::default(), 2).await?;

        let sizes: Vec<usize> = batches.iter().map(|b| b.num_rows()).collect();
        assert_eq!(sizes, vec![7, 7, 7, 3]);
        assert_eq!(
            output_values(&batches),
            (0..24).map(Some).collect::<Vec<_>>()
        );
        Ok(())
    }
}
