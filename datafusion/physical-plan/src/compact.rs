use std::{
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use arrow::{
    array::{
        Array, ArrayRef, AsArray, ByteView, GenericByteViewArray, GenericByteViewBuilder,
        RecordBatch,
    },
    datatypes::ByteViewType,
};
use arrow_schema::SchemaRef;
use datafusion_common::{config::ConfigOptions, internal_err, Result, Statistics};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_physical_expr::PhysicalExpr;
use futures::{Stream, StreamExt};

use crate::{
    execution_plan::CardinalityEffect,
    filter_pushdown::{
        ChildPushdownResult, FilterDescription, FilterPushdownPhase,
        FilterPushdownPropagation,
    },
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput},
    DisplayAs, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};

#[derive(Clone, Debug)]
pub struct CompactExec {
    compact_threshold: f64,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    input: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for CompactExec {
    fn fmt_as(
        &self,
        t: crate::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        use crate::DisplayFormatType::*;

        match t {
            Default | Verbose => {
                write!(
                    f,
                    "CompactExec: compact_threshold={}",
                    self.compact_threshold,
                )?;
            }
            TreeRender => {
                write!(f, "compact_threshold={}", self.compact_threshold)?;
            }
        }

        Ok(())
    }
}

impl CompactExec {
    pub fn new(compact_threshold: f64, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Self::compute_properties(&input);

        Self {
            compact_threshold,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            input,
        }
    }

    /// This function creates the cache object that stores the plan properties such as
    /// schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(input: &Arc<dyn ExecutionPlan>) -> PlanProperties {
        // The compact operator does not make any changes to the
        // partitioning of its input.
        PlanProperties::new(
            input.equivalence_properties().clone(), // Equivalence Properties
            input.output_partitioning().clone(),    // Output Partitioning
            input.pipeline_behavior(),
            input.boundedness(),
        )
    }
}

impl ExecutionPlan for CompactExec {
    fn name(&self) -> &str {
        "CompactExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(input) = children.pop() else {
            return internal_err!("CompactExec needs a single child");
        };
        Ok(Arc::new(Self::new(self.compact_threshold, input)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        return Ok(Box::pin(CompactStream {
            schema: self.schema(),
            input: self.input.execute(partition, context)?,
            compact_threshold: self.compact_threshold,
            metrics: BaselineMetrics::new(&self.metrics, partition),
        }));
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
}

struct CompactStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    compact_threshold: f64,
    metrics: BaselineMetrics,
}

impl RecordBatchStream for CompactStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for CompactStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                let _timer = self.metrics.elapsed_compute().timer();
                let output = compact(self.compact_threshold, batch);
                Poll::Ready(Some(Ok(output.record_output(&self.metrics))))
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => Poll::Ready(None),
        }
    }
}

fn compact_view_column<T: ByteViewType>(
    threshold: f64,
    array: &GenericByteViewArray<T>,
) -> Option<ArrayRef> {
    const INLINE_THRESHOLD: u32 = 12;

    if array.data_buffers().is_empty() {
        return None;
    }

    let ideal_buffer_size: usize = array
        .views()
        .iter()
        .map(|view| {
            let byte_view = ByteView::from(*view);
            if byte_view.length > INLINE_THRESHOLD {
                byte_view.length as usize
            } else {
                0
            }
        })
        .sum();

    let actual_buffer_size: usize =
        array.data_buffers().iter().map(|buf| buf.capacity()).sum();

    if actual_buffer_size as f64 > ideal_buffer_size as f64 * threshold {
        // todo: request APIs from arrow-rs to obtain a mutable downcasted array
        // ... so we can reuse the data buffers of the column here.
        // until then: out of place :(

        // No data buffers are needed => Drop them all
        if ideal_buffer_size == 0 {
            return Some(Arc::new(unsafe {
                GenericByteViewArray::<T>::new_unchecked(
                    array.views().clone(),
                    vec![],
                    array.nulls().cloned(),
                )
            }));
        }

        let mut builder = GenericByteViewBuilder::<T>::with_capacity(array.len())
            .with_fixed_block_size(ideal_buffer_size as u32);

        for view in array.iter() {
            builder.append_option(view);
        }

        Some(Arc::new(builder.finish()))
    } else {
        None
    }
}

fn compact(threshold: f64, batch: RecordBatch) -> RecordBatch {
    let compact_column = |column: ArrayRef| -> ArrayRef {
        if let Some(string_view_array) = column.as_string_view_opt() {
            return compact_view_column(threshold, string_view_array).unwrap_or(column);
        }

        if let Some(bin_view_array) = column.as_binary_view_opt() {
            return compact_view_column(threshold, bin_view_array).unwrap_or(column);
        }

        column
    };

    let (schema, columns, row_count) = batch.into_parts();
    let columns = columns.into_iter().map(compact_column).collect();

    unsafe { RecordBatch::new_unchecked(schema, columns, row_count) }
}
