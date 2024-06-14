use std::{
    any::Any,
    borrow::Cow,
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread::current,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::PrimitiveBuilder,
    compute::{concat_batches, filter_record_batch, kernels::window},
    datatypes::TimestampMillisecondType,
};

use arrow_array::{Array, PrimitiveArray, RecordBatch, TimestampMillisecondArray};
use arrow_ord::cmp;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion_common::{internal_err, stats::Precision, DataFusionError, Statistics};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_expr::Accumulator;
use datafusion_physical_expr::{
    equivalence::{collapse_lex_req, ProjectionMapping},
    expressions::UnKnownColumn,
    AggregateExpr, Partitioning, PhysicalExpr, PhysicalSortRequirement,
};
use futures::{Stream, StreamExt};
use tracing::debug;

use crate::{
    aggregates::{
        aggregate_expressions, create_accumulators, finalize_aggregation,
        get_finer_aggregate_exprs_requirement, AccumulatorItem, AggregateMode,
        PhysicalGroupBy,
    },
    filter::batch_filter,
    time::RecordBatchWatermark,
    InputOrderMode,
};
use crate::{
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};

use crate::windows::get_ordered_partition_by_indices;
pub struct FranzWindowFrame {
    pub window_start_time: SystemTime,
    window_end_time: SystemTime,
    timestamp_column: String,
    accumulators: Vec<AccumulatorItem>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    aggregation_mode: AggregateMode,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl DisplayAs for FranzWindowFrame {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
                write!(
                    f,
                    "start_time: {:?}, end_time {:?}, timestamp_column {}",
                    self.window_start_time.duration_since(UNIX_EPOCH),
                    self.window_end_time.duration_since(UNIX_EPOCH),
                    self.timestamp_column
                )?;
            }
        }
        Ok(())
    }
}

use datafusion_common::Result;

impl FranzWindowFrame {
    pub fn new(
        window_start_time: SystemTime,
        window_end_time: SystemTime,
        timestamp_column: String,
        accumulators: Vec<AccumulatorItem>,
        aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
        filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
        aggregation_mode: AggregateMode,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let res = Self {
            window_start_time,
            window_end_time,
            timestamp_column,
            accumulators,
            aggregate_expressions,
            filter_expressions,
            aggregation_mode,
            schema,
            baseline_metrics,
        };
        res
    }

    pub fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let ts_column = batch
            .column_by_name(self.timestamp_column.as_str())
            .unwrap();

        let ts_array = ts_column
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap()
            .to_owned();

        let start_time_duration = self
            .window_start_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let gte_cmp_filter = cmp::gt_eq(
            &ts_array,
            &TimestampMillisecondArray::new_scalar(start_time_duration),
        )?;

        let filtered_batch: RecordBatch = filter_record_batch(batch, &gte_cmp_filter)?;

        let end_time_duration = self
            .window_end_time
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let ts_column = filtered_batch
            .column_by_name(self.timestamp_column.as_str())
            .unwrap();

        let ts_array = ts_column
            .as_any()
            .downcast_ref::<PrimitiveArray<TimestampMillisecondType>>()
            .unwrap()
            .to_owned();

        let lt_cmp_filter = cmp::lt(
            &ts_array,
            &TimestampMillisecondArray::new_scalar(end_time_duration),
        )?;
        let final_batch = filter_record_batch(&filtered_batch, &lt_cmp_filter)?;

        let _ = aggregate_batch(
            &self.aggregation_mode,
            final_batch,
            &mut self.accumulators,
            &self.aggregate_expressions,
            &self.filter_expressions,
        );
        Ok(())
    }

    pub fn evaluate(&mut self) -> Result<RecordBatch, DataFusionError> {
        let timer = self.baseline_metrics.elapsed_compute().timer();
        let result = finalize_aggregation(&mut self.accumulators, &self.aggregation_mode)
            .and_then(|columns| {
                RecordBatch::try_new(self.schema.clone(), columns).map_err(Into::into)
            });

        timer.done();
        result
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FranzStreamingWindowType {
    Session(Duration),
    Sliding(Duration, Duration),
    Tumbling(Duration),
}

#[derive(Debug)]
pub struct FranzStreamingWindowExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub aggregate_expressions: Vec<Arc<dyn AggregateExpr>>,
    pub filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    /// Schema after the window is run
    pub group_by: PhysicalGroupBy,
    schema: SchemaRef,
    pub input_schema: SchemaRef,

    pub watermark: Arc<Mutex<Option<SystemTime>>>,
    metrics: ExecutionPlanMetricsSet,
    cache: PlanProperties,
    pub mode: AggregateMode,
    pub window_type: FranzStreamingWindowType,
}

impl FranzStreamingWindowExec {
    /// Create a new execution plan for window aggregates
    ///
    pub fn try_new(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        window_type: FranzStreamingWindowType,
    ) -> Result<Self> {
        let schema = create_schema(
            &input.schema(),
            &group_by.expr,
            &aggr_expr,
            group_by.contains_null(),
            mode,
        )?;

        let schema = Arc::new(schema);
        FranzStreamingWindowExec::try_new_with_schema(
            mode,
            group_by,
            aggr_expr,
            filter_expr,
            input,
            input_schema,
            schema,
            window_type,
        )
    }

    pub fn try_new_with_schema(
        mode: AggregateMode,
        group_by: PhysicalGroupBy,
        mut aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        filter_expr: Vec<Option<Arc<dyn PhysicalExpr>>>,
        input: Arc<dyn ExecutionPlan>,
        input_schema: SchemaRef,
        schema: SchemaRef,
        window_type: FranzStreamingWindowType,
    ) -> Result<Self> {
        if aggr_expr.len() != filter_expr.len() {
            return internal_err!("Inconsistent aggregate expr: {:?} and filter expr: {:?} for AggregateExec, their size should match", aggr_expr, filter_expr);
        }

        let input_eq_properties = input.equivalence_properties();
        // Get GROUP BY expressions:
        let groupby_exprs = group_by.input_exprs();
        // If existing ordering satisfies a prefix of the GROUP BY expressions,
        // prefix requirements with this section. In this case, aggregation will
        // work more efficiently.
        let indices = get_ordered_partition_by_indices(&groupby_exprs, &input);
        let mut _new_requirement = indices
            .iter()
            .map(|&idx| PhysicalSortRequirement {
                expr: groupby_exprs[idx].clone(),
                options: None,
            })
            .collect::<Vec<_>>();

        let req = get_finer_aggregate_exprs_requirement(
            &mut aggr_expr,
            &group_by,
            input_eq_properties,
            &mode,
        )?;
        _new_requirement.extend(req);
        _new_requirement = collapse_lex_req(_new_requirement);

        let input_order_mode =
            if indices.len() == groupby_exprs.len() && !indices.is_empty() {
                InputOrderMode::Sorted
            } else if !indices.is_empty() {
                InputOrderMode::PartiallySorted(indices)
            } else {
                InputOrderMode::Linear
            };

        // construct a map from the input expression to the output expression of the Aggregation group by
        let projection_mapping =
            ProjectionMapping::try_new(&group_by.expr, &input.schema())?;

        let cache = FranzStreamingWindowExec::compute_properties(
            &input,
            schema.clone(),
            &projection_mapping,
            &mode,
            &input_order_mode,
        );

        Ok(Self {
            input,
            aggregate_expressions: aggr_expr,
            filter_expressions: filter_expr,
            group_by,
            schema,
            input_schema,
            watermark: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            mode,
            window_type,
        })
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Get the input schema before any aggregates are applied
    pub fn input_schema(&self) -> SchemaRef {
        self.input_schema.clone()
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    pub fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        projection_mapping: &ProjectionMapping,
        mode: &AggregateMode,
        _input_order_mode: &InputOrderMode,
    ) -> PlanProperties {
        // Construct equivalence properties:
        let eq_properties = input
            .equivalence_properties()
            .project(projection_mapping, schema);

        // Get output partitioning:
        let mut output_partitioning = input.output_partitioning().clone();
        if mode.is_first_stage() {
            // First stage aggregation will not change the output partitioning,
            // but needs to respect aliases (e.g. mapping in the GROUP BY
            // expression).
            let input_eq_properties = input.equivalence_properties();
            if let Partitioning::Hash(exprs, part) = output_partitioning {
                let normalized_exprs = exprs
                    .iter()
                    .map(|expr| {
                        input_eq_properties
                            .project_expr(expr, projection_mapping)
                            .unwrap_or_else(|| {
                                Arc::new(UnKnownColumn::new(&expr.to_string()))
                            })
                    })
                    .collect();
                output_partitioning = Partitioning::Hash(normalized_exprs, part);
            }
        }

        PlanProperties::new(eq_properties, output_partitioning, ExecutionMode::Unbounded)
    }
    /// Aggregate expressions
    pub fn aggr_expr(&self) -> &[Arc<dyn AggregateExpr>] {
        &self.aggregate_expressions
    }

    /// Grouping expressions as they occur in the output schema
    pub fn output_group_expr(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.group_by.output_exprs()
    }
}

impl ExecutionPlan for FranzStreamingWindowExec {
    fn name(&self) -> &'static str {
        "FranzWindowExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FranzStreamingWindowExec::try_new(
            self.mode,
            self.group_by.clone(),
            self.aggregate_expressions.clone(),
            self.filter_expressions.clone(),
            children[0].clone(),
            self.input_schema.clone(),
            self.window_type.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream: Pin<Box<FranzWindowAggStream>> = Box::pin(FranzWindowAggStream::new(
            self,
            context,
            partition,
            self.watermark.clone(),
            self.window_type.clone(),
            self.mode,
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let column_statistics = Statistics::unknown_column(&self.schema());
        match self.mode {
            AggregateMode::Final | AggregateMode::FinalPartitioned
                if self.group_by.expr.is_empty() =>
            {
                Ok(Statistics {
                    num_rows: Precision::Exact(1),
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
            _ => {
                // When the input row count is 0 or 1, we can adopt that statistic keeping its reliability.
                // When it is larger than 1, we degrade the precision since it may decrease after aggregation.
                let num_rows = if let Some(value) =
                    self.input().statistics()?.num_rows.get_value()
                {
                    if *value > 1 {
                        self.input().statistics()?.num_rows.to_inexact()
                    } else if *value == 0 {
                        // Aggregation on an empty table creates a null row.
                        self.input()
                            .statistics()?
                            .num_rows
                            .add(&Precision::Exact(1))
                    } else {
                        // num_rows = 1 case
                        self.input().statistics()?.num_rows
                    }
                } else {
                    Precision::Absent
                };
                Ok(Statistics {
                    num_rows,
                    column_statistics,
                    total_byte_size: Precision::Absent,
                })
            }
        }
    }

    fn schema(&self) -> SchemaRef {
        self.properties().schema().clone()
        /*         Arc::new(add_window_columns_to_schema(
            self.properties().schema().clone(),
        )) */
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

impl DisplayAs for FranzStreamingWindowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FranzStreamingWindowExec: mode={:?}", self.mode)?;
                let g: Vec<String> = if self.group_by.is_single() {
                    self.group_by
                        .expr
                        .iter()
                        .map(|(e, alias)| {
                            let e = e.to_string();
                            if &e != alias {
                                format!("{e} as {alias}")
                            } else {
                                e
                            }
                        })
                        .collect()
                } else {
                    self.group_by
                        .groups
                        .iter()
                        .map(|group| {
                            let terms = group
                                .iter()
                                .enumerate()
                                .map(|(idx, is_null)| {
                                    if *is_null {
                                        let (e, alias) = &self.group_by.null_expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    } else {
                                        let (e, alias) = &self.group_by.expr[idx];
                                        let e = e.to_string();
                                        if &e != alias {
                                            format!("{e} as {alias}")
                                        } else {
                                            e
                                        }
                                    }
                                })
                                .collect::<Vec<String>>()
                                .join(", ");
                            format!("({terms})")
                        })
                        .collect()
                };

                write!(f, ", gby=[{}]", g.join(", "))?;

                let a: Vec<String> = self
                    .aggregate_expressions
                    .iter()
                    .map(|agg| agg.name().to_string())
                    .collect();
                write!(f, ", aggr=[{}]", a.join(", "))?;
                write!(f, ", window_type=[{:?}]", self.window_type)?;
                //if let Some(limit) = self.limit {
                //    write!(f, ", lim=[{limit}]")?;
                //}

                //if self.input_order_mode != InputOrderMode::Linear {
                //    write!(f, ", ordering_mode={:?}", self.input_order_mode)?;
                //}
            }
        }
        Ok(())
    }
}

pub struct FranzWindowAggStream {
    pub schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    exec_aggregate_expressions: Vec<Arc<dyn AggregateExpr>>,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    latest_watermark: Arc<Mutex<Option<SystemTime>>>,
    window_frames: BTreeMap<SystemTime, FranzWindowFrame>,
    window_type: FranzStreamingWindowType,
    aggregation_mode: AggregateMode,
}

impl FranzWindowAggStream {
    pub fn new(
        exec_operator: &FranzStreamingWindowExec,
        context: Arc<TaskContext>,
        partition: usize,
        watermark: Arc<Mutex<Option<SystemTime>>>,
        window_type: FranzStreamingWindowType,
        aggregation_mode: AggregateMode,
    ) -> Result<Self> {
        // TODO: 6/12/2024 Why was this commented out?

        // In WindowAggExec all partition by columns should be ordered.
        //if window_expr[0].partition_by().len() != ordered_partition_by_indices.len() {
        //    return internal_err!("All partition by columns should have an ordering");
        //}
        let agg_schema = Arc::clone(&exec_operator.schema);
        let agg_filter_expr = exec_operator.filter_expressions.clone();

        let baseline_metrics = BaselineMetrics::new(&exec_operator.metrics, partition);
        let input = exec_operator
            .input
            .execute(partition, Arc::clone(&context))?;

        let aggregate_expressions = aggregate_expressions(
            &exec_operator.aggregate_expressions,
            &exec_operator.mode,
            0,
        )?;
        let filter_expressions = match exec_operator.mode {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; exec_operator.aggregate_expressions.len()]
            }
        };

        Ok(Self {
            schema: agg_schema,
            input,
            baseline_metrics,
            exec_aggregate_expressions: exec_operator.aggregate_expressions.clone(),
            aggregate_expressions,
            filter_expressions,
            latest_watermark: watermark,
            window_frames: BTreeMap::new(),
            window_type,
            aggregation_mode,
        })
    }

    pub fn output_schema_with_window(&self) -> SchemaRef {
        Arc::new(add_window_columns_to_schema(self.schema.clone()))
    }

    fn trigger_windows(&mut self) -> Result<RecordBatch, DataFusionError> {
        let mut results: Vec<RecordBatch> = Vec::new();
        let watermark_lock: std::sync::MutexGuard<'_, Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        if let Some(watermark) = *watermark_lock {
            let mut window_frames_to_remove: Vec<SystemTime> = Vec::new();

            for (timestamp, frame) in self.window_frames.iter_mut() {
                if watermark >= frame.window_end_time {
                    let rb = frame.evaluate()?;
                    let result = add_window_columns_to_record_batch(
                        rb,
                        frame.window_start_time,
                        frame.window_end_time,
                    );
                    results.push(result);
                    window_frames_to_remove.push(*timestamp);
                }
            }

            for timestamp in window_frames_to_remove {
                self.window_frames.remove(&timestamp);
            }
        }
        concat_batches(&self.output_schema_with_window(), &results)
            .map_err(|err| DataFusionError::ArrowError(err, None))
    }

    fn process_watermark(&mut self, watermark: RecordBatchWatermark) {
        // should this be within a mutex?
        let mut watermark_lock: std::sync::MutexGuard<Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        debug!("latest watermark currently is {:?}", *watermark_lock);
        if let Some(current_watermark) = *watermark_lock {
            if current_watermark <= watermark.min_timestamp {
                *watermark_lock = Some(watermark.min_timestamp)
            }
        } else {
            *watermark_lock = Some(watermark.min_timestamp)
        }
    }

    fn get_window_length(&mut self) -> Duration {
        match self.window_type {
            FranzStreamingWindowType::Session(duration) => duration,
            FranzStreamingWindowType::Sliding(duration, _) => duration,
            FranzStreamingWindowType::Tumbling(duration) => duration,
        }
    }

    fn ensure_window_frames_for_ranges(
        &mut self,
        ranges: &Vec<(SystemTime, SystemTime)>,
    ) -> Result<(), DataFusionError> {
        for (start_time, end_time) in ranges {
            self.window_frames.entry(*start_time).or_insert({
                let accumulators = create_accumulators(&self.exec_aggregate_expressions)?;
                FranzWindowFrame::new(
                    *start_time,
                    *end_time,
                    "franz_canonical_timestamp".to_string(),
                    accumulators,
                    self.aggregate_expressions.clone(),
                    self.filter_expressions.clone(),
                    self.aggregation_mode,
                    self.schema.clone(),
                    self.baseline_metrics.clone(),
                )
            });
        }
        Ok(())
    }

    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let result: std::prelude::v1::Result<RecordBatch, DataFusionError> =
                match self.input.poll_next_unpin(cx) {
                    Poll::Ready(rdy) => match rdy {
                        Some(Ok(batch)) => {
                            if batch.num_rows() > 0 {
                                let watermark: RecordBatchWatermark =
                                    RecordBatchWatermark::try_from(
                                        &batch,
                                        "franz_canonical_timestamp",
                                    )?;
                                let ranges = get_windows_for_watermark(
                                    &watermark,
                                    self.window_type,
                                );
                                let _ = self.ensure_window_frames_for_ranges(&ranges);
                                for range in ranges {
                                    let frame =
                                        self.window_frames.get_mut(&range.0).unwrap();
                                    let _ = frame.push(&batch);
                                }
                                self.process_watermark(watermark);
                                let triggered_result = self.trigger_windows();
                                triggered_result
                            } else {
                                Ok(RecordBatch::new_empty(
                                    self.output_schema_with_window(),
                                ))
                            }
                        }
                        Some(Err(e)) => Err(e),
                        None => {
                            Ok(RecordBatch::new_empty(self.output_schema_with_window()))
                        }
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                };
            return Poll::Ready(Some(result));
        }
    }
}

impl RecordBatchStream for FranzWindowAggStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for FranzWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll: Poll<Option<std::prelude::v1::Result<RecordBatch, DataFusionError>>> =
            self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

fn get_windows_for_watermark(
    watermark: &RecordBatchWatermark,
    window_type: FranzStreamingWindowType,
) -> Vec<(SystemTime, SystemTime)> {
    let start_time = watermark.min_timestamp;
    let end_time = watermark.max_timestamp;
    let mut window_ranges = Vec::new();

    match window_type {
        FranzStreamingWindowType::Session(_) => todo!(),
        FranzStreamingWindowType::Sliding(window_length, slide) => {
            let mut current_start =
                snap_to_window_start(start_time - window_length, window_length);
            while current_start <= end_time {
                let current_end = current_start + window_length;
                if start_time > current_end || end_time < current_start {
                    // out of bounds
                    current_start += slide;
                    continue;
                }
                window_ranges.push((current_start, current_end));
                current_start += slide;
            }
        }
        FranzStreamingWindowType::Tumbling(window_length) => {
            let mut current_start: SystemTime =
                snap_to_window_start(start_time, window_length);
            while current_start <= end_time {
                let current_end = current_start + window_length;
                window_ranges.push((current_start, current_end));
                current_start = current_end;
            }
        }
    };
    window_ranges
}

fn snap_to_window_start(timestamp: SystemTime, window_length: Duration) -> SystemTime {
    let timestamp_duration = timestamp.duration_since(UNIX_EPOCH).unwrap();
    let window_length_secs = window_length.as_secs();
    let timestamp_secs = timestamp_duration.as_secs();
    let window_start_secs = (timestamp_secs / window_length_secs) * window_length_secs;
    UNIX_EPOCH + Duration::from_secs(window_start_secs)
}

fn create_schema(
    input_schema: &Schema,
    group_expr: &[(Arc<dyn PhysicalExpr>, String)],
    aggr_expr: &[Arc<dyn AggregateExpr>],
    contains_null_expr: bool,
    mode: AggregateMode,
) -> Result<Schema> {
    let mut fields = Vec::with_capacity(group_expr.len() + aggr_expr.len());
    for (expr, name) in group_expr {
        fields.push(Field::new(
            name,
            expr.data_type(input_schema)?,
            // In cases where we have multiple grouping sets, we will use NULL expressions in
            // order to align the grouping sets. So the field must be nullable even if the underlying
            // schema field is not.
            contains_null_expr || expr.nullable(input_schema)?,
        ))
    }

    match mode {
        AggregateMode::Partial => {
            // in partial mode, the fields of the accumulator's state
            for expr in aggr_expr {
                fields.extend(expr.state_fields()?.iter().cloned())
            }
        }
        AggregateMode::Final
        | AggregateMode::FinalPartitioned
        | AggregateMode::Single
        | AggregateMode::SinglePartitioned => {
            // in final mode, the field with the final result of the accumulator
            for expr in aggr_expr {
                fields.push(expr.field()?)
            }
        }
    }

    Ok(Schema::new(fields))
}

pub fn aggregate_batch(
    mode: &AggregateMode,
    batch: RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    filters: &[Option<Arc<dyn PhysicalExpr>>],
) -> Result<usize> {
    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 filter the batch if necessary
    // 1.3 evaluate expressions
    // 1.4 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .zip(filters)
        .try_for_each(|((accum, expr), filter)| {
            // 1.2
            let batch = match filter {
                Some(filter) => Cow::Owned(batch_filter(&batch, filter)?),
                None => Cow::Borrowed(&batch),
            };
            // 1.3
            let values = &expr
                .iter()
                .map(|e| {
                    e.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;

            // 1.4
            let size_pre = accum.size();
            let res = match mode {
                AggregateMode::Partial
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => accum.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accum.merge_batch(values)
                }
            };
            let size_post = accum.size();
            allocated += size_post.saturating_sub(size_pre);
            res
        })?;

    Ok(allocated)
}

fn add_window_columns_to_schema(schema: SchemaRef) -> Schema {
    let fields = schema.all_fields().to_owned();

    let mut builder = SchemaBuilder::new();

    for field in fields {
        builder.push(field.clone());
    }
    builder.push(Field::new(
        "window_start_time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    builder.push(Field::new(
        "window_end_time",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));

    builder.finish()
}

fn add_window_columns_to_record_batch(
    record_batch: RecordBatch,
    start_time: SystemTime,
    end_time: SystemTime,
) -> RecordBatch {
    let start_time_duration =
        start_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let end_time_duration =
        end_time.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;

    let mut start_builder = PrimitiveBuilder::<TimestampMillisecondType>::new();
    let mut end_builder = PrimitiveBuilder::<TimestampMillisecondType>::new();

    for _ in 0..record_batch.num_rows() {
        start_builder.append_value(start_time_duration);
        end_builder.append_value(end_time_duration);
    }

    let start_array = start_builder.finish();
    let end_array = end_builder.finish();

    let new_schema = add_window_columns_to_schema(record_batch.schema());
    let mut new_columns = record_batch.columns().to_vec();
    new_columns.push(Arc::new(start_array));
    new_columns.push(Arc::new(end_array));

    RecordBatch::try_new(Arc::new(new_schema), new_columns).unwrap()
}
