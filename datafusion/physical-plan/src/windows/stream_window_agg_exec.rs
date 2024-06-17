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

use std::{
    any::Any,
    collections::BTreeMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    compute::{concat, concat_batches, filter_record_batch},
    datatypes::TimestampMillisecondType,
};

use arrow_array::{
    Array, ArrayRef, PrimitiveArray, RecordBatch, TimestampMillisecondArray,
};
use arrow_ord::cmp;
use arrow_schema::{ArrowError, Schema, SchemaBuilder, SchemaRef};
use datafusion_common::{
    stats::Precision, utils::evaluate_partition_ranges, ColumnStatistics,
    DataFusionError, Statistics,
};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{
    window::WindowExpr, Distribution, PhysicalExpr, PhysicalSortExpr,
    PhysicalSortRequirement,
};
use futures::{Stream, StreamExt};
use tracing::{debug, error, info};

use crate::time::RecordBatchWatermark;
use crate::{
    common::transpose,
    metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet},
    windows::calc_requirements,
    DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
    PlanProperties,
};

use super::{
    get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_agg_exec::compute_window_aggregates, window_equivalence_properties,
};
#[derive(Clone)]
pub struct FranzWindowFrame {
    batches: Vec<RecordBatch>,
    window_start_time: SystemTime,
    window_end_time: SystemTime,
    finished: bool,
    timestamp_column: String,
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
    ) -> Self {
        let res = Self {
            window_start_time,
            window_end_time,
            batches: Vec::new(),
            finished: false,
            timestamp_column,
        };
        res
    }

    pub fn push(&mut self, batch: &RecordBatch) -> Result<(), DataFusionError> {
        let num_rows = batch.num_rows();

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
        info!(
            "WindowFrame - {:?} original_num_rows {} final_num_rows {}",
            self.window_start_time,
            num_rows,
            final_batch.num_rows()
        );
        self.batches.push(final_batch);
        info!("frame {:?} successfully updated", self.window_start_time);
        Ok(())
    }

    pub fn evaluate(&self) -> Vec<RecordBatch> {
        self.batches.clone()
    }
}

#[derive(Debug, Clone)]
pub enum FranzWindowType {
    Session(Duration),
    Sliding(Duration, Duration),
    Tumbling(Duration),
}

#[derive(Debug)]
pub struct FranzWindowExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    pub partition_keys: Vec<Arc<dyn PhysicalExpr>>,
    pub watermark: Arc<Mutex<Option<SystemTime>>>,
    metrics: ExecutionPlanMetricsSet,
    ordered_partition_by_indices: Vec<usize>,
    cache: PlanProperties,
    window_type: FranzWindowType,
}

impl FranzWindowExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        partition_keys: Vec<Arc<dyn PhysicalExpr>>,
        window_type: FranzWindowType,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(window_expr[0].partition_by(), &input);
        let cache = Self::compute_properties(schema.clone(), &input, &window_expr);
        Ok(Self {
            input,
            window_expr,
            schema,
            partition_keys,
            watermark: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            ordered_partition_by_indices,
            cache,
            window_type,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let partition_by = self.window_expr()[0].partition_by();
        get_partition_by_sort_exprs(
            &self.input,
            partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(&schema, input, window_expr);
        let output_partitioning = input.output_partitioning().clone();

        // Determine execution mode:
        let mode = match input.execution_mode() {
            ExecutionMode::Bounded => ExecutionMode::Bounded,
            ExecutionMode::Unbounded | ExecutionMode::PipelineBreaking => {
                ExecutionMode::Unbounded
            }
        };

        // Construct properties cache:
        PlanProperties::new(eq_properties, output_partitioning, mode)
    }
}

impl ExecutionPlan for FranzWindowExec {
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

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        if self.ordered_partition_by_indices.len() < partition_bys.len() {
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            let partition_bys = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| &partition_bys[*idx]);
            vec![calc_requirements(partition_bys, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys.is_empty() {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::HashPartitioned(self.partition_keys.clone())]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FranzWindowExec::try_new(
            self.window_expr.clone(),
            children[0].clone(),
            self.partition_keys.clone(),
            self.window_type.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input: Pin<Box<dyn RecordBatchStream + Send>> =
            self.input.execute(partition, context)?;
        let stream: Pin<Box<FranzWindowAggStream>> = Box::pin(FranzWindowAggStream::new(
            self.schema.clone(),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
            self.ordered_partition_by_indices.clone(),
            self.window_type.clone(),
            self.watermark.clone(),
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let input_stat = self.input.statistics()?;
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        })
    }

    fn schema(&self) -> SchemaRef {
        self.properties().schema().clone()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // By default try to maximize parallelism with more CPUs if
        // possible
        self.required_input_distribution()
            .into_iter()
            .map(|dist| !matches!(dist, Distribution::SinglePartition))
            .collect()
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &datafusion_common::config::ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

impl DisplayAs for FranzWindowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FranzWindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
        }
        Ok(())
    }
}

fn create_schema(
    input_schema: &Schema,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Schema> {
    let capacity = input_schema.fields().len() + window_expr.len();
    let mut builder = SchemaBuilder::with_capacity(capacity);
    builder.extend(input_schema.fields().iter().cloned());
    // append results to the schema
    for expr in window_expr {
        builder.push(expr.field()?);
    }
    Ok(builder.finish())
}

pub struct FranzWindowAggStream {
    pub schema: SchemaRef,
    input: SendableRecordBatchStream,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    ordered_partition_by_indices: Vec<usize>,
    baseline_metrics: BaselineMetrics,
    latest_watermark: Arc<Mutex<Option<SystemTime>>>,
    window_frames: BTreeMap<SystemTime, FranzWindowFrame>,
    window_type: FranzWindowType,
}

impl FranzWindowAggStream {
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
        window_type: FranzWindowType,
        watermark: Arc<Mutex<Option<SystemTime>>>,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        //if window_expr[0].partition_by().len() != ordered_partition_by_indices.len() {
        //    return internal_err!("All partition by columns should have an ordering");
        //}
        Ok(Self {
            schema,
            input,
            window_expr,
            partition_by_sort_keys,
            ordered_partition_by_indices,
            baseline_metrics,
            latest_watermark: watermark,
            window_frames: BTreeMap::new(),
            window_type,
        })
    }

    fn trigger_windows(&mut self) -> (Result<RecordBatch>, bool) {
        let mut results: Vec<RecordBatch> = Vec::new();
        let watermark_lock: std::sync::MutexGuard<'_, Option<SystemTime>> =
            self.latest_watermark.lock().unwrap();

        if let Some(watermark) = *watermark_lock {
            let mut window_frames_to_remove: Vec<SystemTime> = Vec::new();

            for (timestamp, frame) in self.window_frames.iter() {
                if watermark >= frame.window_end_time {
                    let batches_to_eval = frame.evaluate();
                    for batch in batches_to_eval {
                        results.push(batch);
                    }
                    window_frames_to_remove.push(*timestamp);
                }
            }

            for timestamp in window_frames_to_remove {
                self.window_frames.remove(&timestamp);
            }
        }

        if results.is_empty() {
            return (Ok(RecordBatch::new_empty(self.input.schema())), true);
        }

        (
            concat_batches(&self.input.schema(), &results)
                .map_err(|err| DataFusionError::ArrowError(err, None)),
            false,
        )
    }

    fn compute_aggregates(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // record compute time on drop
        let _timer: crate::metrics::ScopedTimerGuard<'_> =
            self.baseline_metrics.elapsed_compute().timer();
        //let batch = concat_batches(&self.schema, &window_frame.batches)?;
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let partition_by_sort_keys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx: &usize| {
                self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&batch)
            })
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &partition_by_sort_keys)?;

        let mut partition_results: Vec<Vec<Arc<dyn Array>>> = vec![];
        // Calculate window cols
        for partition_point in partition_points {
            let length = partition_point.end - partition_point.start;
            partition_results.push(compute_window_aggregates(
                &self.window_expr,
                &batch.slice(partition_point.start, length),
            )?)
        }
        let columns = transpose(partition_results)
            .iter()
            .map(|elems: &Vec<Arc<dyn Array>>| {
                concat(&elems.iter().map(|x| x.as_ref()).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expression results are always appended to the columns
        let mut batch_columns = batch.columns().to_vec();
        // calculate window cols
        batch_columns.extend_from_slice(&columns);
        Ok(RecordBatch::try_new(self.schema.clone(), batch_columns)?)
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
            FranzWindowType::Session(duration) => duration,
            FranzWindowType::Sliding(duration, _) => duration,
            FranzWindowType::Tumbling(duration) => duration,
        }
    }

    fn get_frames_for_ranges(&mut self, ranges: &Vec<(SystemTime, SystemTime)>) {
        for (start_time, end_time) in ranges {
            self.window_frames
                .entry(*start_time)
                .or_insert(FranzWindowFrame::new(
                    *start_time,
                    *end_time,
                    "franz_canonical_timestamp".to_string(),
                ));
        }
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
                            let watermark: RecordBatchWatermark =
                                RecordBatchWatermark::try_from(
                                    &batch,
                                    "franz_canonical_timestamp",
                                )?;
                            let window_length = self.get_window_length();
                            let ranges = get_window_ranges(&watermark, window_length);
                            self.get_frames_for_ranges(&ranges);
                            for range in ranges {
                                let frame = self.window_frames.get_mut(&range.0).unwrap();
                                let _ = frame.push(&batch);
                            }
                            self.process_watermark(watermark);
                            let (result, is_empty) = self.trigger_windows();
                            if is_empty {
                                continue;
                            }
                            self.compute_aggregates(result.unwrap())
                        }
                        Some(Err(e)) => Err(e),
                        None => Ok(RecordBatch::new_empty(self.schema.clone())),
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

fn get_window_ranges(
    watermark: &RecordBatchWatermark,
    window_length: Duration,
) -> Vec<(SystemTime, SystemTime)> {
    let start_time = watermark.min_timestamp;
    let end_time = watermark.max_timestamp;
    let mut window_ranges = Vec::new();
    let mut current_start = snap_to_window_start(start_time, window_length);

    while current_start < end_time {
        let current_end = current_start + window_length;
        window_ranges.push((current_start, current_end));
        current_start = current_end;
    }

    window_ranges
}

fn snap_to_window_start(timestamp: SystemTime, window_length: Duration) -> SystemTime {
    let timestamp_duration = timestamp.duration_since(UNIX_EPOCH).unwrap();
    let window_length_secs = window_length.as_secs();
    let timestamp_secs = timestamp_duration.as_secs();
    let window_start_secs = (timestamp_secs / window_length_secs) * window_length_secs;
    UNIX_EPOCH + Duration::from_secs(window_start_secs)
}
