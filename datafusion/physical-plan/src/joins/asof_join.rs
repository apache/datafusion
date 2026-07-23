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

//! Ordered, left-preserving ASOF join execution.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Formatter;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow::compute::{SortOptions, interleave};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::utils::{
    compare_rows, get_row_at_idx, normalize_float_zero_scalar,
};
use datafusion_common::{
    ColumnStatistics, JoinType, Result, ScalarValue, Statistics,
    assert_eq_or_internal_err, internal_err, plan_err,
};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::Column as PhysicalColumn;
use datafusion_physical_expr::projection::ProjectionMapping;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion_physical_expr_common::physical_expr::{
    PhysicalExprRef, fmt_sql, is_volatile,
};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, OrderingRequirements};
use futures::{StreamExt, stream};

use crate::execution_plan::{Boundedness, EmissionType};
use crate::filter_pushdown::{
    ChildFilterDescription, ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::joins::utils::{JoinOn, build_join_schema};
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricCategory,
    MetricsSet, RecordOutput, Time,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    InputDistributionRequirements, PlanProperties, SendableRecordBatchStream,
    check_if_same_properties,
};

/// Physical ordered comparison for an ASOF join.
#[derive(Debug, Clone)]
pub struct AsOfMatchExpr {
    /// Expression evaluated against the left input.
    pub left: PhysicalExprRef,
    /// Ordered comparison operator.
    pub op: Operator,
    /// Expression evaluated against the right input.
    pub right: PhysicalExprRef,
}

impl AsOfMatchExpr {
    /// Creates a physical ASOF match expression.
    pub fn new(left: PhysicalExprRef, op: Operator, right: PhysicalExprRef) -> Self {
        Self { left, op, right }
    }
}

/// A sort-merge ASOF join that emits exactly one row for every left row.
#[derive(Debug, Clone)]
pub struct AsOfJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    match_condition: AsOfMatchExpr,
    right_output_indices: Vec<usize>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    left_ordering: LexOrdering,
    right_ordering: LexOrdering,
    cache: Arc<PlanProperties>,
}

impl AsOfJoinExec {
    /// Creates a bounded ASOF join over sorted inputs.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        match_condition: AsOfMatchExpr,
        right_output_indices: Vec<usize>,
    ) -> Result<Self> {
        if !matches!(
            match_condition.op,
            Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
        ) {
            return plan_err!(
                "AsOfJoinExec requires <, <=, >, or >=, found {}",
                match_condition.op
            );
        }
        if left.boundedness().is_unbounded() || right.boundedness().is_unbounded() {
            return plan_err!("AsOfJoinExec requires bounded inputs");
        }
        if is_volatile(&match_condition.left) || is_volatile(&match_condition.right) {
            return plan_err!("AsOfJoinExec match expression must be deterministic");
        }
        if on
            .iter()
            .any(|(left, right)| is_volatile(left) || is_volatile(right))
        {
            return plan_err!("AsOfJoinExec equality expressions must be deterministic");
        }

        let left_schema = left.schema();
        let right_schema = right.schema();
        validate_expr_side(&match_condition.left, &left_schema, "left match")?;
        validate_expr_side(&match_condition.right, &right_schema, "right match")?;
        for (left_expr, right_expr) in &on {
            validate_expr_side(left_expr, &left_schema, "left equality")?;
            validate_expr_side(right_expr, &right_schema, "right equality")?;
            let left_type = left_expr.data_type(&left_schema)?;
            let right_type = right_expr.data_type(&right_schema)?;
            if left_type != right_type {
                return plan_err!(
                    "AsOfJoinExec equality expression types differ: {left_type} and {right_type}"
                );
            }
            if !datafusion_expr::utils::can_hash(&left_type) {
                return plan_err!(
                    "AsOfJoinExec equality expressions have unsupported hash type {left_type}"
                );
            }
        }
        let left_match_type = match_condition.left.data_type(&left_schema)?;
        let right_match_type = match_condition.right.data_type(&right_schema)?;
        if left_match_type != right_match_type {
            return plan_err!(
                "AsOfJoinExec match expression types differ: {left_match_type} and {right_match_type}"
            );
        }
        if let Some(index) = right_output_indices
            .iter()
            .find(|index| **index >= right_schema.fields().len())
        {
            return plan_err!(
                "AsOfJoinExec right output index {index} is outside schema with {} fields",
                right_schema.fields().len()
            );
        }
        if !right_output_indices
            .windows(2)
            .all(|pair| pair[0] < pair[1])
        {
            return plan_err!(
                "AsOfJoinExec right output indices must be strictly increasing"
            );
        }

        let schema =
            build_output_schema(&left_schema, &right_schema, &right_output_indices);
        let descending = matches!(match_condition.op, Operator::Lt | Operator::LtEq);
        let equality_options = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let match_options = SortOptions {
            descending,
            nulls_first: true,
        };
        let mut left_sort_exprs = on
            .iter()
            .map(|(left, _)| PhysicalSortExpr {
                expr: Arc::clone(left),
                options: equality_options,
            })
            .collect::<Vec<_>>();
        left_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::clone(&match_condition.left),
            options: match_options,
        });
        let mut right_sort_exprs = on
            .iter()
            .map(|(_, right)| PhysicalSortExpr {
                expr: Arc::clone(right),
                options: equality_options,
            })
            .collect::<Vec<_>>();
        right_sort_exprs.push(PhysicalSortExpr {
            expr: Arc::clone(&match_condition.right),
            options: match_options,
        });
        let left_ordering = LexOrdering::new(left_sort_exprs).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ASOF left ordering must not be empty"
            )
        })?;
        let right_ordering = LexOrdering::new(right_sort_exprs).ok_or_else(|| {
            datafusion_common::internal_datafusion_err!(
                "ASOF right ordering must not be empty"
            )
        })?;
        let cache = Arc::new(Self::compute_properties(&left, &schema, on.is_empty())?);

        Ok(Self {
            left,
            right,
            on,
            match_condition,
            right_output_indices,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            left_ordering,
            right_ordering,
            cache,
        })
    }

    fn compute_properties(
        left: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        single_partition: bool,
    ) -> Result<PlanProperties> {
        let left_schema = left.schema();
        let mapping = ProjectionMapping::try_new(
            left_schema
                .fields()
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    (
                        Arc::new(PhysicalColumn::new(field.name(), index))
                            as PhysicalExprRef,
                        field.name().to_string(),
                    )
                }),
            &left_schema,
        )?;
        let input_eq_properties = left.equivalence_properties();
        let eq_properties = input_eq_properties.project(&mapping, Arc::clone(schema));
        let output_partitioning = if single_partition {
            Partitioning::UnknownPartitioning(1)
        } else {
            left.output_partitioning()
                .project(&mapping, input_eq_properties)
        };
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }

    /// Equality expressions.
    pub fn on(&self) -> &JoinOn {
        &self.on
    }

    /// Ordered match expression.
    pub fn match_condition(&self) -> &AsOfMatchExpr {
        &self.match_condition
    }

    /// Indices of right input columns emitted after the left columns.
    pub fn right_output_indices(&self) -> &[usize] {
        &self.right_output_indices
    }

    /// Left input.
    pub fn left(&self) -> &Arc<dyn ExecutionPlan> {
        &self.left
    }

    /// Right input.
    pub fn right(&self) -> &Arc<dyn ExecutionPlan> {
        &self.right
    }
}

fn build_output_schema(
    left: &SchemaRef,
    right: &SchemaRef,
    right_output_indices: &[usize],
) -> SchemaRef {
    let full_schema = build_join_schema(left, right, &JoinType::Left).0;
    let left_len = left.fields().len();
    let fields = full_schema
        .fields()
        .iter()
        .take(left_len)
        .cloned()
        .chain(
            right_output_indices
                .iter()
                .map(|index| Arc::clone(&full_schema.fields()[left_len + *index])),
        )
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(
        fields,
        full_schema.metadata().clone(),
    ))
}

impl DisplayAs for AsOfJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let on = self
            .on
            .iter()
            .map(|(left, right)| {
                format!("({} = {})", fmt_sql(left.as_ref()), fmt_sql(right.as_ref()))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let match_condition = format!(
            "{} {} {}",
            fmt_sql(self.match_condition.left.as_ref()),
            self.match_condition.op,
            fmt_sql(self.match_condition.right.as_ref())
        );
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "{}: on=[{}], match=[{}]",
                Self::static_name(),
                on,
                match_condition
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "on={on}")?;
                writeln!(f, "match={match_condition}")
            }
        }
    }
}

impl ExecutionPlan for AsOfJoinExec {
    fn name(&self) -> &'static str {
        "AsOfJoinExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.on.is_empty() {
            InputDistributionRequirements::new(vec![
                Distribution::SinglePartition,
                Distribution::SinglePartition,
            ])
        } else {
            let (left, right) = self
                .on
                .iter()
                .map(|(left, right)| (Arc::clone(left), Arc::clone(right)))
                .unzip();
            InputDistributionRequirements::co_partitioned(vec![
                Distribution::KeyPartitioned(left),
                Distribution::KeyPartitioned(right),
            ])
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![
            Some(OrderingRequirements::from(self.left_ordering.clone())),
            Some(OrderingRequirements::from(self.right_ordering.clone())),
        ]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true, false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        match &children[..] {
            [left, right] => Ok(Arc::new(Self::try_new(
                Arc::clone(left),
                Arc::clone(right),
                self.on.clone(),
                self.match_condition.clone(),
                self.right_output_indices.clone(),
            )?)),
            _ => internal_err!("AsOfJoinExec requires two children"),
        }
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            2,
            "AsOfJoinExec requires two children"
        );
        let left = children.remove(0);
        let right = children.remove(0);
        Ok(Arc::new(Self {
            left,
            right,
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(&self)
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let left_partitions = self.left.output_partitioning().partition_count();
        let right_partitions = self.right.output_partitioning().partition_count();
        assert_eq_or_internal_err!(
            left_partitions,
            right_partitions,
            "AsOfJoinExec partition count mismatch: {left_partitions} != {right_partitions}"
        );
        let left_stream = self.left.execute(partition, Arc::clone(&context))?;
        let right_stream = self.right.execute(partition, Arc::clone(&context))?;
        let (left_keys, right_keys) = self.on.iter().cloned().unzip();
        let state = AsOfJoinStreamState::new(
            Arc::clone(&self.schema),
            InputCursor::new(
                left_stream,
                left_keys,
                Arc::clone(&self.match_condition.left),
            ),
            InputCursor::new(
                right_stream,
                right_keys,
                Arc::clone(&self.match_condition.right),
            ),
            self.match_condition.op,
            self.right_output_indices.clone(),
            context.session_config().batch_size(),
            AsOfJoinMetrics::new(partition, &self.metrics),
        );
        let stream = stream::try_unfold(state, |mut state| async move {
            match state.next_batch().await? {
                Some(batch) => Ok(Some((batch, state))),
                None => Ok(None),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition), ChildStats::Skip]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let left = &input_stats[0];
        let mut column_statistics = left.column_statistics.clone();
        column_statistics.truncate(self.left.schema().fields().len());
        column_statistics.resize_with(
            self.left.schema().fields().len(),
            ColumnStatistics::new_unknown,
        );
        column_statistics.extend(
            self.right_output_indices
                .iter()
                .map(|_| ColumnStatistics::new_unknown()),
        );
        Ok(Arc::new(Statistics {
            num_rows: left.num_rows,
            total_byte_size: Precision::Absent,
            column_statistics,
        }))
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<PhysicalExprRef>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        let left_indices = (0..self.left.schema().fields().len()).collect::<HashSet<_>>();
        let left = ChildFilterDescription::from_child_with_allowed_indices(
            &parent_filters,
            left_indices,
            &self.left,
        )?;
        let right = ChildFilterDescription::all_unsupported(&parent_filters);
        Ok(FilterDescription::new().with_child(left).with_child(right))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_any(child_pushdown_result))
    }
}

#[derive(Clone)]
struct Candidate {
    batch: Arc<RecordBatch>,
    row: usize,
    group: Vec<ScalarValue>,
}

struct InputCursor {
    stream: SendableRecordBatchStream,
    key_exprs: Vec<PhysicalExprRef>,
    match_expr: PhysicalExprRef,
    batch: Option<Arc<RecordBatch>>,
    key_arrays: Vec<ArrayRef>,
    match_array: Option<ArrayRef>,
    row: usize,
    eof: bool,
}

impl InputCursor {
    fn new(
        stream: SendableRecordBatchStream,
        key_exprs: Vec<PhysicalExprRef>,
        match_expr: PhysicalExprRef,
    ) -> Self {
        Self {
            stream,
            key_exprs,
            match_expr,
            batch: None,
            key_arrays: vec![],
            match_array: None,
            row: 0,
            eof: false,
        }
    }

    async fn ensure_row(&mut self, elapsed_compute: &Time) -> Result<bool> {
        loop {
            if let Some(batch) = &self.batch
                && self.row < batch.num_rows()
            {
                return Ok(true);
            }
            self.batch = None;
            self.key_arrays.clear();
            self.match_array = None;
            self.row = 0;
            if self.eof {
                return Ok(false);
            }
            let Some(batch) = self.stream.next().await.transpose()? else {
                self.eof = true;
                return Ok(false);
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = Arc::new(batch);
            let _timer = elapsed_compute.timer();
            self.key_arrays = self
                .key_exprs
                .iter()
                .map(|expr| expr.evaluate(&batch)?.into_array(batch.num_rows()))
                .collect::<Result<_>>()?;
            self.match_array = Some(
                self.match_expr
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?,
            );
            self.batch = Some(batch);
        }
    }

    fn group(&self) -> Result<Vec<ScalarValue>> {
        get_row_at_idx(&self.key_arrays, self.row)
            .map(|row| row.into_iter().map(normalize_float_zero_scalar).collect())
    }

    fn match_value(&self) -> Result<ScalarValue> {
        let array = self.match_array.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("ASOF match array is missing")
        })?;
        ScalarValue::try_from_array(array, self.row).map(normalize_float_zero_scalar)
    }

    fn batch_row(&self) -> Result<(Arc<RecordBatch>, usize)> {
        let batch = self.batch.as_ref().ok_or_else(|| {
            datafusion_common::internal_datafusion_err!("ASOF input batch is missing")
        })?;
        Ok((Arc::clone(batch), self.row))
    }

    fn advance(&mut self) {
        self.row += 1;
    }
}

struct AsOfJoinMetrics {
    baseline: BaselineMetrics,
    matched_rows: Count,
    unmatched_left_rows: Count,
}

impl AsOfJoinMetrics {
    fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            matched_rows: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("matched_rows", partition),
            unmatched_left_rows: MetricBuilder::new(metrics)
                .with_category(MetricCategory::Rows)
                .counter("unmatched_left_rows", partition),
        }
    }
}

#[derive(Default)]
struct PendingRows {
    sources: Vec<Arc<RecordBatch>>,
    source_by_ptr: HashMap<usize, usize>,
    indices: Vec<Option<(usize, usize)>>,
}

impl PendingRows {
    fn len(&self) -> usize {
        self.indices.len()
    }

    fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    fn push(&mut self, batch: Arc<RecordBatch>, row: usize) {
        let ptr = Arc::as_ptr(&batch) as usize;
        let source = *self.source_by_ptr.entry(ptr).or_insert_with(|| {
            let source = self.sources.len();
            self.sources.push(batch);
            source
        });
        self.indices.push(Some((source, row)));
    }

    fn push_null(&mut self) {
        self.indices.push(None);
    }

    fn materialize_column(
        &self,
        source_column: usize,
        data_type: &arrow::datatypes::DataType,
    ) -> Result<ArrayRef> {
        if self.indices.is_empty() {
            return internal_err!("ASOF output materialization has no pending rows");
        }

        if self.sources.len() == 1
            && self.indices.iter().all(Option::is_some)
            && let Some((0, first_row)) = self.indices[0]
            && self
                .indices
                .iter()
                .enumerate()
                .all(|(offset, index)| *index == Some((0, first_row + offset)))
        {
            return Ok(self.sources[0]
                .column(source_column)
                .slice(first_row, self.indices.len()));
        }

        let has_null = self.indices.iter().any(Option::is_none);
        let null_array = has_null.then(|| new_null_array(data_type, 1));
        let mut source_arrays: Vec<&dyn Array> =
            Vec::with_capacity(self.sources.len() + usize::from(has_null));
        if let Some(null_array) = &null_array {
            source_arrays.push(null_array.as_ref());
        }
        source_arrays.extend(
            self.sources
                .iter()
                .map(|batch| batch.column(source_column).as_ref()),
        );
        let source_offset = usize::from(has_null);
        let interleave_indices = self
            .indices
            .iter()
            .map(|index| match index {
                Some((source, row)) => (source + source_offset, *row),
                None => (0, 0),
            })
            .collect::<Vec<_>>();
        interleave(&source_arrays, &interleave_indices).map_err(Into::into)
    }

    fn clear(&mut self) {
        self.sources.clear();
        self.source_by_ptr.clear();
        self.indices.clear();
    }
}

struct AsOfJoinStreamState {
    schema: SchemaRef,
    left: InputCursor,
    right: InputCursor,
    op: Operator,
    right_output_indices: Vec<usize>,
    candidate: Option<Candidate>,
    group_sort_options: Vec<SortOptions>,
    pending_left: PendingRows,
    pending_right: PendingRows,
    batch_size: usize,
    metrics: AsOfJoinMetrics,
}

impl AsOfJoinStreamState {
    fn new(
        schema: SchemaRef,
        left: InputCursor,
        right: InputCursor,
        op: Operator,
        right_output_indices: Vec<usize>,
        batch_size: usize,
        metrics: AsOfJoinMetrics,
    ) -> Self {
        let group_sort_options = vec![
            SortOptions {
                descending: false,
                nulls_first: true,
            };
            left.key_exprs.len()
        ];
        Self {
            pending_left: PendingRows::default(),
            pending_right: PendingRows::default(),
            schema,
            left,
            right,
            op,
            right_output_indices,
            candidate: None,
            group_sort_options,
            batch_size: batch_size.max(1),
            metrics,
        }
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.pending_left.len() >= self.batch_size {
                return self.flush().map(Some);
            }
            if !self
                .left
                .ensure_row(self.metrics.baseline.elapsed_compute())
                .await?
            {
                if !self.pending_left.is_empty() {
                    return self.flush().map(Some);
                }
                self.metrics.baseline.done();
                return Ok(None);
            }

            let (left_group, left_match) = {
                let _timer = self.metrics.baseline.elapsed_compute().timer();
                (self.left.group()?, self.left.match_value()?)
            };
            if left_match.is_null() || left_group.iter().any(ScalarValue::is_null) {
                self.candidate = None;
                self.push_current_left(None)?;
                self.left.advance();
                continue;
            }
            let candidate_is_other_group = if let Some(candidate) = &self.candidate {
                let _timer = self.metrics.baseline.elapsed_compute().timer();
                compare_rows(&candidate.group, &left_group, &self.group_sort_options)?
                    != Ordering::Equal
            } else {
                false
            };
            if candidate_is_other_group {
                self.candidate = None;
            }

            loop {
                if !self
                    .right
                    .ensure_row(self.metrics.baseline.elapsed_compute())
                    .await?
                {
                    break;
                }
                let action = {
                    let _timer = self.metrics.baseline.elapsed_compute().timer();
                    let right_group = self.right.group()?;
                    if right_group.iter().any(ScalarValue::is_null) {
                        RightAction::Advance
                    } else {
                        match compare_rows(
                            &right_group,
                            &left_group,
                            &self.group_sort_options,
                        )? {
                            Ordering::Less => RightAction::Advance,
                            Ordering::Greater => RightAction::Stop,
                            Ordering::Equal => {
                                let right_match = self.right.match_value()?;
                                if right_match.is_null() {
                                    RightAction::Advance
                                } else if is_eligible(self.op, &left_match, &right_match)?
                                {
                                    let (batch, row) = self.right.batch_row()?;
                                    RightAction::Candidate(Candidate {
                                        batch,
                                        row,
                                        group: right_group,
                                    })
                                } else {
                                    RightAction::Stop
                                }
                            }
                        }
                    }
                };
                match action {
                    RightAction::Advance => self.right.advance(),
                    RightAction::Candidate(candidate) => {
                        self.candidate = Some(candidate);
                        self.right.advance();
                    }
                    RightAction::Stop => break,
                }
            }

            self.push_current_left(self.candidate.clone())?;
            self.left.advance();
        }
    }

    fn push_current_left(&mut self, candidate: Option<Candidate>) -> Result<()> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let (left_batch, left_row) = self.left.batch_row()?;
        self.pending_left.push(left_batch, left_row);
        match candidate {
            Some(candidate) => {
                if !self.right_output_indices.is_empty() {
                    self.pending_right.push(candidate.batch, candidate.row);
                }
                self.metrics.matched_rows.add(1);
            }
            None => {
                if !self.right_output_indices.is_empty() {
                    self.pending_right.push_null();
                }
                self.metrics.unmatched_left_rows.add(1);
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<RecordBatch> {
        let _timer = self.metrics.baseline.elapsed_compute().timer();
        let left_len = self.schema.fields().len() - self.right_output_indices.len();
        let mut arrays = Vec::with_capacity(self.schema.fields().len());
        for index in 0..left_len {
            arrays.push(
                self.pending_left
                    .materialize_column(index, self.schema.field(index).data_type())?,
            );
        }
        for (offset, source_index) in self.right_output_indices.iter().enumerate() {
            arrays.push(self.pending_right.materialize_column(
                *source_index,
                self.schema.field(left_len + offset).data_type(),
            )?);
        }
        self.pending_left.clear();
        self.pending_right.clear();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        (&batch).record_output(&self.metrics.baseline);
        Ok(batch)
    }
}

fn validate_expr_side(expr: &PhysicalExprRef, schema: &Schema, name: &str) -> Result<()> {
    let columns = collect_columns(expr);
    if columns.is_empty() {
        return plan_err!("AsOfJoinExec {name} expression must reference its input");
    }
    if let Some(column) = columns.iter().find(|column| {
        schema
            .fields()
            .get(column.index())
            .is_none_or(|field| field.name() != column.name())
    }) {
        return plan_err!(
            "AsOfJoinExec {name} expression references column {column} outside its input"
        );
    }
    Ok(())
}

enum RightAction {
    Advance,
    Candidate(Candidate),
    Stop,
}

fn is_eligible(op: Operator, left: &ScalarValue, right: &ScalarValue) -> Result<bool> {
    let ordering = right.try_cmp(left)?;
    Ok(match op {
        Operator::Gt => ordering == Ordering::Less,
        Operator::GtEq => ordering != Ordering::Greater,
        Operator::Lt => ordering == Ordering::Greater,
        Operator::LtEq => ordering != Ordering::Less,
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::test::TestMemoryExec;
    use arrow::array::{
        DictionaryArray, Int32Array, Int64Array, StringArray, StringDictionaryBuilder,
    };
    use arrow::datatypes::{DataType, Field, Int8Type};
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr_common::metrics::MetricValue;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct VolatileExpr;

    impl std::fmt::Display for VolatileExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "volatile")
        }
    }

    impl PhysicalExpr for VolatileExpr {
        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            Ok(DataType::Int64)
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            Ok(false)
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(1))))
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn is_volatile_node(&self) -> bool {
            true
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "volatile()")
        }
    }

    fn make_batch(
        schema: &SchemaRef,
        keys: Vec<Option<&str>>,
        times: Vec<Option<i64>>,
        values: Vec<i32>,
    ) -> Result<RecordBatch> {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(times)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .map_err(Into::into)
    }

    fn test_exec() -> Result<Arc<AsOfJoinExec>> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("id", DataType::Int32, false),
        ]));
        let left_batches = vec![
            RecordBatch::new_empty(Arc::clone(&left_schema)),
            make_batch(&left_schema, vec![None], vec![Some(3)], vec![0])?,
            make_batch(
                &left_schema,
                vec![Some("A"), Some("A")],
                vec![None, Some(1)],
                vec![1, 2],
            )?,
            make_batch(
                &left_schema,
                vec![Some("A"), Some("A")],
                vec![Some(4), Some(7)],
                vec![3, 4],
            )?,
            make_batch(
                &left_schema,
                vec![Some("B"), Some("C")],
                vec![Some(2), Some(3)],
                vec![5, 6],
            )?,
        ];
        let left = TestMemoryExec::try_new_exec(
            &[left_batches],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, true),
            Field::new("price", DataType::Int32, false),
        ]));
        let right_batches = vec![
            RecordBatch::new_empty(Arc::clone(&right_schema)),
            make_batch(
                &right_schema,
                vec![None, Some("A")],
                vec![Some(2), None],
                vec![999, 777],
            )?,
            make_batch(&right_schema, vec![Some("A")], vec![Some(2)], vec![20])?,
            make_batch(&right_schema, vec![Some("A")], vec![Some(4)], vec![40])?,
            RecordBatch::new_empty(Arc::clone(&right_schema)),
            make_batch(
                &right_schema,
                vec![Some("A"), Some("B")],
                vec![Some(6), Some(1)],
                vec![60, 101],
            )?,
        ];
        let right = TestMemoryExec::try_new_exec(
            &[right_batches],
            Arc::clone(&right_schema),
            None,
        )?;

        let on: JoinOn = vec![(
            Arc::new(PhysicalColumn::new("key", 0)),
            Arc::new(PhysicalColumn::new("key", 0)),
        )];
        Ok(Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            on,
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )?))
    }

    #[test]
    fn eligibility_matches_public_semantics() -> Result<()> {
        let left = ScalarValue::Int64(Some(10));
        let lower = ScalarValue::Int64(Some(9));
        let equal = ScalarValue::Int64(Some(10));
        let higher = ScalarValue::Int64(Some(11));
        assert!(is_eligible(Operator::Gt, &left, &lower)?);
        assert!(!is_eligible(Operator::Gt, &left, &equal)?);
        assert!(is_eligible(Operator::GtEq, &left, &equal)?);
        assert!(is_eligible(Operator::Lt, &left, &higher)?);
        assert!(!is_eligible(Operator::Lt, &left, &equal)?);
        assert!(is_eligible(Operator::LtEq, &left, &equal)?);
        Ok(())
    }

    #[tokio::test]
    async fn state_survives_empty_input_batches_and_output_flushes() -> Result<()> {
        let exec = test_exec()?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(2)),
        );
        let batches = collect(Arc::clone(&exec) as _, context).await?;
        assert_eq!(
            batches
                .iter()
                .map(RecordBatch::num_rows)
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 1]
        );
        let ids = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        let prices = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
            ]
        );
        assert_eq!(
            prices,
            vec![None, None, None, Some(40), Some(60), Some(101), None]
        );

        let metrics = exec.metrics().expect("ASOF metrics must be present");
        assert_eq!(metrics.output_rows(), Some(7));
        assert_eq!(
            metrics
                .sum_by_name("matched_rows")
                .map(|value| value.as_usize()),
            Some(3)
        );
        assert_eq!(
            metrics
                .sum_by_name("unmatched_left_rows")
                .map(|value| value.as_usize()),
            Some(4)
        );
        assert!(metrics.elapsed_compute().is_some());
        assert!(
            metrics.iter().any(|metric| {
                matches!(metric.value(), MetricValue::ElapsedCompute(_))
            })
        );
        Ok(())
    }

    #[tokio::test]
    async fn preserves_dictionary_outputs_across_large_flush() -> Result<()> {
        let dictionary_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("payload", dictionary_type.clone(), false),
        ]));
        let mut left_payload = StringDictionaryBuilder::<Int8Type>::new();
        for _ in 0..129 {
            left_payload.append_value("left");
        }
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![
                Arc::new(StringArray::from(vec!["A"; 129])),
                Arc::new(Int64Array::from_iter_values(-1..128)),
                Arc::new(left_payload.finish()),
            ],
        )?;
        let left = TestMemoryExec::try_new_exec(
            &[vec![left_batch]],
            Arc::clone(&left_schema),
            None,
        )?;

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("payload", dictionary_type.clone(), false),
        ]));
        let mut right_payload = StringDictionaryBuilder::<Int8Type>::new();
        right_payload.append_value("right");
        let right_batch = RecordBatch::try_new(
            Arc::clone(&right_schema),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int64Array::from(vec![0])),
                Arc::new(right_payload.finish()),
            ],
        )?;
        let right = TestMemoryExec::try_new_exec(
            &[vec![right_batch]],
            Arc::clone(&right_schema),
            None,
        )?;

        let exec = Arc::new(AsOfJoinExec::try_new(
            left,
            right,
            vec![(
                Arc::new(PhysicalColumn::new("key", 0)),
                Arc::new(PhysicalColumn::new("key", 0)),
            )],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )?);
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(256)),
        );
        let batches = collect(exec, context).await?;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 129);
        assert_eq!(batches[0].column(2).data_type(), &dictionary_type);
        assert_eq!(batches[0].column(3).data_type(), &dictionary_type);

        let right_output = batches[0]
            .column(3)
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .expect("right output must remain Dictionary(Int8, Utf8)");
        assert!(right_output.is_null(0));
        assert_eq!(right_output.null_count(), 1);
        let values = right_output
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values must be Utf8");
        for row in 1..129 {
            assert_eq!(
                values.value(right_output.keys().value(row) as usize),
                "right"
            );
        }
        Ok(())
    }

    #[test]
    fn rejects_volatile_physical_expressions() -> Result<()> {
        let exec = test_exec()?;
        let volatile = Arc::new(VolatileExpr) as PhysicalExprRef;
        let match_error = AsOfJoinExec::try_new(
            Arc::clone(exec.left()),
            Arc::clone(exec.right()),
            exec.on().clone(),
            AsOfMatchExpr::new(
                Arc::clone(&volatile),
                Operator::GtEq,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )
        .expect_err("volatile match expression must be rejected");
        assert!(match_error.to_string().contains("must be deterministic"));

        let equality_error = AsOfJoinExec::try_new(
            Arc::clone(exec.left()),
            Arc::clone(exec.right()),
            vec![(volatile, Arc::new(PhysicalColumn::new("key", 0)))],
            exec.match_condition().clone(),
            vec![2],
        )
        .expect_err("volatile equality expression must be rejected");
        assert!(equality_error.to_string().contains("must be deterministic"));
        Ok(())
    }

    #[test]
    fn properties_and_statistics_follow_left_preserving_contract() -> Result<()> {
        let exec = test_exec()?;
        let exec_plan: Arc<dyn ExecutionPlan> = Arc::clone(&exec) as _;
        assert_eq!(exec.maintains_input_order(), vec![true, false]);
        assert_eq!(exec_plan.pipeline_behavior(), EmissionType::Incremental);
        assert_eq!(exec_plan.boundedness(), Boundedness::Bounded);
        assert!(matches!(
            &exec.input_distribution_requirements().into_per_child()[..],
            [
                Distribution::KeyPartitioned(_),
                Distribution::KeyPartitioned(_)
            ]
        ));
        for ordering in exec.required_input_ordering() {
            let requirement = ordering.expect("ASOF ordering is required").into_single();
            assert_eq!(requirement.len(), 2);
            assert_eq!(
                requirement[0].options,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                })
            );
            assert_eq!(
                requirement[1].options,
                Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                })
            );
        }

        let no_keys: Arc<dyn ExecutionPlan> = Arc::new(AsOfJoinExec::try_new(
            Arc::clone(exec.left()),
            Arc::clone(exec.right()),
            vec![],
            AsOfMatchExpr::new(
                Arc::new(PhysicalColumn::new("ts", 1)),
                Operator::Lt,
                Arc::new(PhysicalColumn::new("ts", 1)),
            ),
            vec![2],
        )?);
        assert_eq!(no_keys.output_partitioning().partition_count(), 1);
        assert!(matches!(
            &no_keys.input_distribution_requirements().into_per_child()[..],
            [Distribution::SinglePartition, Distribution::SinglePartition]
        ));
        for ordering in no_keys.required_input_ordering() {
            let requirement = ordering.expect("ASOF ordering is required").into_single();
            assert_eq!(requirement.len(), 1);
            assert_eq!(
                requirement[0].options,
                Some(SortOptions {
                    descending: true,
                    nulls_first: true,
                })
            );
        }

        let mut key_stats = ColumnStatistics::new_unknown();
        key_stats.null_count = Precision::Exact(1);
        key_stats.distinct_count = Precision::Exact(4);
        let mut ts_stats = ColumnStatistics::new_unknown();
        ts_stats.min_value = Precision::Exact(ScalarValue::Int64(Some(1)));
        ts_stats.max_value = Precision::Exact(ScalarValue::Int64(Some(7)));
        let mut id_stats = ColumnStatistics::new_unknown();
        id_stats.null_count = Precision::Exact(0);
        id_stats.distinct_count = Precision::Exact(7);
        let left_column_statistics = vec![key_stats, ts_stats, id_stats];
        let left_stats = Arc::new(Statistics {
            num_rows: Precision::Exact(7),
            total_byte_size: Precision::Exact(128),
            column_statistics: left_column_statistics.clone(),
        });
        let right_stats = Arc::new(Statistics::new_unknown(&exec.right().schema()));
        let stats = exec
            .statistics_from_inputs(&[left_stats, right_stats], &StatisticsArgs::new())?;
        assert_eq!(stats.num_rows, Precision::Exact(7));
        assert_eq!(stats.total_byte_size, Precision::Absent);
        assert_eq!(stats.column_statistics.len(), 4);
        assert_eq!(
            &stats.column_statistics[..3],
            left_column_statistics.as_slice()
        );
        assert_eq!(stats.column_statistics[3], ColumnStatistics::new_unknown());
        assert_eq!(
            exec.child_stats_requests(None),
            vec![ChildStats::At(None), ChildStats::Skip]
        );
        Ok(())
    }
}
