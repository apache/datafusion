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

//! Stream and channel implementations for window function expressions.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::utils::create_schema;
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::EmptyRecordBatchStream;
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, InputDistributionRequirements, PhysicalExpr, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics, WindowExpr,
    check_if_same_properties,
};

use arrow::array::ArrayRef;
use arrow::compute::{concat, concat_batches};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::utils::{evaluate_partition_ranges, transpose};
use datafusion_common::{Result, assert_eq_or_internal_err};
use datafusion_execution::TaskContext;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use futures::{Stream, StreamExt, ready};

/// Window execution plan
#[derive(Debug, Clone)]
pub struct WindowAggExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Partition by indices that defines preset for existing ordering
    // see `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// If `can_partition` is false, partition_keys is always empty.
    can_repartition: bool,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(window_expr[0].partition_by(), &input)?;
        let cache = Self::compute_properties(&schema, &input, &window_expr)?;
        Ok(Self {
            input,
            window_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            ordered_partition_by_indices,
            cache: Arc::new(cache),
            can_repartition,
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
        schema: &SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
        window_exprs: &[Arc<dyn WindowExpr>],
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_exprs)?;

        // Get output partitioning:
        // Because we can have repartitioning using the partition keys this
        // would be either 1 or more than 1 depending on the presence of repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache:
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            // TODO: Emission type and boundedness information can be enhanced here
            EmissionType::Final,
            input.boundedness(),
        ))
    }

    pub fn partition_keys(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        if !self.can_repartition {
            vec![]
        } else {
            let all_partition_keys = self
                .window_expr()
                .iter()
                .map(|expr| expr.partition_by().to_vec())
                .collect::<Vec<_>>();

            all_partition_keys
                .into_iter()
                .min_by_key(|s| s.len())
                .unwrap_or_else(Vec::new)
        }
    }
}

impl DisplayAs for WindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
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
            DisplayFormatType::TreeRender => {
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| e.name().to_owned().to_string())
                    .collect();
                writeln!(f, "select_list={}", g.join(", "))?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for WindowAggExec {
    fn name(&self) -> &'static str {
        "WindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
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
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.partition_keys().is_empty() {
            InputDistributionRequirements::new(vec![Distribution::SinglePartition])
        } else {
            InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(
                self.partition_keys(),
            )])
            .allow_range_satisfaction_for_key_partitioning()
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        Ok(Arc::new(WindowAggExec::try_new(
            self.window_expr.clone(),
            children.swap_remove(0),
            true,
        )?))
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(&*self)
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let stream = Box::pin(WindowAggStream::new(
            Arc::clone(&self.schema),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
            self.ordered_partition_by_indices.clone(),
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let input_stat = input_stats[0].as_ref().clone();
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Arc::new(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use datafusion_proto_models::protobuf;
        let input = ctx.encode_child(self.input())?;
        let window_expr = self
            .window_expr()
            .iter()
            .map(|e| encode_physical_window_expr(e, ctx))
            .collect::<Result<Vec<_>>>()?;
        let partition_keys = self
            .partition_keys()
            .iter()
            .map(|e| ctx.encode_expr(e))
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Window(Box::new(
                    protobuf::WindowAggExecNode {
                        input: Some(Box::new(input)),
                        window_expr,
                        partition_keys,
                        // `None` distinguishes a `WindowAggExec` from a
                        // `BoundedWindowAggExec` on the shared `Window` variant.
                        input_order_mode: None,
                    },
                )),
            ),
        }))
    }
}

/// Reconstruct a window plan from its protobuf representation.
///
/// Both [`WindowAggExec`] and [`BoundedWindowAggExec`] serialize to the single
/// `PhysicalPlanType::Window(WindowAggExecNode)` proto variant; they are told
/// apart by the optional `input_order_mode` field (`None` => `WindowAggExec`,
/// `Some` => `BoundedWindowAggExec`). This one associated function therefore
/// owns the decode for *both* plans and the central dispatch routes the `Window`
/// arm here.
#[cfg(feature = "proto")]
impl WindowAggExec {
    pub fn try_from_proto(
        node: &datafusion_proto_models::protobuf::PhysicalPlanNode,
        ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        use super::BoundedWindowAggExec;
        use crate::InputOrderMode;
        use datafusion_proto_models::protobuf;
        use protobuf::window_agg_exec_node::InputOrderMode as ProtoInputOrderMode;

        let window_agg = crate::expect_plan_variant!(
            node,
            protobuf::physical_plan_node::PhysicalPlanType::Window,
            "WindowAggExec",
        );
        let input = ctx.decode_required_child(
            window_agg.input.as_deref(),
            "WindowAggExec",
            "input",
        )?;
        let input_schema = input.schema();

        let window_expr = window_agg
            .window_expr
            .iter()
            .map(|w| decode_physical_window_expr(w, ctx, input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        let partition_keys = window_agg
            .partition_keys
            .iter()
            .map(|e| ctx.decode_expr(e, input_schema.as_ref()))
            .collect::<Result<Vec<_>>>()?;

        if let Some(input_order_mode) = window_agg.input_order_mode.as_ref() {
            let input_order_mode = match input_order_mode {
                ProtoInputOrderMode::Linear(_) => InputOrderMode::Linear,
                ProtoInputOrderMode::PartiallySorted(
                    protobuf::PartiallySortedInputOrderMode { columns },
                ) => InputOrderMode::PartiallySorted(
                    columns.iter().map(|c| *c as usize).collect(),
                ),
                ProtoInputOrderMode::Sorted(_) => InputOrderMode::Sorted,
            };
            Ok(Arc::new(BoundedWindowAggExec::try_new(
                window_expr,
                input,
                input_order_mode,
                !partition_keys.is_empty(),
            )?))
        } else {
            Ok(Arc::new(WindowAggExec::try_new(
                window_expr,
                input,
                !partition_keys.is_empty(),
            )?))
        }
    }
}

/// Serialize a single [`WindowExpr`] to its protobuf representation.
///
/// Shared by [`WindowAggExec`] and [`BoundedWindowAggExec`]. UD(A)Fs used as
/// window functions are encoded to opaque bytes through the ctx
/// (`encode_udaf`/`encode_udwf`); the extension codec never leaks here.
#[cfg(feature = "proto")]
pub(crate) fn encode_physical_window_expr(
    window_expr: &Arc<dyn WindowExpr>,
    ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
) -> Result<datafusion_proto_models::protobuf::PhysicalWindowExprNode> {
    use super::{PlainAggregateWindowExpr, StandardWindowExpr, WindowUDFExpr};
    use datafusion_common::not_impl_err;
    use datafusion_physical_expr::window::SlidingAggregateWindowExpr;
    use datafusion_proto_models::protobuf::{self, physical_window_expr_node};

    let expr = window_expr.as_any();
    let mut args = window_expr.expressions().to_vec();
    let window_frame = window_expr.get_window_frame();

    let (window_function, fun_definition, ignore_nulls, distinct) =
        if let Some(plain) = expr.downcast_ref::<PlainAggregateWindowExpr>() {
            let aggr_expr = plain.get_aggregate_expr();
            (
                physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
                    aggr_expr.fun().name().to_string(),
                ),
                ctx.encode_udaf(aggr_expr.fun())?,
                aggr_expr.ignore_nulls(),
                aggr_expr.is_distinct(),
            )
        } else if let Some(sliding) = expr.downcast_ref::<SlidingAggregateWindowExpr>() {
            let aggr_expr = sliding.get_aggregate_expr();
            (
                physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
                    aggr_expr.fun().name().to_string(),
                ),
                ctx.encode_udaf(aggr_expr.fun())?,
                aggr_expr.ignore_nulls(),
                aggr_expr.is_distinct(),
            )
        } else if let Some(udf_window_expr) = expr.downcast_ref::<StandardWindowExpr>() {
            if let Some(e) = udf_window_expr
                .get_standard_func_expr()
                .as_any()
                .downcast_ref::<WindowUDFExpr>()
            {
                // `WindowUDFExpr::args` returns the full, unfiltered argument list so
                // every argument survives the round-trip.
                args = e.args().to_vec();
                (
                    physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
                        e.fun().name().to_string(),
                    ),
                    ctx.encode_udwf(e.fun().as_ref())?,
                    // `WindowUDFExpr` has no ignore_nulls/distinct.
                    false,
                    false,
                )
            } else {
                return not_impl_err!(
                    "User-defined window function not supported: {window_expr:?}"
                );
            }
        } else {
            return not_impl_err!("WindowExpr not supported: {window_expr:?}");
        };

    let args = ctx.encode_expressions(&args)?;
    let partition_by = ctx.encode_expressions(window_expr.partition_by())?;
    let order_by = window_expr
        .order_by()
        .iter()
        .map(|sort_expr| {
            Ok(protobuf::PhysicalSortExprNode {
                expr: Some(Box::new(ctx.encode_expr(&sort_expr.expr)?)),
                asc: !sort_expr.options.descending,
                nulls_first: sort_expr.options.nulls_first,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let window_frame = encode_window_frame(window_frame.as_ref())?;

    Ok(protobuf::PhysicalWindowExprNode {
        args,
        partition_by,
        order_by,
        window_frame: Some(window_frame),
        window_function: Some(window_function),
        name: window_expr.name().to_string(),
        fun_definition,
        ignore_nulls,
        distinct,
    })
}

/// Reconstruct a single [`WindowExpr`] from its protobuf representation.
#[cfg(feature = "proto")]
fn decode_physical_window_expr(
    proto: &datafusion_proto_models::protobuf::PhysicalWindowExprNode,
    ctx: &crate::proto::ExecutionPlanDecodeCtx<'_>,
    input_schema: &arrow::datatypes::Schema,
) -> Result<Arc<dyn WindowExpr>> {
    use super::{create_window_expr, schema_add_window_field};
    use arrow::compute::SortOptions;
    use datafusion_common::{internal_datafusion_err, internal_err};
    use datafusion_expr::WindowFunctionDefinition;
    use datafusion_proto_models::protobuf::physical_window_expr_node;

    let args = proto
        .args
        .iter()
        .map(|e| ctx.decode_expr(e, input_schema))
        .collect::<Result<Vec<_>>>()?;
    let partition_by = proto
        .partition_by
        .iter()
        .map(|e| ctx.decode_expr(e, input_schema))
        .collect::<Result<Vec<_>>>()?;
    let order_by = proto
        .order_by
        .iter()
        .map(|sort_expr| {
            let expr = sort_expr.expr.as_ref().ok_or_else(|| {
                internal_datafusion_err!(
                    "Missing expr in window order_by sort expression"
                )
            })?;
            Ok(PhysicalSortExpr {
                expr: ctx.decode_expr(expr, input_schema)?,
                options: SortOptions {
                    descending: !sort_expr.asc,
                    nulls_first: sort_expr.nulls_first,
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let window_frame = proto
        .window_frame
        .as_ref()
        .map(decode_window_frame)
        .transpose()?
        .ok_or_else(|| {
            internal_datafusion_err!("Missing required field 'window_frame' in protobuf")
        })?;

    let fun = match proto.window_function.as_ref() {
        Some(physical_window_expr_node::WindowFunction::UserDefinedAggrFunction(
            name,
        )) => WindowFunctionDefinition::AggregateUDF(
            ctx.decode_udaf(name, proto.fun_definition.as_deref())?,
        ),
        Some(physical_window_expr_node::WindowFunction::UserDefinedWindowFunction(
            name,
        )) => WindowFunctionDefinition::WindowUDF(
            ctx.decode_udwf(name, proto.fun_definition.as_deref())?,
        ),
        None => {
            return internal_err!("Missing required field 'window_function' in protobuf");
        }
    };

    let name = proto.name.clone();
    // TODO: Remove extended_schema if functions are all UDAF
    let extended_schema = schema_add_window_field(&args, input_schema, &fun, &name)?;
    create_window_expr(
        &fun,
        name,
        &args,
        &partition_by,
        &order_by,
        Arc::new(window_frame),
        extended_schema,
        proto.ignore_nulls,
        proto.distinct,
        None,
    )
}

/// Serialize a [`WindowFrame`](datafusion_expr::WindowFrame) inline, by-name.
///
/// The `datafusion-proto` `TryFromProto` conversions live above this crate, so
/// the frame/units/bound enums are matched exhaustively by name here (never a
/// numeric cast) to stay wire-identical and to make a new variant a compile
/// error.
#[cfg(feature = "proto")]
fn encode_window_frame(
    window_frame: &datafusion_expr::WindowFrame,
) -> Result<datafusion_proto_models::protobuf::WindowFrame> {
    use datafusion_expr::WindowFrameUnits;
    use datafusion_proto_models::protobuf;

    let units = match window_frame.units {
        WindowFrameUnits::Rows => protobuf::WindowFrameUnits::Rows,
        WindowFrameUnits::Range => protobuf::WindowFrameUnits::Range,
        WindowFrameUnits::Groups => protobuf::WindowFrameUnits::Groups,
    };
    Ok(protobuf::WindowFrame {
        window_frame_units: units.into(),
        start_bound: Some(encode_window_frame_bound(&window_frame.start_bound)?),
        end_bound: Some(protobuf::window_frame::EndBound::Bound(
            encode_window_frame_bound(&window_frame.end_bound)?,
        )),
    })
}

#[cfg(feature = "proto")]
fn encode_window_frame_bound(
    bound: &datafusion_expr::WindowFrameBound,
) -> Result<datafusion_proto_models::protobuf::WindowFrameBound> {
    use datafusion_expr::WindowFrameBound;
    use datafusion_proto_common::protobuf_common;
    use datafusion_proto_models::protobuf;

    // Bind the conversion target explicitly so the `?` error type is unambiguous
    // and stays wire-identical to the `TryFrom<&ScalarValue>` proto encoding.
    let encode_value =
        |v: &datafusion_common::ScalarValue| -> Result<protobuf_common::ScalarValue> {
            Ok(v.try_into()?)
        };
    Ok(match bound {
        WindowFrameBound::CurrentRow => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::CurrentRow.into(),
            bound_value: None,
        },
        WindowFrameBound::Preceding(v) => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::Preceding.into(),
            bound_value: Some(encode_value(v)?),
        },
        WindowFrameBound::Following(v) => protobuf::WindowFrameBound {
            window_frame_bound_type: protobuf::WindowFrameBoundType::Following.into(),
            bound_value: Some(encode_value(v)?),
        },
    })
}

#[cfg(feature = "proto")]
fn decode_window_frame(
    window_frame: &datafusion_proto_models::protobuf::WindowFrame,
) -> Result<datafusion_expr::WindowFrame> {
    use datafusion_common::internal_datafusion_err;
    use datafusion_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use datafusion_proto_models::protobuf;

    let units =
        match protobuf::WindowFrameUnits::try_from(window_frame.window_frame_units)
            .map_err(|_| {
                internal_datafusion_err!(
                    "Received a WindowFrame message with unknown WindowFrameUnits {}",
                    window_frame.window_frame_units
                )
            })? {
            protobuf::WindowFrameUnits::Rows => WindowFrameUnits::Rows,
            protobuf::WindowFrameUnits::Range => WindowFrameUnits::Range,
            protobuf::WindowFrameUnits::Groups => WindowFrameUnits::Groups,
        };
    let start_bound =
        decode_window_frame_bound(window_frame.start_bound.as_ref().ok_or_else(
            || internal_datafusion_err!("Missing start_bound in WindowFrame"),
        )?)?;
    let end_bound = window_frame
        .end_bound
        .as_ref()
        .map(|end_bound| match end_bound {
            protobuf::window_frame::EndBound::Bound(end_bound) => {
                decode_window_frame_bound(end_bound)
            }
        })
        .transpose()?
        .unwrap_or(WindowFrameBound::CurrentRow);
    Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
}

#[cfg(feature = "proto")]
fn decode_window_frame_bound(
    bound: &datafusion_proto_models::protobuf::WindowFrameBound,
) -> Result<datafusion_expr::WindowFrameBound> {
    use datafusion_common::{ScalarValue, internal_datafusion_err};
    use datafusion_expr::WindowFrameBound;
    use datafusion_proto_common::protobuf_common;
    use datafusion_proto_models::protobuf;

    // Bind the conversion source explicitly so the `?` error type is unambiguous.
    let decode_value = |x: &protobuf_common::ScalarValue| -> Result<ScalarValue> {
        Ok(ScalarValue::try_from(x)?)
    };
    let bound_type = protobuf::WindowFrameBoundType::try_from(
        bound.window_frame_bound_type,
    )
    .map_err(|_| {
        internal_datafusion_err!(
            "Received a WindowFrameBound message with unknown WindowFrameBoundType {}",
            bound.window_frame_bound_type
        )
    })?;
    match bound_type {
        protobuf::WindowFrameBoundType::CurrentRow => Ok(WindowFrameBound::CurrentRow),
        protobuf::WindowFrameBoundType::Preceding => match &bound.bound_value {
            Some(x) => Ok(WindowFrameBound::Preceding(decode_value(x)?)),
            None => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None))),
        },
        protobuf::WindowFrameBoundType::Following => match &bound.bound_value {
            Some(x) => Ok(WindowFrameBound::Following(decode_value(x)?)),
            None => Ok(WindowFrameBound::Following(ScalarValue::UInt64(None))),
        },
    }
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    baseline_metrics: BaselineMetrics,
    ordered_partition_by_indices: Vec<usize>,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        assert_eq_or_internal_err!(
            window_expr[0].partition_by().len(),
            ordered_partition_by_indices.len(),
            "All partition by columns should have an ordering"
        );
        Ok(Self {
            schema,
            input,
            batches: vec![],
            finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
            ordered_partition_by_indices,
        })
    }

    fn compute_aggregates(&self) -> Result<Option<RecordBatch>> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = concat_batches(&self.input.schema(), &self.batches)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        let partition_by_sort_keys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &partition_by_sort_keys)?;

        let mut partition_results = vec![];
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
            .map(|elems| concat(&elems.iter().map(|x| x.as_ref()).collect::<Vec<_>>()))
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?;

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expression results are always appended to the columns
        let mut batch_columns = batch.columns().to_vec();
        // calculate window cols
        batch_columns.extend_from_slice(&columns);
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            batch_columns,
        )?))
    }
}

impl Stream for WindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            return Poll::Ready(Some(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.batches.push(batch);
                    continue;
                }
                Some(Err(e)) => Err(e),
                None => {
                    // Release the input pipeline's resources before computing
                    // the final aggregates.
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    let Some(result) = self.compute_aggregates()? else {
                        return Poll::Ready(None);
                    };
                    self.finished = true;
                    // Empty record batches should not be emitted.
                    // They need to be treated as  [`Option<RecordBatch>`]es and handled separately
                    debug_assert!(result.num_rows() > 0);
                    Ok(result)
                }
            }));
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::TestMemoryExec;
    use crate::windows::create_window_expr;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::count::count_udaf;

    #[test]
    fn test_window_agg_cardinality_effect() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let args = vec![crate::expressions::col("a", &schema)?];
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count(a)".to_string(),
            &args,
            &[],
            &[],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;

        let window = WindowAggExec::try_new(vec![window_expr], input, true)?;
        assert!(matches!(
            window.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        Ok(())
    }
}
