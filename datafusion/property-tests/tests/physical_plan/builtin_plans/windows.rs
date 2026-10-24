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

//! Window operators, built as the physical planner builds them, with the
//! input order modes that the optimizer chooses for input that is already
//! sorted, and the `PartitionedTopKExec` that `WindowTopN` puts below a
//! ranking window.

use std::sync::Arc;

use arrow::compute::SortOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_functions_window::cume_dist::cume_dist_udwf;
use datafusion_functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion_functions_window::ntile::ntile_udwf;
use datafusion_functions_window::rank::{dense_rank_udwf, rank_udwf};
use datafusion_functions_window::row_number::row_number_udwf;
use datafusion_physical_expr::expressions::{Literal, col};
use datafusion_physical_expr::window::WindowExpr;
use datafusion_physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion_physical_plan::InputOrderMode;
use datafusion_physical_plan::sorts::partitioned_topk::{
    PartitionedTopKExec, WindowFnKind,
};
use datafusion_physical_plan::windows::{
    BoundedWindowAggExec, WindowAggExec, create_window_expr,
};
use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
use datafusion_property_tests::physical_plan::harness::PlanFactory;

use super::{Plan, aggregate_spec, aggregate_spec_sorted_on_g, one_input};

/// The rows a `PartitionedTopKExec` keeps per window partition
const TOP_K: usize = 5;

/// Why window operators allow `limit_pushdown_missed`, which reports them
/// because they produce one row per input row in order
const WINDOWS_NEED_LATER_ROWS: &str = "a window function can need later \
    rows, as LEAD, NTILE, CUME_DIST and frames that end after the current row \
    do, so a window operator cannot support limit pushdown in general; \
    limit_pushdown_past_window pushes limits past the windows that only read \
    earlier rows";

/// A window function with its arguments and frame, before it is bound to the
/// `PARTITION BY` and `ORDER BY` clauses shared by every function of a window
/// operator
#[derive(Clone)]
struct Function {
    definition: WindowFunctionDefinition,
    name: &'static str,
    /// The columns passed as arguments
    args: Vec<&'static str>,
    /// An integer literal passed after the columns, such as the number of
    /// buckets of `NTILE`
    literal: Option<i64>,
    frame: Frame,
}

/// The frame of a window function
#[derive(Debug, Clone, Copy)]
enum Frame {
    /// The default frame: from the start of the partition to the current row
    /// and its peers with an `ORDER BY`, and the whole partition without one
    Default,
    /// `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW`
    PreviousRow,
    /// `RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING`
    Range,
    /// `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    WholePartition,
}

impl Frame {
    fn window_frame(self, has_order_by: bool) -> WindowFrame {
        match self {
            Frame::Default => WindowFrame::new(has_order_by.then_some(false)),
            Frame::PreviousRow => WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                WindowFrameBound::CurrentRow,
            ),
            Frame::Range => WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Preceding(ScalarValue::Int64(Some(2))),
                WindowFrameBound::Following(ScalarValue::Int64(Some(2))),
            ),
            Frame::WholePartition => WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::Following(ScalarValue::UInt64(None)),
            ),
        }
    }
}

fn function(definition: WindowFunctionDefinition, name: &'static str) -> Function {
    Function {
        definition,
        name,
        args: vec![],
        literal: None,
        frame: Frame::Default,
    }
}

fn row_number() -> Function {
    function(
        WindowFunctionDefinition::WindowUDF(row_number_udwf()),
        "row_number",
    )
}

fn rank() -> Function {
    function(WindowFunctionDefinition::WindowUDF(rank_udwf()), "rank")
}

fn dense_rank() -> Function {
    function(
        WindowFunctionDefinition::WindowUDF(dense_rank_udwf()),
        "dense_rank",
    )
}

/// `sum(v)` over `frame`
fn sum_v(frame: Frame) -> Function {
    Function {
        args: vec!["v"],
        frame,
        ..function(WindowFunctionDefinition::AggregateUDF(sum_udaf()), "sum")
    }
}

/// The clauses shared by the functions of a window operator
#[derive(Debug, Clone, Copy)]
struct Over {
    partition_by: &'static [&'static str],
    order_by: &'static [&'static str],
}

/// `PARTITION BY g ORDER BY v`
const BY_G_ORDER_BY_V: Over = Over {
    partition_by: &["g"],
    order_by: &["v"],
};

/// The window expressions of `functions` over `over`, bound to the schema of
/// `input`, as the physical planner creates them
fn window_exprs(
    functions: &[Function],
    over: Over,
    input: &Plan,
) -> Result<Vec<Arc<dyn WindowExpr>>> {
    let schema = input.schema();
    let partition_by = over
        .partition_by
        .iter()
        .map(|name| col(name, &schema))
        .collect::<Result<Vec<_>>>()?;
    let order_by = over
        .order_by
        .iter()
        .map(|name| {
            Ok(PhysicalSortExpr::new(
                col(name, &schema)?,
                SortOptions::default(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    functions
        .iter()
        .map(|function| {
            let mut args = function
                .args
                .iter()
                .map(|name| col(name, &schema))
                .collect::<Result<Vec<_>>>()?;
            if let Some(literal) = function.literal {
                args.push(Arc::new(Literal::new(ScalarValue::Int64(Some(literal))))
                    as Arc<dyn PhysicalExpr>);
            }
            let name = format!(
                "{}({}) PARTITION BY [{}] ORDER BY [{}]",
                function.name,
                function.args.join(", "),
                over.partition_by.join(", "),
                over.order_by.join(", ")
            );
            let frame = function.frame.window_frame(!order_by.is_empty());
            create_window_expr(
                &function.definition,
                name,
                &args,
                &partition_by,
                &order_by,
                Arc::new(frame),
                Arc::clone(&schema),
                false,
                false,
                None,
            )
        })
        .collect()
}

/// A `BoundedWindowAggExec` of `functions` over `over`, in `mode`, that can
/// be repartitioned, as the physical planner builds it with more than one
/// target partition (and the optimizer for input that is already sorted
/// when the mode is not `Sorted`)
fn bounded(
    name: &str,
    spec: SourceSpec,
    functions: Vec<Function>,
    over: Over,
    mode: InputOrderMode,
) -> PlanFactory {
    one_input(name, spec, move |input| {
        let exprs = window_exprs(&functions, over, &input)?;
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            exprs,
            input,
            mode.clone(),
            true,
        )?))
    })
    .allow("limit_pushdown_missed", WINDOWS_NEED_LATER_ROWS)
}

/// A `WindowAggExec` of `functions` over `over`, that can be repartitioned,
/// as the physical planner builds it for functions that need the whole
/// window partition
fn unbounded(name: &str, functions: Vec<Function>, over: Over) -> PlanFactory {
    one_input(name, aggregate_spec(), move |input| {
        let exprs = window_exprs(&functions, over, &input)?;
        Ok(Arc::new(WindowAggExec::try_new(exprs, input, true)?))
    })
    .allow("limit_pushdown_missed", WINDOWS_NEED_LATER_ROWS)
}

/// `function() OVER (PARTITION BY g ORDER BY v)` limited to the first
/// [`TOP_K`] ranks of each window partition, as `WindowTopN` rewrites a
/// filter on the rank: the window over a `PartitionedTopKExec`
fn top_k(name: &str, function: Function, kind: WindowFnKind) -> PlanFactory {
    one_input(name, aggregate_spec(), move |input| {
        let schema = input.schema();
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new_default(col("g", &schema)?),
            PhysicalSortExpr::new(col("v", &schema)?, SortOptions::default()),
        ])
        .unwrap();
        let top_k: Plan = Arc::new(PartitionedTopKExec::try_new(
            input, ordering, 1, TOP_K, kind,
        )?);
        let exprs =
            window_exprs(std::slice::from_ref(&function), BY_G_ORDER_BY_V, &top_k)?;
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            exprs,
            top_k,
            InputOrderMode::Sorted,
            true,
        )?))
    })
    .allow("limit_pushdown_missed", WINDOWS_NEED_LATER_ROWS)
}

/// The window operators under test
pub(super) fn window_plans() -> Result<Vec<PlanFactory>> {
    let lag_lead = vec![
        Function {
            args: vec!["v"],
            ..function(WindowFunctionDefinition::WindowUDF(lag_udwf()), "lag")
        },
        Function {
            args: vec!["v"],
            ..function(WindowFunctionDefinition::WindowUDF(lead_udwf()), "lead")
        },
    ];
    let ntile_cume_dist = vec![
        Function {
            literal: Some(4),
            ..function(WindowFunctionDefinition::WindowUDF(ntile_udwf()), "ntile")
        },
        function(
            WindowFunctionDefinition::WindowUDF(cume_dist_udwf()),
            "cume_dist",
        ),
    ];
    Ok(vec![
        bounded(
            "BoundedWindowAggExec ROW_NUMBER",
            aggregate_spec(),
            vec![row_number()],
            BY_G_ORDER_BY_V,
            InputOrderMode::Sorted,
        ),
        bounded(
            "BoundedWindowAggExec RANK, DENSE_RANK and a running SUM",
            aggregate_spec(),
            vec![rank(), dense_rank(), sum_v(Frame::PreviousRow)],
            BY_G_ORDER_BY_V,
            InputOrderMode::Sorted,
        ),
        bounded(
            "BoundedWindowAggExec LAG and LEAD",
            aggregate_spec(),
            lag_lead,
            BY_G_ORDER_BY_V,
            InputOrderMode::Sorted,
        ),
        bounded(
            "BoundedWindowAggExec SUM over a RANGE frame",
            aggregate_spec(),
            vec![sum_v(Frame::Range)],
            BY_G_ORDER_BY_V,
            InputOrderMode::Sorted,
        ),
        bounded(
            "BoundedWindowAggExec ROW_NUMBER without PARTITION BY",
            aggregate_spec(),
            vec![row_number()],
            Over {
                partition_by: &[],
                order_by: &["v"],
            },
            InputOrderMode::Sorted,
        ),
        // On input sorted on g, the window partitions of (g, h) are sorted
        // on their first key only
        bounded(
            "BoundedWindowAggExec PartiallySorted",
            aggregate_spec_sorted_on_g()?,
            vec![row_number()],
            Over {
                partition_by: &["g", "h"],
                order_by: &["v"],
            },
            InputOrderMode::PartiallySorted(vec![0]),
        ),
        // On input sorted on g, the window partitions of h are not sorted
        bounded(
            "BoundedWindowAggExec Linear",
            aggregate_spec_sorted_on_g()?,
            vec![row_number()],
            Over {
                partition_by: &["h"],
                order_by: &["v"],
            },
            InputOrderMode::Linear,
        ),
        unbounded(
            "WindowAggExec SUM over the whole window partition",
            vec![sum_v(Frame::WholePartition)],
            Over {
                partition_by: &["g"],
                order_by: &[],
            },
        ),
        unbounded(
            "WindowAggExec NTILE and CUME_DIST",
            ntile_cume_dist,
            BY_G_ORDER_BY_V,
        ),
        unbounded(
            "WindowAggExec SUM without PARTITION BY",
            vec![sum_v(Frame::WholePartition)],
            Over {
                partition_by: &[],
                order_by: &[],
            },
        ),
        top_k(
            "PartitionedTopKExec ROW_NUMBER",
            row_number(),
            WindowFnKind::RowNumber,
        ),
        top_k("PartitionedTopKExec RANK", rank(), WindowFnKind::Rank),
        top_k(
            "PartitionedTopKExec DENSE_RANK",
            dense_rank(),
            WindowFnKind::DenseRank,
        ),
    ])
}
