// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Grouped aggregate tables.
//!
//! # Design Goals
//!
//! These tables are designed so that updating the aggregates can be
//! vectorized (done in a tight loop) without allocations. The
//! accumulator state is *not* stored inline with the group values and
//! instead is delegated to the individual
//! accumulators which have type specialized inner loops that perform
//! the aggregation.
//!
//! # Architecture
//!
//! ```text
//!
//!     Assigns a consecutive group           internally stores aggregate values
//!     index for each unique set                     for all groups
//!         of group values
//!
//!         ┌────────────┐              ┌──────────────┐       ┌──────────────┐
//!         │ ┌────────┐ │              │┌────────────┐│       │┌────────────┐│
//!         │ │  "A"   │ │              ││accumulator ││       ││accumulator ││
//!         │ ├────────┤ │              ││     0      ││       ││     N      ││
//!         │ │  "Z"   │ │              ││ ┌────────┐ ││       ││ ┌────────┐ ││
//!         │ └────────┘ │              ││ │ state  │ ││       ││ │ state  │ ││
//!         │            │              ││ │┌─────┐ │ ││  ...  ││ │┌─────┐ │ ││
//!         │    ...     │              ││ │├─────┤ │ ││       ││ │├─────┤ │ ││
//!         │            │              ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
//!         │            │              ││ │        │ ││       ││ │        │ ││
//!         │ ┌────────┐ │              ││ │  ...   │ ││       ││ │  ...   │ ││
//!         │ │  "Q"   │ │              ││ │        │ ││       ││ │        │ ││
//!         │ └────────┘ │              ││ │┌─────┐ │ ││       ││ │┌─────┐ │ ││
//!         │            │              ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
//!         └────────────┘              ││ └────────┘ ││       ││ └────────┘ ││
//!                                     │└────────────┘│       │└────────────┘│
//!                                     └──────────────┘       └──────────────┘
//!
//!         group_values                             accumulators
//!
//! ```
//!
//! For example, given a query like `COUNT(x), SUM(y) ... GROUP BY z`,
//! [`GroupValues`] will store the distinct values of `z`. There will
//! be one accumulator for `COUNT(x)`, specialized for the data type
//! of `x` and one accumulator for `SUM(y)`, specialized for the data
//! type of `y`.
//!
//! # Discussion
//!
//! [`GroupValues`] does not store any aggregate state inline. It only
//! assigns "group indices", one for each (distinct) group value. The
//! accumulators manage the in-progress aggregate state for each
//! group, while the group values themselves are stored in
//! [`GroupValues`] at the corresponding group index.
//!
//! The accumulator state (e.g partial sums) is managed by and stored
//! by a [`GroupsAccumulator`] accumulator. There is one accumulator
//! per aggregate expression (COUNT, AVG, etc) in the
//! table. Internally, each `GroupsAccumulator` manages the state for
//! multiple groups, and is passed `group_indices` during update. Note
//! that the accumulator state is not stored inline with the group values.
//!
//! [`GroupValues`]: crate::aggregates::group_values::GroupValues
//! [`GroupsAccumulator`]: datafusion_expr::GroupsAccumulator
//!

mod clustered_final_table;
mod clustered_partial_table;
mod clustered_single_table;
mod common;
mod common_clustered;
mod final_table;
mod partial_reduce_table;
mod partial_table;
mod single_table;

use std::sync::Arc;

use crate::aggregates::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics, aggregate_sub_metrics,
};
use crate::aggregates::{AggregateExec, AggregateMode, aggregate_metric_label};
use datafusion_expr::AggregateMetrics;

pub(super) fn accumulator_phases(mode: &AggregateMode) -> &'static [AccumulatorPhase] {
    match mode {
        AggregateMode::Partial => &[
            AccumulatorPhase::Update,
            AccumulatorPhase::State,
            AccumulatorPhase::ConvertToState,
        ],
        AggregateMode::PartialReduce => {
            &[AccumulatorPhase::Merge, AccumulatorPhase::State]
        }
        // Final and single aggregation emit intermediate states when spilling,
        // then replay them through a final aggregate table.
        AggregateMode::Final | AggregateMode::FinalPartitioned => &[
            AccumulatorPhase::Merge,
            AccumulatorPhase::State,
            AccumulatorPhase::Evaluate,
        ],
        AggregateMode::Single | AggregateMode::SinglePartitioned => &[
            AccumulatorPhase::Update,
            AccumulatorPhase::State,
            AccumulatorPhase::Merge,
            AccumulatorPhase::Evaluate,
        ],
    }
}

pub(super) struct AggregateTableMetrics {
    pub(super) group_by: GroupByMetrics,
    pub(super) aggregate_arguments: AggregateArgumentMetrics,
    pub(super) accumulator: Arc<AggregateAccumulatorMetrics>,
    pub(super) submetrics: Vec<Arc<dyn AggregateMetrics>>,
}

impl AggregateTableMetrics {
    pub(super) fn new(agg: &AggregateExec, partition: usize) -> Self {
        let aggregate_labels = agg
            .aggr_expr()
            .iter()
            .map(|agg_expr| aggregate_metric_label(agg_expr))
            .collect::<Vec<_>>();

        Self {
            group_by: GroupByMetrics::new(&agg.metrics, partition),
            submetrics: aggregate_sub_metrics(
                &agg.metrics,
                partition,
                aggregate_labels.iter().cloned(),
            ),
            aggregate_arguments: AggregateArgumentMetrics::new(
                &agg.metrics,
                partition,
                aggregate_labels.clone(),
            ),
            accumulator: Arc::new(AggregateAccumulatorMetrics::new(
                &agg.metrics,
                partition,
                aggregate_labels,
                accumulator_phases(&agg.mode),
            )),
        }
    }
}

pub(super) use common::{
    AggregateHashTable, FinalMarker, PartialMarker, PartialReduceMarker,
    PartialSkipMarker, SingleMarker, create_group_accumulator,
};
pub(super) use common_clustered::{
    ClusteredAggregateTable, ClusteredAggregateTableMetrics,
};

#[cfg(test)]
mod tests {
    use super::accumulator_phases;
    use crate::aggregates::AggregateMode;
    use crate::aggregates::group_values::AccumulatorPhase;

    #[test]
    fn accumulator_phases_match_aggregate_mode() {
        let cases: [(AggregateMode, &[AccumulatorPhase]); 6] = [
            (
                AggregateMode::Partial,
                &[
                    AccumulatorPhase::Update,
                    AccumulatorPhase::State,
                    AccumulatorPhase::ConvertToState,
                ],
            ),
            (
                AggregateMode::PartialReduce,
                &[AccumulatorPhase::Merge, AccumulatorPhase::State],
            ),
            (
                AggregateMode::Final,
                &[
                    AccumulatorPhase::Merge,
                    AccumulatorPhase::State,
                    AccumulatorPhase::Evaluate,
                ],
            ),
            (
                AggregateMode::FinalPartitioned,
                &[
                    AccumulatorPhase::Merge,
                    AccumulatorPhase::State,
                    AccumulatorPhase::Evaluate,
                ],
            ),
            (
                AggregateMode::Single,
                &[
                    AccumulatorPhase::Update,
                    AccumulatorPhase::State,
                    AccumulatorPhase::Merge,
                    AccumulatorPhase::Evaluate,
                ],
            ),
            (
                AggregateMode::SinglePartitioned,
                &[
                    AccumulatorPhase::Update,
                    AccumulatorPhase::State,
                    AccumulatorPhase::Merge,
                    AccumulatorPhase::Evaluate,
                ],
            ),
        ];

        for (mode, expected) in cases {
            assert!(accumulator_phases(&mode) == expected, "{mode:?}");
        }
    }
}
