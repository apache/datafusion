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

mod common;
mod common_ordered;
mod final_table;
mod ordered_final_table;
mod ordered_partial_table;
mod ordered_single_table;
mod partial_reduce_table;
mod partial_table;
mod single_table;

use std::sync::Arc;

use crate::aggregates::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics,
};
use crate::aggregates::{AggregateExec, AggregateMode, aggregate_metric_label};

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
}

impl AggregateTableMetrics {
    pub(super) fn new(agg: &AggregateExec, partition: usize) -> Self {
        let aggregate_labels = agg
            .aggr_expr
            .iter()
            .map(|agg_expr| aggregate_metric_label(agg_expr))
            .collect::<Vec<_>>();

        Self {
            group_by: GroupByMetrics::new(&agg.metrics, partition),
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
    PartialSkipMarker, SingleMarker,
};
pub(super) use common_ordered::{OrderedAggregateTable, OrderedAggregateTableMetrics};

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
