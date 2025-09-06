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

//! Streaming pattern matcher operating on Arrow `RecordBatch`es.

use crate::metrics::{Count, Gauge, Time};
use arrow_schema::Schema;
use datafusion_common::Result;
use std::ops::Deref;
use std::sync::Arc;

use crate::match_recognize::compile::CompiledPattern;
use crate::match_recognize::nfa::RowIdx;

// Add module declarations and imports for the split helpers
mod dedup;
mod symbols;

pub(crate) mod candidate_mod;
pub(crate) mod epsilon;
pub(crate) mod generation;
pub(crate) mod row_loop;
pub(crate) mod state_set;

/// Mode that controls how ε-closures should treat ^ / $ anchor predicates.
#[derive(Copy, Clone, Debug)]
pub(crate) enum AnchorMode {
    /// Ignore anchor predicates completely (fast path).
    Ignore,
    /// Evaluate anchors on the given virtual row (0-based) within a partition
    /// of `total_virt_rows` rows (including the virtual End-Of-Input row).
    Check {
        virt_row: RowIdx,
        total_virt_rows: usize,
    },
}

/// Metadata for a symbol column. Keeps the numeric symbol id so that the hot
/// path (`evaluate_symbols_static`) can avoid a hashmap lookup.
#[derive(Debug, Clone)]
struct SymbolColumn {
    id: usize,
    // Resolved (static) column index inside the input RecordBatch schema
    column_idx: usize,
}

/// Core pattern matching engine implementing NFA logic
pub struct PatternMatcher {
    /// Shared, immutable compiled representation.
    compiled: Arc<CompiledPattern>,
    /// Symbol metadata, including the resolved (static) column index. The
    /// mapping is computed once during construction as the schema of every
    /// RecordBatch produced by a physical plan is immutable.
    symbol_columns: Vec<SymbolColumn>,
    /// Next match number to assign (run-time mutable state)
    next_match_number: u64,
    /// Optional metrics hooks to update runtime counters/gauges
    metrics: Option<MatcherMetrics>,
}

#[derive(Clone, Debug)]
struct MatcherMetrics {
    define_pred_evals: Count,
    nfa_state_transitions: Count,
    active_states_max: Gauge,
    match_compute_time: Time,
    symbol_eval_time: Time,
    epsilon_eval_time: Time,
    nfa_eval_time: Time,
    slice_time: Time,
    row_loop_time: Time,
    transition_time: Time,
    candidate_time: Time,
    alloc_time: Time,
}

// Candidate now lives in `candidate_mod`.

impl PatternMatcher {
    /// Create a new `PatternMatcher` instance.
    pub fn new(compiled: Arc<CompiledPattern>, schema: &Schema) -> Result<Self> {
        // Build symbol column metadata based on the compiled pattern's symbol mapping
        let symbol_columns: Vec<SymbolColumn> = compiled
            .symbols_iter()
            .map(|(id, name)| {
                let col_name =
                    datafusion_expr::match_recognize::columns::symbol_col_name(name);
                let column_idx = schema.index_of(&col_name)?;
                Ok(SymbolColumn { id, column_idx })
            })
            .collect::<Result<Vec<_>, datafusion_common::DataFusionError>>()?;

        Ok(Self {
            compiled,
            symbol_columns,
            next_match_number: 1,
            metrics: None,
        })
    }

    /// Create a new PatternMatcher wired up with runtime metrics.
    pub(crate) fn new_with_metrics(
        compiled: Arc<CompiledPattern>,
        schema: &Schema,
        pm: &crate::match_recognize::pattern_exec::PatternMetrics,
    ) -> Result<Self> {
        let mut matcher = Self::new(compiled, schema)?;
        matcher.metrics = Some(MatcherMetrics {
            define_pred_evals: pm.define_pred_evals.clone(),
            nfa_state_transitions: pm.nfa_state_transitions.clone(),
            active_states_max: pm.active_states_max.clone(),
            match_compute_time: pm.match_compute_time.clone(),
            symbol_eval_time: pm.symbol_eval_time.clone(),
            epsilon_eval_time: pm.epsilon_eval_time.clone(),
            nfa_eval_time: pm.nfa_eval_time.clone(),
            slice_time: pm.slice_time.clone(),
            row_loop_time: pm.row_loop_time.clone(),
            transition_time: pm.transition_time.clone(),
            candidate_time: pm.candidate_time.clone(),
            alloc_time: pm.alloc_time.clone(),
        });
        Ok(matcher)
    }

    /// Returns a clone of the shared compiled pattern `Arc`.
    #[inline]
    pub fn compiled_arc(&self) -> Arc<CompiledPattern> {
        Arc::clone(&self.compiled)
    }

    /// Reset internal state so the matcher can process a new logical partition.
    #[inline]
    pub fn finish_partition(&mut self) {
        self.reset();
    }

    /// Reset per-partition counters (exposed for `finish_partition`).
    #[inline]
    pub fn reset(&mut self) {
        self.next_match_number = 1;
    }
}

// Provide transparent access to compile-time fields (e.g. `self.id_to_symbol`).
impl Deref for PatternMatcher {
    type Target = CompiledPattern;
    fn deref(&self) -> &Self::Target {
        &self.compiled
    }
}
