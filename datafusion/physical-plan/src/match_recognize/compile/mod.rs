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

//! Compiles SQL MATCH_RECOGNIZE `Pattern`s into a dense `NFA`.
//!
//! The implementation is split into several focused sub‐modules for
//! ease of navigation:
//!
//! * `quantifier` – logic related to repetition quantifiers
//! * `concat_alt` – helpers for concatenation and alternation
//! * `builder` – the `NfaBuilder` façade used by [`CompiledPattern::compile`]

pub mod builder;
pub mod concat_alt;
pub mod permute;
pub mod quantifier;

pub use builder::NfaBuilder;

use datafusion_common::{HashMap, Result};
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, Pattern, RowsPerMatch,
};
use std::sync::Arc;

use crate::match_recognize::nfa::{Sym, NFA};

/// Compiled, thread-safe representation of a MATCH_RECOGNIZE pattern.
#[derive(Debug)]
pub struct CompiledPattern {
    /// Original pattern AST for display/debugging purposes
    pub(crate) pattern: Pattern,
    pub(crate) id_to_symbol: Vec<Option<String>>,
    pub(crate) symbol_to_id: HashMap<String, usize>,
    pub(crate) nfa: NFA,
    pub(crate) after_match_skip: AfterMatchSkip,
    pub(crate) empty_matches_mode: EmptyMatchesMode,
    pub(crate) has_anchor_preds: bool,
}

impl CompiledPattern {
    /// Iterator over `(symbol_id, name)` pairs for user-defined symbols.
    pub fn symbols_iter(&self) -> impl Iterator<Item = (usize, &str)> + '_ {
        self.id_to_symbol
            .iter()
            .enumerate()
            .skip(1) // skip Empty placeholder (id 0)
            .filter_map(|(id, opt)| opt.as_deref().map(|name| (id, name)))
    }

    pub fn compile(
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: AfterMatchSkip,
        rows_per_match: RowsPerMatch,
    ) -> Result<Self> {
        let mut nfa = NfaBuilder::build(&pattern)?;
        let empty_matches_mode = match rows_per_match {
            RowsPerMatch::AllRows(mode) => mode,
            // Default to OMIT semantics for modes that do not emit unmatched rows
            RowsPerMatch::OneRow => EmptyMatchesMode::Omit,
        };

        // Symbol mapping (no dedicated anchor symbols anymore)
        // Index 0 reserved for `Empty`, user symbols start at 1
        let mut id_to_symbol: Vec<Option<String>> = vec![None]; // 0 = Empty

        let mut symbol_to_id: HashMap<String, usize> = HashMap::new();

        let mut next_id = Sym::User(0).to_index();
        for s in &symbols {
            symbol_to_id.insert(s.clone(), next_id);
            id_to_symbol.push(Some(s.clone()));
            next_id += 1;
        }

        let num_symbols = id_to_symbol.len();
        // Populate per-state numeric transitions in-place and detect anchor predicates.
        if let Some(slice) = Arc::get_mut(&mut nfa.0) {
            for state in slice.iter_mut() {
                // Allocate dense vector sized to `num_symbols` with default empty vectors
                let mut dense: Vec<Vec<usize>> = vec![Vec::new(); num_symbols];
                for (sym, dests) in &state.transitions {
                    if let Some(&sym_id) = symbol_to_id.get(sym) {
                        dense[sym_id] = dests.iter().copied().collect();
                    }
                }
                state.numeric_transitions = dense;
                // Drop the string-based map to avoid duplication
                state.transitions = HashMap::new();
            }
        }

        // Detect presence of anchor predicates on ε-transitions
        let has_anchor_preds = nfa.iter().any(|state| {
            state
                .epsilon_transitions_pred
                .iter()
                .any(|edge| edge.1.is_some())
        });

        Ok(Self {
            pattern,
            id_to_symbol,
            symbol_to_id,
            nfa,
            after_match_skip,
            empty_matches_mode,
            has_anchor_preds,
        })
    }
}

////////////////////////////////////////////////////////////////
// Builder-style API //////////////////////////////////////////

pub struct CompiledPatternBuilder {
    pattern: Pattern,
    symbols: Vec<String>,
    after_match_skip: AfterMatchSkip,
    rows_per_match: RowsPerMatch,
}

impl CompiledPatternBuilder {
    /// Start a new builder with the required `pattern`.
    pub fn new(pattern: Pattern) -> Self {
        Self {
            pattern,
            symbols: Vec::new(),
            after_match_skip: AfterMatchSkip::PastLastRow,
            rows_per_match: RowsPerMatch::OneRow,
        }
    }

    /// Specify the DECLARE / DEFINE symbols for the pattern.
    pub fn symbols(mut self, symbols: Vec<String>) -> Self {
        self.symbols = symbols;
        self
    }

    /// Specify the `AFTER MATCH SKIP` strategy.
    pub fn after_match_skip(mut self, skip: AfterMatchSkip) -> Self {
        self.after_match_skip = skip;
        self
    }

    /// Specify the `ROWS PER MATCH` clause.
    pub fn rows_per_match(mut self, rows: RowsPerMatch) -> Self {
        self.rows_per_match = rows;
        self
    }

    /// Finalise construction by delegating to the existing `compile` fn.
    pub fn build(self) -> Result<CompiledPattern> {
        CompiledPattern::compile(
            self.pattern,
            self.symbols,
            self.after_match_skip,
            self.rows_per_match,
        )
    }
}

impl CompiledPattern {
    /// Convenience helper: `CompiledPattern::builder(pattern)`.
    pub fn builder(pattern: Pattern) -> CompiledPatternBuilder {
        CompiledPatternBuilder::new(pattern)
    }
}
