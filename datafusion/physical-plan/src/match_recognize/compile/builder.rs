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

//! Public façade for building an NFA from a MATCH_RECOGNIZE [`Pattern`].

use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr::match_recognize::{Pattern, Symbol};
use std::sync::Arc;

use super::permute;
use crate::match_recognize::nfa::{AnchorPredicate, NFAState, NFA}; // for PERMUTE compilation

use super::concat_alt::{compile_alternation, compile_concat};
// compile_quantifier logic moved to quantifier.rs; QuantifierParams no longer needed here.

#[derive(Debug, Default)]
pub struct NfaBuilder {
    states: Vec<NFAState>,
}

impl NfaBuilder {
    /// Create a fresh, empty builder.
    fn new() -> Self {
        Self { states: Vec::new() }
    }

    /// Compile a [`Pattern`] into a fully-formed NFA.
    pub(crate) fn build(pattern: &Pattern) -> Result<NFA> {
        let mut b = Self::new();
        let (_, end) = b.build_pattern(pattern)?;

        // Add a single accepting sink so the compiled NFA always has exactly
        // one accepting state reachable from the pattern.
        let sink = b.new_state(true, b.states[end].is_excluded);
        b.add_epsilon(end, sink);

        // Remove proxy-only ε states to produce a more compact NFA.
        b.prune_epsilon_proxies();

        // Pre-compute ε-closures once all states are in place.
        b.compute_epsilon_closures();

        Ok(NFA(b.finish()))
    }

    /// Insert a new state and return its identifier.
    pub(crate) fn new_state(&mut self, is_accepting: bool, is_excluded: bool) -> usize {
        let id = self.states.len();
        self.states.push(NFAState {
            id,
            is_accepting,
            is_excluded,
            transitions: HashMap::new(),
            numeric_transitions: Vec::new(),
            epsilon_transitions_pred: Vec::new(),
            epsilon_closure: Vec::new(),
            unconditional_closure: Vec::new(),
        });
        id
    }

    /// Add an ε-transition from `from` → `to`, propagating the exclusion flag.
    pub(crate) fn add_epsilon(&mut self, from: usize, to: usize) {
        self.states[from].epsilon_transitions_pred.push((to, None));
        if self.states[from].is_excluded {
            self.states[to].is_excluded = true;
        }
    }

    /// Build `pat` into the builder and return its local `(start,end)` indices.
    pub(crate) fn build_pattern(&mut self, pat: &Pattern) -> Result<(usize, usize)> {
        match pat {
            // Single symbol or anchor
            Pattern::Symbol(sym) => {
                let start = self.new_state(false, false);
                let end = self.new_state(false, false);

                match sym {
                    Symbol::Named(n) => {
                        let name = n.clone();
                        self.states[start]
                            .transitions
                            .entry(name)
                            .or_insert_with(HashSet::new)
                            .insert(end);
                    }
                    Symbol::Start => {
                        self.states[start]
                            .epsilon_transitions_pred
                            .push((end, Some(AnchorPredicate::StartOfInput)));
                    }
                    Symbol::End => {
                        self.states[start]
                            .epsilon_transitions_pred
                            .push((end, Some(AnchorPredicate::EndOfInput)));
                    }
                }
                Ok((start, end))
            }

            // Exclusion wrapper – behaves like Symbol but marks the end state.
            Pattern::Exclude(sym) => {
                let (s, e) = self.build_pattern(&Pattern::Symbol(sym.clone()))?;
                self.states[e].is_excluded = true;
                Ok((s, e))
            }

            // Concatenation – sequentially link sub-patterns.
            Pattern::Concat(parts) => compile_concat(self, parts),

            // Alternation – classic Thompson construction.
            Pattern::Alternation(alts) => compile_alternation(self, alts),

            // Quantifiers / repetition.
            Pattern::Repetition(inner, quant) => {
                super::quantifier::compile_quantifier(self, inner, quant)
            }

            // Explicit grouping – just delegate to inner pattern.
            Pattern::Group(inner) => self.build_pattern(inner),

            // PERMUTE – delegate to dedicated helper for clarity.
            Pattern::Permute(symbols) => permute::attach_permute(self, symbols),
        }
    }

    /// Consume the builder and return the completed state arena.
    fn finish(self) -> Arc<[NFAState]> {
        Arc::from(self.states.into_boxed_slice())
    }

    /// Shallow-clone a slice of states, applying `offset` to all IDs / links.
    pub(crate) fn copy_states_with_offset(
        states: &[NFAState],
        offset: usize,
    ) -> Vec<NFAState> {
        let mut cloned = Vec::with_capacity(states.len());
        for s in states {
            let mut new_state = s.clone();
            new_state.id = s.id + offset;

            new_state.transitions = s
                .transitions
                .iter()
                .map(|(sym, set)| {
                    let new_set: HashSet<usize> =
                        set.iter().map(|t| t + offset).collect();
                    (sym.clone(), new_set)
                })
                .collect();

            // numeric_transitions are populated later in the compiler once
            // the final symbol mapping is known, therefore reset here.
            new_state.numeric_transitions = Vec::new();

            new_state.epsilon_transitions_pred = s
                .epsilon_transitions_pred
                .iter()
                .map(|&(dst, pred)| (dst + offset, pred))
                .collect();

            new_state.epsilon_closure = Vec::new();
            cloned.push(new_state);
        }
        cloned
    }

    /// Current number of states in the arena.
    pub(crate) fn state_count(&self) -> usize {
        self.states.len()
    }

    /// Append a pre-shifted slice of states into the arena and return the
    /// starting offset.
    pub(crate) fn extend_states(&mut self, mut new_states: Vec<NFAState>) -> usize {
        let offset = self.states.len();
        self.states.append(&mut new_states);
        offset
    }

    /// After all states are in place compute the ε-closure for each state.
    fn compute_epsilon_closures(&mut self) {
        let num_states = self.states.len();
        let mut visited = vec![0u32; num_states]; // Generation counter instead of bool

        for state_id in 0..num_states {
            let mut stack = vec![state_id];
            let mut reachable = Vec::new();

            while let Some(id) = stack.pop() {
                if visited[id] == state_id as u32 {
                    continue;
                }
                visited[id] = state_id as u32;
                reachable.push(id);

                for &(dst, _) in &self.states[id].epsilon_transitions_pred {
                    stack.push(dst);
                }
            }

            reachable.sort_unstable();
            self.states[state_id].epsilon_closure = reachable;

            // Compute unconditional closure (DFS following only None predicates)
            let mut stack_u = vec![state_id];
            let mut visited_u = vec![false; num_states];
            let mut uncond_closure: Vec<usize> = Vec::new();
            while let Some(id) = stack_u.pop() {
                if visited_u[id] {
                    continue;
                }
                visited_u[id] = true;
                uncond_closure.push(id);
                for &(dst, pred) in &self.states[id].epsilon_transitions_pred {
                    if pred.is_none() {
                        stack_u.push(dst);
                    }
                }
            }
            uncond_closure.sort_unstable();
            self.states[state_id].unconditional_closure = uncond_closure;
        }
    }

    /// Remove unconditional ε proxy states (non-accepting, non-excluded,
    /// no symbol transitions, exactly one unconditional ε-edge). These
    /// states only forward control and can be eliminated without affecting
    /// semantics.
    fn prune_epsilon_proxies(&mut self) {
        let n = self.states.len();
        if n == 0 {
            return;
        }

        // Identify proxy ε states and their immediate targets
        let redirects: Vec<Option<usize>> = self
            .states
            .iter()
            .map(|state| {
                if !state.is_accepting
                    && !state.is_excluded
                    && state.transitions.is_empty()
                    && state.epsilon_transitions_pred.len() == 1
                    && state.epsilon_transitions_pred[0].1.is_none()
                {
                    Some(state.epsilon_transitions_pred[0].0)
                } else {
                    None
                }
            })
            .collect();

        // Resolve chains a→b→c ⇒ a→c on-the-fly
        let final_target = |mut id: usize| {
            while let Some(next) = redirects[id] {
                id = next;
            }
            id
        };

        // Assign new dense IDs, skipping proxies
        let mut new_id = vec![usize::MAX; n];
        let mut kept = 0;
        for i in 0..n {
            if redirects[i].is_none() {
                new_id[i] = kept;
                kept += 1;
            }
        }
        if kept == n {
            return; // nothing pruned
        }

        // Rebuild kept states with patched edges
        let mut new_states = Vec::with_capacity(kept);
        for (old_id, old_state) in self.states.iter().enumerate() {
            if redirects[old_id].is_some() {
                continue; // drop proxy
            }
            let mut state = old_state.clone();
            state.id = new_id[old_id];

            // Patch ε edges
            state.epsilon_transitions_pred.iter_mut().for_each(|edge| {
                edge.0 = new_id[final_target(edge.0)];
            });

            // Patch symbol edges
            for set in state.transitions.values_mut() {
                *set = set.iter().map(|&dst| new_id[final_target(dst)]).collect();
            }

            new_states.push(state);
        }

        self.states = new_states;
    }
}
