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

use datafusion_common::{HashMap, HashSet, Result};
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, Pattern, RepetitionQuantifier, RowsPerMatch, Symbol,
};
use std::collections::VecDeque;
use std::sync::Arc;

use crate::match_recognize::nfa::{AnchorPredicate, NFAState, Sym, NFA};

/// Compiled, thread-safe representation of a MATCH_RECOGNIZE pattern.
#[derive(Debug)]
pub struct CompiledPattern {
    /// Original pattern AST for display/debugging purposes
    pub(crate) pattern: Pattern,
    pub(crate) id_to_symbol: Vec<Option<String>>,
    pub(crate) symbol_to_id: HashMap<String, usize>,
    pub(crate) numeric_transitions: Vec<Vec<Vec<usize>>>,
    pub(crate) nfa: NFA,
    pub(crate) after_match_skip: Option<AfterMatchSkip>,
    pub(crate) with_unmatched_rows: bool,
}

impl CompiledPattern {
    /// Iterator over (symbol_id, name) pairs for user-defined symbols.
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
        after_match_skip: Option<AfterMatchSkip>,
        rows_per_match: Option<RowsPerMatch>,
    ) -> Result<Self> {
        let mut nfa = NfaBuilder::build(&pattern)?;
        let with_unmatched_rows = matches!(
            rows_per_match,
            Some(RowsPerMatch::AllRows(Some(EmptyMatchesMode::WithUnmatched)))
        );
        if with_unmatched_rows
            && pattern_any(&pattern, |p| matches!(p, Pattern::Exclude(_)))
        {
            return Err(datafusion_common::DataFusionError::Plan(
                "WITH UNMATCHED ROWS is not supported when the pattern contains EXCLUDE clauses".into(),
            ));
        }

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
        let mut numeric_transitions = vec![vec![Vec::new(); num_symbols]; nfa.len()];
        for (sid, st) in nfa.iter().enumerate() {
            for (sym, dests) in &st.transitions {
                if let Some(&sym_id) = symbol_to_id.get(sym) {
                    numeric_transitions[sid][sym_id] = dests.iter().copied().collect();
                }
            }
        }

        // Keep only the dense transition matrix; drop bulky per-state HashMaps.
        if let Some(slice) = Arc::get_mut(&mut nfa.0) {
            for st in slice.iter_mut() {
                st.transitions = HashMap::new();
            }
        }

        Ok(Self {
            pattern,
            id_to_symbol,
            symbol_to_id,
            numeric_transitions,
            nfa,
            after_match_skip,
            with_unmatched_rows,
        })
    }
}

/// Returns `true` if *any* node in the pattern satisfies `pred`.
fn pattern_any<F>(pat: &Pattern, mut pred: F) -> bool
where
    F: FnMut(&Pattern) -> bool,
{
    // Depth-first walk using a small helper. `matches!` identifies leaf nodes.
    fn walk<'a, F>(node: &'a Pattern, pred: &mut F) -> bool
    where
        F: FnMut(&'a Pattern) -> bool,
    {
        if (pred)(node) {
            return true;
        }

        match node {
            Pattern::Concat(parts) | Pattern::Alternation(parts) => {
                parts.iter().any(|p| walk(p, pred))
            }
            Pattern::Repetition(inner, _) | Pattern::Group(inner) => walk(inner, pred),
            Pattern::Permute(_) | Pattern::Symbol(_) | Pattern::Exclude(_) => false,
        }
    }

    walk(pat, &mut pred)
}

/// Parameters extracted from a `RepetitionQuantifier` for NFA construction.
#[derive(Debug, Clone)]
struct QuantifierParams {
    /// Minimum number of repetitions required
    min: usize,
    /// Maximum number of repetitions allowed (None = unbounded)
    max: Option<usize>,
    /// Whether this quantifier supports looping (for +, *, {m,})
    looping: bool,
}

impl QuantifierParams {
    /// Extract parameters from a `RepetitionQuantifier`.
    fn from_quantifier(quant: &RepetitionQuantifier) -> Self {
        use datafusion_expr::match_recognize::RepetitionQuantifier::*;

        match quant {
            OneOrMore => Self {
                min: 1,
                max: None,
                looping: true,
            },
            ZeroOrMore => Self {
                min: 0,
                max: None,
                looping: true,
            },
            AtMostOne => Self {
                min: 0,
                max: Some(1),
                looping: false,
            },
            Exactly(n) => Self {
                min: *n as usize,
                max: Some(*n as usize),
                looping: false,
            },
            AtLeast(n) => Self {
                min: *n as usize,
                max: None,
                looping: true,
            },
            AtMost(n) => Self {
                min: 0,
                max: Some(*n as usize),
                looping: false,
            },
            Range(a, b) => Self {
                min: *a as usize,
                max: Some(*b as usize),
                looping: false,
            },
        }
    }
}

#[derive(Debug, Default)]
pub struct NfaBuilder {
    states: Vec<NFAState>,
}

impl NfaBuilder {
    /// Create a fresh, empty builder.
    fn new() -> Self {
        Self { states: Vec::new() }
    }

    /// Compile a `Pattern` into a fully-formed NFA.
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
    fn new_state(&mut self, is_accepting: bool, is_excluded: bool) -> usize {
        let id = self.states.len();
        self.states.push(NFAState {
            id,
            is_accepting,
            is_excluded,
            transitions: HashMap::new(),
            epsilon_transitions_pred: Vec::new(),
            epsilon_closure: Vec::new(),
        });
        id
    }

    /// Add an ε-transition from `from` → `to`, propagating the exclusion flag.
    fn add_epsilon(&mut self, from: usize, to: usize) {
        self.states[from].epsilon_transitions_pred.push((to, None));
        if self.states[from].is_excluded {
            self.states[to].is_excluded = true;
        }
    }

    /// Build `pat` into the builder and return its local `(start,end)` indices.
    fn build_pattern(&mut self, pat: &Pattern) -> Result<(usize, usize)> {
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
            Pattern::Concat(parts) => {
                if parts.is_empty() {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "Concatenation must contain at least one sub-pattern".into(),
                    ));
                }

                let mut iter = parts.iter();
                let (start, mut end) = self.build_pattern(iter.next().unwrap())?;
                for p in iter {
                    let (s, e) = self.build_pattern(p)?;
                    self.add_epsilon(end, s);
                    end = e;
                }
                Ok((start, end))
            }

            // Alternation – classic Thompson construction.
            Pattern::Alternation(alts) => {
                if alts.is_empty() {
                    return Err(datafusion_common::DataFusionError::Internal(
                        "Alternation must contain at least one branch".into(),
                    ));
                }

                let start = self.new_state(false, false);
                let mut ends = Vec::new();
                for sub in alts {
                    let (s, e) = self.build_pattern(sub)?;
                    self.add_epsilon(start, s);
                    ends.push(e);
                }

                let end = self.new_state(false, false);
                for e in ends {
                    self.add_epsilon(e, end);
                }
                Ok((start, end))
            }

            // Quantifiers / repetition.
            Pattern::Repetition(inner, quant) => self.compile_quantifier(inner, quant),

            // Explicit grouping – just delegate to inner pattern.
            Pattern::Group(inner) => self.build_pattern(inner),

            // PERMUTE – build once detached and clone into arena with offset.
            Pattern::Permute(symbols) => {
                let sub = Self::compile_permute_pattern(symbols)?;
                let offset = self.states.len();
                let mut cloned = Self::copy_states_with_offset(&sub, offset);

                // Collect accepting states and clear the flag – we will add a shared end.
                let mut accepting_ids = Vec::new();
                for st in cloned.iter_mut() {
                    if st.is_accepting {
                        accepting_ids.push(st.id);
                        st.is_accepting = false;
                    }
                }

                let sub_start = offset; // first cloned id
                self.states.extend(cloned);

                let end = self.new_state(false, false);
                for id in accepting_ids {
                    self.add_epsilon(id, end);
                }
                Ok((sub_start, end))
            }
        }
    }

    /// Consume the builder and return the completed state arena.
    fn finish(self) -> Arc<[NFAState]> {
        Arc::from(self.states.into_boxed_slice())
    }

    /// Compile a quantified sub-pattern.
    fn compile_quantifier(
        &mut self,
        inner: &Pattern,
        quant: &RepetitionQuantifier,
    ) -> Result<(usize, usize)> {
        let params = QuantifierParams::from_quantifier(quant);

        // Degenerate {0} pattern – single empty state.
        if params.min == 0 && params.max == Some(0) {
            let s = self.new_state(false, false);
            return Ok((s, s));
        }

        // 1. Optional dummy entry node when min == 0 so we always have a stable start.
        let entry = if params.min == 0 {
            Some(self.new_state(false, false))
        } else {
            None
        };

        let mut first_start: usize = entry.unwrap_or(usize::MAX);
        let mut last_end: usize = entry.unwrap_or(usize::MAX);
        let mut last_copy_start: usize = usize::MAX; // for looping

        // 2. Mandatory copies.
        for i in 0..params.min {
            let (s, e) = self.build_pattern(inner)?;
            if i == 0 {
                first_start = if entry.is_some() { entry.unwrap() } else { s };
            }
            if last_end != usize::MAX {
                self.add_epsilon(last_end, s);
            }
            last_end = e;
            last_copy_start = s;
        }

        // 3. Optional bounded copies up to max.
        if let Some(max_bound) = params.max {
            let mut remaining = max_bound.saturating_sub(params.min);
            while remaining > 0 {
                let (s, e) = self.build_pattern(inner)?;
                self.add_epsilon(last_end, s);
                last_end = e;
                remaining -= 1;
            }
            if params.min == 0 {
                self.add_epsilon(first_start, last_end);
            }
        }

        // 4. Unbounded loop for + / * / {m,}
        if params.max.is_none() && params.looping {
            if last_copy_start == usize::MAX {
                // ZeroOrMore without prior mandatory copy
                let (s, e) = self.build_pattern(inner)?;
                self.add_epsilon(first_start, s);
                last_copy_start = s;
                last_end = e;
            }
            self.add_epsilon(last_end, last_copy_start);
            if params.min == 0 {
                self.add_epsilon(first_start, last_end);
            }
        }

        Ok((first_start, last_end))
    }

    /// Shallow-clone a slice of states, applying `offset` to all IDs / links.
    fn copy_states_with_offset(states: &[NFAState], offset: usize) -> Vec<NFAState> {
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

    /// Compile a PERMUTE(S) standalone NFA where symbol order is irrelevant.
    fn compile_permute_pattern(symbols: &[Symbol]) -> Result<Vec<NFAState>> {
        if symbols.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "PERMUTE pattern must contain at least one symbol".to_string(),
            ));
        }

        // Collect unique symbols and their multiplicities.
        let mut unique_syms: Vec<String> = Vec::new();
        let mut counts_total: Vec<usize> = Vec::new();
        let mut index_of: HashMap<String, usize> = HashMap::new();

        for sym in symbols {
            let name = match sym {
                Symbol::Named(s) => s.clone(),
                Symbol::Start => "^".to_string(),
                Symbol::End => "$".to_string(),
            };
            if let Some(idx) = index_of.get(&name) {
                counts_total[*idx] += 1;
            } else {
                let idx = unique_syms.len();
                unique_syms.push(name.clone());
                counts_total.push(1);
                index_of.insert(name, idx);
            }
        }

        type Key = Vec<usize>;
        let mut key_to_id: HashMap<Key, usize> = HashMap::new();
        let mut states: Vec<NFAState> = Vec::new();
        let mut queue: VecDeque<Key> = VecDeque::new();

        let start_key: Key = vec![0; unique_syms.len()];
        key_to_id.insert(start_key.clone(), 0);
        states.push(NFAState {
            id: 0,
            is_accepting: false,
            is_excluded: false,
            transitions: HashMap::new(),
            epsilon_transitions_pred: Vec::new(),
            epsilon_closure: Vec::new(),
        });
        queue.push_back(start_key);

        while let Some(key) = queue.pop_front() {
            let state_id = *key_to_id.get(&key).unwrap();
            let is_accepting = key == counts_total;
            states[state_id].is_accepting = is_accepting;

            // For every symbol that can still be consumed, add a transition.
            for (idx, used) in key.iter().enumerate() {
                if *used < counts_total[idx] {
                    let mut next_key = key.clone();
                    next_key[idx] += 1;

                    let next_id = if let Some(id) = key_to_id.get(&next_key) {
                        *id
                    } else {
                        let id = states.len();
                        key_to_id.insert(next_key.clone(), id);
                        states.push(NFAState {
                            id,
                            is_accepting: false,
                            is_excluded: false,
                            transitions: HashMap::new(),
                            epsilon_transitions_pred: Vec::new(),
                            epsilon_closure: Vec::new(),
                        });
                        queue.push_back(next_key.clone());
                        id
                    };

                    let sym_name = unique_syms[idx].clone();
                    states[state_id]
                        .transitions
                        .entry(sym_name)
                        .or_insert_with(HashSet::new)
                        .insert(next_id);
                }
            }
        }

        Ok(states)
    }

    /// After all states are in place compute the ε-closure for each state.
    fn compute_epsilon_closures(&mut self) {
        let num_states = self.states.len();
        let mut visited = vec![0u32; num_states]; // Generation counter instead of bool

        for sid in 0..num_states {
            let mut stack = vec![sid];
            let mut reachable = Vec::new();

            while let Some(id) = stack.pop() {
                if visited[id] == sid as u32 {
                    continue;
                }
                visited[id] = sid as u32;
                reachable.push(id);

                for &(dst, _) in &self.states[id].epsilon_transitions_pred {
                    stack.push(dst);
                }
            }

            reachable.sort_unstable();
            self.states[sid].epsilon_closure = reachable;
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
            .map(|st| {
                if !st.is_accepting
                    && !st.is_excluded
                    && st.transitions.is_empty()
                    && st.epsilon_transitions_pred.len() == 1
                    && st.epsilon_transitions_pred[0].1.is_none()
                {
                    Some(st.epsilon_transitions_pred[0].0)
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
        for (old_id, old_st) in self.states.iter().enumerate() {
            if redirects[old_id].is_some() {
                continue; // drop proxy
            }
            let mut st = old_st.clone();
            st.id = new_id[old_id];

            // Patch ε edges
            st.epsilon_transitions_pred.iter_mut().for_each(|tp| {
                tp.0 = new_id[final_target(tp.0)];
            });

            // Patch symbol edges
            for set in st.transitions.values_mut() {
                *set = set.iter().map(|&dst| new_id[final_target(dst)]).collect();
            }

            new_states.push(st);
        }

        self.states = new_states;
    }
}
