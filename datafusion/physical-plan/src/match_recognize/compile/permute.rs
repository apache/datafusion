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

//! PERMUTE pattern compilation utilities.
//!
//! This module is logically independent from the rest of the classic
//! Thompson‐style NFA construction used for MATCH_RECOGNIZE patterns.  It
//! builds a dedicated NFA that accepts any ordering of the provided symbols
//! (respecting multiplicities) without incurring factorial blow-up by using a
//! state space indexed by a *multiset* key.

use super::builder::NfaBuilder;
use crate::match_recognize::nfa::NFAState;
use datafusion_common::{HashMap, Result};
use datafusion_expr::match_recognize::Symbol;
use std::collections::VecDeque;

/// Compile a PERMUTE(S) standalone NFA where symbol order is irrelevant.
pub fn compile_permute_pattern(symbols: &[Symbol]) -> Result<Vec<NFAState>> {
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
        transitions: HashMap::new(),
        numeric_transitions: Vec::new(),
        epsilon_transitions_pred: Vec::new(),
        epsilon_closure: Vec::new(),
        unconditional_closure: Vec::new(),
        alt_branch_idx: None,
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
                        transitions: HashMap::new(),
                        numeric_transitions: Vec::new(),
                        epsilon_transitions_pred: Vec::new(),
                        epsilon_closure: Vec::new(),
                        unconditional_closure: Vec::new(),
                        alt_branch_idx: None,
                    });
                    queue.push_back(next_key.clone());
                    id
                };

                let sym_name = unique_syms[idx].clone();
                states[state_id]
                    .transitions
                    .entry(sym_name)
                    .or_default()
                    .insert(next_id);
            }
        }
    }

    Ok(states)
}

/// Attach a PERMUTE pattern to the main `NfaBuilder` and return `(start,end)`
/// state ids inside the builder’s arena.
pub(crate) fn attach_permute(
    b: &mut NfaBuilder,
    symbols: &[Symbol],
) -> Result<(usize, usize)> {
    // 1. Build a standalone PERMUTE NFA (ids starting at 0)
    let sub = compile_permute_pattern(symbols)?;

    // 2. Shift state ids so they do not clash with the builder’s arena
    let offset = b.state_count();
    let mut cloned = NfaBuilder::copy_states_with_offset(&sub, offset);

    // 3. Collect accepting states and clear their flag – we will add a single
    //    shared sink to ensure the surrounding compiler always sees exactly
    //    one accepting state.
    let mut accepting_ids = Vec::new();
    for st in cloned.iter_mut() {
        if st.is_accepting {
            accepting_ids.push(st.id);
            st.is_accepting = false;
        }
    }

    // 4. Append the cloned states to the builder and remember the start id.
    let sub_start = offset; // first cloned id after shifting
    b.extend_states(cloned);

    // 5. Create a fresh accepting sink and wire ε-edges from every permute
    //    accepting state.
    let end = b.new_state(false);
    for id in accepting_ids {
        b.add_epsilon(id, end);
    }

    Ok((sub_start, end))
}
