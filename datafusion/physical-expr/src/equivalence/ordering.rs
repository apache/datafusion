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

use arrow_schema::SortOptions;
use std::hash::Hash;
use std::sync::Arc;

use crate::equivalence::add_offset_to_expr;
use crate::{LexOrdering, LexOrderingRef, PhysicalExpr, PhysicalSortExpr};

/// An `OrderingEquivalenceClass` object keeps track of different alternative
/// orderings than can describe a schema. For example, consider the following table:
///
/// ```text
/// |a|b|c|d|
/// |1|4|3|1|
/// |2|3|3|2|
/// |3|1|2|2|
/// |3|2|1|3|
/// ```
///
/// Here, both `vec![a ASC, b ASC]` and `vec![c DESC, d ASC]` describe the table
/// ordering. In this case, we say that these orderings are equivalent.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OrderingEquivalenceClass {
    orderings: Vec<LexOrdering>,
}

impl OrderingEquivalenceClass {
    /// Creates new empty ordering equivalence class.
    fn empty() -> Self {
        Self { orderings: vec![] }
    }

    /// Clears (empties) this ordering equivalence class.
    pub fn clear(&mut self) {
        self.orderings.clear();
    }

    /// Creates new ordering equivalence class from the given orderings.
    pub fn new(orderings: Vec<LexOrdering>) -> Self {
        let mut result = Self { orderings };
        result.remove_redundant_entries();
        result
    }

    /// Checks whether `ordering` is a member of this equivalence class.
    pub fn contains(&self, ordering: &LexOrdering) -> bool {
        self.orderings.contains(ordering)
    }

    /// Adds `ordering` to this equivalence class.
    #[allow(dead_code)]
    fn push(&mut self, ordering: LexOrdering) {
        self.orderings.push(ordering);
        // Make sure that there are no redundant orderings:
        self.remove_redundant_entries();
    }

    /// Checks whether this ordering equivalence class is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns an iterator over the equivalent orderings in this class.
    pub fn iter(&self) -> impl Iterator<Item = &LexOrdering> {
        self.orderings.iter()
    }

    /// Returns how many equivalent orderings there are in this class.
    pub fn len(&self) -> usize {
        self.orderings.len()
    }

    /// Extend this ordering equivalence class with the `other` class.
    pub fn extend(&mut self, other: Self) {
        self.orderings.extend(other.orderings);
        // Make sure that there are no redundant orderings:
        self.remove_redundant_entries();
    }

    /// Adds new orderings into this ordering equivalence class.
    pub fn add_new_orderings(
        &mut self,
        orderings: impl IntoIterator<Item = LexOrdering>,
    ) {
        self.orderings.extend(orderings);
        // Make sure that there are no redundant orderings:
        self.remove_redundant_entries();
    }

    /// Removes redundant orderings from this equivalence class. For instance,
    /// if we already have the ordering `[a ASC, b ASC, c DESC]`, then there is
    /// no need to keep ordering `[a ASC, b ASC]` in the state.
    fn remove_redundant_entries(&mut self) {
        let mut work = true;
        while work {
            work = false;
            let mut idx = 0;
            while idx < self.orderings.len() {
                let mut ordering_idx = idx + 1;
                let mut removal = self.orderings[idx].is_empty();
                while ordering_idx < self.orderings.len() {
                    work |= resolve_overlap(&mut self.orderings, idx, ordering_idx);
                    if self.orderings[idx].is_empty() {
                        removal = true;
                        break;
                    }
                    work |= resolve_overlap(&mut self.orderings, ordering_idx, idx);
                    if self.orderings[ordering_idx].is_empty() {
                        self.orderings.swap_remove(ordering_idx);
                    } else {
                        ordering_idx += 1;
                    }
                }
                if removal {
                    self.orderings.swap_remove(idx);
                } else {
                    idx += 1;
                }
            }
        }
    }

    /// Returns the concatenation of all the orderings. This enables merge
    /// operations to preserve all equivalent orderings simultaneously.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        let output_ordering = self.orderings.iter().flatten().cloned().collect();
        let output_ordering = collapse_lex_ordering(output_ordering);
        (!output_ordering.is_empty()).then_some(output_ordering)
    }

    // Append orderings in `other` to all existing orderings in this equivalence
    // class.
    pub fn join_suffix(mut self, other: &Self) -> Self {
        let n_ordering = self.orderings.len();
        // Replicate entries before cross product
        let n_cross = std::cmp::max(n_ordering, other.len() * n_ordering);
        self.orderings = self
            .orderings
            .iter()
            .cloned()
            .cycle()
            .take(n_cross)
            .collect();
        // Suffix orderings of other to the current orderings.
        for (outer_idx, ordering) in other.iter().enumerate() {
            for idx in 0..n_ordering {
                // Calculate cross product index
                let idx = outer_idx * n_ordering + idx;
                self.orderings[idx].extend(ordering.iter().cloned());
            }
        }
        self
    }

    /// Adds `offset` value to the index of each expression inside this
    /// ordering equivalence class.
    pub fn add_offset(&mut self, offset: usize) {
        for ordering in self.orderings.iter_mut() {
            for sort_expr in ordering {
                sort_expr.expr = add_offset_to_expr(sort_expr.expr.clone(), offset);
            }
        }
    }

    /// Gets sort options associated with this expression if it is a leading
    /// ordering expression. Otherwise, returns `None`.
    fn get_options(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<SortOptions> {
        for ordering in self.iter() {
            let leading_ordering = &ordering[0];
            if leading_ordering.expr.eq(expr) {
                return Some(leading_ordering.options);
            }
        }
        None
    }
}

/// This function constructs a duplicate-free `LexOrdering` by filtering out
/// duplicate entries that have same physical expression inside. For example,
/// `vec![a ASC, a DESC]` collapses to `vec![a ASC]`.
pub fn collapse_lex_ordering(input: LexOrdering) -> LexOrdering {
    let mut output = Vec::<PhysicalSortExpr>::new();
    for item in input {
        if !output.iter().any(|req| req.expr.eq(&item.expr)) {
            output.push(item);
        }
    }
    output
}

/// Trims `orderings[idx]` if some suffix of it overlaps with a prefix of
/// `orderings[pre_idx]`. Returns `true` if there is any overlap, `false` otherwise.
fn resolve_overlap(orderings: &mut [LexOrdering], idx: usize, pre_idx: usize) -> bool {
    let length = orderings[idx].len();
    let other_length = orderings[pre_idx].len();
    for overlap in 1..=length.min(other_length) {
        if orderings[idx][length - overlap..] == orderings[pre_idx][..overlap] {
            orderings[idx].truncate(length - overlap);
            return true;
        }
    }
    false
}
