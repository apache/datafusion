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
    pub fn empty() -> Self {
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

    /// Returns the underlying vector of orderings.
    pub fn into_vec(self) -> Vec<LexOrdering> {
        self.orderings
    }

    /// Adds `ordering` to this equivalence class.
    #[cfg(test)]
    pub fn push(&mut self, ordering: LexOrdering) {
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

    /// Removes redundant orderings from this equivalence class.
    /// For instance, If we already have the ordering [a ASC, b ASC, c DESC],
    /// then there is no need to keep ordering [a ASC, b ASC] in the state.
    fn remove_redundant_entries(&mut self) {
        let mut idx = 0;
        while idx < self.orderings.len() {
            let mut removal = false;
            for (ordering_idx, ordering) in self.orderings[0..idx].iter().enumerate() {
                if let Some(right_finer) = finer_side(ordering, &self.orderings[idx]) {
                    if right_finer {
                        self.orderings.swap(ordering_idx, idx);
                    }
                    removal = true;
                    break;
                }
            }
            if removal {
                self.orderings.swap_remove(idx);
            } else {
                idx += 1;
            }
        }
    }

    /// Returns the concatenation of all the orderings. This enables merge
    /// operations to preserve all equivalent orderings simultaneously.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        let output_ordering =
            self.orderings.iter().flatten().cloned().collect::<Vec<_>>();
        let output_ordering = collapse_lex_ordering(output_ordering);
        (!output_ordering.is_empty()).then_some(output_ordering)
    }

    // Append orderings in `other` to all existing orderings in this equivalence
    // class.
    pub fn join_suffix(mut self, other: &Self) -> Self {
        for ordering in other.iter() {
            for idx in 0..self.orderings.len() {
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
    pub fn get_options(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<SortOptions> {
        for ordering in self.iter() {
            let leading_ordering = &ordering[0];
            if leading_ordering.expr.eq(expr) {
                return Some(leading_ordering.options);
            }
        }
        None
    }
}

/// Returns `true` if the ordering `rhs` is strictly finer than the ordering `rhs`,
/// `false` if the ordering `lhs` is at least as fine as the ordering `lhs`, and
/// `None` otherwise (i.e. when given orderings are incomparable).
fn finer_side(lhs: LexOrderingRef, rhs: LexOrderingRef) -> Option<bool> {
    let all_equal = lhs.iter().zip(rhs.iter()).all(|(lhs, rhs)| lhs.eq(rhs));
    all_equal.then_some(lhs.len() < rhs.len())
}

/// This function constructs a duplicate-free `LexOrdering` by filtering out
/// duplicate entries that have same physical expression inside. For example,
/// `vec![a ASC, a DESC]` collapses to `vec![a ASC]`.
fn collapse_lex_ordering(input: LexOrdering) -> LexOrdering {
    let mut output = Vec::<PhysicalSortExpr>::new();
    for item in input {
        if !output.iter().any(|req| req.expr.eq(&item.expr)) {
            output.push(item);
        }
    }
    output
}
