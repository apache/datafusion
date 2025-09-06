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

//! Pure, allocation-free NFA types used by both the compiler and the matcher.

use datafusion_common::{HashMap, HashSet};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Type alias for logical row index (includes virtual anchor rows).
pub type RowIdx = usize;

// Strongly-typed symbol identifier used throughout the matcher.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Sym {
    /// Zero-length placeholder inserted for empty matches
    Empty,
    /// User-defined pattern symbol (0-based ordinal)
    User(u16),
}

impl Sym {
    // convert symbolic enum to dense numeric index (0-based)
    pub const fn to_index(self) -> usize {
        match self {
            Sym::Empty => 0,
            Sym::User(n) => 1 + n as usize,
        }
    }

    pub const fn from_index(idx: usize) -> Self {
        match idx {
            0 => Sym::Empty,
            n => Sym::User((n - 1) as u16),
        }
    }

    pub fn is_special(self) -> bool {
        matches!(self, Sym::Empty)
    }
}

impl From<usize> for Sym {
    fn from(value: usize) -> Self {
        Sym::from_index(value)
    }
}

impl From<Sym> for usize {
    fn from(sym: Sym) -> usize {
        sym.to_index()
    }
}

// Symbol index map (see `Sym::to_index()`):
//   0=Empty, 1+=user-defined symbols (in declaration order)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PathStep {
    pub row: RowIdx,
    pub sym: Sym,
}

/// Linked-list node sharing common prefixes (cheap push, no per-step clone).
#[derive(Debug)]
pub struct PathNode {
    pub step: PathStep,
    pub prev: Option<Arc<PathNode>>, // older steps (None for list tail)
}

/// Convert an `Arc`-linked path into a forward `Vec<PathStep>` (allocates once).
pub fn collect_path(node: &Arc<PathNode>) -> Vec<PathStep> {
    // Collect nodes in reverse order and flip once to obtain forward ordering.
    let mut steps: Vec<PathStep> = std::iter::successors(Some(node), |n| n.prev.as_ref())
        .map(|n| n.step)
        .collect();
    steps.reverse();
    steps
}

/// Cheap `(len, classified)` score used to pick the greedy/left-most match.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
/// Greedy / left-most scoring used to choose between competing paths.
///
/// Ordering rules:
///   1. Longer overall path wins (more pattern consumed).
///   2. Ties are broken by the number of *classified* (real) symbols.
///   3. Remaining ties are resolved outside this struct by comparing
///      `(state_id, row)` so that the left-most match wins.
pub struct PathScore {
    /// Total number of NFA steps (anchors, Empty sentinel, user symbols…).
    pub total_steps: u32,
    /// Count of user-defined symbols (i.e. steps that are not special).
    pub classified_steps: u32,
}

impl PathScore {
    /// Incrementally update the score after appending `sym` to the path.
    #[inline]
    pub fn extend(self, sym: Sym) -> Self {
        Self {
            total_steps: self.total_steps + 1,
            classified_steps: self.classified_steps + (!sym.is_special()) as u32,
        }
    }
}

/// Represents a state in the pattern matching NFA
#[derive(Debug, Clone)]
pub struct NFAState {
    /// State identifier
    pub id: usize,
    /// Whether this is an accepting state (complete match)
    pub is_accepting: bool,
    /// Transitions from this state: symbol -> set of next state IDs
    pub transitions: HashMap<String, HashSet<usize>>,
    /// Dense transition matrix indexed by numeric symbol id (0 = Empty).
    /// Populated by the compiler once all user-defined symbols are known.
    pub numeric_transitions: Vec<Vec<usize>>,
    /// Epsilon transitions (do not consume input) with an optional position predicate.
    /// `None` means unconditional; `Some(pred)` restricts the transition to positions
    /// where the predicate is satisfied.
    pub epsilon_transitions_pred: Vec<(usize, Option<AnchorPredicate>)>,
    /// Pre-computed ε-closure for this state (includes the state itself).
    /// Stored densely as a sorted vector for cache-friendly iteration and deterministic order
    pub epsilon_closure: Vec<usize>,
    /// Pre-computed ε-closure using only unconditional (None) transitions.
    pub unconditional_closure: Vec<usize>,

    /// Index of the alternation branch this state belongs to (if any).
    ///
    /// Assigned by the pattern compiler for the **terminal** state of each
    /// branch in a `Pattern::Alternation` construct so that the runtime can
    /// apply left‐most precedence when multiple branches might produce a
    /// complete match for the same start row.
    pub alt_branch_idx: Option<u32>,
}

/// Thin wrapper around an immutable slice of NFA states.
#[derive(Clone, Debug)]
pub struct NFA(pub Arc<[NFAState]>);

// Allow direct access to the underlying slice (indexing, iteration…)
impl Deref for NFA {
    type Target = [NFAState];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for NFA {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut states: Vec<&NFAState> = self.iter().collect();
        states.sort_by_key(|s| s.id);

        for (idx, st) in states.iter().enumerate() {
            if idx > 0 {
                // Each state on a new line
                writeln!(f)?;
            }
            write!(f, "{st}")?;
        }
        Ok(())
    }
}

impl fmt::Display for NFAState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Build formatted segments for epsilon transitions
        let mut parts: Vec<String> = Vec::new();
        if !self.epsilon_transitions_pred.is_empty() {
            let mut segs: Vec<String> = self
                .epsilon_transitions_pred
                .iter()
                .map(|(dst, pred)| {
                    let pred_str = match pred {
                        None => "".to_string(),
                        Some(AnchorPredicate::StartOfInput) => "[^]".to_string(),
                        Some(AnchorPredicate::EndOfInput) => "[$]".to_string(),
                    };
                    format!("ε{pred_str}->{dst}")
                })
                .collect();
            segs.sort();
            parts.extend(segs);
        }

        // Build formatted segments for symbol transitions
        let mut keys: Vec<_> = self.transitions.keys().collect();
        keys.sort();
        for sym in keys {
            let mut dests: Vec<_> = self.transitions[sym].iter().copied().collect();
            dests.sort_unstable();
            parts.push(format!(
                "{}->{}",
                sym,
                dests
                    .iter()
                    .map(|d| d.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }

        // Join parts with " | " if any exist
        let transitions = if parts.is_empty() {
            String::new()
        } else {
            format!(" {}", parts.join(" | "))
        };

        if self.is_accepting {
            write!(f, "{}*:{}", self.id, transitions)
        } else {
            write!(f, "{}:{}", self.id, transitions)
        }
    }
}

/// Active NFA state for tracking path through the NFA
#[derive(Debug, Clone)]
pub struct ActiveNFAState {
    pub state_id: usize,
    pub path: Option<Arc<PathNode>>, // shared persistent path (None for empty)
    pub score: PathScore,            // cached summary for quick comparisons
}

/// Zero-width anchor predicates attached to ε-transitions
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AnchorPredicate {
    StartOfInput,
    EndOfInput,
}

/// Best-match envelope used only on the *cold* path when a complete match is
/// found.  It carries the materialised path (needed later for output / SKIP
/// logic) plus cheap metadata that allows us to compare two matches via
/// `prefer` without walking the vector again.
#[derive(Clone, Debug)]
pub struct BestMatch {
    pub path: Vec<PathStep>,
    pub score: PathScore,
    pub state_id: usize,
    pub row: RowIdx,
}

#[cfg(test)]
mod tests {
    use crate::match_recognize::compile::NfaBuilder;
    use datafusion_sql::match_recognize::sql_pattern_to_df;
    use insta::assert_snapshot;
    use sqlparser::ast::{SetExpr, Statement, TableFactor};
    use sqlparser::{dialect::GenericDialect, parser::Parser};

    /// Parse a SQL MATCH_RECOGNIZE PATTERN fragment and build an NFA for snapshot testing.
    /// Example: `build_nfa("A B+")`
    fn build_nfa(pat_sql: &str) -> super::NFA {
        // Wrap fragment in minimal valid query for sqlparser
        // Note: PATTERN usually requires parentheses around the pattern and DEFINE clause is required
        let sql = format!(
            "SELECT * FROM t MATCH_RECOGNIZE (PATTERN ({pat_sql}) DEFINE A AS TRUE)"
        );

        let mut ast =
            Parser::parse_sql(&GenericDialect {}, &sql).expect("SQL parse error");
        let stmt = ast.pop().expect("no statement parsed");

        let pattern_ast = match stmt {
            Statement::Query(q) => match *q.body {
                SetExpr::Select(select) => {
                    let twj = select.from.first().expect("FROM missing in wrapper query");
                    match &twj.relation {
                        TableFactor::MatchRecognize { pattern, .. } => pattern.clone(),
                        _ => unreachable!("wrapper guarantees MATCH_RECOGNIZE"),
                    }
                }
                _ => unreachable!("wrapper query should be simple SELECT"),
            },
            _ => unreachable!("expected Query"),
        };

        // Tests build NFAs under ONE ROW PER MATCH mode
        let pattern = sql_pattern_to_df(&pattern_ast);
        NfaBuilder::build(&pattern).expect("compile failed")
    }

    #[test]
    fn snapshot_cases() {
        // Simple symbol / anchor
        {
            let nfa = build_nfa("A");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2*:
            ");
        }
        {
            let nfa = build_nfa("^");
            assert_snapshot!(nfa, @r"
            0: ε[^]->1
            1: ε->2
            2*:
            ");
        }
        // End-anchor only patterns
        {
            let nfa = build_nfa("$");
            assert_snapshot!(nfa, @r"
            0: ε[$]->1
            1: ε->2
            2*:
            ");
        }
        {
            let nfa = build_nfa("^ $");
            assert_snapshot!(nfa, @r"
            0: ε[^]->1
            1: ε->2
            2: ε[$]->3
            3: ε->4
            4*:
            ");
        }
        {
            let nfa = build_nfa("^ A $");
            assert_snapshot!(nfa, @r"
            0: ε[^]->1
            1: ε->2
            2: A->3
            3: ε->4
            4: ε[$]->5
            5: ε->6
            6*:
            ");
        }
        // Repetition / concatenation
        {
            let nfa = build_nfa("A A A");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: A->3
            3: ε->4
            4: A->5
            5: ε->6
            6*:
            ");
        }
        {
            let nfa = build_nfa("A?");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->2
            1: A->2
            2: ε->3
            3*:
            ");
        }
        {
            let nfa = build_nfa("A{,3}");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->6
            1: A->2
            2: ε->3
            3: A->4
            4: ε->5
            5: A->6
            6: ε->7
            7*:
            ");
        }
        {
            let nfa = build_nfa("A B");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: B->3
            3: ε->4
            4*:
            ");
        }
        {
            let nfa = build_nfa("A B C");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: B->3
            3: ε->4
            4: C->5
            5: ε->6
            6*:
            ");
        }
        // Anchors in concat
        {
            let nfa = build_nfa("^ A");
            assert_snapshot!(nfa, @r"
            0: ε[^]->1
            1: ε->2
            2: A->3
            3: ε->4
            4*:
            ");
        }
        {
            let nfa = build_nfa("A $");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: ε[$]->3
            3: ε->4
            4*:
            ");
        }
        // Alternations
        {
            let nfa = build_nfa("A | B");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->3
            1: A->2
            2: ε->5
            3: B->4
            4: ε->5
            5: ε->6
            6*:
            ");
        }
        {
            let nfa = build_nfa("^ | A");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->3
            1: ε[^]->2
            2: ε->5
            3: A->4
            4: ε->5
            5: ε->6
            6*:
            ");
        }
        {
            let nfa = build_nfa("A | B | C");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->3 | ε->5
            1: A->2
            2: ε->7
            3: B->4
            4: ε->7
            5: C->6
            6: ε->7
            7: ε->8
            8*:
            ");
        }
        {
            let nfa = build_nfa("A{2} | B");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->5
            1: A->2
            2: ε->3
            3: A->4
            4: ε->7
            5: B->6
            6: ε->7
            7: ε->8
            8*:
            ");
        }
        // Group patterns
        {
            let nfa = build_nfa("(A)");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2*:
            ");
        }
        {
            let nfa = build_nfa("(A B)");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: B->3
            3: ε->4
            4*:
            ");
        }
        {
            let nfa = build_nfa("(A B | C+)");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->5
            1: A->2
            2: ε->3
            3: B->4
            4: ε->7
            5: C->6
            6: ε->5 | ε->7
            7: ε->8
            8*:
            ");
        }
        // PERMUTE patterns
        {
            let nfa = build_nfa("PERMUTE(A, B)");
            assert_snapshot!(nfa, @r"
            0: A->1 | B->2
            1: B->3
            2: A->3
            3: ε->4
            4: ε->5
            5*:
            ");
        }
        {
            let nfa = build_nfa("PERMUTE(A)");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: ε->3
            3*:
            ");
        }
        {
            let nfa = build_nfa("PERMUTE(A, B, C)");
            assert_snapshot!(nfa, @r"
            0: A->1 | B->2 | C->3
            1: B->4 | C->5
            2: A->4 | C->6
            3: A->5 | B->6
            4: C->7
            5: B->7
            6: A->7
            7: ε->8
            8: ε->9
            9*:
            ");
        }
        {
            let nfa = build_nfa("PERMUTE(A, B, C, D, E)");
            assert_snapshot!(nfa, @r"
            0: A->1 | B->2 | C->3 | D->4 | E->5
            1: B->6 | C->7 | D->8 | E->9
            2: A->6 | C->10 | D->11 | E->12
            3: A->7 | B->10 | D->13 | E->14
            4: A->8 | B->11 | C->13 | E->15
            5: A->9 | B->12 | C->14 | D->15
            6: C->16 | D->17 | E->18
            7: B->16 | D->19 | E->20
            8: B->17 | C->19 | E->21
            9: B->18 | C->20 | D->21
            10: A->16 | D->22 | E->23
            11: A->17 | C->22 | E->24
            12: A->18 | C->23 | D->24
            13: A->19 | B->22 | E->25
            14: A->20 | B->23 | D->25
            15: A->21 | B->24 | C->25
            16: D->26 | E->27
            17: C->26 | E->28
            18: C->27 | D->28
            19: B->26 | E->29
            20: B->27 | D->29
            21: B->28 | C->29
            22: A->26 | E->30
            23: A->27 | D->30
            24: A->28 | C->30
            25: A->29 | B->30
            26: E->31
            27: D->31
            28: C->31
            29: B->31
            30: A->31
            31: ε->32
            32: ε->33
            33*:
            ");
        }
        {
            let nfa = build_nfa("PERMUTE(A, A, C)");
            assert_snapshot!(nfa, @r"
            0: A->1 | C->2
            1: A->3 | C->4
            2: A->4
            3: C->5
            4: A->5
            5: ε->6
            6: ε->7
            7*:
            ");
        }
        // Exclusion patterns
        {
            let nfa = build_nfa("({- A -})");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2*:
            ");
        }
        {
            let nfa = build_nfa("A ({- B -})");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: B->3
            3: ε->4
            4*:
            ");
        }
        {
            let nfa = build_nfa("({- A -}) ({- A -}) A");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: A->3
            3: ε->4
            4: A->5
            5: ε->6
            6*:
            ");
        }
        {
            let nfa = build_nfa("A ({- A -}) ({- A -})");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: A->3
            3: ε->4
            4: A->5
            5: ε->6
            6*:
            ");
        }
        {
            let nfa = build_nfa("A ({- A -}) A");
            assert_snapshot!(nfa, @r"
            0: A->1
            1: ε->2
            2: A->3
            3: ε->4
            4: A->5
            5: ε->6
            6*:
            ");
        }
        {
            // Complex Snowflake example pattern
            let nfa = build_nfa("^ S1 S2* ( ({- S3 -}) S4 )+ | PERMUTE(S1, S2){1,2} $");
            assert_snapshot!(nfa, @r"
            0: ε->1 | ε->12
            1: ε[^]->2
            2: ε->3
            3: S1->4
            4: ε->5
            5: ε->6 | ε->7
            6: S2->7
            7: ε->6 | ε->8
            8: S3->9
            9: ε->10
            10: S4->11
            11: ε->24 | ε->8
            12: S1->13 | S2->14
            13: S2->15
            14: S1->15
            15: ε->16
            16: ε->17
            17: S1->18 | S2->19
            18: S2->20
            19: S1->20
            20: ε->21
            21: ε->22
            22: ε[$]->23
            23: ε->24
            24: ε->25
            25*:
            ");
        }
    }
}
