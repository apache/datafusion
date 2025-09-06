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

//! MATCH_RECOGNIZE exclusions analysis and expression rewriting.
//!
//! This module encapsulates all logic related to ANSI SQL pattern exclusions
//! used in the `MATCH_RECOGNIZE` clause. It analyzes the compiled pattern to
//! discover excluded symbols, mints deterministic "shadow" symbols when a base
//! symbol appears both included and excluded, and rewrites the internal pattern
//! to reference these shadows. The result preserves user-visible semantics while
//! keeping the physical operator simple and uniform.
//!
//! In addition to pattern analysis and rewriting, the `ExclusionContext` offers
//! expression-level helpers used during planning: it normalizes `CLASSIFIER()`
//! values (mapping shadow names back to their base symbol) and expands
//! classifier bitset columns so predicates like `__mr_classifier_S` transparently
//! include all shadow bits for `S`. All types are internal to SQL planning.

use std::collections::{BTreeSet, HashMap};

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::col;
use datafusion_expr::expr::Case;
use datafusion_expr::match_recognize::columns;
use datafusion_expr::match_recognize::pattern::{Pattern, Symbol};
use datafusion_expr::Expr;

/// Results of analyzing a pattern for exclusions.
#[derive(Debug)]
pub(super) struct PatternAnalysis {
    pub(super) excluded_pairs: Vec<(String, String)>,
    pub(super) excluded_names_to_filter: Vec<String>,
    pub(super) rewritten_pattern: Pattern,
}

impl PatternAnalysis {
    pub(super) fn rewrite_pattern(&self) -> Pattern {
        self.rewritten_pattern.clone()
    }
}

/// Analyzes patterns for exclusions and creates shadow symbols.
pub(super) struct PatternAnalyzer;

impl PatternAnalyzer {
    /// Analyze a pattern and return exclusion analysis
    pub(super) fn analyze(pattern: &Pattern) -> PatternAnalysis {
        let mut state = PatternAnalysisState::new();

        // Collect all symbol names and included names
        state.collect_symbols(pattern);

        // Rewrite the pattern with shadow symbols
        let rewritten_pattern = state.rewrite_pattern(pattern);

        PatternAnalysis {
            excluded_pairs: state.excluded_pairs,
            excluded_names_to_filter: state.excluded_names_to_filter,
            rewritten_pattern,
        }
    }
}

/// State for pattern analysis.
#[derive(Debug)]
struct PatternAnalysisState {
    used_names: BTreeSet<String>,
    included_names: BTreeSet<String>,
    excluded_pairs: Vec<(String, String)>,
    excluded_names_to_filter: Vec<String>,
    base_to_shadow: HashMap<String, String>,
    next_idx: HashMap<String, usize>,
}

impl PatternAnalysisState {
    fn new() -> Self {
        Self {
            used_names: BTreeSet::new(),
            included_names: BTreeSet::new(),
            excluded_pairs: Vec::new(),
            excluded_names_to_filter: Vec::new(),
            base_to_shadow: HashMap::new(),
            next_idx: HashMap::new(),
        }
    }

    /// Collect all symbol names and identify included vs excluded symbols
    fn collect_symbols(&mut self, pattern: &Pattern) {
        let mut used_names = std::mem::take(&mut self.used_names);
        let mut included_names = std::mem::take(&mut self.included_names);

        visit_symbols(pattern, &mut |sym, excluded| {
            if let Symbol::Named(name) = sym {
                used_names.insert(name.clone());
                if !excluded {
                    included_names.insert(name.clone());
                }
            }
        });

        self.used_names = used_names;
        self.included_names = included_names;
    }

    /// Rewrite the pattern with shadow symbols
    fn rewrite_pattern(&mut self, pattern: &Pattern) -> Pattern {
        let included_names = self.included_names.clone();
        let mut excluded_names_to_filter = Vec::new();
        let mut excluded_pairs = Vec::new();
        let mut base_to_shadow: HashMap<String, String> = HashMap::new();

        // First pass: collect all excluded symbols and create shadows
        let mut temp_state = PatternAnalysisState {
            used_names: self.used_names.clone(),
            included_names: self.included_names.clone(),
            excluded_pairs: Vec::new(),
            excluded_names_to_filter: Vec::new(),
            base_to_shadow: HashMap::new(),
            next_idx: self.next_idx.clone(),
        };

        // Collect excluded symbols first
        let mut excluded_symbols = Vec::new();
        visit_symbols(pattern, &mut |sym, excluded| {
            if let Symbol::Named(base) = sym {
                if excluded {
                    excluded_symbols.push((base.clone(), included_names.contains(base)));
                }
            }
        });

        // Process excluded symbols and create shadows
        for (base, needs_shadow) in excluded_symbols {
            if needs_shadow {
                // Create shadow for excluded symbol that also appears included
                if !base_to_shadow.contains_key(&base) {
                    let shadow_name = temp_state.mint_shadow_name(&base);
                    excluded_pairs.push((base.clone(), shadow_name.clone()));
                    excluded_names_to_filter.push(shadow_name.clone());
                    base_to_shadow.insert(base.clone(), shadow_name);
                }
            } else {
                // Symbol appears only in excluded form
                excluded_names_to_filter.push(base);
            }
        }

        // Second pass: rewrite the pattern
        let rewritten =
            map_symbols_in_pattern(pattern, &mut |sym, excluded| match (sym, excluded) {
                (Symbol::Named(base), true) if included_names.contains(&base) => {
                    if let Some(shadow) = base_to_shadow.get(&base) {
                        Symbol::Named(shadow.clone())
                    } else {
                        Symbol::Named(base)
                    }
                }
                (sym, _) => sym,
            });

        // Update state with collected information
        self.excluded_pairs.extend(excluded_pairs);
        self.excluded_names_to_filter
            .extend(excluded_names_to_filter);
        self.base_to_shadow.extend(base_to_shadow);
        self.used_names = temp_state.used_names;
        self.next_idx = temp_state.next_idx;

        rewritten
    }

    /// Mint a unique shadow name for the given base symbol
    fn mint_shadow_name(&mut self, base: &str) -> String {
        let mut idx = *self.next_idx.get(base).unwrap_or(&1);

        loop {
            let candidate = if idx == 1 {
                format!("{base}_excl")
            } else {
                format!("{base}_excl{idx}")
            };

            if !self.used_names.contains(&candidate) {
                self.used_names.insert(candidate.clone());
                self.next_idx.insert(base.to_string(), idx + 1);
                return candidate;
            }

            idx += 1;
        }
    }
}

/// Visit all symbols in a pattern
fn visit_symbols<F>(pattern: &Pattern, callback: &mut F)
where
    F: FnMut(&Symbol, bool),
{
    match pattern {
        Pattern::Symbol(sym) => callback(sym, false),
        Pattern::Exclude(sym) => callback(sym, true),
        Pattern::Permute(list) => {
            for sym in list {
                callback(sym, false);
            }
        }
        Pattern::Concat(parts) | Pattern::Alternation(parts) => {
            for part in parts {
                visit_symbols(part, callback);
            }
        }
        Pattern::Group(inner) => visit_symbols(inner, callback),
        Pattern::Repetition(inner, _) => visit_symbols(inner, callback),
    }
}

/// Map symbols in a pattern using the provided function
fn map_symbols_in_pattern<F>(pattern: &Pattern, mapper: &mut F) -> Pattern
where
    F: FnMut(Symbol, bool) -> Symbol,
{
    match pattern {
        Pattern::Symbol(sym) => Pattern::Symbol(mapper(sym.clone(), false)),
        Pattern::Exclude(sym) => Pattern::Symbol(mapper(sym.clone(), true)),
        Pattern::Permute(list) => Pattern::Permute(list.clone()),
        Pattern::Concat(parts) => Pattern::Concat(
            parts
                .iter()
                .map(|part| map_symbols_in_pattern(part, mapper))
                .collect(),
        ),
        Pattern::Alternation(parts) => Pattern::Alternation(
            parts
                .iter()
                .map(|part| map_symbols_in_pattern(part, mapper))
                .collect(),
        ),
        Pattern::Group(inner) => {
            Pattern::Group(Box::new(map_symbols_in_pattern(inner, mapper)))
        }
        Pattern::Repetition(inner, quant) => Pattern::Repetition(
            Box::new(map_symbols_in_pattern(inner, mapper)),
            quant.clone(),
        ),
    }
}

/// Encapsulates exclusion mappings and provides expression rewriting functionality
#[derive(Clone)]
pub(super) struct ExclusionContext {
    shadow_to_base: HashMap<String, String>,
    base_to_shadows: HashMap<String, Vec<String>>,
    pairs: Vec<(String, String)>,
}

impl ExclusionContext {
    pub(super) fn new(pairs: &[(String, String)]) -> Self {
        let (shadow_to_base, base_to_shadows) = build_shadow_maps(pairs);
        Self {
            shadow_to_base,
            base_to_shadows,
            pairs: pairs.to_vec(),
        }
    }

    /// Rewrite an expression to handle shadow symbols
    pub(super) fn rewrite_expr(&self, expr: Expr) -> Result<Expr> {
        let expr = self.normalize_classifier_expr(expr)?;
        self.expand_classifier_bitsets(expr)
    }

    /// Normalize CLASSIFIER() columns by mapping shadow names back to base names
    pub(super) fn normalize_classifier_expr(&self, expr: Expr) -> Result<Expr> {
        expr.transform_up(|node| match node {
            Expr::Column(col) if columns::is_classifier_metadata_column(&col.name) => {
                self.build_classifier_case_expr(col)
            }
            _ => Ok(Transformed::no(node)),
        })
        .map(|t| t.data)
    }

    /// Build a CASE expression for CLASSIFIER() normalization
    fn build_classifier_case_expr(&self, col: Column) -> Result<Transformed<Expr>> {
        let mut whens: Vec<(Box<Expr>, Box<Expr>)> = Vec::new();

        for (shadow, base) in &self.shadow_to_base {
            let when = Expr::Column(col.clone())
                .eq(Expr::Literal(ScalarValue::Utf8(Some(shadow.clone())), None));
            let then = Expr::Literal(ScalarValue::Utf8(Some(base.clone())), None);
            whens.push((Box::new(when), Box::new(then)));
        }

        if whens.is_empty() {
            Ok(Transformed::no(Expr::Column(col)))
        } else {
            let else_expr = Some(Box::new(Expr::Column(col)));
            Ok(Transformed::yes(Expr::Case(Case::new(
                None, whens, else_expr,
            ))))
        }
    }

    /// Expand classifier bitset columns to include shadow flags
    pub(super) fn expand_classifier_bitsets(&self, expr: Expr) -> Result<Expr> {
        expr.transform_up(|node| match node {
            Expr::Column(col) => self.expand_bitset_column(col),
            _ => Ok(Transformed::no(node)),
        })
        .map(|t| t.data)
    }

    /// Expand a single classifier bitset column
    fn expand_bitset_column(&self, column: Column) -> Result<Transformed<Expr>> {
        let name = &column.name;

        if let Some(sym) = columns::classifier_bits_symbol(name) {
            let sym_lc = sym.to_ascii_lowercase();
            let base_lc = self.resolve_base_symbol(&sym_lc)?;

            let mut acc = col(columns::classifier_bits_col_name(&base_lc));

            if let Some(shadows) = self.base_to_shadows.get(&base_lc) {
                for shadow in shadows {
                    acc = acc.or(col(columns::classifier_bits_col_name(shadow)));
                }
            }

            Ok(Transformed::yes(acc))
        } else {
            Ok(Transformed::no(Expr::Column(column)))
        }
    }

    /// Resolve the base symbol name for a given symbol (handles both base and shadow symbols)
    fn resolve_base_symbol(&self, sym_lc: &str) -> Result<String> {
        if self.base_to_shadows.contains_key(sym_lc) {
            Ok(sym_lc.to_string())
        } else if let Some(base) = self.pairs.iter().find_map(|(base, shadow)| {
            if shadow.to_ascii_lowercase() == sym_lc {
                Some(base.to_ascii_lowercase())
            } else {
                None
            }
        }) {
            Ok(base)
        } else {
            // It's a base symbol without any shadows; return as-is
            Ok(sym_lc.to_string())
        }
    }
}

/// Build shadow mapping structures from exclusion pairs
fn build_shadow_maps(
    pairs: &[(String, String)],
) -> (HashMap<String, String>, HashMap<String, Vec<String>>) {
    let mut shadow_to_base: HashMap<String, String> = HashMap::new();
    let mut base_to_shadows: HashMap<String, Vec<String>> = HashMap::new();

    for (base, shadow) in pairs {
        shadow_to_base.insert(shadow.clone(), base.clone());
        base_to_shadows
            .entry(base.to_ascii_lowercase())
            .or_default()
            .push(shadow.to_ascii_lowercase());
    }

    (shadow_to_base, base_to_shadows)
}
