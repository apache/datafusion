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

//! Applies aggregate decompositions whose components can be shared.

use datafusion_common::Result;
use datafusion_expr::Expr;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::utils::find_aggregate_exprs;

#[derive(Debug)]
struct AggregateDecomposition {
    /// Original aggregate expression, without its output alias.
    original: Expr,
    /// Full replacement expression, with the original output name preserved.
    expression: Expr,
    /// Distinct aggregate expressions needed to evaluate the replacement.
    components: Vec<Expr>,
}

/// Applies aggregate decompositions only when at least one resulting aggregate
/// expression occurs elsewhere in the same aggregate node.
///
/// Candidates connected by shared components are considered together. A group
/// is applied only when deduplication will not increase the number of distinct
/// aggregate expressions.
pub(super) fn rewrite_shared_aggregate_components(
    aggr_expr: &mut [Expr],
    info: &SimplifyContext,
) -> Result<bool> {
    let mut candidates = Vec::with_capacity(aggr_expr.len());
    let mut existing_components = vec![];

    // Phase 1: ask each aggregate for a decomposition candidate.
    for expr in aggr_expr.iter() {
        let unaliased_expr = expr.clone().unalias_nested().data;
        let candidate =
            if let Expr::AggregateFunction(aggregate_function) = unaliased_expr {
                if let Some(decomposed) = aggregate_function
                    .func
                    .decompose(&aggregate_function, info)?
                {
                    let components = find_aggregate_exprs([&decomposed]);
                    let original_name = expr.name_for_alias()?;
                    let expression = decomposed.alias_if_changed(original_name)?;
                    Some(AggregateDecomposition {
                        original: Expr::AggregateFunction(aggregate_function),
                        expression,
                        components,
                    })
                } else {
                    None
                }
            } else {
                None
            };

        if candidate.is_none() {
            existing_components.extend(find_aggregate_exprs([expr]));
        }
        candidates.push(candidate);
    }

    // Phase 2: decide which connected groups of candidates are profitable.
    let apply_candidate =
        select_profitable_decompositions(&candidates, &existing_components);

    // Phase 3: replace only the candidates selected by the profitability check.
    let mut rewritten = false;
    for ((expr, candidate), apply) in
        aggr_expr.iter_mut().zip(candidates).zip(apply_candidate)
    {
        if apply {
            let candidate =
                candidate.expect("an applied decomposition candidate must exist");
            debug_assert!(!candidate.components.is_empty());
            *expr = candidate.expression;
            rewritten = true;
        }
    }

    Ok(rewritten)
}

/// Selects which aggregate decomposition candidates should be applied.
///
/// It favors groups that reuse existing or duplicate components and rejects a
/// group when decomposition would increase the number of distinct aggregates.
/// For AVG decompositions into SUM and COUNT, this usually selects a beneficial
/// set of rewrites.
fn select_profitable_decompositions(
    candidates: &[Option<AggregateDecomposition>],
    existing_components: &[Expr],
) -> Vec<bool> {
    let mut apply_candidate = vec![false; candidates.len()];
    let mut visited = vec![false; candidates.len()];

    for start in 0..candidates.len() {
        if visited[start] || candidates[start].is_none() {
            continue;
        }

        visited[start] = true;
        // Find the connected group of candidates linked by shared components.
        let mut group = vec![start];
        let mut pending = vec![start];
        while let Some(current) = pending.pop() {
            let current_components = &candidates[current]
                .as_ref()
                .expect("a pending decomposition candidate must exist")
                .components;
            for (other, candidate) in candidates.iter().enumerate() {
                if visited[other] {
                    continue;
                }
                let Some(candidate) = candidate else {
                    continue;
                };
                if current_components
                    .iter()
                    .any(|component| candidate.components.contains(component))
                {
                    visited[other] = true;
                    group.push(other);
                    pending.push(other);
                }
            }
        }

        // Existing components are free. Count only distinct components that
        // this group would add after deduplication.
        let mut shared_component = false;
        let mut new_components = vec![];
        let mut original_aggregates = vec![];
        for &index in &group {
            let candidate = candidates[index]
                .as_ref()
                .expect("a grouped decomposition candidate must exist");
            if !original_aggregates.contains(&candidate.original) {
                original_aggregates.push(candidate.original.clone());
            }
            for component in &candidate.components {
                if existing_components.contains(component)
                    || new_components.contains(component)
                {
                    shared_component = true;
                } else {
                    new_components.push(component.clone());
                }
            }
        }

        // Replacing N distinct aggregates is worthwhile only if there is actual
        // sharing and the group adds at most N distinct aggregates.
        if shared_component && new_components.len() <= original_aggregates.len() {
            for index in group {
                apply_candidate[index] = true;
            }
        }
    }

    apply_candidate
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::col;

    fn candidate(name: &str, components: &[&str]) -> Option<AggregateDecomposition> {
        Some(AggregateDecomposition {
            original: col(name),
            expression: col(name),
            components: components.iter().map(|name| col(*name)).collect(),
        })
    }

    fn aliased_candidate(
        name: &str,
        original: &str,
        components: &[&str],
    ) -> Option<AggregateDecomposition> {
        Some(AggregateDecomposition {
            original: col(original),
            expression: col(name),
            components: components.iter().map(|name| col(*name)).collect(),
        })
    }

    #[test]
    fn applies_when_component_already_exists() {
        let candidates = vec![candidate("avg", &["sum", "count"])];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[col("sum")]),
            vec![true]
        );
    }

    #[test]
    fn rejects_without_shared_component() {
        let candidates = vec![candidate("avg", &["sum", "count"])];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[]),
            vec![false]
        );
    }

    #[test]
    fn rejects_when_candidate_sharing_increases_aggregate_count() {
        let candidates = vec![
            candidate("avg_a", &["sum_a", "count"]),
            candidate("avg_b", &["sum_b", "count"]),
        ];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[]),
            vec![false, false]
        );
    }

    #[test]
    fn applies_candidate_sharing_without_increasing_aggregate_count() {
        let candidates = vec![
            candidate("aggregate_a", &["sum", "count"]),
            candidate("aggregate_b", &["sum", "count"]),
        ];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[]),
            vec![true, true]
        );
    }

    #[test]
    fn selects_disconnected_groups_independently() {
        let candidates = vec![
            candidate("profitable", &["existing", "new"]),
            candidate("unprofitable", &["other_a", "other_b"]),
        ];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[col("existing")]),
            vec![true, false]
        );
    }

    #[test]
    fn rejects_duplicate_original_aggregates_that_expand() {
        let candidates = vec![
            aliased_candidate("avg_a", "avg", &["sum", "count"]),
            aliased_candidate("avg_b", "avg", &["sum", "count"]),
        ];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[]),
            vec![false, false]
        );
    }

    #[test]
    fn applies_transitively_connected_profitable_candidates() {
        let candidates = vec![
            candidate("a", &["existing", "b"]),
            candidate("b", &["b", "c"]),
            candidate("c", &["c", "d"]),
        ];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[col("existing")]),
            vec![true, true, true]
        );
    }

    #[test]
    fn rejects_empty_decomposition() {
        let candidates = vec![candidate("empty", &[])];
        assert_eq!(
            select_profitable_decompositions(&candidates, &[]),
            vec![false]
        );
    }
}
