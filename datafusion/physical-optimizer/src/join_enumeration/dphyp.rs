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

//! Connectivity-driven join order enumeration, after Moerkotte and Neumann's
//! *Dynamic Programming Strikes Back* (SIGMOD 2008).
//!
//! Rather than walk all `3^n` splits of the relation subsets and keep the ones a
//! predicate spans, this walks the predicates: it grows connected subgraphs, and for each
//! grows the connected complements it can join. It reaches the same pairs, so it finds the
//! same plan, but a sparse graph has far fewer connected pairs than it has splits.
//!
//! Each pair is reached once, from the lowest-numbered relation it holds: a subgraph grown
//! from relation `i` may not reach back below `i`, and a complement is grown from its own
//! lowest neighbour of the subgraph. That is what the `excluded` sets carry.
//!
//! A predicate over three or more relations links all of them pairwise here, as it does
//! for [`DefaultJoinCostModel`]: the search has to reach every pair that model admits,
//! and [`DpTable::join`] prunes what it rejects. So a cost model can narrow the search
//! but not widen it past the graph's own predicates.
//!
//! [`DefaultJoinCostModel`]: super::DefaultJoinCostModel

use super::graph::{JoinGraph, RelSet, bit, iter_rels};
use super::{DpTable, JoinCostModel, Solution};

pub(super) fn solve(graph: &JoinGraph, model: &dyn JoinCostModel) -> Option<Solution> {
    let mut search = DpHyp {
        adjacency: adjacency(graph),
        table: DpTable::new(model, graph.relations.len()),
    };
    // Highest relation first, each forbidding the ones below it.
    for rel in (0..graph.relations.len()).rev() {
        search.emit_csg(bit(rel));
        search.grow_csg(bit(rel), upto(rel));
    }
    search.table.solution(graph.all())
}

struct DpHyp<'a> {
    /// Which relations each relation shares a predicate with.
    adjacency: Vec<RelSet>,
    table: DpTable<'a>,
}

impl DpHyp<'_> {
    /// The relations one predicate away from `set`, less the excluded ones.
    fn neighbours(&self, set: RelSet, excluded: RelSet) -> RelSet {
        iter_rels(set).fold(0, |mask, rel| mask | self.adjacency[rel]) & !excluded & !set
    }

    /// Whether a predicate spans the two sets.
    fn connected(&self, left: RelSet, right: RelSet) -> bool {
        iter_rels(left).any(|rel| self.adjacency[rel] & right != 0)
    }

    /// Joins the connected subgraph `csg` with each connected complement it has.
    fn emit_csg(&mut self, csg: RelSet) {
        let excluded = csg | upto(csg.trailing_zeros() as usize);
        let neighbours = self.neighbours(csg, excluded);
        for rel in iter_rels(neighbours) {
            // A complement grows from its own lowest neighbour of `csg`, so a complement
            // started here leaves the neighbours below `rel` to their own turn.
            self.table.join(csg, bit(rel));
            self.grow_cmp(csg, bit(rel), excluded | (neighbours & upto(rel)));
        }
    }

    /// Grows `csg` into the larger connected subgraphs reachable from it, emitting each.
    fn grow_csg(&mut self, csg: RelSet, excluded: RelSet) {
        let neighbours = self.neighbours(csg, excluded);
        // Every subgraph one step out is emitted before any of them grows further: a
        // subgraph's own splits must all be costed before it is joined to a complement.
        for grown in subsets(neighbours) {
            if self.table.planned(csg | grown) {
                self.emit_csg(csg | grown);
            }
        }
        for grown in subsets(neighbours) {
            self.grow_csg(csg | grown, excluded | neighbours);
        }
    }

    /// Grows the complement `cmp`, joining `csg` with each larger one still connected to
    /// it. A complement holds only relations above `csg`'s lowest, so its own plans are
    /// complete: they were costed in an earlier turn of the outer loop.
    fn grow_cmp(&mut self, csg: RelSet, cmp: RelSet, excluded: RelSet) {
        let neighbours = self.neighbours(cmp, excluded);
        for grown in subsets(neighbours) {
            let grown = cmp | grown;
            if self.table.planned(grown) && self.connected(csg, grown) {
                self.table.join(csg, grown);
            }
        }
        for grown in subsets(neighbours) {
            self.grow_cmp(csg, cmp | grown, excluded | neighbours);
        }
    }
}

/// Which relations each relation shares a predicate with. An applied relation links to
/// the relations its keys come from: those are the ones it can be applied to.
fn adjacency(graph: &JoinGraph) -> Vec<RelSet> {
    let mut adjacency = vec![0 as RelSet; graph.relations.len()];
    for edge in &graph.edges {
        adjacency[edge.left.column.rel] |= bit(edge.right.column.rel);
        adjacency[edge.right.column.rel] |= bit(edge.left.column.rel);
    }
    for filter in &graph.filters {
        for rel in iter_rels(filter.required) {
            adjacency[rel] |= filter.required & !bit(rel);
        }
    }
    for rel in 0..graph.relations.len() {
        if let Some(applied) = graph.applied(rel) {
            adjacency[rel] |= applied.required;
            for required in iter_rels(applied.required) {
                adjacency[required] |= bit(rel);
            }
        }
    }

    // A cut that separates whole components is a cross product, and the cost model prices
    // one by the pairs it forms, so a cross product below a join can be cheaper than the
    // same one above it. Link the components, and the search reaches those cuts as well;
    // what it then reaches that no predicate and no such cut allows, `combine` prunes.
    let components = components(&adjacency);
    if components.len() > 1 {
        let all = components
            .iter()
            .fold(0, |mask, component| mask | component);
        for component in &components {
            for rel in iter_rels(*component) {
                adjacency[rel] |= all & !component;
            }
        }
    }
    adjacency
}

/// The relations of each set of them no predicate reaches out of.
fn components(adjacency: &[RelSet]) -> Vec<RelSet> {
    let mut components: Vec<RelSet> = vec![];
    let mut seen: RelSet = 0;
    for rel in 0..adjacency.len() {
        if seen & bit(rel) != 0 {
            continue;
        }
        let mut component = bit(rel);
        loop {
            let grown =
                iter_rels(component).fold(component, |mask, rel| mask | adjacency[rel]);
            if grown == component {
                break;
            }
            component = grown;
        }
        seen |= component;
        components.push(component);
    }
    components
}

/// Every relation up to and including `rel`.
fn upto(rel: usize) -> RelSet {
    bit(rel) | (bit(rel) - 1)
}

/// The non-empty subsets of `mask`, each of them after all of its own subsets.
fn subsets(mask: RelSet) -> impl Iterator<Item = RelSet> {
    // Counting in the bits of `mask`: subtracting it borrows from the bits above the ones
    // the subtraction clears, which is the increment.
    std::iter::successors(Some(0 as RelSet), move |subset| {
        Some(subset.wrapping_sub(mask) & mask)
    })
    .skip(1)
    .take_while(|subset| *subset != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join_enumeration::graph::{
        Applied, AppliedKey, AppliedKind, ColRef, Edge, Filter, Key, Relation, Role,
    };
    use crate::join_enumeration::{DefaultJoinCostModel, DpTable};

    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_physical_expr::expressions::{Column, lit};
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::joins::utils::JoinFilter;

    /// Deterministic pseudo-randomness, so a failing seed can be rerun.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }

        fn below(&mut self, limit: usize) -> usize {
            (self.next() % limit as u64) as usize
        }

        fn chance(&mut self, percent: u64) -> bool {
            self.next() % 100 < percent
        }
    }

    const COLUMNS: usize = 2;

    fn key(rel: usize, col: usize) -> Key {
        Key {
            column: ColRef { rel, col },
            wrapper: None,
        }
    }

    fn relation(rows: f64, role: Role) -> Relation {
        let fields: Vec<Field> = (0..COLUMNS)
            .map(|col| Field::new(format!("c{col}"), DataType::Int32, true))
            .collect();
        Relation {
            plan: Arc::new(EmptyExec::new(Arc::new(Schema::new(fields)))),
            rows,
            width: Some(8.0),
            ndv: vec![(rows / 2.0).max(1.0); COLUMNS],
            role,
        }
    }

    /// A graph of `relations` relations: a random tree of equi-joins unless `disconnect`
    /// leaves one out of it, plus extra edges, a multi-relation filter, and relations
    /// applied to the rest by a semi, anti, outer or mark join.
    fn random_graph(rng: &mut Rng, relations: usize, disconnect: bool) -> JoinGraph {
        let mut graph = JoinGraph {
            relations: (0..relations)
                .map(|_| {
                    relation(10u64.pow(1 + rng.below(5) as u32) as f64, Role::Output)
                })
                .collect(),
            edges: vec![],
            filters: vec![],
            output: vec![],
            null_equality: None,
            original_nodes: vec![],
            applied: 0,
            kind: None,
        };

        let mut connected = vec![0];
        for rel in 1..relations {
            // Leaving one relation unconnected exercises cross products.
            if disconnect && rel == relations - 1 {
                continue;
            }
            let parent = connected[rng.below(connected.len())];
            graph.edges.push(Edge {
                left: key(parent, rng.below(COLUMNS)),
                right: key(rel, rng.below(COLUMNS)),
            });
            connected.push(rel);
        }
        for _ in 0..rng.below(relations) {
            let (left, right) = (rng.below(relations), rng.below(relations));
            if left != right {
                graph.edges.push(Edge {
                    left: key(left, rng.below(COLUMNS)),
                    right: key(right, rng.below(COLUMNS)),
                });
            }
        }

        // A relation joined by a semi, anti, outer or mark join is applied instead: it has
        // no edges, and its keys name the relations it can be applied to.
        for rel in (1..relations).rev() {
            let joined = graph
                .edges
                .iter()
                .any(|edge| [edge.left.column.rel, edge.right.column.rel].contains(&rel));
            if joined || !rng.chance(25) {
                continue;
            }
            let required: Vec<usize> = (0..rel).filter(|_| rng.chance(40)).collect();
            if required.is_empty() {
                continue;
            }
            graph.applied |= bit(rel);
            graph.relations[rel].role = Role::Applied(Applied {
                kind: match rng.below(4) {
                    0 => AppliedKind::Semi,
                    1 => AppliedKind::Anti,
                    2 => AppliedKind::Outer,
                    _ => AppliedKind::Mark,
                },
                keys: required
                    .iter()
                    .map(|other| AppliedKey {
                        other: key(*other, 0),
                        column: 0,
                        expr: Arc::new(Column::new("c0", 0)),
                    })
                    .collect(),
                required: required.iter().fold(0, |mask, rel| mask | bit(*rel)),
            });
        }

        if relations > 3 && rng.chance(40) {
            let required = (0..relations)
                .filter(|rel| graph.applied & bit(*rel) == 0)
                .filter(|_| rng.chance(50))
                .fold(0, |mask, rel| mask | bit(rel));
            if required.count_ones() > 1 {
                graph.filters.push(Filter {
                    filter: JoinFilter::new(lit(true), vec![], Arc::new(Schema::empty())),
                    columns: vec![],
                    required,
                });
            }
        }
        graph
    }

    /// Every split of every subset. The search visits far fewer pairs than this, so this
    /// is what says it does not miss any.
    fn full_search(graph: &JoinGraph, model: &dyn JoinCostModel) -> Option<Solution> {
        let full = graph.all();
        let mut table = DpTable::new(model, graph.relations.len());
        for mask in 1..=full {
            if mask.count_ones() < 2 {
                continue;
            }
            // Subsets holding the lowest set bit, so each pair of halves is seen once.
            let lowest = mask & mask.wrapping_neg();
            let mut left = mask;
            while left != 0 {
                left = (left - 1) & mask;
                if left & lowest != 0 {
                    table.join(left, mask ^ left);
                }
            }
        }
        table.solution(full)
    }

    #[test]
    fn finds_what_a_full_search_finds() {
        let config = ConfigOptions::new();
        for seed in 0..400 {
            let mut rng = Rng(seed);
            let relations = 3 + rng.below(6);
            let disconnect = rng.chance(25);
            let graph = random_graph(&mut rng, relations, disconnect);
            let model = DefaultJoinCostModel::new(&graph, &config);

            match (full_search(&graph, &model), solve(&graph, &model)) {
                (None, None) => {}
                (Some(full), Some(found)) => assert!(
                    (full.cost - found.cost).abs() <= full.cost * 1e-9,
                    "seed {seed}: full search {} vs dphyp {}, graph {graph:?}",
                    full.cost,
                    found.cost
                ),
                (full, found) => panic!(
                    "seed {seed}: full search {} and dphyp {} disagree on solvability, \
                     graph {graph:?}",
                    full.is_some(),
                    found.is_some()
                ),
            }
        }
    }
}
