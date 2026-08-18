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

//! Turning a tree of [`HashJoinExec`]s into a flat join graph, and recognizing
//! the double star shape in it.
//!
//! # Flattening
//!
//! A join's `on` keys address columns as positions in that join's own input
//! schemas, so nothing at the top of a join tree says which base relation a
//! key belongs to. Recovering that relies on one identity: for an inner join
//! with no projection, the output schema is the left input's fields followed
//! by the right input's (see `build_join_schema`). Collecting leaves left to
//! right therefore produces a concatenated schema identical to the subtree's
//! own output schema, giving every relation a contiguous range of one global
//! coordinate space:
//!
//! ```text
//!   global index:  0    1    2  |  3    4    5  |  6    7  |  8    9
//!                  '-- orders --'  '- customer -'  'nation'  'lineitem'
//! ```
//!
//! Each subtree therefore reports an **output map**: for every column it
//! emits, the global coordinate of the relation column behind it. A join
//! concatenates its children's maps; a key at index `i` is translated through
//! the corresponding child's map.
//!
//! That identity is exactly what [`reorderable_join`] protects: a semi join
//! emits only its left fields, and a projection attached to a join reorders or
//! drops columns. Either would silently break the arithmetic, so both are
//! refused.
//!
//! # Seeing through pruning projections
//!
//! The physical planner inserts a standalone `ProjectionExec` between joins to
//! drop columns nothing above needs. Treating those as opaque relations splits
//! every join tree into single-join pieces, far too small to be a double star,
//! which made this rule inert on real SQL.
//!
//! A projection that only selects columns is a subset and reordering of its
//! input, so [`pass_through_projection`] lets the clump continue through it and
//! each output column inherits its input's coordinate. Projections that compute
//! a value or rename a column are still refused: the first has no relation
//! column behind it, and the second would change the field names of the plan's
//! output.
//!
//! The consequence is that a subtree's output can be narrower than the
//! relations beneath it, which is why the map is tracked explicitly rather than
//! assuming output position `i` sits at `base + i`. A rewrite reproduces the
//! original output by projecting the rebuilt tree back down through that map,
//! restoring both the column order and the pruning in one step.
//!
//! # Detection
//!
//! [`detect_double_stars`] works purely on relation indices and edges, with no
//! [`ExecutionPlan`]s involved, so the shape rules can be tested directly.

use std::sync::Arc;

use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;

/// An equijoin edge between two relations of a [`JoinGraph`].
///
/// `left` is always the smaller relation index, so a given pair of relations
/// has exactly one canonical representation. `keys` holds column positions
/// local to each relation, in the same order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinEdge {
    /// Index of the lower-numbered relation.
    pub left: usize,
    /// Index of the higher-numbered relation.
    pub right: usize,
    /// `(column in left, column in right)` pairs, local to each relation.
    pub keys: Vec<(usize, usize)>,
}

impl JoinEdge {
    /// Create an edge, normalizing so `left < right`.
    fn new(left: usize, right: usize, key: (usize, usize)) -> Self {
        if left < right {
            Self {
                left,
                right,
                keys: vec![key],
            }
        } else {
            Self {
                left: right,
                right: left,
                keys: vec![(key.1, key.0)],
            }
        }
    }

    /// Whether this edge connects the same pair of relations as `other`.
    fn same_pair(&self, other: &Self) -> bool {
        self.left == other.left && self.right == other.right
    }
}

/// A tree of inner hash joins, flattened into relations and the equijoin edges
/// between them.
#[derive(Debug)]
pub struct JoinGraph {
    relations: Vec<Arc<dyn ExecutionPlan>>,
    offsets: Vec<usize>,
    edges: Vec<JoinEdge>,
    null_equality: NullEquality,
    /// For each column of the clump's output, the global coordinate of the
    /// relation column it reads. Narrower than the relations beneath it when a
    /// pruning projection was passed through.
    output_map: Vec<usize>,
}

impl JoinGraph {
    /// Flatten the join tree rooted at `plan`.
    ///
    /// Returns `None` unless `plan` is itself a reorderable join and the whole
    /// clump below it yields at least two relations. Anything that is not a
    /// reorderable join becomes an opaque leaf relation, so a `FilterExec`
    /// sitting between two joins simply splits them into separate clumps.
    pub fn try_new(plan: &Arc<dyn ExecutionPlan>) -> Option<Self> {
        // The root must be a join, otherwise there is nothing to reorder here.
        reorderable_join(plan.as_ref())?;

        let mut flattener = Flattener::default();
        let output_map = flattener.visit(plan)?;
        flattener.finish(output_map)
    }

    /// The flattened relations, in left-to-right order.
    pub fn relations(&self) -> &[Arc<dyn ExecutionPlan>] {
        &self.relations
    }

    /// Where each relation starts in the flattened schema.
    pub fn offsets(&self) -> &[usize] {
        &self.offsets
    }

    /// The equijoin edges, in canonical form.
    pub fn edges(&self) -> &[JoinEdge] {
        &self.edges
    }

    /// The null-handling behavior shared by every join in the clump.
    pub fn null_equality(&self) -> NullEquality {
        self.null_equality
    }

    /// For each column of the clump's output, the global coordinate of the
    /// relation column it reads.
    ///
    /// This is what a rewrite must reproduce: the same columns, in the same
    /// order, even though the rebuilt tree holds every relation column and the
    /// original output may have been pruned down from them.
    pub fn output_map(&self) -> &[usize] {
        &self.output_map
    }

    /// Resolve a global coordinate to `(relation, column within it)`.
    pub fn locate(&self, global: usize) -> Option<(usize, usize)> {
        let widths: Vec<usize> = self
            .relations
            .iter()
            .map(|relation| relation.schema().fields().len())
            .collect();
        resolve(&self.offsets, &widths, global)
    }

    /// Total number of columns across all relations.
    pub fn width(&self) -> usize {
        self.relations
            .iter()
            .map(|relation| relation.schema().fields().len())
            .sum()
    }

    /// Every valid double star decomposition of this graph.
    pub fn detect_double_stars(&self) -> Vec<DoubleStarShape> {
        detect_double_stars(self.relations.len(), &self.edges)
    }

    /// Replace each relation with the result of `map`, keeping offsets and
    /// edges as they are.
    ///
    /// Only valid for transformations that preserve schemas, since the offsets
    /// and the edges' column indices are positions within each relation. A
    /// replacement whose schema differs is discarded in favor of the original
    /// rather than trusted, because using it would silently misalign every
    /// index that follows.
    pub fn map_relations<F>(mut self, mut map: F) -> Result<Self>
    where
        F: FnMut(&Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
    {
        for relation in &mut self.relations {
            let mapped = map(relation)?;
            if mapped.schema() == relation.schema() {
                *relation = mapped;
            } else {
                log::debug!(
                    "double star: keeping a relation whose replacement changed schema"
                );
            }
        }
        Ok(self)
    }
}

/// Whether `plan` is a join this rule may reorder, and if so the join itself.
///
/// Each rejection below is a correctness gate rather than a simplification:
/// reordering past any of them would change the query's answer, not merely its
/// speed.
fn reorderable_join(plan: &dyn ExecutionPlan) -> Option<&HashJoinExec> {
    let join = plan.downcast_ref::<HashJoinExec>()?;

    // Outer joins are not freely reorderable: which rows get null-extended
    // depends on which side is which.
    if join.join_type != JoinType::Inner {
        log::debug!("double star: skipping {} join", join.join_type);
        return None;
    }
    // A join filter is a non-equi predicate bound to these specific inputs.
    if join.filter.is_some() {
        log::debug!("double star: skipping join carrying a filter");
        return None;
    }
    // A projection breaks the `output == left ++ right` identity that the
    // offset arithmetic depends on.
    if join.contains_projection() {
        log::debug!("double star: skipping join carrying a projection");
        return None;
    }
    // `fetch` caps the number of rows returned, so the join order decides
    // *which* rows survive. Reordering would return different data.
    if join.fetch().is_some() {
        log::debug!("double star: skipping join carrying a fetch limit");
        return None;
    }
    // The join has already been wired into a runtime filter aimed at a
    // particular scan.
    if !join.dynamic_expressions_produced().is_empty() {
        log::debug!("double star: skipping join carrying a dynamic filter");
        return None;
    }
    // Only legal on anti joins, so the `Inner` check above already excludes
    // it; asserted anyway so the invariant is local.
    if join.null_aware {
        log::debug!("double star: skipping null-aware join");
        return None;
    }
    // Keys such as `cast(a.x) = b.y` cannot be traced back to a base relation
    // column, so the graph cannot be built.
    if !join.on.iter().all(|(left, right)| {
        column_index(left.as_ref()).is_some() && column_index(right.as_ref()).is_some()
    }) {
        log::debug!("double star: skipping join with non-column equijoin keys");
        return None;
    }

    Some(join)
}

/// The position a `Column` expression reads, or `None` for any other
/// expression.
fn column_index(expr: &dyn PhysicalExpr) -> Option<usize> {
    expr.downcast_ref::<Column>().map(|column| column.index())
}

/// Accumulates relations and resolved keys while walking the join tree.
#[derive(Default)]
struct Flattener {
    relations: Vec<Arc<dyn ExecutionPlan>>,
    offsets: Vec<usize>,
    /// Where the next relation encountered will start.
    next_offset: usize,
    /// Key pairs in global coordinates, resolved to relations in
    /// [`Flattener::finish`] once every leaf is known.
    global_keys: Vec<(usize, usize)>,
    null_equality: Option<NullEquality>,
}

/// A projection the clump can continue through: one that only selects columns,
/// without computing anything or renaming.
///
/// A computed expression has no single relation column behind it, and a rename
/// would change the field names of the plan's output, which this rule promises
/// not to do.
fn pass_through_projection(plan: &dyn ExecutionPlan) -> Option<&ProjectionExec> {
    let projection = plan.downcast_ref::<ProjectionExec>()?;
    let input_schema = projection.input().schema();

    for entry in projection.expr() {
        let Some(index) = column_index(entry.expr.as_ref()) else {
            log::debug!("double star: projection computes a value, stopping there");
            return None;
        };
        let field = input_schema.fields().get(index)?;
        if field.name() != &entry.alias {
            log::debug!("double star: projection renames a column, stopping there");
            return None;
        }
    }

    Some(projection)
}

impl Flattener {
    /// Walk `plan` and return its **output map**: for each column of the
    /// subtree's output, the global coordinate of the relation column it
    /// ultimately reads.
    ///
    /// An output map rather than a width because a column-pruning projection
    /// between two joins makes a subtree's output narrower than the relations
    /// beneath it, so output position and relation position stop coinciding.
    /// Tracking the correspondence explicitly keeps the arithmetic honest
    /// through such a node; assuming `output position == base + i` does not.
    ///
    /// `None` means the clump is unusable and the caller should leave the plan
    /// alone.
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn visit(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<usize>> {
        if let Some(join) = reorderable_join(plan.as_ref()) {
            // Rebuilt joins may merge keys that came from different original
            // joins, so mixing null semantics within a clump is not safe.
            match self.null_equality {
                None => self.null_equality = Some(join.null_equality),
                Some(existing) if existing == join.null_equality => {}
                Some(_) => {
                    log::debug!("double star: clump mixes null equality settings");
                    return None;
                }
            }

            let left_map = self.visit(&join.left)?;
            let right_map = self.visit(&join.right)?;

            for (left_key, right_key) in &join.on {
                // `reorderable_join` already established both are columns, and
                // each index addresses that child's *output*, which is what the
                // child's map translates.
                let left_index = column_index(left_key.as_ref())?;
                let right_index = column_index(right_key.as_ref())?;
                self.global_keys
                    .push((*left_map.get(left_index)?, *right_map.get(right_index)?));
            }

            // An inner join's output is its left input's columns followed by
            // its right input's.
            let mut map = left_map;
            map.extend(right_map);
            return Some(map);
        }

        if let Some(projection) = pass_through_projection(plan.as_ref()) {
            // A pruning projection only selects and reorders, so the clump
            // continues below it and each output column inherits its input's
            // coordinate. Without this the physical planner's column pruning
            // splits every join tree into single-join pieces, too small to be
            // a double star at all.
            let child_map = self.visit(projection.input())?;
            return projection
                .expr()
                .iter()
                .map(|entry| {
                    let index = column_index(entry.expr.as_ref())?;
                    child_map.get(index).copied()
                })
                .collect();
        }

        // A leaf: its contents are opaque to us, only its width matters.
        // Naming it matters for diagnosis, since an unexpected operator between
        // two joins is what splits a clump into pieces too small to reorder.
        log::debug!("double star: treating {} as a relation", plan.name());
        let width = plan.schema().fields().len();
        let base = self.next_offset;
        self.relations.push(Arc::clone(plan));
        self.offsets.push(base);
        self.next_offset += width;
        Some((base..base + width).collect())
    }

    /// Resolve global key positions to relations and group them into edges.
    fn finish(self, output_map: Vec<usize>) -> Option<JoinGraph> {
        if self.relations.len() < 2 {
            return None;
        }

        let widths: Vec<usize> = self
            .relations
            .iter()
            .map(|relation| relation.schema().fields().len())
            .collect();

        let mut edges: Vec<JoinEdge> = Vec::new();
        for (left_global, right_global) in &self.global_keys {
            let (left_relation, left_column) =
                resolve(&self.offsets, &widths, *left_global)?;
            let (right_relation, right_column) =
                resolve(&self.offsets, &widths, *right_global)?;

            // Both sides of a join come from disjoint subtrees, so a key
            // cannot land inside a single relation.
            if left_relation == right_relation {
                log::debug!("double star: equijoin key resolved within one relation");
                return None;
            }

            let edge =
                JoinEdge::new(left_relation, right_relation, (left_column, right_column));
            match edges.iter_mut().find(|existing| existing.same_pair(&edge)) {
                // Several joins in the clump may connect the same pair; those
                // keys belong to one edge.
                Some(existing) => existing.keys.extend(edge.keys),
                None => edges.push(edge),
            }
        }

        Some(JoinGraph {
            relations: self.relations,
            offsets: self.offsets,
            edges,
            // A clump always contains at least one join, so this is set.
            null_equality: self.null_equality?,
            output_map,
        })
    }
}

/// Map a global column position to `(relation, column within that relation)`.
///
/// `offsets` is ascending by construction, since relations are recorded in
/// left-to-right order.
fn resolve(offsets: &[usize], widths: &[usize], global: usize) -> Option<(usize, usize)> {
    let relation = offsets
        .partition_point(|&offset| offset <= global)
        .checked_sub(1)?;
    let local = global - offsets[relation];
    (local < widths[relation]).then_some((relation, local))
}

/// One way of reading a join graph as a double star.
///
/// Holds relation indices only; cardinalities and selectivities are attached
/// separately, so recognizing the shape stays independent of whether
/// statistics are good enough to act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoubleStarShape {
    /// The relation bridging the two hubs.
    pub central: usize,
    /// The lower-numbered hub.
    pub hub_a: usize,
    /// The higher-numbered hub.
    pub hub_b: usize,
    /// Relations joined only to `hub_a`, ascending.
    pub spokes_a: Vec<usize>,
    /// Relations joined only to `hub_b`, ascending.
    pub spokes_b: Vec<usize>,
}

/// Find every valid double star decomposition of a join graph.
///
/// A decomposition is valid when the graph is a tree, some relation has degree
/// exactly two, and every relation other than that one and its two neighbors
/// has degree one and hangs off one of those neighbors.
///
/// # Why a list
///
/// A single graph can decompose more than one way. The four-relation path
/// `a - b - c - d` has two degree-two relations and both yield a valid double
/// star with different costs, so the caller prices each and keeps the cheapest
/// rather than guessing here. Results are ordered by `central`, and a
/// canonical hub ordering makes each decomposition unique, so the list is
/// deterministic.
///
/// # Why a tree
///
/// The cost model multiplies each edge's fanout as though the edges were
/// independent. A cycle means two paths between the same relations, so the
/// predicates are correlated and multiplying them double-counts the reduction.
/// That is a wrong answer rather than a poor one, so cyclic graphs are refused.
pub fn detect_double_stars(
    relation_count: usize,
    edges: &[JoinEdge],
) -> Vec<DoubleStarShape> {
    // Below four relations there is only one possible shape, so there is
    // nothing to decide.
    if relation_count < 4 {
        return Vec::new();
    }
    // A tree on `n` relations has exactly `n - 1` edges. Combined with the
    // connectivity check below this rules out cycles: a connected graph with
    // `n - 1` edges is acyclic.
    if edges.len() + 1 != relation_count {
        log::debug!(
            "double star: {relation_count} relations with {} edges is not a tree",
            edges.len()
        );
        return Vec::new();
    }

    let mut adjacency = vec![Vec::new(); relation_count];
    for edge in edges {
        adjacency[edge.left].push(edge.right);
        adjacency[edge.right].push(edge.left);
    }

    if !is_connected(&adjacency) {
        log::debug!("double star: join graph is disconnected");
        return Vec::new();
    }

    let mut shapes = Vec::new();
    for central in 0..relation_count {
        let [first, second] = adjacency[central][..] else {
            continue;
        };
        let hub_a = first.min(second);
        let hub_b = first.max(second);

        let mut spokes_a = Vec::new();
        let mut spokes_b = Vec::new();
        let mut valid = true;

        for (relation, neighbors) in adjacency.iter().enumerate() {
            if relation == central || relation == hub_a || relation == hub_b {
                continue;
            }
            // Anything hanging off a spoke, or bridging the hubs a second
            // time, disqualifies this decomposition.
            let [neighbor] = neighbors[..] else {
                valid = false;
                break;
            };
            if neighbor == hub_a {
                spokes_a.push(relation);
            } else if neighbor == hub_b {
                spokes_b.push(relation);
            } else {
                valid = false;
                break;
            }
        }

        if valid {
            shapes.push(DoubleStarShape {
                central,
                hub_a,
                hub_b,
                spokes_a,
                spokes_b,
            });
        }
    }

    shapes
}

/// Whether every relation is reachable from relation zero.
fn is_connected(adjacency: &[Vec<usize>]) -> bool {
    if adjacency.is_empty() {
        return false;
    }

    let mut seen = vec![false; adjacency.len()];
    let mut stack = vec![0];
    seen[0] = true;
    let mut reached = 1;

    while let Some(relation) = stack.pop() {
        for &neighbor in &adjacency[relation] {
            if !seen[neighbor] {
                seen[neighbor] = true;
                reached += 1;
                stack.push(neighbor);
            }
        }
    }

    reached == adjacency.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::double_star_join_reorder::test_support::{
        join, join_builder, scan, typed_join,
    };

    use arrow::datatypes::{DataType, Schema};
    use datafusion_physical_expr::expressions::{CastExpr, lit};
    use datafusion_physical_plan::joins::HashJoinExecBuilder;
    use datafusion_physical_plan::joins::utils::JoinFilter;
    use std::sync::Arc;

    // ---------- shape detection: pure graphs, no execution plans ----------

    /// Build edges from `(left, right)` pairs with a single dummy key each.
    fn edges(pairs: &[(usize, usize)]) -> Vec<JoinEdge> {
        pairs
            .iter()
            .map(|&(left, right)| JoinEdge::new(left, right, (0, 0)))
            .collect()
    }

    #[test]
    fn detects_the_canonical_bowtie() {
        //   1  2        4  5
        //    \ |        | /
        //      0 -- 3 -- 6         0 and 6 are hubs, 3 is central
        let shapes = detect_double_stars(
            7,
            &edges(&[(0, 1), (0, 2), (0, 3), (3, 6), (6, 4), (6, 5)]),
        );

        assert_eq!(
            shapes,
            vec![DoubleStarShape {
                central: 3,
                hub_a: 0,
                hub_b: 6,
                spokes_a: vec![1, 2],
                spokes_b: vec![4, 5],
            }]
        );
    }

    #[test]
    fn a_four_path_decomposes_two_ways() {
        // 0 - 1 - 2 - 3: both 1 and 2 have degree two, and both work.
        let shapes = detect_double_stars(4, &edges(&[(0, 1), (1, 2), (2, 3)]));

        assert_eq!(
            shapes,
            vec![
                DoubleStarShape {
                    central: 1,
                    hub_a: 0,
                    hub_b: 2,
                    spokes_a: vec![],
                    spokes_b: vec![3],
                },
                DoubleStarShape {
                    central: 2,
                    hub_a: 1,
                    hub_b: 3,
                    spokes_a: vec![0],
                    spokes_b: vec![],
                },
            ]
        );
    }

    #[test]
    fn a_five_path_decomposes_one_way() {
        // 0 - 1 - 2 - 3 - 4: only the middle relation can be central, because
        // any other choice leaves a degree-two relation stranded.
        let shapes = detect_double_stars(5, &edges(&[(0, 1), (1, 2), (2, 3), (3, 4)]));

        assert_eq!(
            shapes,
            vec![DoubleStarShape {
                central: 2,
                hub_a: 1,
                hub_b: 3,
                spokes_a: vec![0],
                spokes_b: vec![4],
            }]
        );
    }

    #[test]
    fn rejects_a_cycle() {
        // A square: four relations, four edges, so not a tree.
        assert!(
            detect_double_stars(4, &edges(&[(0, 1), (1, 2), (2, 3), (3, 0)])).is_empty()
        );
    }

    #[test]
    fn rejects_a_disconnected_graph() {
        // 0-1, 2-3 and a duplicate edge: the count matches a tree but the
        // graph is in two pieces, which is exactly what connectivity catches.
        let mut pairs = edges(&[(0, 1), (2, 3)]);
        pairs.push(JoinEdge::new(2, 3, (1, 1)));
        // Merged edges would collapse the duplicate, so build it by hand to
        // hit the disconnected branch rather than the edge-count branch.
        assert_eq!(pairs.len(), 3);
        assert!(detect_double_stars(4, &pairs).is_empty());
    }

    #[test]
    fn rejects_a_single_star() {
        // 0 at the center with three spokes: the only degree-two candidate
        // would have to be a spoke, and none is.
        assert!(detect_double_stars(4, &edges(&[(0, 1), (0, 2), (0, 3)])).is_empty());
    }

    #[test]
    fn rejects_a_triple_star() {
        //   1     3     5        three hubs chained by two centrals
        //   |     |     |
        //   0 - 2 - 4 - 6 ... hub 4 sits between two bridges
        let shapes = detect_double_stars(
            8,
            &edges(&[(0, 1), (0, 2), (2, 4), (4, 3), (4, 6), (6, 5), (6, 7)]),
        );
        assert!(shapes.is_empty());
    }

    #[test]
    fn rejects_a_spoke_with_its_own_child() {
        // The canonical bowtie, but spoke 1 carries a child of its own:
        //
        //   7 - 1  2            5  6
        //        \ |            | /
        //          0 ---- 3 ---- 4
        //
        // Relation 1 now has degree two, so neither degree-two candidate can
        // work: central 1 is stranded by relation 3, and central 3 by
        // relation 1.
        //
        // Note this needs the full bowtie. A smaller graph tends to admit some
        // other valid assignment of roles, since detection searches every
        // candidate rather than honouring the roles the diagram implies.
        let shapes = detect_double_stars(
            8,
            &edges(&[(0, 1), (0, 2), (0, 3), (3, 4), (4, 5), (4, 6), (1, 7)]),
        );
        assert!(shapes.is_empty());
    }

    #[test]
    fn rejects_graphs_too_small_to_reorder() {
        // hub - central - hub admits only one order, so there is no decision.
        assert!(detect_double_stars(3, &edges(&[(0, 1), (1, 2)])).is_empty());
    }

    // ---------- flattening: real execution plans ----------

    /// The worked example from the module docs: a left-deep tree over
    /// orders(3), customer(3), nation(2), lineitem(2).
    #[test]
    fn flattens_a_left_deep_tree() {
        let orders = scan(&["orderkey", "custkey", "total"]);
        let customer = scan(&["custkey", "nationkey", "name"]);
        let nation = scan(&["nationkey", "nname"]);
        let lineitem = scan(&["orderkey", "qty"]);

        // orders.custkey = customer.custkey
        let hj1 = join(orders, customer, &[(1, 0)]);
        // customer.nationkey is column 4 of hj1's output
        let hj2 = join(hj1, nation, &[(4, 0)]);
        // orders.orderkey is column 0 of hj2's output
        let hj3 = join(hj2, lineitem, &[(0, 0)]);

        let graph = JoinGraph::try_new(&hj3).expect("a reorderable clump");

        assert_eq!(graph.relations().len(), 4);
        assert_eq!(graph.offsets(), &[0, 3, 6, 8]);
        assert_eq!(graph.width(), 10);

        // The index 4 in hj2 had no hint that it belonged to customer, let
        // alone that it was that relation's second column.
        assert_eq!(
            graph.edges(),
            &[
                JoinEdge {
                    left: 0,
                    right: 1,
                    keys: vec![(1, 0)]
                },
                JoinEdge {
                    left: 1,
                    right: 2,
                    keys: vec![(1, 0)]
                },
                JoinEdge {
                    left: 0,
                    right: 3,
                    keys: vec![(0, 0)]
                },
            ]
        );
    }

    /// A bushy tree is where the `base` offset earns its keep: the right
    /// subtree's keys are numbered from zero locally but start at global 2.
    #[test]
    fn flattens_a_bushy_tree() {
        //        hj3
        //       /   \
        //     hj1    hj2
        //    /  \   /   \
        //   a    b c     d
        let a = scan(&["a0", "a1"]);
        let b = scan(&["b0"]);
        let c = scan(&["c0", "c1"]);
        let d = scan(&["d0"]);

        let hj1 = join(a, b, &[(0, 0)]);
        // Local indices (0, 0) here mean c.c0 = d.d0, not a.a0 = b.b0.
        let hj2 = join(c, d, &[(0, 0)]);
        // hj1 output is a0,a1,b0; hj2 output is c0,c1,d0.
        let hj3 = join(hj1, hj2, &[(1, 0)]);

        let graph = JoinGraph::try_new(&hj3).expect("a reorderable clump");

        assert_eq!(graph.offsets(), &[0, 2, 3, 5]);
        assert_eq!(
            graph.edges(),
            &[
                JoinEdge {
                    left: 0,
                    right: 1,
                    keys: vec![(0, 0)]
                },
                // Would have been resolved as relations 0 and 1 again if the
                // subtree offset were ignored.
                JoinEdge {
                    left: 2,
                    right: 3,
                    keys: vec![(0, 0)]
                },
                JoinEdge {
                    left: 0,
                    right: 2,
                    keys: vec![(1, 0)]
                },
            ]
        );
    }

    #[test]
    fn merges_multiple_keys_between_one_pair() {
        let left = scan(&["x", "y"]);
        let right = scan(&["x", "y"]);
        let other = scan(&["z"]);

        let inner = join(left, right, &[(0, 0), (1, 1)]);
        let root = join(inner, other, &[(0, 0)]);

        let graph = JoinGraph::try_new(&root).expect("a reorderable clump");

        assert_eq!(graph.edges()[0].keys, vec![(0, 0), (1, 1)]);
    }

    #[test]
    fn one_join_can_produce_two_edges() {
        // After flattening, `a.x = c.x AND b.y = c.y` on a single join spans
        // two different relation pairs.
        let a = scan(&["x"]);
        let b = scan(&["y"]);
        let c = scan(&["x", "y"]);

        let ab = join(a, b, &[(0, 0)]);
        // Column 0 of ab is a.x, column 1 is b.y.
        let root = join(ab, c, &[(0, 0), (1, 1)]);

        let graph = JoinGraph::try_new(&root).expect("a reorderable clump");

        assert_eq!(
            graph.edges(),
            &[
                JoinEdge {
                    left: 0,
                    right: 1,
                    keys: vec![(0, 0)]
                },
                JoinEdge {
                    left: 0,
                    right: 2,
                    keys: vec![(0, 0)]
                },
                JoinEdge {
                    left: 1,
                    right: 2,
                    keys: vec![(0, 1)]
                },
            ]
        );
    }

    #[test]
    fn stops_at_a_non_inner_join() {
        let a = scan(&["x"]);
        let b = scan(&["x"]);
        let c = scan(&["x"]);

        let outer = typed_join(a, b, &[(0, 0)], JoinType::Left);
        let root = join(outer, c, &[(0, 0)]);

        let graph = JoinGraph::try_new(&root).expect("the root is still reorderable");

        // The left join is opaque, so the clump has two relations, not three.
        assert_eq!(graph.relations().len(), 2);
        assert_eq!(graph.offsets(), &[0, 2]);
    }

    // ---------- eligibility gates ----------
    //
    // Each of these guards the query's answer rather than its speed, so each
    // gets its own test. The shape of every assertion is the same: an inner
    // join that trips one gate becomes an opaque leaf, so the clump sees two
    // relations rather than three.

    /// Build `a JOIN b` (tripped by `alter`) then join the result to `c`.
    fn clump_over_a_gated_join(
        alter: impl FnOnce(HashJoinExecBuilder) -> HashJoinExecBuilder,
    ) -> Arc<dyn ExecutionPlan> {
        let inner = alter(join_builder(scan(&["a0", "a1"]), scan(&["b0"]), &[(0, 0)]))
            .build_exec()
            .expect("valid inner join");
        join(inner, scan(&["c0"]), &[(0, 0)])
    }

    fn assert_inner_join_was_opaque(root: &Arc<dyn ExecutionPlan>) {
        let graph = JoinGraph::try_new(root).expect("the root is still reorderable");
        assert_eq!(
            graph.relations().len(),
            2,
            "expected the gated join to become an opaque leaf"
        );
    }

    #[test]
    fn stops_at_a_join_carrying_a_filter() {
        // A join filter is a non-equi predicate bound to these exact inputs;
        // moving the join without it would change the result.
        let filter = JoinFilter::new(lit(true), vec![], Arc::new(Schema::empty()));
        assert_inner_join_was_opaque(&clump_over_a_gated_join(|builder| {
            builder.with_filter(Some(filter))
        }));
    }

    #[test]
    fn stops_at_a_join_carrying_a_projection() {
        // A projection breaks the `output == left ++ right` identity that the
        // offset arithmetic depends on.
        assert_inner_join_was_opaque(&clump_over_a_gated_join(|builder| {
            builder.with_projection(Some(vec![0, 1]))
        }));
    }

    #[test]
    fn stops_at_a_join_carrying_a_fetch_limit() {
        // `fetch` caps the rows returned, so the join order decides *which*
        // rows survive. Reordering would return different data.
        assert_inner_join_was_opaque(&clump_over_a_gated_join(|builder| {
            builder.with_fetch(Some(10))
        }));
    }

    #[test]
    fn stops_at_non_column_equijoin_keys() {
        // `cast(a.a0) = b.b0` cannot be traced back to a base relation column,
        // so the graph cannot be built from it.
        let left = scan(&["a0", "a1"]);
        let right = scan(&["b0"]);
        let cast = Arc::new(CastExpr::new(
            Arc::new(Column::new("a0", 0)),
            DataType::Int64,
            None,
        )) as Arc<dyn PhysicalExpr>;
        let on = vec![(
            cast,
            Arc::new(Column::new("b0", 0)) as Arc<dyn PhysicalExpr>,
        )];

        let inner = HashJoinExecBuilder::new(left, right, on, JoinType::Inner)
            .build_exec()
            .expect("valid inner join");
        let root = join(inner, scan(&["c0"]), &[(0, 0)]);

        assert_inner_join_was_opaque(&root);
    }

    #[test]
    fn refuses_a_clump_that_mixes_null_equality() {
        // A rebuilt join may merge keys that came from different original
        // joins, so there would be no single correct setting to apply.
        let inner = join_builder(scan(&["a0", "a1"]), scan(&["b0"]), &[(0, 0)])
            .with_null_equality(NullEquality::NullEqualsNull)
            .build_exec()
            .expect("valid inner join");
        let root = join(inner, scan(&["c0"]), &[(0, 0)]);

        assert!(JoinGraph::try_new(&root).is_none());
    }

    #[test]
    fn a_uniform_null_equality_clump_is_accepted() {
        // The counterpart to the test above: agreeing joins flatten normally,
        // and the setting is carried through for the rewrite to reapply.
        let inner = join_builder(scan(&["a0", "a1"]), scan(&["b0"]), &[(0, 0)])
            .with_null_equality(NullEquality::NullEqualsNull)
            .build_exec()
            .expect("valid inner join");
        let root = join_builder(inner, scan(&["c0"]), &[(0, 0)])
            .with_null_equality(NullEquality::NullEqualsNull)
            .build_exec()
            .expect("valid inner join");

        let graph = JoinGraph::try_new(&root).expect("a reorderable clump");
        assert_eq!(graph.relations().len(), 3);
        assert_eq!(graph.null_equality(), NullEquality::NullEqualsNull);
    }

    // ---------- substituting relations ----------

    #[test]
    fn map_relations_substitutes_each_relation() {
        // The rule optimizes leaves before rebuilding, so substitution has to
        // actually take effect.
        let plan = join(scan(&["a0", "a1"]), scan(&["b0"]), &[(0, 0)]);
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let replacement = scan(&["a0", "a1"]);

        let mapped = graph
            .map_relations(|relation| {
                Ok(if relation.schema().fields().len() == 2 {
                    Arc::clone(&replacement)
                } else {
                    Arc::clone(relation)
                })
            })
            .expect("mapping succeeds");

        assert!(Arc::ptr_eq(&mapped.relations()[0], &replacement));
    }

    #[test]
    fn map_relations_discards_a_replacement_that_changed_schema() {
        // Offsets and edge keys are positions within each relation, so a
        // narrower replacement would misalign every index after it. Keeping
        // the original is the fail-safe choice.
        let plan = join(scan(&["a0", "a1"]), scan(&["b0"]), &[(0, 0)]);
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let original = Arc::clone(&graph.relations()[0]);
        let narrower = scan(&["a0"]);

        let mapped = graph
            .map_relations(|_| Ok(Arc::clone(&narrower)))
            .expect("mapping succeeds");

        assert!(Arc::ptr_eq(&mapped.relations()[0], &original));
    }

    #[test]
    fn a_non_join_root_is_not_a_clump() {
        assert!(JoinGraph::try_new(&scan(&["x"])).is_none());
    }

    #[test]
    fn end_to_end_bowtie_is_detected() {
        //   a1  a2            b1
        //     \ |             |
        //      hub_a -- c -- hub_b
        let hub_a = scan(&["k", "sa1", "sa2"]);
        let a1 = scan(&["k"]);
        let a2 = scan(&["k"]);
        let central = scan(&["ka", "kb"]);
        let hub_b = scan(&["k", "sb1"]);
        let b1 = scan(&["k"]);

        let left = join(hub_a, a1, &[(1, 0)]);
        let left = join(left, a2, &[(2, 0)]);
        // hub_a.k is column 0; the central relation joins on its first column.
        let left = join(left, central, &[(0, 0)]);
        let right = join(hub_b, b1, &[(1, 0)]);
        // central.kb is the last column of `left`; hub_b.k starts `right`.
        let root = join(left, right, &[(6, 0)]);

        let graph = JoinGraph::try_new(&root).expect("a reorderable clump");
        let shapes = graph.detect_double_stars();

        assert_eq!(
            shapes,
            vec![DoubleStarShape {
                central: 3,
                hub_a: 0,
                hub_b: 4,
                spokes_a: vec![1, 2],
                spokes_b: vec![5],
            }]
        );
    }
}
