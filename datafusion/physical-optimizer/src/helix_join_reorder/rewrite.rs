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

//! Emitting a join tree in a chosen order.
//!
//! # Why indices are the whole problem
//!
//! A physical [`Column`] is `{ name, index }` where only the index is read at
//! execution time; the name is for display. Reordering joins moves every
//! column, so an index carried over from the old plan points at whatever now
//! happens to sit in that position: the query reads the wrong column, joins on
//! the wrong values and returns wrong rows, with no error anywhere.
//!
//! The defense is structural rather than careful arithmetic. Each subtree
//! under construction carries the offset of every relation it contains, and
//! every index emitted is `offset + local column`. Old indices were consumed
//! once during flattening, turned into `(relation, local column)` pairs, and
//! discarded; none is in scope here to be copied by mistake.
//!
//! # Restoring the column order
//!
//! Reordering permutes the output columns, but everything above the join tree
//! still expects the original order. Rather than rewriting all of it,
//! [`rebuild`] reports the permutation and [`apply_projection`] pushes it into
//! the top join's own projection, so the rewrite is invisible from outside:
//! same schema, same column order, same rows.
//!
//! The two are kept separate so the join tree can be inspected without the
//! projection, which is what lets tests flatten the result and compare it to
//! the input.

use std::sync::Arc;

use datafusion_common::JoinType;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::{HashJoinExec, HashJoinExecBuilder, JoinOn};

use super::cost_model::JoinTree;
use super::join_graph::JoinGraph;

/// One `(build side, probe side)` equijoin key pair.
type EquijoinKey = (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>);

/// A rebuilt join tree together with the permutation restoring the original
/// column order.
#[derive(Debug, Clone)]
pub struct Rewritten {
    /// The rebuilt tree, with no column-order projection applied yet.
    pub plan: Arc<dyn ExecutionPlan>,
    /// `projection[i]` is where the original column `i` now sits.
    pub projection: Vec<usize>,
}

/// A partially built join tree, tracking where each relation's columns start.
struct Subtree {
    plan: Arc<dyn ExecutionPlan>,
    /// Offset of each relation within this subtree, `None` when absent.
    offsets: Vec<Option<usize>>,
    width: usize,
}

impl Subtree {
    /// A subtree consisting of a single relation.
    fn leaf(
        relation: usize,
        plan: Arc<dyn ExecutionPlan>,
        relation_count: usize,
    ) -> Self {
        let width = plan.schema().fields().len();
        let mut offsets = vec![None; relation_count];
        offsets[relation] = Some(0);
        Self {
            plan,
            offsets,
            width,
        }
    }

    /// Where `relation`'s columns start here, if it is present.
    fn offset_of(&self, relation: usize) -> Option<usize> {
        self.offsets[relation]
    }
}

/// Build the join tree described by `tree`.
///
/// Returns `None` if the order and the graph disagree — a join step with no
/// connecting predicate, a relation named twice, or one left unplaced. All
/// would mean a bug in the cost model rather than an unsupported query, and
/// all are refused rather than emitted.
pub fn rebuild(graph: &JoinGraph, tree: &JoinTree) -> Option<Rewritten> {
    let top = build(graph, tree)?;

    // Every relation placed exactly once, and no columns gained or lost.
    if top.offsets.iter().any(Option::is_none) || top.width != graph.width() {
        log::debug!("helix: rebuilt tree does not cover every relation");
        return None;
    }

    // Reproduce the clump's original output: the same columns, in the same
    // order. The rebuilt tree carries every relation column, and the original
    // output may have been pruned down from them by a projection we passed
    // through, so this projection restores the order *and* re-applies that
    // pruning.
    let mut projection = Vec::with_capacity(graph.output_map().len());
    for &global in graph.output_map() {
        let (relation, column) = graph.locate(global)?;
        projection.push(top.offset_of(relation)? + column);
    }

    Some(Rewritten {
        plan: top.plan,
        projection,
    })
}

/// Push the column-order permutation into the top join's projection.
///
/// A rewrite that happens to leave the column order untouched needs no
/// projection, and emitting one would only clutter the plan.
pub fn apply_projection(rewritten: Rewritten) -> Option<Arc<dyn ExecutionPlan>> {
    let Rewritten { plan, projection } = rewritten;

    // Only skip when the projection selects every column in its existing
    // order. The width check is load-bearing: when a pruning projection was
    // passed through, `projection` is shorter than the tree, and handing back
    // the tree unprojected would emit columns the clump's output never had.
    if projection.len() == plan.schema().fields().len()
        && projection
            .iter()
            .enumerate()
            .all(|(position, &source)| position == source)
    {
        return Some(plan);
    }

    let join = plan.downcast_ref::<HashJoinExec>()?;
    join.builder()
        .with_projection(Some(projection))
        .build_exec()
        .inspect_err(|error| log::debug!("helix: could not apply projection: {error}"))
        .ok()
}

/// Emit one subtree of a chosen order.
///
/// Recursion is bounded by the relation count, which the cost model caps.
fn build(graph: &JoinGraph, tree: &JoinTree) -> Option<Subtree> {
    match tree {
        JoinTree::Leaf(relation) => {
            // Indexed rather than assumed: a cost model naming a relation the
            // graph does not have should decline, not panic.
            let plan = graph.relations().get(*relation)?;
            Some(Subtree::leaf(
                *relation,
                Arc::clone(plan),
                graph.relations().len(),
            ))
        }
        JoinTree::Join(left, right) => {
            join(build(graph, left)?, build(graph, right)?, graph)
        }
    }
}

/// Join two subtrees on every graph edge that crosses between them.
fn join(left: Subtree, right: Subtree, graph: &JoinGraph) -> Option<Subtree> {
    let on = crossing_keys(&left, &right, graph)?;
    if on.is_empty() {
        // Without a predicate this would silently become a cross join: the
        // same columns, catastrophically more rows.
        log::debug!("helix: no predicate connects two subtrees");
        return None;
    }

    let Subtree {
        plan: left_plan,
        mut offsets,
        width: left_width,
    } = left;
    let Subtree {
        plan: right_plan,
        offsets: right_offsets,
        width: right_width,
    } = right;

    let plan = HashJoinExecBuilder::new(left_plan, right_plan, on, JoinType::Inner)
        // Partition mode is left unresolved on purpose: `JoinSelection` runs
        // after this rule and picks the build side for the tree we emit.
        .with_null_equality(graph.null_equality())
        .build_exec()
        .inspect_err(|error| log::debug!("helix: could not build join: {error}"))
        .ok()?;

    // The new schema is the left subtree's columns followed by the right's.
    for (relation, offset) in right_offsets.iter().enumerate() {
        if let Some(offset) = offset {
            offsets[relation] = Some(offset + left_width);
        }
    }

    Some(Subtree {
        plan,
        offsets,
        width: left_width + right_width,
    })
}

/// Equijoin keys for every edge with one endpoint in `left` and the other in
/// `right`.
fn crossing_keys(left: &Subtree, right: &Subtree, graph: &JoinGraph) -> Option<JoinOn> {
    let mut on = JoinOn::new();

    for edge in graph.edges() {
        // Edges are canonical (`left < right`), but which endpoint ends up on
        // the build side depends on the join order, not on relation numbering.
        // When the edge is flipped its key pair flips with it.
        let placement = match (left.offset_of(edge.left), right.offset_of(edge.right)) {
            (Some(left_base), Some(right_base)) => (left_base, right_base, false),
            _ => match (left.offset_of(edge.right), right.offset_of(edge.left)) {
                (Some(left_base), Some(right_base)) => (left_base, right_base, true),
                _ => continue,
            },
        };
        let (left_base, right_base, flipped) = placement;

        for &(first, second) in &edge.keys {
            let (in_left, in_right) = if flipped {
                (second, first)
            } else {
                (first, second)
            };
            on.push(key(
                &left.plan,
                left_base + in_left,
                &right.plan,
                right_base + in_right,
            )?);
        }
    }

    Some(on)
}

/// One equijoin key, naming each column from the schema position it reads so
/// the name cannot disagree with the index.
fn key(
    left: &Arc<dyn ExecutionPlan>,
    left_index: usize,
    right: &Arc<dyn ExecutionPlan>,
    right_index: usize,
) -> Option<EquijoinKey> {
    let left_schema = left.schema();
    let right_schema = right.schema();
    let left_name = left_schema.fields().get(left_index)?.name();
    let right_name = right_schema.fields().get(right_index)?.name();

    Some((
        Arc::new(Column::new(left_name, left_index)),
        Arc::new(Column::new(right_name, right_index)),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::helix_join_reorder::cost_model::optimal_join_order;
    use crate::helix_join_reorder::join_graph::JoinEdge;
    use crate::helix_join_reorder::statistics::GraphStatistics;

    use crate::helix_join_reorder::test_support::{
        diamond, join, measured_diamond, scan,
    };
    use std::collections::HashSet;

    /// Edges as `(left relation, right relation, sorted keys)`.
    type EdgeSet = HashSet<(usize, usize, Vec<(usize, usize)>)>;

    /// Edges as an unordered set, renumbered into `reference`'s relation
    /// indices.
    ///
    /// Re-flattening a rewritten plan numbers relations by their new
    /// left-to-right position, so the two graphs describe the same edges in
    /// different index spaces. Relations are matched by `Arc` identity, which
    /// the rewrite preserves and which is independent of the index arithmetic
    /// under test, so this stays an honest oracle rather than checking the
    /// remapping against itself.
    fn edge_set(graph: &JoinGraph, reference: &JoinGraph) -> EdgeSet {
        let relabel: Vec<usize> = graph
            .relations()
            .iter()
            .map(|relation| {
                reference
                    .relations()
                    .iter()
                    .position(|other| Arc::ptr_eq(relation, other))
                    .expect("the rewrite reuses the original relations")
            })
            .collect();

        graph
            .edges()
            .iter()
            .map(|edge| {
                let (left, right) = (relabel[edge.left], relabel[edge.right]);
                // Restore the canonical `left < right` orientation under the
                // reference numbering, flipping the key pairs with it.
                let (left, right, mut keys) = if left < right {
                    (left, right, edge.keys.clone())
                } else {
                    let flipped = edge.keys.iter().map(|&(a, b)| (b, a)).collect();
                    (right, left, flipped)
                };
                keys.sort_unstable();
                (left, right, keys)
            })
            .collect()
    }

    fn leaf(relation: usize) -> JoinTree {
        JoinTree::Leaf(relation)
    }

    /// An order for the [`diamond`] fixture that is not the one it is in.
    ///
    /// Relations there are `p0` 0, `a0` 1, `b0` 2, `p1` 3. The input tree is
    /// `((p0 a0) b0) p1`; this closes the diamond from the other side and is
    /// bushy at the root, which the input never is.
    fn reordered_diamond() -> JoinTree {
        JoinTree::join(
            JoinTree::join(leaf(1), leaf(3)),
            JoinTree::join(leaf(0), leaf(2)),
        )
    }

    /// Rebuild `plan` under `tree`, returning the projected tree.
    fn rewrite(
        plan: &Arc<dyn ExecutionPlan>,
        tree: &JoinTree,
    ) -> (Arc<dyn ExecutionPlan>, Rewritten) {
        let graph = JoinGraph::try_new(plan).expect("a reorderable clump");

        let rewritten = rebuild(&graph, tree).expect("a rebuildable order");
        let projected = apply_projection(rewritten.clone())
            .expect("projection applies to the top join");
        (projected, rewritten)
    }

    #[test]
    fn preserves_the_output_schema() {
        let original = diamond();
        let (rewritten, _) = rewrite(&original, &reordered_diamond());

        assert_eq!(rewritten.schema(), original.schema());
    }

    /// Flattening is the inverse of rebuilding, so re-flattening the result
    /// must recover exactly the edges we started from. Any index remapped
    /// wrongly would resolve to a different relation or column here.
    #[test]
    fn re_flattening_recovers_the_same_edges() {
        let original = diamond();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");

        let (_, rewritten) = rewrite(&original, &reordered_diamond());
        // The un-projected tree, since a projection would stop flattening.
        let after =
            JoinGraph::try_new(&rewritten.plan).expect("still a reorderable clump");

        assert_eq!(edge_set(&before, &before), edge_set(&after, &before));
    }

    #[test]
    fn places_every_relation_exactly_once() {
        let original = diamond();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original, &reordered_diamond());
        let after =
            JoinGraph::try_new(&rewritten.plan).expect("still a reorderable clump");

        assert_eq!(before.relations().len(), after.relations().len());
        assert_eq!(before.width(), after.width());

        // Same relations, in some order.
        let identify = |graph: &JoinGraph| {
            let mut names: Vec<String> = graph
                .relations()
                .iter()
                .map(|relation| format!("{:?}", relation.schema().fields()))
                .collect();
            names.sort();
            names
        };
        assert_eq!(identify(&before), identify(&after));
    }

    #[test]
    fn keeps_every_predicate_exactly_once() {
        let original = diamond();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original, &reordered_diamond());
        let after =
            JoinGraph::try_new(&rewritten.plan).expect("still a reorderable clump");

        let count = |graph: &JoinGraph| {
            graph
                .edges()
                .iter()
                .map(|edge: &JoinEdge| edge.keys.len())
                .sum::<usize>()
        };
        assert_eq!(count(&before), count(&after));
    }

    #[test]
    fn the_projection_actually_permutes() {
        let (_, rewritten) = rewrite(&diamond(), &reordered_diamond());

        // A real reorder, not an accidental no-op.
        assert!(
            rewritten
                .projection
                .iter()
                .enumerate()
                .any(|(position, &source)| position != source),
            "expected the rewrite to move columns"
        );

        // And it is a permutation of every column.
        let mut sorted = rewritten.projection.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, (0..rewritten.projection.len()).collect::<Vec<_>>());
    }

    /// Statistics-driven end to end, so the order under test is one the cost
    /// model actually chose rather than one written out by hand.
    #[test]
    fn rewrites_from_real_statistics() {
        let plan = measured_diamond();
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");

        let statistics =
            GraphStatistics::try_new(&graph, None).expect("statistics are usable");
        let query_graph = statistics.query_graph(&graph).expect("within the cap");
        let chosen = optimal_join_order(&query_graph).expect("a usable order");

        let rewritten = rebuild(&graph, &chosen.tree).expect("a rebuildable order");
        let projected = apply_projection(rewritten).expect("projection applies");

        assert_eq!(projected.schema(), plan.schema());
    }

    #[test]
    fn refuses_an_order_that_leaves_a_relation_unplaced() {
        let graph = JoinGraph::try_new(&diamond()).expect("a reorderable clump");
        // Three of the diamond's four relations.
        let tree = JoinTree::join(JoinTree::join(leaf(0), leaf(1)), leaf(3));

        assert!(rebuild(&graph, &tree).is_none());
    }

    #[test]
    fn refuses_an_order_naming_a_relation_the_graph_lacks() {
        let graph = JoinGraph::try_new(&diamond()).expect("a reorderable clump");
        let tree = JoinTree::join(leaf(0), leaf(99));

        assert!(rebuild(&graph, &tree).is_none());
    }

    #[test]
    fn refuses_an_order_naming_a_relation_twice() {
        let graph = JoinGraph::try_new(&diamond()).expect("a reorderable clump");
        let tree = JoinTree::join(JoinTree::join(leaf(0), leaf(1)), leaf(1));

        assert!(rebuild(&graph, &tree).is_none());
    }

    // ---------- the key swap ----------

    /// Every equijoin key pair in a plan, named by the column each side reads,
    /// with the two names sorted so a pair is compared regardless of which side
    /// it landed on.
    ///
    /// Names come from the schema position the key reads, so a wrong index
    /// produces a wrong name here.
    fn key_pairs(plan: &Arc<dyn ExecutionPlan>) -> Vec<(String, String)> {
        fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<(String, String)>) {
            if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
                for (left, right) in &join.on {
                    let name = |expr: &Arc<dyn PhysicalExpr>| {
                        expr.downcast_ref::<Column>()
                            .expect("an equijoin key is a column")
                            .name()
                            .to_string()
                    };
                    let (a, b) = (name(left), name(right));
                    out.push(if a <= b { (a, b) } else { (b, a) });
                }
            }
            for child in plan.children() {
                walk(child, out);
            }
        }

        let mut out = Vec::new();
        walk(plan, &mut out);
        out.sort();
        out
    }

    /// A diamond whose first relation is a link rather than a spine relation:
    /// `a0, p0, b0, p1` in flattening order.
    ///
    /// Edges are canonicalized so `left < right`, but which endpoint ends up on
    /// a join's build side depends on the order chosen, not on numbering. Under
    /// [`flipped_order`] this arrangement puts `edge.left` on a join's right
    /// side, which is the only way to reach the key swap in `crossing_keys`.
    fn link_before_spine_diamond() -> Arc<dyn ExecutionPlan> {
        let a0 = scan(&["a0_p0", "a0_p1"]);
        let p0 = scan(&["p0_ka", "p0_kb", "p0_x"]);
        let b0 = scan(&["b0_p0", "b0_p1"]);
        let p1 = scan(&["p1_a", "p1_b", "p1_c"]);

        // a0.a0_p0 = p0.p0_ka
        let left = join(a0, p0, &[(0, 0)]);
        // p0.p0_kb sits at global 3, after a0's two columns.
        let left = join(left, b0, &[(3, 0)]);
        // a0.a0_p1 is global 1 and b0.b0_p1 is global 6.
        join(left, p1, &[(1, 0), (6, 1)])
    }

    /// An order for [`link_before_spine_diamond`] whose top join carries one
    /// edge in each orientation: `0-1` reversed and `2-3` as canonicalized.
    fn flipped_order() -> JoinTree {
        JoinTree::join(
            JoinTree::join(leaf(1), leaf(2)),
            JoinTree::join(leaf(0), leaf(3)),
        )
    }

    #[test]
    fn remaps_a_flipped_edge() {
        let original = link_before_spine_diamond();
        let (rewritten, _) = rewrite(&original, &flipped_order());

        // The same columns are paired after the rewrite. A key pair that was
        // not swapped alongside its edge would either name different columns
        // here or read past the end of the narrower relation.
        assert_eq!(key_pairs(&original), key_pairs(&rewritten));
        assert_eq!(original.schema(), rewritten.schema());
    }

    #[test]
    fn a_flipped_edge_still_round_trips_through_flattening() {
        let original = link_before_spine_diamond();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original, &flipped_order());
        let after = JoinGraph::try_new(&rewritten.plan).expect("still reorderable");

        assert_eq!(edge_set(&before, &before), edge_set(&after, &before));
    }

    #[test]
    fn keeps_multi_key_pairings_together() {
        // Two keys between the same pair of relations must stay matched to
        // their own partner, not transposed with each other. A diamond reaches
        // this twice over: the closing join carries one key pair per path, and
        // `p0` here is joined on two columns at once.
        let p0 = scan(&["k1", "k2", "pad"]);
        let a0 = scan(&["k1", "k2"]);
        let b0 = scan(&["b0_p0", "b0_p1"]);
        let p1 = scan(&["p1_a", "p1_b"]);

        let left = join(p0, a0, &[(0, 0), (1, 1)]);
        let left = join(left, b0, &[(2, 0)]);
        // a0's second column is global 4 and b0's is global 6.
        let original = join(left, p1, &[(4, 0), (6, 1)]);

        let (rewritten, _) = rewrite(&original, &reordered_diamond());

        assert_eq!(key_pairs(&original), key_pairs(&rewritten));
        assert_eq!(original.schema(), rewritten.schema());
    }

    #[test]
    fn emits_no_projection_when_the_order_is_unchanged() {
        // A rewrite that happens to reproduce the original column order needs
        // no projection, and `apply_projection` should hand the tree back
        // untouched rather than wrapping it in an identity permutation.
        let graph = JoinGraph::try_new(&diamond()).expect("a reorderable clump");
        let plan = Arc::clone(&graph.relations()[0]);
        // The permutation must cover exactly this plan's columns; a shorter one
        // is a pruning projection and does need emitting.
        let identity = (0..plan.schema().fields().len()).collect::<Vec<_>>();

        let unchanged = apply_projection(Rewritten {
            plan: Arc::clone(&plan),
            projection: identity,
        })
        .expect("an identity permutation needs no projection");

        assert!(Arc::ptr_eq(&plan, &unchanged));
    }

    #[test]
    fn emits_a_projection_when_columns_were_pruned() {
        // A projection shorter than the tree selects a subset, so skipping it
        // would hand back columns the clump's output never had.
        let plan = diamond();
        let full_width = plan.schema().fields().len();
        assert!(full_width > 2);

        let projected = apply_projection(Rewritten {
            plan: Arc::clone(&plan),
            projection: vec![0, 1],
        })
        .expect("a pruning projection applies to the top join");

        assert_eq!(projected.schema().fields().len(), 2);
    }
}
