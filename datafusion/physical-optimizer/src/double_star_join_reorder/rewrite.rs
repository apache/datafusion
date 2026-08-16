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

use super::cost_model::DoubleStarPlan;
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

/// Build the join tree described by `plan`.
///
/// Returns `None` if the plan and the graph disagree — a join step with no
/// connecting predicate, or a relation left unplaced. Both would mean a bug
/// rather than an unsupported query, and both are refused rather than emitted.
pub fn rebuild(graph: &JoinGraph, plan: &DoubleStarPlan) -> Option<Rewritten> {
    let relation_count = graph.relations().len();
    let leaf = |relation: usize| {
        Subtree::leaf(
            relation,
            Arc::clone(&graph.relations()[relation]),
            relation_count,
        )
    };

    let mut left = leaf(plan.left_hub);
    for &relation in &plan.left_prefix {
        left = join(left, leaf(relation), graph)?;
    }

    let mut right = leaf(plan.right_hub);
    for &relation in &plan.right_prefix {
        right = join(right, leaf(relation), graph)?;
    }

    // The big merge, across the central relation's edge to the other hub.
    let mut top = join(left, right, graph)?;

    for &relation in &plan.leftovers {
        top = join(top, leaf(relation), graph)?;
    }

    // Every relation placed exactly once, and no columns gained or lost.
    if top.offsets.iter().any(Option::is_none) || top.width != graph.width() {
        log::debug!("double star: rebuilt tree does not cover every relation");
        return None;
    }

    // Relations were flattened left to right with contiguous offsets, so
    // walking them in order walks the original schema in order.
    let mut projection = Vec::with_capacity(top.width);
    for (relation, node) in graph.relations().iter().enumerate() {
        let base = top.offset_of(relation)?;
        for column in 0..node.schema().fields().len() {
            projection.push(base + column);
        }
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

    if projection
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
        .inspect_err(|error| {
            log::debug!("double star: could not apply projection: {error}")
        })
        .ok()
}

/// Join two subtrees on every graph edge that crosses between them.
fn join(left: Subtree, right: Subtree, graph: &JoinGraph) -> Option<Subtree> {
    let on = crossing_keys(&left, &right, graph)?;
    if on.is_empty() {
        // Without a predicate this would silently become a cross join: the
        // same columns, catastrophically more rows.
        log::debug!("double star: no predicate connects two subtrees");
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
        .inspect_err(|error| log::debug!("double star: could not build join: {error}"))
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

    use crate::double_star_join_reorder::cost_model::{
        DoubleStar, Relation, Spoke, optimal_double_star,
    };
    use crate::double_star_join_reorder::join_graph::{DoubleStarShape, JoinEdge};
    use crate::double_star_join_reorder::statistics::GraphStatistics;

    use crate::double_star_join_reorder::test_support::{bowtie, join, scan};
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

    /// Rebuild `plan` under a chosen order, returning the projected tree.
    fn rewrite(plan: &Arc<dyn ExecutionPlan>) -> (Arc<dyn ExecutionPlan>, Rewritten) {
        let graph = JoinGraph::try_new(plan).expect("a reorderable clump");
        let shapes = graph.detect_double_stars();
        assert!(!shapes.is_empty(), "expected a double star");

        // EmptyExec reports zero rows, so drive the order from an explicit
        // cost model input rather than statistics.
        let star = synthetic_star(&shapes[0]);
        let chosen = optimal_double_star(&star).expect("a usable order");

        let rewritten = rebuild(&graph, &chosen).expect("a rebuildable order");
        let projected = apply_projection(rewritten.clone())
            .expect("projection applies to the top join");
        (projected, rewritten)
    }

    /// Weights and selectivities chosen so the cheapest order differs from the
    /// order the input tree happens to be in.
    fn synthetic_star(shape: &DoubleStarShape) -> DoubleStar {
        let weight = |relation: usize| (relation as f64 + 1.0) * 100.0;
        let spoke = |relation: usize| {
            Spoke::new(relation, weight(relation), 1.0 / (relation as f64 + 2.0))
        };

        DoubleStar {
            hub_a: Relation::new(shape.hub_a, weight(shape.hub_a)),
            hub_b: Relation::new(shape.hub_b, weight(shape.hub_b)),
            central: Relation::new(shape.central, weight(shape.central)),
            sel_a: 0.01,
            sel_b: 0.02,
            spokes_a: shape.spokes_a.iter().copied().map(spoke).collect(),
            spokes_b: shape.spokes_b.iter().copied().map(spoke).collect(),
        }
    }

    #[test]
    fn preserves_the_output_schema() {
        let original = bowtie();
        let (rewritten, _) = rewrite(&original);

        assert_eq!(rewritten.schema(), original.schema());
    }

    /// Flattening is the inverse of rebuilding, so re-flattening the result
    /// must recover exactly the edges we started from. Any index remapped
    /// wrongly would resolve to a different relation or column here.
    #[test]
    fn re_flattening_recovers_the_same_edges() {
        let original = bowtie();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");

        let (_, rewritten) = rewrite(&original);
        // The un-projected tree, since a projection would stop flattening.
        let after =
            JoinGraph::try_new(&rewritten.plan).expect("still a reorderable clump");

        assert_eq!(edge_set(&before, &before), edge_set(&after, &before));
    }

    #[test]
    fn places_every_relation_exactly_once() {
        let original = bowtie();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original);
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
        let original = bowtie();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original);
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
        let (_, rewritten) = rewrite(&bowtie());

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

    /// Statistics-driven end to end, using relations whose statistics make the
    /// spoke ordering unambiguous.
    #[test]
    fn rewrites_from_real_statistics() {
        let plan = bowtie();
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let shapes = graph.detect_double_stars();

        // EmptyExec reports exactly zero rows, which is a usable estimate.
        let statistics =
            GraphStatistics::try_new(&graph, None).expect("statistics are usable");
        let star = statistics
            .double_star(&graph, &shapes[0])
            .expect("every edge exists");
        let chosen = optimal_double_star(&star).expect("a usable order");

        let rewritten = rebuild(&graph, &chosen).expect("a rebuildable order");
        let projected = apply_projection(rewritten).expect("projection applies");

        assert_eq!(projected.schema(), plan.schema());
    }

    #[test]
    fn refuses_a_plan_that_leaves_a_relation_unplaced() {
        let plan = bowtie();
        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");
        let shapes = graph.detect_double_stars();
        let star = synthetic_star(&shapes[0]);
        let mut chosen = optimal_double_star(&star).expect("a usable order");

        // Drop a relation from the order.
        chosen.leftovers.clear();
        chosen.left_prefix.pop();

        assert!(rebuild(&graph, &chosen).is_none());
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

    /// A double star in which a spoke appears to the *left* of its hub.
    ///
    /// Edges are canonicalized so `left < right`, but which endpoint ends up on
    /// a join's build side depends on the order chosen, not on numbering. When
    /// the hub is joined first, this arrangement puts `edge.left` on the
    /// join's right side, which is the only way to reach the key swap in
    /// `crossing_keys`. The relations are `a1, hub_a, central, hub_b, b1` in
    /// that order, forming the path `a1 - hub_a - central - hub_b - b1`.
    fn spoke_before_hub_star() -> Arc<dyn ExecutionPlan> {
        let a1 = scan(&["a1_k"]);
        let hub_a = scan(&["ha_k", "ha_s1", "ha_s2"]);
        let central = scan(&["c_ka", "c_kb"]);
        let hub_b = scan(&["hb_k", "hb_s1"]);
        let b1 = scan(&["b1_k"]);

        // a1.a1_k = hub_a.ha_s1
        let left = join(a1, hub_a, &[(0, 1)]);
        // hub_a.ha_k sits at index 1, after a1's single column.
        let left = join(left, central, &[(1, 0)]);
        let right = join(hub_b, b1, &[(0, 0)]);
        // central.c_kb is the last of the six columns on the left.
        join(left, right, &[(5, 0)])
    }

    #[test]
    fn remaps_a_flipped_edge() {
        let original = spoke_before_hub_star();
        let (rewritten, _) = rewrite(&original);

        // The same columns are paired after the rewrite. A key pair that was
        // not swapped alongside its edge would either name different columns
        // here or read past the end of the narrower relation.
        assert_eq!(key_pairs(&original), key_pairs(&rewritten));
        assert_eq!(original.schema(), rewritten.schema());
    }

    #[test]
    fn a_flipped_edge_still_round_trips_through_flattening() {
        let original = spoke_before_hub_star();
        let before = JoinGraph::try_new(&original).expect("a reorderable clump");
        let (_, rewritten) = rewrite(&original);
        let after = JoinGraph::try_new(&rewritten.plan).expect("still reorderable");

        assert_eq!(edge_set(&before, &before), edge_set(&after, &before));
    }

    #[test]
    fn keeps_multi_key_pairings_together() {
        // Two keys between the same pair of relations must stay matched to
        // their own partner, not transposed with each other.
        let hub_a = scan(&["k1", "k2", "pad"]);
        let a1 = scan(&["k1", "k2"]);
        let central = scan(&["c_ka", "c_kb"]);
        let hub_b = scan(&["hb_k", "hb_s"]);
        let b1 = scan(&["b1_k"]);

        let left = join(hub_a, a1, &[(0, 0), (1, 1)]);
        let left = join(left, central, &[(0, 0)]);
        let right = join(hub_b, b1, &[(0, 0)]);
        let original = join(left, right, &[(6, 0)]);

        let (rewritten, _) = rewrite(&original);

        assert_eq!(key_pairs(&original), key_pairs(&rewritten));
        assert_eq!(original.schema(), rewritten.schema());
    }

    #[test]
    fn emits_no_projection_when_the_order_is_unchanged() {
        // A rewrite that happens to reproduce the original column order needs
        // no projection, and `apply_projection` should hand the tree back
        // untouched rather than wrapping it in an identity permutation.
        let graph = JoinGraph::try_new(&bowtie()).expect("a reorderable clump");
        let identity = (0..graph.width()).collect::<Vec<_>>();
        let plan = Arc::clone(&graph.relations()[0]);

        let unchanged = apply_projection(Rewritten {
            plan: Arc::clone(&plan),
            projection: identity,
        })
        .expect("an identity permutation needs no projection");

        assert!(Arc::ptr_eq(&plan, &unchanged));
    }
}
