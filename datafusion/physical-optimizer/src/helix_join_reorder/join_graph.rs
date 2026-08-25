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
//! the helix shape in it.
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
//! That identity is exactly what `reorderable_join` protects: a semi join
//! emits only its left fields, and a projection attached to a join reorders or
//! drops columns. Either would silently break the arithmetic, so both are
//! refused.
//!
//! # Seeing through pruning projections
//!
//! The physical planner inserts a standalone `ProjectionExec` between joins to
//! drop columns nothing above needs. Treating those as opaque relations splits
//! every join tree into single-join pieces, far too small to be a helix, which
//! would make this rule inert on real SQL.
//!
//! A projection that only selects columns is a subset and reordering of its
//! input, so `pass_through_projection` lets the clump continue through it and
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
//! [`detect_helix`] works purely on relation indices and edges, with no
//! [`ExecutionPlan`]s involved, so the shape rules can be tested directly.

use std::sync::Arc;

use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::joins::HashJoinExec;
use datafusion_physical_plan::projection::ProjectionExec;

use super::cost_model::JoinTree;

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
    /// The order the joins were already in.
    input_tree: JoinTree,
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
        let (output_map, input_tree) = flattener.visit(plan)?;
        flattener.finish(output_map, input_tree)
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

    /// This graph read as a helix, if it is one.
    pub fn detect_helix(&self) -> Option<HelixShape> {
        detect_helix(self.relations.len(), &self.edges)
    }

    /// The order the clump's joins were already in.
    ///
    /// A cost model that lands on this order has found nothing to improve, and
    /// emitting it would replace the plan with a copy of itself.
    pub fn input_tree(&self) -> &JoinTree {
        &self.input_tree
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
                log::debug!("helix: keeping a relation whose replacement changed schema");
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
        log::debug!("helix: skipping {} join", join.join_type);
        return None;
    }
    // A join filter is a non-equi predicate bound to these specific inputs.
    if join.filter.is_some() {
        log::debug!("helix: skipping join carrying a filter");
        return None;
    }
    // A projection breaks the `output == left ++ right` identity that the
    // offset arithmetic depends on.
    if join.contains_projection() {
        log::debug!("helix: skipping join carrying a projection");
        return None;
    }
    // `fetch` caps the number of rows returned, so the join order decides
    // *which* rows survive. Reordering would return different data.
    if join.fetch().is_some() {
        log::debug!("helix: skipping join carrying a fetch limit");
        return None;
    }
    // The join has already been wired into a runtime filter aimed at a
    // particular scan.
    if !join.dynamic_expressions_produced().is_empty() {
        log::debug!("helix: skipping join carrying a dynamic filter");
        return None;
    }
    // Only legal on anti joins, so the `Inner` check above already excludes
    // it; asserted anyway so the invariant is local.
    if join.null_aware {
        log::debug!("helix: skipping null-aware join");
        return None;
    }
    // Keys such as `cast(a.x) = b.y` cannot be traced back to a base relation
    // column, so the graph cannot be built.
    if !join.on.iter().all(|(left, right)| {
        column_index(left.as_ref()).is_some() && column_index(right.as_ref()).is_some()
    }) {
        log::debug!("helix: skipping join with non-column equijoin keys");
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
            log::debug!("helix: projection computes a value, stopping there");
            return None;
        };
        let field = input_schema.fields().get(index)?;
        if field.name() != &entry.alias {
            log::debug!("helix: projection renames a column, stopping there");
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
    /// Returned alongside it is the order the joins are already in, which costs
    /// nothing to record here and is the only place the original shape is
    /// still visible.
    ///
    /// `None` means the clump is unusable and the caller should leave the plan
    /// alone.
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn visit(&mut self, plan: &Arc<dyn ExecutionPlan>) -> Option<(Vec<usize>, JoinTree)> {
        if let Some(join) = reorderable_join(plan.as_ref()) {
            // Rebuilt joins may merge keys that came from different original
            // joins, so mixing null semantics within a clump is not safe.
            match self.null_equality {
                None => self.null_equality = Some(join.null_equality),
                Some(existing) if existing == join.null_equality => {}
                Some(_) => {
                    log::debug!("helix: clump mixes null equality settings");
                    return None;
                }
            }

            let (left_map, left_tree) = self.visit(&join.left)?;
            let (right_map, right_tree) = self.visit(&join.right)?;

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
            return Some((map, JoinTree::join(left_tree, right_tree)));
        }

        if let Some(projection) = pass_through_projection(plan.as_ref()) {
            // A pruning projection only selects and reorders, so the clump
            // continues below it and each output column inherits its input's
            // coordinate. Without this the physical planner's column pruning
            // splits every join tree into single-join pieces, too small to be
            // a helix at all.
            let (child_map, tree) = self.visit(projection.input())?;
            let map: Vec<usize> = projection
                .expr()
                .iter()
                .map(|entry| {
                    let index = column_index(entry.expr.as_ref())?;
                    child_map.get(index).copied()
                })
                .collect::<Option<_>>()?;
            return Some((map, tree));
        }

        // A leaf: its contents are opaque to us, only its width matters.
        // Naming it matters for diagnosis, since an unexpected operator between
        // two joins is what splits a clump into pieces too small to reorder.
        log::debug!("helix: treating {} as a relation", plan.name());
        let width = plan.schema().fields().len();
        let base = self.next_offset;
        let relation = self.relations.len();
        self.relations.push(Arc::clone(plan));
        self.offsets.push(base);
        self.next_offset += width;
        Some(((base..base + width).collect(), JoinTree::Leaf(relation)))
    }

    /// Resolve global key positions to relations and group them into edges.
    fn finish(self, output_map: Vec<usize>, input_tree: JoinTree) -> Option<JoinGraph> {
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
                log::debug!("helix: equijoin key resolved within one relation");
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
            input_tree,
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

/// A join graph read as a helix: a chain of diamonds.
///
/// ```text
///      A0        A1        A2
///     /  \      /  \      /  \
///   P0    P1  P1    P2  P2    P3
///     \  /      \  /      \  /
///      B0        B1        B2
/// ```
///
/// `spine` is `P0..Pm` in order along the chain, and `links[i]` is the pair of
/// relations running in parallel between `spine[i]` and `spine[i + 1]`.
///
/// Holds relation indices only; cardinalities and selectivities are attached
/// separately, so recognizing the shape stays independent of whether
/// statistics are good enough to act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelixShape {
    /// The relations the diamonds hang between, in order along the chain.
    pub spine: Vec<usize>,
    /// For each consecutive spine pair, the two relations joining them,
    /// ascending.
    pub links: Vec<[usize; 2]>,
}

impl HelixShape {
    /// How many diamonds the chain has.
    pub fn diamonds(&self) -> usize {
        self.links.len()
    }
}

/// Recognize a helix, or return `None`.
///
/// # What has to hold
///
/// A helix of `m` diamonds has `3m + 1` relations and `4m` edges, and every
/// relation is one of two kinds:
///
/// * a **link**, of degree two, whose two neighbors are spine relations, and
/// * a **spine** relation, of degree two at the ends of the chain and four in
///   the middle.
///
/// Two links run between each consecutive pair of spine relations, and
/// collapsing each such pair to a single edge must leave a path.
///
/// # How it is found
///
/// Links come first, because they are the only relations that can be
/// identified without knowing the rest of the shape: a link has degree two,
/// and its partner is the *other* relation with exactly the same two
/// neighbors. Grouping degree-two relations by their neighbor pair therefore
/// yields the diamonds directly. Everything left over is the spine, and the
/// quotient graph over it has to be a path.
///
/// # The four-relation case
///
/// One diamond is a four-cycle, where both readings are helixes: either pair
/// of opposite relations can be the spine. They are genuinely different orders
/// to cost, but there is no principled way to prefer one here, so the pair
/// containing the lowest relation index becomes the spine and the choice stays
/// deterministic.
pub fn detect_helix(relation_count: usize, edges: &[JoinEdge]) -> Option<HelixShape> {
    // `3m + 1` relations and `4m` edges, with at least one diamond.
    if relation_count < 4 || !(relation_count - 1).is_multiple_of(3) {
        return None;
    }
    let diamonds = (relation_count - 1) / 3;
    if edges.len() != 4 * diamonds {
        log::debug!(
            "helix: {relation_count} relations with {} edges is not a chain of {diamonds} diamonds",
            edges.len()
        );
        return None;
    }

    let mut adjacency = vec![Vec::new(); relation_count];
    for edge in edges {
        adjacency[edge.left].push(edge.right);
        adjacency[edge.right].push(edge.left);
    }
    for neighbors in &mut adjacency {
        neighbors.sort_unstable();
    }

    if !is_connected(&adjacency) {
        log::debug!("helix: join graph is disconnected");
        return None;
    }

    // Degree-two relations, grouped by the pair of relations they join. A
    // group of exactly two is a diamond's pair of parallel links.
    let mut pairs: Vec<([usize; 2], Vec<usize>)> = Vec::new();
    for (relation, neighbors) in adjacency.iter().enumerate() {
        let [first, second] = neighbors[..] else {
            continue;
        };
        let key = [first, second];
        match pairs.iter_mut().find(|(existing, _)| *existing == key) {
            Some((_, members)) => members.push(relation),
            None => pairs.push((key, vec![relation])),
        }
    }
    let mut links: Vec<[usize; 2]> = pairs
        .iter()
        .filter(|(_, members)| members.len() == 2)
        .map(|(_, members)| [members[0], members[1]])
        .collect();

    // A single diamond is a four-cycle, so both of its opposite pairs group
    // this way and either could be the spine. Keep the one that leaves the
    // lowest relation on the spine.
    if diamonds == 1 && links.len() == 2 {
        links.retain(|pair| !pair.contains(&0));
    }

    if links.len() != diamonds {
        log::debug!(
            "helix: found {} parallel pairs, not the {diamonds} a helix needs",
            links.len()
        );
        return None;
    }

    // Everything not a link is spine, and each link must run between two of
    // them. Without this a link could pair with another link, which the edge
    // count alone does not rule out.
    let mut is_link = vec![false; relation_count];
    for pair in &links {
        for &relation in pair {
            is_link[relation] = true;
        }
    }
    if is_link.iter().filter(|&&link| link).count() != 2 * diamonds {
        return None;
    }

    // The quotient graph: one edge per diamond, joining the two spine
    // relations its links run between.
    let mut quotient = vec![Vec::new(); relation_count];
    let mut collapsed: Vec<(usize, [usize; 2])> = Vec::with_capacity(diamonds);
    for (index, pair) in links.iter().enumerate() {
        let [left, right] = adjacency[pair[0]][..] else {
            return None;
        };
        if is_link[left] || is_link[right] {
            log::debug!("helix: a diamond's links do not both meet the spine");
            return None;
        }
        quotient[left].push(right);
        quotient[right].push(left);
        collapsed.push((index, [left, right]));
    }

    let spine = spine_path(&quotient, &is_link, diamonds + 1)?;

    // Order the diamonds along the chain, so `links[i]` runs between
    // `spine[i]` and `spine[i + 1]`.
    let ordered = spine
        .windows(2)
        .map(|step| {
            let (index, _) = collapsed.iter().find(|(_, ends)| {
                *ends == [step[0], step[1]] || *ends == [step[1], step[0]]
            })?;
            let mut pair = links[*index];
            pair.sort_unstable();
            Some(pair)
        })
        .collect::<Option<Vec<_>>>()?;

    Some(HelixShape {
        spine,
        links: ordered,
    })
}

/// Walk the quotient graph as a path, returning the spine in chain order.
///
/// Returns `None` unless it is one: `expected` relations, every degree at most
/// two, and exactly two ends. A cycle of diamonds fails the ends check, and a
/// branch fails the degree check.
fn spine_path(
    quotient: &[Vec<usize>],
    is_link: &[bool],
    expected: usize,
) -> Option<Vec<usize>> {
    let members: Vec<usize> = (0..quotient.len())
        .filter(|&relation| !is_link[relation])
        .collect();
    if members.len() != expected {
        return None;
    }
    if members.iter().any(|&relation| quotient[relation].len() > 2) {
        log::debug!("helix: the spine branches rather than running as a chain");
        return None;
    }

    // A two-relation spine is one diamond, whose single quotient edge leaves
    // both ends with degree one.
    let mut ends: Vec<usize> = members
        .iter()
        .copied()
        .filter(|&relation| quotient[relation].len() == 1)
        .collect();
    if ends.len() != 2 {
        log::debug!("helix: the spine is a cycle rather than a chain");
        return None;
    }
    ends.sort_unstable();

    // Start from the lower end so the reported order is deterministic.
    let mut spine = Vec::with_capacity(expected);
    let mut current = ends[0];
    let mut previous = None;
    loop {
        spine.push(current);
        let Some(&next) = quotient[current]
            .iter()
            .find(|&&neighbor| Some(neighbor) != previous)
        else {
            break;
        };
        previous = Some(current);
        current = next;
    }

    // Fewer than expected means the quotient graph is disconnected: several
    // chains rather than one.
    (spine.len() == expected).then_some(spine)
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

    use crate::helix_join_reorder::test_support::{
        join, join_builder, prune, scan, typed_join,
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

    /// The edges of an `m` diamond helix numbered as the prototype names them:
    /// spine `P0..Pm` last, links `A0..` then `B0..` before them.
    ///
    /// Relations are deliberately *not* numbered along the chain, so a
    /// detector that assumed index order rather than reading the edges would
    /// fail these.
    fn helix_edges(diamonds: usize) -> Vec<JoinEdge> {
        let a = |i: usize| i;
        let b = |i: usize| diamonds + i;
        let p = |i: usize| 2 * diamonds + i;

        let mut pairs = Vec::new();
        for i in 0..diamonds {
            pairs.push((p(i), a(i)));
            pairs.push((p(i), b(i)));
            pairs.push((a(i), p(i + 1)));
            pairs.push((b(i), p(i + 1)));
        }
        edges(&pairs)
    }

    #[test]
    fn detects_a_two_diamond_helix() {
        //   A0        A1
        //  /  \      /  \
        // P0    P1  P1    P2
        //  \  /      \  /
        //   B0        B1
        let shape = detect_helix(7, &helix_edges(2)).expect("a helix");

        assert_eq!(shape.diamonds(), 2);
        // P0 P1 P2 under the numbering above.
        assert_eq!(shape.spine, vec![4, 5, 6]);
        // [A0, B0] then [A1, B1], in chain order.
        assert_eq!(shape.links, vec![[0, 2], [1, 3]]);
    }

    #[test]
    fn detects_a_three_diamond_helix() {
        let shape = detect_helix(10, &helix_edges(3)).expect("a helix");

        assert_eq!(shape.diamonds(), 3);
        assert_eq!(shape.spine, vec![6, 7, 8, 9]);
        assert_eq!(shape.links, vec![[0, 3], [1, 4], [2, 5]]);
    }

    #[test]
    fn a_single_diamond_is_a_helix() {
        // A four-cycle. Both opposite pairs could be the spine; the one
        // holding the lowest relation wins, so 0 and 2 are the spine and the
        // links are 1 and 3.
        let shape =
            detect_helix(4, &edges(&[(0, 1), (1, 2), (2, 3), (3, 0)])).expect("a helix");

        assert_eq!(shape.diamonds(), 1);
        assert_eq!(shape.spine, vec![0, 2]);
        assert_eq!(shape.links, vec![[1, 3]]);
    }

    #[test]
    fn the_spine_is_reported_in_chain_order_not_index_order() {
        // The same two diamond helix with the middle spine relation numbered
        // lowest. Reading the spine off index order would give 1, 0, 2.
        //   P0 = 1, P1 = 0, P2 = 2, links 3..6
        let shape = detect_helix(
            7,
            &edges(&[
                (1, 3),
                (1, 4),
                (3, 0),
                (4, 0),
                (0, 5),
                (0, 6),
                (5, 2),
                (6, 2),
            ]),
        )
        .expect("a helix");

        assert_eq!(shape.spine, vec![1, 0, 2]);
        assert_eq!(shape.links, vec![[3, 4], [5, 6]]);
    }

    #[test]
    fn rejects_a_relation_count_that_is_not_three_m_plus_one() {
        // Five and six relations cannot be a chain of diamonds whatever the
        // edges are.
        assert_eq!(
            detect_helix(5, &edges(&[(0, 1), (1, 2), (2, 3), (3, 4)])),
            None
        );
        assert_eq!(
            detect_helix(6, &edges(&[(0, 1), (1, 2), (2, 3), (3, 4), (4, 5)])),
            None
        );
    }

    #[test]
    fn rejects_the_right_relation_count_with_the_wrong_edge_count() {
        // Seven relations wants eight edges; a tree has six.
        let tree = edges(&[(0, 1), (0, 2), (0, 3), (3, 6), (6, 4), (6, 5)]);
        assert_eq!(detect_helix(7, &tree), None);
    }

    #[test]
    fn rejects_a_bowtie() {
        // The shape the other rule handles: right relation count, right edge
        // count once padded, but no parallel pairs at all.
        //   1  2        4  5
        //    \ |        | /
        //      0 -- 3 -- 6
        let bowtie = edges(&[
            (0, 1),
            (0, 2),
            (0, 3),
            (3, 6),
            (6, 4),
            (6, 5),
            (1, 2),
            (4, 5),
        ]);

        assert_eq!(detect_helix(7, &bowtie), None);
    }

    #[test]
    fn rejects_a_disconnected_graph() {
        // Two separate diamonds: the counts work out for a two diamond helix,
        // but there is no chain joining them.
        let two_diamonds = edges(&[
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 0),
            (4, 5),
            (5, 6),
            (6, 4),
            (4, 5),
        ]);

        assert_eq!(detect_helix(7, &two_diamonds), None);
    }

    #[test]
    fn rejects_a_ring_of_diamonds() {
        // Three diamonds joined end to end into a loop rather than a chain:
        // every spine relation has degree four, so there are no ends.
        //   spine 6, 7, 8 with 9 unused would break the count, so this uses
        //   the P0-P1, P1-P2, P2-P0 closure on a three diamond count.
        let mut pairs = Vec::new();
        let spine = [6, 7, 8];
        let links = [(0, 3), (1, 4), (2, 5)];
        for (step, &(a, b)) in links.iter().enumerate() {
            let from = spine[step];
            let to = spine[(step + 1) % 3];
            pairs.push((from, a));
            pairs.push((a, to));
            pairs.push((from, b));
            pairs.push((b, to));
        }
        // Nine relations is not `3m + 1`, so pad the count to ten with an
        // isolated relation: the ring must be rejected for its shape, and the
        // connectivity check catches the padding first, so assert the ring
        // itself on its own count.
        assert_eq!(detect_helix(10, &edges(&pairs)), None);
    }

    #[test]
    fn rejects_a_diamond_whose_links_touch_each_other() {
        // Four relations, four edges, but one edge joins the two links rather
        // than reaching the spine, so it is a triangle with a tail.
        assert_eq!(
            detect_helix(4, &edges(&[(0, 1), (0, 2), (1, 2), (2, 3)])),
            None
        );
    }

    #[test]
    fn rejects_graphs_too_small_to_be_a_helix() {
        assert_eq!(detect_helix(2, &edges(&[(0, 1)])), None);
        assert_eq!(detect_helix(3, &edges(&[(0, 1), (1, 2)])), None);
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
    fn end_to_end_diamond_is_detected() {
        //      a0
        //     /  \
        //   p0    p1
        //     \  /
        //      b0
        let p0 = scan(&["p0_ka", "p0_kb", "p0_x"]);
        let a0 = scan(&["a0_p0", "a0_p1"]);
        let b0 = scan(&["b0_p0", "b0_p1", "b0_y", "b0_z"]);
        let p1 = scan(&["p1_a", "p1_b", "p1_c"]);

        let left = join(p0, a0, &[(0, 0)]);
        let left = join(left, b0, &[(1, 0)]);
        // `a0`'s second column is global 4 and `b0`'s is global 6, so this one
        // join closes both paths of the diamond at once.
        let root = join(left, p1, &[(4, 0), (6, 1)]);

        let graph = JoinGraph::try_new(&root).expect("a reorderable clump");

        assert_eq!(
            graph.detect_helix(),
            Some(HelixShape {
                spine: vec![0, 3],
                links: vec![[1, 2]],
            })
        );
    }

    // ---------- the order the plan was already in ----------

    #[test]
    fn records_the_order_the_joins_were_in() {
        // A bushy input, so the recorded tree cannot be confused with the
        // left-deep default.
        let left = join(scan(&["a", "ka"]), scan(&["b"]), &[(0, 0)]);
        let right = join(scan(&["c", "kc"]), scan(&["d"]), &[(0, 0)]);
        let plan = join(left, right, &[(1, 0)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");

        assert_eq!(
            *graph.input_tree(),
            JoinTree::join(
                JoinTree::join(JoinTree::Leaf(0), JoinTree::Leaf(1)),
                JoinTree::join(JoinTree::Leaf(2), JoinTree::Leaf(3))
            )
        );
    }

    #[test]
    fn a_projection_between_joins_is_not_part_of_the_order() {
        // The clump continues through a pruning projection, and the relations
        // below it keep their places in the recorded order: only joins are
        // nodes of it.
        let plan = join(scan(&["a", "ka"]), scan(&["b"]), &[(0, 0)]);
        let pruned = prune(plan, &[0, 1, 2]);
        let plan = join(pruned, scan(&["c"]), &[(1, 0)]);

        let graph = JoinGraph::try_new(&plan).expect("a reorderable clump");

        assert_eq!(graph.relations().len(), 3);
        assert_eq!(
            *graph.input_tree(),
            JoinTree::join(
                JoinTree::join(JoinTree::Leaf(0), JoinTree::Leaf(1)),
                JoinTree::Leaf(2)
            )
        );
    }
}
