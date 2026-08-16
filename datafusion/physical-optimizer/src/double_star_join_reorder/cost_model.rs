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

//! Cost model for double star join graphs.
//!
//! # The cost being minimized
//!
//! Every relation carries a `weight` (its cardinality) and every join edge a
//! `selectivity` (the fraction of the cross product that survives). Their
//! product is the edge's **fanout**: the factor by which absorbing that
//! relation multiplies the running intermediate size.
//!
//! Fanout below 1 shrinks the intermediate result, above 1 grows it. Joining
//! a chain of relations onto a hub therefore costs
//!
//! ```text
//! cost = sum over steps of (running size before the step * fanout of the step)
//! ```
//!
//! and cheapest-fanout-first minimizes it. For a single star that ordering is
//! optimal, which is what [`chain_cost_and_size`] implements.
//!
//! # Why the double star needs a search
//!
//! The two stars are joined through the central relation, and that **big
//! merge** costs `left size * right size * selectivity`. So a spoke with
//! fanout below 1 is worth absorbing *before* the merge (it shrinks the merge
//! inputs), while a spoke with fanout above 1 is better deferred until
//! *after*. Each side therefore has a split point, and
//! [`Orientation::solve`] evaluates every combination of the two.
//!
//! Because the central relation can be attached to either hub, and that choice
//! changes the cost, [`optimal_double_star`] runs the search in both
//! orientations and keeps the cheaper one.
//!
//! # Relationship to the reference implementation
//!
//! This is a port of a Python prototype. Floating point operations are kept in
//! the same order as the reference so that the two agree bit for bit; the unit
//! tests assert against values produced by that prototype. Deviations that
//! adapt the model to running inside an optimizer are called out in comments
//! marked `Deviation:`.

/// Upper bound on spokes per hub.
///
/// Deviation: the reference implementation has no bound because a human types
/// the inputs. Here the search is `O(n * m * (n + m))` over spoke counts taken
/// from a user query, so it is capped to keep planning time predictable. Real
/// double stars have single-digit spoke counts; anything beyond this is not a
/// shape worth optimizing.
pub const MAX_SPOKES_PER_HUB: usize = 64;

/// A relation with no join ordering freedom of its own: a hub or the central
/// relation. Its selectivity lives on the edges that reach it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Relation {
    /// Caller-assigned identifier, echoed back in [`DoubleStarPlan`].
    ///
    /// Deviation: the reference implementation names relations with strings.
    /// Here callers pass an index into their own relation list, which keeps
    /// the model allocation-free and makes the result directly usable as a
    /// lookup key when rebuilding a plan.
    pub id: usize,
    /// Cardinality of the relation.
    pub weight: f64,
}

impl Relation {
    /// Create a relation with the given id and cardinality.
    pub fn new(id: usize, weight: f64) -> Self {
        Self { id, weight }
    }
}

/// A relation joined to exactly one hub, together with the selectivity of the
/// edge connecting it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Spoke {
    /// Caller-assigned identifier, echoed back in [`DoubleStarPlan`].
    pub id: usize,
    /// Cardinality of the spoke relation.
    pub weight: f64,
    /// Fraction of the cross product with the hub that survives the join.
    pub selectivity: f64,
}

impl Spoke {
    /// Create a spoke with the given id, cardinality and edge selectivity.
    pub fn new(id: usize, weight: f64, selectivity: f64) -> Self {
        Self {
            id,
            weight,
            selectivity,
        }
    }

    /// The factor by which absorbing this spoke multiplies the running
    /// intermediate size. Below 1 shrinks, above 1 grows.
    pub fn fanout(&self) -> f64 {
        self.weight * self.selectivity
    }
}

/// A double star join graph: two hubs with their spokes, bridged by a central
/// relation.
#[derive(Debug, Clone, PartialEq)]
pub struct DoubleStar {
    /// First hub.
    pub hub_a: Relation,
    /// Second hub.
    pub hub_b: Relation,
    /// The relation bridging the two hubs.
    pub central: Relation,
    /// Selectivity of the `hub_a`-to-`central` edge.
    pub sel_a: f64,
    /// Selectivity of the `hub_b`-to-`central` edge.
    pub sel_b: f64,
    /// Relations joined only to `hub_a`.
    pub spokes_a: Vec<Spoke>,
    /// Relations joined only to `hub_b`.
    pub spokes_b: Vec<Spoke>,
}

/// The chosen join order, as a sequence of relation ids.
///
/// Reading it back as a plan:
///
/// ```text
/// left  = left_hub;  for r in left_prefix  { left  = join(left, r) }
/// right = right_hub; for r in right_prefix { right = join(right, r) }
/// top   = join(left, right)                 // the big merge
///                    for r in leftovers    { top   = join(top, r) }
/// ```
///
/// Deviation: the reference implementation returns only the cost and the two
/// split points, which is enough to compare orders but not to build one. This
/// carries the materialized sequence so a caller can emit the join tree
/// directly.
#[derive(Debug, Clone, PartialEq)]
pub struct DoubleStarPlan {
    /// Estimated cost of this order, in rows flowing through joins.
    pub cost: f64,
    /// Hub that absorbs the central relation before the big merge.
    pub left_hub: usize,
    /// Relations joined onto `left_hub` before the merge, in order. Always
    /// contains the central relation, positioned by its own fanout.
    pub left_prefix: Vec<usize>,
    /// The other hub.
    pub right_hub: usize,
    /// Relations joined onto `right_hub` before the merge, in order.
    pub right_prefix: Vec<usize>,
    /// Relations deferred until after the merge, in order. Spokes of both hubs
    /// pool together here because the merge has made them one cluster.
    pub leftovers: Vec<usize>,
}

/// Sort spokes cheapest-fanout-first.
///
/// Deviation: `f64` is not `Ord` in Rust, so ordering goes through
/// [`f64::total_cmp`], which is total even in the presence of NaN and so
/// cannot panic. [`slice::sort_by`] is stable, matching Python's `sorted`, and
/// that stability is load-bearing: ties must resolve identically in both
/// languages for the costs to agree.
fn sort_by_fanout(spokes: &mut [Spoke]) {
    spokes.sort_by(|a, b| a.fanout().total_cmp(&b.fanout()));
}

/// Join `spokes` onto a hub of cardinality `hub_weight`, cheapest fanout
/// first.
///
/// Returns the total cost, the resulting intermediate size, and the order the
/// spokes were joined in.
pub fn chain_cost_and_size(hub_weight: f64, spokes: &[Spoke]) -> (f64, f64, Vec<usize>) {
    let mut sorted = spokes.to_vec();
    sort_by_fanout(&mut sorted);

    let mut cost = 0.0;
    let mut size = hub_weight;
    let mut order = Vec::with_capacity(sorted.len());
    for spoke in &sorted {
        // Kept as two separate multiplications, matching the reference: the
        // rounding of `(size * weight) * selectivity` differs from that of
        // `size * (weight * selectivity)`.
        cost += size * spoke.weight * spoke.selectivity;
        size *= spoke.weight;
        size *= spoke.selectivity;
        order.push(spoke.id);
    }
    (cost, size, order)
}

/// Cost and size of one side of the star for every possible split point.
///
/// Entry `p` describes joining the `p` cheapest spokes onto the hub. When
/// `extra` is supplied (the central relation, on whichever hub owns it) it
/// participates in the fanout ordering rather than being pinned to the end, so
/// it may be absorbed part way through the prefix.
///
/// `sorted_spokes` must already be sorted by fanout.
///
/// Deviation: the reference sorts inside this function; here sorting is
/// hoisted to the caller, which has already sorted. Sorting is idempotent, so
/// the results are unchanged.
fn side_table(
    hub_weight: f64,
    sorted_spokes: &[Spoke],
    extra: Option<Spoke>,
) -> (Vec<f64>, Vec<f64>) {
    let n = sorted_spokes.len();
    let mut cost_table = Vec::with_capacity(n + 1);
    let mut size_table = Vec::with_capacity(n + 1);
    let mut before_merge = Vec::with_capacity(n + 1);

    for p in 0..=n {
        before_merge.clear();
        before_merge.extend_from_slice(&sorted_spokes[..p]);
        if let Some(extra) = extra {
            before_merge.push(extra);
        }
        let (cost, size, _) = chain_cost_and_size(hub_weight, &before_merge);
        cost_table.push(cost);
        size_table.push(size);
    }

    (cost_table, size_table)
}

/// The order deferred spokes are absorbed in after the big merge.
///
/// Both piles pool together because the merge has made them a single cluster,
/// and they are taken cheapest-fanout-first. Concatenating `deferred_a` before
/// `deferred_b` and stable sorting reproduces the reference's tie handling.
fn merged_leftovers(deferred_a: &[Spoke], deferred_b: &[Spoke]) -> Vec<Spoke> {
    let mut leftovers = Vec::with_capacity(deferred_a.len() + deferred_b.len());
    leftovers.extend_from_slice(deferred_a);
    leftovers.extend_from_slice(deferred_b);
    sort_by_fanout(&mut leftovers);
    leftovers
}

/// Multiplier applied to the post-merge size to price the merge plus every
/// deferred spoke.
///
/// The leading `1.0` is the merge itself; each later term is one deferred
/// spoke joined onto the by-then-larger intermediate.
fn leftover_multiplier(deferred_a: &[Spoke], deferred_b: &[Spoke]) -> f64 {
    let leftovers = merged_leftovers(deferred_a, deferred_b);

    let mut w = 1.0;
    let mut running = 1.0;
    for spoke in &leftovers {
        // Reference grouping: `running * (weight * selectivity)`. Note this
        // associates differently from `chain_cost_and_size` above, which is
        // intentional.
        running *= spoke.weight * spoke.selectivity;
        w += running;
    }
    w
}

/// The best split points found for one assignment of the central relation.
#[derive(Debug, Clone, Copy)]
struct SideSolution {
    cost: f64,
    /// Number of left-hub spokes absorbed before the merge.
    p: usize,
    /// Number of right-hub spokes absorbed before the merge.
    q: usize,
}

/// One assignment of the central relation to a hub.
///
/// `left` is the hub that absorbs the central relation before the merge;
/// `right` is the hub reached across it.
struct Orientation<'a> {
    left_hub: Relation,
    right_hub: Relation,
    central: Relation,
    /// Selectivity of the edge from the central relation to `left_hub`.
    sel_left: f64,
    /// Selectivity of the edge from the central relation to `right_hub`,
    /// i.e. the selectivity of the big merge.
    sel_right: f64,
    /// Spokes of `left_hub`, sorted by fanout.
    left_spokes: &'a [Spoke],
    /// Spokes of `right_hub`, sorted by fanout.
    right_spokes: &'a [Spoke],
}

impl Orientation<'_> {
    /// The central relation seen as a spoke of the left hub.
    fn shared_entry(&self) -> Spoke {
        Spoke::new(self.central.id, self.central.weight, self.sel_left)
    }

    /// Search every split point pair and return the cheapest.
    ///
    /// Deviation: returns `None` when no candidate has a finite cost, rather
    /// than the reference's implicit infinity with both split points at zero.
    /// An optimizer must be able to tell "this order is free" from "these
    /// numbers are unusable", and only the former should rewrite a plan.
    fn solve(&self) -> Option<SideSolution> {
        let (left_cost, left_size) = side_table(
            self.left_hub.weight,
            self.left_spokes,
            Some(self.shared_entry()),
        );
        let (right_cost, right_size) =
            side_table(self.right_hub.weight, self.right_spokes, None);

        let mut best: Option<SideSolution> = None;
        for p in 0..=self.left_spokes.len() {
            for q in 0..=self.right_spokes.len() {
                let critical_size = left_size[p] * right_size[q] * self.sel_right;
                let w =
                    leftover_multiplier(&self.left_spokes[p..], &self.right_spokes[q..]);
                let total = left_cost[p] + right_cost[q] + critical_size * w;

                // The reference reaches the same outcome implicitly: both
                // `NaN < inf` and `inf < inf` are false, so non-finite
                // candidates never win there either.
                if !total.is_finite() {
                    continue;
                }
                // Strictly less, so the lowest `p` then lowest `q` wins ties.
                // Determinism matters here because the resulting plan is
                // compared against committed expected output in tests.
                if best.is_none_or(|best| total < best.cost) {
                    best = Some(SideSolution { cost: total, p, q });
                }
            }
        }
        best
    }

    /// Expand split points into the concrete sequence of joins.
    fn materialize(&self, solution: SideSolution) -> DoubleStarPlan {
        let SideSolution { cost, p, q } = solution;

        // Rebuild the left prefix through `chain_cost_and_size` rather than
        // just sorting, so the central relation lands in exactly the position
        // the costing assumed.
        let mut before_merge = self.left_spokes[..p].to_vec();
        before_merge.push(self.shared_entry());
        let (_, _, left_prefix) =
            chain_cost_and_size(self.left_hub.weight, &before_merge);

        let (_, _, right_prefix) =
            chain_cost_and_size(self.right_hub.weight, &self.right_spokes[..q]);

        // Same helper the pricing used, so the emitted order cannot drift from
        // the order that was costed.
        let leftovers = merged_leftovers(&self.left_spokes[p..], &self.right_spokes[q..])
            .iter()
            .map(|spoke| spoke.id)
            .collect();

        DoubleStarPlan {
            cost,
            left_hub: self.left_hub.id,
            left_prefix,
            right_hub: self.right_hub.id,
            right_prefix,
            leftovers,
        }
    }
}

impl DoubleStar {
    /// Reject inputs the cost model cannot draw a conclusion from.
    ///
    /// Deviation: the reference takes hand-entered numbers and assumes they
    /// are sane. Statistics-derived inputs are not: a zero distinct count
    /// yields an infinite selectivity, absent statistics can surface as NaN,
    /// and a long chain of large cardinalities overflows to infinity. Feeding
    /// those through would produce a confidently wrong ordering, so they are
    /// refused up front and the caller leaves the plan alone.
    fn validate(&self) -> Option<()> {
        fn usable(value: f64) -> bool {
            value.is_finite() && value >= 0.0
        }

        let relations_ok = [self.hub_a, self.hub_b, self.central]
            .iter()
            .all(|relation| usable(relation.weight));
        let spokes_ok = self
            .spokes_a
            .iter()
            .chain(self.spokes_b.iter())
            .all(|spoke| usable(spoke.weight) && usable(spoke.selectivity));
        let bounded = self.spokes_a.len() <= MAX_SPOKES_PER_HUB
            && self.spokes_b.len() <= MAX_SPOKES_PER_HUB;

        (relations_ok && spokes_ok && usable(self.sel_a) && usable(self.sel_b) && bounded)
            .then_some(())
    }
}

/// Find the cheapest join order for a double star.
///
/// Both assignments of the central relation are searched and the cheaper is
/// returned. Ties go to attaching the central relation to `hub_a`, matching
/// the reference.
///
/// Returns `None` when the inputs are unusable (see [`DoubleStar::validate`])
/// or when no candidate order has a finite cost. Callers should treat `None`
/// as "leave the plan as it is".
pub fn optimal_double_star(star: &DoubleStar) -> Option<DoubleStarPlan> {
    star.validate()?;

    let mut spokes_a = star.spokes_a.clone();
    sort_by_fanout(&mut spokes_a);
    let mut spokes_b = star.spokes_b.clone();
    sort_by_fanout(&mut spokes_b);

    let central_on_a = Orientation {
        left_hub: star.hub_a,
        right_hub: star.hub_b,
        central: star.central,
        sel_left: star.sel_a,
        sel_right: star.sel_b,
        left_spokes: &spokes_a,
        right_spokes: &spokes_b,
    };
    let central_on_b = Orientation {
        left_hub: star.hub_b,
        right_hub: star.hub_a,
        central: star.central,
        sel_left: star.sel_b,
        sel_right: star.sel_a,
        left_spokes: &spokes_b,
        right_spokes: &spokes_a,
    };

    let on_a = central_on_a.solve();
    let on_b = central_on_b.solve();

    match (on_a, on_b) {
        (Some(a), Some(b)) if a.cost <= b.cost => Some(central_on_a.materialize(a)),
        (Some(_), Some(b)) => Some(central_on_b.materialize(b)),
        (Some(a), None) => Some(central_on_a.materialize(a)),
        (None, Some(b)) => Some(central_on_b.materialize(b)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ids used throughout: spokes 1..=9, hubs 10 and 11, central 12.
    const HUB_A: usize = 10;
    const HUB_B: usize = 11;
    const CENTRAL: usize = 12;

    /// The three spokes used by the reference cases, deliberately spanning a
    /// shrinking fanout (0.2), a neutral one (1.0) and a growing one (5.0).
    fn spokes() -> Vec<Spoke> {
        vec![
            Spoke::new(1, 50.0, 0.02),   // fanout 1.0
            Spoke::new(2, 10.0, 0.5),    // fanout 5.0
            Spoke::new(3, 200.0, 0.001), // fanout 0.2
        ]
    }

    fn sorted_spokes() -> Vec<Spoke> {
        let mut spokes = spokes();
        sort_by_fanout(&mut spokes);
        spokes
    }

    #[test]
    fn chain_joins_cheapest_fanout_first() {
        let (cost, size, order) = chain_cost_and_size(1000.0, &spokes());

        // Worked by hand: 1000 -> (x0.2) 200 -> (x1.0) 200 -> (x5.0) 1000,
        // paying 200 + 200 + 1000.
        assert_eq!(cost, 1400.0);
        assert_eq!(size, 1000.0);
        assert_eq!(order, vec![3, 1, 2]);
    }

    #[test]
    fn chain_of_nothing_is_free() {
        let (cost, size, order) = chain_cost_and_size(1000.0, &[]);

        assert_eq!(cost, 0.0);
        assert_eq!(size, 1000.0);
        assert!(order.is_empty());
    }

    #[test]
    fn chain_order_beats_worst_order() {
        // Same spokes, forced worst-first by pricing them one at a time.
        let (best, _, _) = chain_cost_and_size(1000.0, &spokes());
        let reversed = {
            let mut sorted = sorted_spokes();
            sorted.reverse();
            let mut cost = 0.0;
            let mut size = 1000.0;
            for spoke in &sorted {
                cost += size * spoke.weight * spoke.selectivity;
                size *= spoke.weight;
                size *= spoke.selectivity;
            }
            cost
        };

        assert_eq!(best, 1400.0);
        assert_eq!(reversed, 11000.0);
        assert!(best < reversed);
    }

    #[test]
    fn side_table_prices_every_split_point() {
        let (cost, size) = side_table(1000.0, &sorted_spokes(), None);

        assert_eq!(cost, vec![0.0, 200.0, 400.0, 1400.0]);
        assert_eq!(size, vec![1000.0, 200.0, 200.0, 1000.0]);
    }

    #[test]
    fn side_table_places_central_by_fanout() {
        // Central relation has fanout 3.0, so it sorts after the 0.2 and 1.0
        // spokes but before the 5.0 one.
        let extra = Spoke::new(CENTRAL, 300.0, 0.01);
        let (cost, size) = side_table(1000.0, &sorted_spokes(), Some(extra));

        assert_eq!(cost, vec![3000.0, 800.0, 1000.0, 4000.0]);
        assert_eq!(size, vec![3000.0, 600.0, 600.0, 3000.0]);
    }

    #[test]
    fn leftover_multiplier_pools_both_piles() {
        let deferred_a = [Spoke::new(1, 50.0, 0.02)]; // fanout 1.0
        let deferred_b = [
            Spoke::new(4, 10.0, 0.5), // fanout 5.0
            Spoke::new(5, 4.0, 0.25), // fanout 1.0
        ];

        // Merged order is 1.0, 1.0, 5.0, so W = 1 + 1 + 1 + 5.
        assert_eq!(leftover_multiplier(&deferred_a, &deferred_b), 8.0);
    }

    #[test]
    fn leftover_multiplier_of_nothing_is_the_merge_alone() {
        assert_eq!(leftover_multiplier(&[], &[]), 1.0);
    }

    /// The symmetric reference case: every spoke is worth absorbing before the
    /// merge, so both split points sit at their maximum.
    #[test]
    fn solves_the_symmetric_reference_case() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 1000.0),
            hub_b: Relation::new(HUB_B, 2000.0),
            central: Relation::new(CENTRAL, 300.0),
            sel_a: 0.01,
            sel_b: 0.005,
            spokes_a: spokes(),
            spokes_b: vec![
                Spoke::new(4, 20.0, 0.1), // fanout 2.0
                Spoke::new(5, 5.0, 0.4),  // fanout 2.0
            ],
        };

        let plan = optimal_double_star(&star).expect("stats are usable");

        assert_eq!(plan.cost, 136000.0);
        assert_eq!(plan.left_hub, HUB_A);
        assert_eq!(plan.right_hub, HUB_B);
        // All three A spokes plus the central relation, in fanout order:
        // 0.2, 1.0, 3.0 (central), 5.0.
        assert_eq!(plan.left_prefix, vec![3, 1, CENTRAL, 2]);
        assert_eq!(plan.right_prefix, vec![4, 5]);
        assert!(plan.leftovers.is_empty());
    }

    /// Attaching the central relation to the other hub is not cosmetic: here
    /// it is worth 35%.
    #[test]
    fn picks_the_cheaper_orientation() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 500_000.0),
            hub_b: Relation::new(HUB_B, 10.0),
            central: Relation::new(CENTRAL, 100.0),
            sel_a: 0.5,
            sel_b: 0.001,
            spokes_a: vec![Spoke::new(1, 1000.0, 0.9)], // fanout 900
            spokes_b: vec![Spoke::new(2, 2.0, 0.1)],    // fanout 0.2
        };

        let plan = optimal_double_star(&star).expect("stats are usable");

        // Central on hub A costs 70050002.0; on hub B it costs this.
        assert_eq!(plan.cost, 45050001.2);
        assert_eq!(plan.left_hub, HUB_B);
        assert_eq!(plan.right_hub, HUB_A);
        // Central has fanout 0.1 against hub B, the spoke 0.2.
        assert_eq!(plan.left_prefix, vec![CENTRAL, 2]);
        assert!(plan.right_prefix.is_empty());
        // The fanout-900 spoke is deferred until after the merge.
        assert_eq!(plan.leftovers, vec![1]);
    }

    /// A growing spoke is worth deferring; a shrinking one is not.
    #[test]
    fn defers_growing_spokes_past_the_merge() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 1000.0),
            hub_b: Relation::new(HUB_B, 1000.0),
            central: Relation::new(CENTRAL, 100.0),
            sel_a: 0.01,
            sel_b: 0.01,
            spokes_a: vec![
                Spoke::new(1, 10.0, 0.001), // fanout 0.01, shrinks
                Spoke::new(2, 1000.0, 5.0), // fanout 5000, grows hard
            ],
            spokes_b: vec![],
        };

        let plan = optimal_double_star(&star).expect("stats are usable");

        assert_eq!(plan.left_prefix, vec![1, CENTRAL]);
        assert_eq!(plan.leftovers, vec![2]);
    }

    #[test]
    fn plan_visits_every_relation_exactly_once() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 1000.0),
            hub_b: Relation::new(HUB_B, 2000.0),
            central: Relation::new(CENTRAL, 300.0),
            sel_a: 0.01,
            sel_b: 0.005,
            spokes_a: spokes(),
            spokes_b: vec![Spoke::new(4, 20.0, 0.1), Spoke::new(5, 5.0, 0.4)],
        };

        let plan = optimal_double_star(&star).expect("stats are usable");

        let mut visited = vec![plan.left_hub, plan.right_hub];
        visited.extend(&plan.left_prefix);
        visited.extend(&plan.right_prefix);
        visited.extend(&plan.leftovers);
        visited.sort_unstable();

        assert_eq!(visited, vec![1, 2, 3, 4, 5, HUB_A, HUB_B, CENTRAL]);
    }

    #[test]
    fn star_with_no_spokes_is_just_the_merge() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 100.0),
            hub_b: Relation::new(HUB_B, 200.0),
            central: Relation::new(CENTRAL, 50.0),
            sel_a: 0.1,
            sel_b: 0.2,
            spokes_a: vec![],
            spokes_b: vec![],
        };

        let plan = optimal_double_star(&star).expect("stats are usable");

        assert_eq!(plan.left_prefix, vec![CENTRAL]);
        assert!(plan.right_prefix.is_empty());
        assert!(plan.leftovers.is_empty());
    }

    #[test]
    fn rejects_unusable_statistics() {
        let base = DoubleStar {
            hub_a: Relation::new(HUB_A, 1000.0),
            hub_b: Relation::new(HUB_B, 2000.0),
            central: Relation::new(CENTRAL, 300.0),
            sel_a: 0.01,
            sel_b: 0.005,
            spokes_a: spokes(),
            spokes_b: vec![],
        };
        assert!(optimal_double_star(&base).is_some());

        // A zero distinct count would surface as an infinite selectivity.
        let infinite_selectivity = DoubleStar {
            sel_a: f64::INFINITY,
            ..base.clone()
        };
        assert!(optimal_double_star(&infinite_selectivity).is_none());

        // Absent statistics can arrive as NaN.
        let nan_weight = DoubleStar {
            hub_a: Relation::new(HUB_A, f64::NAN),
            ..base.clone()
        };
        assert!(optimal_double_star(&nan_weight).is_none());

        let negative_weight = DoubleStar {
            central: Relation::new(CENTRAL, -1.0),
            ..base.clone()
        };
        assert!(optimal_double_star(&negative_weight).is_none());

        let too_many_spokes = DoubleStar {
            spokes_a: (0..MAX_SPOKES_PER_HUB + 1)
                .map(|id| Spoke::new(id, 10.0, 0.1))
                .collect(),
            ..base.clone()
        };
        assert!(optimal_double_star(&too_many_spokes).is_none());
    }

    #[test]
    fn empty_relations_cost_nothing() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 0.0),
            hub_b: Relation::new(HUB_B, 0.0),
            central: Relation::new(CENTRAL, 0.0),
            sel_a: 1.0,
            sel_b: 1.0,
            spokes_a: vec![Spoke::new(1, 0.0, 1.0)],
            spokes_b: vec![],
        };

        let plan = optimal_double_star(&star).expect("zero rows is a usable estimate");
        assert_eq!(plan.cost, 0.0);
    }

    /// Ties must resolve the same way every run: the plan ends up in committed
    /// expected output, so a coin flip here means a flaky test suite.
    #[test]
    fn tied_costs_resolve_deterministically() {
        let star = DoubleStar {
            hub_a: Relation::new(HUB_A, 1000.0),
            hub_b: Relation::new(HUB_B, 1000.0),
            central: Relation::new(CENTRAL, 100.0),
            sel_a: 0.01,
            sel_b: 0.01,
            spokes_a: vec![Spoke::new(1, 10.0, 0.1), Spoke::new(2, 5.0, 0.2)],
            spokes_b: vec![Spoke::new(3, 10.0, 0.1), Spoke::new(4, 5.0, 0.2)],
        };

        let first = optimal_double_star(&star).expect("stats are usable");
        for _ in 0..16 {
            assert_eq!(optimal_double_star(&star).as_ref(), Some(&first));
        }
        // Perfectly symmetric, so the tie goes to hub A.
        assert_eq!(first.left_hub, HUB_A);
    }

    // ---------- brute force ----------
    //
    // Every test above checks this port against the values the reference
    // implementation produced, which establishes that the translation is
    // faithful and nothing about whether the approach is right: an error
    // present in both would pass. These two search the plan space exhaustively
    // and simulate each candidate directly, so they check the answer rather
    // than the agreement.

    /// Every ordering of `items`.
    fn permutations(items: &[Spoke]) -> Vec<Vec<Spoke>> {
        if items.is_empty() {
            return vec![Vec::new()];
        }
        let mut out = Vec::new();
        for index in 0..items.len() {
            let mut rest = items.to_vec();
            let head = rest.remove(index);
            for mut tail in permutations(&rest) {
                tail.insert(0, head);
                out.push(tail);
            }
        }
        out
    }

    /// Join `sequence` onto a hub in exactly that order, accumulating cost the
    /// long way rather than through the precomputed tables.
    fn simulate(hub_weight: f64, sequence: &[Spoke]) -> (f64, f64) {
        let mut cost = 0.0;
        let mut size = hub_weight;
        for spoke in sequence {
            cost += size * spoke.weight * spoke.selectivity;
            size *= spoke.weight;
            size *= spoke.selectivity;
        }
        (cost, size)
    }

    /// Split `spokes` into the subset selected by `mask` and the rest.
    fn split(spokes: &[Spoke], mask: usize) -> (Vec<Spoke>, Vec<Spoke>) {
        let mut chosen = Vec::new();
        let mut deferred = Vec::new();
        for (index, spoke) in spokes.iter().enumerate() {
            if mask & (1 << index) != 0 {
                chosen.push(*spoke);
            } else {
                deferred.push(*spoke);
            }
        }
        (chosen, deferred)
    }

    /// The cheapest cost reachable in the plan space the model searches, found
    /// by trying everything: both orientations, every subset of each side's
    /// spokes taken before the merge, and every ordering within each group.
    fn brute_force(star: &DoubleStar) -> f64 {
        let orientations = [
            (
                star.hub_a.weight,
                star.hub_b.weight,
                star.sel_a,
                star.sel_b,
                &star.spokes_a,
                &star.spokes_b,
            ),
            (
                star.hub_b.weight,
                star.hub_a.weight,
                star.sel_b,
                star.sel_a,
                &star.spokes_b,
                &star.spokes_a,
            ),
        ];

        let mut best = f64::INFINITY;
        for (left_hub, right_hub, sel_left, sel_merge, left_spokes, right_spokes) in
            orientations
        {
            let central = Spoke::new(star.central.id, star.central.weight, sel_left);

            for left_mask in 0..(1usize << left_spokes.len()) {
                let (left_taken, left_deferred) = split(left_spokes, left_mask);
                for right_mask in 0..(1usize << right_spokes.len()) {
                    let (right_taken, right_deferred) = split(right_spokes, right_mask);

                    // The central relation is always absorbed before the merge
                    // on whichever hub owns it.
                    let mut left_group = left_taken.clone();
                    left_group.push(central);

                    let mut deferred = left_deferred.clone();
                    deferred.extend(right_deferred.iter().copied());

                    for left_order in permutations(&left_group) {
                        let (left_cost, left_size) = simulate(left_hub, &left_order);
                        for right_order in permutations(&right_taken) {
                            let (right_cost, right_size) =
                                simulate(right_hub, &right_order);
                            let merged = left_size * right_size * sel_merge;

                            for tail in permutations(&deferred) {
                                let (tail_cost, _) = simulate(merged, &tail);
                                // `merged` is both the size after the merge and
                                // the cost of performing it.
                                let total = left_cost + right_cost + merged + tail_cost;
                                if total < best {
                                    best = total;
                                }
                            }
                        }
                    }
                }
            }
        }
        best
    }

    fn assert_close(actual: f64, expected: f64) {
        let tolerance = 1e-9 * expected.abs().max(1.0);
        assert!(
            (actual - expected).abs() <= tolerance,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn matches_an_exhaustive_search_of_the_plan_space() {
        // Fanouts deliberately straddle 1 in both directions, so the split
        // points and the orientation all matter.
        let cases = [
            (
                vec![Spoke::new(1, 40.0, 0.01), Spoke::new(2, 30.0, 4.0)],
                vec![Spoke::new(3, 8.0, 0.05), Spoke::new(4, 12.0, 2.5)],
            ),
            (
                vec![Spoke::new(1, 1000.0, 0.9), Spoke::new(2, 2.0, 0.1)],
                vec![Spoke::new(3, 5.0, 0.4)],
            ),
            (
                vec![Spoke::new(1, 6.0, 0.5)],
                vec![Spoke::new(3, 100.0, 0.002), Spoke::new(4, 7.0, 3.0)],
            ),
        ];

        for (spokes_a, spokes_b) in cases {
            let star = DoubleStar {
                hub_a: Relation::new(HUB_A, 900.0),
                hub_b: Relation::new(HUB_B, 1_700.0),
                central: Relation::new(CENTRAL, 250.0),
                sel_a: 0.02,
                sel_b: 0.004,
                spokes_a,
                spokes_b,
            };

            let chosen = optimal_double_star(&star).expect("stats are usable");
            assert_close(chosen.cost, brute_force(&star));
        }
    }

    #[test]
    fn cheapest_fanout_first_beats_every_other_order() {
        // The greedy claim for a single star, checked against all 24 orders.
        let spokes = vec![
            Spoke::new(1, 50.0, 0.02),
            Spoke::new(2, 10.0, 0.5),
            Spoke::new(3, 200.0, 0.001),
            Spoke::new(4, 9.0, 0.4),
        ];

        let (greedy, _, _) = chain_cost_and_size(1000.0, &spokes);
        let best = permutations(&spokes)
            .iter()
            .map(|order| simulate(1000.0, order).0)
            .fold(f64::INFINITY, f64::min);

        assert_close(greedy, best);
    }
}
