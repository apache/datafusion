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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use datafusion_common::{HashMap, Result, ScalarValue, internal_err};

/// Context used while converting a logical plan subtree into a physical plan.
///
/// Unlike [`ExecutionProps`](crate::execution_props::ExecutionProps), which
/// applies to the overall planning and execution of a query, this context can
/// differ between recursively planned subtrees. It currently carries the state
/// needed to create physical expressions for [`Expr::ScalarSubquery`] nodes
/// that read from a shared
/// [`ScalarSubqueryResults`] container.
///
/// The physical planner builds this context from the set of uncorrelated scalar
/// subqueries it has scheduled for a subtree. It is then passed explicitly
/// through `create_physical_expr` so that function can find the slot index for
/// each [`Subquery`].
///
/// An empty [`PhysicalPlanningContext`] (the [`Default`]) is what every
/// non-physical-planner caller passes; if such a caller encounters a scalar
/// subquery, `create_physical_expr` returns a `not_impl_err`.
///
/// [`Expr::ScalarSubquery`]: crate::Expr::ScalarSubquery
/// [`Subquery`]: crate::logical_plan::Subquery
#[derive(Clone, Debug, Default)]
pub struct PhysicalPlanningContext {
    indexes: HashMap<crate::logical_plan::Subquery, SubqueryIndex>,
    results: ScalarSubqueryResults,
}

impl PhysicalPlanningContext {
    /// Create a [`PhysicalPlanningContext`] from an index map and a shared
    /// results container. The index map must use the same indices as slots in
    /// `results`.
    pub fn new(
        indexes: HashMap<crate::logical_plan::Subquery, SubqueryIndex>,
        results: ScalarSubqueryResults,
    ) -> Self {
        Self { indexes, results }
    }

    /// Returns the slot index assigned to `subquery`, if any.
    pub fn index_of(
        &self,
        subquery: &crate::logical_plan::Subquery,
    ) -> Option<SubqueryIndex> {
        self.indexes.get(subquery).copied()
    }

    /// Returns the shared results container.
    pub fn results(&self) -> &ScalarSubqueryResults {
        &self.results
    }
}

/// Index of a scalar subquery within a [`ScalarSubqueryResults`] container.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SubqueryIndex(usize);

impl SubqueryIndex {
    /// Creates a new subquery index.
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Returns the underlying slot index.
    pub const fn as_usize(self) -> usize {
        self.0
    }
}

/// Shared results container for uncorrelated scalar subqueries.
///
/// Each entry corresponds to one scalar subquery, identified by its index.
/// Each slot is populated at execution time by `ScalarSubqueryExec`, read by
/// `ScalarSubqueryExpr` instances that share this container, and cleared when
/// the plan is reset for re-execution.
#[derive(Clone, Default)]
pub struct ScalarSubqueryResults {
    slots: Arc<Vec<Mutex<Option<ScalarValue>>>>,
}

impl ScalarSubqueryResults {
    /// Creates a new shared results container with `n` empty slots.
    pub fn new(n: usize) -> Self {
        Self {
            slots: Arc::new((0..n).map(|_| Mutex::new(None)).collect()),
        }
    }

    /// Returns the scalar value stored at `index`, if it has been populated.
    pub fn get(&self, index: SubqueryIndex) -> Option<ScalarValue> {
        let slot = self.slots.get(index.as_usize())?;
        slot.lock().unwrap().clone()
    }

    /// Stores `value` in the slot at `index`.
    pub fn set(&self, index: SubqueryIndex, value: ScalarValue) -> Result<()> {
        let Some(slot) = self.slots.get(index.as_usize()) else {
            return internal_err!(
                "ScalarSubqueryResults: result index {} is out of bounds",
                index.as_usize()
            );
        };

        let mut slot = slot.lock().unwrap();
        if slot.is_some() {
            return internal_err!(
                "ScalarSubqueryResults: result for index {} was already populated",
                index.as_usize()
            );
        }
        *slot = Some(value);

        Ok(())
    }

    /// Clears all populated results so the container can be reused.
    pub fn clear(&self) {
        for slot in self.slots.iter() {
            *slot.lock().unwrap() = None;
        }
    }

    /// Returns true if `this` and `other` point to the same shared container.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.slots, &other.slots)
    }
}

impl fmt::Debug for ScalarSubqueryResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.slots.iter().map(|slot| slot.lock().unwrap().clone()))
            .finish()
    }
}

impl PartialEq for ScalarSubqueryResults {
    fn eq(&self, other: &Self) -> bool {
        Self::ptr_eq(self, other)
    }
}

impl Eq for ScalarSubqueryResults {}

impl Hash for ScalarSubqueryResults {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.slots).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_subquery_results_set_and_get() -> Result<()> {
        let results = ScalarSubqueryResults::new(1);
        assert_eq!(results.get(SubqueryIndex::new(0)), None);

        results.set(SubqueryIndex::new(0), ScalarValue::Int32(Some(42)))?;
        assert_eq!(
            results.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int32(Some(42)))
        );
        assert!(
            results
                .set(SubqueryIndex::new(0), ScalarValue::Int32(Some(7)))
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn scalar_subquery_results_clear() -> Result<()> {
        let results = ScalarSubqueryResults::new(1);
        results.set(SubqueryIndex::new(0), ScalarValue::Int32(Some(42)))?;

        results.clear();

        assert_eq!(results.get(SubqueryIndex::new(0)), None);
        results.set(SubqueryIndex::new(0), ScalarValue::Int32(Some(7)))?;
        assert_eq!(
            results.get(SubqueryIndex::new(0)),
            Some(ScalarValue::Int32(Some(7)))
        );

        Ok(())
    }
}
