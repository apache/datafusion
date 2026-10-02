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

use datafusion_common::{HashMap, Result, ScalarValue, TableReference, internal_err};

/// Context used while converting a logical plan subtree into a physical plan.
///
/// Unlike [`ExecutionProps`](crate::execution_props::ExecutionProps), which
/// applies to the overall planning and execution of a query, this context can
/// differ between recursively planned subtrees. It currently carries:
///
/// * the state needed to create physical expressions for
///   [`Expr::ScalarSubquery`] nodes that read from a shared
///   [`ScalarSubqueryResults`] container, and
/// * the qualifiers assigned to the [`Expr::LambdaVariable`]s that are in scope.
///
/// The physical planner builds this context from the set of uncorrelated scalar
/// subqueries it has scheduled for a subtree. It is then passed explicitly
/// through `create_physical_expr` so that function can find the slot index for
/// each [`Subquery`]. While planning the body of a lambda,
/// `create_physical_expr` extends the context with the lambda's parameters via
/// [`Self::with_qualified_lambda_variables`].
///
/// An empty [`PhysicalPlanningContext`] (the [`Default`]) is what every
/// non-physical-planner caller passes; if such a caller encounters a scalar
/// subquery, `create_physical_expr` returns a `not_impl_err`.
///
/// [`Expr::ScalarSubquery`]: crate::Expr::ScalarSubquery
/// [`Expr::LambdaVariable`]: crate::Expr::LambdaVariable
/// [`Subquery`]: crate::logical_plan::Subquery
#[derive(Clone, Debug, Default)]
pub struct PhysicalPlanningContext {
    /// Behind an `Arc` because the context is cloned for each lambda body that
    /// is planned, and the indexes are the same for the whole subtree.
    indexes: Arc<HashMap<crate::logical_plan::Subquery, SubqueryIndex>>,
    results: ScalarSubqueryResults,
    /// Maps each lambda variable name in scope to the qualifier generated for
    /// its lambda during physical planning.
    lambda_variable_qualifier: HashMap<String, TableReference>,
}

impl PhysicalPlanningContext {
    /// Create a [`PhysicalPlanningContext`] from an index map and a shared
    /// results container. The index map must use the same indices as slots in
    /// `results`.
    pub fn new(
        indexes: HashMap<crate::logical_plan::Subquery, SubqueryIndex>,
        results: ScalarSubqueryResults,
    ) -> Self {
        Self {
            indexes: Arc::new(indexes),
            results,
            lambda_variable_qualifier: HashMap::new(),
        }
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

    /// Adds a mapping for each variable to the given qualifier. Existing
    /// variables with conflicting names are shadowed.
    pub fn with_qualified_lambda_variables(
        mut self,
        qualifier: &TableReference,
        variables: &[String],
    ) -> Self {
        for var in variables {
            self.lambda_variable_qualifier
                .entry_ref(var)
                .insert(qualifier.clone());
        }

        self
    }

    /// Returns the qualifier of the lambda variable `name`, if it is in scope.
    pub fn lambda_variable_qualifier(&self, name: &str) -> Option<&TableReference> {
        self.lambda_variable_qualifier.get(name)
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

/// An alternate backing store for a [`ScalarSubqueryResults`] container.
///
/// A results container normally owns its slots directly (see
/// [`ScalarSubqueryResults::new`]). This trait lets a container instead
/// forward `get`/`set`/`clear` to some other shared container, which is how
/// `datafusion-ffi` bridges the results of a `ScalarSubqueryExec` across a
/// non-Rust-ABI boundary: the far side of the boundary gets a
/// [`ScalarSubqueryResults`] backed by a proxy that calls back into the
/// near side's real container for every operation, so both sides observe the
/// same populated values.
pub trait ScalarSubqueryResultsBackend: fmt::Debug + Send + Sync {
    /// Returns the scalar value stored at `index`, if it has been populated.
    fn get(&self, index: usize) -> Option<ScalarValue>;

    /// Stores `value` in the slot at `index`.
    fn set(&self, index: usize, value: ScalarValue) -> Result<()>;

    /// Clears all populated results so the container can be reused.
    fn clear(&self);
}

#[derive(Clone)]
enum ScalarSubqueryResultsRepr {
    Local(Arc<Vec<Mutex<Option<ScalarValue>>>>),
    Remote(Arc<dyn ScalarSubqueryResultsBackend>),
}

/// Shared results container for uncorrelated scalar subqueries.
///
/// Each entry corresponds to one scalar subquery, identified by its index.
/// Each slot is populated at execution time by `ScalarSubqueryExec`, read by
/// `ScalarSubqueryExpr` instances that share this container, and cleared when
/// the plan is reset for re-execution.
#[derive(Clone)]
pub struct ScalarSubqueryResults {
    repr: ScalarSubqueryResultsRepr,
}

impl Default for ScalarSubqueryResults {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ScalarSubqueryResults {
    /// Creates a new shared results container with `n` empty slots.
    pub fn new(n: usize) -> Self {
        Self {
            repr: ScalarSubqueryResultsRepr::Local(Arc::new(
                (0..n).map(|_| Mutex::new(None)).collect(),
            )),
        }
    }

    /// Creates a results container that forwards every operation to
    /// `backend`, for example a proxy that reaches across an FFI boundary to
    /// a real container owned by the other side.
    pub fn from_backend(backend: Arc<dyn ScalarSubqueryResultsBackend>) -> Self {
        Self {
            repr: ScalarSubqueryResultsRepr::Remote(backend),
        }
    }

    /// Returns the scalar value stored at `index`, if it has been populated.
    pub fn get(&self, index: SubqueryIndex) -> Option<ScalarValue> {
        match &self.repr {
            ScalarSubqueryResultsRepr::Local(slots) => {
                let slot = slots.get(index.as_usize())?;
                slot.lock().unwrap().clone()
            }
            ScalarSubqueryResultsRepr::Remote(backend) => backend.get(index.as_usize()),
        }
    }

    /// Stores `value` in the slot at `index`.
    pub fn set(&self, index: SubqueryIndex, value: ScalarValue) -> Result<()> {
        match &self.repr {
            ScalarSubqueryResultsRepr::Local(slots) => {
                let Some(slot) = slots.get(index.as_usize()) else {
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
            ScalarSubqueryResultsRepr::Remote(backend) => {
                backend.set(index.as_usize(), value)
            }
        }
    }

    /// Clears all populated results so the container can be reused.
    pub fn clear(&self) {
        match &self.repr {
            ScalarSubqueryResultsRepr::Local(slots) => {
                for slot in slots.iter() {
                    *slot.lock().unwrap() = None;
                }
            }
            ScalarSubqueryResultsRepr::Remote(backend) => backend.clear(),
        }
    }

    /// Returns true if `this` and `other` point to the same shared container.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        match (&this.repr, &other.repr) {
            (
                ScalarSubqueryResultsRepr::Local(a),
                ScalarSubqueryResultsRepr::Local(b),
            ) => Arc::ptr_eq(a, b),
            (
                ScalarSubqueryResultsRepr::Remote(a),
                ScalarSubqueryResultsRepr::Remote(b),
            ) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl fmt::Debug for ScalarSubqueryResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.repr {
            ScalarSubqueryResultsRepr::Local(slots) => f
                .debug_list()
                .entries(slots.iter().map(|slot| slot.lock().unwrap().clone()))
                .finish(),
            ScalarSubqueryResultsRepr::Remote(backend) => f
                .debug_tuple("ScalarSubqueryResults::Remote")
                .field(backend)
                .finish(),
        }
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
        match &self.repr {
            ScalarSubqueryResultsRepr::Local(slots) => Arc::as_ptr(slots).hash(state),
            ScalarSubqueryResultsRepr::Remote(backend) => {
                Arc::as_ptr(backend).hash(state)
            }
        }
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
    fn lambda_variables_shadow_outer_scope() {
        let outer = TableReference::bare("lambda_1");
        let inner = TableReference::bare("lambda_2");

        let ctx = PhysicalPlanningContext::default()
            .with_qualified_lambda_variables(&outer, &["x".to_string(), "y".to_string()])
            .with_qualified_lambda_variables(&inner, &["y".to_string()]);

        assert_eq!(ctx.lambda_variable_qualifier("x"), Some(&outer));
        assert_eq!(ctx.lambda_variable_qualifier("y"), Some(&inner));
        assert_eq!(ctx.lambda_variable_qualifier("z"), None);
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
