// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

/// Where a [`ScalarSubqueryResults`] container keeps its slots.
///
/// The slots are normally held in memory next to the plan. `datafusion-ffi`
/// implements this to forward each operation back over an FFI boundary, so a
/// plan decoded on the far side reads the values populated on the near side.
pub trait ScalarSubqueryResultsBackend:
    fmt::Debug + Send + Sync + std::panic::RefUnwindSafe
{
    /// Returns the scalar value stored at `index`, if it has been populated.
    fn get(&self, index: usize) -> Option<ScalarValue>;

    /// Stores `value` in the slot at `index`.
    fn set(&self, index: usize, value: ScalarValue) -> Result<()>;

    /// Clears all populated results so the container can be reused.
    fn clear(&self);
}

/// The in-memory backend behind [`ScalarSubqueryResults::new`].
struct LocalResults(Vec<Mutex<Option<ScalarValue>>>);

impl fmt::Debug for LocalResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|slot| slot.lock().unwrap().clone()))
            .finish()
    }
}

impl ScalarSubqueryResultsBackend for LocalResults {
    fn get(&self, index: usize) -> Option<ScalarValue> {
        self.0.get(index)?.lock().unwrap().clone()
    }

    fn set(&self, index: usize, value: ScalarValue) -> Result<()> {
        let Some(slot) = self.0.get(index) else {
            return internal_err!(
                "ScalarSubqueryResults: result index {index} is out of bounds"
            );
        };

        let mut slot = slot.lock().unwrap();
        if slot.is_some() {
            return internal_err!(
                "ScalarSubqueryResults: result for index {index} was already populated"
            );
        }
        *slot = Some(value);

        Ok(())
    }

    fn clear(&self) {
        for slot in &self.0 {
            *slot.lock().unwrap() = None;
        }
    }
}

/// Shared results container for uncorrelated scalar subqueries.
///
/// Each entry corresponds to one scalar subquery, identified by its index.
/// Each slot is populated at execution time by `ScalarSubqueryExec`, read by
/// `ScalarSubqueryExpr` instances that share this container, and cleared when
/// the plan is reset for re-execution.
#[derive(Clone)]
pub struct ScalarSubqueryResults {
    backend: Arc<dyn ScalarSubqueryResultsBackend>,
}

impl Default for ScalarSubqueryResults {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ScalarSubqueryResults {
    /// Creates a new shared results container with `n` empty slots.
    pub fn new(n: usize) -> Self {
        Self::from_backend(Arc::new(LocalResults(
            (0..n).map(|_| Mutex::new(None)).collect(),
        )))
    }

    /// Creates a results container whose slots live in `backend`.
    pub fn from_backend(backend: Arc<dyn ScalarSubqueryResultsBackend>) -> Self {
        Self { backend }
    }

    /// Returns the scalar value stored at `index`, if it has been populated.
    pub fn get(&self, index: SubqueryIndex) -> Option<ScalarValue> {
        self.backend.get(index.as_usize())
    }

    /// Stores `value` in the slot at `index`.
    pub fn set(&self, index: SubqueryIndex, value: ScalarValue) -> Result<()> {
        self.backend.set(index.as_usize(), value)
    }

    /// Clears all populated results so the container can be reused.
    pub fn clear(&self) {
        self.backend.clear()
    }

    /// Returns true if `this` and `other` point to the same shared container.
    pub fn ptr_eq(this: &Self, other: &Self) -> bool {
        Arc::ptr_eq(&this.backend, &other.backend)
    }
}

impl fmt::Debug for ScalarSubqueryResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.backend.fmt(f)
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
        Arc::as_ptr(&self.backend).cast::<()>().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ScalarSubqueryResults` must stay `UnwindSafe`/`RefUnwindSafe` so a
    /// `ScalarSubqueryResultsBackend` implementation (e.g. `datafusion-ffi`'s
    /// FFI proxy) doesn't regress these auto traits for callers.
    #[test]
    fn scalar_subquery_results_is_unwind_safe() {
        fn assert_bounds<T: std::panic::UnwindSafe + std::panic::RefUnwindSafe>() {}
        assert_bounds::<ScalarSubqueryResults>();
    }

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
