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

use crate::var_provider::{VarProvider, VarType};
use chrono::{DateTime, Utc};
use datafusion_common::HashMap;
use datafusion_common::ScalarValue;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, internal_err};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

/// Holds properties and scratch state used while optimizing a [`LogicalPlan`]
/// and translating it into an executable physical plan, such as the statement
/// start time used during simplification.
///
/// An [`ExecutionProps`] is created each time a `LogicalPlan` is
/// prepared for execution (optimized). If the same plan is optimized
/// multiple times, a new `ExecutionProps` is created each time.
///
/// It is important that this structure be cheap to create as it is
/// done so during predicate pruning and expression simplification
///
/// # Relationship with [`TaskContext`]
///
/// [`ExecutionProps`] is intentionally distinct from [`TaskContext`].
/// It is used while optimizing a logical plan and constructing physical
/// expressions and physical plans, before physical operators are run.
///
/// [`TaskContext`] is the runtime context passed to physical operators during
/// physical-plan execution.
///
/// Keeping these structures separate avoids threading execution/runtime state
/// through planning APIs, and avoids making execution depend on planner-only
/// scratch state.
///
/// [`TaskContext`]: https://docs.rs/datafusion/latest/datafusion/execution/struct.TaskContext.html
/// [`LogicalPlan`]: crate::LogicalPlan
#[derive(Clone, Debug)]
pub struct ExecutionProps {
    /// The time at which the query execution started. If `None`,
    /// functions like `now()` will not be simplified during optimization.
    pub query_execution_start_time: Option<DateTime<Utc>>,
    /// Alias generator used by subquery optimizer rules
    pub alias_generator: Arc<AliasGenerator>,
    /// Snapshot of config options when the query started
    pub config_options: Option<Arc<ConfigOptions>>,
    /// Providers for scalar variables
    pub var_providers: Option<HashMap<VarType, Arc<dyn VarProvider + Send + Sync>>>,
    /// Maps each logical `Subquery` to its index in `subquery_results`.
    /// Populated by the physical planner before calling `create_physical_expr`.
    pub subquery_indexes: HashMap<crate::logical_plan::Subquery, SubqueryIndex>,
    /// Shared results container for uncorrelated scalar subquery values.
    /// Populated at execution time by `ScalarSubqueryExec`.
    pub subquery_results: ScalarSubqueryResults,
}

impl Default for ExecutionProps {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionProps {
    /// Creates a new execution props
    pub fn new() -> Self {
        ExecutionProps {
            query_execution_start_time: None,
            alias_generator: Arc::new(AliasGenerator::new()),
            config_options: None,
            var_providers: None,
            subquery_indexes: HashMap::new(),
            subquery_results: ScalarSubqueryResults::default(),
        }
    }

    /// Set the query execution start time to use
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = Some(query_execution_start_time);
        self
    }

    #[deprecated(since = "50.0.0", note = "Use mark_start_execution instead")]
    pub fn start_execution(&mut self) -> &Self {
        let default_config = Arc::new(ConfigOptions::default());
        self.mark_start_execution(default_config)
    }

    /// Marks the execution of query started timestamp.
    /// This also instantiates a new alias generator.
    pub fn mark_start_execution(&mut self, config_options: Arc<ConfigOptions>) -> &Self {
        self.query_execution_start_time = Some(Utc::now());
        self.alias_generator = Arc::new(AliasGenerator::new());
        self.config_options = Some(config_options);
        &*self
    }

    /// Registers a variable provider, returning the existing provider, if any
    pub fn add_var_provider(
        &mut self,
        var_type: VarType,
        provider: Arc<dyn VarProvider + Send + Sync>,
    ) -> Option<Arc<dyn VarProvider + Send + Sync>> {
        let mut var_providers = self.var_providers.take().unwrap_or_default();

        let old_provider = var_providers.insert(var_type, provider);

        self.var_providers = Some(var_providers);

        old_provider
    }

    /// Returns the provider for the `var_type`, if any
    #[expect(clippy::needless_pass_by_value)]
    pub fn get_var_provider(
        &self,
        var_type: VarType,
    ) -> Option<Arc<dyn VarProvider + Send + Sync>> {
        self.var_providers
            .as_ref()
            .and_then(|var_providers| var_providers.get(&var_type).cloned())
    }

    /// Returns the configuration properties for this execution
    /// if the execution has started
    pub fn config_options(&self) -> Option<&Arc<ConfigOptions>> {
        self.config_options.as_ref()
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
mod test {
    use super::*;

    #[test]
    fn debug() {
        let props = ExecutionProps::new();
        assert_eq!(
            "ExecutionProps { query_execution_start_time: None, alias_generator: AliasGenerator { next_id: 1 }, config_options: None, var_providers: None, subquery_indexes: {}, subquery_results: [] }",
            format!("{props:?}")
        );
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
