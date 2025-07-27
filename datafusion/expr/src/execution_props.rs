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
use chrono::{DateTime, TimeZone, Utc};
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::HashMap;
use std::sync::Arc;

/// Holds per-query execution properties and data (such as statement
/// starting timestamps).
///
/// An [`ExecutionProps`] is created each time a `LogicalPlan` is
/// prepared for execution (optimized). If the same plan is optimized
/// multiple times, a new `ExecutionProps` is created each time.
///
/// It is important that this structure be cheap to create as it is
/// done so during predicate pruning and expression simplification
#[derive(Clone, Debug)]
pub struct ExecutionProps {
    pub query_execution_start_time: DateTime<Utc>,
    /// Alias generator used by subquery optimizer rules
    pub alias_generator: Arc<AliasGenerator>,
    /// Snapshot of config options
    pub config_options: Option<Arc<ConfigOptions>>,
    /// Providers for scalar variables
    pub var_providers: Option<HashMap<VarType, Arc<dyn VarProvider + Send + Sync>>>,
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
            // Set this to a fixed sentinel to make it obvious if this is
            // not being updated / propagated correctly
            query_execution_start_time: Utc.timestamp_nanos(0),
            alias_generator: Arc::new(AliasGenerator::new()),
            config_options: None,
            var_providers: None,
        }
    }

    /// Set the query execution start time to use
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_start_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = query_execution_start_time;
        self
    }

    /// Marks the execution of query started timestamp.
    /// This also instantiates a new alias generator.
    pub fn start_execution(&mut self, config_options: Arc<ConfigOptions>) -> &Self {
        self.query_execution_start_time = Utc::now();
        self.alias_generator = Arc::new(AliasGenerator::new());
        self.config_options = Some(config_options);
        &*self
    }

    /// Registers a variable provider, returning the existing
    /// provider, if any
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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn debug() {
        let props = ExecutionProps::new();
        assert_eq!("ExecutionProps { query_execution_start_time: 1970-01-01T00:00:00Z, alias_generator: AliasGenerator { next_id: 1 }, config_options: None, var_providers: None }", format!("{props:?}"));
    }
}
