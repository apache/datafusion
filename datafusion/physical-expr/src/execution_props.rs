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
use std::collections::HashMap;
use std::sync::Arc;

/// Holds per-execution properties and data (such as starting timestamps, etc).
/// An instance of this struct is created each time a [`LogicalPlan`] is prepared for
/// execution (optimized). If the same plan is optimized multiple times, a new
/// `ExecutionProps` is created each time.
///
/// It is important that this structure be cheap to create as it is
/// done so during predicate pruning and expression simplification
#[derive(Clone)]
pub struct ExecutionProps {
    pub query_execution_start_time: DateTime<Utc>,
    /// providers for scalar variables
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
            query_execution_start_time: chrono::Utc::now(),
            var_providers: None,
        }
    }

    /// Marks the execution of query started timestamp
    pub fn start_execution(&mut self) -> &Self {
        self.query_execution_start_time = chrono::Utc::now();
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

    /// Returns the provider for the var_type, if any
    pub fn get_var_provider(
        &self,
        var_type: VarType,
    ) -> Option<Arc<dyn VarProvider + Send + Sync>> {
        self.var_providers
            .as_ref()
            .and_then(|var_providers| var_providers.get(&var_type).map(Arc::clone))
    }
}
