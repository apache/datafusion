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

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion_common::{
    config::{ConfigOptions, Extensions},
    DataFusionError, Result,
};
use datafusion_expr::{AggregateUDF, ScalarUDF};

use crate::{
    any_map::AnyMap, memory_pool::MemoryPool, registry::FunctionRegistry,
    runtime_env::RuntimeEnv,
};

/// Task Execution Context
pub struct TaskContext {
    /// Session Id
    session_id: String,
    /// Optional Task Identify
    task_id: Option<String>,
    /// Configuration options
    config_options: ConfigOptions,
    /// Opaque extensions.
    extensions: AnyMap,
    /// Scalar functions associated with this task context
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    /// Aggregate functions associated with this task context
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    /// Runtime environment associated with this task context
    runtime: Arc<RuntimeEnv>,
}

impl TaskContext {
    /// Create a new task context instance
    pub fn try_new(
        task_id: String,
        session_id: String,
        task_props: HashMap<String, String>,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        runtime: Arc<RuntimeEnv>,
        extensions: AnyMap,
        config_extensions: Extensions,
    ) -> Result<Self> {
        let mut config_options = ConfigOptions::new().with_extensions(config_extensions);
        for (k, v) in task_props {
            config_options.set(&k, &v)?;
        }

        Ok(Self {
            task_id: Some(task_id),
            session_id,
            config_options,
            extensions,
            scalar_functions,
            aggregate_functions,
            runtime,
        })
    }

    /// Create a new task context instance
    pub fn new(
        session_id: String,
        task_id: Option<String>,
        config_options: ConfigOptions,
        extensions: AnyMap,
        scalar_functions: HashMap<String, Arc<ScalarUDF>>,
        aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            session_id,
            task_id,
            config_options,
            extensions,
            scalar_functions,
            aggregate_functions,
            runtime,
        }
    }

    /// Return a handle to the configuration options.
    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Return the `session_id` of this [TaskContext]
    pub fn session_id(&self) -> String {
        self.session_id.clone()
    }

    /// Return the `task_id` of this [TaskContext]
    pub fn task_id(&self) -> Option<String> {
        self.task_id.clone()
    }

    /// Return the [`MemoryPool`] associated with this [TaskContext]
    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.runtime.memory_pool
    }

    /// Return the [RuntimeEnv] associated with this [TaskContext]
    pub fn runtime_env(&self) -> Arc<RuntimeEnv> {
        self.runtime.clone()
    }

    /// Get the currently configured batch size
    pub fn batch_size(&self) -> usize {
        self.config_options.execution.batch_size
    }

    /// Get the current extensions
    pub fn extensions(&self) -> &AnyMap {
        &self.extensions
    }
}

impl FunctionRegistry for TaskContext {
    fn udfs(&self) -> HashSet<String> {
        self.scalar_functions.keys().cloned().collect()
    }

    fn udf(&self, name: &str) -> Result<Arc<ScalarUDF>> {
        let result = self.scalar_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDF named \"{name}\" in the TaskContext"
            ))
        })
    }

    fn udaf(&self, name: &str) -> Result<Arc<AggregateUDF>> {
        let result = self.aggregate_functions.get(name);

        result.cloned().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "There is no UDAF named \"{name}\" in the TaskContext"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::{config::ConfigExtension, extensions_options};

    use super::*;
    extensions_options! {
        struct TestExtension {
            value: usize, default = 42
        }
    }

    impl ConfigExtension for TestExtension {
        const PREFIX: &'static str = "test";
    }

    #[test]
    fn task_context_extensions() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let task_props = HashMap::from([("test.value".to_string(), "24".to_string())]);
        let mut extensions = Extensions::default();
        extensions.insert(TestExtension::default());

        let task_context = TaskContext::try_new(
            "task_id".to_string(),
            "session_id".to_string(),
            task_props,
            HashMap::default(),
            HashMap::default(),
            runtime,
            extensions,
        )?;

        let test = task_context
            .config_options()
            .extensions
            .get::<TestExtension>();
        assert!(test.is_some());

        assert_eq!(test.unwrap().value, 24);

        Ok(())
    }
}
