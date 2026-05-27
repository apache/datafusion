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

//! Extension planner for materialized CTEs.
//!
//! This module provides [`MaterializedCtePlanner`] which connects the logical
//! plan nodes ([`MaterializedCteProducer`] and [`MaterializedCteReader`]) to
//! their physical execution counterparts.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_expr::logical_plan::{MaterializedCteProducer, MaterializedCteReader};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::materialized_cte::{
    MaterializedCteCache, MaterializedCteExec, MaterializedCteReaderExec,
};

use crate::execution::context::SessionState;
use crate::physical_planner::{ExtensionPlanner, PhysicalPlanner};

/// An extension planner that handles materialized CTE logical nodes.
///
/// It maintains a map of CTE name to shared cache, ensuring that
/// producers and readers for the same CTE share the same cache instance.
#[derive(Debug)]
pub struct MaterializedCtePlanner {
    /// Map of CTE name to shared cache
    caches: Mutex<HashMap<String, Arc<MaterializedCteCache>>>,
}

impl MaterializedCtePlanner {
    /// Create a new `MaterializedCtePlanner`.
    pub fn new() -> Self {
        Self {
            caches: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a cache for the given CTE name.
    fn get_or_create_cache(&self, name: &str) -> Arc<MaterializedCteCache> {
        let mut caches = self.caches.lock().unwrap();
        caches
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(MaterializedCteCache::new(name.to_string())))
            .clone()
    }
}

impl Default for MaterializedCtePlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExtensionPlanner for MaterializedCtePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Handle MaterializedCteProducer
        if let Some(producer) = node.as_any().downcast_ref::<MaterializedCteProducer>() {
            let cache = self.get_or_create_cache(&producer.name);
            let cte_plan = Arc::clone(&physical_inputs[0]);
            let continuation = Arc::clone(&physical_inputs[1]);
            let exec = MaterializedCteExec::new(
                producer.name.clone(),
                cte_plan,
                continuation,
                cache,
            );
            return Ok(Some(Arc::new(exec)));
        }

        // Handle MaterializedCteReader
        if let Some(reader) = node.as_any().downcast_ref::<MaterializedCteReader>() {
            let cache = self.get_or_create_cache(&reader.name);
            let schema = Arc::clone(reader.schema.inner());
            let exec = MaterializedCteReaderExec::new(reader.name.clone(), schema, cache);
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}
