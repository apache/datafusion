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
use datafusion_physical_plan::materialized_cte::{
    MaterializedCteCache, MaterializedCteExec, MaterializedCteReaderExec,
    materialized_cte_statistics, replace_materialized_cte_readers,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

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
    /// Map of CTE name to the number of partitions readers should expose
    partition_counts: Mutex<HashMap<String, usize>>,
}

impl MaterializedCtePlanner {
    /// Create a new `MaterializedCtePlanner`.
    pub fn new() -> Self {
        Self {
            caches: Mutex::new(HashMap::new()),
            partition_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a cache for the given CTE name.
    fn get_or_create_cache(&self, name: &str) -> Arc<MaterializedCteCache> {
        let mut caches = self.caches.lock().unwrap();
        Arc::clone(
            caches
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(MaterializedCteCache::new(name.to_string()))),
        )
    }

    fn create_cache(&self, name: &str) -> Arc<MaterializedCteCache> {
        let cache = Arc::new(MaterializedCteCache::new(name.to_string()));
        self.caches
            .lock()
            .unwrap()
            .insert(name.to_string(), Arc::clone(&cache));
        cache
    }

    fn set_partition_count(&self, name: &str, partition_count: usize) {
        self.partition_counts
            .lock()
            .unwrap()
            .insert(name.to_string(), partition_count);
    }

    fn partition_count(&self, name: &str) -> usize {
        self.partition_counts
            .lock()
            .unwrap()
            .get(name)
            .copied()
            .unwrap_or(1)
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
            let cache = self.create_cache(&producer.name);
            let cte_plan = Arc::clone(&physical_inputs[0]);
            let partition_count = cte_plan.output_partitioning().partition_count();
            let statistics = materialized_cte_statistics(cte_plan.as_ref())?;
            self.set_partition_count(&producer.name, partition_count);
            let continuation = replace_materialized_cte_readers(
                Arc::clone(&physical_inputs[1]),
                &producer.name,
                &cache,
                partition_count,
                &statistics,
            )?;
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
            let statistics =
                Arc::new(datafusion_physical_plan::Statistics::new_unknown(&schema));
            let exec = MaterializedCteReaderExec::new(
                reader.name.clone(),
                schema,
                cache,
                self.partition_count(&reader.name),
                statistics,
            );
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}
