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

//! Extension planner for materialized subplans.
//!
//! This module provides [`MaterializedSubplanPlanner`] which connects the
//! logical plan nodes ([`MaterializedSubplanProducer`] and
//! [`MaterializedSubplanReader`]) to their physical execution counterparts.
//!
//! Caches are keyed by [`SubplanId`], **not** by the human-readable name. Names
//! are not unique under nesting (e.g. two distinct CTEs both named `t` in
//! sibling subqueries), so a name-keyed map would let one subplan's readers
//! resolve to another subplan's cache. The id is unique per producer and shared
//! with exactly its readers, so the binding is unambiguous regardless of name.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion_common::Result;
use datafusion_expr::logical_plan::{
    MaterializedSubplanProducer, MaterializedSubplanReader, SubplanId,
};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion_physical_plan::materialized_subplan::{
    MaterializedSubplanCache, MaterializedSubplanExec, MaterializedSubplanReaderExec,
    materialized_subplan_statistics, replace_materialized_subplan_readers,
};
use datafusion_physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use crate::execution::context::SessionState;
use crate::physical_planner::{ExtensionPlanner, PhysicalPlanner};

/// An extension planner that handles materialized subplan logical nodes.
///
/// It maintains a map of [`SubplanId`] to shared cache, ensuring that the
/// producer and readers for the same subplan share the same cache instance.
#[derive(Debug)]
pub struct MaterializedSubplanPlanner {
    /// Map of subplan id to shared cache.
    caches: Mutex<HashMap<SubplanId, Arc<MaterializedSubplanCache>>>,
    /// Map of subplan id to the number of partitions readers should expose.
    partition_counts: Mutex<HashMap<SubplanId, usize>>,
}

impl MaterializedSubplanPlanner {
    /// Create a new `MaterializedSubplanPlanner`.
    pub fn new() -> Self {
        Self {
            caches: Mutex::new(HashMap::new()),
            partition_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Get or create a cache for the given subplan id.
    fn get_or_create_cache(&self, id: SubplanId) -> Arc<MaterializedSubplanCache> {
        let mut caches = self.caches.lock().unwrap();
        Arc::clone(
            caches
                .entry(id)
                .or_insert_with(|| Arc::new(MaterializedSubplanCache::new(id))),
        )
    }

    fn create_cache(&self, id: SubplanId) -> Arc<MaterializedSubplanCache> {
        let cache = Arc::new(MaterializedSubplanCache::new(id));
        self.caches.lock().unwrap().insert(id, Arc::clone(&cache));
        cache
    }

    fn set_partition_count(&self, id: SubplanId, partition_count: usize) {
        self.partition_counts
            .lock()
            .unwrap()
            .insert(id, partition_count);
    }

    fn partition_count(&self, id: SubplanId) -> usize {
        self.partition_counts
            .lock()
            .unwrap()
            .get(&id)
            .copied()
            .unwrap_or(1)
    }
}

impl Default for MaterializedSubplanPlanner {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ExtensionPlanner for MaterializedSubplanPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        // Handle MaterializedSubplanProducer
        if let Some(producer) =
            node.as_any().downcast_ref::<MaterializedSubplanProducer>()
        {
            let cache = self.create_cache(producer.id);
            let plan = Arc::clone(&physical_inputs[0]);
            let partition_count = plan.output_partitioning().partition_count();
            let statistics = materialized_subplan_statistics(plan.as_ref())?;
            self.set_partition_count(producer.id, partition_count);
            let continuation = replace_materialized_subplan_readers(
                Arc::clone(&physical_inputs[1]),
                producer.id,
                &cache,
                partition_count,
                &statistics,
            )?;
            let exec = MaterializedSubplanExec::new(
                producer.id,
                producer.name.clone(),
                plan,
                continuation,
                cache,
            );
            return Ok(Some(Arc::new(exec)));
        }

        // Handle MaterializedSubplanReader
        if let Some(reader) = node.as_any().downcast_ref::<MaterializedSubplanReader>() {
            let cache = self.get_or_create_cache(reader.id);
            let schema = Arc::clone(reader.schema.inner());
            let statistics =
                Arc::new(datafusion_physical_plan::Statistics::new_unknown(&schema));
            let exec = MaterializedSubplanReaderExec::new(
                reader.id,
                reader.name.clone(),
                schema,
                cache,
                self.partition_count(reader.id),
                statistics,
            );
            return Ok(Some(Arc::new(exec)));
        }

        Ok(None)
    }
}
