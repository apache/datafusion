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

//! See `main.rs` for how to run it.

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::RwLock;

use arrow::array::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::CacheFactory;
use datafusion::logical_expr::Extension;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::UserDefinedLogicalNode;
use datafusion::logical_expr::UserDefinedLogicalNodeCore;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::collect_partitioned;
use datafusion::physical_planner::DefaultPhysicalPlanner;
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use datafusion::prelude::*;
use datafusion_common::HashMap;

/// This example demonstrates how to leverage [CacheFactory] to implement custom caching strategies for dataframes in DataFusion.
/// By default, [DataFrame::cache] in Datafusion is eager and creates an in-memory table. This example shows a basic alternative implementation for lazy caching.
/// Specifically, it implements:
/// - A [CustomCacheFactory] that creates a logical node [CacheNode] representing the cache operation.
/// - A [CacheNodePlanner] (an [ExtensionPlanner]) that understands [CacheNode] and performs caching.
/// - A [CacheNodeQueryPlanner] that installs [CacheNodePlanner].
/// - A simple in-memory [CacheManager] that stores cached [RecordBatch]es. Note that the implementation for this example is very naive and only implements put, but for real production use cases cache eviction and drop should also be implemented.
pub async fn cache_dataframe_with_custom_logic() -> Result<()> {
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = &format!("{testdata}/alltypes_plain.parquet");

    let session_state = SessionStateBuilder::new()
        .with_cache_factory(Some(Arc::new(CustomCacheFactory {})))
        .with_query_planner(Arc::new(CacheNodeQueryPlanner::default()))
        .build();
    let ctx = SessionContext::new_with_state(session_state);

    // Read the parquet files and show its schema using 'describe'
    let parquet_df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?;

    let df_cached = parquet_df
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(1)))?
        .cache()
        .await?;

    let df1 = df_cached.clone().filter(col("bool_col").is_true())?;
    let df2 = df1.clone().sort(vec![col("id").sort(true, false)])?;

    // should see log for caching only once
    df_cached.show().await?;
    df1.show().await?;
    df2.show().await?;

    Ok(())
}

#[derive(Debug)]
struct CustomCacheFactory {}

impl CacheFactory for CustomCacheFactory {
    fn create(
        &self,
        plan: LogicalPlan,
        _session_state: &SessionState,
    ) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(CacheNode { input: plan }),
        }))
    }
}

#[derive(PartialEq, Eq, PartialOrd, Hash, Debug)]
struct CacheNode {
    input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for CacheNode {
    fn name(&self) -> &str {
        "CacheNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CacheNode")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        assert_eq!(inputs.len(), 1, "input size must be one");
        Ok(Self {
            input: inputs.swap_remove(0),
        })
    }
}

struct CacheNodePlanner {
    cache_manager: Arc<RwLock<CacheManager>>,
}

#[async_trait]
impl ExtensionPlanner for CacheNodePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(cache_node) = node.as_any().downcast_ref::<CacheNode>() {
            assert_eq!(logical_inputs.len(), 1, "Inconsistent number of inputs");
            assert_eq!(physical_inputs.len(), 1, "Inconsistent number of inputs");
            if self
                .cache_manager
                .read()
                .unwrap()
                .get(&cache_node.input)
                .is_none()
            {
                let ctx = session_state.task_ctx();
                println!("caching in memory");
                let batches =
                    collect_partitioned(physical_inputs[0].clone(), ctx).await?;
                self.cache_manager
                    .write()
                    .unwrap()
                    .put(cache_node.input.clone(), batches);
            } else {
                println!("fetching directly from cache manager");
            }
            Ok(self
                .cache_manager
                .read()
                .unwrap()
                .get(&cache_node.input)
                .map(|batches| {
                    let exec: Arc<dyn ExecutionPlan> = MemorySourceConfig::try_new_exec(
                        batches,
                        physical_inputs[0].schema(),
                        None,
                    )
                    .unwrap();
                    exec
                }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Default)]
struct CacheNodeQueryPlanner {
    cache_manager: Arc<RwLock<CacheManager>>,
}

#[async_trait]
impl QueryPlanner for CacheNodeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(
                CacheNodePlanner {
                    cache_manager: Arc::clone(&self.cache_manager),
                },
            )]);
        physical_planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

// This naive implementation only includes put, but for real production use cases cache eviction and drop should also be implemented.
#[derive(Debug, Default)]
struct CacheManager {
    cache: HashMap<LogicalPlan, Vec<Vec<RecordBatch>>>,
}

impl CacheManager {
    pub fn put(&mut self, k: LogicalPlan, v: Vec<Vec<RecordBatch>>) {
        self.cache.insert(k, v);
    }

    pub fn get(&self, k: &LogicalPlan) -> Option<&Vec<Vec<RecordBatch>>> {
        self.cache.get(k)
    }
}
