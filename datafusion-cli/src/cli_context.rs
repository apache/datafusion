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

use std::num::NonZeroUsize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{
        context::SessionState,
        memory_pool::{MemoryPool, TrackConsumersPool, TrackedPool},
        runtime_env::RuntimeEnvBuilder,
        session_state::SessionStateBuilder,
        TaskContext,
    },
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};
use object_store::ObjectStore;

use crate::object_storage::{AwsOptions, GcpOptions};

#[async_trait::async_trait]
/// The CLI session context trait provides a way to have a session context that can be used with datafusion's CLI code.
pub trait CliSessionContext {
    /// Get an atomic reference counted task context.
    fn task_ctx(&self) -> Arc<TaskContext>;

    /// Get the session state.
    fn session_state(&self) -> SessionState;

    /// Register an object store with the session context.
    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>>;

    /// Register table options extension from scheme.
    fn register_table_options_extension_from_scheme(&self, scheme: &str);

    /// Execute a logical plan and return a DataFrame.
    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError>;

    /// Return true if memory profiling is enabled.
    fn memory_profiling(&self) -> bool {
        false
    }

    /// Enable or disable memory profiling.
    fn set_memory_profiling(&self, _enable: bool) {}

    /// Return the tracked memory pool used for profiling, if any.
    fn tracked_memory_pool(&self) -> Option<Arc<dyn TrackedPool>> {
        None
    }
}

#[async_trait::async_trait]
impl CliSessionContext for SessionContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.state()
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>> {
        self.register_object_store(url, object_store)
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        match scheme {
            // For Amazon S3 or Alibaba Cloud OSS
            "s3" | "oss" | "cos" => {
                // Register AWS specific table options in the session context:
                self.register_table_options_extension(AwsOptions::default())
            }
            // For Google Cloud Storage
            "gs" | "gcs" => {
                // Register GCP specific table options in the session context:
                self.register_table_options_extension(GcpOptions::default())
            }
            // For unsupported schemes, do nothing:
            _ => {}
        }
    }

    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError> {
        SessionContext::execute_logical_plan(self, plan).await
    }
}

/// Session context used by the CLI with memory profiling support.
pub struct ReplSessionContext {
    ctx: SessionContext,
    memory_profiling: AtomicBool,
    base_memory_pool: Arc<dyn MemoryPool>,
    tracked_memory_pool: RwLock<Option<Arc<dyn TrackedPool>>>,
    top_memory_consumers: usize,
}

impl ReplSessionContext {
    pub fn new(
        ctx: SessionContext,
        base_memory_pool: Arc<dyn MemoryPool>,
        top_memory_consumers: usize,
    ) -> Self {
        Self {
            ctx,
            memory_profiling: AtomicBool::new(false),
            base_memory_pool,
            tracked_memory_pool: RwLock::new(None),
            top_memory_consumers,
        }
    }
}

#[async_trait::async_trait]
impl CliSessionContext for ReplSessionContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.ctx.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.ctx.state()
    }

    fn register_object_store(
        &self,
        url: &url::Url,
        object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>> {
        self.ctx.register_object_store(url, object_store)
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        match scheme {
            // For Amazon S3 or Alibaba Cloud OSS
            "s3" | "oss" | "cos" => self
                .ctx
                .register_table_options_extension(AwsOptions::default()),
            // For Google Cloud Storage
            "gs" | "gcs" => self
                .ctx
                .register_table_options_extension(GcpOptions::default()),
            // For unsupported schemes, do nothing:
            _ => {}
        }
    }

    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError> {
        self.ctx.execute_logical_plan(plan).await
    }

    fn memory_profiling(&self) -> bool {
        self.memory_profiling.load(Ordering::Relaxed)
    }

    fn set_memory_profiling(&self, enable: bool) {
        if enable {
            if self.top_memory_consumers == 0 {
                return;
            }
            if self.memory_profiling.swap(true, Ordering::Relaxed) {
                return;
            }
            let tracked = Arc::new(TrackConsumersPool::new(
                Arc::clone(&self.base_memory_pool),
                NonZeroUsize::new(self.top_memory_consumers).unwrap(),
            ));
            let runtime = self.ctx.runtime_env();
            let builder = RuntimeEnvBuilder::from_runtime_env(runtime.as_ref());
            let runtime = Arc::new(
                builder
                    .with_memory_pool(tracked.clone() as Arc<dyn MemoryPool>)
                    .build()
                    .unwrap(),
            );
            let state_ref = self.ctx.state_ref();
            let mut state = state_ref.write();
            *state = SessionStateBuilder::from(state.clone())
                .with_runtime_env(runtime)
                .build();
            *self.tracked_memory_pool.write().unwrap() =
                Some(tracked as Arc<dyn TrackedPool>);
        } else {
            if !self.memory_profiling.swap(false, Ordering::Relaxed) {
                return;
            }
            let runtime = self.ctx.runtime_env();
            let builder = RuntimeEnvBuilder::from_runtime_env(runtime.as_ref());
            let runtime = Arc::new(
                builder
                    .with_memory_pool(self.base_memory_pool.clone())
                    .build()
                    .unwrap(),
            );
            let state_ref = self.ctx.state_ref();
            let mut state = state_ref.write();
            *state = SessionStateBuilder::from(state.clone())
                .with_runtime_env(runtime)
                .build();
            *self.tracked_memory_pool.write().unwrap() = None;
        }
    }

    fn tracked_memory_pool(&self) -> Option<Arc<dyn TrackedPool>> {
        self.tracked_memory_pool.read().unwrap().clone()
    }
}
