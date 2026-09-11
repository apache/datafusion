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

use std::sync::Arc;

use datafusion::storage::{Storage, StorageBinding};
use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{TaskContext, context::SessionState},
    logical_expr::LogicalPlan,
    prelude::SessionContext,
};

use crate::object_storage::{AwsOptions, GcpOptions};

#[async_trait::async_trait]
/// The CLI session context trait provides a way to have a session context that can be used with datafusion's CLI code.
pub trait CliSessionContext {
    /// Get an atomic reference counted task context.
    fn task_ctx(&self) -> Arc<TaskContext>;

    /// Get the session state.
    fn session_state(&self) -> SessionState;

    /// Register an object store with the session context.
    fn register_storage(
        &self,
        url: &url::Url,
        storage: Arc<dyn Storage>,
    ) -> Result<Option<Arc<StorageBinding>>, DataFusionError>;

    /// Register table options extension from scheme.
    fn register_table_options_extension_from_scheme(&self, scheme: &str);

    /// Execute a logical plan and return a DataFrame.
    async fn execute_logical_plan(
        &self,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError>;
}

#[async_trait::async_trait]
impl CliSessionContext for SessionContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.state()
    }

    fn register_storage(
        &self,
        url: &url::Url,
        storage: Arc<dyn Storage>,
    ) -> Result<Option<Arc<StorageBinding>>, DataFusionError> {
        self.register_storage(url, storage)
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
        self.execute_logical_plan(plan).await
    }
}
