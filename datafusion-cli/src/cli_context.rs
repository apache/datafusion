// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use datafusion::{
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{TaskContext, context::SessionState},
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
        self.execute_logical_plan(plan).await
    }
}
