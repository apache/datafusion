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

//! Context (remote or local)

use ballista::context::BallistaContext;
use ballista::prelude::BallistaConfig;
use datafusion::dataframe::DataFrame;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

/// The CLI supports using a local DataFusion context or a distributed BallistaContext
pub enum Context {
    /// In-process execution with DataFusion
    Local(ExecutionContext),
    /// Distributed execution with Ballista
    Remote(BallistaContext),
}

impl Context {
    /// create a new remote context with given host and port
    pub fn new_remote(host: &str, port: u16) -> Result<Context> {
        let config: BallistaConfig = BallistaConfig::new()
            .map_err(|e| DataFusionError::Execution(format!("{:?}", e)))?;
        Ok(Context::Remote(BallistaContext::remote(
            host, port, &config,
        )))
    }

    /// create a local context using the given config
    pub fn new_local(config: &ExecutionConfig) -> Context {
        Context::Local(ExecutionContext::with_config(config.clone()))
    }

    /// execute an SQL statement against the context
    pub async fn sql(&mut self, sql: &str) -> Result<Arc<dyn DataFrame>> {
        match self {
            Context::Local(datafusion) => datafusion.sql(sql).await,
            Context::Remote(ballista) => ballista.sql(sql).await,
        }
    }
}
