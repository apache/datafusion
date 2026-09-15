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

use datafusion::prelude::SessionContext;

/// Default per-channel frame capacity (backpressure bound). A small constant:
/// large enough to pipeline without pointless blocking, small enough to keep
/// the streaming model honest.
const DEFAULT_CHANNEL_CAPACITY: usize = 16;

#[derive(Clone)]
pub struct SchedulerConfig {
    /// Rebuilds an isolated SessionContext per task (UDFs, object stores, config).
    pub session_builder: Arc<dyn Fn() -> SessionContext + Send + Sync>,
    /// Per-channel frame capacity for the in-memory exchange (backpressure
    /// bound). Bounded channels give the streaming analogue of a memory budget:
    /// a fast producer blocks until the consumer drains.
    pub channel_capacity: usize,
}

impl SchedulerConfig {
    /// Convenience for tests: a `session_builder` that clones the driver
    /// context's state (so registered tables/UDFs survive rebuild) plus a
    /// sensible default `channel_capacity`.
    pub fn in_memory(ctx: &SessionContext) -> Self {
        let state = ctx.state();
        Self {
            session_builder: Arc::new(move || {
                SessionContext::new_with_state(state.clone())
            }),
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}
