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

//! Execution context: session config and memory pool per case.

use super::*;

pub(super) fn task_context(case: &Case) -> Arc<TaskContext> {
    let config = SessionConfig::new()
        .with_batch_size(BATCH_SIZE)
        .with_target_partitions(PARTITIONS)
        // The default is 100k rows. Lower it so the skip-partial probe can
        // fire on our per-partition row counts. A ratio threshold of 1.0
        // disables the probe entirely.
        .set_usize(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            1024,
        );
    let mut config = config;
    config
        .options_mut()
        .execution
        .skip_partial_aggregation_probe_ratio_threshold =
        if case.params.skip_partial_enabled {
            0.8
        } else {
            1.0
        };

    let runtime = match case.params.memory {
        // Not using UnboundedMemoryPool, so users would still think that we have a valid pool, but just with enough memory
        Memory::Unlimited => RuntimeEnvBuilder::new().with_memory_limit(usize::MAX, 1.0),
        // Small enough that a very-high-cardinality final table spills, large
        // enough that the legacy stream can still reserve its sort headroom
        // and that RepartitionExec / SortPreservingMergeExec succeed. The
        // fair pool keeps one stage from starving the others.
        Memory::Limited => {
            RuntimeEnvBuilder::new().with_memory_pool(Arc::new(TrackConsumersPool::new(
                FairSpillPool::new(LIMITED_POOL_BYTES),
                NonZeroUsize::new(5).unwrap(),
            )))
        }
    }
    .build_arc()
    .unwrap();

    Arc::new(
        TaskContext::default()
            .with_session_config(config)
            .with_runtime(runtime),
    )
}
