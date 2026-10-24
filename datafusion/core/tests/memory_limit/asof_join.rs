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

//! SQL-level ASOF execution with input sorting that spills to disk.

use std::sync::Arc;

use datafusion::assert_batches_eq;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_execution::disk_manager::DiskManagerBuilder;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;

fn sort_spill_count(plan: &dyn ExecutionPlan) -> usize {
    let own = if plan.name() == "SortExec" {
        plan.metrics()
            .and_then(|metrics| metrics.spill_count())
            .unwrap_or(0)
    } else {
        0
    };
    own + plan
        .children()
        .into_iter()
        .map(|child| sort_spill_count(child.as_ref()))
        .sum::<usize>()
}

#[tokio::test]
async fn partitioned_asof_join_streams_spilled_sort_output() -> Result<()> {
    let sql = "
        WITH left_input AS (
            SELECT value % 64 AS key, value / 64 + 1 AS ts
            FROM range(4096)
        ), right_input AS (
            SELECT value % 64 AS key, value / 64 AS ts, value AS payload
            FROM range(1048576)
        )
        SELECT count(*) AS n, sum(r.payload) AS total
        FROM left_input l
        ASOF JOIN right_input r MATCH_CONDITION (l.ts >= r.ts)
        ON l.key = r.key
    ";

    for partitioned in [false, true] {
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(1024)
            .with_sort_spill_reservation_bytes(256 * 1024)
            .with_sort_in_place_threshold_bytes(1024 * 1024);
        config.options_mut().optimizer.repartition_asof_joins = partitioned;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(8 * 1024 * 1024, 1.0)
            .with_disk_manager_builder(DiskManagerBuilder::default())
            .build_arc()?;
        let ctx = SessionContext::new_with_config_rt(config, runtime);

        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let result = collect(Arc::clone(&plan), ctx.task_ctx()).await;
        if !partitioned {
            let error = result.expect_err("broadcast should exceed the memory budget");
            assert!(error.to_string().contains("Resources exhausted"), "{error}");
            assert!(
                error.to_string().contains("AsOfJoinInput"),
                "broadcast failure should involve the collected right input: {error}"
            );
            continue;
        }

        let batches = result?;
        assert_batches_eq!(
            [
                "+------+---------+",
                "| n    | total   |",
                "+------+---------+",
                "| 4096 | 8648704 |",
                "+------+---------+",
            ],
            &batches
        );
        assert!(
            sort_spill_count(plan.as_ref()) > 0,
            "input sorts must spill"
        );
    }
    Ok(())
}
