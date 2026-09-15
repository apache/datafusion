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

use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;

use crate::config::SchedulerConfig;
use crate::run_distributed;

/// Assert distributed execution yields the same multiset of rows as `collect`.
pub async fn assert_distributed_eq(ctx: &SessionContext, plan: Arc<dyn ExecutionPlan>) {
    let expected = collect(plan.clone(), ctx.task_ctx())
        .await
        .expect("collect");
    let actual = run_distributed(ctx, plan, SchedulerConfig::in_memory(ctx))
        .await
        .expect("run_distributed");
    // Compare as sorted string rows via pretty-format for schema-agnostic equality.
    let e = arrow::util::pretty::pretty_format_batches(&expected)
        .unwrap()
        .to_string();
    let a = arrow::util::pretty::pretty_format_batches(&actual)
        .unwrap()
        .to_string();
    let mut el: Vec<&str> = e.lines().collect();
    el.sort();
    let mut al: Vec<&str> = a.lines().collect();
    al.sort();
    assert_eq!(el, al, "distributed output != collect output");
}
