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

use std::process::Command;
use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_expr::logical_plan::{EmptyRelation, LogicalPlan, LogicalPlanBuilder};
use datafusion_proto::bytes::logical_plan_to_bytes;

const CHILD_ENV: &str = "DATAFUSION_PROTO_ISSUE_23823_CHILD";
const ALIAS_DEPTH_ENV: &str = "DATAFUSION_PROTO_ISSUE_23823_ALIAS_DEPTH";
const TWO_MIB_TEST_NAME: &str =
    "cases::stack_safety::logical_plan_serialization_fits_a_two_mib_stack";
#[cfg(feature = "recursive_protection")]
const GROWABLE_STACK_TEST_NAME: &str =
    "cases::stack_safety::deeply_nested_logical_plan_serialization_uses_a_growable_stack";

fn deeply_aliased_plan(alias_depth: usize) -> LogicalPlan {
    let mut plan = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    });

    for level in 0..alias_depth {
        plan = LogicalPlanBuilder::from(plan)
            .alias(format!("level_{level}"))
            .unwrap()
            .build()
            .unwrap();
    }

    plan
}

fn serialize_on_two_mib_stack(alias_depth: usize) {
    let plan = deeply_aliased_plan(alias_depth);
    std::thread::Builder::new()
        .name("two-megabyte-stack".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(move || logical_plan_to_bytes(&plan).unwrap())
        .unwrap()
        .join()
        .unwrap();
}

fn run_in_child(test_name: &str, alias_depth: usize) {
    run_in_child_with(test_name, alias_depth, serialize_on_two_mib_stack);
}

fn run_in_child_with(test_name: &str, depth: usize, run: fn(usize)) {
    if std::env::var_os(CHILD_ENV).is_some() {
        let depth = std::env::var(ALIAS_DEPTH_ENV).unwrap().parse().unwrap();
        run(depth);
        return;
    }

    // A native stack overflow aborts the process. Re-run this exact test in a
    // child process so a regression produces a normal test failure.
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(CHILD_ENV, "1")
        .env(ALIAS_DEPTH_ENV, depth.to_string())
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "child process failed with status {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn logical_plan_serialization_fits_a_two_mib_stack() {
    // Ten aliases reproduce #23823. Use 100 to provide a safety margin while
    // verifying the dispatcher reduction without runtime stack growth.
    run_in_child(TWO_MIB_TEST_NAME, 100);
}

#[cfg(feature = "recursive_protection")]
#[test]
fn deeply_nested_logical_plan_serialization_uses_a_growable_stack() {
    // This depth exceeds the 2 MiB thread stack without recursive protection,
    // exercising the `recursive` stack-growth checkpoint.
    run_in_child(GROWABLE_STACK_TEST_NAME, 2_000);
}

// Physical plan serde (#26738): nested plans go through
// `try_from_physical_plan_with_converter` / `try_into_physical_plan_with_context`
// recursively, so a deep plan needs the same stack-growth checkpoint.
#[cfg(feature = "recursive_protection")]
mod physical {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::expressions::{NotExpr, lit};
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
    use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
    use datafusion_proto::physical_plan::{
        AsExecutionPlan, DefaultPhysicalExtensionCodec,
    };
    use datafusion_proto::protobuf::PhysicalPlanNode;

    use super::run_in_child_with;

    const TEST_NAME: &str = "cases::stack_safety::physical::deeply_nested_physical_plan_roundtrip_uses_a_growable_stack";

    fn deeply_nested_plan(depth: usize) -> Arc<dyn ExecutionPlan> {
        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        for _ in 0..depth {
            plan = Arc::new(GlobalLimitExec::new(plan, 0, Some(1)));
        }
        plan
    }

    fn roundtrip_on_two_mib_stack(depth: usize) {
        std::thread::Builder::new()
            .name("two-megabyte-stack".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let codec = DefaultPhysicalExtensionCodec {};
                let proto = PhysicalPlanNode::try_from_physical_plan(
                    deeply_nested_plan(depth),
                    &codec,
                )
                .unwrap();
                let ctx = SessionContext::new();
                proto
                    .try_into_physical_plan(&ctx.task_ctx(), &codec)
                    .unwrap();
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn deeply_nested_physical_plan_roundtrip_uses_a_growable_stack() {
        run_in_child_with(TEST_NAME, 2_000, roundtrip_on_two_mib_stack);
    }

    const EXPR_TEST_NAME: &str = "cases::stack_safety::physical::deeply_nested_physical_expr_roundtrip_uses_a_growable_stack";

    fn expr_roundtrip_on_two_mib_stack(depth: usize) {
        std::thread::Builder::new()
            .name("two-megabyte-stack".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let mut expr: Arc<dyn PhysicalExpr> = lit(true);
                for _ in 0..depth {
                    expr = Arc::new(NotExpr::new(expr));
                }
                let codec = DefaultPhysicalExtensionCodec {};
                let proto = serialize_physical_expr(&expr, &codec).unwrap();
                let ctx = SessionContext::new();
                parse_physical_expr(&proto, &ctx.task_ctx(), &Schema::empty(), &codec)
                    .unwrap();
            })
            .unwrap()
            .join()
            .unwrap();
    }

    #[test]
    fn deeply_nested_physical_expr_roundtrip_uses_a_growable_stack() {
        run_in_child_with(EXPR_TEST_NAME, 2_000, expr_roundtrip_on_two_mib_stack);
    }
}
