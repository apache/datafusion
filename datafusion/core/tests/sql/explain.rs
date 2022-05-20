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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::test_util::scan_empty;
use datafusion::{
    logical_plan::{LogicalPlan, PlanType},
    prelude::SessionContext,
};

#[test]
fn optimize_explain() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);

    let plan = scan_empty(Some("employee"), &schema, None)
        .unwrap()
        .explain(true, false)
        .unwrap()
        .build()
        .unwrap();

    if let LogicalPlan::Explain(e) = &plan {
        assert_eq!(e.stringified_plans.len(), 1);
    } else {
        panic!("plan was not an explain: {:?}", plan);
    }

    // now optimize the plan and expect to see more plans
    let optimized_plan = SessionContext::new().optimize(&plan).unwrap();
    if let LogicalPlan::Explain(e) = &optimized_plan {
        // should have more than one plan
        assert!(
            e.stringified_plans.len() > 1,
            "plans: {:#?}",
            e.stringified_plans
        );
        // should have at least one optimized plan
        let opt = e
            .stringified_plans
            .iter()
            .any(|p| matches!(p.plan_type, PlanType::OptimizedLogicalPlan { .. }));

        assert!(opt, "plans: {:#?}", e.stringified_plans);
    } else {
        panic!("plan was not an explain: {:?}", plan);
    }
}
