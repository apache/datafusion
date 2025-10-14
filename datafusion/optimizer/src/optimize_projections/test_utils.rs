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

// Shared test-only utilities for optimize_projections

#[cfg(test)]
macro_rules! assert_optimized_plan_equal {
    (
        $plan:expr,
        @ $expected:literal $(,)?
    ) => {{
        let optimizer_ctx = $crate::OptimizerContext::new().with_max_passes(1);
        let rules: Vec<std::sync::Arc<dyn $crate::OptimizerRule + Send + Sync>> =
            vec![std::sync::Arc::new($crate::optimize_projections::OptimizeProjections::new())];
        $crate::assert_optimized_plan_eq_snapshot!(
            optimizer_ctx,
            rules,
            $plan,
            @ $expected,
        )
    }};
}
