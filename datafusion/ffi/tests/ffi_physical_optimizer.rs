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

#[cfg(feature = "integration-tests")]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_common::config::ConfigOptions;
    use datafusion_ffi::execution_plan::tests::EmptyExec;
    use datafusion_ffi::physical_optimizer::ForeignPhysicalOptimizerRule;
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_plan::ExecutionPlan;

    fn create_test_plan() -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn test_ffi_physical_optimizer_rule() -> Result<(), DataFusionError> {
        let module = get_module()?;

        let ffi_rule = module.create_physical_optimizer_rule().ok_or(
            DataFusionError::NotImplemented(
                "External module failed to implement create_physical_optimizer_rule"
                    .to_string(),
            ),
        )?();

        let foreign_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            (&ffi_rule).into();

        // Verify the rule is wrapped as a foreign rule
        let any_ref: &dyn std::any::Any = &*foreign_rule;
        assert!(
            any_ref
                .downcast_ref::<ForeignPhysicalOptimizerRule>()
                .is_some()
        );

        // Verify name and schema_check pass through FFI
        assert_eq!(foreign_rule.name(), "add_limit_rule");
        assert!(foreign_rule.schema_check());

        // Verify the rule actually transforms the plan
        let plan = create_test_plan();
        let config = ConfigOptions::new();
        let optimized = foreign_rule.optimize(plan, &config)?;

        assert_eq!(optimized.name(), "GlobalLimitExec");
        assert_eq!(optimized.children().len(), 1);
        assert_eq!(optimized.children()[0].name(), "empty-exec");

        Ok(())
    }
}
