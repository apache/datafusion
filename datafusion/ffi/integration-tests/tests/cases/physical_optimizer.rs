// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::DataFusionError;
    use datafusion_common::config::ConfigOptions;
    use datafusion_ffi::execution_plan::tests::EmptyExec;
    use datafusion_ffi::physical_optimizer::ForeignPhysicalOptimizerRule;
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_physical_optimizer::{ConfigOnlyContext, PhysicalOptimizerRule};
    use datafusion_physical_plan::ExecutionPlan;

    fn create_test_plan() -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn test_ffi_physical_optimizer_rule() -> Result<(), DataFusionError> {
        let module = get_module()?;

        let ffi_rule = (module.create_physical_optimizer_rule)();

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

    #[test]
    fn test_ffi_physical_optimizer_rule_with_context() -> Result<(), DataFusionError> {
        let module = get_module()?;

        let ffi_rule = (module.create_context_aware_optimizer_rule)();

        let foreign_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            (&ffi_rule).into();

        // Verify that plain optimize fails (proving we need context path)
        let plan = create_test_plan();
        let config = ConfigOptions::new();
        assert!(foreign_rule.optimize(plan, &config).is_err());

        // Verify context-aware path works
        let plan = create_test_plan();
        let context = ConfigOnlyContext::new(&config);
        let optimized = foreign_rule.optimize_with_context(plan, &context)?;

        assert_eq!(optimized.name(), "GlobalLimitExec");
        assert_eq!(optimized.children().len(), 1);
        assert_eq!(optimized.children()[0].name(), "empty-exec");

        Ok(())
    }
}
