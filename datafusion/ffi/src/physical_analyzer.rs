// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::ffi::c_void;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_optimizer::{PhysicalAnalyzerRule, PhysicalOptimizerContext};
use datafusion_physical_plan::ExecutionPlan;
use stabby::string::String as SString;
use tokio::runtime::Handle;

use crate::config::FFI_ConfigOptions;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::physical_optimizer::{FFI_PhysicalOptimizerContext, ForeignOptimizerContext};
use crate::util::FFI_Result;
use crate::{df_result, sresult_return};

/// A stable struct for sharing [`PhysicalAnalyzerRule`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalAnalyzerRule {
    pub analyze: unsafe extern "C" fn(
        &Self,
        plan: &FFI_ExecutionPlan,
        config: FFI_ConfigOptions,
    ) -> FFI_Result<FFI_ExecutionPlan>,

    pub analyze_with_context: unsafe extern "C" fn(
        &Self,
        plan: &FFI_ExecutionPlan,
        context: &FFI_PhysicalOptimizerContext,
    ) -> FFI_Result<FFI_ExecutionPlan>,

    pub name: unsafe extern "C" fn(&Self) -> SString,

    pub schema_check: unsafe extern "C" fn(&Self) -> bool,

    /// Used to create a clone on the rule. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this rule.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the rule.
    /// A [`ForeignPhysicalAnalyzerRule`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_PhysicalAnalyzerRule {}
unsafe impl Sync for FFI_PhysicalAnalyzerRule {}

struct RulePrivateData {
    rule: Arc<dyn PhysicalAnalyzerRule + Send + Sync>,
    runtime: Option<Handle>,
}

impl FFI_PhysicalAnalyzerRule {
    fn inner(&self) -> &Arc<dyn PhysicalAnalyzerRule + Send + Sync> {
        let private_data = self.private_data as *const RulePrivateData;
        unsafe { &(*private_data).rule }
    }

    fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const RulePrivateData;
        unsafe { (*private_data).runtime.clone() }
    }
}

unsafe extern "C" fn analyze_fn_wrapper(
    rule: &FFI_PhysicalAnalyzerRule,
    plan: &FFI_ExecutionPlan,
    config: FFI_ConfigOptions,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = rule.runtime();
    let rule = rule.inner();
    let plan: Arc<dyn ExecutionPlan> = sresult_return!(plan.try_into());
    let config = sresult_return!(ConfigOptions::try_from(config));
    let analyzed_plan = sresult_return!(rule.analyze(plan, &config));

    FFI_Result::Ok(FFI_ExecutionPlan::new(analyzed_plan, runtime))
}

unsafe extern "C" fn analyze_with_context_fn_wrapper(
    rule: &FFI_PhysicalAnalyzerRule,
    plan: &FFI_ExecutionPlan,
    context: &FFI_PhysicalOptimizerContext,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = rule.runtime();
    let inner = rule.inner();
    let plan: Arc<dyn ExecutionPlan> = sresult_return!(plan.try_into());
    let config = sresult_return!(ConfigOptions::try_from(unsafe {
        (context.config_options)(context)
    }));
    let foreign_ctx = ForeignOptimizerContext { config };
    let analyzed_plan = sresult_return!(inner.analyze_with_context(plan, &foreign_ctx));

    FFI_Result::Ok(FFI_ExecutionPlan::new(analyzed_plan, runtime))
}

unsafe extern "C" fn name_fn_wrapper(rule: &FFI_PhysicalAnalyzerRule) -> SString {
    rule.inner().name().into()
}

unsafe extern "C" fn schema_check_fn_wrapper(rule: &FFI_PhysicalAnalyzerRule) -> bool {
    rule.inner().schema_check()
}

unsafe extern "C" fn release_fn_wrapper(rule: &mut FFI_PhysicalAnalyzerRule) {
    unsafe {
        debug_assert!(!rule.private_data.is_null());
        let private_data = Box::from_raw(rule.private_data.cast::<RulePrivateData>());
        drop(private_data);
        rule.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    rule: &FFI_PhysicalAnalyzerRule,
) -> FFI_PhysicalAnalyzerRule {
    FFI_PhysicalAnalyzerRule::new(Arc::clone(rule.inner()), rule.runtime())
}

impl Drop for FFI_PhysicalAnalyzerRule {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_PhysicalAnalyzerRule {
    /// Creates a new [`FFI_PhysicalAnalyzerRule`].
    pub fn new(
        rule: Arc<dyn PhysicalAnalyzerRule + Send + Sync>,
        runtime: Option<Handle>,
    ) -> Self {
        if let Some(rule) = (Arc::clone(&rule) as Arc<dyn std::any::Any>)
            .downcast_ref::<ForeignPhysicalAnalyzerRule>()
        {
            return rule.rule.clone();
        }

        let private_data = Box::new(RulePrivateData { rule, runtime });
        let private_data = Box::into_raw(private_data).cast::<c_void>();

        Self {
            analyze: analyze_fn_wrapper,
            analyze_with_context: analyze_with_context_fn_wrapper,
            name: name_fn_wrapper,
            schema_check: schema_check_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Clone for FFI_PhysicalAnalyzerRule {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_PhysicalAnalyzerRule to interact with the foreign rule.
#[derive(Debug)]
pub struct ForeignPhysicalAnalyzerRule {
    name: String,
    rule: FFI_PhysicalAnalyzerRule,
}

unsafe impl Send for ForeignPhysicalAnalyzerRule {}
unsafe impl Sync for ForeignPhysicalAnalyzerRule {}

impl From<&FFI_PhysicalAnalyzerRule> for Arc<dyn PhysicalAnalyzerRule + Send + Sync> {
    fn from(rule: &FFI_PhysicalAnalyzerRule) -> Self {
        if (rule.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(rule.inner());
        }

        let name: String = unsafe { (rule.name)(rule).into() };
        Arc::new(ForeignPhysicalAnalyzerRule {
            name,
            rule: rule.clone(),
        }) as Arc<dyn PhysicalAnalyzerRule + Send + Sync>
    }
}

impl PhysicalAnalyzerRule for ForeignPhysicalAnalyzerRule {
    fn analyze(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config_options: FFI_ConfigOptions = config.into();
        let plan = FFI_ExecutionPlan::new(plan, None);

        let analyzed_plan = unsafe {
            df_result!((self.rule.analyze)(&self.rule, &plan, config_options))?
        };
        (&analyzed_plan).try_into()
    }

    fn analyze_with_context(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: &dyn PhysicalOptimizerContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ffi_context = FFI_PhysicalOptimizerContext::new(context);
        let plan = FFI_ExecutionPlan::new(plan, None);

        let analyzed_plan = unsafe {
            df_result!((self.rule.analyze_with_context)(
                &self.rule,
                &plan,
                &ffi_context
            ))?
        };
        (&analyzed_plan).try_into()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn schema_check(&self) -> bool {
        unsafe { (self.rule.schema_check)(&self.rule) }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_physical_optimizer::ConfigOnlyContext;

    use super::*;
    use crate::execution_plan::tests::EmptyExec;

    /// Fails through `analyze`, so a successful call proves the context path.
    #[derive(Debug)]
    struct ContextAwareRule {
        schema_check: bool,
    }

    impl PhysicalAnalyzerRule for ContextAwareRule {
        fn analyze(
            &self,
            _plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            datafusion_common::plan_err!("analyze should not be called directly")
        }

        fn analyze_with_context(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _context: &dyn PhysicalOptimizerContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(plan)
        }

        fn name(&self) -> &str {
            "context_aware_rule"
        }

        fn schema_check(&self) -> bool {
            self.schema_check
        }
    }

    fn foreign_rule(schema_check: bool) -> Arc<dyn PhysicalAnalyzerRule + Send + Sync> {
        let rule = Arc::new(ContextAwareRule { schema_check });
        let mut ffi_rule = FFI_PhysicalAnalyzerRule::new(rule, None);
        ffi_rule.library_marker_id = crate::mock_foreign_marker_id;
        (&ffi_rule).into()
    }

    #[test]
    fn test_round_trip_ffi_physical_analyzer_rule() -> Result<()> {
        for expected_schema_check in [true, false] {
            let rule = foreign_rule(expected_schema_check);
            let any_ref: &dyn std::any::Any = &*rule;
            assert!(any_ref.is::<ForeignPhysicalAnalyzerRule>());
            assert_eq!(rule.name(), "context_aware_rule");
            assert_eq!(rule.schema_check(), expected_schema_check);

            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
            let plan = Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>;
            let config = ConfigOptions::new();
            let context = ConfigOnlyContext::new(&config);
            let analyzed = rule.analyze_with_context(Arc::clone(&plan), &context)?;
            assert_eq!(analyzed.name(), "empty-exec");
            assert!(rule.analyze(plan, &config).is_err());

            // Wrapping the foreign rule again reuses its FFI struct.
            let rewrapped = FFI_PhysicalAnalyzerRule::new(rule, None);
            let name: String = unsafe { (rewrapped.name)(&rewrapped).into() };
            assert_eq!(name, "context_aware_rule");
        }

        Ok(())
    }

    #[test]
    fn test_local_bypass() {
        let rule = Arc::new(ContextAwareRule { schema_check: true });
        let ffi_rule = FFI_PhysicalAnalyzerRule::new(rule, None);
        let recovered: Arc<dyn PhysicalAnalyzerRule + Send + Sync> = (&ffi_rule).into();
        let any_ref: &dyn std::any::Any = &*recovered;
        assert!(any_ref.is::<ContextAwareRule>());
    }
}
