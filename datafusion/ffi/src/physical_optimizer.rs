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

use std::ffi::c_void;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use stabby::string::String as SString;
use tokio::runtime::Handle;

use crate::config::FFI_ConfigOptions;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::util::FFI_Result;
use crate::{df_result, sresult_return};

/// A stable struct for sharing [`PhysicalOptimizerRule`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalOptimizerRule {
    pub optimize: unsafe extern "C" fn(
        &Self,
        plan: &FFI_ExecutionPlan,
        config: FFI_ConfigOptions,
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
    /// A [`ForeignPhysicalOptimizerRule`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_PhysicalOptimizerRule {}
unsafe impl Sync for FFI_PhysicalOptimizerRule {}

struct RulePrivateData {
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    runtime: Option<Handle>,
}

impl FFI_PhysicalOptimizerRule {
    fn inner(&self) -> &Arc<dyn PhysicalOptimizerRule + Send + Sync> {
        let private_data = self.private_data as *const RulePrivateData;
        unsafe { &(*private_data).rule }
    }

    fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const RulePrivateData;
        unsafe { (*private_data).runtime.clone() }
    }
}

unsafe extern "C" fn optimize_fn_wrapper(
    rule: &FFI_PhysicalOptimizerRule,
    plan: &FFI_ExecutionPlan,
    config: FFI_ConfigOptions,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = rule.runtime();
    let rule = rule.inner();
    let plan: Arc<dyn ExecutionPlan> = sresult_return!(plan.try_into());
    let config = sresult_return!(ConfigOptions::try_from(config));
    let optimized_plan = sresult_return!(rule.optimize(plan, &config));

    FFI_Result::Ok(FFI_ExecutionPlan::new(optimized_plan, runtime))
}

unsafe extern "C" fn name_fn_wrapper(rule: &FFI_PhysicalOptimizerRule) -> SString {
    let rule = rule.inner();
    rule.name().into()
}

unsafe extern "C" fn schema_check_fn_wrapper(rule: &FFI_PhysicalOptimizerRule) -> bool {
    rule.inner().schema_check()
}

unsafe extern "C" fn release_fn_wrapper(rule: &mut FFI_PhysicalOptimizerRule) {
    unsafe {
        debug_assert!(!rule.private_data.is_null());
        let private_data = Box::from_raw(rule.private_data as *mut RulePrivateData);
        drop(private_data);
        rule.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    rule: &FFI_PhysicalOptimizerRule,
) -> FFI_PhysicalOptimizerRule {
    let runtime = rule.runtime();
    let rule = Arc::clone(rule.inner());

    let private_data =
        Box::into_raw(Box::new(RulePrivateData { rule, runtime })) as *mut c_void;

    FFI_PhysicalOptimizerRule {
        optimize: optimize_fn_wrapper,
        name: name_fn_wrapper,
        schema_check: schema_check_fn_wrapper,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        library_marker_id: crate::get_library_marker_id,
    }
}

impl Drop for FFI_PhysicalOptimizerRule {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_PhysicalOptimizerRule {
    /// Creates a new [`FFI_PhysicalOptimizerRule`].
    pub fn new(
        rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
        runtime: Option<Handle>,
    ) -> Self {
        if let Some(rule) = (Arc::clone(&rule) as Arc<dyn std::any::Any>)
            .downcast_ref::<ForeignPhysicalOptimizerRule>()
        {
            return rule.rule.clone();
        }

        let private_data = Box::new(RulePrivateData { rule, runtime });
        let private_data = Box::into_raw(private_data) as *mut c_void;

        Self {
            optimize: optimize_fn_wrapper,
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

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_PhysicalOptimizerRule to interact with the foreign rule.
#[derive(Debug)]
pub struct ForeignPhysicalOptimizerRule {
    name: String,
    rule: FFI_PhysicalOptimizerRule,
}

unsafe impl Send for ForeignPhysicalOptimizerRule {}
unsafe impl Sync for ForeignPhysicalOptimizerRule {}

impl From<&FFI_PhysicalOptimizerRule> for Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    fn from(rule: &FFI_PhysicalOptimizerRule) -> Self {
        if (rule.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(rule.inner());
        }

        let name: String = unsafe { (rule.name)(rule).into() };
        Arc::new(ForeignPhysicalOptimizerRule {
            name,
            rule: rule.clone(),
        }) as Arc<dyn PhysicalOptimizerRule + Send + Sync>
    }
}

impl Clone for FFI_PhysicalOptimizerRule {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl PhysicalOptimizerRule for ForeignPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let config_options: FFI_ConfigOptions = config.into();
        let plan = FFI_ExecutionPlan::new(plan, None);

        let optimized_plan = unsafe {
            df_result!((self.rule.optimize)(&self.rule, &plan, config_options))?
        };
        (&optimized_plan).try_into()
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
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::error::Result;
    use datafusion_physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_plan::ExecutionPlan;

    use super::*;
    use crate::execution_plan::tests::EmptyExec;

    #[derive(Debug)]
    struct NoOpRule {
        schema_check: bool,
    }

    impl PhysicalOptimizerRule for NoOpRule {
        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _config: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(plan)
        }

        fn name(&self) -> &str {
            "no_op_rule"
        }

        fn schema_check(&self) -> bool {
            self.schema_check
        }
    }

    fn create_test_plan() -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        Arc::new(EmptyExec::new(schema))
    }

    #[test]
    fn test_round_trip_ffi_physical_optimizer_rule() -> Result<()> {
        for expected_schema_check in [true, false] {
            let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> = Arc::new(NoOpRule {
                schema_check: expected_schema_check,
            });

            let mut ffi_rule = FFI_PhysicalOptimizerRule::new(rule, None);
            ffi_rule.library_marker_id = crate::mock_foreign_marker_id;

            let foreign_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
                (&ffi_rule).into();

            assert_eq!(foreign_rule.name(), "no_op_rule");
            assert_eq!(foreign_rule.schema_check(), expected_schema_check);
        }

        Ok(())
    }

    #[test]
    fn test_round_trip_optimize() -> Result<()> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(NoOpRule { schema_check: true });

        let mut ffi_rule = FFI_PhysicalOptimizerRule::new(rule, None);
        ffi_rule.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            (&ffi_rule).into();

        let plan = create_test_plan();
        let config = ConfigOptions::new();

        let optimized = foreign_rule.optimize(plan, &config)?;
        assert_eq!(optimized.name(), "empty-exec");

        Ok(())
    }

    #[test]
    fn test_local_bypass() -> Result<()> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(NoOpRule { schema_check: true });

        // Without mock marker, local bypass should return the original rule
        let ffi_rule = FFI_PhysicalOptimizerRule::new(rule, None);
        let recovered: Arc<dyn PhysicalOptimizerRule + Send + Sync> = (&ffi_rule).into();
        let any_ref: &dyn std::any::Any = &*recovered;
        assert!(any_ref.downcast_ref::<NoOpRule>().is_some());

        // With mock marker, should wrap in ForeignPhysicalOptimizerRule
        let rule2: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(NoOpRule { schema_check: true });
        let mut ffi_rule2 = FFI_PhysicalOptimizerRule::new(rule2, None);
        ffi_rule2.library_marker_id = crate::mock_foreign_marker_id;
        let recovered2: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            (&ffi_rule2).into();
        let any_ref2: &dyn std::any::Any = &*recovered2;
        assert!(
            any_ref2
                .downcast_ref::<ForeignPhysicalOptimizerRule>()
                .is_some()
        );

        Ok(())
    }

    #[test]
    fn test_clone() -> Result<()> {
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(NoOpRule { schema_check: true });

        let ffi_rule = FFI_PhysicalOptimizerRule::new(rule, None);
        let cloned = ffi_rule.clone();

        let name1: String = unsafe { (ffi_rule.name)(&ffi_rule).into() };
        let name2: String = unsafe { (cloned.name)(&cloned).into() };
        assert_eq!(name1, name2);

        Ok(())
    }

    #[test]
    fn test_foreign_rule_rewrap_bypass() -> Result<()> {
        // When creating an FFI wrapper from a ForeignPhysicalOptimizerRule,
        // it should return the inner FFI rule rather than double-wrapping.
        let rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            Arc::new(NoOpRule { schema_check: true });

        let mut ffi_rule = FFI_PhysicalOptimizerRule::new(rule, None);
        ffi_rule.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> =
            (&ffi_rule).into();

        // Now wrap the foreign rule back into FFI - should not double-wrap
        let re_wrapped = FFI_PhysicalOptimizerRule::new(foreign_rule, None);
        let name: String = unsafe { (re_wrapped.name)(&re_wrapped).into() };
        assert_eq!(name, "no_op_rule");

        Ok(())
    }
}
