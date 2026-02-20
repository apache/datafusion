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

use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RStr};
use async_trait::async_trait;
use datafusion_common::config::ConfigOptions;
use datafusion_common::error::Result;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use tokio::runtime::Handle;

use crate::config::FFI_ConfigOptions;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::util::FFIResult;
use crate::{df_result, rresult_return};

/// A stable struct for sharing [`PhysicalOptimizerRule`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_PhysicalOptimizerRule {
    pub optimize: unsafe extern "C" fn(
        &Self,
        plan: &FFI_ExecutionPlan,
        config: FFI_ConfigOptions,
    ) -> FFIResult<FFI_ExecutionPlan>,

    pub name: unsafe extern "C" fn(&Self) -> RStr,

    pub schema_check: unsafe extern "C" fn(&Self) -> bool,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
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
) -> FFIResult<FFI_ExecutionPlan> {
    let runtime = rule.runtime();
    let rule = rule.inner();
    let plan: Arc<dyn ExecutionPlan> = rresult_return!(plan.try_into());
    let config = rresult_return!(ConfigOptions::try_from(config));
    let optimized_plan = rresult_return!(rule.optimize(plan, &config));

    RResult::ROk(FFI_ExecutionPlan::new(optimized_plan, runtime))
}

unsafe extern "C" fn name_fn_wrapper(rule: &FFI_PhysicalOptimizerRule) -> RStr<'_> {
    let rule = rule.inner();
    rule.name().into()
}

unsafe extern "C" fn schema_check_fn_wrapper(rule: &FFI_PhysicalOptimizerRule) -> bool {
    rule.inner().schema_check()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_PhysicalOptimizerRule) {
    let private_data =
        unsafe { Box::from_raw(provider.private_data as *mut RulePrivateData) };
    drop(private_data);
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
            return rule.0.clone();
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
/// FFI_PhysicalOptimizerRule to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignPhysicalOptimizerRule(pub FFI_PhysicalOptimizerRule);

unsafe impl Send for ForeignPhysicalOptimizerRule {}
unsafe impl Sync for ForeignPhysicalOptimizerRule {}

impl From<&FFI_PhysicalOptimizerRule> for Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    fn from(provider: &FFI_PhysicalOptimizerRule) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(provider.inner());
        }

        Arc::new(ForeignPhysicalOptimizerRule(provider.clone()))
            as Arc<dyn PhysicalOptimizerRule + Send + Sync>
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

        let optimized_plan =
            unsafe { df_result!((self.0.optimize)(&self.0, &plan, config_options))? };
        (&optimized_plan).try_into()
    }

    fn name(&self) -> &str {
        unsafe { (self.0.name)(&self.0).as_str() }
    }

    fn schema_check(&self) -> bool {
        unsafe { (self.0.schema_check)(&self.0) }
    }
}
