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
use std::pin::Pin;
use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use tokio::runtime::Handle;

use crate::config::FFI_ConfigOptions;
use crate::execution::FFI_TaskContext;
use crate::plan_properties::FFI_PlanProperties;
use crate::record_batch_stream::FFI_RecordBatchStream;
use crate::util::FFIResult;
use crate::{df_result, rresult, rresult_return};

/// A stable struct for sharing a [`ExecutionPlan`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_ExecutionPlan {
    /// Return the plan properties
    pub properties: unsafe extern "C" fn(plan: &Self) -> FFI_PlanProperties,

    /// Return a vector of children plans
    pub children: unsafe extern "C" fn(plan: &Self) -> RVec<FFI_ExecutionPlan>,

    pub with_new_children:
        unsafe extern "C" fn(plan: &Self, children: RVec<Self>) -> FFIResult<Self>,

    /// Return the plan name.
    pub name: unsafe extern "C" fn(plan: &Self) -> RString,

    /// Execute the plan and return a record batch stream. Errors
    /// will be returned as a string.
    pub execute: unsafe extern "C" fn(
        plan: &Self,
        partition: usize,
        context: FFI_TaskContext,
    ) -> FFIResult<FFI_RecordBatchStream>,

    pub repartitioned: unsafe extern "C" fn(
        plan: &Self,
        target_partitions: usize,
        config: FFI_ConfigOptions,
    ) -> FFIResult<ROption<FFI_ExecutionPlan>>,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignExecutionPlan`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ExecutionPlan {}
unsafe impl Sync for FFI_ExecutionPlan {}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan>,
    pub runtime: Option<Handle>,
}

impl FFI_ExecutionPlan {
    fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        let private_data = self.private_data as *const ExecutionPlanPrivateData;
        unsafe { &(*private_data).plan }
    }

    fn runtime(&self) -> Option<Handle> {
        let private_data = self.private_data as *const ExecutionPlanPrivateData;
        unsafe { (*private_data).runtime.clone() }
    }
}

unsafe extern "C" fn properties_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> FFI_PlanProperties {
    plan.inner().properties().into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> RVec<FFI_ExecutionPlan> {
    let runtime = plan.runtime();
    let plan = plan.inner();

    let children: Vec<_> = plan
        .children()
        .into_iter()
        .map(|child| FFI_ExecutionPlan::new(Arc::clone(child), runtime.clone()))
        .collect();

    children.into()
}

unsafe extern "C" fn with_new_children_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    children: RVec<FFI_ExecutionPlan>,
) -> FFIResult<FFI_ExecutionPlan> {
    let runtime = plan.runtime();
    let plan = Arc::clone(plan.inner());
    let children = rresult_return!(
        children
            .iter()
            .map(<Arc<dyn ExecutionPlan>>::try_from)
            .collect::<Result<Vec<_>>>()
    );

    let new_plan = rresult_return!(plan.with_new_children(children));

    RResult::ROk(FFI_ExecutionPlan::new(new_plan, runtime))
}

unsafe extern "C" fn execute_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    partition: usize,
    context: FFI_TaskContext,
) -> FFIResult<FFI_RecordBatchStream> {
    let ctx = context.into();
    let runtime = plan.runtime();
    let plan = plan.inner();

    let _guard = runtime.as_ref().map(|rt| rt.enter());

    rresult!(
        plan.execute(partition, ctx)
            .map(|rbs| FFI_RecordBatchStream::new(rbs, runtime))
    )
}

unsafe extern "C" fn repartitioned_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    target_partitions: usize,
    config: FFI_ConfigOptions,
) -> FFIResult<ROption<FFI_ExecutionPlan>> {
    let maybe_config: Result<ConfigOptions, DataFusionError> = config.try_into();
    let config = rresult_return!(maybe_config);
    let runtime = plan.runtime();
    let plan = plan.inner();

    rresult!(
        plan.repartitioned(target_partitions, &config)
            .map(|maybe_plan| maybe_plan
                .map(|plan| FFI_ExecutionPlan::new(plan, runtime))
                .into())
    )
}

unsafe extern "C" fn name_fn_wrapper(plan: &FFI_ExecutionPlan) -> RString {
    plan.inner().name().into()
}

unsafe extern "C" fn release_fn_wrapper(plan: &mut FFI_ExecutionPlan) {
    unsafe {
        debug_assert!(!plan.private_data.is_null());
        let private_data =
            Box::from_raw(plan.private_data as *mut ExecutionPlanPrivateData);
        drop(private_data);
        plan.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(plan: &FFI_ExecutionPlan) -> FFI_ExecutionPlan {
    let runtime = plan.runtime();
    let plan = plan.inner();

    FFI_ExecutionPlan::new(Arc::clone(plan), runtime)
}

impl Clone for FFI_ExecutionPlan {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

/// Helper function to recursively identify any children that do not
/// have a runtime set but should because they are local to this same
/// library. This does imply a restriction that all execution plans
/// in this chain that are within the same library use the same runtime.
fn pass_runtime_to_children(
    plan: &Arc<dyn ExecutionPlan>,
    runtime: &Handle,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    println!("checking plan {:?}", plan.name());
    let mut updated_children = false;
    let plan_is_foreign = plan.as_any().is::<ForeignExecutionPlan>();

    let children = plan
        .children()
        .into_iter()
        .map(|child| {
            let child = match pass_runtime_to_children(child, runtime)? {
                Some(child) => {
                    updated_children = true;
                    child
                }
                None => Arc::clone(child),
            };

            // If the parent is foreign and the child is local to this library, then when
            // we called `children()` above we will get something other than a
            // `ForeignExecutionPlan`. In this case wrap the plan in a `ForeignExecutionPlan`
            // because when we call `with_new_children` below it will extract the
            // FFI plan that does contain the runtime.
            if plan_is_foreign && !child.as_any().is::<ForeignExecutionPlan>() {
                updated_children = true;
                let ffi_child = FFI_ExecutionPlan::new(child, Some(runtime.clone()));
                let foreign_child = ForeignExecutionPlan::try_from(ffi_child);
                foreign_child.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
            } else {
                Ok(child)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    if updated_children {
        Arc::clone(plan).with_new_children(children).map(Some)
    } else {
        Ok(None)
    }
}

impl FFI_ExecutionPlan {
    /// This function is called on the provider's side.
    pub fn new(mut plan: Arc<dyn ExecutionPlan>, runtime: Option<Handle>) -> Self {
        // Note to developers: `pass_runtime_to_children` relies on the logic here to
        // get the underlying FFI plan during calls to `new_with_children`.
        if let Some(plan) = plan.as_any().downcast_ref::<ForeignExecutionPlan>() {
            return plan.plan.clone();
        }

        if let Some(rt) = &runtime
            && let Ok(Some(p)) = pass_runtime_to_children(&plan, rt)
        {
            plan = p;
        }

        let private_data = Box::new(ExecutionPlanPrivateData { plan, runtime });
        Self {
            properties: properties_fn_wrapper,
            children: children_fn_wrapper,
            with_new_children: with_new_children_fn_wrapper,
            name: name_fn_wrapper,
            execute: execute_fn_wrapper,
            repartitioned: repartitioned_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_ExecutionPlan {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an execution plan provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignExecutionPlan is to be used by the caller of the plan, so it has
/// no knowledge or access to the private data. All interaction with the plan
/// must occur through the functions defined in FFI_ExecutionPlan.
#[derive(Debug)]
pub struct ForeignExecutionPlan {
    name: String,
    plan: FFI_ExecutionPlan,
    properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

unsafe impl Send for ForeignExecutionPlan {}
unsafe impl Sync for ForeignExecutionPlan {}

impl DisplayAs for ForeignExecutionPlan {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "FFI_ExecutionPlan: {}, number_of_children={}",
                    self.name,
                    self.children.len(),
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

impl TryFrom<&FFI_ExecutionPlan> for Arc<dyn ExecutionPlan> {
    type Error = DataFusionError;

    fn try_from(plan: &FFI_ExecutionPlan) -> Result<Self, Self::Error> {
        if (plan.library_marker_id)() == crate::get_library_marker_id() {
            Ok(Arc::clone(plan.inner()))
        } else {
            let plan = ForeignExecutionPlan::try_from(plan.clone())?;
            Ok(Arc::new(plan))
        }
    }
}

impl TryFrom<FFI_ExecutionPlan> for ForeignExecutionPlan {
    type Error = DataFusionError;
    fn try_from(plan: FFI_ExecutionPlan) -> Result<Self, Self::Error> {
        unsafe {
            let name = (plan.name)(&plan).into();

            let properties: PlanProperties = (plan.properties)(&plan).try_into()?;

            let children_rvec = (plan.children)(&plan);
            let children = children_rvec
                .iter()
                .map(<Arc<dyn ExecutionPlan>>::try_from)
                .collect::<Result<Vec<_>>>()?;

            Ok(ForeignExecutionPlan {
                name,
                plan,
                properties,
                children,
            })
        }
    }
}

impl ExecutionPlan for ForeignExecutionPlan {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unsafe {
            let children = children
                .into_iter()
                .map(|child| FFI_ExecutionPlan::new(child, None))
                .collect::<RVec<_>>();
            let new_plan =
                df_result!((self.plan.with_new_children)(&self.plan, children))?;

            (&new_plan).try_into()
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let context = FFI_TaskContext::from(context);
        unsafe {
            df_result!((self.plan.execute)(&self.plan, partition, context))
                .map(|stream| Pin::new(Box::new(stream)) as SendableRecordBatchStream)
        }
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let config = config.into();
        let maybe_plan: Option<FFI_ExecutionPlan> = df_result!(unsafe {
            (self.plan.repartitioned)(&self.plan, target_partitions, config)
        })?
        .into();

        maybe_plan
            .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
            .transpose()
    }
}

#[cfg(any(test, feature = "integration-tests"))]
pub mod tests {
    use datafusion_physical_plan::Partitioning;
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};

    use super::*;

    #[derive(Debug)]
    pub struct EmptyExec {
        props: PlanProperties,
        children: Vec<Arc<dyn ExecutionPlan>>,
    }

    impl EmptyExec {
        pub fn new(schema: arrow::datatypes::SchemaRef) -> Self {
            Self {
                props: PlanProperties::new(
                    datafusion_physical_expr::EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(3),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                ),
                children: Vec::default(),
            }
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            unimplemented!()
        }
    }

    impl ExecutionPlan for EmptyExec {
        fn name(&self) -> &'static str {
            "empty-exec"
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn properties(&self) -> &PlanProperties {
            &self.props
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.children.iter().collect()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(EmptyExec {
                props: self.props.clone(),
                children,
            }))
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unimplemented!()
        }
    }

    #[test]
    fn test_round_trip_ffi_execution_plan() -> Result<()> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        let original_plan = Arc::new(EmptyExec::new(schema));
        let original_name = original_plan.name().to_string();

        let mut local_plan = FFI_ExecutionPlan::new(original_plan, None);
        local_plan.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_plan: Arc<dyn ExecutionPlan> = (&local_plan).try_into()?;

        assert_eq!(original_name, foreign_plan.name());

        let display = datafusion_physical_plan::display::DisplayableExecutionPlan::new(
            foreign_plan.as_ref(),
        );

        let buf = display.one_line().to_string();
        assert_eq!(
            buf.trim(),
            "FFI_ExecutionPlan: empty-exec, number_of_children=0"
        );

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_children() -> Result<()> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        // Version 1: Adding child to the foreign plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut child_local = FFI_ExecutionPlan::new(child_plan, None);
        child_local.library_marker_id = crate::mock_foreign_marker_id;
        let child_foreign = <Arc<dyn ExecutionPlan>>::try_from(&child_local)?;

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut parent_local = FFI_ExecutionPlan::new(parent_plan, None);
        parent_local.library_marker_id = crate::mock_foreign_marker_id;
        let parent_foreign = <Arc<dyn ExecutionPlan>>::try_from(&parent_local)?;

        assert_eq!(parent_foreign.children().len(), 0);
        assert_eq!(child_foreign.children().len(), 0);

        let parent_foreign = parent_foreign.with_new_children(vec![child_foreign])?;
        assert_eq!(parent_foreign.children().len(), 1);

        // Version 2: Adding child to the local plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let mut child_local = FFI_ExecutionPlan::new(child_plan, None);
        child_local.library_marker_id = crate::mock_foreign_marker_id;
        let child_foreign = <Arc<dyn ExecutionPlan>>::try_from(&child_local)?;

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let parent_plan = parent_plan.with_new_children(vec![child_foreign])?;
        let mut parent_local = FFI_ExecutionPlan::new(parent_plan, None);
        parent_local.library_marker_id = crate::mock_foreign_marker_id;
        let parent_foreign = <Arc<dyn ExecutionPlan>>::try_from(&parent_local)?;

        assert_eq!(parent_foreign.children().len(), 1);

        Ok(())
    }

    #[test]
    fn test_ffi_execution_plan_local_bypass() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Float32, false),
        ]));

        let plan = Arc::new(EmptyExec::new(schema));

        let mut ffi_plan = FFI_ExecutionPlan::new(plan, None);

        // Verify local libraries can be downcast to their original
        let foreign_plan: Arc<dyn ExecutionPlan> = (&ffi_plan).try_into().unwrap();
        assert!(foreign_plan.as_any().downcast_ref::<EmptyExec>().is_some());

        // Verify different library markers generate foreign providers
        ffi_plan.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_plan: Arc<dyn ExecutionPlan> = (&ffi_plan).try_into().unwrap();
        assert!(
            foreign_plan
                .as_any()
                .downcast_ref::<ForeignExecutionPlan>()
                .is_some()
        );
    }
}
