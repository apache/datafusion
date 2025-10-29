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

use std::{ffi::c_void, pin::Pin, sync::Arc};

use abi_stable::{
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, ExecutionPlan, PlanProperties},
};
use datafusion::{error::Result, physical_plan::DisplayFormatType};
use tokio::runtime::Handle;

use crate::{
    df_result, plan_properties::FFI_PlanProperties,
    record_batch_stream::FFI_RecordBatchStream, rresult,
};

/// A stable struct for sharing a [`ExecutionPlan`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    /// Return the plan properties
    pub properties: unsafe extern "C" fn(plan: &Self) -> FFI_PlanProperties,

    /// Return a vector of children plans
    pub children: unsafe extern "C" fn(plan: &Self) -> RVec<FFI_ExecutionPlan>,

    /// Return the plan name.
    pub name: unsafe extern "C" fn(plan: &Self) -> RString,

    /// Execute the plan and return a record batch stream. Errors
    /// will be returned as a string.
    pub execute: unsafe extern "C" fn(
        plan: &Self,
        partition: usize,
    ) -> RResult<FFI_RecordBatchStream, RString>,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignExecutionPlan`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ExecutionPlan {}
unsafe impl Sync for FFI_ExecutionPlan {}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan>,
    pub context: Arc<TaskContext>,
    pub runtime: Option<Handle>,
}

unsafe extern "C" fn properties_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> FFI_PlanProperties {
    let private_data = plan.private_data as *const ExecutionPlanPrivateData;
    let plan = &(*private_data).plan;

    plan.properties().into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: &FFI_ExecutionPlan,
) -> RVec<FFI_ExecutionPlan> {
    let private_data = plan.private_data as *const ExecutionPlanPrivateData;
    let plan = &(*private_data).plan;
    let ctx = &(*private_data).context;
    let runtime = &(*private_data).runtime;

    let children: Vec<_> = plan
        .children()
        .into_iter()
        .map(|child| {
            FFI_ExecutionPlan::new(Arc::clone(child), Arc::clone(ctx), runtime.clone())
        })
        .collect();

    children.into()
}

unsafe extern "C" fn execute_fn_wrapper(
    plan: &FFI_ExecutionPlan,
    partition: usize,
) -> RResult<FFI_RecordBatchStream, RString> {
    let private_data = plan.private_data as *const ExecutionPlanPrivateData;
    let plan = &(*private_data).plan;
    let ctx = &(*private_data).context;
    let runtime = (*private_data).runtime.clone();

    rresult!(plan
        .execute(partition, Arc::clone(ctx))
        .map(|rbs| FFI_RecordBatchStream::new(rbs, runtime)))
}

unsafe extern "C" fn name_fn_wrapper(plan: &FFI_ExecutionPlan) -> RString {
    let private_data = plan.private_data as *const ExecutionPlanPrivateData;
    let plan = &(*private_data).plan;

    plan.name().into()
}

unsafe extern "C" fn release_fn_wrapper(plan: &mut FFI_ExecutionPlan) {
    let private_data = Box::from_raw(plan.private_data as *mut ExecutionPlanPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(plan: &FFI_ExecutionPlan) -> FFI_ExecutionPlan {
    let private_data = plan.private_data as *const ExecutionPlanPrivateData;
    let plan_data = &(*private_data);

    FFI_ExecutionPlan::new(
        Arc::clone(&plan_data.plan),
        Arc::clone(&plan_data.context),
        plan_data.runtime.clone(),
    )
}

impl Clone for FFI_ExecutionPlan {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl FFI_ExecutionPlan {
    /// This function is called on the provider's side.
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        runtime: Option<Handle>,
    ) -> Self {
        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            context,
            runtime,
        });

        Self {
            properties: properties_fn_wrapper,
            children: children_fn_wrapper,
            name: name_fn_wrapper,
            execute: execute_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
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

impl TryFrom<&FFI_ExecutionPlan> for ForeignExecutionPlan {
    type Error = DataFusionError;

    fn try_from(plan: &FFI_ExecutionPlan) -> Result<Self, Self::Error> {
        unsafe {
            let name = (plan.name)(plan).into();

            let properties: PlanProperties = (plan.properties)(plan).try_into()?;

            let children_rvec = (plan.children)(plan);
            let children = children_rvec
                .iter()
                .map(ForeignExecutionPlan::try_from)
                .map(|child| child.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>))
                .collect::<Result<Vec<_>>>()?;

            Ok(Self {
                name,
                plan: plan.clone(),
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
        self.children
            .iter()
            .map(|p| p as &Arc<dyn ExecutionPlan>)
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ForeignExecutionPlan {
            plan: self.plan.clone(),
            name: self.name.clone(),
            children,
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unsafe {
            df_result!((self.plan.execute)(&self.plan, partition))
                .map(|stream| Pin::new(Box::new(stream)) as SendableRecordBatchStream)
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        physical_plan::{
            execution_plan::{Boundedness, EmissionType},
            Partitioning,
        },
        prelude::SessionContext,
    };

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
                    datafusion::physical_expr::EquivalenceProperties::new(schema),
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

        fn statistics(&self) -> Result<datafusion::common::Statistics> {
            unimplemented!()
        }
    }

    #[test]
    fn test_round_trip_ffi_execution_plan() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        let ctx = SessionContext::new();

        let original_plan = Arc::new(EmptyExec::new(schema));
        let original_name = original_plan.name().to_string();

        let local_plan = FFI_ExecutionPlan::new(original_plan, ctx.task_ctx(), None);

        let foreign_plan: ForeignExecutionPlan = (&local_plan).try_into()?;

        assert!(original_name == foreign_plan.name());

        let display = datafusion::physical_plan::display::DisplayableExecutionPlan::new(
            &foreign_plan,
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
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        let ctx = SessionContext::new();

        // Version 1: Adding child to the foreign plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let child_local = FFI_ExecutionPlan::new(child_plan, ctx.task_ctx(), None);
        let child_foreign = Arc::new(ForeignExecutionPlan::try_from(&child_local)?);

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let parent_local = FFI_ExecutionPlan::new(parent_plan, ctx.task_ctx(), None);
        let parent_foreign = Arc::new(ForeignExecutionPlan::try_from(&parent_local)?);

        assert_eq!(parent_foreign.children().len(), 0);
        assert_eq!(child_foreign.children().len(), 0);

        let parent_foreign = parent_foreign.with_new_children(vec![child_foreign])?;
        assert_eq!(parent_foreign.children().len(), 1);

        // Version 2: Adding child to the local plan
        let child_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let child_local = FFI_ExecutionPlan::new(child_plan, ctx.task_ctx(), None);
        let child_foreign = Arc::new(ForeignExecutionPlan::try_from(&child_local)?);

        let parent_plan = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let parent_plan = parent_plan.with_new_children(vec![child_foreign])?;
        let parent_local = FFI_ExecutionPlan::new(parent_plan, ctx.task_ctx(), None);
        let parent_foreign = Arc::new(ForeignExecutionPlan::try_from(&parent_local)?);

        assert_eq!(parent_foreign.children().len(), 1);

        Ok(())
    }
}
