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

use std::{
    ffi::{c_char, c_void, CStr, CString},
    pin::Pin,
    ptr::null_mut,
    slice,
    sync::Arc,
};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::error::Result;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, ExecutionPlan, PlanProperties},
};

use crate::plan_properties::{FFI_PlanProperties, ForeignPlanProperties};

use super::record_batch_stream::{
    record_batch_to_arrow_stream, ConsumerRecordBatchStream,
};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub properties: Option<
        unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> FFI_PlanProperties,
    >,

    pub children: Option<
        unsafe extern "C" fn(
            plan: *const FFI_ExecutionPlan,
            num_children: &mut usize,
            err_code: &mut i32,
        ) -> *const FFI_ExecutionPlan,
    >,

    pub name:
        Option<unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> *const c_char>,

    pub execute: Option<
        unsafe extern "C" fn(
            plan: *const FFI_ExecutionPlan,
            partition: usize,
            err_code: &mut i32,
        ) -> FFI_ArrowArrayStream,
    >,

    pub clone:
        Option<unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> FFI_ExecutionPlan>,

    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
    pub private_data: *mut c_void,
}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan>,
    pub last_error: Option<CString>,
    pub children: Vec<FFI_ExecutionPlan>,
    pub context: Arc<TaskContext>,
    pub name: CString,
}

unsafe extern "C" fn properties_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
) -> FFI_PlanProperties {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    let properties = (*private_data).plan.properties();

    FFI_PlanProperties::new(properties.clone()).unwrap_or(FFI_PlanProperties::empty())
}

unsafe extern "C" fn children_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
    num_children: &mut usize,
    err_code: &mut i32,
) -> *const FFI_ExecutionPlan {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;

    *num_children = (*private_data).children.len();
    *err_code = 0;

    (*private_data).children.as_ptr()
}

unsafe extern "C" fn execute_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
    partition: usize,
    err_code: &mut i32,
) -> FFI_ArrowArrayStream {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;

    let record_batch_stream = match (*private_data)
        .plan
        .execute(partition, Arc::clone(&(*private_data).context))
    {
        Ok(rbs) => rbs,
        Err(_e) => {
            *err_code = 1;
            return FFI_ArrowArrayStream::empty();
        }
    };

    record_batch_to_arrow_stream(record_batch_stream)
}
unsafe extern "C" fn name_fn_wrapper(plan: *const FFI_ExecutionPlan) -> *const c_char {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    (*private_data).name.as_ptr()
}

unsafe extern "C" fn release_fn_wrapper(plan: *mut FFI_ExecutionPlan) {
    if plan.is_null() {
        return;
    }
    let plan = &mut *plan;

    plan.properties = None;
    plan.children = None;
    plan.name = None;
    plan.execute = None;

    let private_data = Box::from_raw(plan.private_data as *mut ExecutionPlanPrivateData);
    drop(private_data);

    plan.release = None;
}

unsafe extern "C" fn clone_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
) -> FFI_ExecutionPlan {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    let plan_data = &(*private_data);

    FFI_ExecutionPlan::new(Arc::clone(&plan_data.plan), Arc::clone(&plan_data.context))
        .unwrap()
}

// Since the trait ExecutionPlan requires borrowed values, we wrap our FFI.
// This struct exists on the consumer side (datafusion-python, for example) and not
// in the provider's side.
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
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "FFI_ExecutionPlan(number_of_children={})",
            self.children.len(),
        )
    }
}

impl FFI_ExecutionPlan {
    /// This function is called on the provider's side.
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Result<Self> {
        let maybe_children: Result<Vec<_>> = plan
            .children()
            .into_iter()
            .map(|child| FFI_ExecutionPlan::new(Arc::clone(child), Arc::clone(&context)))
            .collect();
        let children = maybe_children?;

        let name = CString::new(plan.name()).map_err(|e| {
            DataFusionError::Plan(format!(
                "Unable to convert name to CString in FFI_ExecutionPlan: {}",
                e
            ))
        })?;

        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            children,
            context,
            last_error: None,
            name,
        });

        Ok(Self {
            properties: Some(properties_fn_wrapper),
            children: Some(children_fn_wrapper),
            name: Some(name_fn_wrapper),
            execute: Some(execute_fn_wrapper),
            release: Some(release_fn_wrapper),
            clone: Some(clone_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        })
    }

    pub fn empty() -> Self {
        Self {
            properties: None,
            children: None,
            name: None,
            execute: None,
            release: None,
            clone: None,
            private_data: null_mut(),
        }
    }
}

impl Drop for FFI_ExecutionPlan {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

impl ForeignExecutionPlan {
    /// Takes ownership of a FFI_ExecutionPlan
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer provided points to a valid implementation
    /// of FFI_ExecutionPlan
    pub unsafe fn new(plan: FFI_ExecutionPlan) -> Result<Self> {
        let name_fn = plan.name.ok_or(DataFusionError::NotImplemented(
            "name() not implemented on FFI_ExecutionPlan".to_string(),
        ))?;
        let name_ptr = name_fn(&plan);

        let name_cstr = CStr::from_ptr(name_ptr);

        let name = name_cstr
            .to_str()
            .map_err(|e| {
                DataFusionError::Plan(format!(
                    "Unable to convert CStr name in FFI_ExecutionPlan: {}",
                    e
                ))
            })?
            .to_string();

        let properties = unsafe {
            let properties_fn =
                plan.properties.ok_or(DataFusionError::NotImplemented(
                    "properties not implemented on FFI_ExecutionPlan".to_string(),
                ))?;

            ForeignPlanProperties::new(properties_fn(&plan))?
        };

        let children = unsafe {
            let children_fn = plan.children.ok_or(DataFusionError::NotImplemented(
                "children not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            let mut num_children = 0;
            let mut err_code = 0;
            let children_ptr = children_fn(&plan, &mut num_children, &mut err_code);

            if err_code != 0 {
                return Err(DataFusionError::Plan(
                    "Error getting children for FFI_ExecutionPlan".to_string(),
                ));
            }

            let ffi_vec = slice::from_raw_parts(children_ptr, num_children);
            let maybe_children: Result<Vec<_>> = ffi_vec
                .iter()
                .map(|child| {
                    let child_plan = ForeignExecutionPlan::new(child.clone());

                    child_plan.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
                })
                .collect();

            maybe_children?
        };

        Ok(Self {
            name,
            plan,
            properties: properties.0,
            children,
        })
    }
}

impl Clone for FFI_ExecutionPlan {
    fn clone(&self) -> Self {
        unsafe {
            let clone_fn = self
                .clone
                .expect("FFI_ExecutionPlan does not have clone defined.");
            clone_fn(self)
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

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
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
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
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
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        unsafe {
            let execute_fn = self.plan.execute.ok_or(DataFusionError::Execution(
                "execute is not defined on FFI_ExecutionPlan".to_string(),
            ))?;

            let mut err_code = 0;
            let arrow_stream = execute_fn(&self.plan, partition, &mut err_code);

            match err_code {
                0 => ConsumerRecordBatchStream::try_from(arrow_stream)
                    .map(|v| Pin::new(Box::new(v)) as SendableRecordBatchStream),
                _ => Err(DataFusionError::Execution(
                    "Error occurred during FFI call to FFI_ExecutionPlan execute."
                        .to_string(),
                )),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{physical_plan::Partitioning, prelude::SessionContext};

    use super::*;

    #[derive(Debug)]
    pub struct EmptyExec {
        props: PlanProperties,
    }

    impl EmptyExec {
        pub fn new(schema: arrow::datatypes::SchemaRef) -> Self {
            Self {
                props: PlanProperties::new(
                    datafusion::physical_expr::EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(3),
                    datafusion::physical_plan::ExecutionMode::Unbounded,
                ),
            }
        }
    }

    impl DisplayAs for EmptyExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
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
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unimplemented!()
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
        use arrow::datatypes::{DataType, Field, Schema};
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        let ctx = SessionContext::new();

        let original_plan = Arc::new(EmptyExec::new(schema));
        let original_name = original_plan.name().to_string();

        let local_plan = FFI_ExecutionPlan::new(original_plan, ctx.task_ctx())?;

        let foreign_plan = unsafe { ForeignExecutionPlan::new(local_plan)? };

        assert!(original_name == foreign_plan.name());

        Ok(())
    }
}
