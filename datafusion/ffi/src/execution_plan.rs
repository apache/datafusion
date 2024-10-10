use std::{
    ffi::{c_char, c_void, CString},
    pin::Pin,
    sync::Arc,
};

use arrow::ffi_stream::FFI_ArrowArrayStream;
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, ExecutionPlan, PlanProperties},
};
use datafusion::error::Result;

use crate::plan_properties::FFI_PlanProperties;

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
        ) -> *mut *const FFI_ExecutionPlan,
    >,
    pub name: Option<unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> *const c_char>,

    pub execute: Option<unsafe extern "C" fn(
        plan: *const FFI_ExecutionPlan,
        partition: usize,
        err_code: &mut i32,
    ) -> FFI_ArrowArrayStream>,

    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
    pub private_data: *mut c_void,
}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
    pub children: Vec<*const FFI_ExecutionPlan>,
    pub context: Arc<TaskContext>,
}

unsafe extern "C" fn properties_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
) -> FFI_PlanProperties {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    let properties = (*private_data).plan.properties();
    properties.clone().into()
}

unsafe extern "C" fn children_fn_wrapper(
    plan: *const FFI_ExecutionPlan,
    num_children: &mut usize,
    err_code: &mut i32,
) -> *mut *const FFI_ExecutionPlan {
    let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;

    *num_children = (*private_data).children.len();
    *err_code = 0;

    let mut children: Vec<_> = (*private_data).children.to_owned();
    let children_ptr = children.as_mut_ptr();

    std::mem::forget(children);

    children_ptr
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

    let name = (*private_data).plan.name();

    CString::new(name)
        .unwrap_or(CString::new("unable to parse execution plan name").unwrap())
        .into_raw()
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


// Since the trait ExecutionPlan requires borrowed values, we wrap our FFI.
// This struct exists on the consumer side (datafusion-python, for example) and not
// in the provider's side.
#[derive(Debug)]
pub struct ExportedExecutionPlan {
    name: String,
    plan: *const FFI_ExecutionPlan,
    properties: PlanProperties,
    children: Vec<Arc<dyn ExecutionPlan>>,
}

unsafe impl Send for ExportedExecutionPlan {}
unsafe impl Sync for ExportedExecutionPlan {}

impl DisplayAs for ExportedExecutionPlan {
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
    pub fn new(plan: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Self {
        let children = plan
            .children()
            .into_iter()
            .map(|child| Box::new(FFI_ExecutionPlan::new(Arc::clone(child), Arc::clone(&context))))
            .map(|child| Box::into_raw(child) as *const FFI_ExecutionPlan)
            .collect();

        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            children,
            context,
            last_error: None,
        });

        Self {
            properties: Some(properties_fn_wrapper),
            children: Some(children_fn_wrapper),
            name: Some(name_fn_wrapper),
            execute: Some(execute_fn_wrapper),
            release: Some(release_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
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

impl ExportedExecutionPlan {
    /// Wrap a FFI Execution Plan
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer provided points to a valid implementation
    /// of FFI_ExecutionPlan
    pub unsafe fn new(plan: *const FFI_ExecutionPlan) -> Result<Self> {
        let name_ptr = (*plan).name.map(|func| func(plan));
        let name = match name_ptr {
            Some(name_cstr) => {
                CString::from_raw(name_cstr as *mut c_char)
                .to_str()
                .unwrap_or("Unable to parse FFI_ExecutionPlan name")
                .to_string()
            }
            None => "Plan has no name".to_string()
        };

        let properties = unsafe {
            let properties_fn =
                (*plan).properties.ok_or(DataFusionError::NotImplemented(
                    "properties not implemented on FFI_ExecutionPlan".to_string(),
                ))?;
            properties_fn(plan).try_into()?
        };

        let children = unsafe {
            let children_fn = (*plan).children.ok_or(DataFusionError::NotImplemented(
                "children not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            let mut num_children = 0;
            let mut err_code = 0;
            let children_ptr = children_fn(plan, &mut num_children, &mut err_code);

            if err_code != 0 {
                return Err(DataFusionError::Plan(
                    "Error getting children for FFI_ExecutionPlan".to_string(),
                ));
            }

            let ffi_vec = Vec::from_raw_parts(children_ptr, num_children, num_children);
            let maybe_children: Result<Vec<_>> = ffi_vec
                .into_iter()
                .map(|child| {

                    let child_plan = ExportedExecutionPlan::new(child);

                    child_plan.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
                })
                .collect();

            maybe_children?
        };

        Ok(Self {
            name,
            plan,
            properties,
            children,
        })
    }
}

impl ExecutionPlan for ExportedExecutionPlan {
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
        Ok(Arc::new(ExportedExecutionPlan {
            plan: self.plan,
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
            let execute_fn = (*self.plan).execute;
            if execute_fn.is_none() {
                return Err(DataFusionError::Execution("execute is not defined on FFI_ExecutionPlan".to_string()));
            }
            let execute_fn = execute_fn.unwrap();

            let mut err_code = 0;
            let arrow_stream = execute_fn(self.plan, partition, &mut err_code);

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
