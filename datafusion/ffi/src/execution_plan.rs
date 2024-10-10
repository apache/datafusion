use std::{
    ffi::{c_char, c_void, CString},
    pin::Pin,
    ptr::null_mut,
    slice,
    sync::Arc,
};

use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream};
use datafusion::{
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, ExecutionMode, ExecutionPlan, PlanProperties},
};
use datafusion::{error::Result, physical_expr::EquivalenceProperties, prelude::SessionContext};
use datafusion_proto::{
    physical_plan::{
        from_proto::{parse_physical_sort_exprs, parse_protobuf_partitioning},
        to_proto::{serialize_partitioning, serialize_physical_sort_exprs},
        DefaultPhysicalExtensionCodec,
    },
    protobuf::{Partitioning, PhysicalSortExprNodeCollection},
};
use prost::Message;

use super::record_batch_stream::{record_batch_to_arrow_stream, ConsumerRecordBatchStream};

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_ExecutionPlan {
    pub properties:
        Option<unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> FFI_PlanProperties>,
    pub children: Option<
        unsafe extern "C" fn(
            plan: *const FFI_ExecutionPlan,
            num_children: &mut usize,
            err_code: &mut i32,
        ) -> *mut *const FFI_ExecutionPlan,
    >,
    pub name: unsafe extern "C" fn(plan: *const FFI_ExecutionPlan) -> *const c_char,

    pub execute: unsafe extern "C" fn(
        plan: *const FFI_ExecutionPlan,
        partition: usize,
        err_code: &mut i32,
    ) -> FFI_ArrowArrayStream,

    pub private_data: *mut c_void,
}

pub struct ExecutionPlanPrivateData {
    pub plan: Arc<dyn ExecutionPlan + Send>,
    pub last_error: Option<CString>,
    pub children: Vec<*const FFI_ExecutionPlan>,
    pub context: Arc<TaskContext>,
}

unsafe extern "C" fn properties_fn_wrapper(plan: *const FFI_ExecutionPlan) -> FFI_PlanProperties {
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
        .execute(partition, (*private_data).context.clone())
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
    pub fn new(plan: Arc<dyn ExecutionPlan + Send>, context: Arc<TaskContext>) -> Self {
        let children = plan
            .children()
            .into_iter()
            .map(|child| Box::new(FFI_ExecutionPlan::new(child.clone(), context.clone())))
            .map(|child| Box::into_raw(child) as *const FFI_ExecutionPlan)
            .collect();
        println!("children collected");

        let private_data = Box::new(ExecutionPlanPrivateData {
            plan,
            children,
            context,
            last_error: None,
        });
        println!("generated private data, ready to return");

        Self {
            properties: Some(properties_fn_wrapper),
            children: Some(children_fn_wrapper),
            name: name_fn_wrapper,
            execute: execute_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    // pub fn empty() -> Self {
    //     Self {
    //         properties: None,
    //         children: None,
    //         private_data: std::ptr::null_mut(),
    //     }
    // }
}

impl ExportedExecutionPlan {
    /// Wrap a FFI Execution Plan
    ///
    /// # Safety
    ///
    /// The caller must ensure the pointer provided points to a valid implementation
    /// of FFI_ExecutionPlan
    pub unsafe fn new(plan: *const FFI_ExecutionPlan) -> Result<Self> {
        let name_fn = (*plan).name;
        let name_cstr = name_fn(plan);
        let name = CString::from_raw(name_cstr as *mut c_char)
            .to_str()
            .unwrap_or("Unable to parse FFI_ExecutionPlan name")
            .to_string();

        println!("entered ExportedExecutionPlan::new");
        let properties = unsafe {
            let properties_fn = (*plan).properties.ok_or(DataFusionError::NotImplemented(
                "properties not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            println!("About to call properties fn");
            properties_fn(plan).try_into()?
        };

        println!("created properties");
        let children = unsafe {
            let children_fn = (*plan).children.ok_or(DataFusionError::NotImplemented(
                "children not implemented on FFI_ExecutionPlan".to_string(),
            ))?;
            let mut num_children = 0;
            let mut err_code = 0;
            let children_ptr = children_fn(plan, &mut num_children, &mut err_code);

            println!(
                "We called the FFI function children so the provider told us we have {} children",
                num_children
            );

            if err_code != 0 {
                return Err(DataFusionError::Plan(
                    "Error getting children for FFI_ExecutionPlan".to_string(),
                ));
            }

            let ffi_vec = Vec::from_raw_parts(children_ptr, num_children, num_children);
            let maybe_children: Result<Vec<_>> = ffi_vec
                .into_iter()
                .map(|child| {
                    println!("Ok, we are about to examine a child ffi_executionplan");
                    if let Some(props_fn) = (*child).properties {
                        println!("We do have properties on the child ");
                        let child_props = props_fn(child);
                        println!("Child schema {:?}", child_props.schema);
                    }

                    let child_plan = ExportedExecutionPlan::new(child);

                    child_plan.map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
                })
                .collect();
            println!("finsihed maybe children");

            maybe_children?
        };

        println!("About to return ExportedExecurtionPlan");

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
            let mut err_code = 0;
            let arrow_stream = execute_fn(self.plan, partition, &mut err_code);

            match err_code {
                0 => ConsumerRecordBatchStream::try_from(arrow_stream)
                    .map(|v| Pin::new(Box::new(v)) as SendableRecordBatchStream),
                _ => Err(DataFusionError::Execution(
                    "Error occurred during FFI call to FFI_ExecutionPlan execute.".to_string(),
                )),
            }
        }
    }
}

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_PlanProperties {
    // Returns protobuf serialized bytes of the partitioning
    pub output_partitioning: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub execution_mode:
        Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ExecutionMode>,

    // PhysicalSortExprNodeCollection proto
    pub output_ordering: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub schema: Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ArrowSchema>,

    pub private_data: *mut c_void,
}

unsafe extern "C" fn output_partitioning_fn_wrapper(
    properties: *const FFI_PlanProperties,
    buffer_size: &mut usize,
    buffer_bytes: &mut *mut u8,
) -> i32 {
    // let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    // let properties = (*private_data).plan.properties();
    // properties.clone().into()
    let private_data = (*properties).private_data as *const PlanProperties;
    let partitioning = (*private_data).output_partitioning();

    let codec = DefaultPhysicalExtensionCodec {};
    let partitioning_data = match serialize_partitioning(partitioning, &codec) {
        Ok(p) => p,
        Err(_) => return 1,
    };

    let mut partition_bytes = partitioning_data.encode_to_vec();
    *buffer_size = partition_bytes.len();
    *buffer_bytes = partition_bytes.as_mut_ptr();

    std::mem::forget(partition_bytes);

    0
}

unsafe extern "C" fn execution_mode_fn_wrapper(
    properties: *const FFI_PlanProperties,
) -> FFI_ExecutionMode {
    // let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    // let properties = (*private_data).plan.properties();
    // properties.clone().into()
    let private_data = (*properties).private_data as *const PlanProperties;
    let execution_mode = (*private_data).execution_mode();

    execution_mode.into()
}

unsafe extern "C" fn output_ordering_fn_wrapper(
    properties: *const FFI_PlanProperties,
    buffer_size: &mut usize,
    buffer_bytes: &mut *mut u8,
) -> i32 {
    // let private_data = (*plan).private_data as *const ExecutionPlanPrivateData;
    // let properties = (*private_data).plan.properties();
    // properties.clone().into()
    let private_data = (*properties).private_data as *const PlanProperties;
    let output_ordering = match (*private_data).output_ordering() {
        Some(o) => o,
        None => {
            *buffer_size = 0;
            return 0;
        }
    }
    .to_owned();

    let codec = DefaultPhysicalExtensionCodec {};
    let physical_sort_expr_nodes = match serialize_physical_sort_exprs(output_ordering, &codec) {
        Ok(p) => p,
        Err(_) => return 1,
    };

    let ordering_data = PhysicalSortExprNodeCollection {
        physical_sort_expr_nodes,
    };

    let mut ordering_bytes = ordering_data.encode_to_vec();
    *buffer_size = ordering_bytes.len();
    *buffer_bytes = ordering_bytes.as_mut_ptr();
    std::mem::forget(ordering_bytes);

    0
}

// pub schema: Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ArrowSchema>,
unsafe extern "C" fn schema_fn_wrapper(properties: *const FFI_PlanProperties) -> FFI_ArrowSchema {
    let private_data = (*properties).private_data as *const PlanProperties;
    let schema = (*private_data).eq_properties.schema();

    // This does silently fail because TableProvider does not return a result
    // so we expect it to always pass. Maybe some logging should be added.
    FFI_ArrowSchema::try_from(schema.as_ref()).unwrap_or(FFI_ArrowSchema::empty())
}

impl From<PlanProperties> for FFI_PlanProperties {
    fn from(value: PlanProperties) -> Self {
        let private_data = Box::new(value);

        Self {
            output_partitioning: Some(output_partitioning_fn_wrapper),
            execution_mode: Some(execution_mode_fn_wrapper),
            output_ordering: Some(output_ordering_fn_wrapper),
            schema: Some(schema_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

// /// Creates a new [`FFI_TableProvider`].
// pub fn new(provider: Box<dyn TableProvider + Send>) -> Self {
//     let private_data = Box::new(ProviderPrivateData {
//         provider,
//         last_error: None,
//     });

//     Self {
//         version: 2,
//         schema: Some(provider_schema),
//         scan: Some(provider_scan),
//         private_data: Box::into_raw(private_data) as *mut c_void,
//     }
// }

impl TryFrom<FFI_PlanProperties> for PlanProperties {
    type Error = DataFusionError;

    fn try_from(value: FFI_PlanProperties) -> std::result::Result<Self, Self::Error> {
        unsafe {
            let schema_fn = value.schema.ok_or(DataFusionError::NotImplemented(
                "schema() not implemented on FFI_PlanProperties".to_string(),
            ))?;
            let ffi_schema = schema_fn(&value);
            let schema: Schema = (&ffi_schema).try_into()?;

            let ordering_fn = value
                .output_ordering
                .ok_or(DataFusionError::NotImplemented(
                    "output_ordering() not implemented on FFI_PlanProperties".to_string(),
                ))?;
            let mut buff_size = 0;
            let mut buff = null_mut();
            if ordering_fn(&value, &mut buff_size, &mut buff) != 0 {
                return Err(DataFusionError::Plan(
                    "Error occurred during FFI call to output_ordering in FFI_PlanProperties"
                        .to_string(),
                ));
            }

            // TODO we will need to get these, but unsure if it happesn on the provider or consumer right now.
            let default_ctx = SessionContext::new();
            let codex = DefaultPhysicalExtensionCodec {};

            let orderings = match buff_size == 0 {
                true => None,
                false => {
                    let data = slice::from_raw_parts(buff, buff_size);

                    let proto_output_ordering = PhysicalSortExprNodeCollection::decode(data)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    Some(parse_physical_sort_exprs(
                        &proto_output_ordering.physical_sort_expr_nodes,
                        &default_ctx,
                        &schema,
                        &codex,
                    )?)
                }
            };

            let partitioning_fn =
                value
                    .output_partitioning
                    .ok_or(DataFusionError::NotImplemented(
                        "output_partitioning() not implemented on FFI_PlanProperties".to_string(),
                    ))?;
            if partitioning_fn(&value, &mut buff_size, &mut buff) != 0 {
                return Err(DataFusionError::Plan(
                    "Error occurred during FFI call to output_partitioning in FFI_PlanProperties"
                        .to_string(),
                ));
            }
            let data = slice::from_raw_parts(buff, buff_size);

            let proto_partitioning =
                Partitioning::decode(data).map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO: Validate this unwrap is safe.
            let partitioning = parse_protobuf_partitioning(
                Some(&proto_partitioning),
                &default_ctx,
                &schema,
                &codex,
            )?
            .unwrap();

            let execution_mode_fn = value.execution_mode.ok_or(DataFusionError::NotImplemented(
                "execution_mode() not implemented on FFI_PlanProperties".to_string(),
            ))?;
            let execution_mode = execution_mode_fn(&value).into();

            let eq_properties = match orderings {
                Some(ordering) => {
                    EquivalenceProperties::new_with_orderings(Arc::new(schema), &[ordering])
                }
                None => EquivalenceProperties::new(Arc::new(schema)),
            };

            Ok(Self::new(eq_properties, partitioning, execution_mode))
        }
    }
    // fn from(value: FFI_PlanProperties) -> Self {
    //     let schema = self.schema()

    //     let equiv_prop = EquivalenceProperties::new_with_orderings(schema, orderings);
    // }
}

#[repr(C)]
#[allow(non_camel_case_types)]
pub enum FFI_ExecutionMode {
    Bounded,

    Unbounded,

    PipelineBreaking,
}

impl From<ExecutionMode> for FFI_ExecutionMode {
    fn from(value: ExecutionMode) -> Self {
        match value {
            ExecutionMode::Bounded => FFI_ExecutionMode::Bounded,
            ExecutionMode::Unbounded => FFI_ExecutionMode::Unbounded,
            ExecutionMode::PipelineBreaking => FFI_ExecutionMode::PipelineBreaking,
        }
    }
}

impl From<FFI_ExecutionMode> for ExecutionMode {
    fn from(value: FFI_ExecutionMode) -> Self {
        match value {
            FFI_ExecutionMode::Bounded => ExecutionMode::Bounded,
            FFI_ExecutionMode::Unbounded => ExecutionMode::Unbounded,
            FFI_ExecutionMode::PipelineBreaking => ExecutionMode::PipelineBreaking,
        }
    }
}
