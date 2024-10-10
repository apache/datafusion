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

use std::{ffi::c_void, ptr::null_mut, sync::Arc};

use arrow::ffi::FFI_ArrowSchema;
use datafusion::{error::DataFusionError, physical_expr::EquivalenceProperties, physical_plan::{ExecutionMode, PlanProperties}, prelude::SessionContext};
use datafusion_proto::{physical_plan::{from_proto::{parse_physical_sort_exprs, parse_protobuf_partitioning}, to_proto::{serialize_partitioning, serialize_physical_sort_exprs}, DefaultPhysicalExtensionCodec}, protobuf::{Partitioning, PhysicalSortExprNodeCollection}};
use prost::Message;


// TODO: should we just make ExecutionMode repr(C)?
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

    pub execution_mode: Option<
        unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ExecutionMode,
    >,

    // PhysicalSortExprNodeCollection proto
    pub output_ordering: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
            buffer_bytes: &mut *mut u8,
        ) -> i32,
    >,

    pub schema:
        Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ArrowSchema>,

    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
    
    pub private_data: *mut c_void,
}

unsafe extern "C" fn output_partitioning_fn_wrapper(
    properties: *const FFI_PlanProperties,
    buffer_size: &mut usize,
    buffer_bytes: &mut *mut u8,
) -> i32 {
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
    let physical_sort_expr_nodes =
        match serialize_physical_sort_exprs(output_ordering, &codec) {
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
unsafe extern "C" fn schema_fn_wrapper(
    properties: *const FFI_PlanProperties,
) -> FFI_ArrowSchema {
    let private_data = (*properties).private_data as *const PlanProperties;
    let schema = (*private_data).eq_properties.schema();

    // This does silently fail because TableProvider does not return a result
    // so we expect it to always pass. Maybe some logging should be added.
    FFI_ArrowSchema::try_from(schema.as_ref()).unwrap_or(FFI_ArrowSchema::empty())
}

unsafe extern "C" fn release_fn_wrapper(props: *mut FFI_PlanProperties) {
    if props.is_null() {
        return;
    }
    let props = &mut *props;

    props.execution_mode = None;
    props.output_partitioning = None;
    props.execution_mode = None;
    props.output_ordering = None;
    props.schema = None;

    let private_data = Box::from_raw(props.private_data as *mut PlanProperties);
    drop(private_data);

    props.release = None;
}


impl Drop for FFI_PlanProperties {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self) },
        };
    }
}

impl From<PlanProperties> for FFI_PlanProperties {
    fn from(value: PlanProperties) -> Self {
        let private_data = Box::new(value);

        Self {
            output_partitioning: Some(output_partitioning_fn_wrapper),
            execution_mode: Some(execution_mode_fn_wrapper),
            output_ordering: Some(output_ordering_fn_wrapper),
            schema: Some(schema_fn_wrapper),
            release: Some(release_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl TryFrom<FFI_PlanProperties> for PlanProperties {
    type Error = DataFusionError;

    fn try_from(value: FFI_PlanProperties) -> std::result::Result<Self, Self::Error> {
        unsafe {
            let schema_fn = value.schema.ok_or(DataFusionError::NotImplemented(
                "schema() not implemented on FFI_PlanProperties".to_string(),
            ))?;
            let ffi_schema = schema_fn(&value);
            let schema = (&ffi_schema).try_into()?;

            let ordering_fn =
                value
                    .output_ordering
                    .ok_or(DataFusionError::NotImplemented(
                        "output_ordering() not implemented on FFI_PlanProperties"
                            .to_string(),
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
                    let data = Vec::from_raw_parts(buff, buff_size, buff_size);

                    let proto_output_ordering =
                        PhysicalSortExprNodeCollection::decode(data.as_ref())
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
                        "output_partitioning() not implemented on FFI_PlanProperties"
                            .to_string(),
                    ))?;
            if partitioning_fn(&value, &mut buff_size, &mut buff) != 0 {
                return Err(DataFusionError::Plan(
                    "Error occurred during FFI call to output_partitioning in FFI_PlanProperties"
                        .to_string(),
                ));
            }
            let data = Vec::from_raw_parts(buff, buff_size, buff_size);

            let proto_partitioning = Partitioning::decode(data.as_ref())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO: Validate this unwrap is safe.
            let partitioning = parse_protobuf_partitioning(
                Some(&proto_partitioning),
                &default_ctx,
                &schema,
                &codex,
            )?
            .unwrap();

            let execution_mode_fn =
                value.execution_mode.ok_or(DataFusionError::NotImplemented(
                    "execution_mode() not implemented on FFI_PlanProperties".to_string(),
                ))?;
            let execution_mode = execution_mode_fn(&value).into();

            let eq_properties = match orderings {
                Some(ordering) => EquivalenceProperties::new_with_orderings(
                    Arc::new(schema),
                    &[ordering],
                ),
                None => EquivalenceProperties::new(Arc::new(schema)),
            };

            Ok(Self::new(eq_properties, partitioning, execution_mode))
        }
    }
}
