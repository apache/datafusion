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

use core::slice;
use std::{
    ffi::{c_uint, c_void},
    ptr::null_mut,
    sync::Arc,
};

use arrow::{datatypes::Schema, ffi::FFI_ArrowSchema};
use datafusion::{
    error::{DataFusionError, Result},
    physical_expr::EquivalenceProperties,
    physical_plan::{ExecutionMode, PlanProperties},
    prelude::SessionContext,
};
use datafusion_proto::{
    physical_plan::{
        from_proto::{parse_physical_sort_exprs, parse_protobuf_partitioning},
        to_proto::{serialize_partitioning, serialize_physical_sort_exprs},
        DefaultPhysicalExtensionCodec,
    },
    protobuf::{Partitioning, PhysicalSortExprNodeCollection},
};
use prost::Message;

// TODO: should we just make ExecutionMode repr(C)?
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone)]
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
            buffer_size: &mut c_uint,
        ) -> *const u8,
    >,

    pub execution_mode: Option<
        unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ExecutionMode,
    >,

    // PhysicalSortExprNodeCollection proto
    pub output_ordering: Option<
        unsafe extern "C" fn(
            plan: *const FFI_PlanProperties,
            buffer_size: &mut usize,
        ) -> *const u8,
    >,

    pub schema:
        Option<unsafe extern "C" fn(plan: *const FFI_PlanProperties) -> FFI_ArrowSchema>,

    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,

    pub private_data: *mut c_void,
}

struct PlanPropertiesPrivateData {
    output_partitioning: Vec<u8>,
    execution_mode: FFI_ExecutionMode,
    output_ordering: Vec<u8>,
    schema: Arc<Schema>,
}

unsafe extern "C" fn output_partitioning_fn_wrapper(
    properties: *const FFI_PlanProperties,
    buffer_size: &mut c_uint,
) -> *const u8 {
    let private_data = (*properties).private_data as *const PlanPropertiesPrivateData;
    *buffer_size = (*private_data).output_partitioning.len() as c_uint;
    (*private_data).output_partitioning.as_ptr()
}

unsafe extern "C" fn execution_mode_fn_wrapper(
    properties: *const FFI_PlanProperties,
) -> FFI_ExecutionMode {
    let private_data = (*properties).private_data as *const PlanPropertiesPrivateData;
    (*private_data).execution_mode.clone()
}

unsafe extern "C" fn output_ordering_fn_wrapper(
    properties: *const FFI_PlanProperties,
    buffer_size: &mut usize,
) -> *const u8 {
    let private_data = (*properties).private_data as *const PlanPropertiesPrivateData;
    *buffer_size = (*private_data).output_ordering.len();
    (*private_data).output_ordering.as_ptr()
}

unsafe extern "C" fn schema_fn_wrapper(
    properties: *const FFI_PlanProperties,
) -> FFI_ArrowSchema {
    let private_data = (*properties).private_data as *const PlanPropertiesPrivateData;
    FFI_ArrowSchema::try_from((*private_data).schema.as_ref())
        .unwrap_or(FFI_ArrowSchema::empty())
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

    let private_data =
        Box::from_raw(props.private_data as *mut PlanPropertiesPrivateData);
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

impl FFI_PlanProperties {
    pub fn new(props: PlanProperties) -> Result<Self> {
        let partitioning = props.output_partitioning();

        let codec = DefaultPhysicalExtensionCodec {};
        let partitioning_data = serialize_partitioning(partitioning, &codec)?;
        let output_partitioning = partitioning_data.encode_to_vec();

        let output_ordering = match props.output_ordering() {
            Some(ordering) => {
                let physical_sort_expr_nodes =
                    serialize_physical_sort_exprs(ordering.to_owned(), &codec)?;

                let ordering_data = PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes,
                };

                ordering_data.encode_to_vec()
            }
            None => Vec::default(),
        };

        let private_data = Box::new(PlanPropertiesPrivateData {
            output_partitioning,
            output_ordering,
            execution_mode: props.execution_mode.into(),
            schema: Arc::clone(props.eq_properties.schema()),
        });

        let ffi_props = FFI_PlanProperties {
            output_partitioning: Some(output_partitioning_fn_wrapper),
            execution_mode: Some(execution_mode_fn_wrapper),
            output_ordering: Some(output_ordering_fn_wrapper),
            schema: Some(schema_fn_wrapper),
            release: Some(release_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        };

        Ok(ffi_props)
    }

    pub fn empty() -> Self {
        Self {
            output_partitioning: None,
            execution_mode: None,
            output_ordering: None,
            schema: None,
            release: None,
            private_data: null_mut(),
        }
    }
}

#[derive(Debug)]
pub struct ForeignPlanProperties(pub PlanProperties);

impl ForeignPlanProperties {
    /// Construct a ForeignPlanProperties object from a FFI Plan Properties.
    ///
    /// # Safety
    ///
    /// This function will call the unsafe interfaces on FFI_PlanProperties
    /// provided, so the user must ensure it remains valid for the lifetime
    /// of the returned struct.
    pub unsafe fn new(ffi_props: FFI_PlanProperties) -> Result<Self> {
        let schema_fn = ffi_props.schema.ok_or(DataFusionError::NotImplemented(
            "schema() not implemented on FFI_PlanProperties".to_string(),
        ))?;
        let ffi_schema = schema_fn(&ffi_props);
        let schema = (&ffi_schema).try_into()?;

        let ordering_fn =
            ffi_props
                .output_ordering
                .ok_or(DataFusionError::NotImplemented(
                    "output_ordering() not implemented on FFI_PlanProperties".to_string(),
                ))?;
        let mut buff_size = 0;
        let buff = ordering_fn(&ffi_props, &mut buff_size);
        if buff.is_null() {
            return Err(DataFusionError::Plan(
                "Error occurred during FFI call to output_ordering in FFI_PlanProperties"
                    .to_string(),
            ));
        }

        // TODO Extend FFI to get the registry and codex
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

        let mut buff_size = 0;

        let partitioning_fn =
            ffi_props
                .output_partitioning
                .ok_or(DataFusionError::NotImplemented(
                    "output_partitioning() not implemented on FFI_PlanProperties"
                        .to_string(),
                ))?;
        let buff = partitioning_fn(&ffi_props, &mut buff_size);
        if buff.is_null() && buff_size != 0 {
            return Err(DataFusionError::Plan(
                "Error occurred during FFI call to output_partitioning in FFI_PlanProperties"
                    .to_string(),
            ));
        }

        let partitioning = {
            println!("ForeignPlanProperties::new buff {:?}", buff);
            let data = slice::from_raw_parts(buff, buff_size as usize);

            let proto_partitioning = Partitioning::decode(data)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            // TODO: Validate this unwrap is safe.
            parse_protobuf_partitioning(
                Some(&proto_partitioning),
                &default_ctx,
                &schema,
                &codex,
            )?
            .unwrap()
        };

        let execution_mode_fn =
            ffi_props
                .execution_mode
                .ok_or(DataFusionError::NotImplemented(
                    "execution_mode() not implemented on FFI_PlanProperties".to_string(),
                ))?;
        let execution_mode: ExecutionMode = execution_mode_fn(&ffi_props).into();

        let eq_properties = match orderings {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(Arc::new(schema), &[ordering])
            }
            None => EquivalenceProperties::new(Arc::new(schema)),
        };

        Ok(Self(PlanProperties::new(
            eq_properties,
            partitioning,
            execution_mode,
        )))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::Partitioning;

    use super::*;

    #[test]
    fn test_round_trip_ffi_plan_properties() -> Result<()> {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        let original_props = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(3),
            ExecutionMode::Unbounded,
        );

        let local_props_ptr = FFI_PlanProperties::new(original_props.clone())?;

        let foreign_props = unsafe { ForeignPlanProperties::new(local_props_ptr)? };

        let returned_props: PlanProperties = foreign_props.0;

        assert!(format!("{:?}", returned_props) == format!("{:?}", original_props));

        Ok(())
    }
}
