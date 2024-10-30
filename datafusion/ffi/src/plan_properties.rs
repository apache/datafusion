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

use std::{ffi::c_void, sync::Arc};

use abi_stable::{
    std_types::{
        RResult::{self, RErr, ROk},
        RStr, RVec,
    },
    StableAbi,
};
use arrow::datatypes::SchemaRef;
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

use crate::arrow_wrappers::WrappedSchema;

/// A stable struct for sharing [`PlanProperties`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PlanProperties {
    /// The output partitioning is a [`Partitioning`] protobuf message serialized
    /// into bytes to pass across the FFI boundary.
    pub output_partitioning:
        unsafe extern "C" fn(plan: &Self) -> RResult<RVec<u8>, RStr<'static>>,

    /// Return the execution mode of the plan.
    pub execution_mode: unsafe extern "C" fn(plan: &Self) -> FFI_ExecutionMode,

    /// The output ordering is a [`PhysicalSortExprNodeCollection`] protobuf message
    /// serialized into bytes to pass across the FFI boundary.
    pub output_ordering:
        unsafe extern "C" fn(plan: &Self) -> RResult<RVec<u8>, RStr<'static>>,

    /// Return the schema of the plan.
    pub schema: unsafe extern "C" fn(plan: &Self) -> WrappedSchema,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,
}

struct PlanPropertiesPrivateData {
    props: PlanProperties,
}

unsafe extern "C" fn output_partitioning_fn_wrapper(
    properties: &FFI_PlanProperties,
) -> RResult<RVec<u8>, RStr<'static>> {
    let private_data = properties.private_data as *const PlanPropertiesPrivateData;
    let props = &(*private_data).props;

    let codec = DefaultPhysicalExtensionCodec {};
    let partitioning_data =
        match serialize_partitioning(props.output_partitioning(), &codec) {
            Ok(p) => p,
            Err(_) => {
                return RErr(
                    "unable to serialize output_partitioning in FFI_PlanProperties"
                        .into(),
                )
            }
        };
    let output_partitioning = partitioning_data.encode_to_vec();

    ROk(output_partitioning.into())
}

unsafe extern "C" fn execution_mode_fn_wrapper(
    properties: &FFI_PlanProperties,
) -> FFI_ExecutionMode {
    let private_data = properties.private_data as *const PlanPropertiesPrivateData;
    let props = &(*private_data).props;
    props.execution_mode().into()
}

unsafe extern "C" fn output_ordering_fn_wrapper(
    properties: &FFI_PlanProperties,
) -> RResult<RVec<u8>, RStr<'static>> {
    let private_data = properties.private_data as *const PlanPropertiesPrivateData;
    let props = &(*private_data).props;

    let codec = DefaultPhysicalExtensionCodec {};
    let output_ordering =
        match props.output_ordering() {
            Some(ordering) => {
                let physical_sort_expr_nodes =
                    match serialize_physical_sort_exprs(ordering.to_owned(), &codec) {
                        Ok(v) => v,
                        Err(_) => return RErr(
                            "unable to serialize output_ordering in FFI_PlanProperties"
                                .into(),
                        ),
                    };

                let ordering_data = PhysicalSortExprNodeCollection {
                    physical_sort_expr_nodes,
                };

                ordering_data.encode_to_vec()
            }
            None => Vec::default(),
        };
    ROk(output_ordering.into())
}

unsafe extern "C" fn schema_fn_wrapper(properties: &FFI_PlanProperties) -> WrappedSchema {
    let private_data = properties.private_data as *const PlanPropertiesPrivateData;
    let props = &(*private_data).props;

    let schema: SchemaRef = Arc::clone(props.eq_properties.schema());
    schema.into()
}

unsafe extern "C" fn release_fn_wrapper(props: &mut FFI_PlanProperties) {
    let private_data =
        Box::from_raw(props.private_data as *mut PlanPropertiesPrivateData);
    drop(private_data);
}

impl Drop for FFI_PlanProperties {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl From<&PlanProperties> for FFI_PlanProperties {
    fn from(props: &PlanProperties) -> Self {
        let private_data = Box::new(PlanPropertiesPrivateData {
            props: props.clone(),
        });

        FFI_PlanProperties {
            output_partitioning: output_partitioning_fn_wrapper,
            execution_mode: execution_mode_fn_wrapper,
            output_ordering: output_ordering_fn_wrapper,
            schema: schema_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl TryFrom<FFI_PlanProperties> for PlanProperties {
    type Error = DataFusionError;

    fn try_from(ffi_props: FFI_PlanProperties) -> Result<Self, Self::Error> {
        let ffi_schema = unsafe { (ffi_props.schema)(&ffi_props) };
        let schema = (&ffi_schema.0).try_into()?;

        // TODO Extend FFI to get the registry and codex
        let default_ctx = SessionContext::new();
        let codex = DefaultPhysicalExtensionCodec {};

        let ffi_orderings = unsafe { (ffi_props.output_ordering)(&ffi_props) };
        let orderings = match ffi_orderings {
            ROk(ordering_vec) => {
                let proto_output_ordering =
                    PhysicalSortExprNodeCollection::decode(ordering_vec.as_ref())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Some(parse_physical_sort_exprs(
                    &proto_output_ordering.physical_sort_expr_nodes,
                    &default_ctx,
                    &schema,
                    &codex,
                )?)
            }
            RErr(e) => return Err(DataFusionError::Plan(e.to_string())),
        };

        let ffi_partitioning = unsafe { (ffi_props.output_partitioning)(&ffi_props) };
        let partitioning = match ffi_partitioning {
            ROk(partitioning_vec) => {
                let proto_output_partitioning =
                    Partitioning::decode(partitioning_vec.as_ref())
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                parse_protobuf_partitioning(
                    Some(&proto_output_partitioning),
                    &default_ctx,
                    &schema,
                    &codex,
                )?
                .ok_or(DataFusionError::Plan(
                    "Unable to deserialize partitioning protobuf in FFI_PlanProperties"
                        .to_string(),
                ))
            }
            RErr(e) => Err(DataFusionError::Plan(e.to_string())),
        }?;

        let execution_mode: ExecutionMode =
            unsafe { (ffi_props.execution_mode)(&ffi_props).into() };

        let eq_properties = match orderings {
            Some(ordering) => {
                EquivalenceProperties::new_with_orderings(Arc::new(schema), &[ordering])
            }
            None => EquivalenceProperties::new(Arc::new(schema)),
        };

        Ok(PlanProperties::new(
            eq_properties,
            partitioning,
            execution_mode,
        ))
    }
}

/// FFI safe version of [`ExecutionMode`].
#[repr(C)]
#[allow(non_camel_case_types)]
#[derive(Clone, StableAbi)]
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

        let local_props_ptr = FFI_PlanProperties::from(&original_props);

        let foreign_props: PlanProperties = local_props_ptr.try_into()?;

        assert!(format!("{:?}", foreign_props) == format!("{:?}", original_props));

        Ok(())
    }
}
