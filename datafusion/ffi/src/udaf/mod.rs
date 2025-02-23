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
    std_types::{ROption, RResult, RStr, RString, RVec},
    StableAbi,
};
use accumulator::{FFI_Accumulator, ForeignAccumulator};
use accumulator_args::{FFI_AccumulatorArgs, ForeignAccumulatorArgs};
use arrow::datatypes::{DataType, Field};
use arrow::ffi::FFI_ArrowSchema;
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
        type_coercion::functions::data_types_with_aggregate_udf,
        utils::AggregateOrderSensitivity,
        Accumulator, GroupsAccumulator,
    },
};
use datafusion::{
    error::Result,
    logical_expr::{AggregateUDF, AggregateUDFImpl, Signature},
};
use datafusion_proto_common::from_proto::parse_proto_fields_to_fields;
use groups_accumulator::{FFI_GroupsAccumulator, ForeignGroupsAccumulator};

use crate::{
    arrow_wrappers::WrappedSchema,
    df_result, rresult, rresult_return,
    util::{rvec_wrapped_to_vec_datatype, vec_datatype_to_rvec_wrapped},
    volatility::FFI_Volatility,
};
use prost::{DecodeError, Message};

mod accumulator;
mod accumulator_args;
mod groups_accumulator;

/// A stable struct for sharing a [`AggregateUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_AggregateUDF {
    /// FFI equivalent to the `name` of a [`AggregateUDF`]
    pub name: RString,

    /// FFI equivalent to the `name` of a [`AggregateUDF`]
    pub aliases: RVec<RString>,

    /// FFI equivalent to the `name` of a [`AggregateUDF`]
    pub volatility: FFI_Volatility,

    pub return_type: unsafe extern "C" fn(
        udaf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<WrappedSchema, RString>,

    pub is_nullable: bool,

    pub groups_accumulator_supported:
        unsafe extern "C" fn(udaf: &FFI_AggregateUDF, args: FFI_AccumulatorArgs) -> bool,

    pub accumulator: unsafe extern "C" fn(
        udaf: &FFI_AggregateUDF,
        args: FFI_AccumulatorArgs,
    ) -> RResult<FFI_Accumulator, RString>,

    pub create_sliding_accumulator:
        unsafe extern "C" fn(
            udaf: &FFI_AggregateUDF,
            args: FFI_AccumulatorArgs,
        ) -> RResult<FFI_Accumulator, RString>,

    #[allow(clippy::type_complexity)]
    pub state_fields: unsafe extern "C" fn(
        udaf: &FFI_AggregateUDF,
        name: &RStr,
        input_types: RVec<WrappedSchema>,
        return_type: WrappedSchema,
        ordering_fields: RVec<RVec<u8>>,
        is_distinct: bool,
    ) -> RResult<RVec<RVec<u8>>, RString>,

    pub create_groups_accumulator:
        unsafe extern "C" fn(
            udaf: &FFI_AggregateUDF,
            args: FFI_AccumulatorArgs,
        ) -> RResult<FFI_GroupsAccumulator, RString>,

    pub with_beneficial_ordering:
        unsafe extern "C" fn(
            udaf: &FFI_AggregateUDF,
            beneficial_ordering: bool,
        ) -> RResult<ROption<FFI_AggregateUDF>, RString>,

    pub order_sensitivity:
        unsafe extern "C" fn(udaf: &FFI_AggregateUDF) -> FFI_AggregateOrderSensitivity,

    /// Performs type coersion. To simply this interface, all UDFs are treated as having
    /// user defined signatures, which will in turn call coerce_types to be called. This
    /// call should be transparent to most users as the internal function performs the
    /// appropriate calls on the underlying [`ScalarUDF`]
    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<RVec<WrappedSchema>, RString>,

    /// Used to create a clone on the provider of the udaf. This should
    /// only need to be called by the receiver of the udaf.
    pub clone: unsafe extern "C" fn(udaf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udaf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udaf.
    /// A [`ForeignAggregateUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_AggregateUDF {}
unsafe impl Sync for FFI_AggregateUDF {}

pub struct AggregateUDFPrivateData {
    pub udaf: Arc<AggregateUDF>,
}

impl FFI_AggregateUDF {
    unsafe fn inner(&self) -> &Arc<AggregateUDF> {
        let private_data = self.private_data as *const AggregateUDFPrivateData;
        &(*private_data).udaf
    }
}

unsafe extern "C" fn return_type_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<WrappedSchema, RString> {
    let udaf = udaf.inner();

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_type = udaf
        .return_type(&arg_types)
        .and_then(|v| FFI_ArrowSchema::try_from(v).map_err(DataFusionError::from))
        .map(WrappedSchema);

    rresult!(return_type)
}

unsafe extern "C" fn accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> RResult<FFI_Accumulator, RString> {
    let udaf = udaf.inner();

    let accumulator_args = &rresult_return!(ForeignAccumulatorArgs::try_from(args));

    rresult!(udaf
        .accumulator(accumulator_args.into())
        .map(FFI_Accumulator::from))
}

unsafe extern "C" fn create_sliding_accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> RResult<FFI_Accumulator, RString> {
    let udaf = udaf.inner();

    let accumulator_args = &rresult_return!(ForeignAccumulatorArgs::try_from(args));

    rresult!(udaf
        .create_sliding_accumulator(accumulator_args.into())
        .map(FFI_Accumulator::from))
}

unsafe extern "C" fn create_groups_accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> RResult<FFI_GroupsAccumulator, RString> {
    let udaf = udaf.inner();

    let accumulator_args = &rresult_return!(ForeignAccumulatorArgs::try_from(args));

    rresult!(udaf
        .create_groups_accumulator(accumulator_args.into())
        .map(FFI_GroupsAccumulator::from))
}

unsafe extern "C" fn groups_accumulator_supported_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> bool {
    let udaf = udaf.inner();

    ForeignAccumulatorArgs::try_from(args)
        .map(|a| udaf.groups_accumulator_supported((&a).into()))
        .unwrap_or_else(|e| {
            log::warn!("Unable to parse accumulator args. {}", e);
            false
        })
}

unsafe extern "C" fn with_beneficial_ordering_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    beneficial_ordering: bool,
) -> RResult<ROption<FFI_AggregateUDF>, RString> {
    let udaf = udaf.inner().as_ref().clone();

    let result = rresult_return!(udaf.with_beneficial_ordering(beneficial_ordering));
    let result = rresult_return!(result
        .map(|func| func.with_beneficial_ordering(beneficial_ordering))
        .transpose())
    .flatten()
    .map(|func| FFI_AggregateUDF::from(Arc::new(func)));

    RResult::ROk(result.into())
}

unsafe extern "C" fn state_fields_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    name: &RStr,
    input_types: RVec<WrappedSchema>,
    return_type: WrappedSchema,
    ordering_fields: RVec<RVec<u8>>,
    is_distinct: bool,
) -> RResult<RVec<RVec<u8>>, RString> {
    let udaf = udaf.inner();

    let input_types = &rresult_return!(rvec_wrapped_to_vec_datatype(&input_types));
    let return_type = &rresult_return!(DataType::try_from(&return_type.0));

    let ordering_fields = &rresult_return!(ordering_fields
        .into_iter()
        .map(|field_bytes| datafusion_proto_common::Field::decode(field_bytes.as_ref()))
        .collect::<std::result::Result<Vec<_>, DecodeError>>());

    let ordering_fields = &rresult_return!(parse_proto_fields_to_fields(ordering_fields));

    let args = StateFieldsArgs {
        name: name.as_str(),
        input_types,
        return_type,
        ordering_fields,
        is_distinct,
    };

    let state_fields = rresult_return!(udaf.state_fields(args));
    let state_fields = rresult_return!(state_fields
        .iter()
        .map(datafusion_proto::protobuf::Field::try_from)
        .map(|v| v.map_err(DataFusionError::from))
        .collect::<Result<Vec<_>>>())
    .into_iter()
    .map(|field| field.encode_to_vec().into())
    .collect();

    RResult::ROk(state_fields)
}

unsafe extern "C" fn order_sensitivity_fn_wrapper(
    udaf: &FFI_AggregateUDF,
) -> FFI_AggregateOrderSensitivity {
    udaf.inner().order_sensitivity().into()
}

unsafe extern "C" fn coerce_types_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<RVec<WrappedSchema>, RString> {
    let udaf = udaf.inner();

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_types = rresult_return!(data_types_with_aggregate_udf(&arg_types, udaf));

    rresult!(vec_datatype_to_rvec_wrapped(&return_types))
}

unsafe extern "C" fn release_fn_wrapper(udaf: &mut FFI_AggregateUDF) {
    let private_data = Box::from_raw(udaf.private_data as *mut AggregateUDFPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(udaf: &FFI_AggregateUDF) -> FFI_AggregateUDF {
    Arc::clone(udaf.inner()).into()
}

impl Clone for FFI_AggregateUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<AggregateUDF>> for FFI_AggregateUDF {
    fn from(udaf: Arc<AggregateUDF>) -> Self {
        let name = udaf.name().into();
        let aliases = udaf.aliases().iter().map(|a| a.to_owned().into()).collect();
        let is_nullable = udaf.is_nullable();
        let volatility = udaf.signature().volatility.into();

        let private_data = Box::new(AggregateUDFPrivateData { udaf });

        Self {
            name,
            is_nullable,
            volatility,
            aliases,
            return_type: return_type_fn_wrapper,
            accumulator: accumulator_fn_wrapper,
            create_sliding_accumulator: create_sliding_accumulator_fn_wrapper,
            create_groups_accumulator: create_groups_accumulator_fn_wrapper,
            groups_accumulator_supported: groups_accumulator_supported_fn_wrapper,
            with_beneficial_ordering: with_beneficial_ordering_fn_wrapper,
            state_fields: state_fields_fn_wrapper,
            order_sensitivity: order_sensitivity_fn_wrapper,
            coerce_types: coerce_types_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl Drop for FFI_AggregateUDF {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignAggregateUDF is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_AggregateUDF.
#[derive(Debug)]
pub struct ForeignAggregateUDF {
    signature: Signature,
    aliases: Vec<String>,
    udaf: FFI_AggregateUDF,
}

unsafe impl Send for ForeignAggregateUDF {}
unsafe impl Sync for ForeignAggregateUDF {}

impl TryFrom<&FFI_AggregateUDF> for ForeignAggregateUDF {
    type Error = DataFusionError;

    fn try_from(udaf: &FFI_AggregateUDF) -> Result<Self, Self::Error> {
        let signature = Signature::user_defined((&udaf.volatility).into());
        let aliases = udaf.aliases.iter().map(|s| s.to_string()).collect();

        Ok(Self {
            udaf: udaf.clone(),
            signature,
            aliases,
        })
    }
}

impl AggregateUDFImpl for ForeignAggregateUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.udaf.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;

        let result = unsafe { (self.udaf.return_type)(&self.udaf, arg_types) };

        let result = df_result!(result);

        result.and_then(|r| (&r.0).try_into().map_err(DataFusionError::from))
    }

    fn is_nullable(&self) -> bool {
        self.udaf.is_nullable
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let args = acc_args.try_into()?;
        unsafe {
            df_result!((self.udaf.accumulator)(&self.udaf, args)).map(|accum| {
                Box::new(ForeignAccumulator::from(accum)) as Box<dyn Accumulator>
            })
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        unsafe {
            let name = RStr::from_str(args.name);
            let input_types = vec_datatype_to_rvec_wrapped(args.input_types)?;
            let return_type = WrappedSchema(FFI_ArrowSchema::try_from(args.return_type)?);
            let ordering_fields = args
                .ordering_fields
                .iter()
                .map(datafusion_proto::protobuf::Field::try_from)
                .map(|v| v.map_err(DataFusionError::from))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|proto_field| proto_field.encode_to_vec().into())
                .collect();

            let fields = df_result!((self.udaf.state_fields)(
                &self.udaf,
                &name,
                input_types,
                return_type,
                ordering_fields,
                args.is_distinct
            ))?;
            let fields = fields
                .into_iter()
                .map(|field_bytes| {
                    datafusion_proto_common::Field::decode(field_bytes.as_ref())
                        .map_err(|e| DataFusionError::Execution(e.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;

            parse_proto_fields_to_fields(fields.iter())
                .map_err(|e| DataFusionError::Execution(e.to_string()))
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        let args = match FFI_AccumulatorArgs::try_from(args) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Attempting to convert accumulator arguments: {}", e);
                return false;
            }
        };

        unsafe { (self.udaf.groups_accumulator_supported)(&self.udaf, args) }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let args = FFI_AccumulatorArgs::try_from(args)?;

        unsafe {
            df_result!((self.udaf.create_groups_accumulator)(&self.udaf, args)).map(
                |accum| {
                    Box::new(ForeignGroupsAccumulator::from(accum))
                        as Box<dyn GroupsAccumulator>
                },
            )
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        let args = args.try_into()?;
        unsafe {
            df_result!((self.udaf.create_sliding_accumulator)(&self.udaf, args)).map(
                |accum| Box::new(ForeignAccumulator::from(accum)) as Box<dyn Accumulator>,
            )
        }
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        unsafe {
            let result = df_result!((self.udaf.with_beneficial_ordering)(
                &self.udaf,
                beneficial_ordering
            ))?
            .into_option();

            let result = result
                .map(|func| ForeignAggregateUDF::try_from(&func))
                .transpose()?;

            Ok(result.map(|func| Arc::new(func) as Arc<dyn AggregateUDFImpl>))
        }
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        unsafe { (self.udaf.order_sensitivity)(&self.udaf).into() }
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        None
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        unsafe {
            let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;
            let result_types =
                df_result!((self.udaf.coerce_types)(&self.udaf, arg_types))?;
            Ok(rvec_wrapped_to_vec_datatype(&result_types)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_udaf() -> Result<()> {
        let original_udaf = datafusion::functions_aggregate::sum::Sum::new();
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        let local_udaf: FFI_AggregateUDF = Arc::clone(&original_udaf).into();

        let foreign_udaf: ForeignAggregateUDF = (&local_udaf).try_into()?;

        assert!(original_udaf.name() == foreign_udaf.name());

        Ok(())
    }
}

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_AggregateOrderSensitivity {
    Insensitive,
    HardRequirement,
    Beneficial,
}

impl From<FFI_AggregateOrderSensitivity> for AggregateOrderSensitivity {
    fn from(value: FFI_AggregateOrderSensitivity) -> Self {
        match value {
            FFI_AggregateOrderSensitivity::Insensitive => Self::Insensitive,
            FFI_AggregateOrderSensitivity::HardRequirement => Self::HardRequirement,
            FFI_AggregateOrderSensitivity::Beneficial => Self::Beneficial,
        }
    }
}

impl From<AggregateOrderSensitivity> for FFI_AggregateOrderSensitivity {
    fn from(value: AggregateOrderSensitivity) -> Self {
        match value {
            AggregateOrderSensitivity::Insensitive => Self::Insensitive,
            AggregateOrderSensitivity::HardRequirement => Self::HardRequirement,
            AggregateOrderSensitivity::Beneficial => Self::Beneficial,
        }
    }
}
