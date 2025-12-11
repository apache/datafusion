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

use abi_stable::{
    std_types::{ROption, RResult, RStr, RString, RVec},
    StableAbi,
};
use accumulator::FFI_Accumulator;
use accumulator_args::{FFI_AccumulatorArgs, ForeignAccumulatorArgs};
use arrow::datatypes::{DataType, Field};
use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::FieldRef;
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
        type_coercion::functions::fields_with_aggregate_udf,
        utils::AggregateOrderSensitivity,
        Accumulator, GroupsAccumulator,
    },
};
use datafusion::{
    error::Result,
    logical_expr::{AggregateUDF, AggregateUDFImpl, Signature},
};
use datafusion_common::ffi_datafusion_err;
use datafusion_proto_common::from_proto::parse_proto_fields_to_fields;
use groups_accumulator::FFI_GroupsAccumulator;
use std::hash::{Hash, Hasher};
use std::{ffi::c_void, sync::Arc};

use crate::util::{
    rvec_wrapped_to_vec_fieldref, vec_fieldref_to_rvec_wrapped, FFIResult,
};
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

    /// FFI equivalent to the `aliases` of a [`AggregateUDF`]
    pub aliases: RVec<RString>,

    /// FFI equivalent to the `volatility` of a [`AggregateUDF`]
    pub volatility: FFI_Volatility,

    /// Determines the return field of the underlying [`AggregateUDF`] based on the
    /// argument fields.
    pub return_field: unsafe extern "C" fn(
        udaf: &Self,
        arg_fields: RVec<WrappedSchema>,
    ) -> FFIResult<WrappedSchema>,

    /// FFI equivalent to the `is_nullable` of a [`AggregateUDF`]
    pub is_nullable: bool,

    /// FFI equivalent to [`AggregateUDF::groups_accumulator_supported`]
    pub groups_accumulator_supported:
        unsafe extern "C" fn(udaf: &FFI_AggregateUDF, args: FFI_AccumulatorArgs) -> bool,

    /// FFI equivalent to [`AggregateUDF::accumulator`]
    pub accumulator: unsafe extern "C" fn(
        udaf: &FFI_AggregateUDF,
        args: FFI_AccumulatorArgs,
    ) -> FFIResult<FFI_Accumulator>,

    /// FFI equivalent to [`AggregateUDF::create_sliding_accumulator`]
    pub create_sliding_accumulator: unsafe extern "C" fn(
        udaf: &FFI_AggregateUDF,
        args: FFI_AccumulatorArgs,
    )
        -> FFIResult<FFI_Accumulator>,

    /// FFI equivalent to [`AggregateUDF::state_fields`]
    #[allow(clippy::type_complexity)]
    pub state_fields: unsafe extern "C" fn(
        udaf: &FFI_AggregateUDF,
        name: &RStr,
        input_fields: RVec<WrappedSchema>,
        return_field: WrappedSchema,
        ordering_fields: RVec<RVec<u8>>,
        is_distinct: bool,
    ) -> FFIResult<RVec<RVec<u8>>>,

    /// FFI equivalent to [`AggregateUDF::create_groups_accumulator`]
    pub create_groups_accumulator:
        unsafe extern "C" fn(
            udaf: &FFI_AggregateUDF,
            args: FFI_AccumulatorArgs,
        ) -> FFIResult<FFI_GroupsAccumulator>,

    /// FFI equivalent to [`AggregateUDF::with_beneficial_ordering`]
    pub with_beneficial_ordering:
        unsafe extern "C" fn(
            udaf: &FFI_AggregateUDF,
            beneficial_ordering: bool,
        ) -> FFIResult<ROption<FFI_AggregateUDF>>,

    /// FFI equivalent to [`AggregateUDF::order_sensitivity`]
    pub order_sensitivity:
        unsafe extern "C" fn(udaf: &FFI_AggregateUDF) -> FFI_AggregateOrderSensitivity,

    /// Performs type coercion. To simply this interface, all UDFs are treated as having
    /// user defined signatures, which will in turn call coerce_types to be called. This
    /// call should be transparent to most users as the internal function performs the
    /// appropriate calls on the underlying [`AggregateUDF`]
    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> FFIResult<RVec<WrappedSchema>>,

    /// Used to create a clone on the provider of the udaf. This should
    /// only need to be called by the receiver of the udaf.
    pub clone: unsafe extern "C" fn(udaf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udaf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udaf.
    /// A [`ForeignAggregateUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
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

unsafe extern "C" fn return_field_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    arg_fields: RVec<WrappedSchema>,
) -> FFIResult<WrappedSchema> {
    let udaf = udaf.inner();

    let arg_fields = rresult_return!(rvec_wrapped_to_vec_fieldref(&arg_fields));

    let return_field = udaf
        .return_field(&arg_fields)
        .and_then(|v| {
            FFI_ArrowSchema::try_from(v.as_ref()).map_err(DataFusionError::from)
        })
        .map(WrappedSchema);

    rresult!(return_field)
}

unsafe extern "C" fn accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> FFIResult<FFI_Accumulator> {
    let udaf = udaf.inner();

    let accumulator_args = &rresult_return!(ForeignAccumulatorArgs::try_from(args));

    rresult!(udaf
        .accumulator(accumulator_args.into())
        .map(FFI_Accumulator::from))
}

unsafe extern "C" fn create_sliding_accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> FFIResult<FFI_Accumulator> {
    let udaf = udaf.inner();

    let accumulator_args = &rresult_return!(ForeignAccumulatorArgs::try_from(args));

    rresult!(udaf
        .create_sliding_accumulator(accumulator_args.into())
        .map(FFI_Accumulator::from))
}

unsafe extern "C" fn create_groups_accumulator_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    args: FFI_AccumulatorArgs,
) -> FFIResult<FFI_GroupsAccumulator> {
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
            log::warn!("Unable to parse accumulator args. {e}");
            false
        })
}

unsafe extern "C" fn with_beneficial_ordering_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    beneficial_ordering: bool,
) -> FFIResult<ROption<FFI_AggregateUDF>> {
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
    input_fields: RVec<WrappedSchema>,
    return_field: WrappedSchema,
    ordering_fields: RVec<RVec<u8>>,
    is_distinct: bool,
) -> FFIResult<RVec<RVec<u8>>> {
    let udaf = udaf.inner();

    let input_fields = &rresult_return!(rvec_wrapped_to_vec_fieldref(&input_fields));
    let return_field = rresult_return!(Field::try_from(&return_field.0)).into();

    let ordering_fields = &rresult_return!(ordering_fields
        .into_iter()
        .map(|field_bytes| datafusion_proto_common::Field::decode(field_bytes.as_ref()))
        .collect::<std::result::Result<Vec<_>, DecodeError>>());

    let ordering_fields = &rresult_return!(parse_proto_fields_to_fields(ordering_fields))
        .into_iter()
        .map(Arc::new)
        .collect::<Vec<_>>();

    let args = StateFieldsArgs {
        name: name.as_str(),
        input_fields,
        return_field,
        ordering_fields,
        is_distinct,
    };

    let state_fields = rresult_return!(udaf.state_fields(args));
    let state_fields = rresult_return!(state_fields
        .iter()
        .map(|f| f.as_ref())
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
) -> FFIResult<RVec<WrappedSchema>> {
    let udaf = udaf.inner();

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let arg_fields = arg_types
        .iter()
        .map(|dt| Field::new("f", dt.clone(), true))
        .map(Arc::new)
        .collect::<Vec<_>>();
    let return_types = rresult_return!(fields_with_aggregate_udf(&arg_fields, udaf))
        .into_iter()
        .map(|f| f.data_type().to_owned())
        .collect::<Vec<_>>();

    rresult!(vec_datatype_to_rvec_wrapped(&return_types))
}

unsafe extern "C" fn release_fn_wrapper(udaf: &mut FFI_AggregateUDF) {
    debug_assert!(!udaf.private_data.is_null());
    let private_data = Box::from_raw(udaf.private_data as *mut AggregateUDFPrivateData);
    drop(private_data);
    udaf.private_data = std::ptr::null_mut();
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
            return_field: return_field_fn_wrapper,
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
            library_marker_id: crate::get_library_marker_id,
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

impl PartialEq for ForeignAggregateUDF {
    fn eq(&self, other: &Self) -> bool {
        // FFI_AggregateUDF cannot be compared, so identity equality is the best we can do.
        std::ptr::eq(self, other)
    }
}
impl Eq for ForeignAggregateUDF {}
impl Hash for ForeignAggregateUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(self, state)
    }
}

impl From<&FFI_AggregateUDF> for Arc<dyn AggregateUDFImpl> {
    fn from(udaf: &FFI_AggregateUDF) -> Self {
        if (udaf.library_marker_id)() == crate::get_library_marker_id() {
            return Arc::clone(unsafe { udaf.inner().inner() });
        }

        let signature = Signature::user_defined((&udaf.volatility).into());
        let aliases = udaf.aliases.iter().map(|s| s.to_string()).collect();

        Arc::new(ForeignAggregateUDF {
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        unimplemented!()
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let arg_fields = vec_fieldref_to_rvec_wrapped(arg_fields)?;

        let result = unsafe { (self.udaf.return_field)(&self.udaf, arg_fields) };

        let result = df_result!(result);

        result.and_then(|r| {
            Field::try_from(&r.0)
                .map(Arc::new)
                .map_err(DataFusionError::from)
        })
    }

    fn is_nullable(&self) -> bool {
        self.udaf.is_nullable
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let args = acc_args.try_into()?;
        unsafe {
            df_result!((self.udaf.accumulator)(&self.udaf, args))
                .map(<Box<dyn Accumulator>>::from)
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        unsafe {
            let name = RStr::from_str(args.name);
            let input_fields = vec_fieldref_to_rvec_wrapped(args.input_fields)?;
            let return_field =
                WrappedSchema(FFI_ArrowSchema::try_from(args.return_field.as_ref())?);
            let ordering_fields = args
                .ordering_fields
                .iter()
                .map(|f| f.as_ref())
                .map(datafusion_proto::protobuf::Field::try_from)
                .map(|v| v.map_err(DataFusionError::from))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .map(|proto_field| proto_field.encode_to_vec().into())
                .collect();

            let fields = df_result!((self.udaf.state_fields)(
                &self.udaf,
                &name,
                input_fields,
                return_field,
                ordering_fields,
                args.is_distinct
            ))?;
            let fields = fields
                .into_iter()
                .map(|field_bytes| {
                    datafusion_proto_common::Field::decode(field_bytes.as_ref())
                        .map_err(|e| ffi_datafusion_err!("{e}"))
                })
                .collect::<Result<Vec<_>>>()?;

            parse_proto_fields_to_fields(fields.iter())
                .map(|fields| fields.into_iter().map(Arc::new).collect())
                .map_err(|e| ffi_datafusion_err!("{e}"))
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        let args = match FFI_AccumulatorArgs::try_from(args) {
            Ok(v) => v,
            Err(e) => {
                log::warn!("Attempting to convert accumulator arguments: {e}");
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
            df_result!((self.udaf.create_groups_accumulator)(&self.udaf, args))
                .map(<Box<dyn GroupsAccumulator>>::from)
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
            df_result!((self.udaf.create_sliding_accumulator)(&self.udaf, args))
                .map(<Box<dyn Accumulator>>::from)
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

            let result = result.map(|func| <Arc<dyn AggregateUDFImpl>>::from(&func));

            Ok(result)
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

#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub enum FFI_AggregateOrderSensitivity {
    Insensitive,
    HardRequirement,
    SoftRequirement,
    Beneficial,
}

impl From<FFI_AggregateOrderSensitivity> for AggregateOrderSensitivity {
    fn from(value: FFI_AggregateOrderSensitivity) -> Self {
        match value {
            FFI_AggregateOrderSensitivity::Insensitive => Self::Insensitive,
            FFI_AggregateOrderSensitivity::HardRequirement => Self::HardRequirement,
            FFI_AggregateOrderSensitivity::SoftRequirement => Self::SoftRequirement,
            FFI_AggregateOrderSensitivity::Beneficial => Self::Beneficial,
        }
    }
}

impl From<AggregateOrderSensitivity> for FFI_AggregateOrderSensitivity {
    fn from(value: AggregateOrderSensitivity) -> Self {
        match value {
            AggregateOrderSensitivity::Insensitive => Self::Insensitive,
            AggregateOrderSensitivity::HardRequirement => Self::HardRequirement,
            AggregateOrderSensitivity::SoftRequirement => Self::SoftRequirement,
            AggregateOrderSensitivity::Beneficial => Self::Beneficial,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use datafusion::{
        common::create_array, functions_aggregate::sum::Sum,
        physical_expr::PhysicalSortExpr, physical_plan::expressions::col,
        scalar::ScalarValue,
    };
    use std::any::Any;
    use std::collections::HashMap;

    #[derive(Default, Debug, Hash, Eq, PartialEq)]
    struct SumWithCopiedMetadata {
        inner: Sum,
    }

    impl AggregateUDFImpl for SumWithCopiedMetadata {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            self.inner.name()
        }

        fn signature(&self) -> &Signature {
            self.inner.signature()
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            unimplemented!()
        }

        fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
            // Copy the input field, so any metadata gets returned
            Ok(Arc::clone(&arg_fields[0]))
        }

        fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
            self.inner.accumulator(acc_args)
        }
    }

    fn create_test_foreign_udaf(
        original_udaf: impl AggregateUDFImpl + 'static,
    ) -> Result<AggregateUDF> {
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        let mut local_udaf: FFI_AggregateUDF = Arc::clone(&original_udaf).into();
        local_udaf.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_udaf: Arc<dyn AggregateUDFImpl> = (&local_udaf).into();
        Ok(AggregateUDF::new_from_shared_impl(foreign_udaf))
    }

    #[test]
    fn test_round_trip_udaf() -> Result<()> {
        let original_udaf = Sum::new();
        let original_name = original_udaf.name().to_owned();
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        // Convert to FFI format
        let mut local_udaf: FFI_AggregateUDF = Arc::clone(&original_udaf).into();
        local_udaf.library_marker_id = crate::mock_foreign_marker_id;

        // Convert back to native format
        let foreign_udaf: Arc<dyn AggregateUDFImpl> = (&local_udaf).into();
        let foreign_udaf = AggregateUDF::new_from_shared_impl(foreign_udaf);

        assert_eq!(original_name, foreign_udaf.name());
        Ok(())
    }

    #[test]
    fn test_foreign_udaf_aliases() -> Result<()> {
        let foreign_udaf =
            create_test_foreign_udaf(Sum::new())?.with_aliases(["my_function"]);

        let return_field =
            foreign_udaf
                .return_field(&[Field::new("a", DataType::Float64, true).into()])?;
        let return_type = return_field.data_type();
        assert_eq!(return_type, &DataType::Float64);
        Ok(())
    }

    #[test]
    fn test_foreign_udaf_accumulator() -> Result<()> {
        let foreign_udaf = create_test_foreign_udaf(Sum::new())?;

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        let acc_args = AccumulatorArgs {
            return_field: Field::new("f", DataType::Float64, true).into(),
            schema: &schema,
            expr_fields: &[Field::new("a", DataType::Float64, true).into()],
            ignore_nulls: true,
            order_bys: &[PhysicalSortExpr::new_default(col("a", &schema)?)],
            is_reversed: false,
            name: "round_trip",
            is_distinct: true,
            exprs: &[col("a", &schema)?],
        };
        let mut accumulator = foreign_udaf.accumulator(acc_args)?;
        let values = create_array!(Float64, vec![10., 20., 30., 40., 50.]);
        accumulator.update_batch(&[values])?;
        let resultant_value = accumulator.evaluate()?;
        assert_eq!(resultant_value, ScalarValue::Float64(Some(150.)));

        Ok(())
    }

    #[test]
    fn test_round_trip_udaf_metadata() -> Result<()> {
        let original_udaf = SumWithCopiedMetadata::default();
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        // Convert to FFI format
        let local_udaf: FFI_AggregateUDF = Arc::clone(&original_udaf).into();

        // Convert back to native format
        let foreign_udaf: Arc<dyn AggregateUDFImpl> = (&local_udaf).into();
        let foreign_udaf = AggregateUDF::new_from_shared_impl(foreign_udaf);

        let metadata: HashMap<String, String> =
            [("a_key".to_string(), "a_value".to_string())]
                .into_iter()
                .collect();
        let input_field = Arc::new(
            Field::new("a", DataType::Float64, false).with_metadata(metadata.clone()),
        );
        let return_field = foreign_udaf.return_field(&[input_field])?;

        assert_eq!(&metadata, return_field.metadata());
        Ok(())
    }

    #[test]
    fn test_beneficial_ordering() -> Result<()> {
        let foreign_udaf = create_test_foreign_udaf(
            datafusion::functions_aggregate::first_last::FirstValue::new(),
        )?;

        let foreign_udaf = foreign_udaf.with_beneficial_ordering(true)?.unwrap();

        assert_eq!(
            foreign_udaf.order_sensitivity(),
            AggregateOrderSensitivity::Beneficial
        );

        let a_field = Arc::new(Field::new("a", DataType::Float64, true));
        let state_fields = foreign_udaf.state_fields(StateFieldsArgs {
            name: "a",
            input_fields: &[Field::new("f", DataType::Float64, true).into()],
            return_field: Field::new("f", DataType::Float64, true).into(),
            ordering_fields: &[Arc::clone(&a_field)],
            is_distinct: false,
        })?;

        assert_eq!(state_fields.len(), 3);
        assert_eq!(state_fields[1], a_field);
        Ok(())
    }

    #[test]
    fn test_sliding_accumulator() -> Result<()> {
        let foreign_udaf = create_test_foreign_udaf(Sum::new())?;

        let schema = Schema::new(vec![Field::new("a", DataType::Float64, true)]);
        // Note: sum distinct is only support Int64 until now
        let acc_args = AccumulatorArgs {
            return_field: Field::new("f", DataType::Float64, true).into(),
            schema: &schema,
            expr_fields: &[Field::new("a", DataType::Float64, true).into()],
            ignore_nulls: true,
            order_bys: &[PhysicalSortExpr::new_default(col("a", &schema)?)],
            is_reversed: false,
            name: "round_trip",
            is_distinct: false,
            exprs: &[col("a", &schema)?],
        };

        let mut accumulator = foreign_udaf.create_sliding_accumulator(acc_args)?;
        let values = create_array!(Float64, vec![10., 20., 30., 40., 50.]);
        accumulator.update_batch(&[values])?;
        let resultant_value = accumulator.evaluate()?;
        assert_eq!(resultant_value, ScalarValue::Float64(Some(150.)));

        Ok(())
    }

    fn test_round_trip_order_sensitivity(sensitivity: AggregateOrderSensitivity) {
        let ffi_sensitivity: FFI_AggregateOrderSensitivity = sensitivity.into();
        let round_trip_sensitivity: AggregateOrderSensitivity = ffi_sensitivity.into();

        assert_eq!(sensitivity, round_trip_sensitivity);
    }

    #[test]
    fn test_round_trip_all_order_sensitivities() {
        test_round_trip_order_sensitivity(AggregateOrderSensitivity::Insensitive);
        test_round_trip_order_sensitivity(AggregateOrderSensitivity::HardRequirement);
        test_round_trip_order_sensitivity(AggregateOrderSensitivity::SoftRequirement);
        test_round_trip_order_sensitivity(AggregateOrderSensitivity::Beneficial);
    }

    #[test]
    fn test_ffi_udaf_local_bypass() -> Result<()> {
        let original_udaf = Sum::new();
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        let mut ffi_udaf = FFI_AggregateUDF::from(original_udaf);

        // Verify local libraries can be downcast to their original
        let foreign_udaf: Arc<dyn AggregateUDFImpl> = (&ffi_udaf).into();
        assert!(foreign_udaf.as_any().downcast_ref::<Sum>().is_some());

        // Verify different library markers generate foreign providers
        ffi_udaf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udaf: Arc<dyn AggregateUDFImpl> = (&ffi_udaf).into();
        assert!(foreign_udaf
            .as_any()
            .downcast_ref::<ForeignAggregateUDF>()
            .is_some());

        Ok(())
    }
}
