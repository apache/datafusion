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
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use arrow::datatypes::DataType;
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowSchema};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        function::{AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs},
        utils::AggregateOrderSensitivity,
        Accumulator, GroupsAccumulator, ReversedUDAF,
    },
};
use datafusion::{
    error::Result,
    logical_expr::{
        AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, Signature,
    },
};

use crate::{
    arrow_wrappers::{WrappedArray, WrappedSchema},
    df_result, rresult, rresult_return,
    signature::{self, rvec_wrapped_to_vec_datatype, FFI_Signature},
};

mod accumulator;

/// A stable struct for sharing a [`AggregateUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_AggregateUDF {
    /// Return the udaf name.
    pub name: RString,

    pub signature: unsafe extern "C" fn(udaf: &Self) -> RResult<FFI_Signature, RString>,

    pub aliases: unsafe extern "C" fn(udaf: &Self) -> RVec<RString>,

    pub return_type: unsafe extern "C" fn(
        udaf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<WrappedSchema, RString>,

    pub is_nullable: bool,


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

unsafe extern "C" fn name_fn_wrapper(udaf: &FFI_AggregateUDF) -> RString {
    let private_data = udaf.private_data as *const AggregateUDFPrivateData;
    let udaf = &(*private_data).udaf;

    udaf.name().into()
}

unsafe extern "C" fn signature_fn_wrapper(
    udaf: &FFI_AggregateUDF,
) -> RResult<FFI_Signature, RString> {
    let private_data = udaf.private_data as *const AggregateUDFPrivateData;
    let udaf = &(*private_data).udaf;

    rresult!(udaf.signature().try_into())
}

unsafe extern "C" fn aliases_fn_wrapper(udaf: &FFI_AggregateUDF) -> RVec<RString> {
    let private_data = udaf.private_data as *const AggregateUDFPrivateData;
    let udaf = &(*private_data).udaf;

    udaf.aliases().iter().map(|s| s.to_owned().into()).collect()
}

unsafe extern "C" fn return_type_fn_wrapper(
    udaf: &FFI_AggregateUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<WrappedSchema, RString> {
    let private_data = udaf.private_data as *const AggregateUDFPrivateData;
    let udaf = &(*private_data).udaf;

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_type = udaf
        .return_type(&arg_types)
        .and_then(|v| FFI_ArrowSchema::try_from(v).map_err(DataFusionError::from))
        .map(WrappedSchema);

    rresult!(return_type)
}

unsafe extern "C" fn release_fn_wrapper(udaf: &mut FFI_AggregateUDF) {
    let private_data = Box::from_raw(udaf.private_data as *mut AggregateUDFPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(udaf: &FFI_AggregateUDF) -> FFI_AggregateUDF {
    let private_data = udaf.private_data as *const AggregateUDFPrivateData;
    let udaf_data = &(*private_data);

    Arc::clone(&udaf_data.udaf).into()
}

impl Clone for FFI_AggregateUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<AggregateUDF>> for FFI_AggregateUDF {
    fn from(udaf: Arc<AggregateUDF>) -> Self {
        let name = udaf.name().into();
        let is_nullable = udaf.is_nullable();

        let private_data = Box::new(AggregateUDFPrivateData { udaf });

        Self {
            name,
            is_nullable,
            signature: signature_fn_wrapper,
            aliases: aliases_fn_wrapper,
            return_type: return_type_fn_wrapper,
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
        unsafe {
            let ffi_signature = df_result!((udaf.signature)(udaf))?;
            let signature = (&ffi_signature).try_into()?;

            let aliases = (udaf.aliases)(udaf)
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            Ok(Self {
                udaf: udaf.clone(),
                signature,
                aliases,
            })
        }
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
        let arg_types = signature::vec_datatype_to_rvec_wrapped(arg_types)?;

        let result = unsafe { (self.udaf.return_type)(&self.udaf, arg_types) };

        let result = df_result!(result);

        result.and_then(|r| (&r.0).try_into().map_err(DataFusionError::from))
    }

    fn is_nullable(&self) -> bool {
        self.udaf.is_nullable
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {}

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {}

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {}

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        _beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {}

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {}

    fn reverse_expr(&self) -> ReversedUDAF {}

    fn coerce_types(&self, _arg_types: &[DataType]) -> Result<Vec<DataType>> {}

    fn equals(&self, other: &dyn AggregateUDFImpl) -> bool {}

    fn is_descending(&self) -> Option<bool> {}

    fn value_from_stats(&self, _statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {}

    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_udaf() -> Result<()> {
        let original_udaf = datafusion::functions::math::abs::AbsFunc::new();
        let original_udaf = Arc::new(AggregateUDF::from(original_udaf));

        let local_udaf: FFI_AggregateUDF = Arc::clone(&original_udaf).into();

        let foreign_udaf: ForeignAggregateUDF = (&local_udaf).try_into()?;

        assert!(original_udaf.name() == foreign_udaf.name());

        Ok(())
    }
}
