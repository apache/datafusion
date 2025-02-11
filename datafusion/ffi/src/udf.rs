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
use arrow::{
    array::ArrayRef,
    ffi::{from_ffi, to_ffi, FFI_ArrowSchema},
};
use arrow_schema::{ArrowError, DataType};
use datafusion::error::DataFusionError;
use datafusion::{
    error::Result,
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    },
};

use crate::{
    arrow_wrappers::{WrappedArray, WrappedSchema},
    df_result, rresult, rresult_return,
    signature::{self, rvec_wrapped_to_vec_datatype, FFI_Signature},
};

/// A stable struct for sharing a [`ScalarUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_ScalarUDF {
    /// Return the udf name.
    pub name: unsafe extern "C" fn(udf: &Self) -> RString,

    pub signature: unsafe extern "C" fn(udf: &Self) -> RResult<FFI_Signature, RString>,

    pub aliases: unsafe extern "C" fn(udf: &Self) -> RVec<RString>,

    pub return_type: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> RResult<WrappedSchema, RString>,

    pub invoke_with_args: unsafe extern "C" fn(
        udf: &Self,
        args: RVec<WrappedArray>,
        num_rows: usize,
        return_type: WrappedSchema,
    ) -> RResult<WrappedArray, RString>,

    pub short_circuits: unsafe extern "C" fn(udf: &Self) -> bool,

    /// Used to create a clone on the provider of the udf. This should
    /// only need to be called by the receiver of the udf.
    pub clone: unsafe extern "C" fn(udf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udf.
    /// A [`ForeignScalarUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_ScalarUDF {}
unsafe impl Sync for FFI_ScalarUDF {}

pub struct ScalarUDFPrivateData {
    pub udf: Arc<ScalarUDF>,
}

unsafe extern "C" fn name_fn_wrapper(udf: &FFI_ScalarUDF) -> RString {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    udf.name().into()
}

unsafe extern "C" fn signature_fn_wrapper(
    udf: &FFI_ScalarUDF,
) -> RResult<FFI_Signature, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    rresult!(udf.signature().try_into())
}

unsafe extern "C" fn aliases_fn_wrapper(udf: &FFI_ScalarUDF) -> RVec<RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    udf.aliases().iter().map(|s| s.to_owned().into()).collect()
}

unsafe extern "C" fn return_type_fn_wrapper(
    udf: &FFI_ScalarUDF,
    arg_types: RVec<WrappedSchema>,
) -> RResult<WrappedSchema, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let return_type = udf
        .return_type(&arg_types)
        .and_then(|v| FFI_ArrowSchema::try_from(v).map_err(DataFusionError::from))
        .map(WrappedSchema);

    rresult!(return_type)
}

unsafe extern "C" fn invoke_with_args_fn_wrapper(
    udf: &FFI_ScalarUDF,
    args: RVec<WrappedArray>,
    number_rows: usize,
    return_type: WrappedSchema,
) -> RResult<WrappedArray, RString> {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    let args = args
        .into_iter()
        .map(|arr| {
            from_ffi(arr.array, &arr.schema.0)
                .map(|v| ColumnarValue::Array(arrow::array::make_array(v)))
        })
        .collect::<std::result::Result<_, _>>();

    let args = rresult_return!(args);
    let return_type = rresult_return!(DataType::try_from(&return_type.0));

    let args = ScalarFunctionArgs {
        args,
        number_rows,
        return_type: &return_type,
    };

    let result = rresult_return!(udf
        .invoke_with_args(args)
        .and_then(|r| r.to_array(number_rows)));

    let (result_array, result_schema) = rresult_return!(to_ffi(&result.to_data()));

    RResult::ROk(WrappedArray {
        array: result_array,
        schema: WrappedSchema(result_schema),
    })

    // TODO create a proc macro to do all the conversion and return from result to RResult
}

unsafe extern "C" fn short_circuits_fn_wrapper(udf: &FFI_ScalarUDF) -> bool {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf = &(*private_data).udf;

    udf.short_circuits()
}

unsafe extern "C" fn release_fn_wrapper(udf: &mut FFI_ScalarUDF) {
    let private_data = Box::from_raw(udf.private_data as *mut ScalarUDFPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(udf: &FFI_ScalarUDF) -> FFI_ScalarUDF {
    let private_data = udf.private_data as *const ScalarUDFPrivateData;
    let udf_data = &(*private_data);

    Arc::clone(&udf_data.udf).into()
}

impl Clone for FFI_ScalarUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<ScalarUDF>> for FFI_ScalarUDF {
    fn from(udf: Arc<ScalarUDF>) -> Self {
        let private_data = Box::new(ScalarUDFPrivateData { udf });

        Self {
            name: name_fn_wrapper,
            signature: signature_fn_wrapper,
            aliases: aliases_fn_wrapper,
            invoke_with_args: invoke_with_args_fn_wrapper,
            short_circuits: short_circuits_fn_wrapper,
            return_type: return_type_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

impl Drop for FFI_ScalarUDF {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// This struct is used to access an UDF provided by a foreign
/// library across a FFI boundary.
///
/// The ForeignScalarUDF is to be used by the caller of the UDF, so it has
/// no knowledge or access to the private data. All interaction with the UDF
/// must occur through the functions defined in FFI_ScalarUDF.
#[derive(Debug)]
pub struct ForeignScalarUDF {
    name: String,
    signature: Signature,
    aliases: Vec<String>,
    udf: FFI_ScalarUDF,
}

unsafe impl Send for ForeignScalarUDF {}
unsafe impl Sync for ForeignScalarUDF {}

impl TryFrom<&FFI_ScalarUDF> for ForeignScalarUDF {
    type Error = DataFusionError;

    fn try_from(udf: &FFI_ScalarUDF) -> Result<Self, Self::Error> {
        unsafe {
            let name = (udf.name)(udf).into();
            let ffi_signature = df_result!((udf.signature)(udf))?;
            let signature = (&ffi_signature).try_into()?;

            let aliases = (udf.aliases)(udf)
                .into_iter()
                .map(|s| s.to_string())
                .collect();

            Ok(Self {
                name,
                udf: udf.clone(),
                signature,
                aliases,
            })
        }
    }
}

impl ScalarUDFImpl for ForeignScalarUDF {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// What [`DataType`] will be returned by this function, given the types of
    /// the arguments.
    ///
    /// # Notes
    ///
    /// If you provide an implementation for [`Self::return_type_from_args`],
    /// DataFusion will not call `return_type` (this function). In such cases
    /// is recommended to return [`DataFusionError::Internal`].
    ///
    /// [`DataFusionError::Internal`]: datafusion_common::DataFusionError::Internal
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_types = signature::vec_datatype_to_rvec_wrapped(arg_types)?;

        let result = unsafe { (self.udf.return_type)(&self.udf, arg_types) };

        let result = df_result!(result);

        result.and_then(|r| (&r.0).try_into().map_err(DataFusionError::from))
    }

    /// Invoke the function returning the appropriate result.
    ///
    /// # Performance
    ///
    /// For the best performance, the implementations should handle the common case
    /// when one or more of their arguments are constant values (aka
    /// [`ColumnarValue::Scalar`]).
    ///
    /// [`ColumnarValue::values_to_arrays`] can be used to convert the arguments
    /// to arrays, which will likely be simpler code, but be slower.
    fn invoke_with_args(&self, invoke_args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            number_rows,
            return_type,
        } = invoke_args;

        let args = args
            .into_iter()
            .map(|v| v.to_array(number_rows))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|v| {
                to_ffi(&v.to_data()).map(|(ffi_array, ffi_schema)| WrappedArray {
                    array: ffi_array,
                    schema: WrappedSchema(ffi_schema),
                })
            })
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?
            .into();

        let return_type = WrappedSchema(FFI_ArrowSchema::try_from(return_type)?);

        let result = unsafe {
            (self.udf.invoke_with_args)(&self.udf, args, number_rows, return_type)
        };

        let result = df_result!(result)?;
        let result_array: ArrayRef = result.try_into()?;

        Ok(ColumnarValue::Array(result_array))
    }

    /// Returns any aliases (alternate names) for this function.
    ///
    /// Aliases can be used to invoke the same function using different names.
    /// For example in some databases `now()` and `current_timestamp()` are
    /// aliases for the same function. This behavior can be obtained by
    /// returning `current_timestamp` as an alias for the `now` function.
    ///
    /// Note: `aliases` should only include names other than [`Self::name`].
    /// Defaults to `[]` (no aliases)
    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn short_circuits(&self) -> bool {
        unsafe { (self.udf.short_circuits)(&self.udf) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_scalar_udf() -> Result<()> {
        let original_udf = datafusion::functions::math::abs::AbsFunc::new();
        let original_udf = Arc::new(ScalarUDF::from(original_udf));

        let local_udf: FFI_ScalarUDF = Arc::clone(&original_udf).into();

        let foreign_udf: ForeignScalarUDF = (&local_udf).try_into()?;

        assert!(original_udf.name() == foreign_udf.name());

        Ok(())
    }
}
