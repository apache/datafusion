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

use std::ffi::c_void;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use abi_stable::StableAbi;
use abi_stable::std_types::{RResult, RString, RVec};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::error::ArrowError;
use arrow::ffi::{FFI_ArrowSchema, from_ffi, to_ffi};
use arrow_schema::FieldRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature,
};
use return_type_args::{
    FFI_ReturnFieldArgs, ForeignReturnFieldArgs, ForeignReturnFieldArgsOwned,
};

use crate::arrow_wrappers::{WrappedArray, WrappedSchema};
use crate::util::{
    FFIResult, rvec_wrapped_to_vec_datatype, vec_datatype_to_rvec_wrapped,
};
use crate::volatility::FFI_Volatility;
use crate::{df_result, rresult, rresult_return};

pub mod return_type_args;

/// A stable struct for sharing a [`ScalarUDF`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_ScalarUDF {
    /// FFI equivalent to the `name` of a [`ScalarUDF`]
    pub name: RString,

    /// FFI equivalent to the `aliases` of a [`ScalarUDF`]
    pub aliases: RVec<RString>,

    /// FFI equivalent to the `volatility` of a [`ScalarUDF`]
    pub volatility: FFI_Volatility,

    /// Determines the return info of the underlying [`ScalarUDF`].
    pub return_field_from_args: unsafe extern "C" fn(
        udf: &Self,
        args: FFI_ReturnFieldArgs,
    ) -> FFIResult<WrappedSchema>,

    /// Execute the underlying [`ScalarUDF`] and return the result as a `FFI_ArrowArray`
    /// within an AbiStable wrapper.
    pub invoke_with_args: unsafe extern "C" fn(
        udf: &Self,
        args: RVec<WrappedArray>,
        arg_fields: RVec<WrappedSchema>,
        num_rows: usize,
        return_field: WrappedSchema,
    ) -> FFIResult<WrappedArray>,

    /// See [`ScalarUDFImpl`] for details on short_circuits
    pub short_circuits: bool,

    /// Performs type coercion. To simply this interface, all UDFs are treated as having
    /// user defined signatures, which will in turn call coerce_types to be called. This
    /// call should be transparent to most users as the internal function performs the
    /// appropriate calls on the underlying [`ScalarUDF`]
    pub coerce_types: unsafe extern "C" fn(
        udf: &Self,
        arg_types: RVec<WrappedSchema>,
    ) -> FFIResult<RVec<WrappedSchema>>,

    /// Used to create a clone on the provider of the udf. This should
    /// only need to be called by the receiver of the udf.
    pub clone: unsafe extern "C" fn(udf: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(udf: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the udf.
    /// A [`ForeignScalarUDF`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_ScalarUDF {}
unsafe impl Sync for FFI_ScalarUDF {}

pub struct ScalarUDFPrivateData {
    pub udf: Arc<ScalarUDF>,
}

impl FFI_ScalarUDF {
    fn inner(&self) -> &Arc<ScalarUDF> {
        let private_data = self.private_data as *const ScalarUDFPrivateData;
        unsafe { &(*private_data).udf }
    }
}

unsafe extern "C" fn return_field_from_args_fn_wrapper(
    udf: &FFI_ScalarUDF,
    args: FFI_ReturnFieldArgs,
) -> FFIResult<WrappedSchema> {
    let args: ForeignReturnFieldArgsOwned = rresult_return!((&args).try_into());
    let args_ref: ForeignReturnFieldArgs = (&args).into();

    let return_type = udf
        .inner()
        .return_field_from_args((&args_ref).into())
        .and_then(|f| FFI_ArrowSchema::try_from(&f).map_err(DataFusionError::from))
        .map(WrappedSchema);

    rresult!(return_type)
}

unsafe extern "C" fn coerce_types_fn_wrapper(
    udf: &FFI_ScalarUDF,
    arg_types: RVec<WrappedSchema>,
) -> FFIResult<RVec<WrappedSchema>> {
    let arg_types = rresult_return!(rvec_wrapped_to_vec_datatype(&arg_types));

    let arg_fields = arg_types
        .iter()
        .map(|dt| Field::new("f", dt.clone(), true))
        .map(Arc::new)
        .collect::<Vec<_>>();
    let return_types =
        rresult_return!(fields_with_udf(&arg_fields, udf.inner().as_ref()))
            .into_iter()
            .map(|f| f.data_type().to_owned())
            .collect::<Vec<_>>();

    rresult!(vec_datatype_to_rvec_wrapped(&return_types))
}

unsafe extern "C" fn invoke_with_args_fn_wrapper(
    udf: &FFI_ScalarUDF,
    args: RVec<WrappedArray>,
    arg_fields: RVec<WrappedSchema>,
    number_rows: usize,
    return_field: WrappedSchema,
) -> FFIResult<WrappedArray> {
    unsafe {
        let args = args
            .into_iter()
            .map(|arr| {
                from_ffi(arr.array, &arr.schema.0)
                    .map(|v| ColumnarValue::Array(arrow::array::make_array(v)))
            })
            .collect::<std::result::Result<_, _>>();

        let args = rresult_return!(args);
        let return_field = rresult_return!(Field::try_from(&return_field.0)).into();

        let arg_fields = arg_fields
            .into_iter()
            .map(|wrapped_field| {
                Field::try_from(&wrapped_field.0)
                    .map(Arc::new)
                    .map_err(DataFusionError::from)
            })
            .collect::<Result<Vec<FieldRef>>>();
        let arg_fields = rresult_return!(arg_fields);

        let args = ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            // TODO: pass config options: https://github.com/apache/datafusion/issues/17035
            config_options: Arc::new(ConfigOptions::default()),
        };

        let result = rresult_return!(
            udf.inner()
                .invoke_with_args(args)
                .and_then(|r| r.to_array(number_rows))
        );

        let (result_array, result_schema) = rresult_return!(to_ffi(&result.to_data()));

        RResult::ROk(WrappedArray {
            array: result_array,
            schema: WrappedSchema(result_schema),
        })
    }
}

unsafe extern "C" fn release_fn_wrapper(udf: &mut FFI_ScalarUDF) {
    unsafe {
        debug_assert!(!udf.private_data.is_null());
        let private_data = Box::from_raw(udf.private_data as *mut ScalarUDFPrivateData);
        drop(private_data);
        udf.private_data = std::ptr::null_mut();
    }
}

unsafe extern "C" fn clone_fn_wrapper(udf: &FFI_ScalarUDF) -> FFI_ScalarUDF {
    unsafe {
        let private_data = udf.private_data as *const ScalarUDFPrivateData;
        let udf_data = &(*private_data);

        Arc::clone(&udf_data.udf).into()
    }
}

impl Clone for FFI_ScalarUDF {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl From<Arc<ScalarUDF>> for FFI_ScalarUDF {
    fn from(udf: Arc<ScalarUDF>) -> Self {
        let name = udf.name().into();
        let aliases = udf.aliases().iter().map(|a| a.to_owned().into()).collect();
        let volatility = udf.signature().volatility.into();
        let short_circuits = udf.short_circuits();

        let private_data = Box::new(ScalarUDFPrivateData { udf });

        Self {
            name,
            aliases,
            volatility,
            short_circuits,
            invoke_with_args: invoke_with_args_fn_wrapper,
            return_field_from_args: return_field_from_args_fn_wrapper,
            coerce_types: coerce_types_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
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
    aliases: Vec<String>,
    udf: FFI_ScalarUDF,
    signature: Signature,
}

unsafe impl Send for ForeignScalarUDF {}
unsafe impl Sync for ForeignScalarUDF {}

impl PartialEq for ForeignScalarUDF {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            name,
            aliases,
            udf,
            signature,
        } = self;
        name == &other.name
            && aliases == &other.aliases
            && std::ptr::eq(udf, &other.udf)
            && signature == &other.signature
    }
}
impl Eq for ForeignScalarUDF {}

impl Hash for ForeignScalarUDF {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            name,
            aliases,
            udf,
            signature,
        } = self;
        name.hash(state);
        aliases.hash(state);
        std::ptr::hash(udf, state);
        signature.hash(state);
    }
}

impl From<&FFI_ScalarUDF> for Arc<dyn ScalarUDFImpl> {
    fn from(udf: &FFI_ScalarUDF) -> Self {
        if (udf.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(udf.inner().inner())
        } else {
            let name = udf.name.to_owned().into();
            let signature = Signature::user_defined((&udf.volatility).into());

            let aliases = udf.aliases.iter().map(|s| s.to_string()).collect();

            Arc::new(ForeignScalarUDF {
                name,
                udf: udf.clone(),
                aliases,
                signature,
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

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("ForeignScalarUDF implements return_field_from_args instead.")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let args: FFI_ReturnFieldArgs = args.try_into()?;

        let result = unsafe { (self.udf.return_field_from_args)(&self.udf, args) };

        let result = df_result!(result);

        result.and_then(|r| {
            Field::try_from(&r.0)
                .map(Arc::new)
                .map_err(DataFusionError::from)
        })
    }

    fn invoke_with_args(&self, invoke_args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows,
            return_field,
            // TODO: pass config options: https://github.com/apache/datafusion/issues/17035
            config_options: _config_options,
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

        let arg_fields_wrapped = arg_fields
            .iter()
            .map(FFI_ArrowSchema::try_from)
            .collect::<std::result::Result<Vec<_>, ArrowError>>()?;

        let arg_fields = arg_fields_wrapped
            .into_iter()
            .map(WrappedSchema)
            .collect::<RVec<_>>();

        let return_field = return_field.as_ref().clone();
        let return_field = WrappedSchema(FFI_ArrowSchema::try_from(return_field)?);

        let result = unsafe {
            (self.udf.invoke_with_args)(
                &self.udf,
                args,
                arg_fields,
                number_rows,
                return_field,
            )
        };

        let result = df_result!(result)?;
        let result_array: ArrayRef = result.try_into()?;

        Ok(ColumnarValue::Array(result_array))
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn short_circuits(&self) -> bool {
        self.udf.short_circuits
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        unsafe {
            let arg_types = vec_datatype_to_rvec_wrapped(arg_types)?;
            let result_types = df_result!((self.udf.coerce_types)(&self.udf, arg_types))?;
            Ok(rvec_wrapped_to_vec_datatype(&result_types)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip_scalar_udf() -> Result<()> {
        let original_udf = datafusion::functions::math::abs::AbsFunc::new();
        let original_udf = Arc::new(ScalarUDF::from(original_udf));

        let mut local_udf: FFI_ScalarUDF = Arc::clone(&original_udf).into();
        local_udf.library_marker_id = crate::mock_foreign_marker_id;

        let foreign_udf: Arc<dyn ScalarUDFImpl> = (&local_udf).into();

        assert_eq!(original_udf.name(), foreign_udf.name());

        Ok(())
    }

    #[test]
    fn test_ffi_udf_local_bypass() -> Result<()> {
        use datafusion::functions::math::abs::AbsFunc;
        let original_udf = AbsFunc::new();
        let original_udf = Arc::new(ScalarUDF::from(original_udf));

        let mut ffi_udf = FFI_ScalarUDF::from(original_udf);

        // Verify local libraries can be downcast to their original
        let foreign_udf: Arc<dyn ScalarUDFImpl> = (&ffi_udf).into();
        assert!(foreign_udf.as_any().downcast_ref::<AbsFunc>().is_some());

        // Verify different library markers generate foreign providers
        ffi_udf.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_udf: Arc<dyn ScalarUDFImpl> = (&ffi_udf).into();
        assert!(
            foreign_udf
                .as_any()
                .downcast_ref::<ForeignScalarUDF>()
                .is_some()
        );

        Ok(())
    }
}
