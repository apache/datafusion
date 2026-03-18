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

use std::sync::Arc;

use crate::ffi_option::FfiResult;
use arrow::datatypes::{DataType, Field};
use arrow::ffi::FFI_ArrowSchema;
use arrow_schema::FieldRef;
use stabby::alloc::string::String as SString;
use stabby::alloc::vec::Vec as SVec;

use crate::arrow_wrappers::WrappedSchema;

/// Convenience type for results passed through the FFI boundary. Since the
/// `DataFusionError` enum is complex and little value is gained from creating
/// a FFI safe variant of it, we convert errors to strings when passing results
/// back. These are converted back and forth using the `df_result`, `rresult`,
/// and `rresult_return` macros.
pub type FFIResult<T> = FfiResult<T, SString>;

/// This macro is a helpful conversion utility to convert from an FFIResult to a
/// DataFusion result.
#[macro_export]
macro_rules! df_result {
    ( $x:expr ) => {
        match Into::<::std::result::Result<_, _>>::into($x) {
            Ok(v) => Ok(v),
            Err(err) => {
                datafusion_common::ffi_err!("{err}")
            }
        }
    };
}

/// This macro is a helpful conversion utility to convert from a DataFusion Result to an FFIResult.
#[macro_export]
macro_rules! rresult {
    ( $x:expr ) => {
        match $x {
            Ok(v) => $crate::ffi_option::FfiResult::Ok(v),
            Err(e) => $crate::ffi_option::FfiResult::Err(
                stabby::alloc::string::String::from(e.to_string().as_str()),
            ),
        }
    };
}

/// This macro is a helpful conversion utility to convert from a DataFusion Result to an FFIResult
/// and to also call return when it is an error. Since you cannot use `?` on an FFIResult, this is designed
/// to mimic the pattern.
#[macro_export]
macro_rules! rresult_return {
    ( $x:expr ) => {
        match $x {
            Ok(v) => v,
            Err(e) => {
                return $crate::ffi_option::FfiResult::Err(
                    stabby::alloc::string::String::from(e.to_string().as_str()),
                )
            }
        }
    };
}

/// This is a utility function to convert a slice of [`Field`] to its equivalent
/// FFI friendly counterpart, [`WrappedSchema`]
pub fn vec_fieldref_to_rvec_wrapped(
    fields: &[FieldRef],
) -> Result<SVec<WrappedSchema>, arrow::error::ArrowError> {
    Ok(fields
        .iter()
        .map(FFI_ArrowSchema::try_from)
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?
        .into_iter()
        .map(WrappedSchema)
        .collect())
}

/// This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
/// to their equivalent [`Field`].
pub fn rvec_wrapped_to_vec_fieldref(
    fields: &SVec<WrappedSchema>,
) -> Result<Vec<FieldRef>, arrow::error::ArrowError> {
    fields
        .iter()
        .map(|d| Field::try_from(&d.0).map(Arc::new))
        .collect()
}

/// This is a utility function to convert a slice of [`DataType`] to its equivalent
/// FFI friendly counterpart, [`WrappedSchema`]
pub fn vec_datatype_to_rvec_wrapped(
    data_types: &[DataType],
) -> Result<SVec<WrappedSchema>, arrow::error::ArrowError> {
    Ok(data_types
        .iter()
        .map(FFI_ArrowSchema::try_from)
        .collect::<Result<Vec<_>, arrow::error::ArrowError>>()?
        .into_iter()
        .map(WrappedSchema)
        .collect())
}

/// This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
/// to their equivalent [`DataType`].
pub fn rvec_wrapped_to_vec_datatype(
    data_types: &SVec<WrappedSchema>,
) -> Result<Vec<DataType>, arrow::error::ArrowError> {
    data_types
        .iter()
        .map(|d| DataType::try_from(&d.0))
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use datafusion::error::DataFusionError;
    use datafusion::prelude::SessionContext;
    use datafusion_execution::TaskContextProvider;
    use stabby::alloc::string::String as SString;

    use crate::execution::FFI_TaskContextProvider;
    use crate::ffi_option::FfiResult;
    use crate::util::FFIResult;

    pub(crate) fn test_session_and_ctx() -> (Arc<SessionContext>, FFI_TaskContextProvider)
    {
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        (ctx, task_ctx_provider)
    }

    fn wrap_result(result: Result<String, DataFusionError>) -> FFIResult<String> {
        FfiResult::Ok(rresult_return!(result))
    }

    #[test]
    fn test_conversion() {
        const VALID_VALUE: &str = "valid_value";
        const ERROR_VALUE: &str = "error_value";

        let ok_r_result: FFIResult<SString> = FfiResult::Ok(SString::from(VALID_VALUE));
        let err_r_result: FFIResult<SString> = FfiResult::Err(SString::from(ERROR_VALUE));

        let returned_ok_result = df_result!(ok_r_result);
        assert!(returned_ok_result.is_ok());
        assert!(*returned_ok_result.unwrap() == *VALID_VALUE);

        let returned_err_result = df_result!(err_r_result);
        assert!(returned_err_result.is_err());
        assert!(
            returned_err_result.unwrap_err().strip_backtrace()
                == format!("FFI error: {ERROR_VALUE}")
        );

        let ok_result: Result<String, DataFusionError> = Ok(VALID_VALUE.to_string());
        let err_result: Result<String, DataFusionError> =
            datafusion_common::ffi_err!("{ERROR_VALUE}");

        let returned_ok_r_result = wrap_result(ok_result);
        let std_result: Result<String, SString> = returned_ok_r_result.into();
        assert!(std_result == Ok(VALID_VALUE.into()));

        let returned_err_r_result = wrap_result(err_result);
        let std_result: Result<String, SString> = returned_err_r_result.into();
        assert!(std_result.is_err());
        assert!(
            std_result
                .unwrap_err()
                .as_str()
                .starts_with(format!("FFI error: {ERROR_VALUE}").as_str())
        );
    }
}
