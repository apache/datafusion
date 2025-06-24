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

use crate::arrow_wrappers::WrappedSchema;
use abi_stable::std_types::RVec;
use arrow::datatypes::Field;
use arrow::{datatypes::DataType, ffi::FFI_ArrowSchema};
use arrow_schema::FieldRef;
use std::sync::Arc;

/// This macro is a helpful conversion utility to convert from an abi_stable::RResult to a
/// DataFusion result.
#[macro_export]
macro_rules! df_result {
    ( $x:expr ) => {
        match $x {
            abi_stable::std_types::RResult::ROk(v) => Ok(v),
            abi_stable::std_types::RResult::RErr(e) => {
                Err(datafusion::error::DataFusionError::Execution(e.to_string()))
            }
        }
    };
}

/// This macro is a helpful conversion utility to conver from a DataFusion Result to an abi_stable::RResult
#[macro_export]
macro_rules! rresult {
    ( $x:expr ) => {
        match $x {
            Ok(v) => abi_stable::std_types::RResult::ROk(v),
            Err(e) => abi_stable::std_types::RResult::RErr(
                abi_stable::std_types::RString::from(e.to_string()),
            ),
        }
    };
}

/// This macro is a helpful conversion utility to conver from a DataFusion Result to an abi_stable::RResult
/// and to also call return when it is an error. Since you cannot use `?` on an RResult, this is designed
/// to mimic the pattern.
#[macro_export]
macro_rules! rresult_return {
    ( $x:expr ) => {
        match $x {
            Ok(v) => v,
            Err(e) => {
                return abi_stable::std_types::RResult::RErr(
                    abi_stable::std_types::RString::from(e.to_string()),
                )
            }
        }
    };
}

/// This is a utility function to convert a slice of [`Field`] to its equivalent
/// FFI friendly counterpart, [`WrappedSchema`]
pub fn vec_fieldref_to_rvec_wrapped(
    fields: &[FieldRef],
) -> Result<RVec<WrappedSchema>, arrow::error::ArrowError> {
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
    fields: &RVec<WrappedSchema>,
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
) -> Result<RVec<WrappedSchema>, arrow::error::ArrowError> {
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
    data_types: &RVec<WrappedSchema>,
) -> Result<Vec<DataType>, arrow::error::ArrowError> {
    data_types
        .iter()
        .map(|d| DataType::try_from(&d.0))
        .collect()
}

#[cfg(test)]
mod tests {
    use abi_stable::std_types::{RResult, RString};
    use datafusion::error::DataFusionError;

    fn wrap_result(result: Result<String, DataFusionError>) -> RResult<String, RString> {
        RResult::ROk(rresult_return!(result))
    }

    #[test]
    fn test_conversion() {
        const VALID_VALUE: &str = "valid_value";
        const ERROR_VALUE: &str = "error_value";

        let ok_r_result: RResult<RString, RString> =
            RResult::ROk(VALID_VALUE.to_string().into());
        let err_r_result: RResult<RString, RString> =
            RResult::RErr(ERROR_VALUE.to_string().into());

        let returned_ok_result = df_result!(ok_r_result);
        assert!(returned_ok_result.is_ok());
        assert!(returned_ok_result.unwrap().to_string() == VALID_VALUE);

        let returned_err_result = df_result!(err_r_result);
        assert!(returned_err_result.is_err());
        assert!(
            returned_err_result.unwrap_err().to_string()
                == format!("Execution error: {ERROR_VALUE}")
        );

        let ok_result: Result<String, DataFusionError> = Ok(VALID_VALUE.to_string());
        let err_result: Result<String, DataFusionError> =
            Err(DataFusionError::Execution(ERROR_VALUE.to_string()));

        let returned_ok_r_result = wrap_result(ok_result);
        assert!(returned_ok_r_result == RResult::ROk(VALID_VALUE.into()));

        let returned_err_r_result = wrap_result(err_result);
        assert!(
            returned_err_r_result
                == RResult::RErr(format!("Execution error: {ERROR_VALUE}").into())
        );
    }
}
