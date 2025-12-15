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

use abi_stable::StableAbi;
use arrow::{
    array::{make_array, ArrayRef},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
use datafusion_common::{DataFusionError, ScalarValue};
use log::error;

/// This is a wrapper struct around FFI_ArrowSchema simply to indicate
/// to the StableAbi macros that the underlying struct is FFI safe.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct WrappedSchema(#[sabi(unsafe_opaque_field)] pub FFI_ArrowSchema);

impl From<SchemaRef> for WrappedSchema {
    fn from(value: SchemaRef) -> Self {
        let ffi_schema = match FFI_ArrowSchema::try_from(value.as_ref()) {
            Ok(s) => s,
            Err(e) => {
                error!("Unable to convert DataFusion Schema to FFI_ArrowSchema in FFI_PlanProperties. {e}");
                FFI_ArrowSchema::empty()
            }
        };

        WrappedSchema(ffi_schema)
    }
}
/// Some functions are expected to always succeed, like getting the schema from a TableProvider.
/// Since going through the FFI always has the potential to fail, we need to catch these errors,
/// give the user a warning, and return some kind of result. In this case we default to an
/// empty schema.
#[cfg(not(tarpaulin_include))]
fn catch_df_schema_error(e: &ArrowError) -> Schema {
    error!("Unable to convert from FFI_ArrowSchema to DataFusion Schema in FFI_PlanProperties. {e}");
    Schema::empty()
}

impl From<WrappedSchema> for SchemaRef {
    fn from(value: WrappedSchema) -> Self {
        let schema =
            Schema::try_from(&value.0).unwrap_or_else(|e| catch_df_schema_error(&e));
        Arc::new(schema)
    }
}

/// This is a wrapper struct for FFI_ArrowArray to indicate to StableAbi
/// that the struct is FFI Safe. For convenience, we also include the
/// schema needed to create a record batch from the array.
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct WrappedArray {
    #[sabi(unsafe_opaque_field)]
    pub array: FFI_ArrowArray,

    pub schema: WrappedSchema,
}

impl TryFrom<WrappedArray> for ArrayRef {
    type Error = ArrowError;

    fn try_from(value: WrappedArray) -> Result<Self, Self::Error> {
        let data = unsafe { from_ffi(value.array, &value.schema.0)? };

        Ok(make_array(data))
    }
}

impl TryFrom<&ArrayRef> for WrappedArray {
    type Error = ArrowError;

    fn try_from(array: &ArrayRef) -> Result<Self, Self::Error> {
        let (array, schema) = to_ffi(&array.to_data())?;
        let schema = WrappedSchema(schema);

        Ok(WrappedArray { array, schema })
    }
}

impl TryFrom<&ScalarValue> for WrappedArray {
    type Error = DataFusionError;

    fn try_from(value: &ScalarValue) -> Result<Self, Self::Error> {
        let array = value.to_array()?;
        WrappedArray::try_from(&array).map_err(Into::into)
    }
}

impl TryFrom<WrappedArray> for ScalarValue {
    type Error = DataFusionError;

    fn try_from(value: WrappedArray) -> Result<Self, Self::Error> {
        let array: ArrayRef = value.try_into()?;
        ScalarValue::try_from_array(array.as_ref(), 0)
    }
}
