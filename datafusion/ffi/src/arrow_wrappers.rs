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
    ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
};
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
                error!("Unable to convert DataFusion Schema to FFI_ArrowSchema in FFI_PlanProperties. {}", e);
                FFI_ArrowSchema::empty()
            }
        };

        WrappedSchema(ffi_schema)
    }
}

impl From<WrappedSchema> for SchemaRef {
    fn from(value: WrappedSchema) -> Self {
        let schema = match Schema::try_from(&value.0) {
            Ok(s) => s,
            Err(e) => {
                error!("Unable to convert from FFI_ArrowSchema to DataFusion Schema in FFI_PlanProperties. {}", e);
                Schema::empty()
            }
        };
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
    type Error = arrow::error::ArrowError;

    fn try_from(value: WrappedArray) -> Result<Self, Self::Error> {
        let data = unsafe { from_ffi(value.array, &value.schema.0)? };

        Ok(make_array(data))
    }
}
