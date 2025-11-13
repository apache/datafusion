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
use std::sync::Arc;

use crate::arrow_wrappers::WrappedSchema;
use crate::execution::FFI_TaskContextProvider;
use crate::table_provider::FFI_TableProvider;
use crate::udf::FFI_ScalarUDF;
use crate::{df_result, rresult_return};
use abi_stable::std_types::{RResult, RSlice, RStr, RString, RVec};
use abi_stable::StableAbi;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_catalog::TableProvider;
use datafusion_common::error::Result;
use datafusion_common::{not_impl_err, TableReference};
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, Extension, LogicalPlan, ScalarUDF, ScalarUDFImpl, WindowUDF,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use tokio::runtime::Handle;

/// A stable struct for sharing [`LogicalExtensionCodec`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_LogicalExtensionCodec {
    try_decode_table_provider:
        unsafe extern "C" fn(
            &Self,
            buf: RSlice<u8>,
            table_ref: RStr,
            schema: WrappedSchema,
        ) -> RResult<FFI_TableProvider, RString>,

    try_encode_table_provider: unsafe extern "C" fn(
        &Self,
        table_ref: RStr,
        node: FFI_TableProvider,
    ) -> RResult<RVec<u8>, RString>,

    try_decode_udf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> RResult<FFI_ScalarUDF, RString>,

    try_encode_udf:
        unsafe extern "C" fn(&Self, node: FFI_ScalarUDF) -> RResult<RVec<u8>, RString>,

    // fn try_decode_udaf(&self, name: &str, _buf: &[u8]) -> Result<Arc<AggregateUDF>> {
    // todo!()
    // }
    //
    // fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
    // todo!()
    // }
    //
    // fn try_decode_udwf(&self, name: &str, _buf: &[u8]) -> Result<Arc<WindowUDF>> {
    // todo!()
    // }
    //
    // fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> Result<()> {
    // todo!()
    // }
    //
    /// Provider for TaskContext to be used during protobuf serialization
    /// and deserialization.
    pub task_ctx_provider: FFI_TaskContextProvider,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignLogicalExtensionCodec`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> u64,
}

unsafe impl Send for FFI_LogicalExtensionCodec {}
unsafe impl Sync for FFI_LogicalExtensionCodec {}

struct LogicalExtensionCodecPrivateData {
    provider: Arc<dyn LogicalExtensionCodec>,
    runtime: Option<Handle>,
}

impl FFI_LogicalExtensionCodec {
    fn inner(&self) -> &Arc<dyn LogicalExtensionCodec> {
        let private_data = self.private_data as *const LogicalExtensionCodecPrivateData;
        unsafe { &(*private_data).provider }
    }

    fn runtime(&self) -> &Option<Handle> {
        let private_data = self.private_data as *const LogicalExtensionCodecPrivateData;
        unsafe { &(*private_data).runtime }
    }
}

unsafe extern "C" fn try_decode_table_provider_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    buf: RSlice<u8>,
    table_ref: RStr,
    schema: WrappedSchema,
) -> RResult<FFI_TableProvider, RString> {
    let ctx: Arc<TaskContext> = rresult_return!((&codec.task_ctx_provider).try_into());
    let task_ctx_provider = codec.task_ctx_provider.clone();
    let runtime = codec.runtime().clone();
    let codec = codec.inner();
    let table_ref = TableReference::from(table_ref.as_str());
    let schema: SchemaRef = schema.into();

    let table_provider = rresult_return!(codec.try_decode_table_provider(
        buf.as_ref(),
        &table_ref,
        schema,
        ctx.as_ref()
    ));

    RResult::ROk(FFI_TableProvider::new(
        table_provider,
        true,
        runtime,
        task_ctx_provider,
    ))
}

unsafe extern "C" fn try_encode_table_provider_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    table_ref: RStr,
    node: FFI_TableProvider,
) -> RResult<RVec<u8>, RString> {
    let table_ref = TableReference::from(table_ref.as_str());
    let table_provider: Arc<dyn TableProvider> = (&node).into();
    let codec = codec.inner();

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_table_provider(
        &table_ref,
        table_provider,
        &mut bytes
    ));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udf_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> RResult<FFI_ScalarUDF, RString> {
    let codec = codec.inner();

    let udf = rresult_return!(codec.try_decode_udf(name.as_str(), buf.as_ref()));
    let udf = FFI_ScalarUDF::from(udf);

    RResult::ROk(udf)
}

unsafe extern "C" fn try_encode_udf_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    node: FFI_ScalarUDF,
) -> RResult<RVec<u8>, RString> {
    let codec = codec.inner();
    let node: Arc<dyn ScalarUDFImpl> = rresult_return!((&node).try_into());
    let node = ScalarUDF::new_from_shared_impl(node);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udf(&node, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_LogicalExtensionCodec) {
    let private_data =
        Box::from_raw(provider.private_data as *mut LogicalExtensionCodecPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
) -> FFI_LogicalExtensionCodec {
    let old_codec = Arc::clone(codec.inner());
    let runtime = codec.runtime().clone();

    FFI_LogicalExtensionCodec::new(old_codec, runtime, codec.task_ctx_provider.clone())
}

impl Drop for FFI_LogicalExtensionCodec {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_LogicalExtensionCodec {
    /// Creates a new [`FFI_LogicalExtensionCodec`].
    pub fn new(
        provider: Arc<dyn LogicalExtensionCodec + Send>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let private_data =
            Box::new(LogicalExtensionCodecPrivateData { provider, runtime });

        Self {
            try_decode_table_provider: try_decode_table_provider_fn_wrapper,
            try_encode_table_provider: try_encode_table_provider_fn_wrapper,
            try_decode_udf: try_decode_udf_fn_wrapper,
            try_encode_udf: try_encode_udf_fn_wrapper,

            task_ctx_provider,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_LogicalExtensionCodec to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignLogicalExtensionCodec(pub FFI_LogicalExtensionCodec);

unsafe impl Send for ForeignLogicalExtensionCodec {}
unsafe impl Sync for ForeignLogicalExtensionCodec {}

impl From<&FFI_LogicalExtensionCodec> for Arc<dyn LogicalExtensionCodec> {
    fn from(provider: &FFI_LogicalExtensionCodec) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(provider.inner())
        } else {
            Arc::new(ForeignLogicalExtensionCodec(provider.clone()))
        }
    }
}

impl Clone for FFI_LogicalExtensionCodec {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl LogicalExtensionCodec for ForeignLogicalExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("FFI does not support decode of Extensions")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("FFI does not support encode of Extensions")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        table_ref: &TableReference,
        schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        let table_ref = table_ref.to_string();
        let schema: WrappedSchema = schema.into();

        let ffi_table_provider = unsafe {
            df_result!((self.0.try_decode_table_provider)(
                &self.0,
                buf.into(),
                table_ref.as_str().into(),
                schema
            ))
        }?;

        Ok((&ffi_table_provider).into())
    }

    fn try_encode_table_provider(
        &self,
        table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        let table_ref = table_ref.to_string();
        let node =
            FFI_TableProvider::new(node, true, None, self.0.task_ctx_provider.clone());

        let bytes = df_result!(unsafe {
            (self.0.try_encode_table_provider)(&self.0, table_ref.as_str().into(), node)
        })?;

        buf.extend(bytes);

        Ok(())
    }

    fn try_decode_file_format(
        &self,
        _buf: &[u8],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn FileFormatFactory>> {
        not_impl_err!("FFI does not support decode_file_format")
    }

    fn try_encode_file_format(
        &self,
        _buf: &mut Vec<u8>,
        _node: Arc<dyn FileFormatFactory>,
    ) -> Result<()> {
        not_impl_err!("FFI does not support encode_file_format")
    }

    fn try_decode_udf(&self, _name: &str, _buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        todo!()
    }

    fn try_encode_udf(&self, _node: &ScalarUDF, _buf: &mut Vec<u8>) -> Result<()> {
        todo!()
    }

    fn try_decode_udaf(&self, _name: &str, _buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        todo!()
    }

    fn try_encode_udaf(&self, _node: &AggregateUDF, _buf: &mut Vec<u8>) -> Result<()> {
        todo!()
    }

    fn try_decode_udwf(&self, _name: &str, _buf: &[u8]) -> Result<Arc<WindowUDF>> {
        todo!()
    }

    fn try_encode_udwf(&self, _node: &WindowUDF, _buf: &mut Vec<u8>) -> Result<()> {
        todo!()
    }
}
