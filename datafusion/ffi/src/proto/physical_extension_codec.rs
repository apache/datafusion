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

use crate::execution::FFI_TaskContextProvider;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::{df_result, rresult_return};
use abi_stable::std_types::{RResult, RSlice, RStr, RString, RVec};
use abi_stable::StableAbi;
use async_trait::async_trait;
use datafusion_common::error::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use tokio::runtime::Handle;

/// A stable struct for sharing [`PhysicalExtensionCodec`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PhysicalExtensionCodec {
    try_decode: unsafe extern "C" fn(
        &Self,
        buf: RSlice<u8>,
        inputs: RVec<FFI_ExecutionPlan>,
    ) -> RResult<FFI_ExecutionPlan, RString>,

    try_encode: unsafe extern "C" fn(
        &Self,
        node: FFI_ExecutionPlan,
    ) -> RResult<RVec<u8>, RString>,

    try_decode_udf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> RResult<FFI_ScalarUDF, RString>,

    try_encode_udf:
        unsafe extern "C" fn(&Self, node: FFI_ScalarUDF) -> RResult<RVec<u8>, RString>,

    try_decode_udaf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> RResult<FFI_AggregateUDF, RString>,

    try_encode_udaf:
        unsafe extern "C" fn(&Self, node: FFI_AggregateUDF) -> RResult<RVec<u8>, RString>,

    try_decode_udwf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> RResult<FFI_WindowUDF, RString>,

    try_encode_udwf:
        unsafe extern "C" fn(&Self, node: FFI_WindowUDF) -> RResult<RVec<u8>, RString>,

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
    /// A [`ForeignPhysicalExtensionCodec`] should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> u64,
}

unsafe impl Send for FFI_PhysicalExtensionCodec {}
unsafe impl Sync for FFI_PhysicalExtensionCodec {}

struct PhysicalExtensionCodecPrivateData {
    provider: Arc<dyn PhysicalExtensionCodec>,
    runtime: Option<Handle>,
}

impl FFI_PhysicalExtensionCodec {
    fn inner(&self) -> &Arc<dyn PhysicalExtensionCodec> {
        let private_data = self.private_data as *const PhysicalExtensionCodecPrivateData;
        unsafe { &(*private_data).provider }
    }

    fn runtime(&self) -> &Option<Handle> {
        let private_data = self.private_data as *const PhysicalExtensionCodecPrivateData;
        unsafe { &(*private_data).runtime }
    }
}

unsafe extern "C" fn try_decode_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    buf: RSlice<u8>,
    inputs: RVec<FFI_ExecutionPlan>,
) -> RResult<FFI_ExecutionPlan, RString> {
    let task_ctx_provider = codec.task_ctx_provider.clone();
    let task_ctx: Arc<TaskContext> = rresult_return!((&task_ctx_provider).try_into());
    let codec = codec.inner();
    let inputs = inputs
        .into_iter()
        .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
        .collect::<Result<Vec<_>>>();
    let inputs = rresult_return!(inputs);

    let plan =
        rresult_return!(codec.try_decode(buf.as_ref(), &inputs, task_ctx.as_ref()));

    RResult::ROk(FFI_ExecutionPlan::new(plan, task_ctx_provider, None))
}

unsafe extern "C" fn try_encode_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ExecutionPlan,
) -> RResult<RVec<u8>, RString> {
    let codec = codec.inner();
    let plan: Arc<dyn ExecutionPlan> = rresult_return!((&node).try_into());

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode(plan, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> RResult<FFI_ScalarUDF, RString> {
    let codec = codec.inner();

    let udf = rresult_return!(codec.try_decode_udf(name.as_str(), buf.as_ref()));
    let udf = FFI_ScalarUDF::from(udf);

    RResult::ROk(udf)
}

unsafe extern "C" fn try_encode_udf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ScalarUDF,
) -> RResult<RVec<u8>, RString> {
    let codec = codec.inner();
    let node: Arc<dyn ScalarUDFImpl> = rresult_return!((&node).try_into());
    let node = ScalarUDF::new_from_shared_impl(node);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udf(&node, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> RResult<FFI_AggregateUDF, RString> {
    let task_ctx_provider = codec.task_ctx_provider.clone();
    let codec = codec.inner();
    let udaf = rresult_return!(codec.try_decode_udaf(name.into(), buf.as_ref()));
    let udaf = FFI_AggregateUDF::new(udaf, task_ctx_provider);

    RResult::ROk(udaf)
}

unsafe extern "C" fn try_encode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_AggregateUDF,
) -> RResult<RVec<u8>, RString> {
    let codec = codec.inner();
    let udaf: Arc<dyn AggregateUDFImpl> = rresult_return!((&node).try_into());
    let udaf = AggregateUDF::new_from_shared_impl(udaf);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udaf(&udaf, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> RResult<FFI_WindowUDF, RString> {
    let task_ctx_provider = codec.task_ctx_provider.clone();
    let codec = codec.inner();
    let udwf = rresult_return!(codec.try_decode_udwf(name.into(), buf.as_ref()));
    let udwf = FFI_WindowUDF::new(udwf, task_ctx_provider);

    RResult::ROk(udwf)
}

unsafe extern "C" fn try_encode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_WindowUDF,
) -> RResult<RVec<u8>, RString> {
    let codec = codec.inner();
    let udwf: Arc<dyn WindowUDFImpl> = rresult_return!((&node).try_into());
    let udwf = WindowUDF::new_from_shared_impl(udwf);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udwf(&udwf, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_PhysicalExtensionCodec) {
    let private_data =
        Box::from_raw(provider.private_data as *mut PhysicalExtensionCodecPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
) -> FFI_PhysicalExtensionCodec {
    let old_codec = Arc::clone(codec.inner());
    let runtime = codec.runtime().clone();

    FFI_PhysicalExtensionCodec::new(old_codec, runtime, codec.task_ctx_provider.clone())
}

impl Drop for FFI_PhysicalExtensionCodec {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_PhysicalExtensionCodec {
    /// Creates a new [`FFI_PhysicalExtensionCodec`].
    pub fn new(
        provider: Arc<dyn PhysicalExtensionCodec + Send>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let private_data =
            Box::new(PhysicalExtensionCodecPrivateData { provider, runtime });

        Self {
            try_decode: try_decode_fn_wrapper,
            try_encode: try_encode_fn_wrapper,
            try_decode_udf: try_decode_udf_fn_wrapper,
            try_encode_udf: try_encode_udf_fn_wrapper,
            try_decode_udaf: try_decode_udaf_fn_wrapper,
            try_encode_udaf: try_encode_udaf_fn_wrapper,
            try_decode_udwf: try_decode_udwf_fn_wrapper,
            try_encode_udwf: try_encode_udwf_fn_wrapper,

            task_ctx_provider,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: crate::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_PhysicalExtensionCodec to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignPhysicalExtensionCodec(pub FFI_PhysicalExtensionCodec);

unsafe impl Send for ForeignPhysicalExtensionCodec {}
unsafe impl Sync for ForeignPhysicalExtensionCodec {}

impl From<&FFI_PhysicalExtensionCodec> for Arc<dyn PhysicalExtensionCodec> {
    fn from(provider: &FFI_PhysicalExtensionCodec) -> Self {
        if (provider.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(provider.inner())
        } else {
            Arc::new(ForeignPhysicalExtensionCodec(provider.clone()))
        }
    }
}

impl Clone for FFI_PhysicalExtensionCodec {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl PhysicalExtensionCodec for ForeignPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs = inputs
            .iter()
            .map(|plan| {
                FFI_ExecutionPlan::new(
                    Arc::clone(plan),
                    self.0.task_ctx_provider.clone(),
                    None,
                )
            })
            .collect();

        let plan =
            df_result!(unsafe { (self.0.try_decode)(&self.0, buf.into(), inputs) })?;
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;

        Ok(plan)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let plan = FFI_ExecutionPlan::new(node, self.0.task_ctx_provider.clone(), None);
        let bytes = df_result!(unsafe { (self.0.try_encode)(&self.0, plan) })?;

        buf.extend(bytes);
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        let udf = unsafe {
            df_result!((self.0.try_decode_udf)(&self.0, name.into(), buf.into()))
        }?;
        let udf: Arc<dyn ScalarUDFImpl> = (&udf).try_into()?;

        Ok(Arc::new(ScalarUDF::new_from_shared_impl(udf)))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        let node = FFI_ScalarUDF::from(Arc::new(node.clone()));
        let bytes = df_result!(unsafe { (self.0.try_encode_udf)(&self.0, node) })?;

        buf.extend(bytes);

        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        let udaf = unsafe {
            df_result!((self.0.try_decode_udaf)(&self.0, name.into(), buf.into()))
        }?;
        let udaf: Arc<dyn AggregateUDFImpl> = (&udaf).try_into()?;

        Ok(Arc::new(AggregateUDF::new_from_shared_impl(udaf)))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let node = Arc::new(node.clone());
        let node = FFI_AggregateUDF::new(node, self.0.task_ctx_provider.clone());
        let bytes = df_result!(unsafe { (self.0.try_encode_udaf)(&self.0, node) })?;

        buf.extend(bytes);

        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        let udwf = unsafe {
            df_result!((self.0.try_decode_udwf)(&self.0, name.into(), buf.into()))
        }?;
        let udwf: Arc<dyn WindowUDFImpl> = (&udwf).try_into()?;

        Ok(Arc::new(WindowUDF::new_from_shared_impl(udwf)))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        let node = Arc::new(node.clone());
        let node = FFI_WindowUDF::new(node, self.0.task_ctx_provider.clone());
        let bytes = df_result!(unsafe { (self.0.try_encode_udwf)(&self.0, node) })?;

        buf.extend(bytes);

        Ok(())
    }
}
