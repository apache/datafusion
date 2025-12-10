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

use abi_stable::std_types::{RResult, RSlice, RStr, RVec};
use abi_stable::StableAbi;
use datafusion_common::error::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFIResult;
use crate::{df_result, rresult_return};

/// A stable struct for sharing [`PhysicalExtensionCodec`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_PhysicalExtensionCodec {
    /// Decode bytes into an execution plan.
    try_decode: unsafe extern "C" fn(
        &Self,
        buf: RSlice<u8>,
        inputs: RVec<FFI_ExecutionPlan>,
    ) -> FFIResult<FFI_ExecutionPlan>,

    /// Encode an execution plan into bytes.
    try_encode:
        unsafe extern "C" fn(&Self, node: FFI_ExecutionPlan) -> FFIResult<RVec<u8>>,

    /// Decode bytes into a user defined scalar function.
    try_decode_udf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> FFIResult<FFI_ScalarUDF>,

    /// Encode a user defined scalar function into bytes.
    try_encode_udf:
        unsafe extern "C" fn(&Self, node: FFI_ScalarUDF) -> FFIResult<RVec<u8>>,

    /// Decode bytes into a user defined aggregate function.
    try_decode_udaf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> FFIResult<FFI_AggregateUDF>,

    /// Encode a user defined aggregate function into bytes.
    try_encode_udaf:
        unsafe extern "C" fn(&Self, node: FFI_AggregateUDF) -> FFIResult<RVec<u8>>,

    /// Decode bytes into a user defined window function.
    try_decode_udwf: unsafe extern "C" fn(
        &Self,
        name: RStr,
        buf: RSlice<u8>,
    ) -> FFIResult<FFI_WindowUDF>,

    /// Encode a user defined window function into bytes.
    try_encode_udwf:
        unsafe extern "C" fn(&Self, node: FFI_WindowUDF) -> FFIResult<RVec<u8>>,

    /// Access the current [`TaskContext`].
    task_ctx_provider: FFI_TaskContextProvider,

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
    pub library_marker_id: extern "C" fn() -> usize,
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
) -> FFIResult<FFI_ExecutionPlan> {
    let task_ctx: Arc<TaskContext> =
        rresult_return!((&codec.task_ctx_provider).try_into());
    let codec = codec.inner();
    let inputs = inputs
        .into_iter()
        .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
        .collect::<Result<Vec<_>>>();
    let inputs = rresult_return!(inputs);

    let plan =
        rresult_return!(codec.try_decode(buf.as_ref(), &inputs, task_ctx.as_ref()));

    RResult::ROk(FFI_ExecutionPlan::new(plan, task_ctx, None))
}

unsafe extern "C" fn try_encode_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ExecutionPlan,
) -> FFIResult<RVec<u8>> {
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
) -> FFIResult<FFI_ScalarUDF> {
    let codec = codec.inner();

    let udf = rresult_return!(codec.try_decode_udf(name.as_str(), buf.as_ref()));
    let udf = FFI_ScalarUDF::from(udf);

    RResult::ROk(udf)
}

unsafe extern "C" fn try_encode_udf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ScalarUDF,
) -> FFIResult<RVec<u8>> {
    let codec = codec.inner();
    let node: Arc<dyn ScalarUDFImpl> = (&node).into();
    let node = ScalarUDF::new_from_shared_impl(node);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udf(&node, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> FFIResult<FFI_AggregateUDF> {
    let codec_inner = codec.inner();
    let udaf = rresult_return!(codec_inner.try_decode_udaf(name.into(), buf.as_ref()));
    let udaf = FFI_AggregateUDF::from(udaf);

    RResult::ROk(udaf)
}

unsafe extern "C" fn try_encode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_AggregateUDF,
) -> FFIResult<RVec<u8>> {
    let codec = codec.inner();
    let udaf: Arc<dyn AggregateUDFImpl> = (&node).into();
    let udaf = AggregateUDF::new_from_shared_impl(udaf);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udaf(&udaf, &mut bytes));

    RResult::ROk(bytes.into())
}

unsafe extern "C" fn try_decode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> FFIResult<FFI_WindowUDF> {
    let codec = codec.inner();
    let udwf = rresult_return!(codec.try_decode_udwf(name.into(), buf.as_ref()));
    let udwf = FFI_WindowUDF::from(udwf);

    RResult::ROk(udwf)
}

unsafe extern "C" fn try_encode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_WindowUDF,
) -> FFIResult<RVec<u8>> {
    let codec = codec.inner();
    let udwf: Arc<dyn WindowUDFImpl> = (&node).into();
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

impl PhysicalExtensionCodec for ForeignPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let task_ctx = (&self.0.task_ctx_provider).try_into()?;
        let inputs = inputs
            .iter()
            .map(|plan| {
                FFI_ExecutionPlan::new(Arc::clone(plan), Arc::clone(&task_ctx), None)
            })
            .collect();

        let plan =
            df_result!(unsafe { (self.0.try_decode)(&self.0, buf.into(), inputs) })?;
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;

        Ok(plan)
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let task_ctx = (&self.0.task_ctx_provider).try_into()?;
        let plan = FFI_ExecutionPlan::new(node, task_ctx, None);
        let bytes = df_result!(unsafe { (self.0.try_encode)(&self.0, plan) })?;

        buf.extend(bytes);
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        let udf = unsafe {
            df_result!((self.0.try_decode_udf)(&self.0, name.into(), buf.into()))
        }?;
        let udf: Arc<dyn ScalarUDFImpl> = (&udf).into();

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
        let udaf: Arc<dyn AggregateUDFImpl> = (&udaf).into();

        Ok(Arc::new(AggregateUDF::new_from_shared_impl(udaf)))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        let node = Arc::new(node.clone());
        let node = FFI_AggregateUDF::from(node);
        let bytes = df_result!(unsafe { (self.0.try_encode_udaf)(&self.0, node) })?;

        buf.extend(bytes);

        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        let udwf = unsafe {
            df_result!((self.0.try_decode_udwf)(&self.0, name.into(), buf.into()))
        }?;
        let udwf: Arc<dyn WindowUDFImpl> = (&udwf).into();

        Ok(Arc::new(WindowUDF::new_from_shared_impl(udwf)))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        let node = Arc::new(node.clone());
        let node = FFI_WindowUDF::from(node);
        let bytes = df_result!(unsafe { (self.0.try_encode_udwf)(&self.0, node) })?;

        buf.extend(bytes);

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::{exec_err, Result};
    use datafusion_execution::TaskContext;
    use datafusion_expr::ptr_eq::arc_ptr_eq;
    use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF, WindowUDFImpl};
    use datafusion_functions::math::abs::AbsFunc;
    use datafusion_functions_aggregate::sum::Sum;
    use datafusion_functions_window::rank::{Rank, RankType};
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    use crate::execution_plan::tests::EmptyExec;
    use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;

    #[derive(Debug)]
    pub(crate) struct TestExtensionCodec;

    impl TestExtensionCodec {
        pub(crate) const MAGIC_NUMBER: u8 = 127;
        pub(crate) const EMPTY_EXEC_SERIALIZED: u8 = 1;
        pub(crate) const ABS_FUNC_SERIALIZED: u8 = 2;
        pub(crate) const SUM_UDAF_SERIALIZED: u8 = 3;
        pub(crate) const RANK_UDWF_SERIALIZED: u8 = 4;
        pub(crate) const MEMTABLE_SERIALIZED: u8 = 5;
    }

    impl PhysicalExtensionCodec for TestExtensionCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if buf[0] != Self::MAGIC_NUMBER {
                return exec_err!(
                    "TestExtensionCodec input buffer does not start with magic number"
                );
            }

            if buf.len() != 2 || buf[1] != Self::EMPTY_EXEC_SERIALIZED {
                return exec_err!("TestExtensionCodec unable to decode execution plan");
            }

            Ok(create_test_exec())
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            let Some(_) = node.as_any().downcast_ref::<EmptyExec>() else {
                return exec_err!("TestExtensionCodec only expects EmptyExec");
            };

            buf.push(Self::EMPTY_EXEC_SERIALIZED);

            Ok(())
        }

        fn try_decode_udf(&self, _name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
            if buf[0] != Self::MAGIC_NUMBER {
                return exec_err!(
                    "TestExtensionCodec input buffer does not start with magic number"
                );
            }

            if buf.len() != 2 || buf[1] != Self::ABS_FUNC_SERIALIZED {
                return exec_err!("TestExtensionCodec unable to decode udf");
            }

            Ok(Arc::new(ScalarUDF::from(AbsFunc::new())))
        }

        fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            let udf = node.inner();
            if !udf.as_any().is::<AbsFunc>() {
                return exec_err!("TestExtensionCodec only expects Abs UDF");
            };

            buf.push(Self::ABS_FUNC_SERIALIZED);

            Ok(())
        }

        fn try_decode_udaf(&self, _name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
            if buf[0] != Self::MAGIC_NUMBER {
                return exec_err!(
                    "TestExtensionCodec input buffer does not start with magic number"
                );
            }

            if buf.len() != 2 || buf[1] != Self::SUM_UDAF_SERIALIZED {
                return exec_err!("TestExtensionCodec unable to decode udaf");
            }

            Ok(Arc::new(AggregateUDF::from(Sum::new())))
        }

        fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            let udf = node.inner();
            let Some(_udf) = udf.as_any().downcast_ref::<Sum>() else {
                return exec_err!("TestExtensionCodec only expects Sum UDAF");
            };

            buf.push(Self::SUM_UDAF_SERIALIZED);

            Ok(())
        }

        fn try_decode_udwf(&self, _name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
            if buf[0] != Self::MAGIC_NUMBER {
                return exec_err!(
                    "TestExtensionCodec input buffer does not start with magic number"
                );
            }

            if buf.len() != 2 || buf[1] != Self::RANK_UDWF_SERIALIZED {
                return exec_err!("TestExtensionCodec unable to decode udwf");
            }

            Ok(Arc::new(WindowUDF::from(Rank::new(
                "my_rank".to_owned(),
                RankType::Basic,
            ))))
        }

        fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            let udf = node.inner();
            let Some(udf) = udf.as_any().downcast_ref::<Rank>() else {
                return exec_err!("TestExtensionCodec only expects Rank UDWF");
            };

            if udf.name() != "my_rank" {
                return exec_err!("TestExtensionCodec only expects my_rank UDWF name");
            }

            buf.push(Self::RANK_UDWF_SERIALIZED);

            Ok(())
        }
    }

    fn create_test_exec() -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));
        Arc::new(EmptyExec::new(schema)) as Arc<dyn ExecutionPlan>
    }

    #[test]
    fn roundtrip_ffi_physical_extension_codec_exec_plan() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let exec = create_test_exec();
        let input_execs = [create_test_exec()];
        let mut bytes = Vec::new();
        foreign_codec.try_encode(Arc::clone(&exec), &mut bytes)?;

        let returned_exec =
            foreign_codec.try_decode(&bytes, &input_execs, ctx.task_ctx().as_ref())?;

        assert!(returned_exec.as_any().is::<EmptyExec>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_physical_extension_codec_udf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let udf = Arc::new(ScalarUDF::from(AbsFunc::new()));
        let mut bytes = Vec::new();
        foreign_codec.try_encode_udf(udf.as_ref(), &mut bytes)?;

        let returned_udf = foreign_codec.try_decode_udf(udf.name(), &bytes)?;

        assert!(returned_udf.inner().as_any().is::<AbsFunc>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_physical_extension_codec_udaf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let udf = Arc::new(AggregateUDF::from(Sum::new()));
        let mut bytes = Vec::new();
        foreign_codec.try_encode_udaf(udf.as_ref(), &mut bytes)?;

        let returned_udf = foreign_codec.try_decode_udaf(udf.name(), &bytes)?;

        assert!(returned_udf.inner().as_any().is::<Sum>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_physical_extension_codec_udwf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let udf = Arc::new(WindowUDF::from(Rank::new(
            "my_rank".to_owned(),
            RankType::Basic,
        )));
        let mut bytes = Vec::new();
        foreign_codec.try_encode_udwf(udf.as_ref(), &mut bytes)?;

        let returned_udf = foreign_codec.try_decode_udwf(udf.name(), &bytes)?;

        assert!(returned_udf.inner().as_any().is::<Rank>());

        Ok(())
    }

    #[test]
    fn ffi_physical_extension_codec_local_bypass() {
        let codec =
            Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec + Send>;
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(Arc::clone(&codec), None, task_ctx_provider);

        let codec = codec as Arc<dyn PhysicalExtensionCodec>;
        // Verify local libraries can be downcast to their original
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();
        assert!(arc_ptr_eq(&foreign_codec, &codec));

        // Verify different library markers generate foreign providers
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();
        assert!(!arc_ptr_eq(&foreign_codec, &codec));
    }
}
