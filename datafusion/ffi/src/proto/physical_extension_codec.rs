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

use std::any::Any;
use std::ffi::c_void;
use std::sync::Arc;

use datafusion_common::error::Result;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::{
    DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    PhysicalProtoConverterExtension,
};

use stabby::slice::Slice as SSlice;
use stabby::str::Str as SStr;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::proto::scalar_subquery_results::FFI_ScalarSubqueryResults;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::{FFI_Option, FFI_Result};
use crate::{df_result, sresult_return};

/// A stable struct for sharing [`PhysicalExtensionCodec`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_PhysicalExtensionCodec {
    /// Decode bytes into an execution plan.
    try_decode: unsafe extern "C" fn(
        &Self,
        buf: SSlice<u8>,
        inputs: SVec<FFI_ExecutionPlan>,
    ) -> FFI_Result<FFI_ExecutionPlan>,

    /// Decode bytes into an execution plan, forwarding the active scalar
    /// subquery results scope (if any) from the caller's decode context so a
    /// `ScalarSubqueryExpr` decoded on this side of the boundary shares the
    /// same populated results as the `ScalarSubqueryExec` that owns them.
    try_decode_with_ctx: unsafe extern "C" fn(
        &Self,
        buf: SSlice<u8>,
        inputs: SVec<FFI_ExecutionPlan>,
        scalar_subquery_results: FFI_Option<FFI_ScalarSubqueryResults>,
    ) -> FFI_Result<FFI_ExecutionPlan>,

    /// Encode an execution plan into bytes.
    try_encode:
        unsafe extern "C" fn(&Self, node: FFI_ExecutionPlan) -> FFI_Result<SVec<u8>>,

    /// Decode bytes into a user defined scalar function.
    try_decode_udf: unsafe extern "C" fn(
        &Self,
        name: SStr,
        buf: SSlice<u8>,
    ) -> FFI_Result<FFI_ScalarUDF>,

    /// Encode a user defined scalar function into bytes.
    try_encode_udf:
        unsafe extern "C" fn(&Self, node: FFI_ScalarUDF) -> FFI_Result<SVec<u8>>,

    /// Decode bytes into a user defined aggregate function.
    try_decode_udaf: unsafe extern "C" fn(
        &Self,
        name: SStr,
        buf: SSlice<u8>,
    ) -> FFI_Result<FFI_AggregateUDF>,

    /// Encode a user defined aggregate function into bytes.
    try_encode_udaf:
        unsafe extern "C" fn(&Self, node: FFI_AggregateUDF) -> FFI_Result<SVec<u8>>,

    /// Decode bytes into a user defined window function.
    try_decode_udwf: unsafe extern "C" fn(
        &Self,
        name: SStr,
        buf: SSlice<u8>,
    ) -> FFI_Result<FFI_WindowUDF>,

    /// Encode a user defined window function into bytes.
    try_encode_udwf:
        unsafe extern "C" fn(&Self, node: FFI_WindowUDF) -> FFI_Result<SVec<u8>>,

    /// Access the current [`TaskContext`].
    pub(crate) task_ctx_provider: FFI_TaskContextProvider,

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
    codec: Arc<dyn PhysicalExtensionCodec>,
    runtime: Option<Handle>,
}

impl FFI_PhysicalExtensionCodec {
    fn inner(&self) -> &Arc<dyn PhysicalExtensionCodec> {
        let private_data = self.private_data as *const PhysicalExtensionCodecPrivateData;
        unsafe { &(*private_data).codec }
    }

    fn runtime(&self) -> Option<&Handle> {
        let private_data = self.private_data as *const PhysicalExtensionCodecPrivateData;
        unsafe { (*private_data).runtime.as_ref() }
    }
}

unsafe extern "C" fn try_decode_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    buf: SSlice<u8>,
    inputs: SVec<FFI_ExecutionPlan>,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = codec.runtime().cloned();
    let task_ctx: Arc<TaskContext> =
        sresult_return!((&codec.task_ctx_provider).try_into());
    let codec = codec.inner();
    let inputs = inputs
        .into_iter()
        .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
        .collect::<Result<Vec<_>>>();
    let inputs = sresult_return!(inputs);

    // The caller's decode context cannot cross the FFI boundary, so decode
    // with a root context for this side's codec.
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), codec.as_ref());
    let plan = sresult_return!(codec.try_decode_with_ctx(
        buf.as_ref(),
        &inputs,
        &decode_ctx,
        &DefaultPhysicalProtoConverter {},
    ));

    FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime))
}

unsafe extern "C" fn try_decode_with_ctx_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    buf: SSlice<u8>,
    inputs: SVec<FFI_ExecutionPlan>,
    scalar_subquery_results: FFI_Option<FFI_ScalarSubqueryResults>,
) -> FFI_Result<FFI_ExecutionPlan> {
    let runtime = codec.runtime().cloned();
    let task_ctx: Arc<TaskContext> =
        sresult_return!((&codec.task_ctx_provider).try_into());
    let codec = codec.inner();
    let inputs = inputs
        .into_iter()
        .map(|plan| <Arc<dyn ExecutionPlan>>::try_from(&plan))
        .collect::<Result<Vec<_>>>();
    let inputs = sresult_return!(inputs);

    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), codec.as_ref());
    let decode_ctx = match scalar_subquery_results.into_option() {
        Some(results) => decode_ctx.with_scalar_subquery_results(results.into()),
        None => decode_ctx,
    };

    let plan = sresult_return!(codec.try_decode_with_ctx(
        buf.as_ref(),
        &inputs,
        &decode_ctx,
        &DefaultPhysicalProtoConverter {},
    ));

    FFI_Result::Ok(FFI_ExecutionPlan::new(plan, runtime))
}

unsafe extern "C" fn try_encode_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ExecutionPlan,
) -> FFI_Result<SVec<u8>> {
    let codec = codec.inner();

    let plan: Arc<dyn ExecutionPlan> = sresult_return!((&node).try_into());

    let mut bytes = Vec::new();
    sresult_return!(codec.try_encode(
        plan,
        &mut bytes,
        &DefaultPhysicalProtoConverter {}
    ));

    FFI_Result::Ok(bytes.into_iter().collect())
}

unsafe extern "C" fn try_decode_udf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: SStr,
    buf: SSlice<u8>,
) -> FFI_Result<FFI_ScalarUDF> {
    let codec = codec.inner();

    let udf = sresult_return!(codec.try_decode_udf(name.as_str(), buf.as_ref()));
    let udf = FFI_ScalarUDF::from(udf);

    FFI_Result::Ok(udf)
}

unsafe extern "C" fn try_encode_udf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_ScalarUDF,
) -> FFI_Result<SVec<u8>> {
    let codec = codec.inner();
    let node: Arc<dyn ScalarUDFImpl> = (&node).into();
    let node = ScalarUDF::new_from_shared_impl(node);

    let mut bytes = Vec::new();
    sresult_return!(codec.try_encode_udf(&node, &mut bytes));

    FFI_Result::Ok(bytes.into_iter().collect())
}

unsafe extern "C" fn try_decode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: SStr,
    buf: SSlice<u8>,
) -> FFI_Result<FFI_AggregateUDF> {
    let codec_inner = codec.inner();
    let udaf = sresult_return!(codec_inner.try_decode_udaf(name.into(), buf.as_ref()));
    let udaf = FFI_AggregateUDF::from(udaf);

    FFI_Result::Ok(udaf)
}

unsafe extern "C" fn try_encode_udaf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_AggregateUDF,
) -> FFI_Result<SVec<u8>> {
    let codec = codec.inner();
    let udaf: Arc<dyn AggregateUDFImpl> = (&node).into();
    let udaf = AggregateUDF::new_from_shared_impl(udaf);

    let mut bytes = Vec::new();
    sresult_return!(codec.try_encode_udaf(&udaf, &mut bytes));

    FFI_Result::Ok(bytes.into_iter().collect())
}

unsafe extern "C" fn try_decode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    name: SStr,
    buf: SSlice<u8>,
) -> FFI_Result<FFI_WindowUDF> {
    let codec = codec.inner();
    let udwf = sresult_return!(codec.try_decode_udwf(name.into(), buf.as_ref()));
    let udwf = FFI_WindowUDF::from(udwf);

    FFI_Result::Ok(udwf)
}

unsafe extern "C" fn try_encode_udwf_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
    node: FFI_WindowUDF,
) -> FFI_Result<SVec<u8>> {
    let codec = codec.inner();
    let udwf: Arc<dyn WindowUDFImpl> = (&node).into();
    let udwf = WindowUDF::new_from_shared_impl(udwf);

    let mut bytes = Vec::new();
    sresult_return!(codec.try_encode_udwf(&udwf, &mut bytes));

    FFI_Result::Ok(bytes.into_iter().collect())
}

unsafe extern "C" fn release_fn_wrapper(codec: &mut FFI_PhysicalExtensionCodec) {
    unsafe {
        let private_data = Box::from_raw(
            codec
                .private_data
                .cast::<PhysicalExtensionCodecPrivateData>(),
        );
        drop(private_data);
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    codec: &FFI_PhysicalExtensionCodec,
) -> FFI_PhysicalExtensionCodec {
    let old_codec = Arc::clone(codec.inner());
    let runtime = codec.runtime().cloned();

    FFI_PhysicalExtensionCodec::new(old_codec, runtime, codec.task_ctx_provider.clone())
}

impl Drop for FFI_PhysicalExtensionCodec {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_PhysicalExtensionCodec {
    /// Creates a new [`FFI_PhysicalExtensionCodec`].
    ///
    /// If `codec` is already foreign, this re-exports its original FFI handle
    /// rather than adding another wrapper layer. The handle still adopts the
    /// `task_ctx_provider` supplied here, so it is never silently discarded and
    /// an imported codec can be rebound to a different session.
    ///
    /// `runtime` is only honored when a new wrapper is created. An
    /// already-foreign handle keeps the runtime of the library that owns it,
    /// because that value lives in private data this side cannot reach.
    pub fn new(
        codec: Arc<dyn PhysicalExtensionCodec>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
    ) -> Self {
        if let Some(codec) = (Arc::clone(&codec) as Arc<dyn Any>)
            .downcast_ref::<ForeignPhysicalExtensionCodec>()
        {
            let mut codec = codec.0.clone();
            codec.task_ctx_provider = task_ctx_provider.into();
            return codec;
        }

        let task_ctx_provider = task_ctx_provider.into();
        let private_data = Box::new(PhysicalExtensionCodecPrivateData { codec, runtime });

        Self {
            try_decode: try_decode_fn_wrapper,
            try_decode_with_ctx: try_decode_with_ctx_fn_wrapper,
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
            private_data: Box::into_raw(private_data).cast::<c_void>(),
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
    fn from(codec: &FFI_PhysicalExtensionCodec) -> Self {
        if (codec.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(codec.inner())
        } else {
            Arc::new(ForeignPhysicalExtensionCodec(codec.clone()))
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
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs = inputs
            .iter()
            .map(|plan| FFI_ExecutionPlan::new(Arc::clone(plan), None))
            .collect();

        let plan =
            df_result!(unsafe { (self.0.try_decode)(&self.0, buf.into(), inputs) })?;
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;

        Ok(plan)
    }

    fn try_decode_with_ctx(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &PhysicalPlanDecodeContext<'_>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inputs = inputs
            .iter()
            .map(|plan| FFI_ExecutionPlan::new(Arc::clone(plan), None))
            .collect();

        let scalar_subquery_results = ctx
            .scalar_subquery_results()
            .cloned()
            .map(FFI_ScalarSubqueryResults::new)
            .into();

        let plan = df_result!(unsafe {
            (self.0.try_decode_with_ctx)(
                &self.0,
                buf.into(),
                inputs,
                scalar_subquery_results,
            )
        })?;
        let plan: Arc<dyn ExecutionPlan> = (&plan).try_into()?;

        Ok(plan)
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let plan = FFI_ExecutionPlan::new(node, None);
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
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_common::{Result, exec_err, internal_datafusion_err};
    use datafusion_execution::TaskContext;
    use datafusion_expr::physical_planning_context::{
        ScalarSubqueryResults, SubqueryIndex,
    };
    use datafusion_expr::ptr_eq::arc_ptr_eq;
    use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF, WindowUDFImpl};
    use datafusion_functions::math::abs::AbsFunc;
    use datafusion_functions_aggregate::sum::Sum;
    use datafusion_functions_window::rank::{Rank, RankType};
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion_physical_plan::empty::EmptyExec as RealEmptyExec;
    use datafusion_physical_plan::scalar_subquery::{
        ScalarSubqueryExec, ScalarSubqueryLink,
    };
    use datafusion_physical_plan::{
        ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
        PhysicalExpr, PlanProperties, ReplaceChildrenOptions, SendableRecordBatchStream,
        apply_expression_roots,
    };
    use datafusion_proto::bytes::{
        physical_plan_from_bytes_with_proto_converter,
        physical_plan_to_bytes_with_proto_converter,
    };
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
        PhysicalProtoConverterExtension,
    };
    use datafusion_proto::protobuf::PhysicalExprNode;
    use prost::Message;

    use crate::execution_plan::tests::EmptyExec;
    use crate::proto::physical_extension_codec::{
        FFI_PhysicalExtensionCodec, ForeignPhysicalExtensionCodec,
    };

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
            _proto_converter: &dyn PhysicalProtoConverterExtension,
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
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            let Some(_) = node.downcast_ref::<EmptyExec>() else {
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
            if !udf.is::<AbsFunc>() {
                return exec_err!("TestExtensionCodec only expects Abs UDF");
            }

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
            let Some(_udf) = udf.downcast_ref::<Sum>() else {
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
            let Some(udf) = udf.downcast_ref::<Rank>() else {
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
        foreign_codec.try_encode(
            Arc::clone(&exec),
            &mut bytes,
            &DefaultPhysicalProtoConverter {},
        )?;

        let returned_exec = foreign_codec.try_decode(
            &bytes,
            &input_execs,
            ctx.task_ctx().as_ref(),
            &DefaultPhysicalProtoConverter {},
        )?;

        assert!(returned_exec.is::<EmptyExec>());

        Ok(())
    }

    /// A codec whose `try_decode` fails and that only decodes through
    /// `try_decode_with_ctx`.
    #[derive(Debug)]
    struct ContextOnlyCodec;

    impl PhysicalExtensionCodec for ContextOnlyCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            exec_err!("ContextOnlyCodec decodes through try_decode_with_ctx")
        }

        fn try_decode_with_ctx(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &PhysicalPlanDecodeContext<'_>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(create_test_exec())
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            Ok(())
        }
    }

    /// Calls the foreign adapter's legacy `try_decode`, which has no decode
    /// context to forward, but the far side's own codec still reaches its
    /// `try_decode_with_ctx` override (with a root context) because
    /// `try_decode_fn_wrapper` always calls that method rather than
    /// `try_decode` directly.
    #[test]
    fn ffi_physical_extension_codec_legacy_decode_uses_context_aware_default()
    -> Result<()> {
        let codec = Arc::new(ContextOnlyCodec);
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let returned_exec = foreign_codec.try_decode(
            &[],
            &[create_test_exec()],
            ctx.task_ctx().as_ref(),
            &DefaultPhysicalProtoConverter {},
        )?;

        assert!(returned_exec.is::<EmptyExec>());

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

        assert!(returned_udf.inner().is::<AbsFunc>());

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

        assert!(returned_udf.inner().is::<Sum>());

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

        assert!(returned_udf.inner().is::<Rank>());

        Ok(())
    }

    #[test]
    fn ffi_physical_extension_codec_local_bypass() {
        let codec = Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>;
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_PhysicalExtensionCodec::new(Arc::clone(&codec), None, task_ctx_provider);

        // Verify local libraries can be downcast to their original
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();
        assert!(arc_ptr_eq(&foreign_codec, &codec));

        // Verify different library markers generate foreign providers
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();
        assert!(!arc_ptr_eq(&foreign_codec, &codec));
    }

    /// Importing a codec and re-wrapping it with a different task context
    /// provider must rebind the handle. See
    /// <https://github.com/apache/datafusion/issues/24722>.
    #[test]
    fn ffi_physical_extension_codec_rebind_adopts_task_ctx_provider() {
        let (_ctx_a, provider_a) = crate::util::tests::test_session_and_ctx();
        let (ctx_b, provider_b) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(TestExtensionCodec {}) as Arc<dyn PhysicalExtensionCodec>,
            None,
            provider_a,
        );
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;

        let imported: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();
        assert!(
            (Arc::clone(&imported) as Arc<dyn std::any::Any>)
                .downcast_ref::<ForeignPhysicalExtensionCodec>()
                .is_some()
        );

        let rebound = FFI_PhysicalExtensionCodec::new(imported, None, provider_b);

        let task_ctx: Arc<TaskContext> = (&rebound.task_ctx_provider)
            .try_into()
            .expect("rebound codec resolves");
        assert_eq!(task_ctx.session_id(), ctx_b.task_ctx().session_id());
    }

    /// An extension plan that carries a single physical expression, so its
    /// codec has to decode that expression itself.
    #[derive(Debug)]
    struct ScalarSubqueryExprExec {
        expr: Arc<dyn PhysicalExpr>,
        child: Arc<dyn ExecutionPlan>,
    }

    impl DisplayAs for ScalarSubqueryExprExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "ScalarSubqueryExprExec")
        }
    }

    impl ExecutionPlan for ScalarSubqueryExprExec {
        fn name(&self) -> &str {
            "ScalarSubqueryExprExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.child.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.child]
        }

        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            apply_expression_roots(std::slice::from_ref(&self.expr), f)
        }

        fn replace_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
            _: ReplaceChildrenOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unreachable!()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            self.replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            unreachable!()
        }
    }

    #[derive(Clone, PartialEq, Message)]
    struct ScalarSubqueryExprExecProto {
        #[prost(message, optional, tag = "1")]
        expr: Option<PhysicalExprNode>,
    }

    /// Decodes [`ScalarSubqueryExprExec`] through `try_decode_with_ctx`,
    /// passing the decode context on to its expression. `try_decode` fails so
    /// a caller that drops the context (by routing through the plain
    /// `try_decode` FFI entry point instead of `try_decode_with_ctx`) is
    /// caught by this test.
    #[derive(Debug)]
    struct ScalarSubqueryExprExecCodec;

    impl PhysicalExtensionCodec for ScalarSubqueryExprExecCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            exec_err!("ScalarSubqueryExprExecCodec decodes through try_decode_with_ctx")
        }

        fn try_decode_with_ctx(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            ctx: &PhysicalPlanDecodeContext<'_>,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let proto = ScalarSubqueryExprExecProto::decode(buf).map_err(|e| {
                internal_datafusion_err!("failed to decode ScalarSubqueryExprExec: {e}")
            })?;
            let expr_proto = proto.expr.ok_or_else(|| {
                internal_datafusion_err!("ScalarSubqueryExprExec is missing its expr")
            })?;
            let schema = inputs[0].schema();
            let expr =
                proto_converter.proto_to_physical_expr(&expr_proto, &schema, ctx)?;
            Ok(Arc::new(ScalarSubqueryExprExec {
                expr,
                child: Arc::clone(&inputs[0]),
            }))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            let exec =
                node.downcast_ref::<ScalarSubqueryExprExec>()
                    .ok_or_else(|| {
                        internal_datafusion_err!("expected ScalarSubqueryExprExec")
                    })?;
            let proto = ScalarSubqueryExprExecProto {
                expr: Some(proto_converter.physical_expr_to_proto(&exec.expr, self)?),
            };
            proto.encode(buf).map_err(|e| {
                internal_datafusion_err!("failed to encode ScalarSubqueryExprExec: {e}")
            })
        }
    }

    /// The decode context's active scalar subquery results scope must reach a
    /// `ScalarSubqueryExpr` decoded by a codec that is forced foreign through
    /// the FFI boundary, not just a local one. Without the fix, the far side
    /// always decodes with a root context (no scope), so the embedded
    /// `ScalarSubqueryExpr` fails to deserialize.
    #[test]
    fn ffi_physical_extension_codec_forced_foreign_scalar_subquery_roundtrip()
    -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let subquery_schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));

        let results = ScalarSubqueryResults::new(1);
        let sq_expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarSubqueryExpr::new(
            DataType::Int64,
            true,
            SubqueryIndex::new(0),
            results.clone(),
        ));
        let extension_plan: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExprExec {
            expr: sq_expr,
            child: Arc::new(RealEmptyExec::new(Arc::clone(&schema))),
        });
        let plan: Arc<dyn ExecutionPlan> = Arc::new(ScalarSubqueryExec::new(
            extension_plan,
            vec![ScalarSubqueryLink {
                plan: Arc::new(RealEmptyExec::new(subquery_schema)),
                index: SubqueryIndex::new(0),
            }],
            results,
        ));

        let bytes = physical_plan_to_bytes_with_proto_converter(
            Arc::clone(&plan),
            &ScalarSubqueryExprExecCodec,
            &DefaultPhysicalProtoConverter {},
        )?;

        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let mut ffi_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(ScalarSubqueryExprExecCodec),
            None,
            task_ctx_provider,
        );
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn PhysicalExtensionCodec> = (&ffi_codec).into();

        let deserialized = physical_plan_from_bytes_with_proto_converter(
            bytes.as_ref(),
            ctx.task_ctx().as_ref(),
            foreign_codec.as_ref(),
            &DefaultPhysicalProtoConverter {},
        )?;

        let sq_exec = deserialized
            .downcast_ref::<ScalarSubqueryExec>()
            .expect("expected ScalarSubqueryExec");
        let decoded_extension = sq_exec
            .input()
            .downcast_ref::<ScalarSubqueryExprExec>()
            .expect(
                "expected ScalarSubqueryExprExec decoded through the forced-foreign codec",
            );
        let decoded_sq_expr = decoded_extension
            .expr
            .downcast_ref::<ScalarSubqueryExpr>()
            .expect("expected ScalarSubqueryExpr");

        // The expression decoded by the forced-foreign codec must observe
        // values written to the host's ScalarSubqueryExec results container.
        assert_eq!(decoded_sq_expr.results().get(SubqueryIndex::new(0)), None);
        sq_exec.results().set(
            SubqueryIndex::new(0),
            datafusion_common::ScalarValue::Int64(Some(42)),
        )?;
        assert_eq!(
            decoded_sq_expr.results().get(SubqueryIndex::new(0)),
            Some(datafusion_common::ScalarValue::Int64(Some(42)))
        );

        Ok(())
    }
}
