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
use arrow::datatypes::SchemaRef;
use datafusion_catalog::TableProvider;
use datafusion_common::error::Result;
use datafusion_common::{not_impl_err, TableReference};
use datafusion_datasource::file_format::FileFormatFactory;
use datafusion_execution::TaskContext;
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, Extension, LogicalPlan, ScalarUDF, ScalarUDFImpl,
    WindowUDF, WindowUDFImpl,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use tokio::runtime::Handle;

use crate::arrow_wrappers::WrappedSchema;
use crate::execution::FFI_TaskContextProvider;
use crate::table_provider::FFI_TableProvider;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFIResult;
use crate::{df_result, rresult_return};

/// A stable struct for sharing [`LogicalExtensionCodec`] across FFI boundaries.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_LogicalExtensionCodec {
    /// Decode bytes into a table provider.
    try_decode_table_provider: unsafe extern "C" fn(
        &Self,
        buf: RSlice<u8>,
        table_ref: RStr,
        schema: WrappedSchema,
    ) -> FFIResult<FFI_TableProvider>,

    /// Encode a table provider into bytes.
    try_encode_table_provider: unsafe extern "C" fn(
        &Self,
        table_ref: RStr,
        node: FFI_TableProvider,
    ) -> FFIResult<RVec<u8>>,

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

    task_ctx_provider: FFI_TaskContextProvider,

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
    pub library_marker_id: extern "C" fn() -> usize,
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

    fn task_ctx(&self) -> Result<Arc<TaskContext>> {
        (&self.task_ctx_provider).try_into()
    }
}

unsafe extern "C" fn try_decode_table_provider_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    buf: RSlice<u8>,
    table_ref: RStr,
    schema: WrappedSchema,
) -> FFIResult<FFI_TableProvider> {
    let ctx = rresult_return!(codec.task_ctx());
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

    RResult::ROk(FFI_TableProvider::new(table_provider, true, runtime))
}

unsafe extern "C" fn try_encode_table_provider_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    table_ref: RStr,
    node: FFI_TableProvider,
) -> FFIResult<RVec<u8>> {
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
) -> FFIResult<FFI_ScalarUDF> {
    let codec = codec.inner();

    let udf = rresult_return!(codec.try_decode_udf(name.as_str(), buf.as_ref()));
    let udf = FFI_ScalarUDF::from(udf);

    RResult::ROk(udf)
}

unsafe extern "C" fn try_encode_udf_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
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
    codec: &FFI_LogicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> FFIResult<FFI_AggregateUDF> {
    let codec_inner = codec.inner();
    let udaf = rresult_return!(codec_inner.try_decode_udaf(name.into(), buf.as_ref()));
    let udaf = FFI_AggregateUDF::from(udaf);

    RResult::ROk(udaf)
}

unsafe extern "C" fn try_encode_udaf_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
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
    codec: &FFI_LogicalExtensionCodec,
    name: RStr,
    buf: RSlice<u8>,
) -> FFIResult<FFI_WindowUDF> {
    let codec = codec.inner();
    let udwf = rresult_return!(codec.try_decode_udwf(name.into(), buf.as_ref()));
    let udwf = FFI_WindowUDF::from(udwf);

    RResult::ROk(udwf)
}

unsafe extern "C" fn try_encode_udwf_fn_wrapper(
    codec: &FFI_LogicalExtensionCodec,
    node: FFI_WindowUDF,
) -> FFIResult<RVec<u8>> {
    let codec = codec.inner();
    let udwf: Arc<dyn WindowUDFImpl> = (&node).into();
    let udwf = WindowUDF::new_from_shared_impl(udwf);

    let mut bytes = Vec::new();
    rresult_return!(codec.try_encode_udwf(&udwf, &mut bytes));

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
        let node = FFI_TableProvider::new(node, true, None);

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
mod tests {
    use std::sync::Arc;

    use arrow::array::record_batch;
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use datafusion_catalog::{MemTable, TableProvider};
    use datafusion_common::{exec_err, Result, TableReference};
    use datafusion_datasource::file_format::FileFormatFactory;
    use datafusion_execution::TaskContext;
    use datafusion_expr::ptr_eq::arc_ptr_eq;
    use datafusion_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF};
    use datafusion_functions::math::abs::AbsFunc;
    use datafusion_functions_aggregate::sum::Sum;
    use datafusion_functions_window::rank::{Rank, RankType};
    use datafusion_proto::logical_plan::LogicalExtensionCodec;
    use datafusion_proto::physical_plan::PhysicalExtensionCodec;

    use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
    use crate::proto::physical_extension_codec::tests::TestExtensionCodec;

    fn create_test_table() -> MemTable {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let rb = record_batch!(("a", Int32, [1, 2, 3]))
            .expect("should be able to create a record batch");
        MemTable::try_new(schema, vec![vec![rb]])
            .expect("should be able to create an in memory table")
    }

    impl LogicalExtensionCodec for TestExtensionCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> Result<Extension> {
            unimplemented!()
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
            unimplemented!()
        }

        fn try_decode_table_provider(
            &self,
            buf: &[u8],
            _table_ref: &TableReference,
            schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn TableProvider>> {
            if buf[0] != Self::MAGIC_NUMBER {
                return exec_err!(
                    "TestExtensionCodec input buffer does not start with magic number"
                );
            }

            if schema != create_test_table().schema() {
                return exec_err!("Incorrect test table schema");
            }

            if buf.len() != 2 || buf[1] != Self::MEMTABLE_SERIALIZED {
                return exec_err!("TestExtensionCodec unable to decode table provider");
            }

            Ok(Arc::new(create_test_table()) as Arc<dyn TableProvider>)
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            buf.push(Self::MAGIC_NUMBER);

            if !node.as_any().is::<MemTable>() {
                return exec_err!("TestExtensionCodec only expects MemTable");
            };

            if node.schema() != create_test_table().schema() {
                return exec_err!("Unexpected schema for encoding.");
            }

            buf.push(Self::MEMTABLE_SERIALIZED);

            Ok(())
        }

        fn try_decode_file_format(
            &self,
            _buf: &[u8],
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn FileFormatFactory>> {
            unimplemented!()
        }

        fn try_encode_file_format(
            &self,
            _buf: &mut Vec<u8>,
            _node: Arc<dyn FileFormatFactory>,
        ) -> Result<()> {
            unimplemented!()
        }

        fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
            PhysicalExtensionCodec::try_decode_udf(self, name, buf)
        }

        fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
            PhysicalExtensionCodec::try_encode_udf(self, node, buf)
        }

        fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
            PhysicalExtensionCodec::try_decode_udaf(self, name, buf)
        }

        fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
            PhysicalExtensionCodec::try_encode_udaf(self, node, buf)
        }

        fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
            PhysicalExtensionCodec::try_decode_udwf(self, name, buf)
        }

        fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
            PhysicalExtensionCodec::try_encode_udwf(self, node, buf)
        }
    }

    #[test]
    fn roundtrip_ffi_logical_extension_codec_table_provider() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_LogicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();

        let table = Arc::new(create_test_table()) as Arc<dyn TableProvider>;
        let mut bytes = Vec::new();
        foreign_codec.try_encode_table_provider(&"my_table".into(), table, &mut bytes)?;

        let returned_table = foreign_codec.try_decode_table_provider(
            &bytes,
            &"my_table".into(),
            create_test_table().schema(),
            ctx.task_ctx().as_ref(),
        )?;

        assert!(returned_table.as_any().is::<MemTable>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_logical_extension_codec_udf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_LogicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();

        let udf = Arc::new(ScalarUDF::from(AbsFunc::new()));
        let mut bytes = Vec::new();
        foreign_codec.try_encode_udf(udf.as_ref(), &mut bytes)?;

        let returned_udf = foreign_codec.try_decode_udf(udf.name(), &bytes)?;

        assert!(returned_udf.inner().as_any().is::<AbsFunc>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_logical_extension_codec_udaf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_LogicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();

        let udf = Arc::new(AggregateUDF::from(Sum::new()));
        let mut bytes = Vec::new();
        foreign_codec.try_encode_udaf(udf.as_ref(), &mut bytes)?;

        let returned_udf = foreign_codec.try_decode_udaf(udf.name(), &bytes)?;

        assert!(returned_udf.inner().as_any().is::<Sum>());

        Ok(())
    }

    #[test]
    fn roundtrip_ffi_logical_extension_codec_udwf() -> Result<()> {
        let codec = Arc::new(TestExtensionCodec {});
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_LogicalExtensionCodec::new(codec, None, task_ctx_provider);
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();

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
    fn ffi_logical_extension_codec_local_bypass() {
        let codec =
            Arc::new(TestExtensionCodec {}) as Arc<dyn LogicalExtensionCodec + Send>;
        let (_ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();

        let mut ffi_codec =
            FFI_LogicalExtensionCodec::new(Arc::clone(&codec), None, task_ctx_provider);

        let codec = codec as Arc<dyn LogicalExtensionCodec>;
        // Verify local libraries can be downcast to their original
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();
        assert!(arc_ptr_eq(&foreign_codec, &codec));

        // Verify different library markers generate foreign providers
        ffi_codec.library_marker_id = crate::mock_foreign_marker_id;
        let foreign_codec: Arc<dyn LogicalExtensionCodec> = (&ffi_codec).into();
        assert!(!arc_ptr_eq(&foreign_codec, &codec));
    }
}
