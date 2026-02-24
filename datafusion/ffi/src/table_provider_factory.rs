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

use std::{ffi::c_void, sync::Arc};

use abi_stable::{
    StableAbi,
    std_types::{RResult, RString, RVec},
};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider, TableProviderFactory};
use datafusion_common::error::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::{CreateExternalTable, DdlStatement, LogicalPlan};
use datafusion_proto::logical_plan::{
    AsLogicalPlan, DefaultLogicalExtensionCodec, LogicalExtensionCodec,
};
use datafusion_proto::protobuf::LogicalPlanNode;
use prost::Message;
use tokio::runtime::Handle;

use crate::execution::FFI_TaskContextProvider;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::session::{FFI_SessionRef, ForeignSession};
use crate::table_provider::{FFI_TableProvider, ForeignTableProvider};
use crate::{df_result, rresult_return};

/// A stable struct for sharing [`TableProviderFactory`] across FFI boundaries.
///
/// Similar to [`FFI_TableProvider`], this struct uses the FFI-safe pattern where:
/// - The `FFI_*` struct exposes stable function pointers
/// - Private data is stored as an opaque pointer
/// - The `Foreign*` wrapper is used by consumers on the other side of the FFI boundary
///
/// [`FFI_TableProvider`]: crate::table_provider::FFI_TableProvider
#[repr(C)]
#[derive(Debug, StableAbi)]
pub struct FFI_TableProviderFactory {
    /// Create a TableProvider with the given command.
    ///
    /// # Arguments
    ///
    /// * `factory` - the table provider factory
    /// * `session_config` - session configuration
    /// * `cmd_serialized` - a ['CreateExternalTable`] encoded as a [`LogicalPlanNode`] protobuf message serialized into bytes
    ///   to pass across the FFI boundary.
    create: unsafe extern "C" fn(
        factory: &Self,
        session: FFI_SessionRef,
        cmd_serialized: RVec<u8>,
    ) -> FfiFuture<RResult<FFI_TableProvider, RString>>,

    logical_codec: FFI_LogicalExtensionCodec,

    /// Used to create a clone of the factory. This should only need to be called
    /// by the receiver of the factory.
    clone: unsafe extern "C" fn(factory: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(factory: &mut Self),

    /// Return the major DataFusion version number of this factory.
    version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the factory.
    /// A [`ForeignTableProviderFactory`] should never attempt to access this data.
    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_TableProviderFactory {}
unsafe impl Sync for FFI_TableProviderFactory {}

struct FactoryPrivateData {
    factory: Arc<dyn TableProviderFactory + Send>,
    runtime: Option<Handle>,
}

impl FFI_TableProviderFactory {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(
        factory: Arc<dyn TableProviderFactory + Send>,
        runtime: Option<Handle>,
        task_ctx_provider: impl Into<FFI_TaskContextProvider>,
        logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
    ) -> Self {
        let task_ctx_provider = task_ctx_provider.into();
        let logical_codec =
            logical_codec.unwrap_or_else(|| Arc::new(DefaultLogicalExtensionCodec {}));
        let logical_codec = FFI_LogicalExtensionCodec::new(
            logical_codec,
            runtime.clone(),
            task_ctx_provider.clone(),
        );
        Self::new_with_ffi_codec(factory, runtime, logical_codec)
    }

    pub fn new_with_ffi_codec(
        factory: Arc<dyn TableProviderFactory + Send>,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        let private_data = Box::new(FactoryPrivateData { factory, runtime });

        Self {
            create: create_fn_wrapper,
            logical_codec,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }

    fn inner(&self) -> &Arc<dyn TableProviderFactory + Send> {
        let private_data = self.private_data as *const FactoryPrivateData;
        unsafe { &(*private_data).factory }
    }

    fn runtime(&self) -> &Option<Handle> {
        let private_data = self.private_data as *const FactoryPrivateData;
        unsafe { &(*private_data).runtime }
    }

    fn deserialize_cmd(
        &self,
        cmd_serialized: &RVec<u8>,
    ) -> Result<CreateExternalTable, DataFusionError> {
        let task_ctx: Arc<TaskContext> =
            (&self.logical_codec.task_ctx_provider).try_into()?;
        let logical_codec: Arc<dyn LogicalExtensionCodec> = (&self.logical_codec).into();

        let plan = LogicalPlanNode::decode(cmd_serialized.as_ref())
            .map_err(|e| DataFusionError::Internal(format!("{e:?}")))?;
        match plan.try_into_logical_plan(&task_ctx, logical_codec.as_ref())? {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => Ok(cmd),
            _ => Err(DataFusionError::Internal(
                "Invalid logical plan in FFI_TableProviderFactory.".to_owned(),
            )),
        }
    }
}

impl Clone for FFI_TableProviderFactory {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_TableProviderFactory {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl From<&FFI_TableProviderFactory> for Arc<dyn TableProviderFactory> {
    fn from(factory: &FFI_TableProviderFactory) -> Self {
        if (factory.library_marker_id)() == crate::get_library_marker_id() {
            Arc::clone(factory.inner()) as Arc<dyn TableProviderFactory>
        } else {
            Arc::new(ForeignTableProviderFactory(factory.clone()))
        }
    }
}

unsafe extern "C" fn create_fn_wrapper(
    factory: &FFI_TableProviderFactory,
    session: FFI_SessionRef,
    cmd_serialized: RVec<u8>,
) -> FfiFuture<RResult<FFI_TableProvider, RString>> {
    let factory = factory.clone();

    async move {
        let provider = rresult_return!(
            create_fn_wrapper_impl(factory, session, cmd_serialized).await
        );
        RResult::ROk(provider)
    }
    .into_ffi()
}

async fn create_fn_wrapper_impl(
    factory: FFI_TableProviderFactory,
    session: FFI_SessionRef,
    cmd_serialized: RVec<u8>,
) -> Result<FFI_TableProvider, DataFusionError> {
    let runtime = factory.runtime().clone();
    let ffi_logical_codec = factory.logical_codec.clone();
    let internal_factory = Arc::clone(factory.inner());
    let cmd = factory.deserialize_cmd(&cmd_serialized)?;

    let mut foreign_session = None;
    let session = session
        .as_local()
        .map(Ok::<&(dyn Session + Send + Sync), DataFusionError>)
        .unwrap_or_else(|| {
            foreign_session = Some(ForeignSession::try_from(&session)?);
            Ok(foreign_session.as_ref().unwrap())
        })?;

    let provider = internal_factory.create(session, &cmd).await?;
    Ok(FFI_TableProvider::new_with_ffi_codec(
        provider,
        true,
        runtime.clone(),
        ffi_logical_codec,
    ))
}

unsafe extern "C" fn clone_fn_wrapper(
    factory: &FFI_TableProviderFactory,
) -> FFI_TableProviderFactory {
    let runtime = factory.runtime().clone();
    let old_factory = Arc::clone(factory.inner());

    let private_data = Box::into_raw(Box::new(FactoryPrivateData {
        factory: old_factory,
        runtime,
    })) as *mut c_void;

    FFI_TableProviderFactory {
        create: create_fn_wrapper,
        logical_codec: factory.logical_codec.clone(),
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
        library_marker_id: crate::get_library_marker_id,
    }
}

unsafe extern "C" fn release_fn_wrapper(factory: &mut FFI_TableProviderFactory) {
    unsafe {
        debug_assert!(!factory.private_data.is_null());
        let private_data = Box::from_raw(factory.private_data as *mut FactoryPrivateData);
        drop(private_data);
        factory.private_data = std::ptr::null_mut();
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_TableProviderFactory to interact with the foreign table provider factory.
#[derive(Debug)]
pub struct ForeignTableProviderFactory(pub FFI_TableProviderFactory);

impl ForeignTableProviderFactory {
    fn serialize_cmd(
        &self,
        cmd: CreateExternalTable,
    ) -> Result<RVec<u8>, DataFusionError> {
        let logical_codec: Arc<dyn LogicalExtensionCodec> =
            (&self.0.logical_codec).into();

        let plan = LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd));
        let plan: LogicalPlanNode =
            AsLogicalPlan::try_from_logical_plan(&plan, logical_codec.as_ref())?;

        let mut buf: Vec<u8> = Vec::new();
        plan.try_encode(&mut buf)?;

        Ok(buf.into())
    }
}

unsafe impl Send for ForeignTableProviderFactory {}
unsafe impl Sync for ForeignTableProviderFactory {}

#[async_trait]
impl TableProviderFactory for ForeignTableProviderFactory {
    async fn create(
        &self,
        session: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let session = FFI_SessionRef::new(session, None, self.0.logical_codec.clone());
        let cmd = self.serialize_cmd(cmd.clone())?;

        let provider = unsafe {
            let maybe_provider = (self.0.create)(&self.0, session, cmd).await;

            let ffi_provider = df_result!(maybe_provider)?;
            ForeignTableProvider(ffi_provider)
        };

        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{TableReference, ToDFSchema};
    use datafusion_execution::TaskContextProvider;
    use std::collections::HashMap;

    use super::*;

    #[derive(Debug)]
    struct TestTableProviderFactory {}

    #[async_trait]
    impl TableProviderFactory for TestTableProviderFactory {
        async fn create(
            &self,
            _session: &dyn Session,
            _cmd: &CreateExternalTable,
        ) -> Result<Arc<dyn TableProvider>> {
            use arrow::datatypes::Field;
            use datafusion::arrow::array::Float32Array;
            use datafusion::arrow::datatypes::DataType;
            use datafusion::arrow::record_batch::RecordBatch;
            use datafusion::datasource::MemTable;

            let schema =
                Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

            let batch1 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
            )?;
            let batch2 = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Float32Array::from(vec![64.0]))],
            )?;

            Ok(Arc::new(MemTable::try_new(
                schema,
                vec![vec![batch1], vec![batch2]],
            )?))
        }
    }

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_factory() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let factory = Arc::new(TestTableProviderFactory {});
        let mut ffi_factory =
            FFI_TableProviderFactory::new(factory, None, task_ctx_provider, None);
        ffi_factory.library_marker_id = crate::mock_foreign_marker_id;

        let factory: Arc<dyn TableProviderFactory> = (&ffi_factory).into();

        let cmd = CreateExternalTable {
            schema: Schema::empty().to_dfschema_ref()?,
            name: TableReference::bare("test_table"),
            location: "test".to_string(),
            file_type: "test".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Default::default(),
            column_defaults: HashMap::new(),
        };

        let provider = factory.create(&ctx.state(), &cmd).await?;

        assert_eq!(provider.schema().fields().len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_table_provider_factory_clone() -> Result<()> {
        let ctx = Arc::new(SessionContext::new());
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);

        let factory = Arc::new(TestTableProviderFactory {});
        let ffi_factory =
            FFI_TableProviderFactory::new(factory, None, task_ctx_provider, None);

        // Test that we can clone the factory
        let cloned_factory = ffi_factory.clone();
        let factory: Arc<dyn TableProviderFactory> = (&cloned_factory).into();

        let cmd = CreateExternalTable {
            schema: Schema::empty().to_dfschema_ref()?,
            name: TableReference::bare("cloned_test"),
            location: "test".to_string(),
            file_type: "test".to_string(),
            table_partition_cols: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            definition: None,
            order_exprs: vec![],
            unbounded: false,
            options: HashMap::new(),
            constraints: Default::default(),
            column_defaults: HashMap::new(),
        };

        let provider = factory.create(&ctx.state(), &cmd).await?;
        assert_eq!(provider.schema().fields().len(), 1);

        Ok(())
    }
}
