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
    std_types::{RResult, RString, RVec},
    StableAbi,
};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider, TableProviderFactory},
    error::DataFusionError,
    execution::session_state::SessionStateBuilder,
    logical_expr::CreateExternalTable,
    prelude::SessionContext,
};
use datafusion_common::TableReference;
use datafusion_proto::{
    logical_plan::{from_proto, to_proto, DefaultLogicalExtensionCodec},
    protobuf::{CreateExternalTableNode, SortExprNodeCollection},
};
use prost::Message;
use std::collections::HashMap;
use tokio::runtime::Handle;

use crate::{
    df_result, rresult_return,
    session_config::FFI_SessionConfig,
    table_provider::{FFI_TableProvider, ForeignTableProvider},
};

use super::session_config::ForeignSessionConfig;
use datafusion::error::Result;

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
#[allow(non_camel_case_types)]
pub struct FFI_TableProviderFactory {
    /// Create a TableProvider with the given command.
    ///
    /// # Arguments
    ///
    /// * `factory` - the table provider factory
    /// * `session_config` - session configuration
    /// * `cmd_serialized` - a [`CreateExternalTableNode`] protobuf message serialized into bytes
    ///   to pass across the FFI boundary.
    pub create: unsafe extern "C" fn(
        factory: &Self,
        session_config: &FFI_SessionConfig,
        cmd_serialized: RVec<u8>,
    )
        -> FfiFuture<RResult<FFI_TableProvider, RString>>,

    /// Used to create a clone of the factory. This should only need to be called
    /// by the receiver of the factory.
    pub clone: unsafe extern "C" fn(factory: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(factory: &mut Self),

    /// Return the major DataFusion version number of this factory.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the factory.
    /// A [`ForeignTableProviderFactory`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProviderFactory {}
unsafe impl Sync for FFI_TableProviderFactory {}

struct FactoryPrivateData {
    factory: Arc<dyn TableProviderFactory>,
    runtime: Option<Handle>,
}

unsafe extern "C" fn create_fn_wrapper(
    factory: &FFI_TableProviderFactory,
    session_config: &FFI_SessionConfig,
    cmd_serialized: RVec<u8>,
) -> FfiFuture<RResult<FFI_TableProvider, RString>> {
    let private_data = factory.private_data as *mut FactoryPrivateData;
    let internal_factory = &(*private_data).factory;
    let session_config = session_config.clone();
    let runtime = &(*private_data).runtime;

    async move {
        let config = rresult_return!(ForeignSessionConfig::try_from(&session_config));
        let session = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config.0)
            .build();
        let ctx = SessionContext::new_with_state(session);

        let codec = DefaultLogicalExtensionCodec {};

        let proto_cmd =
            rresult_return!(CreateExternalTableNode::decode(cmd_serialized.as_ref()));

        // Deserialize CreateExternalTable from protobuf
        let pb_schema = rresult_return!(proto_cmd.schema.ok_or_else(|| {
            DataFusionError::Internal(
                "CreateExternalTableNode was missing required field schema".to_string(),
            )
        }));

        let constraints = rresult_return!(proto_cmd.constraints.ok_or_else(|| {
            DataFusionError::Internal(
                "CreateExternalTableNode was missing required field constraints"
                    .to_string(),
            )
        }));

        let definition = if !proto_cmd.definition.is_empty() {
            Some(proto_cmd.definition)
        } else {
            None
        };

        let mut order_exprs = vec![];
        for expr in &proto_cmd.order_exprs {
            let sorts = rresult_return!(from_proto::parse_sorts(
                &expr.sort_expr_nodes,
                &ctx,
                &codec
            ));
            order_exprs.push(sorts);
        }

        let mut column_defaults = HashMap::with_capacity(proto_cmd.column_defaults.len());
        for (col_name, expr) in &proto_cmd.column_defaults {
            let expr = rresult_return!(from_proto::parse_expr(expr, &ctx, &codec));
            column_defaults.insert(col_name.clone(), expr);
        }

        let table_ref = rresult_return!(proto_cmd.name.as_ref().ok_or_else(|| {
            DataFusionError::Internal(
                "CreateExternalTableNode was missing required field name".to_string(),
            )
        }));

        let name: TableReference = rresult_return!(table_ref.clone().try_into());

        let cmd = CreateExternalTable {
            schema: rresult_return!(pb_schema.try_into()),
            name,
            location: proto_cmd.location,
            file_type: proto_cmd.file_type,
            table_partition_cols: proto_cmd.table_partition_cols,
            order_exprs,
            if_not_exists: proto_cmd.if_not_exists,
            or_replace: proto_cmd.or_replace,
            temporary: proto_cmd.temporary,
            definition,
            unbounded: proto_cmd.unbounded,
            options: proto_cmd.options,
            constraints: constraints.into(),
            column_defaults,
        };

        let provider = rresult_return!(internal_factory.create(&ctx.state(), &cmd).await);

        RResult::ROk(FFI_TableProvider::new(provider, true, runtime.clone()))
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(factory: &mut FFI_TableProviderFactory) {
    let private_data = Box::from_raw(factory.private_data as *mut FactoryPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(
    factory: &FFI_TableProviderFactory,
) -> FFI_TableProviderFactory {
    let old_private_data = factory.private_data as *const FactoryPrivateData;
    let runtime = (*old_private_data).runtime.clone();

    let private_data = Box::into_raw(Box::new(FactoryPrivateData {
        factory: Arc::clone(&(*old_private_data).factory),
        runtime,
    })) as *mut c_void;

    FFI_TableProviderFactory {
        create: create_fn_wrapper,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
        private_data,
    }
}

impl Drop for FFI_TableProviderFactory {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_TableProviderFactory {
    /// Creates a new [`FFI_TableProviderFactory`].
    pub fn new(factory: Arc<dyn TableProviderFactory>, runtime: Option<Handle>) -> Self {
        let private_data = Box::new(FactoryPrivateData { factory, runtime });

        Self {
            create: create_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_TableProviderFactory to interact with the foreign table provider factory.
#[derive(Debug)]
pub struct ForeignTableProviderFactory(pub FFI_TableProviderFactory);

unsafe impl Send for ForeignTableProviderFactory {}
unsafe impl Sync for ForeignTableProviderFactory {}

impl From<&FFI_TableProviderFactory> for ForeignTableProviderFactory {
    fn from(factory: &FFI_TableProviderFactory) -> Self {
        Self(factory.clone())
    }
}

impl Clone for FFI_TableProviderFactory {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl TableProviderFactory for ForeignTableProviderFactory {
    async fn create(
        &self,
        state: &dyn Session,
        cmd: &CreateExternalTable,
    ) -> Result<Arc<dyn TableProvider>> {
        let session_config: FFI_SessionConfig = state.config().into();

        let codec = DefaultLogicalExtensionCodec {};

        // Serialize CreateExternalTable to protobuf
        let mut converted_order_exprs: Vec<SortExprNodeCollection> = vec![];
        for order in &cmd.order_exprs {
            let temp = SortExprNodeCollection {
                sort_expr_nodes: to_proto::serialize_sorts(order, &codec)?,
            };
            converted_order_exprs.push(temp);
        }

        let mut converted_column_defaults =
            HashMap::with_capacity(cmd.column_defaults.len());
        for (col_name, expr) in &cmd.column_defaults {
            converted_column_defaults
                .insert(col_name.clone(), to_proto::serialize_expr(expr, &codec)?);
        }

        let proto_cmd = CreateExternalTableNode {
            name: Some(cmd.name.clone().into()),
            location: cmd.location.clone(),
            file_type: cmd.file_type.clone(),
            schema: Some(cmd.schema.as_ref().try_into()?),
            table_partition_cols: cmd.table_partition_cols.clone(),
            if_not_exists: cmd.if_not_exists,
            or_replace: cmd.or_replace,
            temporary: cmd.temporary,
            order_exprs: converted_order_exprs,
            definition: cmd.definition.clone().unwrap_or_default(),
            unbounded: cmd.unbounded,
            options: cmd.options.clone(),
            constraints: Some(cmd.constraints.clone().into()),
            column_defaults: converted_column_defaults,
        };

        let cmd_serialized = proto_cmd.encode_to_vec().into();

        let provider = unsafe {
            let maybe_provider =
                (self.0.create)(&self.0, &session_config, cmd_serialized).await;

            let ffi_provider = df_result!(maybe_provider)?;
            ForeignTableProvider::from(&ffi_provider)
        };

        Ok(Arc::new(provider))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing_table_factory::ListingTableFactory;
    use datafusion_common::ToDFSchema;
    use std::collections::HashMap;

    use super::*;

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_factory() -> Result<()> {
        let factory = Arc::new(ListingTableFactory::new());

        let ffi_factory = FFI_TableProviderFactory::new(factory, None);

        let foreign_factory: ForeignTableProviderFactory = (&ffi_factory).into();

        let ctx = SessionContext::new();

        // Create a simple external table command
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let df_schema = schema.to_dfschema_ref()?;

        let cmd = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("test_table"),
            location: "file:///tmp/test.csv".to_string(),
            file_type: "CSV".to_string(),
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

        let provider = foreign_factory.create(&ctx.state(), &cmd).await?;

        assert_eq!(provider.schema().fields().len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_table_provider_factory_clone() -> Result<()> {
        let factory = Arc::new(ListingTableFactory::new());
        let ffi_factory = FFI_TableProviderFactory::new(factory, None);

        // Test that we can clone the factory
        let cloned_factory = ffi_factory.clone();
        let foreign_factory: ForeignTableProviderFactory = (&cloned_factory).into();

        let ctx = SessionContext::new();

        let schema = Schema::new(vec![Field::new("c", DataType::Float64, true)]);
        let df_schema = schema.to_dfschema_ref()?;

        let cmd = CreateExternalTable {
            schema: df_schema,
            name: TableReference::bare("cloned_test"),
            location: "file:///tmp/cloned.parquet".to_string(),
            file_type: "PARQUET".to_string(),
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

        let provider = foreign_factory.create(&ctx.state(), &cmd).await?;
        assert_eq!(provider.schema().fields().len(), 1);

        Ok(())
    }
}
