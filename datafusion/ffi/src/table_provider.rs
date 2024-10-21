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

use std::{any::Any, ffi::c_void, sync::Arc};

use abi_stable::{
    std_types::{ROption, RResult, RString, RVec},
    StableAbi,
};
use arrow::{
    datatypes::{Schema, SchemaRef},
    ffi::FFI_ArrowSchema,
};
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    execution::session_state::SessionStateBuilder,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionContext},
};
use datafusion_proto::{
    logical_plan::{
        from_proto::parse_exprs, to_proto::serialize_exprs, DefaultLogicalExtensionCodec,
    },
    protobuf::LogicalExprList,
};
use prost::Message;

use crate::{
    plan_properties::WrappedSchema,
    session_config::ForeignSessionConfig,
    table_source::{FFI_TableProviderFilterPushDown, FFI_TableType},
};

use super::{
    execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan},
    session_config::FFI_SessionConfig,
};
use datafusion::error::Result;

/// A stable interface for creating a DataFusion TableProvider.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TableProvider {
    pub schema: unsafe extern "C" fn(provider: &Self) -> WrappedSchema,
    pub scan: unsafe extern "C" fn(
        provider: &Self,
        session_config: &FFI_SessionConfig,
        projections: RVec<usize>,
        filters_serialized: RVec<u8>,
        limit: ROption<usize>,
    ) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>>,

    pub table_type: unsafe extern "C" fn(provider: &Self) -> FFI_TableType,

    pub supports_filters_pushdown: Option<
        unsafe extern "C" fn(
            provider: &FFI_TableProvider,
            filters_serialized: RVec<u8>,
        )
            -> RResult<RVec<FFI_TableProviderFilterPushDown>, RString>,
    >,

    pub clone: unsafe extern "C" fn(provider: &Self) -> Self,
    pub release: unsafe extern "C" fn(arg: &mut Self),
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn TableProvider + Send>,
}

unsafe extern "C" fn schema_fn_wrapper(provider: &FFI_TableProvider) -> WrappedSchema {
    let private_data = provider.private_data as *const ProviderPrivateData;
    let provider = &(*private_data).provider;

    // This does silently fail because TableProvider does not return a result.
    // It expects schema to always pass.
    let ffi_schema = FFI_ArrowSchema::try_from(provider.schema().as_ref())
        .unwrap_or(FFI_ArrowSchema::empty());

    WrappedSchema(ffi_schema)
}

unsafe extern "C" fn table_type_fn_wrapper(
    provider: &FFI_TableProvider,
) -> FFI_TableType {
    let private_data = provider.private_data as *const ProviderPrivateData;
    let provider = &(*private_data).provider;

    provider.table_type().into()
}

fn supports_filters_pushdown_internal(
    provider: &Arc<dyn TableProvider + Send>,
    filters_serialized: &[u8],
) -> Result<RVec<FFI_TableProviderFilterPushDown>> {
    let default_ctx = SessionContext::new();
    let codec = DefaultLogicalExtensionCodec {};

    let filters = match filters_serialized.is_empty() {
        true => vec![],
        false => {
            let proto_filters = LogicalExprList::decode(filters_serialized)
                .map_err(|e| DataFusionError::Plan(e.to_string()))?;

            parse_exprs(proto_filters.expr.iter(), &default_ctx, &codec)?
        }
    };
    let filters_borrowed: Vec<&Expr> = filters.iter().collect();

    let results: RVec<_> = provider
        .supports_filters_pushdown(&filters_borrowed)?
        .iter()
        .map(|v| v.into())
        .collect();

    Ok(results)
}

unsafe extern "C" fn supports_filters_pushdown_fn_wrapper(
    provider: &FFI_TableProvider,
    filters_serialized: RVec<u8>,
) -> RResult<RVec<FFI_TableProviderFilterPushDown>, RString> {
    let private_data = provider.private_data as *const ProviderPrivateData;
    let provider = &(*private_data).provider;

    supports_filters_pushdown_internal(provider, &filters_serialized)
        .map_err(|e| e.to_string().into())
        .into()
}

unsafe extern "C" fn scan_fn_wrapper(
    provider: &FFI_TableProvider,
    session_config: &FFI_SessionConfig,
    projections: RVec<usize>,
    filters_serialized: RVec<u8>,
    limit: ROption<usize>,
) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>> {
    let private_data = provider.private_data as *mut ProviderPrivateData;
    let internal_provider = &(*private_data).provider;
    let session_config = session_config.clone();

    async move {
        let config = match ForeignSessionConfig::new(&session_config) {
            Ok(c) => c,
            Err(e) => return RResult::RErr(e.to_string().into()),
        };
        let session = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config.0)
            .build();
        let ctx = SessionContext::new_with_state(session);

        let filters = match filters_serialized.is_empty() {
            true => vec![],
            false => {
                let default_ctx = SessionContext::new();
                let codec = DefaultLogicalExtensionCodec {};

                let proto_filters =
                    match LogicalExprList::decode(filters_serialized.as_ref()) {
                        Ok(f) => f,
                        Err(e) => return RResult::RErr(e.to_string().into()),
                    };

                match parse_exprs(proto_filters.expr.iter(), &default_ctx, &codec) {
                    Ok(f) => f,
                    Err(e) => return RResult::RErr(e.to_string().into()),
                }
            }
        };

        let projections: Vec<_> = projections.into_iter().collect();
        let maybe_projections = match projections.is_empty() {
            true => None,
            false => Some(&projections),
        };

        let plan = match internal_provider
            .scan(&ctx.state(), maybe_projections, &filters, limit.into())
            .await
        {
            Ok(p) => p,
            Err(e) => return RResult::RErr(e.to_string().into()),
        };

        FFI_ExecutionPlan::new(plan, ctx.task_ctx())
            .map_err(|e| e.to_string().into())
            .into()
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_TableProvider) {
    let private_data = Box::from_raw(provider.private_data as *mut ProviderPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_TableProvider) -> FFI_TableProvider {
    let old_private_data = provider.private_data as *const ProviderPrivateData;

    let private_data = Box::into_raw(Box::new(ProviderPrivateData {
        provider: Arc::clone(&(*old_private_data).provider),
    })) as *mut c_void;

    FFI_TableProvider {
        schema: schema_fn_wrapper,
        scan: scan_fn_wrapper,
        table_type: table_type_fn_wrapper,
        supports_filters_pushdown: provider.supports_filters_pushdown,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        private_data,
    }
}

impl Drop for FFI_TableProvider {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(
        provider: Arc<dyn TableProvider + Send>,
        can_support_pushdown_filters: bool,
    ) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider });

        Self {
            schema: schema_fn_wrapper,
            scan: scan_fn_wrapper,
            table_type: table_type_fn_wrapper,
            supports_filters_pushdown: match can_support_pushdown_filters {
                true => Some(supports_filters_pushdown_fn_wrapper),
                false => None,
            },
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }
}

/// This wrapper struct exists on the reciever side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_TableProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignTableProvider(FFI_TableProvider);

unsafe impl Send for ForeignTableProvider {}
unsafe impl Sync for ForeignTableProvider {}

impl ForeignTableProvider {
    pub fn new(provider: &FFI_TableProvider) -> Self {
        Self(provider.clone())
    }
}

impl Clone for FFI_TableProvider {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

#[async_trait]
impl TableProvider for ForeignTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = unsafe {
            let wrapped_schema = (self.0.schema)(&self.0);
            Schema::try_from(&wrapped_schema.0).unwrap_or(Schema::empty())
        };
        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        unsafe { (self.0.table_type)(&self.0).into() }
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_config = FFI_SessionConfig::new(session.config());

        let projections: Option<RVec<usize>> =
            projection.map(|p| p.iter().map(|v| v.to_owned()).collect());

        let codec = DefaultLogicalExtensionCodec {};
        let filter_list = LogicalExprList {
            expr: serialize_exprs(filters, &codec)?,
        };
        let filters_serialized = filter_list.encode_to_vec().into();

        let plan = unsafe {
            let maybe_plan = (self.0.scan)(
                &self.0,
                &session_config,
                projections.unwrap_or_default(),
                filters_serialized,
                limit.into(),
            )
            .await;

            match maybe_plan {
                RResult::ROk(p) => ForeignExecutionPlan::new(p)?,
                RResult::RErr(_) => {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "Unable to perform scan via FFI".to_string(),
                    ))
                }
            }
        };

        Ok(Arc::new(plan))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        unsafe {
            let pushdown_fn = match self.0.supports_filters_pushdown {
                Some(func) => func,
                None => {
                    return Ok(vec![
                        TableProviderFilterPushDown::Unsupported;
                        filters.len()
                    ])
                }
            };

            let codec = DefaultLogicalExtensionCodec {};

            let expr_list = LogicalExprList {
                expr: serialize_exprs(filters.iter().map(|f| f.to_owned()), &codec)?,
            };
            let serialized_filters = expr_list.encode_to_vec();

            let pushdowns = pushdown_fn(&self.0, serialized_filters.into());

            match pushdowns {
                RResult::ROk(p) => Ok(p.iter().map(|v| v.into()).collect()),
                RResult::RErr(e) => Err(DataFusionError::Plan(e.to_string())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit};

    use super::*;

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider() -> Result<()> {
        use arrow::datatypes::Field;
        use datafusion::arrow::{
            array::Float32Array, datatypes::DataType, record_batch::RecordBatch,
        };
        use datafusion::datasource::MemTable;

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

        // define data in two partitions
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![2.0, 4.0, 8.0]))],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Float32Array::from(vec![64.0]))],
        )?;

        let ctx = SessionContext::new();

        let provider =
            Arc::new(MemTable::try_new(schema, vec![vec![batch1], vec![batch2]])?);

        let ffi_provider = FFI_TableProvider::new(provider, true);

        let foreign_table_provider = ForeignTableProvider::new(&ffi_provider);

        ctx.register_table("t", Arc::new(foreign_table_provider))?;

        let df = ctx.table("t").await?;

        df.select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }
}
