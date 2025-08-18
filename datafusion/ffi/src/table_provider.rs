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
use arrow::datatypes::SchemaRef;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion::{
    catalog::{Session, TableProvider},
    datasource::TableType,
    error::DataFusionError,
    execution::{session_state::SessionStateBuilder, TaskContext},
    logical_expr::{logical_plan::dml::InsertOp, TableProviderFilterPushDown},
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
use tokio::runtime::Handle;

use crate::{
    arrow_wrappers::WrappedSchema,
    df_result, rresult_return,
    session_config::ForeignSessionConfig,
    table_source::{FFI_TableProviderFilterPushDown, FFI_TableType},
};

use super::{
    execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan},
    insert_op::FFI_InsertOp,
    session_config::FFI_SessionConfig,
};
use datafusion::error::Result;

/// A stable struct for sharing [`TableProvider`] across FFI boundaries.
///
/// # Struct Layout
///
/// The following description applies to all structs provided in this crate.
///
/// Each of the exposed structs in this crate is provided with a variant prefixed
/// with `Foreign`. This variant is designed to be used by the consumer of the
/// foreign code. The `Foreign` structs should _never_ access the `private_data`
/// fields. Instead they should only access the data returned through the function
/// calls defined on the `FFI_` structs. The second purpose of the `Foreign`
/// structs is to contain additional data that may be needed by the traits that
/// are implemented on them. Some of these traits require borrowing data which
/// can be far more convenient to be locally stored.
///
/// For example, we have a struct `FFI_TableProvider` to give access to the
/// `TableProvider` functions like `table_type()` and `scan()`. If we write a
/// library that wishes to expose it's `TableProvider`, then we can access the
/// private data that contains the Arc reference to the `TableProvider` via
/// `FFI_TableProvider`. This data is local to the library.
///
/// If we have a program that accesses a `TableProvider` via FFI, then it
/// will use `ForeignTableProvider`. When using `ForeignTableProvider` we **must**
/// not attempt to access the `private_data` field in `FFI_TableProvider`. If a
/// user is testing locally, you may be able to successfully access this field, but
/// it will only work if you are building against the exact same version of
/// `DataFusion` for both libraries **and** the same compiler. It will not work
/// in general.
///
/// It is worth noting that which library is the `local` and which is `foreign`
/// depends on which interface we are considering. For example, suppose we have a
/// Python library called `my_provider` that exposes a `TableProvider` called
/// `MyProvider` via `FFI_TableProvider`. Within the library `my_provider` we can
/// access the `private_data` via `FFI_TableProvider`. We connect this to
/// `datafusion-python`, where we access it as a `ForeignTableProvider`. Now when
/// we call `scan()` on this interface, we have to pass it a `FFI_SessionConfig`.
/// The `SessionConfig` is local to `datafusion-python` and **not** `my_provider`.
/// It is important to be careful when expanding these functions to be certain which
/// side of the interface each object refers to.
#[repr(C)]
#[derive(Debug, StableAbi)]
#[allow(non_camel_case_types)]
pub struct FFI_TableProvider {
    /// Return the table schema
    pub schema: unsafe extern "C" fn(provider: &Self) -> WrappedSchema,

    /// Perform a scan on the table. See [`TableProvider`] for detailed usage information.
    ///
    /// # Arguments
    ///
    /// * `provider` - the table provider
    /// * `session_config` - session configuration
    /// * `projections` - if specified, only a subset of the columns are returned
    /// * `filters_serialized` - filters to apply to the scan, which are a
    ///    [`LogicalExprList`] protobuf message serialized into bytes to pass
    ///    across the FFI boundary.
    /// * `limit` - if specified, limit the number of rows returned
    pub scan: unsafe extern "C" fn(
        provider: &Self,
        session_config: &FFI_SessionConfig,
        projections: RVec<usize>,
        filters_serialized: RVec<u8>,
        limit: ROption<usize>,
    ) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>>,

    /// Return the type of table. See [`TableType`] for options.
    pub table_type: unsafe extern "C" fn(provider: &Self) -> FFI_TableType,

    /// Based upon the input filters, identify which are supported. The filters
    /// are a [`LogicalExprList`] protobuf message serialized into bytes to pass
    /// across the FFI boundary.
    pub supports_filters_pushdown: Option<
        unsafe extern "C" fn(
            provider: &FFI_TableProvider,
            filters_serialized: RVec<u8>,
        )
            -> RResult<RVec<FFI_TableProviderFilterPushDown>, RString>,
    >,

    pub insert_into:
        unsafe extern "C" fn(
            provider: &Self,
            session_config: &FFI_SessionConfig,
            input: &FFI_ExecutionPlan,
            insert_op: FFI_InsertOp,
        ) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>>,

    /// Used to create a clone on the provider of the execution plan. This should
    /// only need to be called by the receiver of the plan.
    pub clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this provider.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignExecutionPlan`] should never attempt to access this data.
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn TableProvider + Send>,
    runtime: Option<Handle>,
}

unsafe extern "C" fn schema_fn_wrapper(provider: &FFI_TableProvider) -> WrappedSchema {
    let private_data = provider.private_data as *const ProviderPrivateData;
    let provider = &(*private_data).provider;

    provider.schema().into()
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
    let runtime = &(*private_data).runtime;

    async move {
        let config = rresult_return!(ForeignSessionConfig::try_from(&session_config));
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
                    rresult_return!(LogicalExprList::decode(filters_serialized.as_ref()));

                rresult_return!(parse_exprs(
                    proto_filters.expr.iter(),
                    &default_ctx,
                    &codec
                ))
            }
        };

        let projections: Vec<_> = projections.into_iter().collect();
        let maybe_projections = match projections.is_empty() {
            true => None,
            false => Some(&projections),
        };

        let plan = rresult_return!(
            internal_provider
                .scan(&ctx.state(), maybe_projections, &filters, limit.into())
                .await
        );

        RResult::ROk(FFI_ExecutionPlan::new(
            plan,
            ctx.task_ctx(),
            runtime.clone(),
        ))
    }
    .into_ffi()
}

unsafe extern "C" fn insert_into_fn_wrapper(
    provider: &FFI_TableProvider,
    session_config: &FFI_SessionConfig,
    input: &FFI_ExecutionPlan,
    insert_op: FFI_InsertOp,
) -> FfiFuture<RResult<FFI_ExecutionPlan, RString>> {
    let private_data = provider.private_data as *mut ProviderPrivateData;
    let internal_provider = &(*private_data).provider;
    let session_config = session_config.clone();
    let input = input.clone();
    let runtime = &(*private_data).runtime;

    async move {
        let config = rresult_return!(ForeignSessionConfig::try_from(&session_config));
        let session = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config.0)
            .build();
        let ctx = SessionContext::new_with_state(session);

        let input = rresult_return!(ForeignExecutionPlan::try_from(&input).map(Arc::new));

        let insert_op = InsertOp::from(insert_op);

        let plan = rresult_return!(
            internal_provider
                .insert_into(&ctx.state(), input, insert_op)
                .await
        );

        RResult::ROk(FFI_ExecutionPlan::new(
            plan,
            ctx.task_ctx(),
            runtime.clone(),
        ))
    }
    .into_ffi()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_TableProvider) {
    let private_data = Box::from_raw(provider.private_data as *mut ProviderPrivateData);
    drop(private_data);
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_TableProvider) -> FFI_TableProvider {
    let old_private_data = provider.private_data as *const ProviderPrivateData;
    let runtime = (*old_private_data).runtime.clone();

    let private_data = Box::into_raw(Box::new(ProviderPrivateData {
        provider: Arc::clone(&(*old_private_data).provider),
        runtime,
    })) as *mut c_void;

    FFI_TableProvider {
        schema: schema_fn_wrapper,
        scan: scan_fn_wrapper,
        table_type: table_type_fn_wrapper,
        supports_filters_pushdown: provider.supports_filters_pushdown,
        insert_into: provider.insert_into,
        clone: clone_fn_wrapper,
        release: release_fn_wrapper,
        version: super::version,
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
        runtime: Option<Handle>,
    ) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider, runtime });

        Self {
            schema: schema_fn_wrapper,
            scan: scan_fn_wrapper,
            table_type: table_type_fn_wrapper,
            supports_filters_pushdown: match can_support_pushdown_filters {
                true => Some(supports_filters_pushdown_fn_wrapper),
                false => None,
            },
            insert_into: insert_into_fn_wrapper,
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
/// FFI_TableProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignTableProvider(pub FFI_TableProvider);

unsafe impl Send for ForeignTableProvider {}
unsafe impl Sync for ForeignTableProvider {}

impl From<&FFI_TableProvider> for ForeignTableProvider {
    fn from(provider: &FFI_TableProvider) -> Self {
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
        let wrapped_schema = unsafe { (self.0.schema)(&self.0) };
        wrapped_schema.into()
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
        let session_config: FFI_SessionConfig = session.config().into();

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

            ForeignExecutionPlan::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(Arc::new(plan))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimize data retrieval.
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

            let pushdowns = df_result!(pushdown_fn(&self.0, serialized_filters.into()))?;

            Ok(pushdowns.iter().map(|v| v.into()).collect())
        }
    }

    async fn insert_into(
        &self,
        session: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let session_config: FFI_SessionConfig = session.config().into();

        let rc = Handle::try_current().ok();
        let input =
            FFI_ExecutionPlan::new(input, Arc::new(TaskContext::from(session)), rc);
        let insert_op: FFI_InsertOp = insert_op.into();

        let plan = unsafe {
            let maybe_plan =
                (self.0.insert_into)(&self.0, &session_config, &input, insert_op).await;

            ForeignExecutionPlan::try_from(&df_result!(maybe_plan)?)?
        };

        Ok(Arc::new(plan))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Schema;
    use datafusion::prelude::{col, lit};

    use super::*;

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_scan() -> Result<()> {
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

        let ffi_provider = FFI_TableProvider::new(provider, true, None);

        let foreign_table_provider: ForeignTableProvider = (&ffi_provider).into();

        ctx.register_table("t", Arc::new(foreign_table_provider))?;

        let df = ctx.table("t").await?;

        df.select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_round_trip_ffi_table_provider_insert_into() -> Result<()> {
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

        let ffi_provider = FFI_TableProvider::new(provider, true, None);

        let foreign_table_provider: ForeignTableProvider = (&ffi_provider).into();

        ctx.register_table("t", Arc::new(foreign_table_provider))?;

        let result = ctx
            .sql("INSERT INTO t VALUES (128.0);")
            .await?
            .collect()
            .await?;

        assert!(result.len() == 1 && result[0].num_rows() == 1);

        ctx.table("t")
            .await?
            .select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }
}
