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

use std::{
    any::Any,
    ffi::{c_int, c_uint, c_void},
    ptr::{addr_of, null},
    slice,
    sync::Arc,
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    ffi::FFI_ArrowSchema,
};
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
#[derive(Debug)]
#[allow(non_camel_case_types)]
pub struct FFI_TableProvider {
    pub schema: Option<
        unsafe extern "C" fn(provider: *const FFI_TableProvider) -> FFI_ArrowSchema,
    >,
    pub scan: Option<
        unsafe extern "C" fn(
            provider: *const FFI_TableProvider,
            session_config: *const FFI_SessionConfig,
            n_projections: c_uint,
            projections: *const c_uint,
            filter_buffer_size: c_uint,
            filter_buffer: *const u8,
            limit: c_int,
            err_code: *mut c_int,
        ) -> FFI_ExecutionPlan,
    >,
    pub table_type:
        Option<unsafe extern "C" fn(provider: *const FFI_TableProvider) -> FFI_TableType>,

    pub supports_filters_pushdown: Option<
        unsafe extern "C" fn(
            provider: *const FFI_TableProvider,
            filter_buff_size: c_uint,
            filter_buffer: *const u8,
            num_filters: &mut c_int,
            out: &mut *const FFI_TableProviderFilterPushDown,
        ) -> c_int,
    >,

    pub clone: Option<unsafe extern "C" fn(plan: *const Self) -> Self>,
    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Arc<dyn TableProvider + Send>,
    last_filter_pushdowns: Vec<FFI_TableProviderFilterPushDown>,
}

/// Wrapper struct to provide access functions from the FFI interface to the underlying
/// TableProvider. This struct is allowed to access `private_data` because it lives on
/// the provider's side of the FFI interace.
struct ExportedTableProvider(*const FFI_TableProvider);

unsafe extern "C" fn schema_fn_wrapper(
    provider: *const FFI_TableProvider,
) -> FFI_ArrowSchema {
    ExportedTableProvider(provider).schema()
}

unsafe extern "C" fn table_type_fn_wrapper(
    provider: *const FFI_TableProvider,
) -> FFI_TableType {
    ExportedTableProvider(provider).table_type()
}

unsafe extern "C" fn supports_filters_pushdown_fn_wrapper(
    provider: *const FFI_TableProvider,
    filter_buff_size: c_uint,
    filter_buffer: *const u8,
    num_filters: &mut c_int,
    out: &mut *const FFI_TableProviderFilterPushDown,
) -> c_int {
    let results = ExportedTableProvider(provider)
        .supports_filters_pushdown(filter_buff_size, filter_buffer);

    match results {
        Ok((num_pushdowns, pushdowns_ptr)) => {
            *num_filters = num_pushdowns as c_int;
            std::ptr::copy(addr_of!(pushdowns_ptr), out, 1);
            0
        }
        Err(_e) => 1,
    }
}

unsafe extern "C" fn scan_fn_wrapper(
    provider: *const FFI_TableProvider,
    session_config: *const FFI_SessionConfig,
    n_projections: c_uint,
    projections: *const c_uint,
    filter_buffer_size: c_uint,
    filter_buffer: *const u8,
    limit: c_int,
    err_code: *mut c_int,
) -> FFI_ExecutionPlan {
    let config = match ForeignSessionConfig::new(session_config) {
        Ok(c) => c,
        Err(_) => {
            *err_code = 1;
            return FFI_ExecutionPlan::empty();
        }
    };
    let session = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config.0)
        .build();
    let ctx = SessionContext::new_with_state(session);

    let num_projections: usize = n_projections.try_into().unwrap_or(0);

    let projections: Vec<usize> =
        std::slice::from_raw_parts(projections, num_projections)
            .iter()
            .filter_map(|v| (*v).try_into().ok())
            .collect();
    let maybe_projections = match projections.is_empty() {
        true => None,
        false => Some(&projections),
    };

    let limit = limit.try_into().ok();

    let plan = ExportedTableProvider(provider).provider_scan(
        &ctx,
        maybe_projections,
        filter_buffer_size,
        filter_buffer,
        limit,
    );

    match plan {
        Ok(plan) => {
            *err_code = 0;
            plan
        }
        Err(_) => {
            *err_code = 1;
            FFI_ExecutionPlan::empty()
        }
    }
}

unsafe extern "C" fn clone_fn_wrapper(
    provider: *const FFI_TableProvider,
) -> FFI_TableProvider {
    if provider.is_null() {
        return FFI_TableProvider::empty();
    }

    let private_data = (*provider).private_data as *const ProviderPrivateData;
    let table_provider = unsafe { Arc::clone(&(*private_data).provider) };
    FFI_TableProvider::new(table_provider)
}

unsafe extern "C" fn release_fn_wrapper(provider: *mut FFI_TableProvider) {
    if provider.is_null() {
        return;
    }
    let provider = &mut *provider;

    provider.schema = None;
    provider.scan = None;
    provider.table_type = None;
    provider.supports_filters_pushdown = None;

    let private_data = Box::from_raw(provider.private_data as *mut ProviderPrivateData);

    drop(private_data);

    provider.release = None;
}

impl Drop for FFI_TableProvider {
    fn drop(&mut self) {
        match self.release {
            None => (),
            Some(release) => unsafe { release(self as *mut FFI_TableProvider) },
        };
    }
}

impl ExportedTableProvider {
    fn private_data(&self) -> &ProviderPrivateData {
        unsafe { &*((*self.0).private_data as *const ProviderPrivateData) }
    }

    fn mut_private_data(&mut self) -> &mut ProviderPrivateData {
        unsafe { &mut *((*self.0).private_data as *mut ProviderPrivateData) }
    }

    pub fn schema(&self) -> FFI_ArrowSchema {
        let private_data = self.private_data();
        let provider = &private_data.provider;

        // This does silently fail because TableProvider does not return a result
        // so we expect it to always pass.
        FFI_ArrowSchema::try_from(provider.schema().as_ref())
            .unwrap_or(FFI_ArrowSchema::empty())
    }

    pub fn table_type(&self) -> FFI_TableType {
        let private_data = self.private_data();
        let provider = &private_data.provider;

        provider.table_type().into()
    }

    pub fn provider_scan(
        &mut self,
        ctx: &SessionContext,
        projections: Option<&Vec<usize>>,
        filter_buffer_size: c_uint,
        filter_buffer: *const u8,
        limit: Option<usize>,
    ) -> Result<FFI_ExecutionPlan> {
        let private_data = self.private_data();
        let provider = &private_data.provider;

        let filters = match filter_buffer_size > 0 {
            false => vec![],
            true => {
                let default_ctx = SessionContext::new();
                let codec = DefaultLogicalExtensionCodec {};

                let data = unsafe {
                    slice::from_raw_parts(filter_buffer, filter_buffer_size as usize)
                };

                let proto_filters = LogicalExprList::decode(data)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                parse_exprs(proto_filters.expr.iter(), &default_ctx, &codec)?
            }
        };

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "Error getting runtime during scan(): {}",
                    e
                ))
            })?;
        let plan = runtime.block_on(provider.scan(
            &ctx.state(),
            projections,
            &filters,
            limit,
        ))?;

        FFI_ExecutionPlan::new(plan, ctx.task_ctx())
    }

    pub fn supports_filters_pushdown(
        &mut self,
        buffer_size: c_uint,
        filter_buffer: *const u8,
    ) -> Result<(usize, *const FFI_TableProviderFilterPushDown)> {
        unsafe {
            let default_ctx = SessionContext::new();
            let codec = DefaultLogicalExtensionCodec {};

            let filters = match buffer_size > 0 {
                false => vec![],
                true => {
                    let data = slice::from_raw_parts(filter_buffer, buffer_size as usize);

                    let proto_filters = LogicalExprList::decode(data)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    parse_exprs(proto_filters.expr.iter(), &default_ctx, &codec)?
                }
            };
            let filters_borrowed: Vec<&Expr> = filters.iter().collect();

            let private_data = self.mut_private_data();
            private_data.last_filter_pushdowns = private_data
                .provider
                .supports_filters_pushdown(&filters_borrowed)?
                .iter()
                .map(|v| v.into())
                .collect();

            Ok((
                private_data.last_filter_pushdowns.len(),
                private_data.last_filter_pushdowns.as_ptr(),
            ))
        }
    }
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(provider: Arc<dyn TableProvider + Send>) -> Self {
        let private_data = Box::new(ProviderPrivateData {
            provider,
            last_filter_pushdowns: Vec::default(),
        });

        Self {
            schema: Some(schema_fn_wrapper),
            scan: Some(scan_fn_wrapper),
            table_type: Some(table_type_fn_wrapper),
            supports_filters_pushdown: Some(supports_filters_pushdown_fn_wrapper),
            release: Some(release_fn_wrapper),
            clone: Some(clone_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    /// Create a FFI_TableProvider from a raw pointer
    ///
    /// # Safety
    ///
    /// This function assumes the raw pointer is valid and takes onwership of it.
    pub unsafe fn from_raw(raw_provider: *mut FFI_TableProvider) -> Self {
        std::ptr::replace(raw_provider, Self::empty())
    }

    /// Creates a new empty [FFI_ArrowArrayStream]. Used to import from the C Stream Interface.
    pub fn empty() -> Self {
        Self {
            schema: None,
            scan: None,
            table_type: None,
            supports_filters_pushdown: None,
            release: None,
            clone: None,
            private_data: std::ptr::null_mut(),
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
    pub fn new(provider: FFI_TableProvider) -> Self {
        Self(provider)
    }
}

#[async_trait]
impl TableProvider for ForeignTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = unsafe {
            let schema_fn = self.0.schema;
            schema_fn
                .map(|func| func(&self.0))
                .and_then(|s| Schema::try_from(&s).ok())
                .unwrap_or(Schema::empty())
        };
        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        unsafe {
            let table_type_fn = self.0.table_type;
            table_type_fn
                .map(|func| func(&self.0))
                .unwrap_or(FFI_TableType::Base)
                .into()
        }
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan_fn = self.0.scan.ok_or(DataFusionError::NotImplemented(
            "Scan not defined on FFI_TableProvider".to_string(),
        ))?;

        let session_config = FFI_SessionConfig::new(session.config());

        let n_projections = projection.map(|p| p.len()).unwrap_or(0) as c_uint;
        let projections: Vec<c_uint> = projection
            .map(|p| p.iter().map(|v| *v as c_uint).collect())
            .unwrap_or_default();
        let projections_ptr = projections.as_ptr();

        let codec = DefaultLogicalExtensionCodec {};
        let filter_list = LogicalExprList {
            expr: serialize_exprs(filters, &codec)?,
        };
        let filter_bytes = filter_list.encode_to_vec();
        let filter_buffer_size = filter_bytes.len() as c_uint;
        let filter_buffer = filter_bytes.as_ptr();

        let limit = match limit {
            Some(l) => l as c_int,
            None => -1,
        };

        let mut err_code = 0;
        let plan = unsafe {
            let plan_ptr = scan_fn(
                &self.0,
                &session_config,
                n_projections,
                projections_ptr,
                filter_buffer_size,
                filter_buffer,
                limit,
                &mut err_code,
            );

            if 0 != err_code {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Unable to perform scan via FFI".to_string(),
                ));
            }

            ForeignExecutionPlan::new(plan_ptr)?
        };

        Ok(Arc::new(plan))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        unsafe {
            let codec = DefaultLogicalExtensionCodec {};

            let expr_list = LogicalExprList {
                expr: serialize_exprs(filter.iter().map(|f| f.to_owned()), &codec)?,
            };
            let expr_bytes = expr_list.encode_to_vec();
            let buffer_size = expr_bytes.len();
            let buffer = expr_bytes.as_ptr();

            let pushdown_fn =
                self.0
                    .supports_filters_pushdown
                    .ok_or(DataFusionError::Plan(
                        "FFI_TableProvider does not implement supports_filters_pushdown"
                            .to_string(),
                    ))?;
            let mut num_return = 0;
            let mut pushdowns = null();
            let err_code = pushdown_fn(
                &self.0,
                buffer_size as c_uint,
                buffer,
                &mut num_return,
                &mut pushdowns,
            );
            let num_return = num_return as usize;
            let pushdowns_slice = slice::from_raw_parts(pushdowns, num_return);

            match err_code {
                0 => Ok(pushdowns_slice.iter().map(|v| v.into()).collect()),
                _ => Err(DataFusionError::Plan(
                    "Error occurred during FFI call to supports_filters_pushdown"
                        .to_string(),
                )),
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

        let ffi_provider = FFI_TableProvider::new(provider);

        let foreign_table_provider = ForeignTableProvider::new(ffi_provider);

        ctx.register_table("t", Arc::new(foreign_table_provider))?;

        let df = ctx.table("t").await?;

        df.select(vec![col("a")])?
            .filter(col("a").gt(lit(3.0)))?
            .show()
            .await?;

        Ok(())
    }
}
