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
    ffi::{c_char, c_int, c_void, CStr, CString},
    ptr::null_mut,
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
    common::DFSchema,
    datasource::TableType,
    error::DataFusionError,
    execution::session_state::SessionStateBuilder,
    logical_expr::TableProviderFilterPushDown,
    physical_plan::ExecutionPlan,
    prelude::{Expr, SessionContext},
};
use datafusion_proto::{
    logical_plan::{from_proto::parse_exprs, to_proto::serialize_exprs, DefaultLogicalExtensionCodec},
    protobuf::LogicalExprList,
};
use futures::executor::block_on;
use prost::Message;

use crate::{session_config::ExportedSessionConfig, table_source::{FFI_TableProviderFilterPushDown, FFI_TableType}};

use super::{
    execution_plan::{ExportedExecutionPlan, FFI_ExecutionPlan},
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
            n_projections: c_int,
            projections: *const c_int,
            n_filters: c_int,
            filters: *const *const c_char,
            limit: c_int,
            err_code: *mut c_int,
        ) -> *mut FFI_ExecutionPlan,
    >,
    pub table_type:
        Option<unsafe extern "C" fn(provider: *const FFI_TableProvider) -> FFI_TableType>,

    pub supports_filters_pushdown: Option<
        unsafe extern "C" fn(
            provider: *const FFI_TableProvider,
            filter_buff_size: c_int,
            filter_buffer: *const u8,
            num_filters: &mut c_int,
            out: *mut FFI_TableProviderFilterPushDown,
        ) -> c_int,
    >,

    pub release: Option<unsafe extern "C" fn(arg: *mut Self)>,
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Box<dyn TableProvider + Send>,
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
    filter_buff_size: c_int,
    filter_buffer: *const u8,
    num_filters: &mut c_int,
    out: *mut FFI_TableProviderFilterPushDown,
) -> c_int {
    let results = ExportedTableProvider(provider)
        .supports_filters_pushdown(filter_buff_size, filter_buffer);

    match results {
        Ok(pushdowns) => {
            *num_filters = pushdowns.len() as c_int;
            std::ptr::copy(pushdowns.as_ptr(), out, 1);
            std::mem::forget(pushdowns);
            0
        }
        Err(_e) => 1,
    }
}

unsafe extern "C" fn scan_fn_wrapper(
    provider: *const FFI_TableProvider,
    session_config: *const FFI_SessionConfig,
    n_projections: c_int,
    projections: *const c_int,
    n_filters: c_int,
    filters: *const *const c_char,
    limit: c_int,
    err_code: *mut c_int,
) -> *mut FFI_ExecutionPlan {
    let config = ExportedSessionConfig(session_config).session_config().clone();
    let session = SessionStateBuilder::new()
        .with_config(config)
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

    let filters_slice = std::slice::from_raw_parts(filters, n_filters as usize);
    let filters_vec: Vec<String> = filters_slice
        .iter()
        .map(|&s| CStr::from_ptr(s).to_string_lossy().to_string())
        .collect();

    let limit = limit.try_into().ok();

    let plan = ExportedTableProvider(provider).provider_scan(
        &ctx,
        maybe_projections,
        filters_vec,
        limit,
    );

    match plan {
        Ok(plan) => {
            *err_code = 0;
            plan
        }
        Err(_) => {
            *err_code = 1;
            null_mut()
        }
    }
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
            Some(release) => unsafe { release(self) },
        };
    }
}

impl ExportedTableProvider {
    fn get_private_data(&self) -> &ProviderPrivateData {
        unsafe { &*((*self.0).private_data as *const ProviderPrivateData) }
    }

    pub fn schema(&self) -> FFI_ArrowSchema {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        // This does silently fail because TableProvider does not return a result
        // so we expect it to always pass.
        FFI_ArrowSchema::try_from(provider.schema().as_ref())
            .unwrap_or(FFI_ArrowSchema::empty())
    }

    pub fn table_type(&self) -> FFI_TableType {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        provider.table_type().into()
    }

    pub fn provider_scan(
        &mut self,
        ctx: &SessionContext,
        projections: Option<&Vec<usize>>,
        filters: Vec<String>,
        limit: Option<usize>,
    ) -> Result<*mut FFI_ExecutionPlan> {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        let schema = provider.schema();
        let df_schema: DFSchema = schema.try_into()?;

        let filter_exprs = filters
            .into_iter()
            .map(|expr_str| ctx.state().create_logical_expr(&expr_str, &df_schema))
            .collect::<datafusion::common::Result<Vec<Expr>>>()?;

        let plan =
            block_on(provider.scan(&ctx.state(), projections, &filter_exprs, limit))?;

        let plan_boxed = Box::new(FFI_ExecutionPlan::new(plan, ctx.task_ctx()));
        Ok(Box::into_raw(plan_boxed))
    }

    pub fn supports_filters_pushdown(
        &self,
        buffer_size: c_int,
        filter_buffer: *const u8,
    ) -> Result<Vec<FFI_TableProviderFilterPushDown>> {
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

            let private_data = self.get_private_data();
            let provider = &private_data.provider;

            provider
                .supports_filters_pushdown(&filters_borrowed)
                .map(|f| f.iter().map(|v| v.into()).collect())
        }
    }
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(provider: Box<dyn TableProvider + Send>) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider });

        Self {
            schema: Some(schema_fn_wrapper),
            scan: Some(scan_fn_wrapper),
            table_type: Some(table_type_fn_wrapper),
            supports_filters_pushdown: Some(supports_filters_pushdown_fn_wrapper),
            release: Some(release_fn_wrapper),
            private_data: Box::into_raw(private_data) as *mut c_void,
        }
    }

    /**
        Replace temporary pointer with updated
        # Safety
        User must validate the raw pointer is valid.
    */
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
            private_data: std::ptr::null_mut(),
        }
    }
}

/// This wrapper struct exists on the reciever side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must only use the stable functions provided in
/// FFI_TableProvider to interact with the foreign table provider.
#[derive(Debug)]
pub struct ForeignTableProvider(pub *const FFI_TableProvider);

unsafe impl Send for ForeignTableProvider {}
unsafe impl Sync for ForeignTableProvider {}

#[async_trait]
impl TableProvider for ForeignTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let schema = unsafe {
            let schema_fn = (*self.0).schema;
            schema_fn
                .map(|func| func(self.0))
                .and_then(|s| Schema::try_from(&s).ok())
                .unwrap_or(Schema::empty())
        };

        Arc::new(schema)
    }

    fn table_type(&self) -> TableType {
        unsafe {
            let table_type_fn = (*self.0).table_type;
            table_type_fn
                .map(|func| func(self.0))
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
        let scan_fn = unsafe {
            (*self.0).scan.ok_or(DataFusionError::NotImplemented(
                "Scan not defined on FFI_TableProvider".to_string(),
            ))?
        };

        let session_config = FFI_SessionConfig::new(session);

        let n_projections = projection.map(|p| p.len()).unwrap_or(0) as c_int;
        let projections: Vec<c_int> = projection
            .map(|p| p.iter().map(|v| *v as c_int).collect())
            .unwrap_or_default();
        let projections_ptr = projections.as_ptr();

        let n_filters = filters.len() as c_int;
        let filters: Vec<CString> = filters
            .iter()
            .filter_map(|f| CString::new(f.to_string()).ok())
            .collect();
        let filters_ptr: Vec<*const i8> = filters.iter().map(|s| s.as_ptr()).collect();

        let limit = match limit {
            Some(l) => l as c_int,
            None => -1,
        };

        let mut err_code = 0;
        let plan = unsafe {
            let plan_ptr = scan_fn(
                self.0,
                &session_config,
                n_projections,
                projections_ptr,
                n_filters,
                filters_ptr.as_ptr(),
                limit,
                &mut err_code,
            );

            if 0 != err_code {
                return Err(datafusion::error::DataFusionError::Internal(
                    "Unable to perform scan via FFI".to_string(),
                ));
            }

            ExportedExecutionPlan::new(plan_ptr)?
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
                expr: serialize_exprs(filter.iter().map(|f| f.to_owned()), &codec)?
            };
            let expr_bytes = expr_list.encode_to_vec();
            let buffer_size = expr_bytes.len();
            let buffer = expr_bytes.as_ptr();

            let pushdown_fn = (*self.0).supports_filters_pushdown.ok_or(DataFusionError::Plan("FFI_TableProvider does not implement supports_filters_pushdown".to_string()))?;
            let mut num_return = 0;
            let pushdowns = null_mut();
            let err_code = pushdown_fn(self.0, buffer_size as c_int, buffer, &mut num_return, pushdowns);
            let num_return = num_return as usize;
            let pushdowns_slice = Vec::from_raw_parts(pushdowns, num_return, num_return);

            match err_code {
                0 => {
                    Ok(pushdowns_slice.iter().map(|v| v.into()).collect())
                }
                _ => {
                    Err(DataFusionError::Plan("Error occurred during FFI call to supports_filters_pushdown".to_string()))
                }
            }
        }
    }
}
