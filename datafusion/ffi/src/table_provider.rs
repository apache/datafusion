use std::{
    any::Any,
    ffi::{c_char, c_int, c_void, CStr, CString},
    ptr::null_mut,
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
use futures::executor::block_on;

use super::{
    execution_plan::{ExportedExecutionPlan, FFI_ExecutionPlan},
    session_config::{FFI_SessionConfig, SessionConfigPrivateData},
};
use datafusion::error::Result;

#[repr(C)]
#[derive(Debug)]
#[allow(missing_docs)]
#[allow(non_camel_case_types)]
pub struct FFI_TableProvider {
    pub version: i64,
    pub schema: Option<unsafe extern "C" fn(provider: *const FFI_TableProvider) -> FFI_ArrowSchema>,
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
    pub private_data: *mut c_void,
}

unsafe impl Send for FFI_TableProvider {}
unsafe impl Sync for FFI_TableProvider {}

struct ProviderPrivateData {
    provider: Box<dyn TableProvider + Send>,
}

struct ExportedTableProvider(*const FFI_TableProvider);

// The callback used to get array schema
unsafe extern "C" fn provider_schema(provider: *const FFI_TableProvider) -> FFI_ArrowSchema {
    ExportedTableProvider(provider).provider_schema()
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
    println!("entered scan_fn_wrapper");
    let config = unsafe { (*session_config).private_data as *const SessionConfigPrivateData };
    let session = SessionStateBuilder::new()
        .with_config((*config).config.clone())
        .build();
    let ctx = SessionContext::new_with_state(session);

    let num_projections: usize = n_projections.try_into().unwrap_or(0);

    let projections: Vec<usize> = std::slice::from_raw_parts(projections, num_projections)
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

    let plan =
        ExportedTableProvider(provider).provider_scan(&ctx, maybe_projections, filters_vec, limit);

    println!("leaving scan_fn_wrapper, has plan? {}", plan.is_ok());

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

impl ExportedTableProvider {
    fn get_private_data(&self) -> &ProviderPrivateData {
        unsafe { &*((*self.0).private_data as *const ProviderPrivateData) }
    }

    pub fn provider_schema(&self) -> FFI_ArrowSchema {
        let private_data = self.get_private_data();
        let provider = &private_data.provider;

        // This does silently fail because TableProvider does not return a result
        // so we expect it to always pass. Maybe some logging should be added.
        FFI_ArrowSchema::try_from(provider.schema().as_ref()).unwrap_or(FFI_ArrowSchema::empty())
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
}

impl FFI_TableProvider {
    /// Creates a new [`FFI_TableProvider`].
    pub fn new(provider: Box<dyn TableProvider + Send>) -> Self {
        let private_data = Box::new(ProviderPrivateData { provider });

        Self {
            version: 2,
            schema: Some(provider_schema),
            scan: Some(scan_fn_wrapper),
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
            version: 0,
            schema: None,
            scan: None,
            private_data: std::ptr::null_mut(),
        }
    }
}

#[async_trait]
impl TableProvider for FFI_TableProvider {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        let schema = match self.schema {
            Some(func) => unsafe { Schema::try_from(&func(self)).ok() },
            None => None,
        };
        Arc::new(schema.unwrap_or(Schema::empty()))
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        todo!()
    }

    /// Create an ExecutionPlan that will scan the table.
    /// The table provider will be usually responsible of grouping
    /// the source data into partitions that can be efficiently
    /// parallelized or distributed.
    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let scan_fn = self.scan.ok_or(DataFusionError::NotImplemented(
            "Scan not defined on FFI_TableProvider".to_string(),
        ))?;

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

        println!("Within scan about to call unsafe scan_fn");
        let mut err_code = 0;
        let plan = unsafe {
            let plan_ptr = scan_fn(
                self,
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

            println!(
                "Finished scan_fn inside FFI_TableProvider::scan {}",
                plan_ptr.is_null()
            );

            let p = ExportedExecutionPlan::new(plan_ptr)?;
            println!("ExportedExecutionPlan::new returned inside scan()");
            p
        };
        println!("Scan returned with some plan.");

        Ok(Arc::new(plan))
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        todo!()
    }
}
