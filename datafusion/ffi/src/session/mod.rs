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

//! FFI support for [`Session`].
//!
//! # Delegating physical planning
//!
//! Consider a session owned by library A that uses a query planner owned by
//! library C. After A installs C's planner, [`ForeignSession::query_planner`]
//! returns C's planner. Invoking it to delegate planning back to A is a direct
//! self-call. [`ForeignSession::create_physical_plan`] is deliberately
//! unsupported because dispatching through A's session would likewise re-enter
//! C's planner.
//!
//! To delegate safely, A must export its original planner before installing C's
//! planner, and C must retain and invoke that planner directly. See the
//! [`crate::query_planner`] module for details.

use std::any::Any;
use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, OnceLock};

use arrow_schema::SchemaRef;
use arrow_schema::ffi::FFI_ArrowSchema;
use async_ffi::{FfiFuture, FutureExt};
use async_trait::async_trait;
use datafusion_common::config::{ConfigFileType, ConfigOptions, TableOptions};
use datafusion_common::{DFSchema, DataFusionError, not_impl_err};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::registry::{ExtensionTypeRegistryRef, MemoryExtensionTypeRegistry};
use datafusion_expr::{
    AggregateUDF, AggregateUDFImpl, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF,
    ScalarUDFImpl, WindowUDF, WindowUDFImpl,
};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_proto::bytes::{
    logical_plan_from_bytes_with_extension_codec,
    logical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::logical_plan::from_proto::parse_expr;
use datafusion_proto::logical_plan::to_proto::serialize_expr;
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use datafusion_proto::protobuf::LogicalExprNode;
use datafusion_session::{
    CatalogProviderList, PhysicalOptimizerRule, QueryPlanner, Session,
};
use prost::Message;

use stabby::str::Str as SStr;
use stabby::string::String as SString;
use stabby::vec::Vec as SVec;
use tokio::runtime::Handle;

use crate::arrow_wrappers::WrappedSchema;
use crate::catalog_provider_list::FFI_CatalogProviderList;
use crate::execution::FFI_TaskContext;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::physical_expr::FFI_PhysicalExpr;
use crate::physical_optimizer::FFI_PhysicalOptimizerRule;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use crate::query_planner::FFI_QueryPlanner;
use crate::session::config::FFI_SessionConfig;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFI_Result;
use crate::{df_result, sresult, sresult_return};

pub mod config;

/// A stable struct for sharing [`Session`] across FFI boundaries.
///
/// Care must be taken when using this struct. Unlike most of the structs in
/// this crate, the private data for [`FFI_SessionRef`] contains borrowed data.
/// The lifetime of the borrow is lost when hidden within the ``*mut c_void``
/// of the private data. For this reason, it is the user's responsibility to
/// ensure the lifetime of the [`Session`] remains valid.
///
/// The reason for storing `&dyn Session` is because the primary motivation
/// for implementing this struct is [`crate::table_provider::FFI_TableProvider`]
/// which has methods that require `&dyn Session`. For usage within this crate
/// we know the [`Session`] lifetimes are valid.
#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI_SessionRef {
    session_id: unsafe extern "C" fn(&Self) -> SStr,

    config: unsafe extern "C" fn(&Self) -> FFI_SessionConfig,

    catalog_list: unsafe extern "C" fn(&Self) -> FFI_CatalogProviderList,

    query_planner: unsafe extern "C" fn(&Self) -> FFI_QueryPlanner,

    optimize: unsafe extern "C" fn(
        &Self,
        logical_plan_serialized: SVec<u8>,
    ) -> FFI_Result<SVec<u8>>,

    /// Retained at its original position for ABI compatibility with consumers
    /// compiled against DataFusion 55. Direct session planning is unsupported.
    create_physical_plan:
        unsafe extern "C" fn(
            &Self,
            logical_plan_serialized: SVec<u8>,
        ) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>>,

    create_physical_expr: unsafe extern "C" fn(
        &Self,
        expr_serialized: SVec<u8>,
        schema: WrappedSchema,
    ) -> FFI_Result<FFI_PhysicalExpr>,

    scalar_functions: unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_ScalarUDF)>,

    aggregate_functions: unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_AggregateUDF)>,

    window_functions: unsafe extern "C" fn(&Self) -> SVec<(SString, FFI_WindowUDF)>,

    table_options: unsafe extern "C" fn(&Self) -> SVec<(SString, SString)>,

    default_table_options: unsafe extern "C" fn(&Self) -> SVec<(SString, SString)>,

    task_ctx: unsafe extern "C" fn(&Self) -> FFI_TaskContext,

    physical_optimizers: unsafe extern "C" fn(&Self) -> SVec<FFI_PhysicalOptimizerRule>,

    logical_codec: FFI_LogicalExtensionCodec,

    physical_codec: FFI_PhysicalExtensionCodec,

    /// Used to create a clone on the provider of the registry. This should
    /// only need to be called by the receiver of the plan.
    clone: unsafe extern "C" fn(plan: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    release: unsafe extern "C" fn(arg: &mut Self),

    /// Return the major DataFusion version number of this registry.
    pub version: unsafe extern "C" fn() -> u64,

    /// Internal data. This is only to be accessed by the provider of the plan.
    /// A [`ForeignSession`] should never attempt to access this data.
    private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_SessionRef {}
unsafe impl Sync for FFI_SessionRef {}

struct SessionPrivateData<'a> {
    session: &'a dyn Session,
    runtime: Option<Handle>,
}

impl FFI_SessionRef {
    fn inner(&self) -> &dyn Session {
        let private_data = self.private_data as *const SessionPrivateData;
        unsafe { (*private_data).session }
    }

    unsafe fn runtime(&self) -> Option<&Handle> {
        unsafe {
            let private_data = self.private_data as *const SessionPrivateData;
            (*private_data).runtime.as_ref()
        }
    }
}

unsafe extern "C" fn session_id_fn_wrapper(session: &FFI_SessionRef) -> SStr<'_> {
    let session = session.inner();
    session.session_id().into()
}

unsafe extern "C" fn config_fn_wrapper(session: &FFI_SessionRef) -> FFI_SessionConfig {
    let session = session.inner();
    session.config().into()
}

unsafe extern "C" fn catalog_list_fn_wrapper(
    session: &FFI_SessionRef,
) -> FFI_CatalogProviderList {
    FFI_CatalogProviderList::new_with_ffi_codec(
        session.inner().catalog_list(),
        unsafe { session.runtime() }.cloned(),
        session.logical_codec.clone(),
    )
}

unsafe extern "C" fn query_planner_fn_wrapper(
    session: &FFI_SessionRef,
) -> FFI_QueryPlanner {
    FFI_QueryPlanner::new_with_ffi_codecs(
        session.inner().query_planner(),
        session.logical_codec.clone(),
        session.physical_codec.clone(),
    )
}

unsafe extern "C" fn optimize_fn_wrapper(
    session: &FFI_SessionRef,
    logical_plan_serialized: SVec<u8>,
) -> FFI_Result<SVec<u8>> {
    let logical_codec: Arc<dyn LogicalExtensionCodec> = (&session.logical_codec).into();
    let inner = session.inner();
    let logical_plan = sresult_return!(logical_plan_from_bytes_with_extension_codec(
        logical_plan_serialized.as_slice(),
        inner.task_ctx().as_ref(),
        logical_codec.as_ref(),
    ));
    let optimized_plan = sresult_return!(inner.optimize(&logical_plan));
    let optimized_plan = sresult_return!(logical_plan_to_bytes_with_extension_codec(
        &optimized_plan,
        logical_codec.as_ref(),
    ));

    FFI_Result::Ok(SVec::from(optimized_plan.as_ref()))
}

unsafe extern "C" fn create_physical_plan_fn_wrapper(
    _session: &FFI_SessionRef,
    _logical_plan_serialized: SVec<u8>,
) -> FfiFuture<FFI_Result<FFI_ExecutionPlan>> {
    async move {
        sresult!(not_impl_err!(
            "FFI_SessionRef::create_physical_plan is unsupported; export and invoke an FFI_QueryPlanner captured before installing a foreign planner"
        ))
    }
    .into_ffi()
}

unsafe extern "C" fn create_physical_expr_fn_wrapper(
    session: &FFI_SessionRef,
    expr_serialized: SVec<u8>,
    schema: WrappedSchema,
) -> FFI_Result<FFI_PhysicalExpr> {
    let codec: Arc<dyn LogicalExtensionCodec> = (&session.logical_codec).into();
    let session = session.inner();

    let logical_expr = LogicalExprNode::decode(expr_serialized.as_slice()).unwrap();
    let logical_expr =
        parse_expr(&logical_expr, session.task_ctx().as_ref(), codec.as_ref()).unwrap();
    let schema: SchemaRef = schema.into();
    let schema: DFSchema = sresult_return!(schema.try_into());

    let physical_expr =
        sresult_return!(session.create_physical_expr(logical_expr, &schema));

    FFI_Result::Ok(physical_expr.into())
}

unsafe extern "C" fn scalar_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<(SString, FFI_ScalarUDF)> {
    let session = session.inner();
    session
        .scalar_functions()
        .iter()
        .map(|(name, udf)| (name.clone().into(), FFI_ScalarUDF::from(Arc::clone(udf))))
        .collect()
}

unsafe extern "C" fn aggregate_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<(SString, FFI_AggregateUDF)> {
    let session = session.inner();
    session
        .aggregate_functions()
        .iter()
        .map(|(name, udaf)| {
            (
                name.clone().into(),
                FFI_AggregateUDF::from(Arc::clone(udaf)),
            )
        })
        .collect()
}

unsafe extern "C" fn window_functions_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<(SString, FFI_WindowUDF)> {
    let session = session.inner();
    session
        .window_functions()
        .iter()
        .map(|(name, udwf)| (name.clone().into(), FFI_WindowUDF::from(Arc::clone(udwf))))
        .collect()
}

fn table_options_to_rhash(mut options: TableOptions) -> SVec<(SString, SString)> {
    // It is important that we mutate options here and set current format
    // to None so that when we call `entries()` we get ALL format entries.
    // We will pass current_format as a special case and strip it on the
    // other side of the boundary.
    let current_format = options.current_format.take();
    let mut options: HashMap<SString, SString> = options
        .entries()
        .into_iter()
        .filter_map(|entry| entry.value.map(|v| (entry.key.into(), v.into())))
        .collect();
    if let Some(current_format) = current_format {
        options.insert(
            "datafusion_ffi.table_current_format".into(),
            match current_format {
                ConfigFileType::JSON => "json",
                #[cfg(feature = "parquet")]
                ConfigFileType::PARQUET => "parquet",
                ConfigFileType::CSV => "csv",
            }
            .into(),
        );
    }

    options.into_iter().collect()
}

unsafe extern "C" fn table_options_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<(SString, SString)> {
    let session = session.inner();
    let table_options = session.table_options();
    table_options_to_rhash(table_options.clone())
}

unsafe extern "C" fn default_table_options_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<(SString, SString)> {
    let session = session.inner();
    let table_options = session.default_table_options();

    table_options_to_rhash(table_options)
}

unsafe extern "C" fn task_ctx_fn_wrapper(session: &FFI_SessionRef) -> FFI_TaskContext {
    session.inner().task_ctx().into()
}

unsafe extern "C" fn physical_optimizers_fn_wrapper(
    session: &FFI_SessionRef,
) -> SVec<FFI_PhysicalOptimizerRule> {
    let runtime = unsafe { session.runtime().cloned() };
    session
        .inner()
        .physical_optimizers()
        .iter()
        .map(|rule| FFI_PhysicalOptimizerRule::new(Arc::clone(rule), runtime.clone()))
        .collect()
}

unsafe extern "C" fn release_fn_wrapper(provider: &mut FFI_SessionRef) {
    unsafe {
        let private_data =
            Box::from_raw(provider.private_data.cast::<SessionPrivateData>());
        drop(private_data);
    }
}

unsafe extern "C" fn clone_fn_wrapper(provider: &FFI_SessionRef) -> FFI_SessionRef {
    unsafe {
        let old_private_data = provider.private_data as *const SessionPrivateData;

        let private_data = Box::into_raw(Box::new(SessionPrivateData {
            session: (*old_private_data).session,
            runtime: (*old_private_data).runtime.clone(),
        }))
        .cast::<c_void>();

        FFI_SessionRef {
            session_id: session_id_fn_wrapper,
            config: config_fn_wrapper,
            catalog_list: catalog_list_fn_wrapper,
            query_planner: query_planner_fn_wrapper,
            optimize: optimize_fn_wrapper,
            create_physical_plan: create_physical_plan_fn_wrapper,
            create_physical_expr: create_physical_expr_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            table_options: table_options_fn_wrapper,
            default_table_options: default_table_options_fn_wrapper,
            task_ctx: task_ctx_fn_wrapper,
            physical_optimizers: physical_optimizers_fn_wrapper,
            logical_codec: provider.logical_codec.clone(),
            physical_codec: provider.physical_codec.clone(),

            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data,
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

impl Drop for FFI_SessionRef {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

impl FFI_SessionRef {
    /// Creates a new [`FFI_SessionRef`] with a default physical extension codec.
    ///
    /// The synthesized [`DefaultPhysicalExtensionCodec`] supports built-in physical
    /// nodes only. A query planner obtained through this session reference therefore
    /// cannot encode or decode custom physical extension nodes. Use
    /// [`Self::new_with_ffi_codecs`] with matching logical and physical codecs when
    /// custom physical nodes must cross the FFI boundary.
    ///
    /// The physical codec wrapper requires a
    /// [`FFI_TaskContextProvider`](crate::execution::FFI_TaskContextProvider), but this
    /// constructor has only a session reference and a logical codec. It therefore
    /// reuses the logical codec's provider. The provider may be owned by another
    /// library; this is safe, but it must remain live and return the task context
    /// intended for codec callbacks. The default physical codec does not successfully
    /// decode extension nodes, so callers that need such callbacks must instead use
    /// [`Self::new_with_ffi_codecs`] with an explicitly configured physical codec and
    /// task context provider.
    pub fn new(
        session: &dyn Session,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
    ) -> Self {
        // `Session` provides a TaskContext but not the reference-counted
        // TaskContextProvider needed by the FFI codec. Reuse the provider associated
        // with the logical codec under the assumptions documented above.
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(DefaultPhysicalExtensionCodec {}),
            runtime.clone(),
            logical_codec.task_ctx_provider.clone(),
        );
        Self::new_with_ffi_codecs(session, runtime, logical_codec, physical_codec)
    }

    /// Creates a new [`FFI_SessionRef`] using existing FFI codecs.
    ///
    /// The codecs must form a matching pair that can round-trip every logical and
    /// physical extension node exposed through the session. Their task context
    /// providers must remain live and return contexts appropriate for their decode
    /// callbacks.
    ///
    /// If `session` is already foreign, this re-exports its original FFI handle
    /// rather than adding another wrapper layer. The handle adopts the codecs
    /// supplied here while retaining its original private data and runtime.
    pub fn new_with_ffi_codecs(
        session: &dyn Session,
        runtime: Option<Handle>,
        logical_codec: FFI_LogicalExtensionCodec,
        physical_codec: FFI_PhysicalExtensionCodec,
    ) -> Self {
        if let Some(session) = session.as_any().downcast_ref::<ForeignSession>() {
            let mut session = session.session.clone();
            session.logical_codec = logical_codec;
            session.physical_codec = physical_codec;
            return session;
        }

        let private_data = Box::new(SessionPrivateData { session, runtime });

        Self {
            session_id: session_id_fn_wrapper,
            config: config_fn_wrapper,
            catalog_list: catalog_list_fn_wrapper,
            query_planner: query_planner_fn_wrapper,
            optimize: optimize_fn_wrapper,
            create_physical_plan: create_physical_plan_fn_wrapper,
            create_physical_expr: create_physical_expr_fn_wrapper,
            scalar_functions: scalar_functions_fn_wrapper,
            aggregate_functions: aggregate_functions_fn_wrapper,
            window_functions: window_functions_fn_wrapper,
            table_options: table_options_fn_wrapper,
            default_table_options: default_table_options_fn_wrapper,
            task_ctx: task_ctx_fn_wrapper,
            physical_optimizers: physical_optimizers_fn_wrapper,
            logical_codec,
            physical_codec,

            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            version: super::version,
            private_data: Box::into_raw(private_data).cast::<c_void>(),
            library_marker_id: crate::get_library_marker_id,
        }
    }
}

/// This wrapper struct exists on the receiver side of the FFI interface, so it has
/// no guarantees about being able to access the data in `private_data`. Any functions
/// defined on this struct must use only the stable function pointers in
/// `FFI_SessionRef` to interact with the foreign session.
///
/// # Query planner delegation
///
/// If the session owner installed the current foreign query planner,
/// [`Session::query_planner`] returns that planner. The planner must retain and
/// invoke the session owner's previous planner rather than delegate back through
/// the session. [`Session::create_physical_plan`] returns an error because such
/// delegation would re-enter the installed planner. See [`crate::query_planner`]
/// for details.
#[derive(Debug)]
pub struct ForeignSession {
    session: FFI_SessionRef,
    config: SessionConfig,
    catalog_list: Arc<dyn CatalogProviderList>,
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    higher_order_functions: HashMap<String, Arc<HigherOrderUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    extension_types: ExtensionTypeRegistryRef,
    table_options: TableOptions,
    runtime_env: Arc<RuntimeEnv>,
    props: ExecutionProps,
    query_planner: OnceLock<Arc<dyn QueryPlanner + Send + Sync>>,
    physical_optimizers: OnceLock<Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>>,
}

unsafe impl Send for ForeignSession {}
unsafe impl Sync for ForeignSession {}

impl FFI_SessionRef {
    pub fn as_local(&self) -> Option<&dyn Session> {
        if (self.library_marker_id)() == crate::get_library_marker_id() {
            return Some(self.inner());
        }
        None
    }
}

impl TryFrom<&FFI_SessionRef> for ForeignSession {
    type Error = DataFusionError;
    fn try_from(session: &FFI_SessionRef) -> Result<Self, Self::Error> {
        unsafe {
            let table_options =
                table_options_from_rhashmap((session.table_options)(session));

            let config = (session.config)(session);
            let config = SessionConfig::try_from(&config)?;

            let ffi_catalog_list = (session.catalog_list)(session);
            let catalog_list = (&ffi_catalog_list).into();

            let scalar_functions = (session.scalar_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udf = <Arc<dyn ScalarUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.to_string(),
                        Arc::new(ScalarUDF::new_from_shared_impl(udf)),
                    )
                })
                .collect();
            let aggregate_functions = (session.aggregate_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udaf = <Arc<dyn AggregateUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.to_string(),
                        Arc::new(AggregateUDF::new_from_shared_impl(udaf)),
                    )
                })
                .collect();
            let window_functions = (session.window_functions)(session)
                .into_iter()
                .map(|kv_pair| {
                    let udwf = <Arc<dyn WindowUDFImpl>>::from(&kv_pair.1);

                    (
                        kv_pair.0.to_string(),
                        Arc::new(WindowUDF::new_from_shared_impl(udwf)),
                    )
                })
                .collect();
            Ok(Self {
                session: session.clone(),
                config,
                catalog_list,
                table_options,
                scalar_functions,
                higher_order_functions: HashMap::new(),
                aggregate_functions,
                window_functions,
                extension_types: Arc::new(MemoryExtensionTypeRegistry::default()),
                runtime_env: Default::default(),
                props: Default::default(),
                query_planner: OnceLock::new(),
                physical_optimizers: OnceLock::new(),
            })
        }
    }
}

impl Clone for FFI_SessionRef {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

fn table_options_from_rhashmap(options: SVec<(SString, SString)>) -> TableOptions {
    let mut options: HashMap<String, String> = options
        .into_iter()
        .map(|kv_pair| (kv_pair.0.to_string(), kv_pair.1.to_string()))
        .collect();
    let current_format = options.remove("datafusion_ffi.table_current_format");

    let mut table_options = TableOptions::default();
    let formats = [
        ConfigFileType::CSV,
        ConfigFileType::JSON,
        #[cfg(feature = "parquet")]
        ConfigFileType::PARQUET,
    ];
    for format in formats {
        // It is imperative that if new enum variants are added below that they be
        // included in the formats list above and in the extension check below.
        let format_name = match &format {
            ConfigFileType::CSV => "csv",
            #[cfg(feature = "parquet")]
            ConfigFileType::PARQUET => "parquet",
            ConfigFileType::JSON => "json",
        };
        let format_options: HashMap<String, String> = options
            .iter()
            .filter_map(|(k, v)| {
                let (prefix, key) = k.split_once('.')?;
                if prefix == format_name {
                    Some((format!("format.{key}"), v.to_owned()))
                } else {
                    None
                }
            })
            .collect();
        if !format_options.is_empty() {
            table_options.current_format = Some(format.clone());
            table_options
                .alter_with_string_hash_map(&format_options)
                .unwrap_or_else(|err| log::warn!("Error parsing table options: {err}"));
        }
    }
    let extension_options: HashMap<String, String> = options
        .iter()
        .filter_map(|(k, v)| {
            let (prefix, _) = k.split_once('.')?;
            if !["json", "parquet", "csv"].contains(&prefix) {
                Some((k.to_owned(), v.to_owned()))
            } else {
                None
            }
        })
        .collect();
    if !extension_options.is_empty() {
        table_options
            .alter_with_string_hash_map(&extension_options)
            .unwrap_or_else(|err| log::warn!("Error parsing table options: {err}"));
    }

    table_options.current_format =
        current_format.and_then(|format| match format.as_str() {
            "csv" => Some(ConfigFileType::CSV),
            #[cfg(feature = "parquet")]
            "parquet" => Some(ConfigFileType::PARQUET),
            "json" => Some(ConfigFileType::JSON),
            _ => None,
        });
    table_options
}

#[async_trait]
impl Session for ForeignSession {
    fn session_id(&self) -> &str {
        unsafe { (self.session.session_id)(&self.session).as_str() }
    }

    fn config(&self) -> &SessionConfig {
        &self.config
    }

    fn config_options(&self) -> &ConfigOptions {
        self.config.options()
    }

    fn catalog_list(&self) -> Arc<dyn CatalogProviderList> {
        Arc::clone(&self.catalog_list)
    }

    fn query_planner(&self) -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::clone(self.query_planner.get_or_init(|| unsafe {
            let planner = (self.session.query_planner)(&self.session);
            (&planner).into()
        }))
    }

    fn optimize(&self, plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
        unsafe {
            let codec: Arc<dyn LogicalExtensionCodec> =
                (&self.session.logical_codec).into();
            let logical_plan =
                logical_plan_to_bytes_with_extension_codec(plan, codec.as_ref())?;
            let optimized_plan = df_result!((self.session.optimize)(
                &self.session,
                SVec::from(logical_plan.as_ref()),
            ))?;
            logical_plan_from_bytes_with_extension_codec(
                optimized_plan.as_slice(),
                self.task_ctx().as_ref(),
                codec.as_ref(),
            )
        }
    }

    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!(
            "ForeignSession::create_physical_plan is unsupported; export and invoke an FFI_QueryPlanner captured before installing a foreign planner"
        )
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        unsafe {
            let codec: Arc<dyn LogicalExtensionCodec> =
                (&self.session.logical_codec).into();
            let logical_expr = serialize_expr(&expr, codec.as_ref())?.encode_to_vec();
            let schema = WrappedSchema(FFI_ArrowSchema::try_from(df_schema.as_arrow())?);

            let physical_expr = df_result!((self.session.create_physical_expr)(
                &self.session,
                logical_expr.into_iter().collect(),
                schema
            ))?;

            Ok((&physical_expr).into())
        }
    }

    fn physical_optimizers(&self) -> &[Arc<dyn PhysicalOptimizerRule + Send + Sync>] {
        self.physical_optimizers.get_or_init(|| unsafe {
            (self.session.physical_optimizers)(&self.session)
                .into_iter()
                .map(|rule| (&rule).into())
                .collect()
        })
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        &self.scalar_functions
    }

    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        &self.higher_order_functions
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        &self.aggregate_functions
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        &self.window_functions
    }

    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        &self.extension_types
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.props
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    fn default_table_options(&self) -> TableOptions {
        unsafe {
            table_options_from_rhashmap((self.session.default_table_options)(
                &self.session,
            ))
        }
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        log::warn!(
            "Mutating table options is not supported via FFI. Changes will not have an effect."
        );
        &mut self.table_options
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        unsafe { (self.session.task_ctx)(&self.session).into() }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow_schema::{DataType, Field, Schema};
    use datafusion::catalog::MemoryCatalogProvider;
    use datafusion::execution::SessionStateBuilder;
    use datafusion_common::{DataFusionError, Result, exec_err};
    use datafusion_expr::col;
    use datafusion_expr::registry::FunctionRegistry;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

    use super::*;

    static QUERY_PLANNER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static PHYSICAL_OPTIMIZER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static REENTERING_PLANNER_CALLS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct ReenteringQueryPlanner;

    #[async_trait]
    impl QueryPlanner for ReenteringQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session: &dyn Session,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if REENTERING_PLANNER_CALLS.fetch_add(1, Ordering::Relaxed) == 0 {
                session.create_physical_plan(logical_plan).await
            } else {
                exec_err!("query planner was re-entered through the session")
            }
        }
    }

    unsafe extern "C" fn counting_query_planner(
        session: &FFI_SessionRef,
    ) -> FFI_QueryPlanner {
        QUERY_PLANNER_CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { query_planner_fn_wrapper(session) }
    }

    unsafe extern "C" fn counting_physical_optimizers(
        session: &FFI_SessionRef,
    ) -> SVec<FFI_PhysicalOptimizerRule> {
        PHYSICAL_OPTIMIZER_CALLS.fetch_add(1, Ordering::Relaxed);
        unsafe { physical_optimizers_fn_wrapper(session) }
    }

    #[test]
    fn test_foreign_session_lazily_loads_planning_state() -> Result<(), DataFusionError> {
        QUERY_PLANNER_CALLS.store(0, Ordering::Relaxed);
        PHYSICAL_OPTIMIZER_CALLS.store(0, Ordering::Relaxed);

        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );
        let state = ctx.state();
        let mut local_session = FFI_SessionRef::new(&state, None, logical_codec);
        local_session.query_planner = counting_query_planner;
        local_session.physical_optimizers = counting_physical_optimizers;

        let mut foreign_session = ForeignSession::try_from(&local_session)?;
        assert_eq!(QUERY_PLANNER_CALLS.load(Ordering::Relaxed), 0);
        assert_eq!(PHYSICAL_OPTIMIZER_CALLS.load(Ordering::Relaxed), 0);

        // `FFI_SessionRef::clone` restores the standard function pointers, so
        // instrument the clone retained by `ForeignSession` as well.
        foreign_session.session.query_planner = counting_query_planner;
        foreign_session.session.physical_optimizers = counting_physical_optimizers;

        foreign_session.query_planner();
        foreign_session.query_planner();
        assert_eq!(QUERY_PLANNER_CALLS.load(Ordering::Relaxed), 1);

        foreign_session.physical_optimizers();
        foreign_session.physical_optimizers();
        assert_eq!(PHYSICAL_OPTIMIZER_CALLS.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_ffi_session() -> Result<(), DataFusionError> {
        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let mut table_options = TableOptions::default();
        table_options.csv.has_header = Some(true);
        table_options.json.schema_infer_max_rec = Some(10);
        #[cfg(feature = "parquet")]
        {
            table_options.parquet.global.coerce_int96 = Some("123456789".into());
        }
        table_options.current_format = Some(ConfigFileType::JSON);

        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_table_options(table_options)
            .build();

        let logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );

        let local_session = FFI_SessionRef::new(&state, None, logical_codec);
        let foreign_session = ForeignSession::try_from(&local_session)?;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let df_schema = schema.try_into()?;
        let physical_expr = foreign_session.create_physical_expr(col("a"), &df_schema)?;
        assert_eq!(
            format!("{physical_expr:?}"),
            "Column { name: \"a\", index: 0 }"
        );

        assert_eq!(foreign_session.session_id(), state.session_id());

        let foreign_catalog_list = foreign_session.catalog_list();
        assert_eq!(
            foreign_catalog_list.catalog_names(),
            state.catalog_list().catalog_names()
        );
        foreign_catalog_list.register_catalog(
            "foreign_registered".to_owned(),
            Arc::new(MemoryCatalogProvider::new()),
        );
        assert!(state.catalog_list().catalog("foreign_registered").is_some());

        let logical_plan = LogicalPlan::default();
        assert_eq!(foreign_session.optimize(&logical_plan)?, logical_plan);
        assert_eq!(
            foreign_session.physical_optimizers().len(),
            state.physical_optimizers().len()
        );
        assert!(foreign_session.statistics_registry().is_none());
        let planned = foreign_session
            .query_planner()
            .create_physical_plan(&logical_plan, &foreign_session)
            .await?;
        assert_eq!(planned.name(), "EmptyExec");

        assert_eq!(
            format!("{:?}", foreign_session.default_table_options()),
            format!("{:?}", state.default_table_options())
        );

        assert_eq!(
            format!("{:?}", foreign_session.table_options()),
            format!("{:?}", state.table_options())
        );

        let local_udfs = state.udfs();
        for udf in foreign_session.scalar_functions().keys() {
            assert!(local_udfs.contains(udf));
        }
        let local_udafs = state.udafs();
        for udaf in foreign_session.aggregate_functions().keys() {
            assert!(local_udafs.contains(udaf));
        }
        let local_udwfs = state.udwfs();
        for udwf in foreign_session.window_functions().keys() {
            assert!(local_udwfs.contains(udwf));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_foreign_session_rejects_create_physical_plan() {
        REENTERING_PLANNER_CALLS.store(0, Ordering::Relaxed);

        let (ctx, task_ctx_provider) = crate::util::tests::test_session_and_ctx();
        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(ReenteringQueryPlanner))
            .build();
        let logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );
        let local_session = FFI_SessionRef::new(&state, None, logical_codec);
        let foreign_session = ForeignSession::try_from(&local_session).unwrap();

        let error = foreign_session
            .create_physical_plan(&LogicalPlan::default())
            .await
            .unwrap_err();

        assert_eq!(REENTERING_PLANNER_CALLS.load(Ordering::Relaxed), 0);
        assert!(matches!(error, DataFusionError::NotImplemented(_)));
        assert!(
            error
                .to_string()
                .contains("export and invoke an FFI_QueryPlanner captured before")
        );

        // An already-compiled DataFusion 55 consumer calls this retained slot
        // directly. It must receive the same safe failure without re-entering
        // the installed planner.
        let callback_error = unsafe {
            (local_session.create_physical_plan)(&local_session, SVec::new())
                .await
                .unwrap_err()
        };
        assert_eq!(REENTERING_PLANNER_CALLS.load(Ordering::Relaxed), 0);
        assert!(
            callback_error
                .as_str()
                .contains("export and invoke an FFI_QueryPlanner captured before")
        );
    }
}
