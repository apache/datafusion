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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{RecordBatch, record_batch};
use arrow_schema::{DataType, Field, Schema};
use async_provider::create_async_table_provider;
use async_trait::async_trait;
use catalog::create_catalog_provider;
use datafusion_catalog::MemTable;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Statistics};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{Expr, TableType, col, lit};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::ExecutionPlan;
use sync_provider::create_sync_table_provider;
use udf_udaf_udwf::{
    create_ffi_abs_func, create_ffi_first_value_func, create_ffi_random_func,
    create_ffi_rank_func, create_ffi_stddev_func, create_ffi_sum_func,
    create_ffi_table_func,
};

use crate::catalog_provider::FFI_CatalogProvider;
use crate::catalog_provider_list::FFI_CatalogProviderList;
use crate::config::extension_options::FFI_ExtensionOptions;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::execution_plan::tests::{EmptyExec, create_dynamic_filter};
use crate::physical_optimizer::FFI_PhysicalOptimizerRule;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use crate::query_planner::FFI_QueryPlanner;
use crate::table_provider::FFI_TableProvider;
use crate::table_provider_factory::FFI_TableProviderFactory;
use crate::tests::catalog::create_catalog_provider_list;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udtf::FFI_TableFunction;
use crate::udwf::FFI_WindowUDF;
use crate::util::FFI_Option;

mod async_provider;
pub mod catalog;
pub mod config;
mod physical_optimizer;
mod query_planner;
mod sync_provider;
mod table_provider_factory;
mod udf_udaf_udwf;
pub mod utils;

#[repr(C)]
/// This struct defines the module interfaces. It is to be shared by
/// both the module loading program and library that implements the
/// module.
pub struct ForeignLibraryModule {
    /// Construct an opinionated catalog provider
    pub create_catalog:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_CatalogProvider,

    /// Construct an opinionated catalog provider list
    pub create_catalog_list:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_CatalogProviderList,

    /// Constructs the table provider
    pub create_table: extern "C" fn(
        synchronous: bool,
        codec: FFI_LogicalExtensionCodec,
    ) -> FFI_TableProvider,

    /// Constructs the table provider factory
    pub create_table_factory:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProviderFactory,

    /// Create a scalar UDF
    pub create_scalar_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_nullary_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_timezone_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_placement_udf: extern "C" fn() -> FFI_ScalarUDF,

    pub create_table_function:
        extern "C" fn(FFI_LogicalExtensionCodec) -> FFI_TableFunction,

    /// Create an aggregate UDAF using sum
    pub create_sum_udaf: extern "C" fn() -> FFI_AggregateUDF,

    /// Create  grouping UDAF using stddev
    pub create_stddev_udaf: extern "C" fn() -> FFI_AggregateUDF,

    pub create_rank_udwf: extern "C" fn() -> FFI_WindowUDF,

    /// Create extension options, for either ConfigOptions or TableOptions
    pub create_extension_options: extern "C" fn() -> FFI_ExtensionOptions,

    pub create_empty_exec: extern "C" fn() -> FFI_ExecutionPlan,

    pub create_exec_with_expressions: extern "C" fn() -> FFI_ExecutionPlan,

    pub create_exec_with_dynamic_expressions: extern "C" fn() -> FFI_ExecutionPlan,

    pub create_exec_with_statistics: extern "C" fn() -> FFI_ExecutionPlan,

    pub create_table_with_statistics:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProvider,

    pub create_physical_optimizer_rule: extern "C" fn() -> FFI_PhysicalOptimizerRule,

    pub create_context_aware_optimizer_rule: extern "C" fn() -> FFI_PhysicalOptimizerRule,

    /// Construct a query planner. When `library_a_planner` is provided the
    /// planner delegates to it, as library C does after library A swaps planners.
    pub create_query_planner: extern "C" fn(
        logical_codec: FFI_LogicalExtensionCodec,
        physical_codec: FFI_PhysicalExtensionCodec,
        library_a_planner: FFI_Option<FFI_QueryPlanner>,
    ) -> FFI_QueryPlanner,

    pub version: extern "C" fn() -> u64,

    /// Create an aggregate UDAF using first_value
    pub create_first_value_udaf: extern "C" fn() -> FFI_AggregateUDF,
}

pub fn create_test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Float64, true),
    ]))
}

pub fn create_record_batch(start_value: i32, num_values: usize) -> RecordBatch {
    let end_value = start_value + num_values as i32;
    let a_vals: Vec<i32> = (start_value..end_value).collect();
    let b_vals: Vec<f64> = a_vals.iter().map(|v| *v as f64).collect();

    record_batch!(("a", Int32, a_vals), ("b", Float64, b_vals)).unwrap()
}

/// Here we only wish to create a simple table provider as an example.
/// We create an in-memory table and convert it to it's FFI counterpart.
extern "C" fn construct_table_provider(
    synchronous: bool,
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    match synchronous {
        true => create_sync_table_provider(codec),
        false => create_async_table_provider(codec),
    }
}

/// Here we only wish to create a simple table provider as an example.
/// We create an in-memory table and convert it to it's FFI counterpart.
extern "C" fn construct_table_provider_factory(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProviderFactory {
    table_provider_factory::create(codec)
}

pub(crate) extern "C" fn create_empty_exec() -> FFI_ExecutionPlan {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    let plan = Arc::new(EmptyExec::new(schema));
    FFI_ExecutionPlan::new(plan, None)
}

pub(crate) extern "C" fn create_exec_with_expressions() -> FFI_ExecutionPlan {
    let schema = Arc::new(Schema::empty());
    let expression: Arc<dyn PhysicalExpr> = create_dynamic_filter();
    let plan = Arc::new(EmptyExec::new(schema).with_expressions(vec![expression]));
    FFI_ExecutionPlan::new(plan, None)
}

pub(crate) extern "C" fn create_exec_with_dynamic_expressions() -> FFI_ExecutionPlan {
    let schema = Arc::new(Schema::empty());
    let expression: Arc<dyn PhysicalExpr> = create_dynamic_filter();
    let plan =
        Arc::new(EmptyExec::new(schema).with_dynamic_expressions(vec![expression]));
    FFI_ExecutionPlan::new(plan, None)
}

/// Returns canonical statistics used by both the producer and consumer sides of
/// the integration tests so round-trips can be asserted without hard-coding
/// the values in two places.
pub fn make_test_statistics() -> Statistics {
    Statistics {
        num_rows: Precision::Exact(42),
        total_byte_size: Precision::Exact(672),
        column_statistics: vec![
            ColumnStatistics {
                null_count: Precision::Exact(0),
                max_value: Precision::Exact(ScalarValue::Int32(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int32(Some(-10))),
                sum_value: Precision::Exact(ScalarValue::Int64(Some(1890))),
                distinct_count: Precision::Inexact(40),
                byte_size: Precision::Exact(168),
            },
            ColumnStatistics {
                null_count: Precision::Exact(1),
                max_value: Precision::Exact(ScalarValue::Float64(Some(99.5))),
                min_value: Precision::Exact(ScalarValue::Float64(Some(-1.5))),
                sum_value: Precision::Absent,
                distinct_count: Precision::Absent,
                byte_size: Precision::Exact(328),
            },
        ],
    }
}

pub(crate) extern "C" fn create_exec_with_statistics() -> FFI_ExecutionPlan {
    let schema = create_test_schema();
    let plan = Arc::new(EmptyExec::new(schema).with_statistics(make_test_statistics()));
    FFI_ExecutionPlan::new(plan, None)
}

/// Registers real Bytes-category [`MetricValue::Count`] and
/// [`MetricValue::Gauge`] metrics (via the same [`MetricBuilder::bytes_counter`]
/// and [`MetricBuilder::bytes_gauge`] constructors production code uses for
/// `bytes_scanned`/`stream_memory_usage`) on the returned plan, so the
/// consumer-side integration test can exercise the category-aware
/// byte-formatting `Display` logic through a real cross-library `metrics()`
/// FFI call rather than only the in-process `FFI_MetricValue` conversion
/// tests in `physical_expr::metrics`.
///
/// This is deliberately exported as its own top-level symbol rather than a
/// new field on [`ForeignLibraryModule`]: that struct is public and
/// `#[repr(C)]` with no private/gated constructor, so every field is part of
/// its exhaustive-construction ABI surface - adding one is exactly what
/// `cargo-semver-checks`'s `constructible_struct_adds_field` lint flags, even
/// for a test-only, `integration-tests`-gated struct like this one. A
/// separate exported symbol, loaded the same way [`load_module`] loads
/// `datafusion_ffi_get_module`, avoids touching that struct's layout at all.
///
/// [`MetricValue::Count`]: datafusion_physical_expr_common::metrics::MetricValue::Count
/// [`MetricValue::Gauge`]: datafusion_physical_expr_common::metrics::MetricValue::Gauge
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_ffi_test_create_exec_with_byte_metrics() -> FFI_ExecutionPlan
{
    use datafusion_physical_expr_common::metrics::{
        ExecutionPlanMetricsSet, MetricBuilder,
    };

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, false)]));

    let metrics_set = ExecutionPlanMetricsSet::new();
    MetricBuilder::new(&metrics_set)
        .bytes_counter("bytes_scanned", 0)
        .add(1536);
    MetricBuilder::new(&metrics_set)
        .bytes_gauge("stream_memory_usage", 0)
        .add(2048);

    let plan = Arc::new(EmptyExec::new(schema).with_metrics(metrics_set.clone_inner()));
    FFI_ExecutionPlan::new(plan, None)
}

/// Decodes a [`crate::proto::physical_extension_codec::fixtures::ScalarSubqueryExprExec`]
/// from `bytes` using
/// [`crate::proto::physical_extension_codec::fixtures::ScalarSubqueryExprExecCodec`]
/// *inside this image*, forwarding `scalar_subquery_results` (if present)
/// into the decode context exactly the way a real
/// `FFI_PhysicalExtensionCodec::try_decode_with_ctx` call would, then reads
/// back the embedded `ScalarSubqueryExpr`'s `Int64` result at `index`.
///
/// There is no generic `FFI_PhysicalExpr` wrapper in this crate to carry an
/// arbitrary decoded expression back across the boundary, so the decoded
/// `ScalarSubqueryExprExec`/`ScalarSubqueryExpr` never leave this image as
/// Rust values - only the `i64` their results container reports does. That
/// is still the assertion that matters: when `scalar_subquery_results` is
/// built in a genuinely different image (see the integration test in
/// `datafusion/ffi/tests/ffi_physical_extension_codec.rs`), reading it back
/// here only succeeds if `get`/`set` actually round-tripped through
/// `ForeignScalarSubqueryResultsBackend` into the caller's own results
/// container - the cross-library coverage gap the in-process, marker-mocked
/// unit test in `datafusion/ffi/src/proto/physical_extension_codec.rs`
/// cannot close on its own.
///
/// Exported as its own top-level symbol rather than a new field on
/// [`ForeignLibraryModule`] for the same reason as
/// [`datafusion_ffi_test_create_exec_with_byte_metrics`]: that struct is
/// public, `#[repr(C)]`, and has no private/gated constructor, so adding a
/// field changes its exhaustive-construction ABI surface even for this
/// test-only, `integration-tests`-gated struct.
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_ffi_test_decode_scalar_subquery_expr_exec_result(
    task_ctx_provider: crate::execution::FFI_TaskContextProvider,
    bytes: stabby::slice::Slice<u8>,
    scalar_subquery_results: FFI_Option<
        crate::proto::scalar_subquery_results::FFI_ScalarSubqueryResults,
    >,
    index: u64,
) -> crate::util::FFI_Result<FFI_Option<i64>> {
    use datafusion_common::ScalarValue;
    use datafusion_execution::TaskContext;
    use datafusion_expr::physical_planning_context::SubqueryIndex;
    use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
    use datafusion_proto::physical_plan::{
        DefaultPhysicalProtoConverter, PhysicalExtensionCodec, PhysicalPlanDecodeContext,
    };

    use crate::proto::physical_extension_codec::fixtures::{
        ScalarSubqueryExprExec, ScalarSubqueryExprExecCodec,
    };
    use crate::sresult_return;
    use crate::util::FFI_Result;

    let task_ctx: Arc<TaskContext> = sresult_return!((&task_ctx_provider).try_into());

    let codec = ScalarSubqueryExprExecCodec;
    let decode_ctx = PhysicalPlanDecodeContext::new(task_ctx.as_ref(), &codec);
    let decode_ctx = match scalar_subquery_results.into_option() {
        Some(results) => decode_ctx.with_scalar_subquery_results(results.into()),
        None => decode_ctx,
    };

    // `ScalarSubqueryExprExecCodec::try_decode_with_ctx` only uses this to
    // read a schema; `ScalarSubqueryExpr` carries no column references.
    let dummy_input: Arc<dyn ExecutionPlan> =
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())));

    let plan = sresult_return!(codec.try_decode_with_ctx(
        bytes.as_ref(),
        &[dummy_input],
        &decode_ctx,
        &DefaultPhysicalProtoConverter {},
    ));

    let Some(exec) = plan.downcast_ref::<ScalarSubqueryExprExec>() else {
        return FFI_Result::Err("expected ScalarSubqueryExprExec".into());
    };
    let Some(sq_expr) = exec.expr.downcast_ref::<ScalarSubqueryExpr>() else {
        return FFI_Result::Err("expected ScalarSubqueryExpr".into());
    };

    match sq_expr.results().get(SubqueryIndex::new(index as usize)) {
        None => FFI_Result::Ok(FFI_Option::None),
        Some(ScalarValue::Int64(v)) => FFI_Result::Ok(v.into()),
        Some(_) => FFI_Result::Err("expected an Int64 ScalarValue".into()),
    }
}

/// Thin wrapper that attaches a fixed [`Statistics`] snapshot to any inner
/// [`TableProvider`] without changing its scan behaviour.
#[derive(Debug)]
struct TableWithStats {
    inner: Arc<dyn TableProvider>,
    stats: Statistics,
    delete_calls: AtomicUsize,
    update_calls: AtomicUsize,
}

#[async_trait]
impl TableProvider for TableWithStats {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.stats.clone())
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(session, projection, filters, limit).await
    }

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let call = self.delete_calls.fetch_add(1, Ordering::Relaxed);
        let valid = match call {
            0 => filters == vec![col("a").gt(lit(10_i32)), col("b").lt(lit(2.5_f64))],
            1 => filters.is_empty(),
            _ => false,
        };
        if !valid {
            return exec_err!("Unexpected DELETE filters for call {call}");
        }
        Ok(dml_count_plan())
    }

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if assignments
            != vec![
                ("b".to_string(), lit(42_f64)),
                ("a".to_string(), lit(7_i32)),
            ]
        {
            return exec_err!("Unexpected UPDATE assignments");
        }

        let call = self.update_calls.fetch_add(1, Ordering::Relaxed);
        let valid = match call {
            0 => filters == vec![col("a").eq(lit(7_i32)), col("b").gt(lit(1.5_f64))],
            1 => filters.is_empty(),
            _ => false,
        };

        if !valid {
            return exec_err!("Unexpected UPDATE filters for call {call}");
        }

        Ok(dml_count_plan())
    }

    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(dml_count_plan())
    }
}

pub(crate) extern "C" fn create_table_with_statistics(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    let schema = create_test_schema();
    let batch = create_record_batch(1, 5);
    let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
    let provider = Arc::new(TableWithStats {
        inner,
        stats: make_test_statistics(),
        delete_calls: AtomicUsize::new(0),
        update_calls: AtomicUsize::new(0),
    });
    FFI_TableProvider::new_with_ffi_codec(provider, true, None, codec)
}

fn dml_count_plan() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]));
    Arc::new(EmptyExec::new(schema))
}

/// This defines the entry point for using the module.
#[unsafe(no_mangle)]
pub extern "C" fn datafusion_ffi_get_module() -> ForeignLibraryModule {
    ForeignLibraryModule {
        create_catalog: create_catalog_provider,
        create_catalog_list: create_catalog_provider_list,
        create_table: construct_table_provider,
        create_table_factory: construct_table_provider_factory,
        create_scalar_udf: create_ffi_abs_func,
        create_nullary_udf: create_ffi_random_func,
        create_timezone_udf: udf_udaf_udwf::create_timezone_func,
        create_placement_udf: udf_udaf_udwf::create_placement_func,
        create_table_function: create_ffi_table_func,
        create_sum_udaf: create_ffi_sum_func,
        create_stddev_udaf: create_ffi_stddev_func,
        create_rank_udwf: create_ffi_rank_func,
        create_extension_options: config::create_extension_options,
        create_empty_exec,
        create_exec_with_expressions,
        create_exec_with_dynamic_expressions,
        create_exec_with_statistics,
        create_table_with_statistics,
        create_physical_optimizer_rule:
            physical_optimizer::create_physical_optimizer_rule,
        create_context_aware_optimizer_rule:
            physical_optimizer::create_context_aware_optimizer_rule,
        create_query_planner: query_planner::create_query_planner,
        version: super::version,
        create_first_value_udaf: create_ffi_first_value_func,
    }
}
