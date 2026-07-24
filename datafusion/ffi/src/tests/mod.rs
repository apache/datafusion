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

use arrow::array::{RecordBatch, record_batch};
use arrow_schema::{DataType, Field, Schema};
use async_provider::create_async_table_provider;
use async_trait::async_trait;
use catalog::create_catalog_provider;
use datafusion_catalog::MemTable;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::stats::Precision;
use datafusion_common::{Column, DFSchemaRef, Result, ScalarValue};
use datafusion_common::{ColumnStatistics, Statistics};
use datafusion_expr::dml::{MergeIntoAction, MergeIntoClause, MergeIntoClauseKind};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use sync_provider::create_sync_table_provider;
use udf_udaf_udwf::{
    create_ffi_abs_func, create_ffi_random_func, create_ffi_rank_func,
    create_ffi_stddev_func, create_ffi_sum_func, create_ffi_table_func,
};

use crate::catalog_provider::FFI_CatalogProvider;
use crate::catalog_provider_list::FFI_CatalogProviderList;
use crate::config::extension_options::FFI_ExtensionOptions;
use crate::execution_plan::FFI_ExecutionPlan;
use crate::execution_plan::tests::EmptyExec;
use crate::physical_optimizer::FFI_PhysicalOptimizerRule;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::table_provider::FFI_TableProvider;
use crate::table_provider_factory::FFI_TableProviderFactory;
use crate::tests::catalog::create_catalog_provider_list;
use crate::udaf::FFI_AggregateUDF;
use crate::udf::FFI_ScalarUDF;
use crate::udtf::FFI_TableFunction;
use crate::udwf::FFI_WindowUDF;

mod async_provider;
pub mod catalog;
pub mod config;
mod physical_optimizer;
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

    pub create_exec_with_statistics: extern "C" fn() -> FFI_ExecutionPlan,

    pub create_table_with_statistics:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProvider,

    pub create_merge_table:
        extern "C" fn(codec: FFI_LogicalExtensionCodec) -> FFI_TableProvider,

    pub create_physical_optimizer_rule: extern "C" fn() -> FFI_PhysicalOptimizerRule,

    pub create_context_aware_optimizer_rule: extern "C" fn() -> FFI_PhysicalOptimizerRule,

    pub version: extern "C" fn() -> u64,
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

/// Thin wrapper that attaches a fixed [`Statistics`] snapshot to any inner
/// [`TableProvider`] without changing its scan behaviour.
#[derive(Debug)]
struct TableWithStats {
    inner: Arc<dyn TableProvider>,
    stats: Statistics,
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
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(session, projection, filters, limit).await
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
    });
    FFI_TableProvider::new_with_ffi_codec(provider, true, None, codec)
}

#[derive(Debug)]
struct MergeTableProvider {
    schema: Arc<Schema>,
}

#[async_trait]
impl TableProvider for MergeTableProvider {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(Arc::clone(&self.schema))))
    }

    async fn merge_into(
        &self,
        _state: &dyn Session,
        source: Arc<dyn ExecutionPlan>,
        merge_schema: DFSchemaRef,
        on: Expr,
        clauses: Vec<MergeIntoClause>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let target_id =
            merge_schema.index_of_column(&Column::new(Some("target"), "id"))?;
        let source_id =
            merge_schema.index_of_column(&Column::new(Some("source"), "id"))?;
        if (target_id, source_id) != (0, 2) {
            return datafusion_common::plan_err!(
                "unexpected MERGE schema indices: target={target_id}, source={source_id}"
            );
        }

        if on.to_string() != "target.id = source.id" {
            return datafusion_common::plan_err!(
                "unexpected logical MERGE condition: {on}"
            );
        }

        let [matched, not_matched] = clauses.as_slice() else {
            return datafusion_common::plan_err!(
                "expected two MERGE clauses, got {}",
                clauses.len()
            );
        };
        if matched.kind != MergeIntoClauseKind::Matched
            || matched
                .predicate
                .as_ref()
                .map(ToString::to_string)
                .as_deref()
                != Some("target.val IS NULL")
        {
            return datafusion_common::plan_err!(
                "unexpected matched MERGE clause: {matched:?}"
            );
        }
        let MergeIntoAction::Update(assignments) = &matched.action else {
            return datafusion_common::plan_err!(
                "expected MERGE update action, got {:?}",
                matched.action
            );
        };
        if assignments.len() != 1
            || assignments[0].0 != "val"
            || assignments[0].1.to_string() != "source.val"
        {
            return datafusion_common::plan_err!(
                "unexpected MERGE update assignments: {assignments:?}"
            );
        }

        if not_matched.kind != MergeIntoClauseKind::NotMatched {
            return datafusion_common::plan_err!(
                "unexpected not-matched MERGE clause: {not_matched:?}"
            );
        }
        let MergeIntoAction::Insert { columns, values } = &not_matched.action else {
            return datafusion_common::plan_err!(
                "expected MERGE insert action, got {:?}",
                not_matched.action
            );
        };
        if columns != &["id".to_string(), "val".to_string()]
            || values.iter().map(ToString::to_string).collect::<Vec<_>>()
                != ["source.id".to_string(), "source.val".to_string()]
        {
            return datafusion_common::plan_err!(
                "unexpected MERGE insert action: {not_matched:?}"
            );
        }

        Ok(source)
    }
}

pub(crate) extern "C" fn create_merge_table(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Int64, true),
    ]));
    let provider = Arc::new(MergeTableProvider { schema });
    FFI_TableProvider::new_with_ffi_codec(provider, true, None, codec)
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
        create_exec_with_statistics,
        create_table_with_statistics,
        create_merge_table,
        create_physical_optimizer_rule:
            physical_optimizer::create_physical_optimizer_rule,
        create_context_aware_optimizer_rule:
            physical_optimizer::create_context_aware_optimizer_rule,
        version: super::version,
    }
}
