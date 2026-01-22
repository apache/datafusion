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

//! Tests for DELETE, UPDATE, and TRUNCATE planning to verify filter and assignment extraction.

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::logical_expr::{
    Expr, LogicalPlan, TableProviderFilterPushDown, TableScan,
};
use datafusion_catalog::Session;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ScalarValue, TableReference};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;

/// A TableProvider that captures the filters passed to delete_from().
struct CaptureDeleteProvider {
    schema: SchemaRef,
    received_filters: Arc<Mutex<Option<Vec<Expr>>>>,
    filter_pushdown: TableProviderFilterPushDown,
    per_filter_pushdown: Option<Vec<TableProviderFilterPushDown>>,
}

impl CaptureDeleteProvider {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            filter_pushdown: TableProviderFilterPushDown::Unsupported,
            per_filter_pushdown: None,
        }
    }

    fn new_with_filter_pushdown(
        schema: SchemaRef,
        filter_pushdown: TableProviderFilterPushDown,
    ) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            filter_pushdown,
            per_filter_pushdown: None,
        }
    }

    fn new_with_per_filter_pushdown(
        schema: SchemaRef,
        per_filter_pushdown: Vec<TableProviderFilterPushDown>,
    ) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            filter_pushdown: TableProviderFilterPushDown::Unsupported,
            per_filter_pushdown: Some(per_filter_pushdown),
        }
    }

    fn captured_filters(&self) -> Option<Vec<Expr>> {
        self.received_filters.lock().unwrap().clone()
    }
}

impl std::fmt::Debug for CaptureDeleteProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureDeleteProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CaptureDeleteProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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

    async fn delete_from(
        &self,
        _state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        *self.received_filters.lock().unwrap() = Some(filters);
        Ok(Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("count", DataType::UInt64, false),
        ])))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if let Some(per_filter) = &self.per_filter_pushdown
            && per_filter.len() == filters.len()
        {
            return Ok(per_filter.clone());
        }

        Ok(vec![self.filter_pushdown.clone(); filters.len()])
    }
}

/// A TableProvider that captures filters and assignments passed to update().
#[allow(clippy::type_complexity)]
struct CaptureUpdateProvider {
    schema: SchemaRef,
    received_filters: Arc<Mutex<Option<Vec<Expr>>>>,
    received_assignments: Arc<Mutex<Option<Vec<(String, Expr)>>>>,
    filter_pushdown: TableProviderFilterPushDown,
    per_filter_pushdown: Option<Vec<TableProviderFilterPushDown>>,
}

impl CaptureUpdateProvider {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            received_assignments: Arc::new(Mutex::new(None)),
            filter_pushdown: TableProviderFilterPushDown::Unsupported,
            per_filter_pushdown: None,
        }
    }

    fn new_with_filter_pushdown(
        schema: SchemaRef,
        filter_pushdown: TableProviderFilterPushDown,
    ) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            received_assignments: Arc::new(Mutex::new(None)),
            filter_pushdown,
            per_filter_pushdown: None,
        }
    }

    fn captured_filters(&self) -> Option<Vec<Expr>> {
        self.received_filters.lock().unwrap().clone()
    }

    fn captured_assignments(&self) -> Option<Vec<(String, Expr)>> {
        self.received_assignments.lock().unwrap().clone()
    }
}

impl std::fmt::Debug for CaptureUpdateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureUpdateProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CaptureUpdateProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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

    async fn update(
        &self,
        _state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        *self.received_filters.lock().unwrap() = Some(filters);
        *self.received_assignments.lock().unwrap() = Some(assignments);
        Ok(Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("count", DataType::UInt64, false),
        ])))))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        if let Some(per_filter) = &self.per_filter_pushdown
            && per_filter.len() == filters.len()
        {
            return Ok(per_filter.clone());
        }

        Ok(vec![self.filter_pushdown.clone(); filters.len()])
    }
}

/// A TableProvider that captures whether truncate() was called.
struct CaptureTruncateProvider {
    schema: SchemaRef,
    truncate_called: Arc<Mutex<bool>>,
}

impl CaptureTruncateProvider {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            truncate_called: Arc::new(Mutex::new(false)),
        }
    }

    fn was_truncated(&self) -> bool {
        *self.truncate_called.lock().unwrap()
    }
}

impl std::fmt::Debug for CaptureTruncateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaptureTruncateProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

#[async_trait]
impl TableProvider for CaptureTruncateProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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

    async fn truncate(&self, _state: &dyn Session) -> Result<Arc<dyn ExecutionPlan>> {
        *self.truncate_called.lock().unwrap() = true;

        Ok(Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![
            Field::new("count", DataType::UInt64, false),
        ])))))
    }
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("value", DataType::Int32, true),
    ]))
}

fn expr_has_table_reference(expr: &Expr, table: &str) -> Result<bool> {
    let reference = TableReference::bare(table);
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::Column(column)
                if column.relation.as_ref().is_some_and(|relation| {
                    relation.resolved_eq(&reference)
                })
        ))
    })
}

#[tokio::test]
async fn test_delete_single_filter() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new(test_schema()));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("DELETE FROM t WHERE id = 1")
        .await?
        .collect()
        .await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(filters.len(), 1);
    assert!(filters[0].to_string().contains("id"));
    Ok(())
}

#[tokio::test]
async fn test_delete_multiple_filters() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new(test_schema()));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("DELETE FROM t WHERE id = 1 AND status = 'x'")
        .await?
        .collect()
        .await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(!filters.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_delete_no_filters() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new(test_schema()));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("DELETE FROM t").await?.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(
        filters.is_empty(),
        "DELETE without WHERE should have empty filters"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_complex_expr() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new(test_schema()));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("DELETE FROM t WHERE id > 5 AND (status = 'a' OR status = 'b')")
        .await?
        .collect()
        .await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(!filters.is_empty());
    Ok(())
}

#[tokio::test]
async fn test_delete_filter_pushdown_extracts_table_scan_filters() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    let df = ctx.sql("DELETE FROM t WHERE id = 1").await?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let mut scan_filters = Vec::new();
    optimized_plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { filters, .. }) = node {
            scan_filters.extend(filters.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    assert_eq!(scan_filters.len(), 1);
    assert!(scan_filters[0].to_string().contains("id"));

    df.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(filters.len(), 1);
    assert!(filters[0].to_string().contains("id"));
    Ok(())
}

#[tokio::test]
async fn test_delete_compound_filters_with_pushdown() -> Result<()> {
    let provider = Arc::new(CaptureDeleteProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("DELETE FROM t WHERE id = 1 AND status = 'active'")
        .await?
        .collect()
        .await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    // Should receive both filters, not deduplicate valid separate predicates
    assert_eq!(
        filters.len(),
        2,
        "compound filters should not be over-suppressed"
    );

    let filter_strs: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
    assert!(
        filter_strs.iter().any(|s| s.contains("id")),
        "should contain id filter"
    );
    assert!(
        filter_strs.iter().any(|s| s.contains("status")),
        "should contain status filter"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_mixed_filter_locations() -> Result<()> {
    // Test mixed-location filters: some in Filter node, some in TableScan.filters
    // This happens when provider uses TableProviderFilterPushDown::Inexact,
    // meaning it can push down some predicates but not others.
    let provider = Arc::new(CaptureDeleteProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Inexact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    // Execute DELETE with compound WHERE clause
    ctx.sql("DELETE FROM t WHERE id = 1 AND status = 'active'")
        .await?
        .collect()
        .await?;

    // Verify that both predicates are extracted and passed to delete_from(),
    // even though they may be split between Filter node and TableScan.filters
    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(
        filters.len(),
        2,
        "should extract both predicates (union of Filter and TableScan.filters)"
    );

    let filter_strs: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
    assert!(
        filter_strs.iter().any(|s| s.contains("id")),
        "should contain id filter"
    );
    assert!(
        filter_strs.iter().any(|s| s.contains("status")),
        "should contain status filter"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_per_filter_pushdown_mixed_locations() -> Result<()> {
    // Force per-filter pushdown decisions to exercise mixed locations in one query.
    // First predicate is pushed down (Exact), second stays as residual (Unsupported).
    let provider = Arc::new(CaptureDeleteProvider::new_with_per_filter_pushdown(
        test_schema(),
        vec![
            TableProviderFilterPushDown::Exact,
            TableProviderFilterPushDown::Unsupported,
        ],
    ));

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    let df = ctx
        .sql("DELETE FROM t WHERE id = 1 AND status = 'active'")
        .await?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    // Only the first predicate should be pushed to TableScan.filters.
    let mut scan_filters = Vec::new();
    optimized_plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { filters, .. }) = node {
            scan_filters.extend(filters.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(scan_filters.len(), 1);
    assert!(scan_filters[0].to_string().contains("id"));

    // Both predicates should still reach the provider (union + dedup behavior).
    df.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(filters.len(), 2);

    let filter_strs: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
    assert!(
        filter_strs.iter().any(|s| s.contains("id")),
        "should contain pushed-down id filter"
    );
    assert!(
        filter_strs.iter().any(|s| s.contains("status")),
        "should contain residual status filter"
    );

    Ok(())
}

#[tokio::test]
async fn test_update_assignments() -> Result<()> {
    let provider = Arc::new(CaptureUpdateProvider::new(test_schema()));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("UPDATE t SET value = 100, status = 'updated' WHERE id = 5")
        .await?
        .collect()
        .await?;

    let assignments = provider
        .captured_assignments()
        .expect("assignments should be captured");
    assert_eq!(assignments.len(), 2, "should have 2 assignments");

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(!filters.is_empty(), "should have filter for WHERE clause");
    Ok(())
}

#[tokio::test]
async fn test_update_filter_pushdown_extracts_table_scan_filters() -> Result<()> {
    let provider = Arc::new(CaptureUpdateProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    let df = ctx.sql("UPDATE t SET value = 100 WHERE id = 1").await?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    // Verify that the optimizer pushed down the filter into TableScan
    let mut scan_filters = Vec::new();
    optimized_plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { filters, .. }) = node {
            scan_filters.extend(filters.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    assert_eq!(scan_filters.len(), 1);
    assert!(scan_filters[0].to_string().contains("id"));

    // Execute the UPDATE and verify filters were extracted and passed to update()
    df.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(filters.len(), 1);
    assert!(filters[0].to_string().contains("id"));
    Ok(())
}

#[tokio::test]
async fn test_update_filter_pushdown_passes_table_scan_filters() -> Result<()> {
    let provider = Arc::new(CaptureUpdateProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    let df = ctx
        .sql("UPDATE t SET value = 42 WHERE status = 'ready'")
        .await?;
    let optimized_plan = df.clone().into_optimized_plan()?;

    let mut scan_filters = Vec::new();
    optimized_plan.apply(|node| {
        if let LogicalPlan::TableScan(TableScan { filters, .. }) = node {
            scan_filters.extend(filters.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    assert!(
        !scan_filters.is_empty(),
        "expected filter pushdown to populate TableScan filters"
    );

    df.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(
        !filters.is_empty(),
        "expected filters extracted from TableScan during UPDATE"
    );
    Ok(())
}

#[tokio::test]
async fn test_truncate_calls_provider() -> Result<()> {
    let provider = Arc::new(CaptureTruncateProvider::new(test_schema()));
    let config = SessionConfig::new().set(
        "datafusion.optimizer.max_passes",
        &ScalarValue::UInt64(Some(0)),
    );

    let ctx = SessionContext::new_with_config(config);

    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    ctx.sql("TRUNCATE TABLE t").await?.collect().await?;

    assert!(
        provider.was_truncated(),
        "truncate() should be called on the TableProvider"
    );

    Ok(())
}

#[tokio::test]
async fn test_unsupported_table_delete() -> Result<()> {
    let schema = test_schema();
    let ctx = SessionContext::new();

    let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
    ctx.register_table("empty_t", Arc::new(empty_table))?;

    let result = ctx.sql("DELETE FROM empty_t WHERE id = 1").await;
    assert!(result.is_err() || result.unwrap().collect().await.is_err());
    Ok(())
}

#[tokio::test]
async fn test_unsupported_table_update() -> Result<()> {
    let schema = test_schema();
    let ctx = SessionContext::new();

    let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
    ctx.register_table("empty_t", Arc::new(empty_table))?;

    let result = ctx.sql("UPDATE empty_t SET value = 1 WHERE id = 1").await;

    assert!(result.is_err() || result.unwrap().collect().await.is_err());
    Ok(())
}

#[tokio::test]
async fn test_delete_target_table_scoping() -> Result<()> {
    // Test that DELETE only extracts filters from the target table,
    // not from other tables (important for DELETE...FROM safety)
    let target_provider = Arc::new(CaptureDeleteProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table(
        "target_t",
        Arc::clone(&target_provider) as Arc<dyn TableProvider>,
    )?;

    // For now, we test single-table DELETE
    // and validate that the scoping logic is correct
    let df = ctx.sql("DELETE FROM target_t WHERE id > 5").await?;
    df.collect().await?;

    let filters = target_provider
        .captured_filters()
        .expect("filters should be captured");
    assert_eq!(filters.len(), 1);
    assert!(
        filters[0].to_string().contains("id"),
        "Filter should be for id column"
    );
    assert!(
        filters[0].to_string().contains("5"),
        "Filter should contain the value 5"
    );
    Ok(())
}

#[tokio::test]
async fn test_update_from_drops_non_target_predicates() -> Result<()> {
    let target_provider = Arc::new(CaptureUpdateProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t1", Arc::clone(&target_provider) as Arc<dyn TableProvider>)?;

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("status", DataType::Utf8, true),
        // t2-only column to avoid false negatives after qualifier stripping
        Field::new("src_only", DataType::Utf8, true),
    ]));
    let source_table = datafusion::datasource::empty::EmptyTable::new(source_schema);
    ctx.register_table("t2", Arc::new(source_table))?;

    ctx.sql(
        "UPDATE t1 SET value = 1 FROM t2 \
         WHERE t1.id = t2.id AND t2.src_only = 'active' AND t1.value > 10",
    )
    .await?
    .collect()
    .await?;

    let filters = target_provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(
        !filters.is_empty(),
        "expected target predicates extracted from UPDATE ... FROM"
    );

    let has_t2_reference = filters.iter().try_fold(false, |found, expr| {
        expr_has_table_reference(expr, "t2").map(|has_ref| found || has_ref)
    })?;
    assert!(
        !has_t2_reference,
        "filters should only include target-table predicates"
    );

    let filter_strs: Vec<String> = filters.iter().map(|f| f.to_string()).collect();
    assert!(
        filter_strs.iter().any(|s| s.contains("value")),
        "expected target-table predicate to be retained"
    );
    Ok(())
}

#[tokio::test]
async fn test_delete_qualifier_stripping_and_validation() -> Result<()> {
    // Test that filter qualifiers are properly stripped and validated
    // Unqualified predicates should work fine
    let provider = Arc::new(CaptureDeleteProvider::new_with_filter_pushdown(
        test_schema(),
        TableProviderFilterPushDown::Exact,
    ));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::clone(&provider) as Arc<dyn TableProvider>)?;

    // Execute DELETE with unqualified column reference
    // (After parsing, the planner adds qualifiers, but our validation should accept them)
    let df = ctx.sql("DELETE FROM t WHERE id = 1").await?;
    df.collect().await?;

    let filters = provider
        .captured_filters()
        .expect("filters should be captured");
    assert!(!filters.is_empty(), "Should have extracted filter");

    // Verify qualifiers are stripped: check that Column expressions have no qualifier
    let has_qualified_column = filters[0]
        .exists(|expr| Ok(matches!(expr, Expr::Column(col) if col.relation.is_some())))?;
    assert!(
        !has_qualified_column,
        "Filter should have unqualified columns after stripping"
    );

    // Also verify the string representation doesn't contain table qualifiers
    let filter_str = filters[0].to_string();
    assert!(
        !filter_str.contains("t.id"),
        "Filter should not contain qualified column reference, got: {filter_str}"
    );
    assert!(
        filter_str.contains("id") || filter_str.contains("1"),
        "Filter should reference id column or the value 1, got: {filter_str}"
    );
    Ok(())
}

#[tokio::test]
async fn test_unsupported_table_truncate() -> Result<()> {
    let schema = test_schema();
    let ctx = SessionContext::new();

    let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
    ctx.register_table("empty_t", Arc::new(empty_table))?;

    let result = ctx.sql("TRUNCATE TABLE empty_t").await;

    assert!(result.is_err() || result.unwrap().collect().await.is_err());

    Ok(())
}
