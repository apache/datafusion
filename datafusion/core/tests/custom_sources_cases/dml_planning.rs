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
use datafusion::logical_expr::Expr;
use datafusion_catalog::Session;
use datafusion_common::ScalarValue;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;

/// A TableProvider that captures the filters passed to delete_from().
struct CaptureDeleteProvider {
    schema: SchemaRef,
    received_filters: Arc<Mutex<Option<Vec<Expr>>>>,
}

impl CaptureDeleteProvider {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
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
}

/// A TableProvider that captures filters and assignments passed to update().
#[expect(clippy::type_complexity)]
struct CaptureUpdateProvider {
    schema: SchemaRef,
    received_filters: Arc<Mutex<Option<Vec<Expr>>>>,
    received_assignments: Arc<Mutex<Option<Vec<(String, Expr)>>>>,
}

impl CaptureUpdateProvider {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            received_filters: Arc::new(Mutex::new(None)),
            received_assignments: Arc::new(Mutex::new(None)),
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
async fn test_unsupported_table_truncate() -> Result<()> {
    let schema = test_schema();
    let ctx = SessionContext::new();

    let empty_table = datafusion::datasource::empty::EmptyTable::new(schema);
    ctx.register_table("empty_t", Arc::new(empty_table))?;

    let result = ctx.sql("TRUNCATE TABLE empty_t").await;

    assert!(result.is_err() || result.unwrap().collect().await.is_err());

    Ok(())
}
