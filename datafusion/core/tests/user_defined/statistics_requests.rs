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

//! End-to-end test that the optimizer-derived `StatisticsRequest`s
//! reach a custom `TableProvider`'s `scan_with_args`.

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_expr_common::statistics::StatisticsRequest;

/// A `TableProvider` that records the last `statistics_requests` it was
/// asked for, so the test can assert what reached it.
#[derive(Debug)]
struct RecordingTable {
    schema: SchemaRef,
    batch: RecordBatch,
    last_requests: Arc<Mutex<Vec<StatisticsRequest>>>,
}

#[async_trait]
impl TableProvider for RecordingTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            self.schema.clone(),
            projection.cloned(),
        )?)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        // Record what reached us, then delegate to scan().
        *self.last_requests.lock().unwrap() = args.statistics_requests().to_vec();
        let plan = self
            .scan(
                state,
                args.projection().map(|p| p.to_vec()).as_ref(),
                args.filters().unwrap_or(&[]),
                args.limit(),
            )
            .await?;
        Ok(ScanResult::new(plan))
    }
}

fn make_table() -> (Arc<RecordingTable>, Arc<Mutex<Vec<StatisticsRequest>>>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let last_requests = Arc::new(Mutex::new(Vec::new()));
    let provider = Arc::new(RecordingTable {
        schema,
        batch,
        last_requests: last_requests.clone(),
    });
    (provider, last_requests)
}

#[tokio::test]
async fn requests_reach_provider_scan_with_args() -> Result<()> {
    let (provider, last_requests) = make_table();
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    // Filter on `a` + sort on `b` should request Min/Max/NullCount on
    // both, plus DistinctCount on `a` (filter), plus a RowCount.
    let _ = ctx
        .sql("SELECT a, b FROM t WHERE a > 0 ORDER BY b LIMIT 10")
        .await?
        .collect()
        .await?;

    let got = last_requests.lock().unwrap().clone();
    assert!(!got.is_empty(), "expected non-empty requests, got {got:?}");

    let has = |needle: &StatisticsRequest| got.iter().any(|r| r == needle);
    use datafusion_common::Column;
    use datafusion_expr_common::statistics::StatisticsRequest::*;
    assert!(has(&RowCount), "expected RowCount, got {got:?}");
    assert!(
        has(&Min(Column::new_unqualified("a"))),
        "expected Min(a), got {got:?}"
    );
    assert!(
        has(&DistinctCount(Column::new_unqualified("a"))),
        "expected DistinctCount(a), got {got:?}"
    );
    assert!(
        has(&Min(Column::new_unqualified("b"))),
        "expected Min(b) from ORDER BY, got {got:?}"
    );

    Ok(())
}

#[tokio::test]
async fn no_requests_when_plan_has_no_filter_sort_or_join() -> Result<()> {
    let (provider, last_requests) = make_table();
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    // Plain `SELECT *` — only `RowCount` should be requested.
    let _ = ctx.sql("SELECT a, b FROM t").await?.collect().await?;

    let got = last_requests.lock().unwrap().clone();
    use datafusion_expr_common::statistics::StatisticsRequest::*;
    assert_eq!(got.len(), 1, "expected only RowCount, got {got:?}");
    assert!(matches!(got[0], RowCount));

    Ok(())
}
