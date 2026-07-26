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

//! End-to-end test that a *custom* optimizer rule can annotate a
//! `TableScan` with `StatisticsRequest`s and have them reach a *custom*
//! `TableProvider`'s `scan_with_args`.
//!
//! DataFusion ships no rule that populates `TableScan::statistics_requests`
//! and no provider that consumes `ScanArgs::statistics_requests`. This test
//! plays both roles, demonstrating that the request-side hooks are
//! sufficient to build the whole feature outside of DataFusion.

use std::sync::{Arc, Mutex};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, Result};
use datafusion::datasource::TableType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::statistics::StatisticsRequest;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;

/// A custom optimizer rule that annotates every `TableScan` with a
/// `RowCount` request plus a `Min` request for each of its columns.
///
/// This stands in for whatever request-derivation logic an external
/// implementer would write (e.g. Min/Max for sort keys, DistinctCount for
/// join keys). Here it is intentionally trivial and deterministic.
#[derive(Debug)]
struct RequestColumnStatistics;

impl OptimizerRule for RequestColumnStatistics {
    fn name(&self) -> &str {
        "test_request_column_statistics"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::TableScan(mut scan) = plan else {
            return Ok(Transformed::no(plan));
        };
        // Insert into the scan's existing request set. `BTreeSet::insert`
        // reports whether the value was new, so the rule is idempotent — and
        // composes with other rules' requests for free: re-inserting an
        // existing request is a no-op, and we report `Transformed::yes` only
        // when something was actually added, so the optimizer reaches a
        // fixpoint without a manual "already visited" guard.
        let mut changed = scan.statistics_requests.insert(StatisticsRequest::RowCount);
        for field in scan.projected_schema.fields() {
            let req =
                StatisticsRequest::Min(Arc::new(Column::new_unqualified(field.name())));
            changed |= scan.statistics_requests.insert(req);
        }
        Ok(if changed {
            Transformed::yes(LogicalPlan::TableScan(scan))
        } else {
            Transformed::no(LogicalPlan::TableScan(scan))
        })
    }
}

/// A `TableProvider` that records the `statistics_requests` it was asked
/// for, so the test can assert what reached it.
#[derive(Debug)]
struct RecordingTable {
    schema: SchemaRef,
    batch: RecordBatch,
    last_requests: Arc<Mutex<Vec<StatisticsRequest>>>,
}

#[async_trait]
impl TableProvider for RecordingTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        // Record what reached us, then delegate to `scan`.
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
        Arc::clone(&schema),
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
        last_requests: Arc::clone(&last_requests),
    });
    (provider, last_requests)
}

#[tokio::test]
async fn custom_rule_requests_reach_custom_provider() -> Result<()> {
    let (provider, last_requests) = make_table();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rule(Arc::new(RequestColumnStatistics))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("t", provider)?;

    ctx.sql("SELECT a, b FROM t").await?.collect().await?;

    let got = last_requests.lock().unwrap().clone();
    assert_eq!(
        got.len(),
        3,
        "expected RowCount + Min(a) + Min(b), got {got:?}"
    );
    assert!(
        got.contains(&StatisticsRequest::RowCount),
        "expected RowCount, got {got:?}"
    );
    assert!(
        got.contains(&StatisticsRequest::Min(Arc::new(Column::new_unqualified(
            "a"
        )))),
        "expected Min(a), got {got:?}"
    );
    assert!(
        got.contains(&StatisticsRequest::Min(Arc::new(Column::new_unqualified(
            "b"
        )))),
        "expected Min(b), got {got:?}"
    );
    Ok(())
}

#[tokio::test]
async fn no_requests_without_a_rule() -> Result<()> {
    // Without a rule populating `TableScan::statistics_requests`, the
    // provider sees an empty request list — stock DataFusion behavior.
    let (provider, last_requests) = make_table();
    let ctx = SessionContext::new();
    ctx.register_table("t", provider)?;

    ctx.sql("SELECT a, b FROM t").await?.collect().await?;

    assert!(
        last_requests.lock().unwrap().is_empty(),
        "expected no requests without a custom rule"
    );
    Ok(())
}
