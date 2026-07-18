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

//! Regression tests for volatile uncorrelated scalar subquery deduplication.

use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{
    ColumnarValue, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, scalar_subquery,
};
use datafusion::prelude::SessionContext;
use datafusion_common::{Result, ScalarValue};

#[derive(Debug)]
struct VolatileCounter {
    signature: Signature,
    counter: Arc<AtomicI64>,
}

impl VolatileCounter {
    fn new(counter: Arc<AtomicI64>) -> Self {
        Self {
            signature: Signature::nullary(Volatility::Volatile),
            counter,
        }
    }
}

impl PartialEq for VolatileCounter {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for VolatileCounter {}
impl Hash for VolatileCounter {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl ScalarUDFImpl for VolatileCounter {
    fn name(&self) -> &str {
        "volatile_counter"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(value))))
    }
}

fn ctx_with_counter() -> (SessionContext, Arc<AtomicI64>) {
    let ctx = SessionContext::new();
    let counter = Arc::new(AtomicI64::new(0));
    ctx.register_udf(ScalarUDF::new_from_impl(VolatileCounter::new(Arc::clone(
        &counter,
    ))));
    (ctx, counter)
}

async fn collect_sql(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    ctx.sql(sql).await?.collect().await
}

fn int64_value(batches: &[RecordBatch], col: usize) -> i64 {
    batches[0]
        .column(col)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array")
        .value(0)
}

fn assert_counter(counter: &Arc<AtomicI64>, expected: i64) {
    assert_eq!(counter.load(Ordering::SeqCst), expected);
}

fn num_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn same_node_volatile_subqueries_are_evaluated_independently() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();
    let batches = collect_sql(
        &ctx,
        "SELECT (SELECT volatile_counter()) AS a, (SELECT volatile_counter()) AS b",
    )
    .await?;

    let a = int64_value(&batches, 0);
    let b = int64_value(&batches, 1);
    assert_ne!(a, b, "each volatile subquery must produce its own value");
    assert_counter(&counter, 2);
    Ok(())
}

#[tokio::test]
async fn wrapped_volatile_subqueries_are_not_cse_candidates() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();
    let batches = collect_sql(
        &ctx,
        "SELECT (SELECT volatile_counter()) + 1 AS a, \
                (SELECT volatile_counter()) + 1 AS b",
    )
    .await?;

    let a = int64_value(&batches, 0);
    let b = int64_value(&batches, 1);
    assert_ne!(a, b, "wrapped volatile subqueries must not be shared");
    assert_counter(&counter, 2);
    Ok(())
}

#[tokio::test]
async fn cross_node_volatile_subqueries_are_evaluated_independently() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();
    let batches = collect_sql(
        &ctx,
        "SELECT (SELECT volatile_counter()) AS a \
         FROM (SELECT 1) t \
         WHERE (SELECT volatile_counter()) >= 0",
    )
    .await?;

    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert_counter(&counter, 2);
    Ok(())
}

#[tokio::test]
async fn union_filter_does_not_duplicate_volatile_scalar_subquery() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();
    let batches = collect_sql(
        &ctx,
        "SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a) t \
         WHERE (SELECT volatile_counter()) = 0",
    )
    .await?;

    assert_eq!(num_rows(&batches), 2);
    assert_counter(&counter, 1);
    Ok(())
}

#[tokio::test]
async fn join_or_extraction_does_not_duplicate_volatile_scalar_subquery() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();
    let batches = collect_sql(
        &ctx,
        "SELECT l.a, r.b \
         FROM (SELECT 1 AS a) l JOIN (SELECT 2 AS b) r ON true \
         WHERE (l.a = 1 AND (SELECT volatile_counter()) = 0) OR r.b = 3",
    )
    .await?;

    assert_eq!(num_rows(&batches), 1);
    assert_counter(&counter, 1);
    Ok(())
}

#[tokio::test]
async fn shared_volatile_subquery_expr_is_evaluated_per_occurrence() -> Result<()> {
    let (ctx, counter) = ctx_with_counter();

    let subquery_plan = ctx
        .sql("SELECT volatile_counter()")
        .await?
        .into_unoptimized_plan();
    let subquery = scalar_subquery(Arc::new(subquery_plan));

    let plan = LogicalPlanBuilder::empty(true)
        .project(vec![subquery.clone().alias("a"), subquery.alias("b")])?
        .build()?;

    let batches = ctx.execute_logical_plan(plan).await?.collect().await?;

    let a = int64_value(&batches, 0);
    let b = int64_value(&batches, 1);
    assert_ne!(
        a, b,
        "a shared volatile subquery Expr must be evaluated per occurrence"
    );
    assert_counter(&counter, 2);
    Ok(())
}
