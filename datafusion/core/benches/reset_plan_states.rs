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

use std::sync::{Arc, LazyLock};

use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use datafusion_catalog::MemTable;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::displayable;
use datafusion_physical_plan::execution_plan::reset_plan_states;
use tokio::runtime::Runtime;

const NUM_FIELDS: usize = 1000;
const PREDICATE_LEN: usize = 50;

static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(
        (0..NUM_FIELDS)
            .map(|i| Arc::new(Field::new(format!("x_{i}"), DataType::Int64, false)))
            .collect::<Fields>(),
    ))
});

fn col_name(i: usize) -> String {
    format!("x_{i}")
}

fn aggr_name(i: usize) -> String {
    format!("aggr_{i}")
}

fn physical_plan(
    ctx: &SessionContext,
    rt: &Runtime,
    sql: &str,
) -> Arc<dyn ExecutionPlan> {
    rt.block_on(async {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    })
}

fn predicate(col_name: impl Fn(usize) -> String, len: usize) -> String {
    let mut predicate = String::new();
    for i in 0..len {
        if i > 0 {
            predicate.push_str(" AND ");
        }
        predicate.push_str(&col_name(i));
        predicate.push_str(" = ");
        predicate.push_str(&i.to_string());
    }
    predicate
}

/// Returns a typical plan for the query like:
///
/// ```sql
/// SELECT aggr1(col1) as aggr1, aggr2(col2) as aggr2 FROM t
/// WHERE p1
/// HAVING p2
/// ```
///
/// Where `p1` and `p2` some long predicates.
///
fn query1() -> String {
    let mut query = String::new();
    query.push_str("SELECT ");
    for i in 0..NUM_FIELDS {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str("AVG(");
        query.push_str(&col_name(i));
        query.push_str(") AS ");
        query.push_str(&aggr_name(i));
    }
    query.push_str(" FROM t WHERE ");
    query.push_str(&predicate(col_name, PREDICATE_LEN));
    query.push_str(" HAVING ");
    query.push_str(&predicate(aggr_name, PREDICATE_LEN));
    query
}

/// Returns a typical plan for the query like:
///
/// ```sql
/// SELECT projection FROM t JOIN v ON t.a = v.a
/// WHERE p1
/// ```
///
fn query2() -> String {
    let mut query = String::new();
    query.push_str("SELECT ");
    for i in (0..NUM_FIELDS).step_by(2) {
        if i > 0 {
            query.push_str(", ");
        }
        if (i / 2) % 2 == 0 {
            query.push_str(&format!("t.{}", col_name(i)));
        } else {
            query.push_str(&format!("v.{}", col_name(i)));
        }
    }
    query.push_str(" FROM t JOIN v ON t.x_0 = v.x_0 WHERE ");

    fn qualified_name(i: usize) -> String {
        format!("t.{}", col_name(i))
    }

    query.push_str(&predicate(qualified_name, PREDICATE_LEN));
    query
}

/// Returns a typical plan for the query like:
///
/// ```sql
/// SELECT projection FROM t
/// WHERE p
/// ```
///
fn query3() -> String {
    let mut query = String::new();
    query.push_str("SELECT ");

    // Create non-trivial projection.
    for i in 0..NUM_FIELDS / 2 {
        if i > 0 {
            query.push_str(", ");
        }
        query.push_str(&col_name(i * 2));
        query.push_str(" + ");
        query.push_str(&col_name(i * 2 + 1));
    }

    query.push_str(" FROM t WHERE ");
    query.push_str(&predicate(col_name, PREDICATE_LEN));
    query
}

fn run_reset_states(b: &mut criterion::Bencher, plan: &Arc<dyn ExecutionPlan>) {
    b.iter(|| std::hint::black_box(reset_plan_states(Arc::clone(plan)).unwrap()));
}

/// Benchmark is intended to measure overhead of actions, required to perform
/// making an independent instance of the execution plan to re-execute it, avoiding
/// re-planning stage.
fn bench_reset_plan_states(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(MemTable::try_new(Arc::clone(&SCHEMA), vec![vec![], vec![]]).unwrap()),
    )
    .unwrap();

    ctx.register_table(
        "v",
        Arc::new(MemTable::try_new(Arc::clone(&SCHEMA), vec![vec![], vec![]]).unwrap()),
    )
    .unwrap();

    macro_rules! bench_query {
        ($query_producer: expr) => {{
            let sql = $query_producer();
            let plan = physical_plan(&ctx, &rt, &sql);
            log::debug!("plan:\n{}", displayable(plan.as_ref()).indent(true));
            move |b| run_reset_states(b, &plan)
        }};
    }

    c.bench_function("query1", bench_query!(query1));
    c.bench_function("query2", bench_query!(query2));
    c.bench_function("query3", bench_query!(query3));
}

criterion_group!(benches, bench_reset_plan_states);
criterion_main!(benches);
