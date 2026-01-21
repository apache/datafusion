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

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ToDFSchema;
use datafusion_common::{Column, TableReference};
use datafusion_expr::{Expr, logical_plan::LogicalPlan, projection_schema};
use datafusion_optimizer::optimize_projections::is_projection_unnecessary;
use std::hint::black_box;
use std::sync::Arc;

fn is_projection_unnecessary_old(
    input: &LogicalPlan,
    proj_exprs: &[Expr],
) -> datafusion_common::Result<bool> {
    // First check if all expressions are trivial (cheaper operation than `projection_schema`)
    if !proj_exprs
        .iter()
        .all(|expr| matches!(expr, Expr::Column(_) | Expr::Literal(_, _)))
    {
        return Ok(false);
    }
    let proj_schema = projection_schema(input, proj_exprs)?;
    Ok(&proj_schema == input.schema())
}

fn create_plan_with_many_exprs(num_exprs: usize) -> (LogicalPlan, Vec<Expr>) {
    // Create schema with many fields
    let fields = (0..num_exprs)
        .map(|i| Field::new(format!("col{i}"), DataType::Int32, false))
        .collect::<Vec<_>>();
    let schema = Schema::new(fields);

    // Create table scan
    let table_scan = LogicalPlan::EmptyRelation(datafusion_expr::EmptyRelation {
        produce_one_row: true,
        schema: Arc::new(schema.clone().to_dfschema().unwrap()),
    });

    // Create projection expressions (just column references)
    let exprs = (0..num_exprs)
        .map(|i| Expr::Column(Column::new(None::<TableReference>, format!("col{i}"))))
        .collect();

    (table_scan, exprs)
}

fn benchmark_is_projection_unnecessary(c: &mut Criterion) {
    let (plan, exprs) = create_plan_with_many_exprs(1000);

    let mut group = c.benchmark_group("projection_unnecessary_comparison");

    group.bench_function("is_projection_unnecessary_new", |b| {
        b.iter(|| black_box(is_projection_unnecessary(&plan, &exprs).unwrap()))
    });

    group.bench_function("is_projection_unnecessary_old", |b| {
        b.iter(|| black_box(is_projection_unnecessary_old(&plan, &exprs).unwrap()))
    });

    group.finish();
}

criterion_group!(benches, benchmark_is_projection_unnecessary);
criterion_main!(benches);
