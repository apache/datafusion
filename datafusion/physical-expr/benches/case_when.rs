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

use arrow::array::{Array, ArrayRef, Int32Array, Int32Builder};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{case, col, lit, BinaryExpr};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

fn make_x_cmp_y(
    x: &Arc<dyn PhysicalExpr>,
    op: Operator,
    y: i32,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(Arc::clone(x), op, lit(y)))
}

/// Create a record batch with the given number of rows and columns.
/// Columns are named `c<i>` where `i` is the column index.
///
/// The minimum value for `column_count` is `3`.
/// `c1` contains incrementing int32 values
/// `c2` contains int32 values in blocks of 1000 that increment by 1000
/// `c3` contains int32 values with one null inserted every 9 rows
/// `c4` to `cn`, is present, contain unspecified int32 values
fn make_batch(row_count: usize, column_count: usize) -> RecordBatch {
    assert!(column_count >= 3);

    let mut c2 = Int32Builder::new();
    let mut c3 = Int32Builder::new();
    for i in 0..row_count {
        c2.append_value(i as i32 / 1000 * 1000);

        if i % 9 == 0 {
            c3.append_null();
        } else {
            c3.append_value(i as i32);
        }
    }
    let c1 = Arc::new(Int32Array::from_iter_values(0..row_count as i32));
    let c2 = Arc::new(c2.finish());
    let c3 = Arc::new(c3.finish());
    let mut columns: Vec<ArrayRef> = vec![c1, c2, c3];
    for _ in 3..column_count {
        columns.push(Arc::new(Int32Array::from_iter_values(0..row_count as i32)));
    }

    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            Field::new(
                format!("c{}", i + 1),
                c.data_type().clone(),
                c.is_nullable(),
            )
        })
        .collect::<Vec<_>>();

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(Arc::clone(&schema), columns).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    run_benchmarks(c, &make_batch(8192, 3));
    run_benchmarks(c, &make_batch(8192, 50));
    run_benchmarks(c, &make_batch(8192, 100));
}

fn run_benchmarks(c: &mut Criterion, batch: &RecordBatch) {
    let c1 = col("c1", &batch.schema()).unwrap();
    let c2 = col("c2", &batch.schema()).unwrap();
    let c3 = col("c3", &batch.schema()).unwrap();

    // No expression, when/then/else, literal values
    c.bench_function(
        format!(
            "case_when {}x{}: CASE WHEN c1 <= 500 THEN 1 ELSE 0 END",
            batch.num_rows(),
            batch.num_columns()
        )
        .as_str(),
        |b| {
            let expr = Arc::new(
                case(
                    None,
                    vec![(make_x_cmp_y(&c1, Operator::LtEq, 500), lit(1))],
                    Some(lit(0)),
                )
                .unwrap(),
            );
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        },
    );

    // No expression, when/then/else, column reference values
    c.bench_function(
        format!(
            "case_when {}x{}: CASE WHEN c1 <= 500 THEN c2 ELSE c3 END",
            batch.num_rows(),
            batch.num_columns()
        )
        .as_str(),
        |b| {
            let expr = Arc::new(
                case(
                    None,
                    vec![(make_x_cmp_y(&c1, Operator::LtEq, 500), Arc::clone(&c2))],
                    Some(Arc::clone(&c3)),
                )
                .unwrap(),
            );
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        },
    );

    // No expression, when/then, implicit else
    c.bench_function(
        format!(
            "case_when {}x{}: CASE WHEN c1 <= 500 THEN c2 [ELSE NULL] END",
            batch.num_rows(),
            batch.num_columns()
        )
        .as_str(),
        |b| {
            let expr = Arc::new(
                case(
                    None,
                    vec![(make_x_cmp_y(&c1, Operator::LtEq, 500), Arc::clone(&c2))],
                    None,
                )
                .unwrap(),
            );
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        },
    );

    // With expression, two when/then branches
    c.bench_function(
        format!(
            "case_when {}x{}: CASE c1 WHEN 1 THEN c2 WHEN 2 THEN c3 END",
            batch.num_rows(),
            batch.num_columns()
        )
        .as_str(),
        |b| {
            let expr = Arc::new(
                case(
                    Some(Arc::clone(&c1)),
                    vec![(lit(1), Arc::clone(&c2)), (lit(2), Arc::clone(&c3))],
                    None,
                )
                .unwrap(),
            );
            b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
        },
    );

    // Many when/then branches where all are effectively reachable
    c.bench_function(format!("case_when {}x{}: CASE WHEN c1 == 0 THEN 0 WHEN c1 == 1 THEN 1 ... WHEN c1 == n THEN n ELSE n + 1 END", batch.num_rows(), batch.num_columns()).as_str(), |b| {
        let when_thens = (0..batch.num_rows() as i32).map(|i| (make_x_cmp_y(&c1, Operator::Eq, i), lit(i))).collect();
        let expr = Arc::new(
            case(
                None,
                when_thens,
                Some(lit(batch.num_rows() as i32))
            )
                .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
    });

    // Many when/then branches where all but the first few are effectively unreachable
    c.bench_function(format!("case_when {}x{}: CASE WHEN c1 < 0 THEN 0 WHEN c1 < 1000 THEN 1 ... WHEN c1 < n * 1000 THEN n ELSE n + 1 END", batch.num_rows(), batch.num_columns()).as_str(), |b| {
        let when_thens = (0..batch.num_rows() as i32).map(|i| (make_x_cmp_y(&c1, Operator::Lt, i * 1000), lit(i))).collect();
        let expr = Arc::new(
            case(
                None,
                when_thens,
                Some(lit(batch.num_rows() as i32))
            )
                .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
    });

    // Many when/then branches where all are effectively reachable
    c.bench_function(format!("case_when {}x{}: CASE c1 WHEN 0 THEN 0 WHEN 1 THEN 1 ... WHEN n THEN n ELSE n + 1 END", batch.num_rows(), batch.num_columns()).as_str(), |b| {
        let when_thens = (0..batch.num_rows() as i32).map(|i| (lit(i), lit(i))).collect();
        let expr = Arc::new(
            case(
                Some(Arc::clone(&c1)),
                when_thens,
                Some(lit(batch.num_rows() as i32))
            )
                .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
    });

    // Many when/then branches where all but the first few are effectively unreachable
    c.bench_function(format!("case_when {}x{}: CASE c2 WHEN 0 THEN 0 WHEN 1000 THEN 1 ... WHEN n * 1000 THEN n ELSE n + 1 END", batch.num_rows(), batch.num_columns()).as_str(), |b| {
        let when_thens = (0..batch.num_rows() as i32).map(|i| (lit(i * 1000), lit(i))).collect();
        let expr = Arc::new(
            case(
                Some(Arc::clone(&c2)),
                when_thens,
                Some(lit(batch.num_rows() as i32))
            )
                .unwrap(),
        );
        b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
