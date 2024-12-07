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

use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::{
    expressions::{binary, col, lit, scalar_regex_match},
    PhysicalExpr,
};
use rand::{
    distributions::{Alphanumeric, DistString},
    rngs::StdRng,
    SeedableRng,
};

/// make a record batch with one column and n rows
/// this record batch is single string column is used for
/// scalar regex match benchmarks
fn make_record_batch(
    batch_iter: usize,
    batch_size: usize,
    string_len: usize,
    schema: &Schema,
) -> Vec<RecordBatch> {
    let mut rng = StdRng::from_seed([123; 32]);
    let mut batches = vec![];
    for _ in 0..batch_iter {
        let array = (0..batch_size)
            .map(|_| Some(Alphanumeric.sample_string(&mut rng, string_len)))
            .collect::<Vec<_>>();
        let array = StringArray::from(array);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)])
            .unwrap();
        batches.push(batch);
    }
    batches
}

/// initialize benchmark data and pattern literals
#[allow(clippy::type_complexity)]
fn init_benchmark() -> (
    Vec<(usize, usize, Vec<RecordBatch>)>,
    Schema,
    Arc<dyn PhysicalExpr>,
    Vec<(String, Arc<dyn PhysicalExpr>)>,
) {
    // make common schema
    let column = "s";
    let schema = Schema::new(vec![Field::new(column, DataType::Utf8, true)]);

    // meke test record batch
    let batch_data = vec![
        // (20, 10_usize, make_record_batch(20, 10, 100, schema.clone())),
        // (20, 100_usize, make_record_batch(20, 100, 100, schema.clone())),
        // (20, 1000_usize, make_record_batch(20, 1000, 100, schema.clone())),
        (
            128_usize,
            4096_usize,
            make_record_batch(128, 4096, 100, &schema),
        ),
    ];

    // string column
    let string_col = col(column, &schema).unwrap();

    // some pattern literal
    let pattern_lit = vec![
        (
            "email".to_string(),
            lit(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
        ),
        (
            "url".to_string(),
            lit(r"^(https?|ftp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$"),
        ),
        (
            "ip".to_string(),
            lit(
                r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
            ),
        ),
        (
            "phone".to_string(),
            lit(r"^(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$"),
        ),
        ("zip_code".to_string(), lit(r"^\d{5}(?:[-\s]\d{4})?$")),
    ];
    (batch_data, schema, string_col, pattern_lit)
}

fn regex_match_benchmark(c: &mut Criterion) {
    let (batch_data, schema, string_col, pattern_lit) = init_benchmark();
    for (name, regexp_lit) in pattern_lit.iter() {
        for (batch_iter, batch_size, batches) in batch_data.iter() {
            let group_name = format!(
                "regex_{}_batch_iter_{}_batch_size_{}",
                name, batch_iter, batch_size
            );
            let mut group = c.benchmark_group(group_name.as_str());
            // binary expr match benchmarks
            group.bench_function("binary_expr_match", |b| {
                b.iter(|| {
                    let expr = binary(
                        string_col.clone(),
                        Operator::RegexMatch,
                        regexp_lit.clone(),
                        &schema,
                    )
                    .unwrap();
                    for batch in batches.iter() {
                        expr.evaluate(batch).unwrap();
                    }
                });
            });
            // scalar regex match benchmarks
            group.bench_function("scalar_regex_match", |b| {
                b.iter(|| {
                    let expr = scalar_regex_match(
                        false,
                        false,
                        string_col.clone(),
                        regexp_lit.clone(),
                        &schema,
                    )
                    .unwrap();
                    for batch in batches.iter() {
                        expr.evaluate(batch).unwrap();
                    }
                });
            });
            group.finish();
        }
    }
}

criterion_group!(benches, regex_match_benchmark);
criterion_main!(benches);
