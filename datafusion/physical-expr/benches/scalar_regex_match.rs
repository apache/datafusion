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
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr_common::operator::Operator;
use datafusion_physical_expr::expressions::{binary, col, lit, scalar_regex_match};
use hashbrown::HashMap;
use rand::distributions::{Alphanumeric, DistString};

/// make a record batch with one column and n rows
/// this record batch is single string column is used for
/// scalar regex match benchmarks
fn make_record_batch(rows: usize, string_length: usize, schema: Schema) -> RecordBatch {
    let mut rng = rand::thread_rng();
    let mut array = Vec::with_capacity(rows);
    for _ in 0..rows {
        let data_line = Alphanumeric.sample_string(&mut rng, string_length);
        array.push(Some(data_line));
    }
    let array = StringArray::from(array);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

fn scalar_regex_match_benchmark(c: &mut Criterion) {
    // make common schema
    let column = "string";
    let schema = Schema::new(vec![Field::new(column, DataType::Utf8, true)]);

    // meke test record batch
    let test_batch = [
        (10, make_record_batch(10, 100, schema.clone())),
        (100, make_record_batch(100, 100, schema.clone())),
        (1000, make_record_batch(1000, 100, schema.clone())),
        (2000, make_record_batch(2000, 100, schema.clone())),
    ]
    .iter()
    .map(|(k, v)| (*k, v.clone()))
    .collect::<HashMap<_, _>>();

    // string column
    let string_col = col(column, &schema).unwrap();

    // some pattern literal
    let pattern_lit = [
        ("email".to_string(), lit(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")),
        ("url".to_string(), lit(r"^(https?|ftp)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$")),
        ("ip".to_string(), lit(r"^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")),
        ("phone".to_string(), lit(r"^(\+\d{1,2}\s?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$")),
        ("zip_code".to_string(), lit(r"^\d{5}(?:[-\s]\d{4})?$")),
    ].iter()
    .map(|(k, v)| (k.clone(), v.clone()))
    .collect::<HashMap<_, _>>();

    for (name, regexp_lit) in pattern_lit.iter() {
        for (rows, batch) in test_batch.iter() {
            for iter in [10, 20, 50, 100] {
                // scalar regex match benchmarks
                let bench_name = format!(
                    "scalar_regex_match_pattern_{}_rows_{}_iter_{}",
                    name, rows, iter
                );
                c.bench_function(bench_name.as_str(), |b| {
                    let expr = scalar_regex_match(
                        false,
                        false,
                        string_col.clone(),
                        regexp_lit.clone(),
                        &schema,
                    )
                    .unwrap();
                    b.iter(|| {
                        for _ in 0..iter {
                            expr.evaluate(black_box(batch)).unwrap();
                        }
                    });
                });

                // binary regex match benchmarks
                let bench_name = format!(
                    "binary_regex_match_pattern_{}_rows_{}_iter_{}",
                    name, rows, iter
                );
                c.bench_function(bench_name.as_str(), |b| {
                    let expr = binary(
                        string_col.clone(),
                        Operator::RegexMatch,
                        regexp_lit.clone(),
                        &schema,
                    )
                    .unwrap();
                    b.iter(|| {
                        for _ in 0..iter {
                            expr.evaluate(black_box(batch)).unwrap();
                        }
                    });
                });
            }
        }
    }
}

criterion_group!(benches, scalar_regex_match_benchmark);
criterion_main!(benches);
