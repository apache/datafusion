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

use arrow::array::{ArrayRef, StringDictionaryBuilder};
use arrow::datatypes::{DataType, Field, Schema, UInt8Type};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_expr::EmitTo;
use datafusion_physical_plan::aggregates::group_values::single_group_by::dictionary::GroupValuesDictionary;
use datafusion_physical_plan::aggregates::group_values::{GroupValues, new_group_values};
use datafusion_physical_plan::aggregates::order::GroupOrdering;
use std::sync::Arc;
#[derive(Debug)]
enum Cardinality {
    Xsmall, // 1
    Small,  // 10
    Medium, // 50
    Large,  // 200
}
#[derive(Debug)]
enum BatchSize {
    Small,  // 8192
    Medium, // 32768
    Large,  // 65536
}
#[derive(Debug)]
enum NullRate {
    Zero,   // 0%
    Low,    // 1%
    Medium, // 5%
    High,   // 20%
}
#[derive(Debug, Clone)]
enum GroupType {
    Dictionary,
    GroupValueRows,
}
fn create_string_values(cardinality: &Cardinality) -> Vec<String> {
    let num_values = match cardinality {
        Cardinality::Xsmall => 3,
        Cardinality::Small => 10,
        Cardinality::Medium => 50,
        Cardinality::Large => 200,
    };
    (0..num_values)
        .map(|i| format!("group_value_{i:06}"))
        .collect()
}
fn create_batch(batch_size: &BatchSize, cardinality: &Cardinality) -> Vec<String> {
    let size = match batch_size {
        BatchSize::Small => 8192,
        BatchSize::Medium => 32768,
        BatchSize::Large => 65536,
    };
    let unique_strings = create_string_values(cardinality);
    if unique_strings.is_empty() {
        return Vec::new();
    }

    unique_strings.iter().cycle().take(size).cloned().collect()
}
fn strings_to_dict_array(values: Vec<Option<String>>) -> ArrayRef {
    let mut builder = StringDictionaryBuilder::<UInt8Type>::new();
    for v in values {
        match v {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    Arc::new(builder.finish())
}
fn introduce_nulls(values: Vec<String>, null_rate: &NullRate) -> Vec<Option<String>> {
    let rate = match null_rate {
        NullRate::Zero => 0.0,
        NullRate::Low => 0.01,
        NullRate::Medium => 0.05,
        NullRate::High => 0.20,
    };
    values
        .into_iter()
        .map(|v| {
            if rand::random::<f64>() < rate {
                None
            } else {
                Some(v)
            }
        })
        .collect()
}

fn generate_group_values(kind: &GroupType) -> Box<dyn GroupValues> {
    match kind {
        GroupType::GroupValueRows => {
            // we know this is going to hit the fallback path I.E GroupValueRows, but for the sake of avoiding making private items public call the public api
            let schema = Arc::new(Schema::new(vec![Field::new(
                "group_col",
                DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
                false,
            )]));
            new_group_values(schema, &GroupOrdering::None).unwrap()
        }
        GroupType::Dictionary => {
            // call custom path directly
            Box::new(GroupValuesDictionary::<UInt8Type>::new(&DataType::Utf8))
        }
    }
}

fn bench_single_column_group_values(c: &mut Criterion) {
    let group_types = [GroupType::GroupValueRows, GroupType::Dictionary];
    let cardinalities = [
        Cardinality::Xsmall,
        Cardinality::Small,
        Cardinality::Medium,
        Cardinality::Large,
    ];
    let batch_sizes = [BatchSize::Small, BatchSize::Medium, BatchSize::Large];
    let null_rates = [
        NullRate::Zero,
        NullRate::Low,
        NullRate::Medium,
        NullRate::High,
    ];

    for cardinality in &cardinalities {
        for batch_size in &batch_sizes {
            for null_rate in &null_rates {
                for group_type in &group_types {
                    let group_name = format!(
                        "t1_{group_type:?}_cardinality_{cardinality:?}_batch_{batch_size:?}_null_rate_{null_rate:?}"
                    );

                    let string_vec = create_batch(batch_size, cardinality);
                    let nullable_values = introduce_nulls(string_vec, null_rate);
                    let col_ref = match group_type {
                        GroupType::Dictionary | GroupType::GroupValueRows => {
                            strings_to_dict_array(nullable_values.clone())
                        }
                    };
                    c.bench_function(&group_name, |b| {
                        b.iter_batched(
                            || {
                                //create fresh group values for each iteration
                                let gv = generate_group_values(group_type);
                                let col = col_ref.clone();
                                (gv, col)
                            },
                            |(mut group_values, col)| {
                                let mut groups = Vec::new();
                                group_values.intern(&[col], &mut groups).unwrap();
                                //group_values.emit(EmitTo::All).unwrap();
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    });

                    //  Second benchmark that alternates between intern and emit to simulate more realistic usage patterns where the same group values is used across multiple batches of the same grouping column
                    let multi_batch_name = format!(
                        "multi_batch/{group_type:?}_cardinality_{cardinality:?}_batch_{batch_size:?}_null_rate_{null_rate:?}"
                    );
                    c.bench_function(&multi_batch_name, |b| {
                        b.iter_batched(
                            || {
                                // setup - create 3 batches to simulate multiple record batches
                                let gv = generate_group_values(group_type);
                                let batch1 = col_ref.clone();
                                let batch2 = col_ref.clone();
                                let batch3 = col_ref.clone();
                                (gv, batch1, batch2, batch3)
                            },
                            |(mut group_values, batch1, batch2, batch3)| {
                                // simulate realistic aggregation flow:
                                // multiple intern calls (one per record batch)
                                // followed by emit
                                let mut groups = Vec::new();

                                group_values.intern(&[batch1], &mut groups).unwrap();
                                groups.clear();
                                group_values.intern(&[batch2], &mut groups).unwrap();
                                groups.clear();
                                group_values.intern(&[batch3], &mut groups).unwrap();

                                // emit once at the end like the real aggregation flow
                                group_values.emit(EmitTo::All).unwrap();
                            },
                            criterion::BatchSize::SmallInput,
                        );
                    });
                }
            }
        }
    }
}

fn bench_repeated_intern_prefab_cols(c: &mut Criterion) {
    let cardinality = Cardinality::Small;
    let batch_size = BatchSize::Large;
    let null_rate = NullRate::Low;
    let group_types = [GroupType::GroupValueRows, GroupType::Dictionary];

    for group_type in &group_types {
        let group_type = group_type.clone();
        let string_vec = create_batch(&batch_size, &cardinality);
        let nullable_values = introduce_nulls(string_vec, &null_rate);
        let col_ref = match group_type {
            GroupType::Dictionary | GroupType::GroupValueRows => {
                strings_to_dict_array(nullable_values.clone())
            }
        };

        // Build once outside the benchmark iteration and reuse in intern calls.
        let arr1 = col_ref.clone();
        let arr2 = col_ref.clone();
        let arr3 = col_ref.clone();
        let arr4 = col_ref.clone();

        let group_name = format!(
            "repeated_intern/{group_type:?}_cardinality_{cardinality:?}_batch_{batch_size:?}_null_rate_{null_rate:?}"
        );
        c.bench_function(&group_name, |b| {
            b.iter_batched(
                || generate_group_values(&group_type),
                |mut group_values| {
                    let mut groups = Vec::new();

                    group_values
                        .intern(std::slice::from_ref(&arr1), &mut groups)
                        .unwrap();
                    groups.clear();
                    group_values
                        .intern(std::slice::from_ref(&arr2), &mut groups)
                        .unwrap();
                    groups.clear();
                    group_values
                        .intern(std::slice::from_ref(&arr3), &mut groups)
                        .unwrap();
                    groups.clear();
                    group_values
                        .intern(std::slice::from_ref(&arr4), &mut groups)
                        .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

criterion_group!(
    benches,
    bench_single_column_group_values,
    bench_repeated_intern_prefab_cols
);
criterion_main!(benches);
