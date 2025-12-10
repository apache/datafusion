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

use arrow::array::{Array, ArrayRef, Int32Array, Int32Builder, StringArray};
use arrow::datatypes::{ArrowNativeTypeOp, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::test_util::seedable_rng;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, case, col, lit};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;
use rand::distr::Alphanumeric;
use rand::distr::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{Rng, RngCore};
use std::fmt::{Display, Formatter};
use std::hint::black_box;
use std::ops::Range;
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

    benchmark_lookup_table_case_when(c, 8192);
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

struct Options<T> {
    number_of_rows: usize,
    range_of_values: Vec<T>,
    in_range_probability: f32,
    null_probability: f32,
}

fn generate_other_primitive_value<T: ArrowNativeTypeOp + SampleUniform>(
    rng: &mut impl RngCore,
    exclude: &[T],
) -> T {
    let mut value;
    let retry_limit = 100;
    for _ in 0..retry_limit {
        value = rng.random_range(T::MIN_TOTAL_ORDER..=T::MAX_TOTAL_ORDER);
        if !exclude.contains(&value) {
            return value;
        }
    }

    panic!("Could not generate out of range value after {retry_limit} attempts");
}

fn create_random_string_generator(
    length: Range<usize>,
) -> impl Fn(&mut dyn RngCore, &[String]) -> String {
    assert!(length.end > length.start);

    move |rng, exclude| {
        let retry_limit = 100;
        for _ in 0..retry_limit {
            let length = rng.random_range(length.clone());
            let value: String = rng
                .sample_iter(Alphanumeric)
                .take(length)
                .map(char::from)
                .collect();

            if !exclude.contains(&value) {
                return value;
            }
        }

        panic!("Could not generate out of range value after {retry_limit} attempts");
    }
}

/// Create column with the provided number of rows
/// `in_range_percentage` is the percentage of values that should be inside the specified range
/// `null_percentage` is the percentage of null values
/// The rest of the values will be outside the specified range
fn generate_values_for_lookup<T, A>(
    options: &Options<T>,
    generate_other_value: impl Fn(&mut StdRng, &[T]) -> T,
) -> A
where
    T: Clone,
    A: FromIterator<Option<T>>,
{
    // Create a value with specified range most of the time, but also some nulls and the rest is generic

    assert!(
        options.in_range_probability + options.null_probability <= 1.0,
        "Percentages must sum to 1.0 or less"
    );

    let rng = &mut seedable_rng();

    let in_range_probability = 0.0..options.in_range_probability;
    let null_range_probability =
        in_range_probability.start..in_range_probability.start + options.null_probability;
    let out_range_probability = null_range_probability.end..1.0;

    (0..options.number_of_rows)
        .map(|_| {
            let roll: f32 = rng.random();

            match roll {
                v if out_range_probability.contains(&v) => {
                    let index = rng.random_range(0..options.range_of_values.len());
                    // Generate value in range
                    Some(options.range_of_values[index].clone())
                }
                v if null_range_probability.contains(&v) => None,
                _ => {
                    // Generate value out of range
                    Some(generate_other_value(rng, &options.range_of_values))
                }
            }
        })
        .collect::<A>()
}

fn benchmark_lookup_table_case_when(c: &mut Criterion, batch_size: usize) {
    #[derive(Clone, Copy, Debug)]
    struct CaseWhenLookupInput {
        batch_size: usize,

        in_range_probability: f32,
        null_probability: f32,
    }

    impl Display for CaseWhenLookupInput {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "case_when {} rows: in_range: {}, nulls: {}",
                self.batch_size, self.in_range_probability, self.null_probability,
            )
        }
    }

    let mut case_when_lookup = c.benchmark_group("lookup_table_case_when");

    for in_range_probability in [0.1, 0.5, 0.9, 1.0] {
        for null_probability in [0.0, 0.1, 0.5] {
            if in_range_probability + null_probability > 1.0 {
                continue;
            }

            let input = CaseWhenLookupInput {
                batch_size,
                in_range_probability,
                null_probability,
            };

            let when_thens_primitive_to_string = vec![
                (1, "something"),
                (2, "very"),
                (3, "interesting"),
                (4, "is"),
                (5, "going"),
                (6, "to"),
                (7, "happen"),
                (30, "in"),
                (31, "datafusion"),
                (90, "when"),
                (91, "you"),
                (92, "find"),
                (93, "it"),
                (120, "let"),
                (240, "me"),
                (241, "know"),
                (244, "please"),
                (246, "thank"),
                (250, "you"),
                (252, "!"),
            ];
            let when_thens_string_to_primitive = when_thens_primitive_to_string
                .iter()
                .map(|&(key, value)| (value, key))
                .collect_vec();

            for num_entries in [5, 10, 20] {
                for (name, values_range) in [
                    ("all equally true", 0..num_entries),
                    // Test when early termination is beneficial
                    ("only first 2 are true", 0..2),
                ] {
                    let when_thens_primitive_to_string =
                        when_thens_primitive_to_string[values_range.clone()].to_vec();

                    let when_thens_string_to_primitive =
                        when_thens_string_to_primitive[values_range].to_vec();

                    case_when_lookup.bench_with_input(
                        BenchmarkId::new(
                            format!(
                                "case when i32 -> utf8, {num_entries} entries, {name}"
                            ),
                            input,
                        ),
                        &input,
                        |b, input| {
                            let array: Int32Array = generate_values_for_lookup(
                                &Options::<i32> {
                                    number_of_rows: batch_size,
                                    range_of_values: when_thens_primitive_to_string
                                        .iter()
                                        .map(|(key, _)| *key)
                                        .collect(),
                                    in_range_probability: input.in_range_probability,
                                    null_probability: input.null_probability,
                                },
                                |rng, exclude| {
                                    generate_other_primitive_value::<i32>(rng, exclude)
                                },
                            );
                            let batch = RecordBatch::try_new(
                                Arc::new(Schema::new(vec![Field::new(
                                    "col1",
                                    array.data_type().clone(),
                                    true,
                                )])),
                                vec![Arc::new(array)],
                            )
                            .unwrap();

                            let when_thens = when_thens_primitive_to_string
                                .iter()
                                .map(|&(key, value)| (lit(key), lit(value)))
                                .collect();

                            let expr = Arc::new(
                                case(
                                    Some(col("col1", batch.schema_ref()).unwrap()),
                                    when_thens,
                                    Some(lit("whatever")),
                                )
                                .unwrap(),
                            );

                            b.iter(|| {
                                black_box(expr.evaluate(black_box(&batch)).unwrap())
                            })
                        },
                    );

                    case_when_lookup.bench_with_input(
                        BenchmarkId::new(
                            format!(
                                "case when utf8 -> i32, {num_entries} entries, {name}"
                            ),
                            input,
                        ),
                        &input,
                        |b, input| {
                            let array: StringArray = generate_values_for_lookup(
                                &Options::<String> {
                                    number_of_rows: batch_size,
                                    range_of_values: when_thens_string_to_primitive
                                        .iter()
                                        .map(|(key, _)| (*key).to_string())
                                        .collect(),
                                    in_range_probability: input.in_range_probability,
                                    null_probability: input.null_probability,
                                },
                                |rng, exclude| {
                                    create_random_string_generator(3..10)(rng, exclude)
                                },
                            );
                            let batch = RecordBatch::try_new(
                                Arc::new(Schema::new(vec![Field::new(
                                    "col1",
                                    array.data_type().clone(),
                                    true,
                                )])),
                                vec![Arc::new(array)],
                            )
                            .unwrap();

                            let when_thens = when_thens_string_to_primitive
                                .iter()
                                .map(|&(key, value)| (lit(key), lit(value)))
                                .collect();

                            let expr = Arc::new(
                                case(
                                    Some(col("col1", batch.schema_ref()).unwrap()),
                                    when_thens,
                                    Some(lit(1000)),
                                )
                                .unwrap(),
                            );

                            b.iter(|| {
                                black_box(expr.evaluate(black_box(&batch)).unwrap())
                            })
                        },
                    );
                }
            }
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
