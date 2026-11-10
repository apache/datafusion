// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{Array, ArrayRef, Int32Array, Int32Builder, StringArray};
use arrow::datatypes::{ArrowNativeTypeOp, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, case, col, lit};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::Itertools;
use rand::distr::Alphanumeric;
use rand::distr::uniform::SampleUniform;
use rand::rngs::StdRng;
use rand::{Rng, RngCore, SeedableRng};

/// Returns a fixed-seed RNG using this crate's `rand` version (arrow's
/// `test_util::seedable_rng` returns its own `rand` version's `StdRng`)
fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}
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
    benchmark_divide_by_zero_protection(c, 8192);
    operand_reuse::benchmarks(c);
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

fn benchmark_divide_by_zero_protection(c: &mut Criterion, batch_size: usize) {
    let mut group = c.benchmark_group("divide_by_zero_protection");

    for zero_percentage in [0.0, 0.1, 0.5, 0.9] {
        let rng = &mut seedable_rng();

        let numerator: Int32Array =
            (0..batch_size).map(|_| Some(rng.random::<i32>())).collect();

        let divisor_values: Vec<Option<i32>> = (0..batch_size)
            .map(|_| {
                let roll: f32 = rng.random();
                if roll < zero_percentage {
                    Some(0)
                } else {
                    let mut val = rng.random::<i32>();
                    while val == 0 {
                        val = rng.random::<i32>();
                    }
                    Some(val)
                }
            })
            .collect();

        let divisor: Int32Array = divisor_values.iter().copied().collect();
        let divisor_copy: Int32Array = divisor_values.iter().copied().collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("numerator", numerator.data_type().clone(), true),
            Field::new("divisor", divisor.data_type().clone(), true),
            Field::new("divisor_copy", divisor_copy.data_type().clone(), true),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(numerator),
                Arc::new(divisor),
                Arc::new(divisor_copy),
            ],
        )
        .unwrap();

        let numerator_col = col("numerator", &batch.schema()).unwrap();
        let divisor_col = col("divisor", &batch.schema()).unwrap();

        // DivideByZeroProtection: WHEN condition checks `divisor_col > 0` and division
        // uses `divisor_col` as divisor. Since the checked column matches the divisor,
        // this triggers the DivideByZeroProtection optimization.
        group.bench_function(
            format!(
                "{} rows, {}% zeros: DivideByZeroProtection",
                batch_size,
                (zero_percentage * 100.0) as i32
            ),
            |b| {
                let when = Arc::new(BinaryExpr::new(
                    Arc::clone(&divisor_col),
                    Operator::NotEq,
                    lit(0i32),
                ));
                let then = Arc::new(BinaryExpr::new(
                    Arc::clone(&numerator_col),
                    Operator::Divide,
                    Arc::clone(&divisor_col),
                ));
                let else_null: Arc<dyn PhysicalExpr> = lit(ScalarValue::Int32(None));
                let expr =
                    Arc::new(case(None, vec![(when, then)], Some(else_null)).unwrap());

                b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
            },
        );
    }

    group.finish();
}

// Physical callers opt in through an API that SQL planning does not expose.
mod operand_reuse {
    use super::*;
    use arrow::array::StringViewArray;
    use datafusion_common::Result;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ColumnarValue;
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{CaseExpr, RequiredOperandReusePolicy};
    use std::hash::{Hash, Hasher};
    use std::sync::atomic::{AtomicUsize, Ordering};

    const ROWS: usize = 8192;
    // MD5 of 4096 ASCII 'a' and 'b' bytes, respectively.
    const HASH_A: &str = "21a199c53f422a380e20b162fb6ebe9c";
    const HASH_B: &str = "bdb166b6061ee9331efe719dcee62b41";

    #[derive(Debug)]
    struct Policy;

    impl RequiredOperandReusePolicy for Policy {
        fn first_required_child(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<usize> {
            // Only Eq's left child is traversed in these fixtures.
            expr.downcast_ref::<BinaryExpr>()
                .filter(|expr| *expr.op() == Operator::Eq)
                .map(|_| 0)
        }

        fn same_call(
            &self,
            authorized: &Arc<dyn PhysicalExpr>,
            rewritten: &Arc<dyn PhysicalExpr>,
        ) -> bool {
            authorized.eq(rewritten)
        }
    }

    // Used only by untimed validation. Timed expressions contain the raw operand.
    #[derive(Debug)]
    struct Counted {
        inner: Arc<dyn PhysicalExpr>,
        rows: Arc<AtomicUsize>,
    }

    impl PartialEq for Counted {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner)
        }
    }
    impl Eq for Counted {}
    impl Hash for Counted {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }
    impl Display for Counted {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "counted({})", self.inner)
        }
    }
    impl PhysicalExpr for Counted {
        fn data_type(&self, schema: &Schema) -> Result<arrow::datatypes::DataType> {
            self.inner.data_type(schema)
        }
        fn nullable(&self, schema: &Schema) -> Result<bool> {
            self.inner.nullable(schema)
        }
        fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
            self.rows.fetch_add(batch.num_rows(), Ordering::Relaxed);
            self.inner.evaluate(batch)
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.inner]
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            assert_eq!(children.len(), 1);
            Ok(Arc::new(Self {
                inner: Arc::clone(&children[0]),
                rows: Arc::clone(&self.rows),
            }))
        }
        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    struct Fixture {
        batches: Vec<RecordBatch>,
        expected: Vec<ArrayRef>,
        operand: Arc<dyn PhysicalExpr>,
        when_value: ScalarValue,
        then_value: ScalarValue,
        else_rows: usize,
    }

    impl Fixture {
        fn new(expensive: bool, column_count: usize, else_percent: usize) -> Self {
            let else_rows = ROWS * else_percent / 100;
            let mut batches = Vec::new();
            let mut expected: Vec<ArrayRef> = Vec::new();
            // Rotate which rows reach ELSE between batches, keeping its fraction fixed.
            for offset in [0, ROWS / 2] {
                let is_else = |row: usize| (row + offset) % ROWS < else_rows;
                let values: ArrayRef = if expensive {
                    let a = "a".repeat(4096);
                    let b = "b".repeat(4096);
                    expected.push(Arc::new(StringViewArray::from_iter_values(
                        (0..ROWS).map(|row| if is_else(row) { HASH_B } else { "then" }),
                    )));
                    Arc::new(StringArray::from_iter_values(
                        (0..ROWS).map(
                            |row| if is_else(row) { b.as_str() } else { a.as_str() },
                        ),
                    ))
                } else {
                    expected.push(Arc::new(Int32Array::from_iter_values(
                        (0..ROWS).map(|row| if is_else(row) { 2 } else { 0 }),
                    )));
                    Arc::new(Int32Array::from_iter_values(
                        (0..ROWS).map(|row| i32::from(is_else(row))),
                    ))
                };
                let mut columns = vec![values];
                // Unreferenced columns exercise input-schema and batch-append costs.
                // Sharing their buffer keeps their contents irrelevant to the fixture.
                let unused: ArrayRef = Arc::new(Int32Array::from(vec![0; ROWS]));
                columns.resize_with(column_count, || Arc::clone(&unused));
                let fields = columns
                    .iter()
                    .enumerate()
                    .map(|(index, array)| {
                        let name = if index == 0 {
                            "value".to_owned()
                        } else {
                            format!("unused_{index}")
                        };
                        Field::new(name, array.data_type().clone(), false)
                    })
                    .collect::<Vec<_>>();
                batches.push(
                    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap(),
                );
            }
            let value = col("value", batches[0].schema_ref()).unwrap();
            let (operand, when_value, then_value): (Arc<dyn PhysicalExpr>, _, _) =
                if expensive {
                    (
                        Arc::new(
                            ScalarFunctionExpr::try_new(
                                datafusion_functions::crypto::md5(),
                                vec![value],
                                batches[0].schema_ref(),
                                Arc::new(ConfigOptions::new()),
                            )
                            .unwrap(),
                        ),
                        ScalarValue::Utf8View(Some(HASH_A.to_owned())),
                        ScalarValue::Utf8View(Some("then".to_owned())),
                    )
                } else {
                    (
                        Arc::new(BinaryExpr::new(value, Operator::Plus, lit(1i32))),
                        ScalarValue::Int32(Some(1)),
                        ScalarValue::Int32(Some(0)),
                    )
                };
            Self {
                batches,
                expected,
                operand,
                when_value,
                then_value,
                else_rows,
            }
        }

        fn case(&self, reuse: bool, operand: &Arc<dyn PhysicalExpr>) -> CaseExpr {
            let condition = Arc::new(BinaryExpr::new(
                Arc::clone(operand),
                Operator::Eq,
                lit(self.when_value.clone()),
            ));
            let expr = CaseExpr::try_new(
                None,
                vec![(condition, lit(self.then_value.clone()))],
                Some(Arc::clone(operand)),
            )
            .unwrap();
            if reuse {
                expr.with_required_operand_reuse(operand, Arc::new(Policy))
            } else {
                expr
            }
        }

        fn validate(&self) {
            // Equality includes the grant: verify the unwrapped timed tree did not fall back.
            assert_ne!(
                self.case(true, &self.operand),
                self.case(false, &self.operand)
            );
            for reuse in [false, true] {
                let rows = Arc::new(AtomicUsize::new(0));
                let counted: Arc<dyn PhysicalExpr> = Arc::new(Counted {
                    inner: Arc::clone(&self.operand),
                    rows: Arc::clone(&rows),
                });
                let counted = self.case(reuse, &counted);
                let timed = self.case(reuse, &self.operand);
                // Check both first and subsequent evaluations, including changed rows.
                for (batch, expected) in self.batches.iter().zip(&self.expected) {
                    rows.store(0, Ordering::Relaxed);
                    let actual =
                        counted.evaluate(batch).unwrap().into_array(ROWS).unwrap();
                    assert_eq!(actual.as_ref(), expected.as_ref());
                    assert_eq!(
                        rows.load(Ordering::Relaxed),
                        ROWS + if reuse { 0 } else { self.else_rows }
                    );
                    let actual = timed.evaluate(batch).unwrap().into_array(ROWS).unwrap();
                    assert_eq!(actual.as_ref(), expected.as_ref());
                }
            }
        }
    }

    pub fn benchmarks(c: &mut Criterion) {
        let mut group = c.benchmark_group("case_operand_reuse");
        for (kind, expensive, columns) in [
            ("add_1col", false, 1),
            ("add_100cols", false, 100),
            ("md5_4096_1col", true, 1),
        ] {
            for else_percent in [0, 50, 100] {
                let fixture = Fixture::new(expensive, columns, else_percent);
                fixture.validate();
                eprintln!(
                    "validated {kind}/else_{else_percent}: outputs and operand rows; off={}, on={ROWS}",
                    ROWS + fixture.else_rows
                );
                for reuse in [false, true] {
                    let mode = if reuse { "on" } else { "off" };
                    group.bench_function(
                        format!("{kind}/else_{else_percent}/first/{mode}"),
                        |b| {
                            let mut index = 0;
                            b.iter(|| {
                                let batch = black_box(&fixture.batches[index]);
                                index ^= 1;
                                // Include CASE construction, permission checks and its lazy
                                // prepared plan. The operand and inputs are preplanned.
                                let expr = fixture.case(reuse, &fixture.operand);
                                black_box(expr.evaluate(batch).unwrap())
                            });
                        },
                    );
                    group.bench_function(
                        format!("{kind}/else_{else_percent}/repeated/{mode}"),
                        |b| {
                            let expr = fixture.case(reuse, &fixture.operand);
                            black_box(expr.evaluate(&fixture.batches[0]).unwrap());
                            let mut index = 0;
                            b.iter(|| {
                                let batch = black_box(&fixture.batches[index]);
                                index ^= 1;
                                // Arrow evaluation is eager: consume the entire result
                                // without adding a separate checksum scan to timing.
                                black_box(expr.evaluate(batch).unwrap())
                            });
                        },
                    );
                }
            }
        }
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
