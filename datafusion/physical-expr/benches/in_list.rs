use arrow::array::{Array, ArrayRef, Float32Array, Int32Array, StringArray};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_physical_expr::expressions::{col, in_list, lit};
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::sync::Arc;

fn do_bench(c: &mut Criterion, name: &str, values: ArrayRef, exprs: &[ScalarValue]) {
    let schema = Schema::new(vec![Field::new("a", values.data_type().clone(), true)]);
    let exprs = exprs.iter().map(|s| lit(s.clone())).collect();
    let expr = in_list(col("a", &schema).unwrap(), exprs, &false, &schema).unwrap();
    let batch = RecordBatch::try_new(Arc::new(schema), vec![values]).unwrap();

    c.bench_function(name, |b| {
        b.iter(|| black_box(expr.evaluate(black_box(&batch)).unwrap()))
    });
}

fn random_string(rng: &mut StdRng, len: usize) -> String {
    let value = rng.sample_iter(&Alphanumeric).take(len).collect();
    String::from_utf8(value).unwrap()
}

fn do_benches(
    c: &mut Criterion,
    array_length: usize,
    in_list_length: usize,
    null_percent: f64,
) {
    let mut rng = StdRng::seed_from_u64(120320);
    for string_length in [5, 10, 20] {
        let values: StringArray = (0..array_length)
            .map(|_| {
                rng.gen_bool(null_percent)
                    .then(|| random_string(&mut rng, string_length))
            })
            .collect();

        let in_list: Vec<_> = (0..in_list_length)
            .map(|_| ScalarValue::Utf8(Some(random_string(&mut rng, string_length))))
            .collect();

        do_bench(
            c,
            &format!(
                "in_list_utf8({}) ({}, {}) IN ({}, 0)",
                string_length, array_length, null_percent, in_list_length
            ),
            Arc::new(values),
            &in_list,
        )
    }

    let values: Float32Array = (0..array_length)
        .map(|_| rng.gen_bool(null_percent).then(|| rng.gen()))
        .collect();

    let in_list: Vec<_> = (0..in_list_length)
        .map(|_| ScalarValue::Float32(Some(rng.gen())))
        .collect();

    do_bench(
        c,
        &format!(
            "in_list_f32 ({}, {}) IN ({}, 0)",
            array_length, null_percent, in_list_length
        ),
        Arc::new(values),
        &in_list,
    );

    let values: Int32Array = (0..array_length)
        .map(|_| rng.gen_bool(null_percent).then(|| rng.gen()))
        .collect();

    let in_list: Vec<_> = (0..in_list_length)
        .map(|_| ScalarValue::Int32(Some(rng.gen())))
        .collect();

    do_bench(
        c,
        &format!(
            "in_list_i32 ({}, {}) IN ({}, 0)",
            array_length, null_percent, in_list_length
        ),
        Arc::new(values),
        &in_list,
    )
}

fn criterion_benchmark(c: &mut Criterion) {
    for array_length in [100, 200, 1024] {
        for in_list_length in [1, 4, 10, 50, 100] {
            for null_percent in [0., 0.2] {
                do_benches(c, array_length, in_list_length, null_percent)
            }
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
