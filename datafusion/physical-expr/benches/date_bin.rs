use arrow_array::TimestampNanosecondArray;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::datetime_expressions::date_bin;
use std::sync::Arc;

fn bench_mdn(c: &mut Criterion) {
    let elem_10 = TimestampNanosecondArray::from_iter_values(0..10_i64);
    let elem_1000 = TimestampNanosecondArray::from_iter_values(0..1000_i64);

    let mut group = c.benchmark_group("month-day-nano");
    for elements in [Arc::new(elem_10), Arc::new(elem_1000)].iter() {
        group.throughput(Throughput::Elements(elements.len() as u64));

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::new_interval_mdn(0, 1, 0)),
            ColumnarValue::Array(elements.clone()),
            ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(1), None)),
        ];

        group.bench_with_input(
            BenchmarkId::new("date_bin", elements.len()),
            &args,
            |b, args| {
                b.iter(|| date_bin(args));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_mdn);
criterion_main!(benches);
