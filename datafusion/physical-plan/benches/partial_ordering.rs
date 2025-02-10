use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_physical_expr::{expressions::col, LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::aggregates::order::GroupOrderingPartial;



const BATCH_SIZE: usize = 8192;

fn create_test_arrays(num_columns: usize) -> Vec<ArrayRef> {
    (0..num_columns)
        .map(|i| {
            Arc::new(Int32Array::from_iter_values(
                (0..BATCH_SIZE as i32).map(|x| x * (i + 1) as i32)
            )) as ArrayRef
        })
        .collect()
}
fn bench_new_groups(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_ordering_partial");

    // Test with 1, 2, 4, and 8 order indices
    for num_columns in [1, 2, 4, 8] {
        let fields: Vec<Field> = (0..num_columns)
            .map(|i| Field::new(format!("col{}", i), DataType::Int32, false))
            .collect();
        let schema = Schema::new(fields);

        let order_indices: Vec<usize> = (0..num_columns).collect();
        let ordering = LexOrdering::new(
            (0..num_columns)
                .map(|i| {
                    PhysicalSortExpr::new(
                        col(&format!("col{}", i), &schema).unwrap(),
                        SortOptions::default(),
                    )
                })
                .collect(),
        );

        group.bench_function(format!("order_indices_{}", num_columns), |b| {
            let batch_group_values = create_test_arrays(num_columns);
            let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();
            
            b.iter(|| {
                let mut ordering = GroupOrderingPartial::try_new(&schema, &order_indices, &ordering).unwrap();
                ordering.new_groups(&batch_group_values, &group_indices, BATCH_SIZE).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_new_groups);
criterion_main!(benches);