use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{col, SortExpr};
use datafusion::prelude::*;
use datafusion_common::Result;
use std::sync::Arc;
use tokio::runtime::Runtime;

fn create_presorted_data(rows: usize, groups: usize) -> Result<RecordBatch> {
    let group_size = rows / groups;
    let mut a_vals = Vec::with_capacity(rows);
    let mut b_vals = Vec::with_capacity(rows);
    let mut c_vals = Vec::with_capacity(rows);

    // Create data pre-sorted on (a, b) but not on c
    for group in 0..groups {
        for i in 0..group_size {
            a_vals.push(group as i32);
            b_vals.push(i as i32);
            c_vals.push((rows - (group * group_size + i)) as i32); // Reverse order for c
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(a_vals)),
            Arc::new(Int32Array::from(b_vals)),
            Arc::new(Int32Array::from(c_vals)),
        ],
    )?)
}

fn create_random_data(rows: usize) -> Result<RecordBatch> {
    use rand::Rng;
    let mut rng = rand::rng();
    
    let a_vals: Vec<i32> = (0..rows).map(|_| rng.random_range(0..100)).collect();
    let b_vals: Vec<i32> = (0..rows).map(|_| rng.random_range(0..100)).collect();
    let c_vals: Vec<i32> = (0..rows).map(|_| rng.random_range(0..1000)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(a_vals)),
            Arc::new(Int32Array::from(b_vals)),
            Arc::new(Int32Array::from(c_vals)),
        ],
    )?)
}

async fn benchmark_partial_sort_scenario(rows: usize) -> Result<f64> {
    let ctx = SessionContext::new();
    let batch = create_presorted_data(rows, rows / 100)?;
    let schema = batch.schema();

    // Create sort expressions for (a, b) ordering
    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false), // ascending, nulls last
        SortExpr::new(col("b"), true, false), // ascending, nulls last
    ];

    // Create a table with declared ordering on (a, b)
    let table = MemTable::try_new(schema, vec![vec![batch]])?
        .with_sort_order(vec![sort_exprs]);
    
    ctx.register_table("presorted_table", Arc::new(table))?;

    // Sort on (a, b, c) - should trigger PartialSortExec optimization
    let start = std::time::Instant::now();
    let result = ctx
        .sql("SELECT * FROM presorted_table ORDER BY a, b, c")
        .await?
        .collect()
        .await?;

    let duration = start.elapsed();
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();

    Ok(duration.as_secs_f64() / total_rows as f64 * 1_000_000.0) // microseconds per row
}

async fn benchmark_full_sort_scenario(rows: usize) -> Result<f64> {
    let ctx = SessionContext::new();
    let batch = create_random_data(rows)?;
    let schema = batch.schema();

    // Create table without any ordering information
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("random_table", Arc::new(table))?;

    // Sort on (a, b, c) - should use full SortExec
    let start = std::time::Instant::now();
    let result = ctx
        .sql("SELECT * FROM random_table ORDER BY a, b, c")
        .await?
        .collect()
        .await?;

    let duration = start.elapsed();
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();

    Ok(duration.as_secs_f64() / total_rows as f64 * 1_000_000.0) // microseconds per row
}

async fn benchmark_incompatible_sort_scenario(rows: usize) -> Result<f64> {
    let ctx = SessionContext::new();
    let batch = create_presorted_data(rows, rows / 100)?;
    let schema = batch.schema();

    // Create sort expressions for (a, b) ordering
    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false),
        SortExpr::new(col("b"), true, false),
    ];

    let table = MemTable::try_new(schema, vec![vec![batch]])?
        .with_sort_order(vec![sort_exprs]);
    
    ctx.register_table("presorted_table", Arc::new(table))?;

    // Sort on (c, a, b) - incompatible with existing order, should use full SortExec
    let start = std::time::Instant::now();
    let result = ctx
        .sql("SELECT * FROM presorted_table ORDER BY c, a, b")
        .await?
        .collect()
        .await?;

    let duration = start.elapsed();
    let total_rows: usize = result.iter().map(|batch| batch.num_rows()).sum();

    Ok(duration.as_secs_f64() / total_rows as f64 * 1_000_000.0) // microseconds per row
}

async fn verify_plan_usage(rows: usize) -> Result<()> {
    let ctx = SessionContext::new();
    let batch = create_presorted_data(rows, 10)?;
    let schema = batch.schema();

    let sort_exprs = vec![
        SortExpr::new(col("a"), true, false),
        SortExpr::new(col("b"), true, false),
    ];

    let table = MemTable::try_new(schema, vec![vec![batch]])?
        .with_sort_order(vec![sort_exprs]);
    
    ctx.register_table("test_table", Arc::new(table))?;

    // Query that should use PartialSortExec
    let df = ctx.sql("SELECT * FROM test_table ORDER BY a, b, c").await?;
    let plan = df.explain(false, false)?.collect().await?;
    
    println!("=== Plan for ORDER BY a, b, c (should use PartialSortExec) ===");
    for batch in plan {
        for row in 0..batch.num_rows() {
            if let Some(plan_str) = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>() {
                if let Some(plan_line) = plan_str.value(row).lines().next() {
                    if plan_line.contains("PartialSortExec") || plan_line.contains("SortExec") {
                        println!("{}", plan_line);
                    }
                }
            }
        }
    }

    // Query that should use full SortExec
    let df2 = ctx.sql("SELECT * FROM test_table ORDER BY c, a, b").await?;
    let plan2 = df2.explain(false, false)?.collect().await?;
    
    println!("=== Plan for ORDER BY c, a, b (should use SortExec) ===");
    for batch in plan2 {
        for row in 0..batch.num_rows() {
            if let Some(plan_str) = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>() {
                if let Some(plan_line) = plan_str.value(row).lines().next() {
                    if plan_line.contains("PartialSortExec") || plan_line.contains("SortExec") {
                        println!("{}", plan_line);
                    }
                }
            }
        }
    }

    Ok(())
}

fn bench_sort_optimizations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Verify that our benchmark is actually testing the right thing
    println!("Verifying plan selection...");
    rt.block_on(verify_plan_usage(1000)).unwrap();

    let mut group = c.benchmark_group("sort_comparison");
    
    for &size in &[1000, 5000, 10000, 50000] {
        group.bench_function(&format!("partial_sort_{}", size), |b| {
            b.iter(|| {
                rt.block_on(benchmark_partial_sort_scenario(black_box(size)))
                    .unwrap()
            })
        });

        group.bench_function(&format!("full_sort_random_{}", size), |b| {
            b.iter(|| {
                rt.block_on(benchmark_full_sort_scenario(black_box(size)))
                    .unwrap()
            })
        });

        group.bench_function(&format!("full_sort_incompatible_{}", size), |b| {
            b.iter(|| {
                rt.block_on(benchmark_incompatible_sort_scenario(black_box(size)))
                    .unwrap()
            })
        });
    }
    
    group.finish();
}

criterion_group!(benches, bench_sort_optimizations);
criterion_main!(benches);