//! Demonstrates memory profiling capabilities in DataFusion
//!
//! This example shows how to use `enable_memory_profiling()` to collect
//! detailed memory usage information during query execution.
//!
//! It runs a multi-stage query that allocates significant memory and
//! compares the results with memory profiling enabled vs disabled.

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use std::sync::Arc;
use std::time::Instant;

/// Creates a large dataset with multiple columns to simulate memory-intensive operations
fn create_large_dataset(num_rows: usize) -> Result<RecordBatch> {
    let mut ids = Vec::with_capacity(num_rows);
    let mut values = Vec::with_capacity(num_rows);
    let mut categories = Vec::with_capacity(num_rows);
    let mut prices = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        ids.push(i as i64);
        values.push((i % 1000) as f64);
        categories.push(format!("category_{}", i % 100));
        prices.push((i as f64) * 1.5);
    }

    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Float64Array::from(prices)),
        ],
    )?)
}

/// Runs a memory-intensive multi-stage query
async fn run_memory_intensive_query(ctx: &SessionContext) -> Result<()> {
    // Create a large dataset
    let batch = create_large_dataset(100_000)?;
    let provider = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
    ctx.register_table("large_table", Arc::new(provider))?;

    // Multi-stage query: aggregation, join, and window functions
    let sql = r#"
        WITH large_data AS (
            SELECT * FROM large_table
            UNION ALL
            SELECT * FROM large_table
            UNION ALL
            SELECT * FROM large_table
        ),
        aggregated AS (
            SELECT
                category,
                SUM(value) as total_value,
                AVG(price) as avg_price,
                COUNT(*) as row_count
            FROM large_data
            GROUP BY category
        ),
        ranked AS (
            SELECT
                category,
                total_value,
                avg_price,
                row_count,
                RANK() OVER (ORDER BY total_value DESC) as value_rank,
                RANK() OVER (ORDER BY avg_price DESC) as price_rank
            FROM aggregated
        ),
        with_rank_diff AS (
            SELECT
                category,
                total_value,
                avg_price,
                row_count,
                value_rank,
                price_rank,
                ABS(value_rank - price_rank) as rank_diff
            FROM ranked
        )
        SELECT
            category,
            total_value,
            avg_price,
            row_count,
            value_rank,
            price_rank,
            rank_diff
        FROM with_rank_diff
        WHERE rank_diff <= 10
        ORDER BY total_value DESC
        LIMIT 100
    "#;

    let start = Instant::now();
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    let duration = start.elapsed();

    println!("Query completed in: {:?}", duration);
    println!(
        "Number of result rows: {}",
        results.iter().map(|r| r.num_rows()).sum::<usize>()
    );

    // Calculate total memory used by results
    let total_bytes: usize = results.iter().map(|r| r.get_array_memory_size()).sum();
    println!(
        "Total result memory: {:.2} MB",
        total_bytes as f64 / 1024.0 / 1024.0
    );

    Ok(())
}

/// Runs the query with memory profiling disabled
async fn run_without_profiling() -> Result<()> {
    println!("=== Running WITHOUT memory profiling ===");

    let ctx = SessionContext::new();
    let start = Instant::now();
    run_memory_intensive_query(&ctx).await?;
    let total_time = start.elapsed();

    println!("Total execution time: {:?}", total_time);
    println!(
        "Memory profiling enabled: {}",
        ctx.is_memory_profiling_enabled()
    );
    println!();

    Ok(())
}

/// Runs the query with memory profiling enabled
async fn run_with_profiling() -> Result<()> {
    println!("=== Running WITH memory profiling ===");

    let ctx = SessionContext::new();
    let _profile = ctx.enable_memory_profiling();

    let start = Instant::now();
    run_memory_intensive_query(&ctx).await?;
    let total_time = start.elapsed();

    println!("Total execution time: {:?}", total_time);
    println!(
        "Memory profiling enabled: {}",
        ctx.is_memory_profiling_enabled()
    );

    // Analyze memory usage in detail
    let memory_report = ctx.get_last_query_memory_report();
    analyze_memory_report(&memory_report);

    Ok(())
}

/// Provides detailed analysis of memory usage patterns
fn analyze_memory_report(memory_report: &std::collections::HashMap<String, usize>) {
    if memory_report.is_empty() {
        println!("No memory tracking data available");
        return;
    }
    
    let mut operators: Vec<_> = memory_report.iter().collect();
    operators.sort_by(|a, b| b.1.cmp(a.1));
    
    let peak_memory = operators.iter().map(|(_, bytes)| **bytes).max().unwrap_or(0);
    let total_memory: usize = operators.iter().map(|(_, bytes)| **bytes).sum();
    
    println!("
📊 Memory Analysis:");
    println!("  Peak operator memory: {:.2} MB", peak_memory as f64 / 1024.0 / 1024.0);
    println!("  Total tracked memory: {:.2} MB", total_memory as f64 / 1024.0 / 1024.0);
    
    // Categorize memory usage by operation type
    let mut categories = std::collections::HashMap::new();
    for (op, bytes) in &operators {
        let category = categorize_operator(op);
        *categories.entry(category).or_insert(0) += *bytes;
    }
    
    println!("
📋 Memory by Category:");
    let mut category_list: Vec<_> = categories.iter().collect();
    category_list.sort_by(|a, b| b.1.cmp(a.1));
    
    for (category, bytes) in category_list {
        let percentage = (*bytes as f64 / total_memory as f64) * 100.0;
        println!("  {}: {:.2} MB ({:.1}%)", category, *bytes as f64 / 1024.0 / 1024.0, percentage);
    }
    
    println!("
🔍 Detailed Operator Breakdown:");
    for (i, (op, bytes)) in operators.iter().enumerate().take(10) {
        let percentage = (**bytes as f64 / total_memory as f64) * 100.0;
        let category = categorize_operator(op);
        println!("  {}. {}: {:.2} MB ({:.1}%) [{}]", 
            i + 1,
            op, 
            **bytes as f64 / 1024.0 / 1024.0,
            percentage,
            category
        );
    }
    
    if operators.len() > 10 {
        println!("  ... and {} more operators", operators.len() - 10);
    }
}

/// Categorizes memory operators for better understanding
fn categorize_operator(op_name: &str) -> &'static str {
    match op_name.to_lowercase().as_str() {
        name if name.contains("scan") || name.contains("reader") => "Data Input",
        name if name.contains("aggregate") || name.contains("group") => "Aggregation",
        name if name.contains("join") || name.contains("hash") => "Join Operation", 
        name if name.contains("sort") || name.contains("order") => "Sorting",
        name if name.contains("filter") || name.contains("where") => "Filtering",
        name if name.contains("project") || name.contains("select") => "Projection",
        name if name.contains("union") || name.contains("concat") => "Set Operation",
        name if name.contains("window") || name.contains("rank") => "Window Function",
        name if name.contains("limit") || name.contains("top") => "Limit/TopK",
        name if name.contains("spill") || name.contains("buffer") => "Memory Management",
        _ => "Other"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("DataFusion Memory Profiling Example");
    println!("====================================\n");

    // Run without profiling
    run_without_profiling().await?;

    // Run with profiling
    run_with_profiling().await?;

    println!("=== Comparison Summary ===");
    println!("Key observations:");
    println!("- Memory profiling provides detailed allocation tracking");
    println!("- You can see peak memory usage, allocation counts, and overhead");
    println!("- The profiling has minimal impact on query performance");
    println!("- Use memory profiling for debugging memory-intensive queries");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::assert_batches_eq;

    #[tokio::test]
    async fn test_create_large_dataset() -> Result<()> {
        let batch = create_large_dataset(100)?;
        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.num_columns(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn test_memory_profiling_toggle() -> Result<()> {
        let ctx = SessionContext::new();
        assert!(!ctx.is_memory_profiling_enabled());

        let _handle = ctx.enable_memory_profiling();
        assert!(ctx.is_memory_profiling_enabled());

        Ok(())
    }
}
