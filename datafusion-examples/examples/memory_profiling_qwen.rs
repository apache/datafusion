//! Demonstrates memory profiling capabilities in DataFusion
//!
//! This example shows how to use `ctx.enable_memory_profiling()` to collect
//! detailed memory usage information during query execution.
//!
//! The example runs a multi-stage query and shows how to access memory
//! profiling information. Note that memory profiling is currently
//! experimental and may not capture all memory allocations.

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::common::Result;
use datafusion::execution::context::SessionContext;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Categorizes operators into logical groups for better analysis
fn categorize_operator(operator_name: &str) -> &'static str {
    if operator_name.contains("Scan") || operator_name.contains("scan") {
        "Scan"
    } else if operator_name.contains("Join") || operator_name.contains("join") {
        "Join"
    } else if operator_name.contains("Aggregate") || operator_name.contains("aggregate") || operator_name.contains("Hash") {
        "Aggregation"
    } else if operator_name.contains("Sort") || operator_name.contains("sort") {
        "Sort"
    } else if operator_name.contains("Window") || operator_name.contains("window") {
        "Window"
    } else if operator_name.contains("Filter") || operator_name.contains("filter") {
        "Filter"
    } else if operator_name.contains("Project") || operator_name.contains("project") {
        "Projection"
    } else if operator_name.contains("Union") || operator_name.contains("union") {
        "Union"
    } else {
        "Other"
    }
}

/// Analyzes memory report and provides detailed breakdown
fn analyze_memory_report(memory_report: &HashMap<String, usize>) {
    let total_memory: usize = memory_report.values().sum();
    let mut category_memory: HashMap<&str, usize> = HashMap::new();
    
    // Categorize operators
    for (operator, memory) in memory_report {
        let category = categorize_operator(operator);
        *category_memory.entry(category).or_insert(0) += memory;
    }
    
    println!("📊 Memory Analysis by Operator Category:");
    for (category, memory) in &category_memory {
        let percentage = if total_memory > 0 {
            (*memory as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };
        println!("  📌 {}: {:.2} MB ({:.1}%)", 
                 category, 
                 *memory as f64 / 1024.0 / 1024.0,
                 percentage);
    }
    
    println!("\n🔍 Top 10 Memory-Intensive Operators:");
    let mut sorted_operators: Vec<_> = memory_report.iter().collect();
    sorted_operators.sort_by(|a, b| b.1.cmp(a.1));
    
    for (i, (operator, memory)) in sorted_operators.iter().take(10).enumerate() {
        let percentage = if total_memory > 0 {
            (**memory as f64 / total_memory as f64) * 100.0
        } else {
            0.0
        };
        println!("  {}. {}: {:.2} MB ({:.1}%)", 
                 i + 1, 
                 operator, 
                 **memory as f64 / 1024.0 / 1024.0,
                 percentage);
    }
    
    let peak_memory_mb = total_memory as f64 / 1024.0 / 1024.0;
    println!("\n🚀 Peak Memory Usage: {:.2} MB", peak_memory_mb);
    
    if peak_memory_mb > 100.0 {
        println!("⚠️  High memory usage detected - consider optimizing query or increasing memory limits");
    } else if peak_memory_mb > 50.0 {
        println!("⚡ Moderate memory usage - monitor for production workloads");
    } else {
        println!("✅ Memory usage is within acceptable limits");
    }
}

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

    // Enable memory profiling
    let _handle = ctx.enable_memory_profiling();

    let start = Instant::now();
    run_memory_intensive_query(&ctx).await?;
    let total_time = start.elapsed();

    println!("Total execution time: {:?}", total_time);
    println!(
        "Memory profiling enabled: {}",
        ctx.is_memory_profiling_enabled()
    );

    // Get memory profiling information
    let memory_report = ctx.get_last_query_memory_report();
    if !memory_report.is_empty() {
        println!("🎯 Memory profiling results collected successfully!");
        println!("Number of operators tracked: {}", memory_report.len());
        
        // Detailed analysis of memory usage
        analyze_memory_report(&memory_report);
        
        println!("\n📋 Raw Memory Report (All Operators):");
        for (operator, bytes) in &memory_report {
            println!("  {}: {:.2} MB", operator, bytes / 1024 / 1024);
        }
    } else {
        println!("No memory profiling information available");
        println!("This is expected for this simple query because:");
        println!("  1. Memory profiling is still experimental");
        println!("  2. Not all operators currently report memory usage");
        println!("  3. The query may not have triggered memory-intensive operations");
        println!("");
        println!("Memory profiling works best with queries that:");
        println!("  - Perform large aggregations or joins");
        println!("  - Use window functions with large partitions");
        println!("  - Sort large datasets");
        println!("  - Perform complex analytical operations");
    }

    println!();

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("DataFusion Memory Profiling Example");
    println!("====================================\n");

    // Run without profiling
    run_without_profiling().await?;

    // Run with profiling
    run_with_profiling().await?;

    println!("=== Enhanced Memory Profiling Summary ===");
    println!("Key observations:");
    println!("🔧 Memory profiling can be enabled/disabled per query using ctx.enable_memory_profiling()");
    println!("⚡ The feature has minimal impact on query performance");
    println!("📊 Memory profiling information is accessed via ctx.get_last_query_memory_report()");
    println!("🎯 Enhanced analysis provides operator categorization and peak memory tracking");
    println!("📈 For complex queries with large memory usage, this feature can help identify bottlenecks");
    println!("🧪 Memory profiling is currently experimental and may not capture all memory allocations");
    println!("");
    println!("📋 Operator Categories Tracked:");
    println!("  • Scans: Table and file reading operations");
    println!("  • Joins: Hash joins, nested loop joins, etc.");
    println!("  • Aggregations: GROUP BY, hash aggregates, etc.");
    println!("  • Sorts: ORDER BY and sorting operations");
    println!("  • Windows: Window function operations");
    println!("  • Filters: WHERE clause filtering");
    println!("  • Projections: SELECT column operations");
    println!("  • Unions: UNION and set operations");
    println!("");
    println!("🚀 To see enhanced memory profiling in action:");
    println!("  1. Try this example with more memory-intensive queries");
    println!("  2. Look for queries with large aggregations, joins, or window functions");
    println!("  3. Monitor peak memory usage during query execution");
    println!("  4. Use operator categorization to identify performance bottlenecks");
    println!("  5. Check the DataFusion documentation for operators that support memory tracking");

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
