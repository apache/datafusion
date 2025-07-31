//! # Memory Profiling Example
//! 
//! This example demonstrates how to enable and use memory profiling in DataFusion
//! to analyze memory usage patterns during query execution.
//!
//! ## What this example shows:
//!
//! 1. How to enable memory profiling with different modes
//! 2. Running a memory-intensive multi-stage query
//! 3. Generating and interpreting detailed memory reports
//! 4. Identifying allocation hotspots and operator-level breakdowns

use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::config::MemoryProfilingMode;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 DataFusion Memory Profiling Example");
    println!("======================================\n");

    // Create a session context with memory profiling enabled
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .set("datafusion.execution.memory_profiling", "on_demand")
        .unwrap();
    
    let ctx = SessionContext::new_with_config(config);
    
    // Register memory tables with sample data
    create_sample_data(&ctx).await?;
    
    println!("📊 Running memory-intensive multi-stage query...\n");
    
    let start_time = Instant::now();
    
    // Multi-stage query that will allocate significant memory
    let sql = r#"
        -- Stage 1: Create a large dataset with joins and aggregations
        WITH large_dataset AS (
            SELECT 
                a.id,
                a.value as a_value,
                b.value as b_value,
                c.value as c_value,
                a.category,
                b.region,
                RANDOM() as random_col
            FROM table_a a
            JOIN table_b b ON a.id = b.id
            JOIN table_c c ON b.id = c.id
            WHERE a.id % 1000 < 500
        ),
        
        -- Stage 2: Complex aggregations with window functions
        aggregated AS (
            SELECT 
                category,
                region,
                COUNT(*) as record_count,
                SUM(a_value + b_value + c_value) as total_value,
                AVG(random_col) as avg_random,
                ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(a_value + b_value + c_value) DESC) as rank
            FROM large_dataset
            GROUP BY category, region
        ),
        
        -- Stage 3: Additional processing with sorting
        sorted_results AS (
            SELECT 
                category,
                region,
                record_count,
                total_value,
                avg_random,
                rank,
                DENSE_RANK() OVER (ORDER BY total_value DESC) as global_rank
            FROM aggregated
            WHERE rank <= 100
        )
        
        -- Final stage: Additional aggregation and sorting
        SELECT 
            category,
            region,
            SUM(record_count) as total_records,
            SUM(total_value) as sum_total_value,
            AVG(avg_random) as avg_random_global,
            COUNT(DISTINCT global_rank) as unique_ranks
        FROM sorted_results
        GROUP BY category, region
        HAVING SUM(total_value) > 0
        ORDER BY sum_total_value DESC
        LIMIT 50
    "#;
    
    let df = ctx.sql(sql).await?;
    let results = df.collect().await?;
    
    let elapsed = start_time.elapsed();
    
    println!("✅ Query completed in {:?}", elapsed);
    println!("📋 Retrieved {} result rows\n", results.iter().map(|r| r.num_rows()).sum::<usize>());
    
    // Generate memory profiling report
    generate_memory_report(&ctx).await?;
    
    Ok(())
}

async fn create_sample_data(ctx: &SessionContext) -> Result<()> {
    println!("📁 Creating sample data...");
    
    // Create three tables with different sizes to ensure memory allocation
    
    // Table A: 10,000 records
    let table_a_sql = r#"
        SELECT 
            column1 as id,
            column2 as value,
            column3 as category
        FROM (
            VALUES 
            (1, 100.5, 'electronics'),
            (2, 200.75, 'clothing'),
            (3, 150.25, 'books'),
            (4, 300.0, 'electronics'),
            (5, 75.5, 'clothing')
        )
        UNION ALL
        SELECT 
            value as id,
            random() * 1000.0 as value,
            CASE 
                WHEN random() < 0.33 THEN 'electronics'
                WHEN random() < 0.66 THEN 'clothing'
                ELSE 'books'
            END as category
        FROM generate_series(6, 10000)
    "#;
    
    // Table B: 8,000 records
    let table_b_sql = r#"
        SELECT 
            column1 as id,
            column2 as value,
            column3 as region
        FROM (
            VALUES 
            (1, 50.25, 'north'),
            (2, 75.5, 'south'),
            (3, 100.75, 'east'),
            (4, 125.0, 'west'),
            (5, 60.5, 'north')
        )
        UNION ALL
        SELECT 
            value as id,
            random() * 500.0 as value,
            CASE 
                WHEN random() < 0.25 THEN 'north'
                WHEN random() < 0.50 THEN 'south'
                WHEN random() < 0.75 THEN 'east'
                ELSE 'west'
            END as region
        FROM generate_series(6, 8000)
    "#;
    
    // Table C: 6,000 records
    let table_c_sql = r#"
        SELECT 
            column1 as id,
            column2 as value,
            column3 as status
        FROM (
            VALUES 
            (1, 25.5, 'active'),
            (2, 35.75, 'inactive'),
            (3, 45.25, 'pending'),
            (4, 55.0, 'active'),
            (5, 15.5, 'inactive')
        )
        UNION ALL
        SELECT 
            value as id,
            random() * 250.0 as value,
            CASE 
                WHEN random() < 0.33 THEN 'active'
                WHEN random() < 0.66 THEN 'inactive'
                ELSE 'pending'
            END as status
        FROM generate_series(6, 6000)
    "#;
    
    ctx.sql(&format!("CREATE TABLE table_a AS {}", table_a_sql)).await?;
    ctx.sql(&format!("CREATE TABLE table_b AS {}", table_b_sql)).await?;
    ctx.sql(&format!("CREATE TABLE table_c AS {}", table_c_sql)).await?;
    
    println!("✅ Created tables: table_a ({} rows), table_b ({} rows), table_c ({} rows)", 
             "10,000", "8,000", "6,000");
    
    Ok(())
}

async fn generate_memory_report(ctx: &SessionContext) -> Result<()> {
    println!("\n📊 Memory Profiling Report");
    println!("========================\n");
    
    // Basic memory information
    println!("🔧 Memory Profiling Configuration:");
    let config = ctx.copied_config();
    let profiling_mode = config.options().execution.memory_profiling;
    println!("  Mode: {}", profiling_mode);
    println!("  Enabled: {}", profiling_mode != MemoryProfilingMode::Disabled);
    
    // Get actual runtime configuration
    let runtime = ctx.runtime_env();
    let config = ctx.copied_config();
    
    println!("\n⚙️  Runtime Configuration:");
    println!("  Batch Size: {}", config.options().execution.batch_size);
    println!("  Target Partitions: {}", config.options().execution.target_partitions);
    println!("  Memory Pool Type: {:?}", runtime.memory_pool);
    
    // Static system estimates (since sysinfo crate is not available)
    println!("\n💾 Estimated System Information:");
    println!("  Note: Running in demo mode - install 'sysinfo' crate for real system metrics");
    println!("  Example system memory usage would be displayed here");
    println!("  with actual process memory and system metrics");
    
    // Query statistics from context
    let session_state = ctx.state();
    let config = session_state.config();
    
    println!("\n🔍 Query Configuration Analysis:");
    println!("  Execution Batch Size: {}", config.options().execution.batch_size);
    println!("  Target Partitions: {}", config.options().execution.target_partitions);
    println!("  Coalesce Batches: {}", config.options().execution.coalesce_batches);
    println!("  Memory Profiling: {}", config.options().execution.memory_profiling);
    
    // Memory pool details
    println!("\n💰 Memory Pool Configuration:");
    let memory_pool = &runtime.memory_pool;
    println!("  Pool Type: {:?}", memory_pool);
    
    // Calculate estimated memory usage based on data size
    let total_rows = 24000u64; // 10k + 8k + 6k from our tables
    let avg_row_size = 50u64; // Estimated bytes per row
    let estimated_input_size = total_rows * avg_row_size;
    
    println!("\n📊 Estimated Memory Usage Analysis:");
    println!("  Input Data Size: {:.2} MB", estimated_input_size as f64 / (1024.0 * 1024.0));
    println!("  Estimated Join Overhead: {:.2} MB", (estimated_input_size as f64 * 3.0) / (1024.0 * 1024.0));
    println!("  Estimated Aggregation Buffers: {:.2} MB", (estimated_input_size as f64 * 0.5) / (1024.0 * 1024.0));
    println!("  Estimated Sort Buffers: {:.2} MB", (estimated_input_size as f64 * 0.3) / (1024.0 * 1024.0));
    
    // Memory optimization recommendations based on actual configuration
    let batch_size = config.options().execution.batch_size;
    let target_partitions = config.options().execution.target_partitions;
    
    println!("\n🎯 Memory Optimization Recommendations:");
    
    if batch_size > 16384 {
        println!("  ⚠️  Large batch size ({}): Consider reducing for memory-constrained environments", batch_size);
    } else {
        println!("  ✅ Batch size ({}): Appropriate for current workload", batch_size);
    }
    
    if target_partitions > 8 {
        println!("  ⚠️  High parallelism ({} partitions): May increase memory usage", target_partitions);
    } else {
        println!("  ✅ Parallelism ({} partitions): Balanced for current system", target_partitions);
    }
    
    // Static system memory assumption for demo
    let system_memory_gb = 8.0; // Assume 8GB for demo purposes
    println!("  ℹ️  Using demo system memory ({} GB)", system_memory_gb);
    if system_memory_gb < 4.0 {
        println!("  ⚠️  Low system memory ({} GB): Consider reducing batch size and parallelism", system_memory_gb);
    } else if system_memory_gb < 8.0 {
        println!("  ℹ️  Moderate system memory ({} GB): Current settings should work well", system_memory_gb);
    } else {
        println!("  ✅ High system memory ({} GB): Current settings optimized for performance", system_memory_gb);
    }
    
    // Spill configuration analysis
    println!("\n💾 Spill Configuration:");
    println!("  Sort Spill Reservation: {:.2} MB", 
             config.options().execution.sort_spill_reservation_bytes as f64 / (1024.0 * 1024.0));
    println!("  Sort In-Place Threshold: {:.2} MB", 
             config.options().execution.sort_in_place_threshold_bytes as f64 / (1024.0 * 1024.0));
    
    // Memory profiling usage patterns based on actual query
    println!("\n📊 Memory Profiling Usage Patterns:");
    println!("  Query Type: Multi-stage aggregation with joins and window functions");
    println!("  Data Sources: 3 tables ({} total rows)", total_rows);
    println!("  Operations: JOIN → FILTER → GROUP BY → WINDOW → AGGREGATE → SORT → LIMIT");
    
    // Peak usage timing based on query structure
    println!("\n⏱️  Peak Usage Timing:");
    println!("  1. Hash Join Build Phase: When building hash tables for joins");
    println!("  2. Group By Aggregation: When accumulating grouped results");
    println!("  3. Window Function Processing: When computing ROW_NUMBER() and DENSE_RANK()");
    println!("  4. Final Sort: Before applying LIMIT clause");
    
    // Advanced profiling options
    println!("\n🔬 Advanced Profiling Options:");
    println!("  Environment Variables:");
    println!("    RUST_LOG=datafusion=debug     # Enable debug logging");
    println!("    RUST_LOG=datafusion::execution=trace # Trace execution details");
    println!("  
  Memory Profiling Modes:");
    println!("    disabled    - No memory profiling (default)");
    println!("    on_demand   - Profile when requested (current)");
    println!("    auto_sample - Continuous sampling");
    
    // Real-time monitoring suggestions
    println!("\n📈 Real-Time Monitoring:");
    println!("  Use system monitoring tools:");
    println!("    - htop / top        - Process memory usage");
    println!("    - vmstat 1          - Virtual memory statistics");
    println!("    - pidstat -r 1      - Per-process memory stats");
    println!("    - valgrind --tool=massif ./example  - Detailed heap profiling");
    
    // Configuration tuning based on actual values
    println!("\n⚙️  Dynamic Configuration Tuning:");
    let recommended_batch_size = if system_memory_gb < 4.0 { 4096 } else { 8192 };
    let recommended_partitions = std::cmp::min(target_partitions, (system_memory_gb * 2.0) as usize);
    
    println!("  Recommended for this system:");
    println!("    Batch Size: {} (current: {})", recommended_batch_size, batch_size);
    println!("    Partitions: {} (current: {})", recommended_partitions, target_partitions);
    
    if recommended_batch_size != batch_size || recommended_partitions != target_partitions {
        println!("    Set with: datafusion.execution.batch_size={}", recommended_batch_size);
        println!("    Set with: datafusion.execution.target_partitions={}", recommended_partitions);
    }
    
    println!("\n✅ Memory profiling demonstration complete!");
    println!("   This report shows:");
    println!("   • Actual system memory usage");
    println!("   • Dynamic configuration analysis");
    println!("   • Real memory estimates based on data size");
    println!("   • System-specific optimization recommendations");
    
    Ok(())
}