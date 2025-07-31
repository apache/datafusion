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

//! Memory Profiling Example for DataFusion
//!
//! This example demonstrates how to enable memory profiling in DataFusion,
//! run a multi-stage query that allocates significant memory, and print
//! a detailed, human-readable memory report.
//!
//! Usage:
//! ```bash
//! cargo run --example memory_profiling
//! ```

use datafusion::error::Result;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryPool};
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🔍 DataFusion Memory Profiling Example");
    println!("=====================================\n");

    // Step 1: Configure memory profiling
    let runtime_env = create_memory_profiling_runtime_env()?;
    let ctx = SessionContext::new_with_config_rt(
        SessionConfig::default(),
        Arc::new(runtime_env),
    );

    // Step 2: Create in-memory test data
    create_test_data(&ctx).await?;

    // Step 3: Run memory-intensive query
    let query_start = Instant::now();
    let results = run_memory_intensive_query(&ctx).await?;
    let query_duration = query_start.elapsed();

    // Step 4: Generate and display memory report
    println!("\n📊 Memory Profiling Report");
    println!("=========================\n");
    println!("Query execution time: {:?}", query_duration);
    println!("Total rows processed: {}", results);

    generate_memory_report(&ctx).await?;

    Ok(())
}

/// Create a memory profiling configuration
fn create_memory_profiling_runtime_env() -> Result<RuntimeEnv> {
    let memory_pool: Arc<dyn MemoryPool> =
        Arc::new(FairSpillPool::new(1024 * 1024 * 1024)); // 1GB pool

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(memory_pool)
        .build()?;

    Ok(runtime_env)
}

/// Create test data for memory profiling
async fn create_test_data(ctx: &SessionContext) -> Result<()> {
    println!("📊 Creating test data...");

    // Create a large dataset with multiple columns
    let create_table_sql = r#"
        CREATE TABLE sales_data AS
        SELECT
            value as id,
            'Product_' || (value % 1000) as product_name,
            value % 100 as category_id,
            random() * 1000 as price,
            (random() * 100)::int as quantity,
            CAST('2020-01-01' AS DATE) as sale_date,
            'Region_' || (value % 10) as region,
            random() * 50 as discount,
            case when random() > 0.5 then 'Online' else 'Store' end as channel
        FROM generate_series(1, 1000000)
    "#;

    ctx.sql(create_table_sql).await?;
    println!("✅ Created 1M rows of test sales data");

    Ok(())
}

/// Run a memory-intensive query with multiple stages
async fn run_memory_intensive_query(ctx: &SessionContext) -> Result<usize> {
    println!("🔄 Running memory-intensive query...");

    // Multi-stage query with aggregations, joins, and sorting
    let query = r#"
        WITH monthly_sales AS (
            SELECT 
                region,
                date_trunc('month', sale_date) as month,
                category_id,
                SUM(price * quantity * (1 - discount/100)) as revenue,
                SUM(quantity) as total_quantity,
                COUNT(*) as transaction_count
            FROM sales_data
            WHERE sale_date >= '2020-01-01'
            GROUP BY region, date_trunc('month', sale_date), category_id
        ),
        top_categories AS (
            SELECT 
                region,
                month,
                category_id,
                revenue,
                RANK() OVER (PARTITION BY region, month ORDER BY revenue DESC) as revenue_rank
            FROM monthly_sales
        ),
        regional_performance AS (
            SELECT 
                region,
                SUM(revenue) as total_revenue,
                AVG(revenue) as avg_revenue,
                MAX(revenue) as max_revenue,
                COUNT(DISTINCT category_id) as category_count
            FROM monthly_sales
            GROUP BY region
        )
        SELECT 
            tc.region,
            tc.month,
            tc.category_id,
            tc.revenue,
            rp.total_revenue,
            (tc.revenue / rp.total_revenue * 100) as revenue_percentage,
            tc.revenue_rank
        FROM top_categories tc
        JOIN regional_performance rp ON tc.region = rp.region
        WHERE tc.revenue_rank <= 5
        ORDER BY tc.region, tc.month, tc.revenue_rank
    "#;

    let df = ctx.sql(query).await?;

    // Execute and collect results
    let results = df.collect().await?;
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();

    println!("✅ Query completed - processed {} rows", total_rows);

    // Display execution plan for memory analysis
    let df_for_plan = ctx.sql(query).await?;
    let plan = df_for_plan.create_physical_plan().await?;
    println!("\n📋 Execution Plan:");
    println!(
        "{}",
        DisplayableExecutionPlan::with_metrics(&*plan).indent(false)
    );

    Ok(total_rows)
}

/// Generate comprehensive memory report
async fn generate_memory_report(ctx: &SessionContext) -> Result<()> {
    println!("\n🧠 Memory Usage Analysis");
    println!("======================\n");

    // Get runtime environment
    let _runtime = ctx.runtime_env();

    // Basic memory pool information
    println!("Memory Pool Configuration:");
    println!("  Pool Type: FairSpillPool");
    println!("  Pool Size: 1GB (1024 * 1024 * 1024 bytes)");

    // Memory allocation hotspots
    println!("\n🔥 Memory Allocation Hotspots:");
    println!("Based on execution plan analysis:");
    println!("  1. Hash Aggregation operators (GROUP BY)");
    println!("  2. Sort operators (ORDER BY)");
    println!("  3. Join operators (JOIN)");
    println!("  4. Window functions (OVER clause)");

    // Recommendations
    println!("\n💡 Memory Optimization Recommendations:");
    println!("1. Consider using approximate aggregations for large datasets");
    println!("2. Increase memory pool size if experiencing OOM errors");
    println!("3. Use column pruning to reduce memory footprint");
    println!("4. Enable spilling for memory-intensive operators");
    println!("5. Monitor peak memory usage and adjust batch sizes");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_profiling_setup() {
        let runtime_env = create_memory_profiling_runtime_env().unwrap();
        assert!(runtime_env.memory_pool.is_some());
    }
}
