// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This example demonstrates how to use DataFusion's memory profiling capabilities
//! to analyze memory usage during query execution. It runs a multi-stage query
//! that includes scanning, filtering, aggregation, and sorting operations.

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new DataFusion session context
    let ctx = SessionContext::new();

    // Enable memory profiling to track memory usage
    ctx.enable_memory_profiling();

    println!("=== DataFusion Memory Profiling Example ===");
    println!("Memory profiling enabled for multi-stage query execution\n");

    // Create sample data as a memory table
    let data = vec![
        ("Alice", "Engineering", 100000.0),
        ("Bob", "Engineering", 80000.0),
        ("Charlie", "Sales", 60000.0),
        ("Diana", "Engineering", 120000.0),
        ("Eve", "Sales", 70000.0),
        ("Frank", "Marketing", 50000.0),
        ("Grace", "Engineering", 90000.0),
        ("Henry", "Marketing", 55000.0),
        ("Ivy", "Sales", 65000.0),
        ("Jack", "Engineering", 110000.0),
    ];

    // Create arrays for each column
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..data.len() as i32));
    let names: ArrayRef = Arc::new(StringArray::from(
        data.iter()
            .map(|(name, _, _)| name.to_string())
            .collect::<Vec<_>>(),
    ));
    let departments: ArrayRef = Arc::new(StringArray::from(
        data.iter()
            .map(|(_, dept, _)| dept.to_string())
            .collect::<Vec<_>>(),
    ));
    let salaries: ArrayRef = Arc::new(Float64Array::from(
        data.iter()
            .map(|(_, _, salary)| *salary)
            .collect::<Vec<_>>(),
    ));

    // Create RecordBatch from arrays
    let batch = RecordBatch::try_from_iter(vec![
        ("id", ids),
        ("name", names),
        ("department", departments),
        ("salary", salaries),
    ])?;

    // Register the data as a table
    ctx.register_batch("employees", batch)?;

    println!(
        "1. Created sample employee data with {} records",
        data.len()
    );

    // Multi-stage query: scan → filter → aggregate → sort
    let df = ctx
        .sql(
            "SELECT 
                department,
                COUNT(*) as employee_count,
                AVG(salary) as avg_salary,
                SUM(salary) as total_salary,
                MIN(salary) as min_salary,
                MAX(salary) as max_salary
            FROM employees
            WHERE salary > 50000
            GROUP BY department
            ORDER BY avg_salary DESC",
        )
        .await?;

    println!("2. Executing multi-stage query...");
    println!("   - Scan: Reading employee data");
    println!("   - Filter: Excluding employees with salary <= 50000");
    println!("   - Aggregate: Grouping by department and calculating statistics");
    println!("   - Sort: Ordering results by average salary (descending)\n");

    // Collect results and display
    let results = df.collect().await?;

    println!("3. Query Results:");
    println!("{:-<80}", "");
    println!(
        "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12}",
        "Department", "Count", "Avg Salary", "Total", "Min", "Max"
    );
    println!("{:-<80}", "");

    for batch in results {
        let columns = batch.columns();
        let departments = columns[0].as_any().downcast_ref::<StringArray>().unwrap();
        let counts = columns[1].as_any().downcast_ref::<Int64Array>().unwrap();
        let avg_salaries = columns[2].as_any().downcast_ref::<Float64Array>().unwrap();
        let total_salaries = columns[3].as_any().downcast_ref::<Float64Array>().unwrap();
        let min_salaries = columns[4].as_any().downcast_ref::<Float64Array>().unwrap();
        let max_salaries = columns[5].as_any().downcast_ref::<Float64Array>().unwrap();

        for i in 0..batch.num_rows() {
            println!(
                "{:<15} {:<12} ${:<11.0} ${:<11.0} ${:<11.0} ${:<11.0}",
                departments.value(i),
                counts.value(i),
                avg_salaries.value(i),
                total_salaries.value(i),
                min_salaries.value(i),
                max_salaries.value(i)
            );
        }
    }

    println!("\n=== Memory Profiling Summary ===");
    println!("Memory profiling completed successfully!");
    println!("Check the DataFusion logs or use the memory profiling APIs");
    println!("to analyze memory usage patterns during query execution.");

    Ok(())
}
