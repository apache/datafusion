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

//! Test for CSV schema inference with different column counts (GitHub issue #17516)

use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::test_util::batches_to_sort_string;
use insta::assert_snapshot;
use std::fs;
use tempfile::TempDir;

#[tokio::test]
async fn test_csv_schema_inference_different_column_counts() -> Result<()> {
    // Create temporary directory for test files
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let temp_path = temp_dir.path();

    // Create CSV file 1 with 3 columns (simulating older railway services format)
    let csv1_content = r#"service_id,route_type,agency_id
1,bus,agency1
2,rail,agency2
3,bus,agency3
"#;
    fs::write(temp_path.join("services_2024.csv"), csv1_content)?;

    // Create CSV file 2 with 6 columns (simulating newer railway services format)
    let csv2_content = r#"service_id,route_type,agency_id,stop_platform_change,stop_planned_platform,stop_actual_platform
4,rail,agency2,true,Platform A,Platform B
5,bus,agency1,false,Stop 1,Stop 1
6,rail,agency3,true,Platform C,Platform D
"#;
    fs::write(temp_path.join("services_2025.csv"), csv2_content)?;

    // Create DataFusion context
    let ctx = SessionContext::new();

    // This should now work (previously would have failed with column count mismatch)
    // Enable truncated_rows to handle files with different column counts
    let df = ctx
        .read_csv(
            temp_path.to_str().unwrap(),
            CsvReadOptions::new().truncated_rows(true),
        )
        .await
        .expect("Should successfully read CSV directory with different column counts");

    // Verify the schema contains all 6 columns (union of both files)
    let df_clone = df.clone();
    let schema = df_clone.schema();
    assert_eq!(
        schema.fields().len(),
        6,
        "Schema should contain all 6 columns"
    );

    // Check that we have all expected columns
    let field_names: Vec<&str> =
        schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(field_names.contains(&"service_id"));
    assert!(field_names.contains(&"route_type"));
    assert!(field_names.contains(&"agency_id"));
    assert!(field_names.contains(&"stop_platform_change"));
    assert!(field_names.contains(&"stop_planned_platform"));
    assert!(field_names.contains(&"stop_actual_platform"));

    // All fields should be nullable since they don't appear in all files
    for field in schema.fields() {
        assert!(
            field.is_nullable(),
            "Field {} should be nullable",
            field.name()
        );
    }

    // Verify we can actually read the data
    let results = df.collect().await?;

    // Calculate total rows across all batches
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 6, "Should have 6 total rows across all batches");

    // All batches should have 6 columns (the union schema)
    for batch in &results {
        assert_eq!(batch.num_columns(), 6, "All batches should have 6 columns");
        assert_eq!(
            batch.schema().fields().len(),
            6,
            "Each batch should use the union schema with 6 fields"
        );
    }

    // Verify the actual content of the data using snapshot testing
    assert_snapshot!(batches_to_sort_string(&results), @r"
    +------------+------------+-----------+----------------------+-----------------------+----------------------+
    | service_id | route_type | agency_id | stop_platform_change | stop_planned_platform | stop_actual_platform |
    +------------+------------+-----------+----------------------+-----------------------+----------------------+
    | 1          | bus        | agency1   |                      |                       |                      |
    | 2          | rail       | agency2   |                      |                       |                      |
    | 3          | bus        | agency3   |                      |                       |                      |
    | 4          | rail       | agency2   | true                 | Platform A            | Platform B           |
    | 5          | bus        | agency1   | false                | Stop 1                | Stop 1               |
    | 6          | rail       | agency3   | true                 | Platform C            | Platform D           |
    +------------+------------+-----------+----------------------+-----------------------+----------------------+
    ");

    Ok(())
}
