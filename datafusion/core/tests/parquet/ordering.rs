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

//! Tests for ordering in Parquet sorting_columns metadata

use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use tempfile::tempdir;

/// Test that CREATE TABLE ... WITH ORDER writes sorting_columns to Parquet metadata
#[tokio::test]
async fn test_create_table_with_order_writes_sorting_columns() -> Result<()> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    let ctx = SessionContext::new();
    let tmp_dir = tempdir()?;
    let table_path = tmp_dir.path().join("sorted_table");
    std::fs::create_dir_all(&table_path)?;

    // Create external table with ordering
    let create_table_sql = format!(
        "CREATE EXTERNAL TABLE sorted_data (a INT, b VARCHAR) \
         STORED AS PARQUET \
         LOCATION '{}' \
         WITH ORDER (a ASC NULLS FIRST, b DESC NULLS LAST)",
        table_path.display()
    );
    ctx.sql(&create_table_sql).await?;

    // Insert sorted data
    ctx.sql("INSERT INTO sorted_data VALUES (1, 'x'), (2, 'y'), (3, 'z')")
        .await?
        .collect()
        .await?;

    // Find the parquet file that was written
    let parquet_files: Vec<_> = std::fs::read_dir(&table_path)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();

    assert!(
        !parquet_files.is_empty(),
        "Expected at least one parquet file in {}",
        table_path.display()
    );

    // Read the parquet file and verify sorting_columns metadata
    let file = File::open(parquet_files[0].path())?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // Check that row group has sorting_columns
    let row_group = metadata.row_group(0);
    let sorting_columns = row_group.sorting_columns();

    assert!(
        sorting_columns.is_some(),
        "Expected sorting_columns in row group metadata"
    );
    let sorting = sorting_columns.unwrap();
    assert_eq!(sorting.len(), 2, "Expected 2 sorting columns");

    // First column: a ASC NULLS FIRST (column_idx = 0)
    assert_eq!(sorting[0].column_idx, 0, "First sort column should be 'a'");
    assert!(
        !sorting[0].descending,
        "First column should be ASC (descending=false)"
    );
    assert!(
        sorting[0].nulls_first,
        "First column should have NULLS FIRST"
    );

    // Second column: b DESC NULLS LAST (column_idx = 1)
    assert_eq!(sorting[1].column_idx, 1, "Second sort column should be 'b'");
    assert!(
        sorting[1].descending,
        "Second column should be DESC (descending=true)"
    );
    assert!(
        !sorting[1].nulls_first,
        "Second column should have NULLS LAST"
    );

    Ok(())
}
