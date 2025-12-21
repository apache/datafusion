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

//! Tests for ordering inference from Parquet sorting_columns metadata

use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use datafusion::test_util::parquet::{SortColumnSpec, create_sorted_parquet_file};
use datafusion_common::Result;
use tempfile::tempdir;

/// Test that ordering is inferred from Parquet sorting_columns metadata
#[tokio::test]
async fn test_parquet_ordering_inference() -> Result<()> {
    // Create a schema with two integer columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));

    // Create sorted test data (sorted by 'a' ascending)
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;

    // Create a temp directory for the test file
    let tmp_dir = tempdir()?;
    let file_path = tmp_dir.path().join("sorted.parquet");

    // Create the Parquet file with sorting metadata
    let sorting = vec![
        SortColumnSpec::asc_nulls_first(0), // column 'a' ASC NULLS FIRST
    ];
    let _test_file = create_sorted_parquet_file(file_path.clone(), vec![batch], sorting)?;

    // Register the file as a table and query it
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "sorted_table",
        file_path.to_str().unwrap(),
        Default::default(),
    )
    .await?;

    // Verify the table is registered and can be queried
    let df = ctx.sql("SELECT * FROM sorted_table ORDER BY a").await?;
    let results = df.collect().await?;

    // Should have 1 batch with 5 rows
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 5);

    // Verify data is correct
    let a_col = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(a_col.value(0), 1);
    assert_eq!(a_col.value(4), 5);

    Ok(())
}

/// Test that multi-column sorting metadata is preserved
#[tokio::test]
async fn test_parquet_multi_column_ordering_inference() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
        Field::new("c", DataType::Int32, true),
    ]));

    // Create test data sorted by 'a' ASC, then 'b' DESC
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 1, 2, 2, 3])),
            Arc::new(Int32Array::from(vec![20, 10, 40, 30, 50])),
            Arc::new(Int32Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )?;

    let tmp_dir = tempdir()?;
    let file_path = tmp_dir.path().join("multi_sorted.parquet");

    // Create the file with multi-column sorting metadata
    let sorting = vec![
        SortColumnSpec::asc_nulls_first(0), // 'a' ASC NULLS FIRST
        SortColumnSpec::desc_nulls_last(1), // 'b' DESC NULLS LAST
    ];
    let _test_file = create_sorted_parquet_file(file_path.clone(), vec![batch], sorting)?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "multi_sorted",
        file_path.to_str().unwrap(),
        Default::default(),
    )
    .await?;

    let df = ctx.sql("SELECT a, b FROM multi_sorted").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 5);

    Ok(())
}

/// Test file without sorting metadata
#[tokio::test]
async fn test_parquet_no_ordering_metadata() -> Result<()> {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs::File;

    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![3, 1, 2]))],
    )?;

    let tmp_dir = tempdir()?;
    let file_path = tmp_dir.path().join("unsorted.parquet");

    // Create file WITHOUT sorting metadata
    let file = File::create(&file_path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    let ctx = SessionContext::new();
    ctx.register_parquet("unsorted", file_path.to_str().unwrap(), Default::default())
        .await?;

    let df = ctx.sql("SELECT * FROM unsorted").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    Ok(())
}

/// Test that CREATE EXTERNAL TABLE WITH ORDER writes sorting_columns to Parquet metadata
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
