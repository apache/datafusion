// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

/// Test that partition columns are removed and remaining column indices are
/// remapped when writing sorting_columns to Parquet metadata.
#[tokio::test]
async fn test_partitioned_table_remaps_sorting_columns() -> Result<()> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    let ctx = SessionContext::new();
    let tmp_dir = tempdir()?;
    let table_path = tmp_dir.path().join("sorted_partitioned_table");
    std::fs::create_dir_all(&table_path)?;

    let create_table_sql = format!(
        "CREATE EXTERNAL TABLE sorted_partitioned_data (a INT, b VARCHAR, part VARCHAR) \
         STORED AS PARQUET \
         LOCATION '{}' \
         PARTITIONED BY (part) \
         WITH ORDER (part ASC NULLS FIRST, a ASC NULLS FIRST, b DESC NULLS LAST)",
        table_path.display()
    );
    ctx.sql(&create_table_sql).await?;

    ctx.sql(
        "INSERT INTO sorted_partitioned_data VALUES \
         (2, 'c', 'x'), (1, 'a', 'x'), (1, 'b', 'x')",
    )
    .await?
    .collect()
    .await?;

    let parquet_file = find_parquet_file(&table_path)?
        .expect("expected a Parquet file in the partition directory");

    let file = File::open(parquet_file)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();
    let parquet_schema = metadata.file_metadata().schema_descr();
    assert_eq!(parquet_schema.num_columns(), 2);
    assert_eq!(parquet_schema.column(0).name(), "a");
    assert_eq!(parquet_schema.column(1).name(), "b");

    let sorting = metadata
        .row_group(0)
        .sorting_columns()
        .expect("expected sorting_columns in row group metadata");
    assert_eq!(sorting.len(), 2);

    assert_eq!(sorting[0].column_idx, 0);
    assert!(!sorting[0].descending);
    assert!(sorting[0].nulls_first);

    assert_eq!(sorting[1].column_idx, 1);
    assert!(sorting[1].descending);
    assert!(!sorting[1].nulls_first);

    Ok(())
}

/// Sort key after a struct column is written and read by leaf index.
#[tokio::test]
async fn test_sorting_columns_after_struct_round_trip() -> Result<()> {
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use std::fs::File;

    let ctx = SessionContext::new();
    let tmp_dir = tempdir()?;
    let table_path = tmp_dir.path().join("sorted_after_struct");
    std::fs::create_dir_all(&table_path)?;

    // Leaves: payload.a, payload.b, b, id
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE sorted_by_id (payload STRUCT<a INT, b INT>, b INT, id INT) \
         STORED AS PARQUET \
         LOCATION '{}' \
         WITH ORDER (id ASC)",
        table_path.display()
    ))
    .await?;
    // `b` decreases as `id` increases
    ctx.sql(
        "INSERT INTO sorted_by_id VALUES \
         (named_struct('a', 1, 'b', 30), 30, 1), \
         (named_struct('a', 2, 'b', 20), 20, 2), \
         (named_struct('a', 3, 'b', 10), 10, 3)",
    )
    .await?
    .collect()
    .await?;

    // No WITH ORDER, so the ordering comes from the file metadata
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE inferred (payload STRUCT<a INT, b INT>, b INT, id INT) \
         STORED AS PARQUET LOCATION '{}'",
        table_path.display()
    ))
    .await?;
    let batches = ctx
        .sql("SELECT b, id FROM inferred ORDER BY b")
        .await?
        .collect()
        .await?;
    datafusion::assert_batches_eq!(
        [
            "+----+----+",
            "| b  | id |",
            "+----+----+",
            "| 10 | 3  |",
            "| 20 | 2  |",
            "| 30 | 1  |",
            "+----+----+",
        ],
        &batches
    );

    let parquet_file =
        find_parquet_file(&table_path)?.expect("expected a Parquet file in the table");
    let reader = SerializedFileReader::new(File::open(parquet_file)?)?;
    let sorting = reader
        .metadata()
        .row_group(0)
        .sorting_columns()
        .expect("expected sorting_columns in row group metadata")
        .clone();
    assert_eq!(sorting.len(), 1);
    assert_eq!(sorting[0].column_idx, 3);

    Ok(())
}

fn find_parquet_file(
    path: &std::path::Path,
) -> std::io::Result<Option<std::path::PathBuf>> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(path) = find_parquet_file(&path)? {
                return Ok(Some(path));
            }
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            return Ok(Some(path));
        }
    }

    Ok(None)
}
