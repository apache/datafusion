// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for ordering in Parquet sorting_columns metadata

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
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

#[tokio::test]
async fn test_parquet_unordered_row_groups_require_sort() -> Result<()> {
    for descending in [false, true] {
        for nulls_first in [false, true] {
            // Arrange
            let options = SortOptions {
                descending,
                nulls_first,
            };
            let mut overlapping = [Some(1), Some(4), Some(2), Some(3)];
            let mut reversed = [Some(3), Some(4), Some(1), Some(2)];
            let mut expected = [Some(1), Some(2), Some(3), Some(4)];
            if descending {
                overlapping.reverse();
                reversed.reverse();
                expected.reverse();
            }
            let (first, second) = if descending {
                (Some(2), Some(1))
            } else {
                (Some(1), Some(2))
            };
            let (null_boundary, expected_nulls) = if nulls_first {
                ([None, first, None, second], [None, None, first, second])
            } else {
                ([first, None, second, None], [first, second, None, None])
            };
            let direction = if descending { "DESC" } else { "ASC" };
            let nulls = if nulls_first { "FIRST" } else { "LAST" };
            let query = format!("SELECT a FROM t ORDER BY a {direction} NULLS {nulls}");
            for (name, values, expected) in [
                ("overlapping", overlapping, expected),
                ("reversed", reversed, expected),
                ("null boundary", null_boundary, expected_nulls),
            ] {
                let batch = RecordBatch::try_from_iter([(
                    "a",
                    Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
                )])?;

                // Act
                let (batches, plan) =
                    query_row_groups(&batch, &[options], &query).await?;

                // Assert
                assert_eq!(int64_values(&batches, 0), expected, "{name}: {plan}");
                assert!(plan.contains("SortExec"), "{name}: {plan}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_parquet_ordered_row_groups_eliminate_sort() -> Result<()> {
    for descending in [false, true] {
        for nulls_first in [false, true] {
            // Arrange
            let options = SortOptions {
                descending,
                nulls_first,
            };
            let mut separated = [Some(1), Some(2), Some(3), Some(4)];
            let mut touching = [Some(1), Some(2), Some(2), Some(3)];
            if descending {
                separated.reverse();
                touching.reverse();
            }
            let (first, second) = if descending {
                (Some(2), Some(1))
            } else {
                (Some(1), Some(2))
            };
            let all_null_group = if nulls_first {
                [None, None, first, second]
            } else {
                [first, second, None, None]
            };
            let direction = if descending { "DESC" } else { "ASC" };
            let nulls = if nulls_first { "FIRST" } else { "LAST" };
            let query = format!("SELECT a FROM t ORDER BY a {direction} NULLS {nulls}");
            for (name, values) in [
                ("separated", separated),
                ("touching", touching),
                ("all-null group", all_null_group),
            ] {
                let batch = RecordBatch::try_from_iter([(
                    "a",
                    Arc::new(Int64Array::from(values.to_vec())) as ArrayRef,
                )])?;

                // Act
                let (batches, plan) =
                    query_row_groups(&batch, &[options], &query).await?;

                // Assert
                assert_eq!(int64_values(&batches, 0), values, "{name}: {plan}");
                assert!(!plan.contains("SortExec"), "{name}: {plan}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_parquet_unordered_row_groups_with_limit() -> Result<()> {
    // Arrange
    let batch = RecordBatch::try_from_iter([(
        "a",
        Arc::new(Int64Array::from(vec![1, 4, 2, 3])) as ArrayRef,
    )])?;

    // Act
    let (batches, plan) = query_row_groups(
        &batch,
        &[SortOptions::default()],
        "SELECT a FROM t ORDER BY a ASC NULLS FIRST LIMIT 2",
    )
    .await?;

    // Assert
    assert_eq!(int64_values(&batches, 0), [Some(1), Some(2)], "{plan}");
    assert!(plan.contains("SortExec"), "{plan}");
    Ok(())
}

#[tokio::test]
async fn test_parquet_row_group_lexicographic_ordering() -> Result<()> {
    for descending in [false, true] {
        for (name, a, mut b, requires_sort) in [
            ("strict leading key", [1, 1, 2, 2], [40, 50, 10, 20], false),
            ("tied leading key", [1, 1, 1, 1], [10, 20, 30, 40], false),
            ("overlapping suffix", [1, 1, 1, 1], [10, 40, 20, 30], true),
        ] {
            // Arrange
            if descending {
                b.reverse();
            }
            let expected_b = if requires_sort {
                if descending {
                    [40, 30, 20, 10]
                } else {
                    [10, 20, 30, 40]
                }
            } else {
                b
            };
            let batch = RecordBatch::try_from_iter([
                ("a", Arc::new(Int64Array::from(a.to_vec())) as ArrayRef),
                ("b", Arc::new(Int64Array::from(b.to_vec())) as ArrayRef),
            ])?;
            let direction = if descending { "DESC" } else { "ASC" };
            let query = format!(
                "SELECT a, b FROM t ORDER BY a ASC NULLS FIRST, b {direction} NULLS FIRST"
            );

            // Act
            let (batches, plan) = query_row_groups(
                &batch,
                &[
                    SortOptions::default(),
                    SortOptions {
                        descending,
                        nulls_first: true,
                    },
                ],
                &query,
            )
            .await?;

            // Assert
            assert_eq!(int64_values(&batches, 0), a.map(Some), "{name}: {plan}");
            assert_eq!(
                int64_values(&batches, 1),
                expected_b.map(Some),
                "{name}: {plan}"
            );
            assert_eq!(plan.contains("SortExec"), requires_sort, "{name}: {plan}");
        }
    }
    Ok(())
}

async fn query_row_groups(
    batch: &RecordBatch,
    sort_options: &[SortOptions],
    query: &str,
) -> Result<(Vec<RecordBatch>, String)> {
    let directory = tempdir()?;
    let path = directory.path().join("data.parquet");
    let sorting_columns = sort_options
        .iter()
        .zip(0..)
        .map(|(options, column_idx)| SortingColumn {
            column_idx,
            descending: options.descending,
            nulls_first: options.nulls_first,
        })
        .collect();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .set_sorting_columns(Some(sorting_columns))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path)?,
        batch.schema(),
        Some(properties),
    )?;
    writer.write(batch)?;
    writer.close()?;

    let ctx =
        SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let plan = ctx.sql(query).await?.create_physical_plan().await?;
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(plan, ctx.task_ctx()).await?;
    Ok((batches, plan_text))
}

fn int64_values(batches: &[RecordBatch], column: usize) -> Vec<Option<i64>> {
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(column)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect()
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
