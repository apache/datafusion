// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for ordering in Parquet sorting_columns metadata

use std::fs::File;

use arrow::array::{RecordBatch, record_batch};
use arrow::compute::SortOptions;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::test_util::batches_to_string;
use insta::assert_snapshot;
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

const ASC_NULLS_LAST: SortOptions = SortOptions {
    descending: false,
    nulls_first: false,
};

/// Each row group is sorted by `a`, but the file is not.
#[tokio::test]
async fn test_unordered_row_groups_are_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [1, 4]))?,
        record_batch!(("a", Int64, [2, 3]))?,
    ];

    let output = query_row_groups(
        &row_groups,
        ASC_NULLS_LAST,
        "SELECT a FROM t ORDER BY a LIMIT 2",
    )
    .await?;

    assert_snapshot!(output, @r"
    SortExec: TopK(fetch=2), expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a], file_type=parquet, predicate=DynamicFilter [ empty ], sort_order_for_reorder=[a@0 ASC NULLS LAST], dynamic_rg_pruning=eligible
    +---+
    | a |
    +---+
    | 1 |
    | 2 |
    +---+
    ");
    Ok(())
}

#[tokio::test]
async fn test_ordered_row_groups_are_not_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [1, 2]))?,
        record_batch!(("a", Int64, [2, 3]))?,
    ];

    let output =
        query_row_groups(&row_groups, ASC_NULLS_LAST, "SELECT a FROM t ORDER BY a")
            .await?;

    assert_snapshot!(output, @r"
    DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
    +---+
    | a |
    +---+
    | 1 |
    | 2 |
    | 2 |
    | 3 |
    +---+
    ");
    Ok(())
}

#[tokio::test]
async fn test_unordered_descending_row_groups_are_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [4, 1]))?,
        record_batch!(("a", Int64, [3, 2]))?,
    ];
    let sort_options = SortOptions {
        descending: true,
        nulls_first: true,
    };

    let output =
        query_row_groups(&row_groups, sort_options, "SELECT a FROM t ORDER BY a DESC")
            .await?;

    assert_snapshot!(output, @r"
    SortExec: expr=[a@0 DESC], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a], file_type=parquet, sort_order_for_reorder=[a@0 DESC], reverse_row_groups=true
    +---+
    | a |
    +---+
    | 4 |
    | 3 |
    | 2 |
    | 1 |
    +---+
    ");
    Ok(())
}

/// The NULL at the end of the first row group comes before the values of the
/// second row group.
#[tokio::test]
async fn test_row_groups_with_unordered_nulls_are_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [Some(1), None]))?,
        record_batch!(("a", Int64, [Some(2), Some(3)]))?,
    ];

    let output =
        query_row_groups(&row_groups, ASC_NULLS_LAST, "SELECT a FROM t ORDER BY a")
            .await?;

    assert_snapshot!(output, @r"
    SortExec: expr=[a@0 ASC NULLS LAST], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a], file_type=parquet, sort_order_for_reorder=[a@0 ASC NULLS LAST]
    +---+
    | a |
    +---+
    | 1 |
    | 2 |
    | 3 |
    |   |
    +---+
    ");
    Ok(())
}

#[tokio::test]
async fn test_row_groups_with_ordered_nulls_are_not_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [None, None]))?,
        record_batch!(("a", Int64, [None, Some(1)]))?,
        record_batch!(("a", Int64, [Some(2), Some(3)]))?,
    ];
    let sort_options = SortOptions {
        descending: false,
        nulls_first: true,
    };

    let output = query_row_groups(
        &row_groups,
        sort_options,
        "SELECT a FROM t ORDER BY a NULLS FIRST",
    )
    .await?;

    assert_snapshot!(output, @r"
    DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a], output_ordering=[a@0 ASC], file_type=parquet
    +---+
    | a |
    +---+
    |   |
    |   |
    |   |
    | 1 |
    | 2 |
    | 3 |
    +---+
    ");
    Ok(())
}

/// `b` starts again in the second row group. This is ordered because `a`
/// increases between the row groups.
#[tokio::test]
async fn test_row_groups_ordered_by_two_columns_are_not_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [1, 1]), ("b", Int64, [8, 9]))?,
        record_batch!(("a", Int64, [2, 2]), ("b", Int64, [0, 1]))?,
    ];

    let output = query_row_groups(
        &row_groups,
        ASC_NULLS_LAST,
        "SELECT a, b FROM t ORDER BY a, b",
    )
    .await?;

    assert_snapshot!(output, @r"
    DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a, b], output_ordering=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST], file_type=parquet
    +---+---+
    | a | b |
    +---+---+
    | 1 | 8 |
    | 1 | 9 |
    | 2 | 0 |
    | 2 | 1 |
    +---+---+
    ");
    Ok(())
}

/// `b` starts again in the second row group, but `a` does not increase. The
/// file is ordered by `a` only.
#[tokio::test]
async fn test_row_groups_ordered_by_first_column_only_are_sorted() -> Result<()> {
    let row_groups = [
        record_batch!(("a", Int64, [1, 1]), ("b", Int64, [8, 9]))?,
        record_batch!(("a", Int64, [1, 1]), ("b", Int64, [0, 1]))?,
    ];

    let output = query_row_groups(
        &row_groups,
        ASC_NULLS_LAST,
        "SELECT a, b FROM t ORDER BY a, b",
    )
    .await?;

    assert_snapshot!(output, @r"
    SortExec: expr=[a@0 ASC NULLS LAST, b@1 ASC NULLS LAST], preserve_partitioning=[false]
      DataSourceExec: file_groups={1 group: [[data.parquet]]}, projection=[a, b], output_ordering=[a@0 ASC NULLS LAST], file_type=parquet
    +---+---+
    | a | b |
    +---+---+
    | 1 | 0 |
    | 1 | 1 |
    | 1 | 8 |
    | 1 | 9 |
    +---+---+
    ");
    Ok(())
}

/// Writes one Parquet file with one row group per batch. Every row group
/// declares that it is sorted by all columns with `sort_options`. Returns the
/// plan and the results of `sql`, which reads the file as table `t`.
async fn query_row_groups(
    row_groups: &[RecordBatch],
    sort_options: SortOptions,
    sql: &str,
) -> Result<String> {
    let tmp_dir = tempdir()?;
    let path = tmp_dir.path().join("data.parquet");
    let schema = row_groups[0].schema();
    let sorting_columns = (0..schema.fields().len() as i32)
        .map(|column_idx| SortingColumn {
            column_idx,
            descending: sort_options.descending,
            nulls_first: sort_options.nulls_first,
        })
        .collect();
    let properties = WriterProperties::builder()
        .set_sorting_columns(Some(sorting_columns))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path)?, schema, Some(properties))?;
    for row_group in row_groups {
        writer.write(row_group)?;
        writer.flush()?;
    }
    writer.close()?;

    let config = SessionConfig::new().with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_parquet("t", path.to_str().unwrap(), ParquetReadOptions::default())
        .await?;
    let df = ctx.sql(sql).await?;
    let plan = df.clone().create_physical_plan().await?;
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    let directory = format!("{}/", tmp_dir.path().display());
    let plan = plan.replace(directory.trim_start_matches('/'), "");
    let results = df.collect().await?;
    Ok(format!("{plan}{}", batches_to_string(&results)))
}
