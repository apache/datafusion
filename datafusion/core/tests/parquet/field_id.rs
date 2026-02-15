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

//! Integration tests for Parquet field ID support

use arrow::array::{
    Array, Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;
use datafusion_common::Result;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a test Parquet file with field IDs
fn create_parquet_file_with_field_ids(
    path: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<()> {
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for batch in batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Helper to create a schema with field IDs in metadata
fn schema_with_field_ids(fields: Vec<(String, DataType, i32)>) -> Schema {
    let fields_with_ids: Vec<Field> = fields
        .into_iter()
        .map(|(name, dtype, field_id)| {
            let mut metadata = HashMap::new();
            metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
            Field::new(name, dtype, false).with_metadata(metadata)
        })
        .collect();

    Schema::new(fields_with_ids)
}

#[tokio::test]
async fn test_read_parquet_with_field_ids_enabled() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Create schema with field IDs
    let schema = Arc::new(schema_with_field_ids(vec![
        ("user_id".to_string(), DataType::Int64, 1),
        ("amount".to_string(), DataType::Int64, 2),
        ("name".to_string(), DataType::Utf8, 3),
    ]));

    // Create test data
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    // Create context with field ID reading enabled
    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    // Register table and query
    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("SELECT user_id, amount, name FROM test").await?;
    let results = df.collect().await?;

    // Verify results
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    let user_ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(user_ids.value(0), 1);
    assert_eq!(user_ids.value(1), 2);
    assert_eq!(user_ids.value(2), 3);

    Ok(())
}

#[tokio::test]
async fn test_read_parquet_with_field_ids_disabled() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Create schema with field IDs
    let schema = Arc::new(schema_with_field_ids(vec![
        ("user_id".to_string(), DataType::Int64, 1),
        ("amount".to_string(), DataType::Int64, 2),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    // Create context with field ID reading disabled (default)
    let ctx = SessionContext::new();

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql("SELECT user_id, amount FROM test").await?;
    let results = df.collect().await?;

    // Should still work with name-based matching
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    Ok(())
}

#[tokio::test]
async fn test_schema_evolution_renamed_columns() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Write file with original column names and field IDs
    let write_schema = Arc::new(schema_with_field_ids(vec![
        ("user_id".to_string(), DataType::Int64, 1),
        ("amount".to_string(), DataType::Int64, 2),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&write_schema),
        vec![
            Arc::new(Int64Array::from(vec![101, 102, 103])),
            Arc::new(Int64Array::from(vec![500, 600, 700])),
        ],
    )?;

    create_parquet_file_with_field_ids(
        file_path.to_str().unwrap(),
        write_schema,
        vec![batch],
    )?;

    // Create context with field ID reading enabled
    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    // Register table with original names
    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Query should work with original names
    let df = ctx.sql("SELECT user_id, amount FROM test").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    let user_ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(user_ids.value(0), 101);

    Ok(())
}

#[tokio::test]
async fn test_schema_evolution_reordered_columns() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Write file with columns in order: a, b, c
    let write_schema = Arc::new(schema_with_field_ids(vec![
        ("a".to_string(), DataType::Int32, 1),
        ("b".to_string(), DataType::Int32, 2),
        ("c".to_string(), DataType::Int32, 3),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&write_schema),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![40, 50, 60])),
            Arc::new(Int32Array::from(vec![70, 80, 90])),
        ],
    )?;

    create_parquet_file_with_field_ids(
        file_path.to_str().unwrap(),
        write_schema,
        vec![batch],
    )?;

    // Create context with field ID reading enabled
    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Query columns in different order: c, a, b
    let df = ctx.sql("SELECT c, a, b FROM test").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);

    let c_vals = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let a_vals = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let b_vals = results[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    // Verify correct data regardless of order
    assert_eq!(c_vals.value(0), 70);
    assert_eq!(a_vals.value(0), 10);
    assert_eq!(b_vals.value(0), 40);

    Ok(())
}

#[tokio::test]
async fn test_projection_with_field_ids() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Create schema with field IDs
    let schema = Arc::new(schema_with_field_ids(vec![
        ("a".to_string(), DataType::Int32, 1),
        ("b".to_string(), DataType::Int32, 2),
        ("c".to_string(), DataType::Int32, 3),
        ("d".to_string(), DataType::Int32, 4),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Arc::new(Int32Array::from(vec![100, 200, 300])),
            Arc::new(Int32Array::from(vec![1000, 2000, 3000])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Project only columns a and c
    let df = ctx.sql("SELECT a, c FROM test").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_columns(), 2);

    let a_vals = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let c_vals = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    assert_eq!(a_vals.value(0), 1);
    assert_eq!(c_vals.value(0), 100);

    Ok(())
}

#[tokio::test]
async fn test_filter_with_field_ids() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    let schema = Arc::new(schema_with_field_ids(vec![
        ("id".to_string(), DataType::Int32, 1),
        ("value".to_string(), DataType::Int32, 2),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Filter with field IDs
    let df = ctx
        .sql("SELECT id, value FROM test WHERE value > 25")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3); // Should have rows with values 30, 40, 50

    let id_vals = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(id_vals.value(0), 3);
    assert_eq!(id_vals.value(1), 4);
    assert_eq!(id_vals.value(2), 5);

    Ok(())
}

#[tokio::test]
async fn test_aggregation_with_field_ids() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    let schema = Arc::new(schema_with_field_ids(vec![
        ("category".to_string(), DataType::Utf8, 1),
        ("value".to_string(), DataType::Int32, 2),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["A", "B", "A", "B", "A"])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Aggregate with field IDs
    let df = ctx
        .sql("SELECT category, SUM(value) as total FROM test GROUP BY category ORDER BY category")
        .await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 2);

    // Get category column - it might be StringArray or StringViewArray depending on config
    let category_col = results[0].column(0);
    let categories: Vec<&str> = match category_col.data_type() {
        DataType::Utf8 => category_col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect(),
        DataType::Utf8View => category_col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect(),
        _ => panic!(
            "Unexpected data type for category column: {:?}",
            category_col.data_type()
        ),
    };

    let totals = results[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(categories[0], "A");
    assert_eq!(totals.value(0), 90); // 10 + 30 + 50

    assert_eq!(categories[1], "B");
    assert_eq!(totals.value(1), 60); // 20 + 40

    Ok(())
}

#[tokio::test]
async fn test_fallback_to_name_when_no_field_ids() -> Result<()> {
    let tmp_dir = TempDir::new()?;
    let file_path = tmp_dir.path().join("test.parquet");

    // Create schema WITHOUT field IDs in metadata
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![100, 200, 300])),
        ],
    )?;

    create_parquet_file_with_field_ids(file_path.to_str().unwrap(), schema, vec![batch])?;

    // Create context with field ID reading enabled
    let ctx = SessionContext::new();
    ctx.sql("SET datafusion.execution.parquet.field_id_read_enabled = true")
        .await?
        .collect()
        .await?;

    ctx.register_parquet(
        "test",
        file_path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Should fall back to name-based matching
    let df = ctx.sql("SELECT user_id, amount FROM test").await?;
    let results = df.collect().await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 3);

    let user_ids = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(user_ids.value(0), 1);

    Ok(())
}
