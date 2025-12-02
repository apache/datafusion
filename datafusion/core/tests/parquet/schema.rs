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

//! Tests for parquet schema handling
use std::{collections::HashMap, fs, path::Path};

use tempfile::TempDir;

use super::*;
use datafusion_common::test_util::batches_to_sort_string;
use insta::assert_snapshot;

#[tokio::test]
async fn schema_merge_ignores_metadata_by_default() {
    // Create several parquet files in same directory / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    let options = ParquetReadOptions::default();

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (incompatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
        // field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "bar")),
            f2.clone(),
        ]),
        // incompatible field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "baz")),
            f2.clone(),
        ]),
        // schema with no meta
        Schema::new(vec![f1, f2]),
    ];
    write_files(table_dir.as_path(), schemas);

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();
    let actual = df.collect().await.unwrap();

    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    | 3  | test |
    | 4  | test |
    | 5  | test |
    +----+------+
    ");
    assert_no_metadata(&actual);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let actual = ctx
        .sql("SELECT * from t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    | 3  | test |
    | 4  | test |
    | 5  | test |
    +----+------+
    ");
    assert_no_metadata(&actual);
}

#[tokio::test]
async fn schema_merge_can_preserve_metadata() {
    // Create several parquet files in same directory / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    // explicitly disable schema clearing
    let options = ParquetReadOptions::default().skip_metadata(false);

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (compatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo2", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
    ];
    write_files(table_dir.as_path(), schemas);

    let mut expected_metadata = make_meta("foo", "bar");
    expected_metadata.insert("foo2".into(), "baz".into());

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata,);

    let actual = df.collect().await.unwrap();

    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    +----+------+
    ");
    assert_metadata(&actual, &expected_metadata);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let df = ctx.sql("SELECT * from t").await.unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata);

    let actual = df.collect().await.unwrap();
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    +----+------+
    ");
    assert_metadata(&actual, &expected_metadata);
}

fn make_meta(k: impl Into<String>, v: impl Into<String>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(k.into(), v.into());
    meta
}

/// Writes individual files with the specified schemas to temp_path)
///
/// Assumes each schema has an int32 and a string column
fn write_files(table_path: &Path, schemas: Vec<Schema>) {
    fs::create_dir(table_path).expect("Error creating temp dir");

    for (i, schema) in schemas.into_iter().enumerate() {
        let schema = Arc::new(schema);
        let filename = format!("part-{i}.parquet");
        let path = table_path.join(filename);
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        // create mock record batch
        let ids = Arc::new(Int32Array::from(vec![i as i32]));
        let names = Arc::new(StringArray::from(vec!["test"]));
        let rec_batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();
    }
}

fn assert_no_metadata(batches: &[RecordBatch]) {
    // all batches should have no metadata
    for batch in batches {
        assert!(
            batch.schema().metadata().is_empty(),
            "schema had metadata: {:?}",
            batch.schema()
        );
    }
}

fn assert_metadata(batches: &[RecordBatch], expected_metadata: &HashMap<String, String>) {
    // all batches should have no metadata
    for batch in batches {
        assert_eq!(batch.schema().metadata(), expected_metadata,);
    }
}

#[tokio::test]
async fn infer_schema_from_gzip_parquet() {
    // Test schema inference from a gzip-compressed parquet file
    let file_path = "../../../datafusion-benchmarks/tpcds/data/sf1/web_site.parquet";
    
    // Check if the file exists
    if !Path::new(file_path).exists() {
        eprintln!("Skipping test: file not found at {}", file_path);
        return;
    }

    let ctx = SessionContext::new();
    
    // Read the parquet file and infer schema
    let df = ctx
        .read_parquet(file_path, ParquetReadOptions::default())
        .await
        .expect("Failed to read parquet file");
    
    let schema = df.schema();
    
    // Verify that schema was successfully inferred
    assert!(
        !schema.fields().is_empty(),
        "Schema should have at least one field"
    );
    
    // Print schema for debugging
    println!("Inferred schema from gzip parquet file:");
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }
    
    // Verify we can actually read data from the file
    let results = df.collect().await.expect("Failed to collect results");
    
    // Verify we got some data
    let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    println!("Total rows read: {}", total_rows);
    
    assert!(
        total_rows > 0,
        "Should have read at least one row from the file"
    );
}

#[tokio::test]
async fn infer_schema_from_gzip_parquet_with_listing_options() {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
    use datafusion_common::file_options::file_type::DEFAULT_PARQUET_EXTENSION;
    
    // Test schema inference using ListingOptions and ParquetFormat
    let file_path = "../../../datafusion-benchmarks/tpcds/data/sf1/web_site.parquet";
    
    // Check if the file exists
    if !Path::new(file_path).exists() {
        eprintln!("Skipping test: file not found at {}", file_path);
        return;
    }

    let ctx = SessionContext::new();
    let state = ctx.state();
    
    // Create ParquetFormat with options from the session state
    let format = ParquetFormat::default()
        .with_options(state.table_options().parquet.clone());

    // Parse the file path as a ListingTableUrl
    let table_path = ListingTableUrl::parse(file_path)
        .expect("Failed to parse table path");
    
    // Create ListingOptions with the ParquetFormat
    let options = ListingOptions::new(Arc::new(format))
        .with_file_extension(DEFAULT_PARQUET_EXTENSION)
        .with_target_partitions(state.config().target_partitions())
        .with_collect_stat(state.config().collect_statistics());
    
    // Infer schema using the ListingOptions
    let schema = options
        .infer_schema(&state, &table_path)
        .await
        .expect("Failed to infer schema");
    
    // Verify that schema was successfully inferred
    assert!(
        !schema.fields().is_empty(),
        "Schema should have at least one field"
    );
    
    // Print schema for debugging
    println!("Inferred schema using ListingOptions:");
    for field in schema.fields() {
        println!("  - {}: {:?}", field.name(), field.data_type());
    }
    
    // Verify expected number of fields for web_site table
    assert_eq!(
        schema.fields().len(),
        26,
        "web_site table should have 26 fields"
    );
    
    // Verify some specific fields exist
    assert!(schema.field_with_name("web_site_sk").is_ok());
    assert!(schema.field_with_name("web_site_id").is_ok());
    assert!(schema.field_with_name("web_name").is_ok());
}
