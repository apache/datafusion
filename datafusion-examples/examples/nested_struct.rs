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

use datafusion::arrow::array::{
    Array, Float64Array, StringArray, StructArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::nested_schema_adapter::NestedStructSchemaAdapterFactory;
use datafusion::datasource::schema_adapter::SchemaAdapterFactory;
use datafusion::prelude::*;
use std::error::Error;
use std::fs;
use std::sync::Arc;
// Remove the tokio::test attribute to make this a regular async function
async fn test_datafusion_schema_evolution() -> Result<(), Box<dyn Error>> {
    println!("==> Starting test function");
    let ctx = SessionContext::new();
    println!("==> Session context created");

    println!("==> Creating schema1 (simple additionalInfo structure)");
    let schema1 = create_schema1();
    println!("==> Schema1 created");

    println!("==> Creating schema4");
    let schema4 = create_schema4();
    println!("==> Schema4 created");

    println!("==> Creating batch from schema1");
    let batch1 = create_batch(&schema1)?;
    println!(
        "==> Batch created successfully with {} rows",
        batch1.num_rows()
    );

    println!("==> Creating schema adapter factory");
    let adapter_factory: Arc<dyn SchemaAdapterFactory> =
        Arc::new(NestedStructSchemaAdapterFactory);
    println!("==> Schema adapter factory created");

    let path1 = "test_data1.parquet";
    println!("==> Removing existing file if present: {}", path1);
    let _ = fs::remove_file(path1);
    println!("==> File removal attempted");

    println!("==> Creating DataFrame from batch");
    let df1 = ctx.read_batch(batch1)?;
    println!("==> DataFrame created successfully");

    println!("==> Writing first parquet file to {}", path1);
    df1.write_parquet(
        path1,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;
    println!("==> Successfully wrote first parquet file");
    
    // Create and write path2 (schema2)
    println!("==> Creating schema2 (with query_params field)");
    let schema2 = create_schema2();
    println!("==> Schema2 created");
    
    println!("==> Creating batch from schema2");
    let batch2 = create_batch(&schema2)?;
    println!("==> Batch2 created successfully with {} rows", batch2.num_rows());
    
    let path2 = "test_data2.parquet";
    println!("==> Removing existing file if present: {}", path2);
    let _ = fs::remove_file(path2);
    
    println!("==> Creating DataFrame from batch2");
    let df2 = ctx.read_batch(batch2)?;
    println!("==> Writing schema2 parquet file to {}", path2);
    df2.write_parquet(
        path2,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;
    println!("==> Successfully wrote schema2 parquet file");
    
    // Create and write path3 (schema3)
    println!("==> Creating schema3 (with query_params and error fields)");
    let schema3 = create_schema3();
    println!("==> Schema3 created");
    
    println!("==> Creating batch from schema3");
    let batch3 = create_batch(&schema3)?;
    println!("==> Batch3 created successfully with {} rows", batch3.num_rows());
    
    let path3 = "test_data3.parquet";
    println!("==> Removing existing file if present: {}", path3);
    let _ = fs::remove_file(path3);
    
    println!("==> Creating DataFrame from batch3");
    let df3 = ctx.read_batch(batch3)?;
    println!("==> Writing schema3 parquet file to {}", path3);
    df3.write_parquet(
        path3,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;
    println!("==> Successfully wrote schema3 parquet file");
    
    println!("==> Creating schema4 (with expanded query_params and error fields)");

    let batch4 = create_batch(&schema4)?;

    let path4 = "test_data4.parquet";
    let _ = fs::remove_file(path4);

    let df4 = ctx.read_batch(batch4)?;
    println!("==> Writing schema4 parquet file to {}", path4);
    df4.write_parquet(
        path4,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;
    println!("==> Successfully wrote schema4 parquet file");

    let paths_str = vec![path1.to_string(), path2.to_string(), path3.to_string(), path4.to_string()];
    println!("==> Creating ListingTableConfig for paths: {:?}", paths_str);
    println!("==> Using schema4 for files with different schemas");
    println!(
        "==> Schema difference: schema evolution from basic to expanded fields"
    );

    let config = ListingTableConfig::new_with_multi_paths(
        paths_str
            .into_iter()
            .map(|p| ListingTableUrl::parse(&p))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_schema(schema4.as_ref().clone().into())
    .with_schema_adapter_factory(adapter_factory);

    println!("==> About to infer config");
    println!(
        "==> This is where schema adaptation happens between different file schemas"
    );
    let config = config.infer(&ctx.state()).await?;
    println!("==> Successfully inferred config");

    let config = ListingTableConfig {
        options: Some(ListingOptions {
            file_sort_order: vec![vec![col("timestamp_utc").sort(true, true)]],
            ..config.options.unwrap_or_else(|| {
                ListingOptions::new(Arc::new(ParquetFormat::default()))
            })
        }),
        ..config
    };

    println!("==> About to create ListingTable");
    let listing_table = ListingTable::try_new(config)?;
    println!("==> Successfully created ListingTable");

    println!("==> Registering table 'events'");
    ctx.register_table("events", Arc::new(listing_table))?;
    println!("==> Successfully registered table");

    println!("==> Executing SQL query");
    let df = ctx
        .sql("SELECT * FROM events ORDER BY timestamp_utc")
        .await?;
    println!("==> Successfully executed SQL query");

    println!("==> Collecting results");
    let results = df.clone().collect().await?;
    println!("==> Successfully collected results");

    assert_eq!(results[0].num_rows(), 2);

    let compacted_path = "test_data_compacted.parquet";
    let _ = fs::remove_file(compacted_path);

    println!("==> writing compacted parquet file to {}", compacted_path);
    df.write_parquet(
        compacted_path,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    let new_ctx = SessionContext::new();
    let config = ListingTableConfig::new_with_multi_paths(vec![ListingTableUrl::parse(
        compacted_path,
    )?])
    .with_schema(schema4.as_ref().clone().into())
    .infer(&new_ctx.state())
    .await?;

    let listing_table = ListingTable::try_new(config)?;
    new_ctx.register_table("events", Arc::new(listing_table))?;

    println!("==> select from compacted parquet file");
    let df = new_ctx
        .sql("SELECT * FROM events ORDER BY timestamp_utc")
        .await?;
    let compacted_results = df.collect().await?;

    assert_eq!(compacted_results[0].num_rows(), 2);
    assert_eq!(results, compacted_results);

    let _ = fs::remove_file(path1);
    let _ = fs::remove_file(path2);
    let _ = fs::remove_file(path3);
    let _ = fs::remove_file(path4);
    let _ = fs::remove_file(compacted_path);

    Ok(())
            Ok(Arc::new(StructArray::from(struct_arrays)))
        }
        _ => Err(format!("Unsupported data type: {}", field.data_type()).into()),
    }
}

fn create_schema1() -> Arc<Schema> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
        Field::new("method", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("status_code", DataType::Float64, true),
        Field::new("time_taken", DataType::Float64, true),
        Field::new("timestamp", DataType::Utf8, true),
        Field::new("uid", DataType::Utf8, true),
        Field::new("url", DataType::Utf8, true),
        Field::new(
            "timestamp_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
    ]));
    schema1
}

/// Creates a schema with basic HTTP request fields plus a query_params struct field
fn create_schema2() -> Arc<Schema> {
    // Get the base schema from create_schema1
    let schema1 = create_schema1();

    // Convert to a vector of fields
    let fields = schema1.fields().to_vec();
    // Create a new vector of fields from schema1
    let mut fields = schema1
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<Field>>();

    // Add the query_params field
    fields.push(Field::new(
        "query_params",
        DataType::Struct(vec![Field::new("customer_id", DataType::Utf8, true)].into()),
        true,
    ));

    // Create a new schema with the extended fields
    Arc::new(Schema::new(fields))
}

/// Creates a schema with HTTP request fields, query_params struct field, and an error field
fn create_schema3() -> Arc<Schema> {
    // Get the base schema from create_schema2
    let schema2 = create_schema2();

    // Convert to a vector of fields
    let mut fields = schema2
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<Field>>();

    // Add the error field
    fields.push(Field::new("error", DataType::Utf8, true));

    // Create a new schema with the extended fields
    Arc::new(Schema::new(fields))
}

/// Creates a schema with HTTP request fields, expanded query_params struct with additional fields, and an error field
fn create_schema4() -> Arc<Schema> {
    // Get the base schema from create_schema1 (we can't use schema3 directly since we need to modify query_params)
    let schema1 = create_schema1();

    // Convert to a vector of fields
    let mut fields = schema1
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect::<Vec<Field>>();

    // Add the expanded query_params field with additional fields
    fields.push(Field::new(
        "query_params",
        DataType::Struct(
            vec![
                Field::new("customer_id", DataType::Utf8, true),
                Field::new("document_type", DataType::Utf8, true),
                Field::new("fetch_from_source", DataType::Utf8, true),
                Field::new("source_system", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    ));

    // Add the error field
    fields.push(Field::new("error", DataType::Utf8, true));

    // Create a new schema with the extended fields
    Arc::new(Schema::new(fields))
}

fn main() -> Result<(), Box<dyn Error>> {
    // Create a Tokio runtime for running our async function
    let rt = tokio::runtime::Runtime::new()?;

    // Run the function in the runtime
    rt.block_on(async { test_datafusion_schema_evolution().await })?;

    println!("Example completed successfully!");
    Ok(())
}
