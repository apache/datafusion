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

use datafusion::prelude::*;
use datafusion::{
    arrow::{
        array::Array, array::Float64Array, array::StringArray, array::StructArray,
        array::TimestampMillisecondArray, datatypes::DataType, datatypes::Field,
        datatypes::Schema, datatypes::TimeUnit, record_batch::RecordBatch,
    },
    dataframe::DataFrameWriteOptions,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        nested_schema_adapter::NestedStructSchemaAdapterFactory,
    },
};
use std::{error::Error, fs, sync::Arc};

/// Helper function to create a RecordBatch from a Schema and log the process
async fn create_and_write_parquet_file(
    ctx: &SessionContext,
    schema: &Arc<Schema>,
    schema_name: &str,
    file_path: &str,
) -> Result<(), Box<dyn Error>> {
    let batch = create_batch(schema, schema_name)?;

    let _ = fs::remove_file(file_path);

    let df = ctx.read_batch(batch)?;

    df.write_parquet(
        file_path,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    Ok(())
}

/// Helper function to create a ListingTableConfig for given paths and schema
async fn create_listing_table_config(
    ctx: &SessionContext,
    paths: impl Iterator<Item = String>,
    schema: &Arc<Schema>,
) -> Result<ListingTableConfig, Box<dyn Error>> {
    let config = ListingTableConfig::new_with_multi_paths(
        paths
            .map(|p| ListingTableUrl::parse(&p))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_schema(schema.as_ref().clone().into());

    let config = config.infer(&ctx.state()).await?;

    let updated_options = ListingOptions {
        file_sort_order: vec![vec![col("timestamp_utc").sort(true, true)]],
        ..config
            .options
            .clone()
            .unwrap_or_else(|| ListingOptions::new(Arc::new(ParquetFormat::default())))
    };

    let config = config.with_listing_options(updated_options);

    Ok(config)
}

/// Helper function to create a ListingTable from paths and schema
async fn create_listing_table(
    ctx: &SessionContext,
    paths: impl Iterator<Item = String>,
    schema: &Arc<Schema>,
) -> Result<Arc<ListingTable>, Box<dyn Error>> {
    // Create the config
    let config = create_listing_table_config(ctx, paths, schema).await?;

    // Add the NestedStructSchemaAdapterFactory to handle schema evolution
    let config =
        config.with_schema_adapter_factory(Arc::new(NestedStructSchemaAdapterFactory));

    // Create the listing table
    let listing_table = ListingTable::try_new(config)?;

    Ok(Arc::new(listing_table))
}

/// Helper function to register a table and execute a query
async fn execute_and_display_query(
    ctx: &SessionContext,
    table_name: &str,
    listing_table: Arc<ListingTable>,
) -> Result<(), Box<dyn Error>> {
    println!("==> {}", table_name);
    ctx.register_table(table_name, listing_table)?;

    // Derive query automatically based on table name
    let query = format!("SELECT * FROM {} ORDER BY body", table_name);
    let df = ctx.sql(&query).await?;

    let _results = df.clone().collect().await?;
    // Print the results
    df.show().await?;

    Ok(())
}

async fn test_datafusion_schema_evolution() -> Result<(), Box<dyn Error>> {
    let ctx = SessionContext::new();

    // Create schemas
    let schema1 = create_schema1();
    let schema2 = create_schema2();
    let schema3 = create_schema3();
    let schema4 = create_schema4();

    // Define file paths in an array for easier management
    let test_files = ["jobs1.parquet", "jobs2.parquet", "jobs3.parquet"];
    let [path1, path2, path3] = test_files; // Destructure for individual access

    // Create and write parquet files for each schema
    create_and_write_parquet_file(&ctx, &schema1, "schema1", path1).await?;
    create_and_write_parquet_file(&ctx, &schema2, "schema2", path2).await?;
    create_and_write_parquet_file(&ctx, &schema3, "schema3", path3).await?;

    let paths = vec![path1.to_string(), path2.to_string(), path3.to_string()].into_iter();
    let paths2 = vec![path1.to_string(), path2.to_string(), path3.to_string()]
        .into_iter()
        .rev();

    // Use the helper function to create the listing tables with different paths
    let table_paths = create_listing_table(&ctx, paths, &schema4).await?;
    let table_paths2 = create_listing_table(&ctx, paths2, &schema4).await?;

    // Execute query on the first table with table name "paths"
    execute_and_display_query(
        &ctx,
        "paths", // First table with original path order
        table_paths,
    )
    .await?;

    // Execute query on the second table with table name "paths2"
    execute_and_display_query(
        &ctx,
        "paths_reversed", // Second table with reversed path order
        table_paths2,
    )
    .await?;

    Ok(())
}

fn create_batch(
    schema: &Arc<Schema>,
    schema_name: &str,
) -> Result<RecordBatch, Box<dyn Error>> {
    // Create arrays for each field in the schema
    let columns = schema
        .fields()
        .iter()
        .map(|field| create_array_for_field(field, 1, schema_name))
        .collect::<Result<Vec<_>, _>>()?;

    // Create record batch with the generated arrays
    RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.into())
}

/// Creates an appropriate array for a given field with the specified length
fn create_array_for_field(
    field: &Field,
    length: usize,
    schema_name: &str,
) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    match field.data_type() {
        DataType::Utf8 => {
            // For the body field, use schema_name; for other fields use default
            if field.name() == "body" {
                Ok(Arc::new(StringArray::from(vec![Some(schema_name); length])))
            } else {
                let default_value = format!("{}_{}", field.name(), 1);
                Ok(Arc::new(StringArray::from(vec![
                    Some(default_value);
                    length
                ])))
            }
        }
        DataType::Float64 => {
            // Default float value
            Ok(Arc::new(Float64Array::from(vec![Some(1.0); length])))
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            // Default timestamp (2021-12-31T12:00:00Z)
            let array =
                TimestampMillisecondArray::from(vec![Some(1640995200000); length]);
            // Create the array with the same timezone as specified in the field
            Ok(Arc::new(array.with_data_type(DataType::Timestamp(
                TimeUnit::Millisecond,
                tz.clone(),
            ))))
        }
        DataType::Struct(fields) => {
            // Create arrays for each field in the struct
            let struct_arrays = fields
                .iter()
                .map(|f| {
                    let array = create_array_for_field(f, length, schema_name)?;
                    Ok((f.clone(), array))
                })
                .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

            Ok(Arc::new(StructArray::from(struct_arrays)))
        }
        _ => Err(format!("Unsupported data type: {}", field.data_type()).into()),
    }
}

fn create_schema1() -> Arc<Schema> {
    let schema1 = Arc::new(Schema::new(vec![
        Field::new("body", DataType::Utf8, true),
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
