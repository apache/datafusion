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
use datafusion::prelude::*;
use std::fs;
use std::sync::Arc;
// Import your nested schema adapter
use datafusion::datasource::nested_schema_adapter::{
    NestedStructSchemaAdapter, NestedStructSchemaAdapterFactory,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Running nested schema evolution test with the NestedStructSchemaAdapter..."
    );

    let ctx = SessionContext::new();

    let schema1 = Arc::new(Schema::new(vec![
        Field::new("component", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("stack", DataType::Utf8, true),
        Field::new("timestamp", DataType::Utf8, true),
        Field::new(
            "timestamp_utc",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        ),
    ]));

    let batch1 = RecordBatch::try_new(
        schema1.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("component1")])),
            Arc::new(StringArray::from(vec![Some("message1")])),
            Arc::new(StringArray::from(vec![Some("stack_trace")])),
            Arc::new(StringArray::from(vec![Some("2025-02-18T00:00:00Z")])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1640995200000)])),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("location", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec![Some("USA")])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    )),
                    Arc::new(TimestampMillisecondArray::from(vec![Some(1640995200000)])),
                ),
            ])),
        ],
    )?;

    let path1 = "test_data1.parquet";
    let _ = fs::remove_file(path1);

    let df1 = ctx.read_batch(batch1)?;
    df1.write_parquet(
        path1,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    let schema2 = Arc::new(Schema::new(vec![
        Field::new("component", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("stack", DataType::Utf8, true),
        Field::new("timestamp", DataType::Utf8, true),
        Field::new(
            "timestamp_utc",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "additionalInfo",
            DataType::Struct(
                vec![
                    Field::new("location", DataType::Utf8, true),
                    Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    ),
                    Field::new(
                        "reason",
                        DataType::Struct(
                            vec![
                                Field::new("_level", DataType::Float64, true),
                                Field::new(
                                    "details",
                                    DataType::Struct(
                                        vec![
                                            Field::new("rurl", DataType::Utf8, true),
                                            Field::new("s", DataType::Float64, true),
                                            Field::new("t", DataType::Utf8, true),
                                        ]
                                        .into(),
                                    ),
                                    true,
                                ),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ]
                .into(),
            ),
            true,
        ),
    ]));

    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec![Some("component2")])),
            Arc::new(StringArray::from(vec![Some("message2")])),
            Arc::new(StringArray::from(vec![Some("stack_trace2")])),
            Arc::new(StringArray::from(vec![Some("2025-03-18T00:00:00Z")])),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1643673600000)])),
            Arc::new(StructArray::from(vec![
                (
                    Arc::new(Field::new("location", DataType::Utf8, true)),
                    Arc::new(StringArray::from(vec![Some("Canada")])) as Arc<dyn Array>,
                ),
                (
                    Arc::new(Field::new(
                        "timestamp_utc",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        true,
                    )),
                    Arc::new(TimestampMillisecondArray::from(vec![Some(1643673600000)])),
                ),
                (
                    Arc::new(Field::new(
                        "reason",
                        DataType::Struct(
                            vec![
                                Field::new("_level", DataType::Float64, true),
                                Field::new(
                                    "details",
                                    DataType::Struct(
                                        vec![
                                            Field::new("rurl", DataType::Utf8, true),
                                            Field::new("s", DataType::Float64, true),
                                            Field::new("t", DataType::Utf8, true),
                                        ]
                                        .into(),
                                    ),
                                    true,
                                ),
                            ]
                            .into(),
                        ),
                        true,
                    )),
                    Arc::new(StructArray::from(vec![
                        (
                            Arc::new(Field::new("_level", DataType::Float64, true)),
                            Arc::new(Float64Array::from(vec![Some(1.5)]))
                                as Arc<dyn Array>,
                        ),
                        (
                            Arc::new(Field::new(
                                "details",
                                DataType::Struct(
                                    vec![
                                        Field::new("rurl", DataType::Utf8, true),
                                        Field::new("s", DataType::Float64, true),
                                        Field::new("t", DataType::Utf8, true),
                                    ]
                                    .into(),
                                ),
                                true,
                            )),
                            Arc::new(StructArray::from(vec![
                                (
                                    Arc::new(Field::new("rurl", DataType::Utf8, true)),
                                    Arc::new(StringArray::from(vec![Some(
                                        "https://example.com",
                                    )]))
                                        as Arc<dyn Array>,
                                ),
                                (
                                    Arc::new(Field::new("s", DataType::Float64, true)),
                                    Arc::new(Float64Array::from(vec![Some(3.14)]))
                                        as Arc<dyn Array>,
                                ),
                                (
                                    Arc::new(Field::new("t", DataType::Utf8, true)),
                                    Arc::new(StringArray::from(vec![Some("data")]))
                                        as Arc<dyn Array>,
                                ),
                            ])),
                        ),
                    ])),
                ),
            ])),
        ],
    )?;

    let path2 = "test_data2.parquet";
    let _ = fs::remove_file(path2);

    let df2 = ctx.read_batch(batch2)?;
    df2.write_parquet(
        path2,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    println!("Created two parquet files with different schemas");
    println!("File 1: Basic schema without 'reason' field");
    println!("File 2: Enhanced schema with 'reason' field");

    // First try with the default schema adapter (should fail)
    println!("\nAttempting to read both files with default schema adapter...");
    let paths_str = vec![path1.to_string(), path2.to_string()];

    let mut config = ListingTableConfig::new_with_multi_paths(
        paths_str
            .clone()
            .into_iter()
            .map(|p| ListingTableUrl::parse(&p))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_schema(schema2.as_ref().clone().into());

    // Let this use the default schema adapter
    let inferred_config = config.infer(&ctx.state()).await;

    if inferred_config.is_err() {
        println!(
            "As expected, default schema adapter failed with error: {:?}",
            inferred_config.err()
        );
    } else {
        println!("Unexpected: Default adapter succeeded when it should have failed");
    }

    // Now try with NestedStructSchemaAdapter
    println!("\nNow trying with NestedStructSchemaAdapter...");
    let mut config = ListingTableConfig::new_with_multi_paths(
        paths_str
            .into_iter()
            .map(|p| ListingTableUrl::parse(&p))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_schema(schema2.as_ref().clone().into());

    // Set our custom schema adapter
    config.schema_adapter = Some(NestedStructSchemaAdapterFactory);

    let config = config.infer(&ctx.state()).await?;

    // Add sorting options
    let config = ListingTableConfig {
        options: Some(ListingOptions {
            file_sort_order: vec![vec![col("timestamp_utc").sort(true, true)]],
            ..config.options.unwrap_or_else(|| {
                ListingOptions::new(Arc::new(ParquetFormat::default()))
            })
        }),
        ..config
    };

    let listing_table = ListingTable::try_new(config)?;
    ctx.register_table("events", Arc::new(listing_table))?;

    println!("Successfully created listing table with both files using NestedStructSchemaAdapter");
    println!("Executing query across both files...");

    let df = ctx
        .sql("SELECT * FROM events ORDER BY timestamp_utc")
        .await?;
    let results = df.clone().collect().await?;

    println!("Query successful! Got {} rows", results[0].num_rows());
    assert_eq!(results[0].num_rows(), 2);

    // Compact the data and verify
    let compacted_path = "test_data_compacted.parquet";
    let _ = fs::remove_file(compacted_path);

    println!("\nCompacting data into a single file...");
    df.write_parquet(
        compacted_path,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    // Verify compacted file has the complete schema
    println!("Reading compacted file...");
    let new_ctx = SessionContext::new();
    let mut config =
        ListingTableConfig::new_with_multi_paths(vec![ListingTableUrl::parse(
            compacted_path,
        )?])
        .with_schema(schema2.as_ref().clone().into());

    // Use our custom adapter for the compacted file too
    config.schema_adapter = Some(NestedStructSchemaAdapterFactory);

    let config = config.infer(&new_ctx.state()).await?;

    let listing_table = ListingTable::try_new(config)?;
    new_ctx.register_table("events", Arc::new(listing_table))?;

    let df = new_ctx
        .sql("SELECT * FROM events ORDER BY timestamp_utc")
        .await?;
    let compacted_results = df.collect().await?;

    println!(
        "Successfully read compacted file, found {} rows",
        compacted_results[0].num_rows()
    );
    assert_eq!(compacted_results[0].num_rows(), 2);

    // Check that results are equivalent
    assert_eq!(results, compacted_results);

    println!("\nVerifying schema of compacted file includes all fields...");
    let result_schema = compacted_results[0].schema();

    // Check additionalInfo.reason field exists
    let additional_info_idx = result_schema.index_of("additionalInfo")?;
    let additional_info_field = result_schema.field(additional_info_idx);

    if let DataType::Struct(fields) = additional_info_field.data_type() {
        // Find the reason field
        let reason_field = fields.iter().find(|f| f.name() == "reason");
        if reason_field.is_some() {
            println!("Success! Found 'reason' field in the result schema.");
        } else {
            println!("Error: 'reason' field not found in additionalInfo struct");
            return Err("Missing reason field in results".into());
        }
    } else {
        println!("Error: additionalInfo is not a struct");
        return Err("additionalInfo is not a struct".into());
    }

    // Clean up files
    println!("\nCleaning up test files...");
    let _ = fs::remove_file(path1);
    let _ = fs::remove_file(path2);
    let _ = fs::remove_file(compacted_path);

    println!("\nTest completed successfully!");
    Ok(())
}
