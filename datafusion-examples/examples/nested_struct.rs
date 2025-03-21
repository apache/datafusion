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
use datafusion::prelude::*;
use std::fs;
use std::sync::Arc;

// Remove the tokio::test attribute to make this a regular async function
async fn test_datafusion_schema_evolution_with_compaction(
) -> Result<(), Box<dyn std::error::Error>> {
    println!("==> Starting test function");
    let ctx = SessionContext::new();

    println!("==> Creating schema1 (simple additionalInfo structure)");
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
    println!("==> Creating schema2 (extended additionalInfo with nested reason field)");

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
    println!("==> Writing second parquet file to {}", path2);
    df2.write_parquet(
        path2,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;
    println!("==> Successfully wrote second parquet file");

    // Let's manually read both parquet files and apply the schema adapter
    println!("==> Reading the first parquet file and applying schema adapter");
    let parquet_exec1 = ctx
        .read_parquet(path1, ParquetReadOptions::default())
        .await?;
    let batch1 = parquet_exec1.collect().await?[0].clone();

    println!("==> First file schema: {:?}", batch1.schema());

    println!("==> Reading the second parquet file");
    let parquet_exec2 = ctx
        .read_parquet(path2, ParquetReadOptions::default())
        .await?;
    let batch2 = parquet_exec2.collect().await?[0].clone();

    println!("==> Second file schema: {:?}", batch2.schema());

    // Create combined batches with schema adapter
    println!("==> Creating schema adapter");
    let adapter = NestedStructSchemaAdapterFactory::create_appropriate_adapter(
        schema2.clone(),
        schema2.clone(),
    );

    // Apply schema adapter to the first batch to make it compatible with schema2
    println!("==> Applying schema adapter to first batch");
    let (mapping, _) = adapter
        .map_schema(&batch1.schema(), &schema2)
        .expect("map schema failed");
    let mapped_batch1 = mapping.map_batch(&batch1).unwrap();

    // Second batch already has schema2, so no adaptation needed
    let mapped_batch2 = batch2;

    // Create a table with the adapted batches
    println!("==> Creating new dataframe with adapted batches");
    let combined_df = ctx
        .read_batch(mapped_batch1)?
        .union_distinct(ctx.read_batch(mapped_batch2)?)?;

    println!("==> Combined schema: {:?}", combined_df.schema());

    // Write the combined data to a new table for querying
    let combined_table_name = "events";
    ctx.register_table(combined_table_name, combined_df)?;

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

    // Also apply the adapter for the compacted file
    println!("==> Creating config for compacted file with schema adapter");
    let adapter = NestedStructSchemaAdapterFactory::create_appropriate_adapter(
        schema2.clone(),
        schema2.clone(),
    );
    let config = ListingTableConfig::new_with_multi_paths(vec![ListingTableUrl::parse(
        compacted_path,
    )?])
    .with_schema(schema2.as_ref().clone().into())
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
    let _ = fs::remove_file(compacted_path);

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a Tokio runtime for running our async function
    let rt = tokio::runtime::Runtime::new()?;

    // Run the function in the runtime
    rt.block_on(async { test_datafusion_schema_evolution_with_compaction().await })?;

    println!("Example completed successfully!");
    Ok(())
}
