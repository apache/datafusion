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
use std::error::Error;
use std::fs;
use std::sync::Arc;

async fn test_datafusion_schema_evolution_with_compaction() -> Result<(), Box<dyn Error>>
{
    let ctx = SessionContext::new();

    let schema1 = create_schema1();
    let schema2 = create_schema2();

    let batch1 = create_batch1(&schema1)?;

    // Instead of manually mapping batch1, write it directly
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

    let batch2 = create_batch2(&schema2)?;

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

    let paths_str = vec![path1.to_string(), path2.to_string()];

    // Create schema adapter factory
    let adapter_factory = Arc::new(NestedStructSchemaAdapterFactory::default());

    let config = ListingTableConfig::new_with_multi_paths(
        paths_str
            .into_iter()
            .map(|p| ListingTableUrl::parse(&p))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .with_schema(schema2.as_ref().clone().into())
    .with_schema_adapter_factory(adapter_factory);

    let config = config.infer(&ctx.state()).await?;

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

    let df = ctx
        .sql("SELECT * FROM events ORDER BY timestamp_utc")
        .await?;

    let results = df.clone().collect().await?;

    assert_eq!(results[0].num_rows(), 2);

    let compacted_path = "test_data_compacted.parquet";
    let _ = fs::remove_file(compacted_path);

    df.write_parquet(
        compacted_path,
        DataFrameWriteOptions::default()
            .with_single_file_output(true)
            .with_sort_by(vec![col("timestamp_utc").sort(true, true)]),
        None,
    )
    .await?;

    let new_ctx = SessionContext::new();
    // Create schema adapter factory for the new context too
    let adapter_factory = Arc::new(NestedStructSchemaAdapterFactory::default());
    let config = ListingTableConfig::new_with_multi_paths(vec![ListingTableUrl::parse(
        compacted_path,
    )?])
    .with_schema(schema2.as_ref().clone().into())
    .with_schema_adapter_factory(adapter_factory)
    .infer(&new_ctx.state())
    .await?;

    let listing_table = ListingTable::try_new(config)?;
    new_ctx.register_table("events", Arc::new(listing_table))?;

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

fn create_schema2() -> Arc<Schema> {
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
    schema2
}

fn create_batch1(schema1: &Arc<Schema>) -> Result<RecordBatch, Box<dyn Error>> {
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
    Ok(batch1)
}

fn create_schema1() -> Arc<Schema> {
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
    schema1
}

fn create_batch2(schema2: &Arc<Schema>) -> Result<RecordBatch, Box<dyn Error>> {
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
    Ok(batch2)
}

fn main() -> Result<(), Box<dyn Error>> {
    // Create a Tokio runtime for running our async function
    let rt = tokio::runtime::Runtime::new()?;

    // Run the function in the runtime
    rt.block_on(async { test_datafusion_schema_evolution_with_compaction().await })?;

    println!("Example completed successfully!");
    Ok(())
}
