// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fs::{File, create_dir_all};
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::{PhysicalFileStatistics, Result};
use datafusion_datasource::file_scan_config::FileScanConfig;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::{ARROW_SCHEMA_META_KEY, encode_arrow_schema};
use parquet::basic::Encoding;
use parquet::data_type::{ByteArray, ByteArrayType};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use tempfile::tempdir;

const VALUE: &str = "dictionary-value-dictionary-value";
const OTHER_VALUE: &str = "second-dictionary-value-second-dictionary-value";
const ROWS_PER_ROW_GROUP: usize = 150_000;
const FILE_COUNT: usize = 2;
const QUERY: &str =
    "SELECT key, COUNT(*) AS count FROM t GROUP BY key ORDER BY key NULLS FIRST";
// Keep this independent from the optimizer implementation so a policy change
// must update both the fixture and its asserted eligibility evidence.
const MIN_VALUE_BYTES_PER_PARQUET_BYTE_FOR_TEST: usize = 4;
const MIN_AGGREGATE_VALUE_BYTES_FOR_TEST: usize = 8 * 1024 * 1024;

fn write_dictionary_file(path: &Path) -> Result<()> {
    let schema = Arc::new(parse_message_type(
        "message schema { OPTIONAL BYTE_ARRAY key; }",
    )?);
    let arrow_schema = Schema::new(vec![Field::new("key", DataType::Utf8, true)]);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_key_value_metadata(Some(vec![KeyValue::new(
                ARROW_SCHEMA_META_KEY.to_string(),
                encode_arrow_schema(&arrow_schema),
            )]))
            .build(),
    );
    let mut writer = SerializedFileWriter::new(File::create(path)?, schema, properties)?;
    for value in [VALUE, OTHER_VALUE] {
        let mut row_group = writer.next_row_group()?;
        let mut column = row_group
            .next_column()?
            .expect("test schema contains one column");
        let value = ByteArray::from(value);
        let values = vec![value.clone(); ROWS_PER_ROW_GROUP - 1];
        let mut definition_levels = vec![1; ROWS_PER_ROW_GROUP];
        definition_levels[0] = 0;
        column
            .typed::<ByteArrayType>()
            .write_batch_with_statistics(
                &values,
                Some(&definition_levels),
                None,
                Some(&value),
                Some(&value),
                Some(1),
            )?;
        column.close()?;
        row_group.close()?;
    }
    writer.close()?;
    Ok(())
}

fn scan_details(plan: &Arc<dyn ExecutionPlan>) -> Option<(DataType, usize)> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
    {
        return Some((
            config.file_schema().field(0).data_type().clone(),
            config.file_groups.len(),
        ));
    }
    plan.children().into_iter().find_map(scan_details)
}

fn scan_has_physical_statistics(plan: &Arc<dyn ExecutionPlan>) -> Option<bool> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
        && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
    {
        return Some(
            config
                .file_groups
                .iter()
                .flat_map(|group| group.iter())
                .any(|file| file.extension::<PhysicalFileStatistics>().is_some()),
        );
    }
    plan.children()
        .into_iter()
        .find_map(scan_has_physical_statistics)
}

async fn register_table(ctx: &SessionContext, path: &Path) -> Result<()> {
    ctx.register_parquet(
        "t",
        path.to_str().expect("temporary path should be UTF-8"),
        ParquetReadOptions::default()
            .table_partition_cols(vec![("bucket".to_string(), DataType::Int64)]),
    )
    .await?;
    Ok(())
}

#[tokio::test]
async fn preserves_native_dictionary_for_profitable_group_by() -> Result<()> {
    let directory = tempdir()?;
    let table_path = directory.path().join("table");
    let partition_path = table_path.join("bucket=1");
    create_dir_all(&partition_path)?;
    let paths = (0..FILE_COUNT)
        .map(|index| partition_path.join(format!("part-{index}.parquet")))
        .collect::<Vec<_>>();
    for path in &paths {
        write_dictionary_file(path)?;
    }

    // Verify that the generated file carries every independent fact used by
    // the optimizer. This keeps the test from passing due to a writer-default
    // change that silently makes the optimization ineligible.
    for path in &paths {
        let reader = SerializedFileReader::new(File::open(path)?)?;
        assert!(
            reader
                .metadata()
                .file_metadata()
                .schema_descr()
                .column(0)
                .logical_type_ref()
                .is_none()
        );
        assert_eq!(reader.metadata().num_row_groups(), 2);
        for (row_group, value) in [VALUE, OTHER_VALUE].into_iter().enumerate() {
            let column = reader.metadata().row_group(row_group).column(0);
            assert!(column.dictionary_page_offset().is_some());
            assert!(column.page_encoding_stats_mask().is_some_and(|mask| {
                mask.is_only(Encoding::PLAIN_DICTIONARY)
                    || mask.is_only(Encoding::RLE_DICTIONARY)
            }));
            let decoded_value_bytes = value.len() * (ROWS_PER_ROW_GROUP - 1);
            assert_eq!(
                column.unencoded_byte_array_data_bytes(),
                Some(decoded_value_bytes as i64)
            );
            let parquet_uncompressed_bytes = usize::try_from(column.uncompressed_size())
                .expect("column size should fit usize");
            assert!(
                decoded_value_bytes
                    >= parquet_uncompressed_bytes
                        * MIN_VALUE_BYTES_PER_PARQUET_BYTE_FOR_TEST,
                "test row group must satisfy the optimizer's decoded-to-Parquet byte ratio"
            );
        }
    }

    // The physical column is an unannotated BYTE_ARRAY whose embedded Arrow
    // schema declares Utf8. The requested dictionary value type must be
    // honored inside arrow-rs rather than repaired by a cast in the scan.
    let dictionary_schema = Arc::new(Schema::new(vec![Field::new(
        "key",
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        true,
    )]));
    let mut reader = ParquetRecordBatchReaderBuilder::try_new_with_options(
        File::open(&paths[0])?,
        ArrowReaderOptions::new().with_schema(Arc::clone(&dictionary_schema)),
    )?
    .with_batch_size(1024)
    .build()?;
    let batch = reader.next().expect("test file contains rows")?;
    assert_eq!(batch.schema(), dictionary_schema);
    assert!(
        FILE_COUNT * (VALUE.len() + OTHER_VALUE.len()) * (ROWS_PER_ROW_GROUP - 1)
            >= MIN_AGGREGATE_VALUE_BYTES_FOR_TEST,
        "test input must satisfy the aggregate workload threshold"
    );

    let mut baseline_config = SessionConfig::new();
    baseline_config.options_mut().execution.target_partitions = FILE_COUNT;
    let runtime = Arc::new(RuntimeEnv::default());
    let baseline_ctx =
        SessionContext::new_with_config_rt(baseline_config, Arc::clone(&runtime));
    register_table(&baseline_ctx, &table_path).await?;
    let baseline_plan = baseline_ctx
        .sql(QUERY)
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        scan_details(&baseline_plan),
        Some((DataType::Utf8View, FILE_COUNT))
    );
    let baseline_batches = collect(baseline_plan, baseline_ctx.task_ctx()).await?;
    let statistics_cache = runtime
        .cache_manager
        .get_file_statistic_cache()
        .expect("file statistics cache should be enabled");
    let cached = statistics_cache.list_entries();
    assert_eq!(cached.len(), FILE_COUNT);
    assert!(
        cached
            .values()
            .all(|entry| entry.value.physical_statistics.is_none())
    );

    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_dictionary_preservation = true;
    config.options_mut().execution.target_partitions = FILE_COUNT;
    // Reuse the runtime so this request sees the file-statistics cache entry
    // populated above without physical encoding statistics. Enabling the rule
    // must backfill those statistics instead of treating the entry as complete.
    let ctx = SessionContext::new_with_config_rt(config, Arc::clone(&runtime));
    register_table(&ctx, &table_path).await?;

    // Enabling the optimizer must not backfill physical metadata for scans
    // that project no Utf8View columns and therefore cannot be rewritten.
    // This keeps unrelated queries from paying the dictionary cost model's
    // footer-processing cost.
    let unrelated_plan = ctx
        .sql("SELECT SUM(bucket) FROM t")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(scan_has_physical_statistics(&unrelated_plan), Some(false));
    collect(unrelated_plan, ctx.task_ctx()).await?;
    let cached = statistics_cache.list_entries();
    assert_eq!(cached.len(), FILE_COUNT);
    assert!(
        cached
            .values()
            .all(|entry| entry.value.physical_statistics.is_none())
    );

    // Unsupported data filters and scan limits make the dictionary rewrite
    // ineligible, so they must not backfill or attach its optional metadata.
    for query in [
        "SELECT key, COUNT(*) FROM t WHERE key IS NOT NULL GROUP BY key",
        "SELECT key FROM t LIMIT 1",
    ] {
        let ineligible_plan = ctx.sql(query).await?.create_physical_plan().await?;
        assert_eq!(scan_has_physical_statistics(&ineligible_plan), Some(false));
        assert!(matches!(
            scan_details(&ineligible_plan),
            Some((DataType::Utf8View, _))
        ));
    }
    let cached = statistics_cache.list_entries();
    assert!(
        cached
            .values()
            .all(|entry| entry.value.physical_statistics.is_none())
    );

    // An equality-filtered column can be profitable when filtering is its last
    // consumer, so the listing scan must request physical encoding metadata.
    // This fixture is deliberately below the filter workload threshold: it
    // verifies metadata reachability without changing the scan representation.
    let equality_filter_plan = ctx
        .sql(&format!("SELECT COUNT(*) FROM t WHERE key = '{VALUE}'"))
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        scan_has_physical_statistics(&equality_filter_plan),
        Some(true)
    );
    assert!(matches!(
        scan_details(&equality_filter_plan),
        Some((DataType::Utf8View, _))
    ));

    let dataframe = ctx.sql(QUERY).await?;
    let plan = dataframe.create_physical_plan().await?;
    let cached = statistics_cache.list_entries();
    assert_eq!(cached.len(), FILE_COUNT);
    assert!(
        cached
            .values()
            .all(|entry| entry.value.physical_statistics.is_some())
    );

    // The dictionary is an internal physical choice. SQL-visible output stays
    // Utf8View, while the actual Parquet scan preserves the file-native Utf8
    // dictionary values.
    assert_eq!(plan.schema().field(0).data_type(), &DataType::Utf8View);
    assert_eq!(
        scan_details(&plan),
        Some((
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8),),
            FILE_COUNT,
        ))
    );

    let batches = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(batches, baseline_batches);

    // Rich cache entries are not attached to later scans that did not request
    // physical statistics.
    let unrelated_plan = ctx
        .sql("SELECT SUM(bucket) FROM t")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(scan_has_physical_statistics(&unrelated_plan), Some(false));
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        3
    );
    let key = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringViewArray>()
        .expect("output should retain Utf8View");
    let count = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT output should be Int64");
    assert!(key.is_null(0));
    assert_eq!(count.value(0), (2 * FILE_COUNT) as i64);
    assert_eq!(key.value(1), VALUE);
    assert_eq!(
        count.value(1),
        (FILE_COUNT * (ROWS_PER_ROW_GROUP - 1)) as i64
    );
    assert_eq!(key.value(2), OTHER_VALUE);
    assert_eq!(
        count.value(2),
        (FILE_COUNT * (ROWS_PER_ROW_GROUP - 1)) as i64
    );

    // A second plan is built from cached file metadata. Physical encoding
    // facts must survive that path just like logical column statistics do.
    let cached_plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
    assert!(matches!(
        scan_details(&cached_plan),
        Some((DataType::Dictionary(_, _), FILE_COUNT))
    ));

    // Partition-only filters are removed before the metadata gate and remain
    // eligible for the dictionary rewrite.
    let partition_filtered_plan = ctx
        .sql("SELECT key, COUNT(*) FROM t WHERE bucket = 1 GROUP BY key")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        scan_has_physical_statistics(&partition_filtered_plan),
        Some(true)
    );
    assert!(matches!(
        scan_details(&partition_filtered_plan),
        Some((DataType::Dictionary(_, _), FILE_COUNT))
    ));
    Ok(())
}
