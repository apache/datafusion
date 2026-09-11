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

#![cfg(all(feature = "parquet", feature = "sql", feature = "opendal"))]
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::datasource::file_format::options::{ArrowReadOptions, JsonReadOptions};
use datafusion::{
    arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    prelude::*,
};
use datafusion_common::{Result, assert_batches_eq};
use datafusion_datasource::PartitionedFile;
use datafusion_datasource_parquet::{
    DefaultParquetFileReaderFactory, ParquetFileReaderFactory,
    storage::StorageParquetTable,
};
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_storage::{
    FileAccessContext, FileInfo, FileReader, ReadRange, Storage, StorageUrl,
    WriterOptions,
};
use futures::{StreamExt, stream::BoxStream};
use parquet::{
    arrow::{ArrowWriter, async_reader::AsyncFileReader},
    file::properties::WriterProperties,
};
use std::{
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::AsyncWriteExt;

fn parquet_bytes(start: i32) -> Bytes {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from_iter_values(start..start + 12)),
            Arc::new(Int32Array::from_iter_values(
                (start..start + 12).map(|i| i * 10),
            )),
        ],
    )
    .unwrap();
    let mut bytes = Vec::new();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(bytes)
}

#[tokio::test]
async fn opendal_discovery_sql_and_atomic_replacement() -> Result<()> {
    let ctx = SessionContext::new();
    let url = url::Url::parse("memory://warehouse/events/").unwrap();
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    ctx.register_storage(
        &url,
        Arc::new(datafusion_storage_opendal::OpendalStorage::new(operator)),
    )
    .unwrap();
    let bound = ctx.runtime_env().storage_registry.get(&url).unwrap();
    // The same registration supports output creation and subsequent discovery.
    let data = parquet_bytes(0);
    let file = FileInfo::new(
        datafusion_storage::path::Path::from("events/data.parquet"),
        data.len() as u64,
    );
    let mut writer = bound
        .writer(
            &file.location,
            WriterOptions::default(),
            FileAccessContext::new("write"),
        )
        .await
        .unwrap();
    writer.write_all(&data).await.unwrap();
    writer.shutdown().await.unwrap();
    let old = ctx
        .read_parquet(url.as_str(), ParquetReadOptions::default())
        .await?;
    ctx.register_table("old", old.clone().into_view())?;
    let plan = ctx
        .sql("SELECT value FROM old WHERE id >= 9 ORDER BY value")
        .await?
        .create_physical_plan()
        .await?;
    let plan_text = datafusion_physical_plan::displayable(plan.as_ref())
        .indent(true)
        .to_string();
    assert!(
        plan_text.contains("file_type=parquet")
            && plan_text.contains("pruning_predicate="),
        "{plan_text}"
    );
    let result = ctx
        .sql("SELECT value FROM old WHERE id >= 9 ORDER BY value")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        [
            "+-------+",
            "| value |",
            "+-------+",
            "| 90    |",
            "| 100   |",
            "| 110   |",
            "+-------+"
        ],
        &result
    );

    let replacement =
        opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    replacement
        .write("events/data.parquet", parquet_bytes(100))
        .await
        .unwrap();
    ctx.register_storage(
        &url,
        Arc::new(datafusion_storage_opendal::OpendalStorage::new(replacement)),
    )
    .unwrap();
    let new = ctx
        .read_parquet(url.as_str(), ParquetReadOptions::default())
        .await?;
    ctx.deregister_storage(&url).unwrap();
    assert!(
        ctx.read_parquet(url.as_str(), ParquetReadOptions::default())
            .await
            .is_err()
    );
    // Both tables retain their original complete bindings after deregistration.
    assert_eq!(
        old.collect().await?[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        0
    );
    assert_eq!(
        new.collect().await?[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        100
    );
    Ok(())
}

#[derive(Debug)]
struct ReadOnly {
    bytes: Bytes,
    contexts: Arc<Mutex<Vec<FileAccessContext>>>,
    ranges: Arc<AtomicUsize>,
}
#[async_trait]
impl Storage for ReadOnly {
    async fn open(
        &self,
        _: &datafusion_storage::path::Path,
        context: FileAccessContext,
    ) -> datafusion_storage::Result<Arc<dyn FileReader>> {
        self.contexts.lock().unwrap().push(context);
        Ok(Arc::new(ReadOnly {
            bytes: self.bytes.clone(),
            contexts: Arc::clone(&self.contexts),
            ranges: Arc::clone(&self.ranges),
        }))
    }
}
#[async_trait]
impl FileReader for ReadOnly {
    async fn read_range(&self, range: ReadRange) -> datafusion_storage::Result<Bytes> {
        let range = match range {
            ReadRange::Bounded(r) => r,
            ReadRange::Suffix(n) => {
                let size = self.bytes.len() as u64;
                size.saturating_sub(n)..size
            }
        };
        self.ranges.fetch_add(1, Ordering::Relaxed);
        Ok(self.bytes.slice(range.start as usize..range.end as usize))
    }

    async fn read_ranges(
        &self,
        ranges: Vec<Range<u64>>,
    ) -> datafusion_storage::Result<Vec<Bytes>> {
        self.ranges.fetch_add(ranges.len(), Ordering::Relaxed);
        Ok(ranges
            .into_iter()
            .map(|r| self.bytes.slice(r.start as usize..r.end as usize))
            .collect())
    }
    fn stream(
        self: Arc<Self>,
        _: Option<ReadRange>,
    ) -> BoxStream<'static, datafusion_storage::Result<Bytes>> {
        // Parquet must use the range API, without requiring a stream implementation.
        futures::stream::once(async {
            Err(datafusion_storage::Error::NotSupported("stream".into()))
        })
        .boxed()
    }
}
#[tokio::test]
async fn read_only_manifest_reuses_native_scan_with_fresh_execution_contexts()
-> Result<()> {
    let ctx = SessionContext::new();
    let url = StorageUrl::parse("custom://manifest").unwrap();
    let bytes = parquet_bytes(0);
    let file = FileInfo::new(
        datafusion_storage::path::Path::from("part.parquet"),
        bytes.len() as u64,
    );
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let ranges = Arc::new(AtomicUsize::new(0));
    let storage: Arc<dyn Storage> = Arc::new(ReadOnly {
        bytes,
        contexts: Arc::clone(&contexts),
        ranges: Arc::clone(&ranges),
    });
    ctx.register_storage(url.as_ref(), storage).unwrap();
    let binding = ctx
        .runtime_env()
        .storage_registry
        .get(url.as_ref())
        .unwrap();
    let table = StorageParquetTable::try_new(binding, vec![file], None).await?;
    ctx.register_table("manifest", Arc::new(table))?;
    let query = ctx
        .sql("SELECT value FROM manifest WHERE id > 8 ORDER BY value")
        .await?;
    let first = query.clone().collect().await?;
    // Cancelling a completed execution must not poison a later execution of its table.
    for context in contexts.lock().unwrap().iter() {
        context.cancellation.cancel();
    }
    ctx.deregister_storage(url.as_ref()).unwrap();
    let second = query.collect().await?;
    assert_eq!(first, second);
    assert!(contexts.lock().unwrap().len() >= 3); // inference and two executions
    assert!(ranges.load(Ordering::Relaxed) > 3);
    assert_batches_eq!(
        [
            "+-------+",
            "| value |",
            "+-------+",
            "| 90    |",
            "| 100   |",
            "| 110   |",
            "+-------+"
        ],
        &second
    );
    Ok(())
}

#[derive(Debug)]
struct CustomFactory {
    inner: DefaultParquetFileReaderFactory,
    calls: Arc<AtomicUsize>,
}
impl ParquetFileReaderFactory for CustomFactory {
    fn create_reader(
        &self,
        partition: usize,
        file: PartitionedFile,
        hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.create_reader(partition, file, hint, metrics)
    }
}
#[tokio::test]
async fn custom_parquet_factory_is_preserved_without_object_store_registration()
-> Result<()> {
    let ctx = SessionContext::new();
    let url = StorageUrl::parse("custom://factory").unwrap();
    let bytes = parquet_bytes(0);
    let file = FileInfo::new(
        datafusion_storage::path::Path::from("part.parquet"),
        bytes.len() as u64,
    );
    let storage: Arc<dyn Storage> = Arc::new(ReadOnly {
        bytes,
        contexts: Arc::new(Mutex::new(Vec::new())),
        ranges: Arc::new(AtomicUsize::new(0)),
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(CustomFactory {
        inner: DefaultParquetFileReaderFactory::new(Arc::new(
            datafusion_storage::StorageBinding::new(url.clone(), Arc::clone(&storage)),
        )),
        calls: Arc::clone(&calls),
    });
    let table = StorageParquetTable::try_new(
        Arc::new(datafusion_storage::StorageBinding::new(
            url.clone(),
            storage,
        )),
        vec![file.clone()],
        None,
    )
    .await?
    .with_parquet_file_reader_factory(factory.clone());
    assert_eq!(
        ctx.read_table(Arc::new(table))?
            .collect()
            .await?
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        12
    );
    {
        // The legacy format planning hook must preserve an explicitly supplied factory too.
        use datafusion_datasource::{
            file::FileSource, file_format::FileFormat,
            file_scan_config::FileScanConfigBuilder,
        };
        use datafusion_datasource_parquet::{ParquetFormat, source::ParquetSource};
        let source: Arc<dyn FileSource> = Arc::new(
            ParquetSource::new(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("value", DataType::Int32, false),
            ])))
            .with_parquet_file_reader_factory(factory),
        );
        let config = FileScanConfigBuilder::new(StorageUrl::parse(url.as_str())?, source)
            .with_file(PartitionedFile::new(file.location.to_string(), file.size))
            .build();
        let plan = ParquetFormat::default()
            .create_physical_plan(&ctx.state(), config)
            .await?;
        assert_eq!(
            datafusion_physical_plan::collect(plan, ctx.task_ctx())
                .await?
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            12
        );
    }
    assert!(calls.load(Ordering::Relaxed) > 0);
    Ok(())
}

#[tokio::test]
async fn dropping_a_scan_cancels_its_storage_context() -> Result<()> {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
    let url = StorageUrl::parse("custom://cancel").unwrap();
    let bytes = parquet_bytes(0);
    let file = FileInfo::new(
        datafusion_storage::path::Path::from("part.parquet"),
        bytes.len() as u64,
    );
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let storage: Arc<dyn Storage> = Arc::new(ReadOnly {
        bytes,
        contexts: Arc::clone(&contexts),
        ranges: Arc::new(AtomicUsize::new(0)),
    });
    let table = StorageParquetTable::try_new(
        Arc::new(datafusion_storage::StorageBinding::new(url, storage)),
        vec![file],
        None,
    )
    .await?;
    let mut stream = ctx.read_table(Arc::new(table))?.execute_stream().await?;
    assert_eq!(stream.next().await.unwrap()?.num_rows(), 2);
    drop(stream);
    assert!(
        contexts
            .lock()
            .unwrap()
            .iter()
            .all(|context| context.cancellation.is_cancelled())
    );
    Ok(())
}

#[tokio::test]
async fn one_opendal_registration_supports_builtin_formats_and_copy() -> Result<()> {
    let ctx = SessionContext::new();
    let url = url::Url::parse("memory://formats/").unwrap();
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    ctx.register_storage(
        &url,
        Arc::new(datafusion_storage_opendal::OpendalStorage::new(operator)),
    )?;
    for format in ["CSV", "JSON", "ARROW", "PARQUET"] {
        let path = format!("memory://formats/{}/", format.to_lowercase());
        let options = if format == "CSV" {
            "OPTIONS ('format.has_header' 'true')"
        } else {
            ""
        };
        ctx.sql(&format!(
            "COPY (SELECT * FROM (VALUES (1), (2), (3)) AS t(id)) TO '{path}' STORED AS {format} {options}"
        )).await?.collect().await?;
        let frame = match format {
            "CSV" => {
                ctx.read_csv(&path, CsvReadOptions::new().has_header(true))
                    .await?
            }
            "JSON" => ctx.read_json(&path, JsonReadOptions::default()).await?,
            "ARROW" => ctx.read_arrow(&path, ArrowReadOptions::default()).await?,
            "PARQUET" => {
                ctx.read_parquet(&path, ParquetReadOptions::default())
                    .await?
            }
            _ => unreachable!(),
        };
        ctx.register_table(format.to_lowercase(), frame.into_view())?;
        let batches = ctx
            .sql(&format!(
                "SELECT SUM(id) AS total FROM {}",
                format.to_lowercase()
            ))
            .await?
            .collect()
            .await?;
        assert_batches_eq!(
            [
                "+-------+",
                "| total |",
                "+-------+",
                "| 6     |",
                "+-------+"
            ],
            &batches
        );
    }
    Ok(())
}

#[cfg(feature = "avro")]
#[tokio::test]
async fn opendal_reads_avro_through_the_normal_api() -> Result<()> {
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    let data = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../testing/data/avro/alltypes_plain.avro"
    ))?;
    operator.write("data.avro", data).await.unwrap();
    let ctx = SessionContext::new();
    ctx.register_storage(
        &url::Url::parse("memory://avro").unwrap(),
        Arc::new(datafusion_storage_opendal::OpendalStorage::new(operator)),
    )?;
    assert_eq!(
        ctx.read_avro("memory://avro/data.avro", AvroReadOptions::default())
            .await?
            .count()
            .await?,
        8
    );
    Ok(())
}

#[tokio::test]
async fn opendal_listing_table_insert_uses_the_registered_writer() -> Result<()> {
    let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
    let ctx = SessionContext::new();
    ctx.register_storage(
        &url::Url::parse("memory://insert").unwrap(),
        Arc::new(datafusion_storage_opendal::OpendalStorage::new(operator)),
    )?;
    ctx.sql("CREATE EXTERNAL TABLE events (id INT) STORED AS PARQUET LOCATION 'memory://insert/events/'").await?.collect().await?;
    ctx.sql("INSERT INTO events VALUES (1), (2), (3)")
        .await?
        .collect()
        .await?;
    assert_eq!(ctx.table("events").await?.count().await?, 3);
    Ok(())
}
