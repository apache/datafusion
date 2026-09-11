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

use super::*;
use arrow::array::{
    ArrayRef, BooleanArray, Int64Array, Int64Builder, ListBuilder, StringArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field};
use bytes::Bytes;
use datafusion_execution::memory_pool::UnboundedMemoryPool;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use std::io::Write;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

struct TestOutput(SharedBuffer);

impl AsyncWrite for TestOutput {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Write::write(&mut self.0, bytes))
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn properties(rows: usize, bytes: Option<usize>) -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(rows))
        .set_max_row_group_bytes(bytes)
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .build()
}

fn context(
    schema: SchemaRef,
    props: WriterProperties,
    pool: Arc<dyn MemoryPool>,
    capacity: usize,
) -> ParquetFileWriteContext {
    ParquetFileWriteContext {
        schema,
        props: Arc::new(props),
        skip_arrow_metadata: false,
        parallel_options: Arc::new(ParallelParquetWriterOptions {
            max_parallel_row_groups: 2,
            max_buffered_record_batches_per_stream: capacity,
        }),
        pool,
    }
}

async fn parallel(
    batches: &[RecordBatch],
    props: WriterProperties,
    pool: Arc<dyn MemoryPool>,
    capacity: usize,
) -> Result<(ParquetMetaData, Vec<u8>)> {
    let ctx = context(batches[0].schema(), props, pool, capacity);
    let output = SharedBuffer::new(0);
    let (tx, rx) = mpsc::channel(2);
    let batches = batches.to_vec();
    let feeder = SpawnedTask::spawn(async move {
        for batch in batches {
            if tx.send(batch).await.is_err() {
                break;
            }
        }
    });
    let result = output_single_parquet_file_parallelized(
        Box::new(TestOutput(output.clone())),
        rx,
        ctx,
        Time::new(),
    )
    .await;
    feeder.join_unwind().await.unwrap();
    let bytes = output.buffer.lock().await.clone();
    Ok((result?, bytes))
}

fn serial(batches: &[RecordBatch], props: WriterProperties) -> ParquetMetaData {
    let mut writer =
        ArrowWriter::try_new(Vec::new(), batches[0].schema(), Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap()
}

fn row_counts(metadata: &ParquetMetaData) -> Vec<i64> {
    metadata
        .row_groups()
        .iter()
        .map(|group| group.num_rows())
        .collect()
}

fn integers(rows: usize, start: i64) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from_iter_values(start..start + rows as i64)) as ArrayRef,
    )])
    .unwrap()
}

fn strings(rows: usize, width: usize) -> RecordBatch {
    RecordBatch::try_from_iter([(
        "s",
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|i| format!("{i:0width$}")),
        )) as ArrayRef,
    )])
    .unwrap()
}

async fn check_round_trip(
    batches: &[RecordBatch],
    props: WriterProperties,
    capacity: usize,
) -> ParquetMetaData {
    let pool = Arc::new(UnboundedMemoryPool::default());
    let (metadata, bytes) = tokio::time::timeout(
        Duration::from_secs(10),
        parallel(batches, props.clone(), pool.clone(), capacity),
    )
    .await
    .expect("parallel writer stalled")
    .unwrap();
    assert_eq!(
        metadata.file_metadata().num_rows() as usize,
        batches.iter().map(RecordBatch::num_rows).sum::<usize>()
    );
    assert!(
        metadata
            .row_groups()
            .iter()
            .all(|g| g.num_rows() as usize <= props.max_row_group_row_count().unwrap())
    );
    let read = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_batch_size(137)
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    let schema = batches[0].schema();
    assert_eq!(
        concat_batches(&schema, batches).unwrap(),
        concat_batches(&schema, &read).unwrap()
    );
    assert_eq!(pool.reserved(), 0);
    metadata
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn row_and_byte_limits_round_trip() {
    let batches = [integers(900, 0), integers(1024, 900)];
    for capacity in [1, 2, 8] {
        for bytes in [None, Some(1), Some(7600), Some(1_000_000)] {
            let props = properties(1000, bytes);
            let actual = check_round_trip(&batches, props.clone(), capacity).await;
            assert_eq!(row_counts(&actual), row_counts(&serial(&batches, props)));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prediction_reconsiders_a_cheaper_suffix() {
    let batches = [strings(100, 1000), strings(1000, 8)];
    let props = properties(20_000, Some(131_072));
    let expected = serial(&batches, props.clone());
    assert_eq!(row_counts(&expected), vec![1100]);
    let actual = check_round_trip(&batches, props, 2).await;
    assert_eq!(row_counts(&actual), row_counts(&expected));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fractional_bytes_per_row_and_wide_booleans() {
    for columns in [1, 100] {
        let schema = Arc::new(Schema::new(
            (0..columns)
                .map(|i| Field::new(format!("b{i}"), DataType::Boolean, false))
                .collect::<Vec<_>>(),
        ));
        let values =
            Arc::new(BooleanArray::from_iter((0..1024).map(|i| i % 2 == 0))) as ArrayRef;
        let batch = RecordBatch::try_new(schema, vec![values; columns]).unwrap();
        let batches = vec![batch; 4];
        let props = properties(100_000, Some(if columns == 1 { 256 } else { 4096 }));
        let actual = check_round_trip(&batches, props.clone(), 2).await;
        assert_eq!(row_counts(&actual), row_counts(&serial(&batches, props)));
        if columns == 100 {
            assert_eq!(row_counts(&actual), vec![1024; 4]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_columns_use_root_row_counts() {
    let mut list = ListBuilder::new(Int64Builder::new());
    for row in 0..472 {
        for value in 0..row % 13 {
            list.values().append_value(value);
        }
        list.append(row % 7 != 0);
    }
    let batch = RecordBatch::try_from_iter([
        ("list", Arc::new(list.finish()) as ArrayRef),
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..472)) as ArrayRef,
        ),
    ])
    .unwrap();
    let batches = [
        batch.slice(0, 17),
        batch.slice(17, 103),
        batch.slice(120, 41),
        batch.slice(161, 311),
    ];
    let props = properties(200, Some(2048));
    let actual = check_round_trip(&batches, props.clone(), 1).await;
    assert_eq!(row_counts(&actual), row_counts(&serial(&batches, props)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_batches_and_exact_final_boundary() {
    let data = integers(64, 0);
    let empty = RecordBatch::new_empty(data.schema());
    for batches in [vec![empty.clone()], vec![empty.clone(), data, empty]] {
        for bytes in [None, Some(1), Some(1024)] {
            let props = properties(64, bytes);
            let actual = check_round_trip(&batches, props.clone(), 2).await;
            assert_eq!(row_counts(&actual), row_counts(&serial(&batches, props)));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dictionary_fallback_and_compression() {
    let batches = [strings(100, 1000), strings(1000, 8), strings(1024, 200)];
    for compression in [
        Compression::UNCOMPRESSED,
        Compression::SNAPPY,
        Compression::ZSTD(Default::default()),
    ] {
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2000))
            .set_max_row_group_bytes(Some(131_072))
            .set_dictionary_enabled(true)
            .set_dictionary_page_size_limit(1024)
            .set_data_page_size_limit(4096)
            .set_compression(compression)
            .build();
        let actual = check_round_trip(&batches, props.clone(), 2).await;
        assert_eq!(row_counts(&actual), row_counts(&serial(&batches, props)));
    }
}

#[derive(Debug)]
struct FailingPool {
    inner: UnboundedMemoryPool,
    calls: AtomicUsize,
    fail_after: usize,
}

impl fmt::Display for FailingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FailingPool")
    }
}

impl MemoryPool for FailingPool {
    fn name(&self) -> &str {
        "FailingPool"
    }
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }
    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }
    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if reservation.consumer().name() == "ParquetSink(ArrowColumnWriter)"
            && self.calls.fetch_add(1, Ordering::SeqCst) >= self.fail_after
        {
            return Err(DataFusionError::ResourcesExhausted(
                "injected column allocation failure".into(),
            ));
        }
        self.inner.try_grow(reservation, additional)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn column_failure_is_not_successful_eof() {
    for (fail_after, max_bytes) in [
        (0, None),
        (2, None),
        (0, Some(10_000_000)),
        (2, Some(10_000_000)),
    ] {
        let pool = Arc::new(FailingPool {
            inner: Default::default(),
            calls: AtomicUsize::new(0),
            fail_after,
        });
        let batches = vec![strings(128, 1024); 10];
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            parallel(&batches, properties(100_000, max_bytes), pool.clone(), 1),
        )
        .await
        .expect("error propagation stalled")
        .expect_err("column failure was discarded");
        assert!(
            error
                .to_string()
                .contains("injected column allocation failure"),
            "{error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_group_cancels_workers_and_releases_memory() {
    let batch = strings(128, 1024);
    let pool = Arc::new(UnboundedMemoryPool::default());
    let props = properties(100_000, Some(10_000_000));
    let writer =
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(props.clone())).unwrap();
    let (_, factory) = writer.into_serialized_writer().unwrap();
    let ctx = context(batch.schema(), props, pool.clone(), 2);
    let mut group = InProgressRowGroup::new(&factory, 0, &ctx, &Time::new()).unwrap();
    group.write(&batch, &ctx.schema).await.unwrap();
    group.synchronize().await.unwrap();
    assert!(pool.reserved() > 0);
    drop(group);
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.reserved() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled workers retained memory");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_limit_finishes_a_group_before_more_input_arrives() {
    let batch = integers(1024, 0);
    let props = properties(2000, Some(1));
    let writer =
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(props.clone())).unwrap();
    let (_, factory) = writer.into_serialized_writer().unwrap();
    let ctx = context(
        batch.schema(),
        props,
        Arc::new(UnboundedMemoryPool::default()),
        2,
    );
    let (data_tx, data_rx) = mpsc::channel(1);
    let (serialize_tx, mut serialize_rx) = mpsc::channel(1);
    let dispatcher = spawn_parquet_parallel_serialization_task(
        factory,
        data_rx,
        serialize_tx,
        ctx,
        Time::new(),
    );
    data_tx.send(batch).await.unwrap();
    // Keep the input open: the byte threshold must flush without a next batch
    // or EOF, even though the first batch exceeds the target by itself.
    let group = tokio::time::timeout(Duration::from_secs(5), serialize_rx.recv())
        .await
        .expect("byte boundary waited for more input")
        .unwrap();
    let (_, _, rows) = group.join_unwind().await.unwrap().unwrap();
    assert_eq!(rows, 1024);
    drop(data_tx);
    dispatcher.join_unwind().await.unwrap().unwrap();
    assert!(serialize_rx.recv().await.is_none());
}

#[cfg(feature = "parquet_encryption")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn byte_boundaries_preserve_encrypted_row_group_ordinals() {
    use parquet::arrow::arrow_reader::ArrowReaderOptions;
    use parquet::encryption::decrypt::FileDecryptionProperties;

    let key = b"0123456789012345".to_vec();
    let encryption = FileEncryptionProperties::builder(key.clone())
        .build()
        .unwrap();
    let decryption = FileDecryptionProperties::builder(key).build().unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3000))
        .set_max_row_group_bytes(Some(1))
        .with_file_encryption_properties(encryption)
        .build();
    let batches = [
        integers(1024, 0),
        integers(1024, 1024),
        integers(1024, 2048),
    ];
    let pool = Arc::new(UnboundedMemoryPool::default());
    let (metadata, bytes) = tokio::time::timeout(
        Duration::from_secs(10),
        parallel(&batches, props, pool.clone(), 2),
    )
    .await
    .expect("encrypted write stalled")
    .unwrap();
    assert_eq!(row_counts(&metadata), vec![1024; 3]);
    let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption);
    let decoded = ParquetRecordBatchReaderBuilder::try_new_with_options(
        Bytes::from(bytes),
        options,
    )
    .unwrap()
    .build()
    .unwrap()
    .collect::<std::result::Result<Vec<_>, _>>()
    .unwrap();
    assert_eq!(
        concat_batches(&batches[0].schema(), &batches).unwrap(),
        concat_batches(&batches[0].schema(), &decoded).unwrap(),
    );
    assert_eq!(pool.reserved(), 0);
}
