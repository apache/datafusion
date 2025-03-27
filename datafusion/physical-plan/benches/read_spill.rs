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

use arrow::array::{
    Date32Builder, Decimal128Builder, Int32Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion_common::{exec_datafusion_err, Result};
use std::fs::File;
use std::path::Path;
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use tempfile::NamedTempFile;

fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));

    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();

    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }

    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();

    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    )
    .unwrap()
}

/// Return a temporary file that contains an IPC file with 100 [`RecordBatch`]es
fn ipc_file() -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    let batch = create_batch(8192, true);
    let mut writer =
        StreamWriter::try_new(file.as_file_mut(), batch.schema().as_ref()).unwrap();
    for _ in 0..100 {
        writer.write(&batch).unwrap();
    }
    writer.finish().unwrap();
    file
}

fn bench_spill_read(c: &mut Criterion) {
    let file = ipc_file();
    let path = file.path();

    let mut group = c.benchmark_group("read_spill");

    let benches = &[
        (
            "StreamReader/read_100/with_validation",
            read_spill as fn(_, _) -> _,
        ),
        (
            "StreamReader/read_100/skip_validation",
            read_spill_skip_validation,
        ),
    ];

    for &(name, func) in benches {
        group.bench_with_input(BenchmarkId::new(name, ""), &path, |b, path| {
            b.iter_batched(
                // Setup phase: Create fresh state for each benchmark iteration.
                // - A channel to send/receive RecordBatch results between threads.
                // - A background thread to consume received RecordBatches.
                // This ensures each iteration starts with clean resources.
                || {
                    let (sender, receiver) = channel::<Result<RecordBatch>>();
                    let join_handle = thread::spawn(move || {
                        while let Ok(batch) = receiver.recv() {
                            let _ = batch.unwrap();
                        }
                    });
                    (sender, join_handle)
                },
                // Benchmark phase:
                // - Execute the target function to send batches via the channel
                // - Wait for the consumer thread to finish processing
                |(sender, join_handle)| {
                    func(sender, path).unwrap();
                    join_handle.join().unwrap();
                },
                BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}
criterion_group!(benches, bench_spill_read);
criterion_main!(benches);

fn read_spill(sender: Sender<Result<RecordBatch>>, path: &Path) -> Result<()> {
    let file = File::open(path)?;
    let reader = StreamReader::try_new(file, None)?;
    for batch in reader {
        sender
            .send(batch.map_err(Into::into))
            .map_err(|e| exec_datafusion_err!("{e}"))?;
    }
    Ok(())
}

fn read_spill_skip_validation(
    sender: Sender<Result<RecordBatch>>,
    path: &Path,
) -> Result<()> {
    let file = File::open(path)?;
    let reader = unsafe { StreamReader::try_new(file, None)?.with_skip_validation(true) };
    for batch in reader {
        sender
            .send(batch.map_err(Into::into))
            .map_err(|e| exec_datafusion_err!("{e}"))?;
    }
    Ok(())
}
