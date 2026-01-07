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

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{FileRange, PartitionedFile, TableSchema};
use datafusion_datasource_json::source::JsonSource;
use datafusion_execution::TaskContext;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::runtime::{Builder, Runtime};

// Add CPU cost per requested KB to make read amplification visible in timings.
const CPU_COST_PER_KB_ROUNDS: u32 = 64;
const BYTES_PER_KB: u64 = 1024;

#[derive(Debug)]
struct CountingObjectStore {
    inner: Arc<dyn ObjectStore>,
    requested_bytes: AtomicU64,
    cpu_cost_per_kb_rounds: u32,
}

impl CountingObjectStore {
    fn new_with_cpu_cost(
        inner: Arc<dyn ObjectStore>,
        cpu_cost_per_kb_rounds: u32,
    ) -> Self {
        Self {
            inner,
            requested_bytes: AtomicU64::new(0),
            cpu_cost_per_kb_rounds,
        }
    }

    fn reset(&self) {
        self.requested_bytes.store(0, Ordering::Relaxed);
    }

    fn requested_bytes(&self) -> u64 {
        self.requested_bytes.load(Ordering::Relaxed)
    }
}

impl fmt::Display for CountingObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let should_burn_cpu = self.cpu_cost_per_kb_rounds > 0;
        let mut requested_len = 0u64;
        if let Some(range) = options.range.as_ref() {
            let requested = match range {
                GetRange::Bounded(r) => r.end.saturating_sub(r.start),
                GetRange::Offset(_) | GetRange::Suffix(_) => 0,
            };
            requested_len = requested;
            self.requested_bytes.fetch_add(requested, Ordering::Relaxed);
        }
        let result = self.inner.get_opts(location, options).await;
        if should_burn_cpu {
            burn_cpu_kb(requested_len, self.cpu_cost_per_kb_rounds);
        }
        result
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn build_fixed_json_lines(line_len: usize, lines: usize) -> Bytes {
    let prefix = r#"{"value":""#;
    let suffix = "\"}\n";
    let min_len = prefix.len() + suffix.len() + 1;
    assert!(line_len >= min_len, "line_len must be at least {min_len}");

    let padding_len = line_len - prefix.len() - suffix.len();
    let mut line = Vec::with_capacity(line_len);
    line.extend_from_slice(prefix.as_bytes());
    line.extend(std::iter::repeat(b'a').take(padding_len));
    line.extend_from_slice(suffix.as_bytes());

    let mut data = Vec::with_capacity(line_len * lines);
    for _ in 0..lines {
        data.extend_from_slice(&line);
    }
    Bytes::from(data)
}

fn burn_cpu_kb(bytes: u64, rounds: u32) {
    if bytes == 0 || rounds == 0 {
        return;
    }
    let kb = (bytes + BYTES_PER_KB - 1) / BYTES_PER_KB;
    let mut checksum = 0u64;
    let mut remaining = kb.saturating_mul(rounds as u64);
    while remaining > 0 {
        checksum = checksum.wrapping_add(remaining);
        checksum = checksum.rotate_left(5) ^ 0x9e3779b97f4a7c15;
        remaining -= 1;
    }
    std::hint::black_box(checksum);
}

struct Fixture {
    store: Arc<CountingObjectStore>,
    task_ctx: Arc<TaskContext>,
    exec: Arc<dyn ExecutionPlan>,
}

fn build_fixture(rt: &Runtime) -> Fixture {
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(CountingObjectStore::new_with_cpu_cost(
        Arc::clone(&inner),
        CPU_COST_PER_KB_ROUNDS,
    ));
    let store_dyn: Arc<dyn ObjectStore> = store.clone();
    let path = Path::from("bench.json");

    let line_len = 128usize;
    let lines = 65_536usize;
    let data = build_fixed_json_lines(line_len, lines);
    rt.block_on(inner.put(&path, data.into())).unwrap();
    let object_meta = rt.block_on(inner.head(&path)).unwrap();

    let start = 1_000_003usize;
    let raw_end = start + 256_000;
    let end = (raw_end / line_len).max(1) * line_len;

    let task_ctx = Arc::new(TaskContext::default());
    let runtime_env = task_ctx.runtime_env();
    let object_store_url = ObjectStoreUrl::parse("test://bucket").unwrap();
    // Register a CPU-costed store to approximate non-streaming remote reads.
    runtime_env.register_object_store(object_store_url.as_ref(), Arc::clone(&store_dyn));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let table_schema = TableSchema::from_file_schema(schema);
    let file_source: Arc<dyn FileSource> = Arc::new(JsonSource::new(table_schema));
    let file = build_partitioned_file(object_meta.clone(), start, end);
    let config = FileScanConfigBuilder::new(object_store_url, file_source)
        .with_file_groups(vec![FileGroup::new(vec![file])])
        .build();
    let exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

    Fixture {
        store,
        task_ctx,
        exec,
    }
}

fn measure_datasource_exec_bytes(rt: &Runtime, fixture: &Fixture) -> u64 {
    fixture.store.reset();
    let rows = rt.block_on(run_datasource_exec(
        Arc::clone(&fixture.exec),
        Arc::clone(&fixture.task_ctx),
    ));
    debug_assert!(rows > 0);
    fixture.store.requested_bytes()
}

fn build_partitioned_file(
    object_meta: object_store::ObjectMeta,
    start: usize,
    end: usize,
) -> PartitionedFile {
    PartitionedFile {
        object_meta,
        partition_values: vec![],
        range: Some(FileRange {
            start: start as i64,
            end: end as i64,
        }),
        statistics: None,
        ordering: None,
        extensions: None,
        metadata_size_hint: None,
    }
}

async fn run_datasource_exec(
    exec: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> usize {
    let mut stream = exec.execute(0, task_ctx).unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        rows += batch.num_rows();
    }
    rows
}

fn bench_json_boundary(c: &mut Criterion) {
    let rt = Builder::new_current_thread().build().unwrap();
    let fixture = build_fixture(&rt);

    let exec_bytes = measure_datasource_exec_bytes(&rt, &fixture);

    let mut exec_group = c.benchmark_group("json_boundary_datasource_exec");
    exec_group.throughput(Throughput::Bytes(exec_bytes));
    // Fixed benchmark id for baseline comparisons; read_bytes is reported as throughput.
    exec_group.bench_function("execute", |b| {
        b.iter(|| {
            fixture.store.reset();
            rt.block_on(run_datasource_exec(
                Arc::clone(&fixture.exec),
                Arc::clone(&fixture.task_ctx),
            ));
        });
    });
    exec_group.finish();
}

criterion_group!(benches, bench_json_boundary);
criterion_main!(benches);
