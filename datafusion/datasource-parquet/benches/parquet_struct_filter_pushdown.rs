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

//! Benchmark for struct column filter pushdown (late materialization).
//!
//! This benchmark measures the performance improvement from pushing down
//! predicates on struct fields during parquet scanning. The mechanism is:
//!
//! **With pushdown (late materialization):**
//! 1. Read the small struct column containing the filter field
//! 2. Evaluate the predicate (`struct_col['id'] = X`) to create a row selection mask
//! 3. Only read the pages of the large payload column that match the mask
//!
//! **Without pushdown:**
//! 1. Read both columns entirely (including all payload data)
//! 2. Apply the filter after all data is loaded
//!
//! The dataset contains:
//! - A struct column with a small int32 `id` field (used for filtering)
//! - A separate large binary payload column (~512KB per row)
//!
//! When the predicate matches only 10% of rows (1 row group out of 10),
//! late materialization should avoid reading 90% of the payload data.
//!
//! The benchmark uses `ParquetSource` and `DataSourceExec` to test the full
//! parquet scanning pipeline including the `ParquetOpener`.

use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, BinaryBuilder, Int32Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::config::TableParquetOptions;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{col, lit};
use datafusion_functions::expr_fn::get_field;
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::ExecutionPlan;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::Rng;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const ROW_GROUP_SIZE: usize = 1;
const TOTAL_ROW_GROUPS: usize = 3;
const TOTAL_ROWS: usize = ROW_GROUP_SIZE * TOTAL_ROW_GROUPS;
const TARGET_ID: i32 = ROW_GROUP_SIZE as i32 + 1; // Match a single row in the second row group
const STRUCT_COLUMN_NAME: &str = "struct_col";  
const INT_FIELD_NAME: &str = "id";
const PAYLOAD_COLUMN_NAME: &str = "payload";
// Large payload (~512KB) to emphasize decoding overhead when pushdown is disabled.
const PAYLOAD_BYTES: usize = 512 * 1024;

struct BenchmarkDataset {
    _tempdir: TempDir,
    file_path: PathBuf,
    schema: SchemaRef,
}

static DATASET: LazyLock<BenchmarkDataset> = LazyLock::new(|| {
    create_dataset().expect("failed to prepare parquet benchmark dataset")
});

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
});

fn parquet_struct_filter_pushdown(c: &mut Criterion) {
    // Force dataset creation before benchmarking
    let _ = &*DATASET;

    let mut group = c.benchmark_group("parquet_struct_filter_pushdown");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));

    // Setup for no_pushdown benchmark
    let (exec, ctx) = setup_scan(false);
    group.bench_function("no_pushdown", |b| {
        let exec = Arc::clone(&exec).reset_state().unwrap();
        b.iter(|| {
            let row_count =
                RUNTIME.block_on(execute_and_count(&exec, &ctx));
            assert_eq!(row_count, 1);
        });
    });

    // Setup for with_pushdown benchmark
    let (exec, ctx) = setup_scan(true);
    group.bench_function("with_pushdown", |b| {
        let exec = Arc::clone(&exec).reset_state().unwrap();
        b.iter(|| {
            let row_count =
                RUNTIME.block_on(execute_and_count(&exec, &ctx));
            assert_eq!(row_count, 1);
        });
    });

    group.finish();
}

/// Setup the execution plan and task context for benchmarking
fn setup_scan(pushdown: bool) -> (Arc<dyn ExecutionPlan>, Arc<TaskContext>) {
    let dataset = &*DATASET;
    let file_path = &dataset.file_path;
    let schema = Arc::clone(&dataset.schema);

    // Create predicate: struct_col['id'] = TARGET_ID
    let predicate_expr =
        get_field(col(STRUCT_COLUMN_NAME), INT_FIELD_NAME).eq(lit(TARGET_ID));
    let predicate = logical2physical(&predicate_expr, &schema);

    // Configure parquet options
    let mut parquet_options = TableParquetOptions::default();
    parquet_options.global.pushdown_filters = pushdown;
    parquet_options.global.reorder_filters = pushdown;

    // Create ParquetSource with predicate
    let table_schema = TableSchema::from_file_schema(Arc::clone(&schema));
    let source = Arc::new(
        ParquetSource::new(table_schema)
            .with_table_parquet_options(parquet_options)
            .with_predicate(Arc::clone(&predicate))
            .with_pushdown_filters(pushdown)
    );

    // Get file size
    let file_size = std::fs::metadata(file_path)
        .expect("failed to get file metadata")
        .len();

    // Create FileScanConfig
    let object_store_url = ObjectStoreUrl::local_filesystem();
    let config = FileScanConfigBuilder::new(object_store_url, source)
        .with_file(PartitionedFile::new(
            file_path.to_string_lossy().to_string(),
            file_size,
        ))
        .build();

    // Create the execution plan
    let mut exec: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

    // Wrap in a FilterExec to apply the predicate
    if !pushdown {
        exec = Arc::new(datafusion_physical_plan::filter::FilterExec::try_new(
            predicate,
            exec,
        ).expect("failed to create FilterExec"));
    }

    // Create task context with the local filesystem object store
    let task_ctx = create_task_context();

    (exec, task_ctx)
}

/// Execute the scan and count rows - this is the measured part
async fn execute_and_count(exec: &Arc<dyn ExecutionPlan>, task_ctx: &Arc<TaskContext>) -> usize {
    let stream = exec
        .execute(0, Arc::clone(task_ctx))
        .expect("failed to execute parquet scan");

    count_rows(stream).await
}

fn create_task_context() -> Arc<TaskContext> {
    let task_ctx = TaskContext::default();

    // Register the local filesystem object store
    let local_fs = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
    let object_store_url = ObjectStoreUrl::local_filesystem();
    task_ctx
        .runtime_env()
        .register_object_store(object_store_url.as_ref(), local_fs);

    Arc::new(task_ctx)
}

async fn count_rows(mut stream: SendableRecordBatchStream) -> usize {
    let mut total_rows = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.expect("failed to read batch");
        total_rows += batch.num_rows();
    }
    total_rows
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("struct_data.parquet");

    // Create schema:
    // - struct_col: Struct { id: Int32 }
    // - payload: Binary (large, separate column)
    let struct_fields = Fields::from(vec![Field::new(INT_FIELD_NAME, DataType::Int32, false)]);
    let schema = Arc::new(Schema::new(vec![
        Field::new(STRUCT_COLUMN_NAME, DataType::Struct(struct_fields), false),
        Field::new(PAYLOAD_COLUMN_NAME, DataType::Binary, false),
    ]));

    let writer_props = WriterProperties::builder()
        .set_max_row_group_size(ROW_GROUP_SIZE)
        .build();

    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file_path)?,
        Arc::clone(&schema),
        Some(writer_props),
    )?;

    // Create row groups where each has a distinct id value (0-9).
    // The predicate `struct_col['id'] = 9` will match only the last row group,
    // so with late materialization, only 10% of the payload data needs to be read.
    for group_id in 0..TOTAL_ROW_GROUPS {
        let first_id_value = group_id as i32 * ROW_GROUP_SIZE as i32;
        let batch = build_batch(&schema, first_id_value, ROW_GROUP_SIZE)?;
        writer.write(&batch)?;
    }

    writer.close()?;

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
        schema,
    })
}

fn build_batch(
    schema: &SchemaRef,
    first_id_value: i32,
    len: usize,
) -> datafusion_common::Result<RecordBatch> {
    let mut rng = rand::rng();

    // Build the struct column with just the id field
    let ids: Vec<i32> = (first_id_value..first_id_value + len as i32).collect();
    let id_array = Int32Array::from(ids);
    let struct_array = StructArray::from(vec![(
        Arc::new(Field::new(INT_FIELD_NAME, DataType::Int32, false)),
        Arc::new(id_array) as ArrayRef,
    )]);

    let mut payload = vec![0u8; PAYLOAD_BYTES];

    // Build the payload column (separate from struct) with random large strings
    let mut payload_builder = BinaryBuilder::new();
    for _ in 0..len {
        rng.fill(&mut payload[..]);
        payload_builder.append_value(&payload);
    }
    let payload_array = payload_builder.finish();

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(struct_array), Arc::new(payload_array)],
    )?)
}

criterion_group!(benches, parquet_struct_filter_pushdown);
criterion_main!(benches);
