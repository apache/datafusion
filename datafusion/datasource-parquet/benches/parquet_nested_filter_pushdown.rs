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

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    BinaryBuilder, BooleanArray, ListBuilder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_datasource_parquet::{ParquetFileMetrics, build_row_filter};
use datafusion_expr::{Expr, col};
use datafusion_functions_nested::expr_fn::array_has;
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

const ROW_GROUP_SIZE: usize = 10_000;
const TOTAL_ROW_GROUPS: usize = 10;
const TOTAL_ROWS: usize = ROW_GROUP_SIZE * TOTAL_ROW_GROUPS;
const TARGET_VALUE: &str = "target_value";
const COLUMN_NAME: &str = "list_col";
const PAYLOAD_COLUMN_NAME: &str = "payload";
// Large binary payload to emphasize decoding overhead when pushdown is disabled.
const PAYLOAD_BYTES: usize = 8 * 1024;

struct BenchmarkDataset {
    _tempdir: TempDir,
    file_path: PathBuf,
}

impl BenchmarkDataset {
    fn path(&self) -> &Path {
        &self.file_path
    }
}

static DATASET: LazyLock<BenchmarkDataset> = LazyLock::new(|| {
    create_dataset().expect("failed to prepare parquet benchmark dataset")
});

fn parquet_nested_filter_pushdown(c: &mut Criterion) {
    let dataset_path = DATASET.path().to_owned();
    let mut group = c.benchmark_group("parquet_nested_filter_pushdown");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));

    group.bench_function("no_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&create_predicate(), &file_schema);
        b.iter(|| {
            let matched = scan_with_predicate(&dataset_path, &predicate, false)
                .expect("baseline parquet scan with filter succeeded");
            assert_eq!(matched, ROW_GROUP_SIZE);
        });
    });

    group.bench_function("with_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&create_predicate(), &file_schema);
        b.iter(|| {
            let matched = scan_with_predicate(&dataset_path, &predicate, true)
                .expect("pushdown parquet scan with filter succeeded");
            assert_eq!(matched, ROW_GROUP_SIZE);
        });
    });

    group.finish();
}

fn setup_reader(path: &Path) -> SchemaRef {
    let file = std::fs::File::open(path).expect("failed to open file");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("failed to build reader");
    Arc::clone(builder.schema())
}

fn create_predicate() -> Expr {
    array_has(
        col(COLUMN_NAME),
        Expr::Literal(ScalarValue::Utf8(Some(TARGET_VALUE.to_string())), None),
    )
}

fn scan_with_predicate(
    path: &Path,
    predicate: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    pushdown: bool,
) -> datafusion_common::Result<usize> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let file_schema = builder.schema();
    let projection = ProjectionMask::all();

    let metrics = ExecutionPlanMetricsSet::new();
    let file_metrics = ParquetFileMetrics::new(0, &path.display().to_string(), &metrics);

    let builder = if pushdown {
        if let Some(row_filter) =
            build_row_filter(predicate, file_schema, &metadata, false, &file_metrics)?
        {
            builder.with_row_filter(row_filter)
        } else {
            builder
        }
    } else {
        builder
    };

    let reader = builder.with_projection(projection).build()?;

    let mut matched_rows = 0usize;
    for batch in reader {
        let batch = batch?;
        matched_rows += count_matches(predicate, &batch)?;
    }

    if pushdown {
        let pruned_rows = file_metrics.pushdown_rows_pruned.value();
        assert_eq!(
            pruned_rows,
            TOTAL_ROWS - matched_rows,
            "row-level pushdown should prune 90% of rows"
        );
    }

    Ok(matched_rows)
}

fn count_matches(
    expr: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    batch: &RecordBatch,
) -> datafusion_common::Result<usize> {
    let values = expr.evaluate(batch)?.into_array(batch.num_rows())?;
    let bools = values
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("boolean filter result");

    Ok(bools.iter().filter(|v| matches!(v, Some(true))).count())
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("nested_lists.parquet");

    let field = Arc::new(Field::new("item", DataType::Utf8, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new(COLUMN_NAME, DataType::List(field), false),
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

    // Create sorted row groups with distinct values so that min/max statistics
    // allow skipping most groups when applying a selective predicate.
    let sorted_values = [
        "alpha",
        "bravo",
        "charlie",
        "delta",
        "echo",
        "foxtrot",
        "golf",
        "hotel",
        "india",
        TARGET_VALUE,
    ];

    for value in sorted_values {
        let batch = build_list_batch(&schema, value, ROW_GROUP_SIZE)?;
        writer.write(&batch)?;
    }

    writer.close()?;

    // Ensure the writer respected the requested row group size
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file_path)?)?;
    assert_eq!(reader.metadata().row_groups().len(), TOTAL_ROW_GROUPS);

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
    })
}

fn build_list_batch(
    schema: &SchemaRef,
    value: &str,
    len: usize,
) -> datafusion_common::Result<RecordBatch> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    let mut payload_builder = BinaryBuilder::new();
    let payload = vec![1u8; PAYLOAD_BYTES];
    for _ in 0..len {
        builder.values().append_value(value);
        builder.append(true);
        payload_builder.append_value(&payload);
    }

    let array = builder.finish();
    let payload_array = payload_builder.finish();
    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![Arc::new(array), Arc::new(payload_array)],
    )?)
}

criterion_group!(benches, parquet_nested_filter_pushdown);
criterion_main!(benches);
