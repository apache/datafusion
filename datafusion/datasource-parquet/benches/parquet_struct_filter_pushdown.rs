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

//! Benchmarks for struct field filter pushdown in Parquet.
//!
//! Compares scanning with vs without row-level filter pushdown for
//! predicates on struct sub-fields (e.g. `get_field(s, 'id') = 42`).
//!
//! The dataset schema (in SQL-like notation):
//!
//! ```sql
//! CREATE TABLE t (
//!     id       INT,          -- top-level id, useful for correctness checks
//!     large_string TEXT,     -- wide column so SELECT * is expensive
//!     s STRUCT<
//!         id: INT,           -- mirrors top-level id
//!         large_string: TEXT -- wide sub-field; pushdown with proper projection
//!                            -- should avoid reading this when filtering on s.id
//!     >
//! );
//! ```
//!
//! Benchmark queries:
//!
//! 1. `SELECT * FROM t WHERE get_field(s, 'id') = 42`
//!     - no pushdown vs. row-level filter pushdown
//! 2. `SELECT * FROM t WHERE get_field(s, 'id') = id`
//!     - cross-column predicate; no pushdown vs. row-level filter pushdown
//! 3. `SELECT id FROM t WHERE get_field(s, 'id') = 42`
//!     - narrow projection; pushdown should avoid reading s.large_string

use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use arrow::array::{BooleanArray, Int32Array, RecordBatch, StringBuilder, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_datasource_parquet::{ParquetFileMetrics, build_row_filter};
use datafusion_expr::{Expr, col};
use datafusion_physical_expr::planner::logical2physical;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;
use tempfile::TempDir;

const ROW_GROUP_ROW_COUNT: usize = 10_000;
const TOTAL_ROW_GROUPS: usize = 10;
const TOTAL_ROWS: usize = ROW_GROUP_ROW_COUNT * TOTAL_ROW_GROUPS;
/// Only one row group will contain the target value.
const TARGET_VALUE: i32 = 42;
const ID_COLUMN_NAME: &str = "id";
const LARGE_STRING_COLUMN_NAME: &str = "large_string";
const STRUCT_COLUMN_NAME: &str = "s";
// Large string payload to emphasize decoding overhead when pushdown is disabled.
const LARGE_STRING_LEN: usize = 8 * 1024;

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

fn parquet_struct_filter_pushdown(c: &mut Criterion) {
    let dataset_path = DATASET.path().to_owned();
    let mut group = c.benchmark_group("parquet_struct_filter_pushdown");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));

    // Scenario 1: SELECT * FROM t WHERE get_field(s, 'id') = 42
    group.bench_function("select_star/no_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_literal(), &file_schema);
        b.iter(|| {
            let matched = scan(&dataset_path, &predicate, false, ProjectionMask::all())
                .expect("scan succeeded");
            assert_eq!(matched, ROW_GROUP_ROW_COUNT);
        });
    });

    group.bench_function("select_star/with_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_literal(), &file_schema);
        b.iter(|| {
            let matched = scan(&dataset_path, &predicate, true, ProjectionMask::all())
                .expect("scan succeeded");
            assert_eq!(matched, ROW_GROUP_ROW_COUNT);
        });
    });

    // Scenario 2: SELECT * FROM t WHERE get_field(s, 'id') = id
    group.bench_function("select_star_cross_col/no_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_top_id(), &file_schema);
        b.iter(|| {
            let matched = scan(&dataset_path, &predicate, false, ProjectionMask::all())
                .expect("scan succeeded");
            assert_eq!(matched, TOTAL_ROWS);
        });
    });

    group.bench_function("select_star_cross_col/with_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_top_id(), &file_schema);
        b.iter(|| {
            let matched = scan(&dataset_path, &predicate, true, ProjectionMask::all())
                .expect("scan succeeded");
            assert_eq!(matched, TOTAL_ROWS);
        });
    });

    // Scenario 3: SELECT id FROM t WHERE get_field(s, 'id') = 42
    group.bench_function("select_id/no_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_literal(), &file_schema);
        b.iter(|| {
            // Without pushdown we must read all columns to evaluate the predicate.
            let matched = scan(&dataset_path, &predicate, false, ProjectionMask::all())
                .expect("scan succeeded");
            assert_eq!(matched, ROW_GROUP_ROW_COUNT);
        });
    });

    group.bench_function("select_id/with_pushdown", |b| {
        let file_schema = setup_reader(&dataset_path);
        let predicate = logical2physical(&struct_id_eq_literal(), &file_schema);
        let id_only = id_projection(&dataset_path);
        b.iter(|| {
            // With pushdown the filter runs first, then we only project `id`.
            let matched = scan(&dataset_path, &predicate, true, id_only.clone())
                .expect("scan succeeded");
            assert_eq!(matched, ROW_GROUP_ROW_COUNT);
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

/// `get_field(s, 'id') = TARGET_VALUE`
fn struct_id_eq_literal() -> Expr {
    let get_field_expr = datafusion_functions::core::get_field().call(vec![
        col(STRUCT_COLUMN_NAME),
        Expr::Literal(ScalarValue::Utf8(Some("id".to_string())), None),
    ]);
    get_field_expr.eq(Expr::Literal(ScalarValue::Int32(Some(TARGET_VALUE)), None))
}

/// `get_field(s, 'id') = id`
fn struct_id_eq_top_id() -> Expr {
    let get_field_expr = datafusion_functions::core::get_field().call(vec![
        col(STRUCT_COLUMN_NAME),
        Expr::Literal(ScalarValue::Utf8(Some("id".to_string())), None),
    ]);
    get_field_expr.eq(col(ID_COLUMN_NAME))
}

/// Build a [`ProjectionMask`] that only reads the top-level `id` leaf column.
fn id_projection(path: &Path) -> ProjectionMask {
    let file = std::fs::File::open(path).expect("failed to open file");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("failed to build reader");
    let parquet_schema = builder.metadata().file_metadata().schema_descr_ptr();
    // Leaf index 0 corresponds to the top-level `id` column.
    ProjectionMask::leaves(&parquet_schema, [0])
}

fn scan(
    path: &Path,
    predicate: &Arc<dyn datafusion_physical_expr::PhysicalExpr>,
    pushdown: bool,
    projection: ProjectionMask,
) -> datafusion_common::Result<usize> {
    let file = std::fs::File::open(path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let file_schema = builder.schema();

    let metrics = ExecutionPlanMetricsSet::new();
    let file_metrics = ParquetFileMetrics::new(0, &path.display().to_string(), &metrics);

    let mut filter_applied = false;
    let builder = if pushdown {
        if let Some(row_filter) =
            build_row_filter(predicate, file_schema, &metadata, false, &file_metrics)?
        {
            filter_applied = true;
            builder.with_row_filter(row_filter)
        } else {
            builder
        }
    } else {
        builder
    };

    // Only apply a narrow projection when the filter was actually pushed down.
    // Otherwise we need all columns to evaluate the predicate manually.
    let output_projection = if filter_applied {
        projection
    } else {
        ProjectionMask::all()
    };
    let reader = builder.with_projection(output_projection).build()?;

    let mut matched_rows = 0usize;
    for batch in reader {
        let batch = batch?;
        if filter_applied {
            // When the row filter was applied, rows are already filtered.
            matched_rows += batch.num_rows();
        } else {
            matched_rows += count_matches(predicate, &batch)?;
        }
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

fn schema() -> SchemaRef {
    let struct_fields = Fields::from(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(LARGE_STRING_COLUMN_NAME, DataType::Utf8, false),
    ]);
    Arc::new(Schema::new(vec![
        Field::new(ID_COLUMN_NAME, DataType::Int32, false),
        Field::new(LARGE_STRING_COLUMN_NAME, DataType::Utf8, false),
        Field::new(STRUCT_COLUMN_NAME, DataType::Struct(struct_fields), false),
    ]))
}

fn create_dataset() -> datafusion_common::Result<BenchmarkDataset> {
    let tempdir = TempDir::new()?;
    let file_path = tempdir.path().join("struct_filter.parquet");

    let schema = schema();
    let writer_props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
        .build();

    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&file_path)?,
        Arc::clone(&schema),
        Some(writer_props),
    )?;

    // Each row group has a distinct `s.id` value. Only one row group
    // matches the target, so pushdown should prune 90% of rows.
    for rg_idx in 0..TOTAL_ROW_GROUPS {
        let id_value = if rg_idx == TOTAL_ROW_GROUPS - 1 {
            TARGET_VALUE
        } else {
            (rg_idx as i32 + 1) * 1000
        };
        let batch = build_struct_batch(&schema, id_value, ROW_GROUP_ROW_COUNT)?;
        writer.write(&batch)?;
    }

    writer.close()?;

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&file_path)?)?;
    assert_eq!(reader.metadata().row_groups().len(), TOTAL_ROW_GROUPS);

    Ok(BenchmarkDataset {
        _tempdir: tempdir,
        file_path,
    })
}

fn build_struct_batch(
    schema: &SchemaRef,
    id_value: i32,
    len: usize,
) -> datafusion_common::Result<RecordBatch> {
    let large_string: String = "x".repeat(LARGE_STRING_LEN);

    // Top-level columns
    let top_id_array = Arc::new(Int32Array::from(vec![id_value; len]));
    let mut top_string_builder = StringBuilder::new();
    for _ in 0..len {
        top_string_builder.append_value(&large_string);
    }
    let top_string_array = Arc::new(top_string_builder.finish());

    // Struct sub-fields: s.id mirrors top-level id, s.large_string is the same payload
    let struct_id_array = Arc::new(Int32Array::from(vec![id_value; len]));
    let mut struct_string_builder = StringBuilder::new();
    for _ in 0..len {
        struct_string_builder.append_value(&large_string);
    }
    let struct_string_array = Arc::new(struct_string_builder.finish());

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("id", DataType::Int32, false)),
            struct_id_array as Arc<dyn arrow::array::Array>,
        ),
        (
            Arc::new(Field::new(LARGE_STRING_COLUMN_NAME, DataType::Utf8, false)),
            struct_string_array as Arc<dyn arrow::array::Array>,
        ),
    ]);

    Ok(RecordBatch::try_new(
        Arc::clone(schema),
        vec![top_id_array, top_string_array, Arc::new(struct_array)],
    )?)
}

criterion_group!(benches, parquet_struct_filter_pushdown);
criterion_main!(benches);
