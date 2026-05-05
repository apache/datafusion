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

//! Microbenchmarks for per-file work that scales with parquet schema width.
//!
//! These isolate the operations that wide-schema (1024+ column) parquet
//! datasets pay on every file open: building the arrow schema view from
//! parquet metadata, computing schema coercions, building pruning
//! predicates, and per-column statistics conversion. The dataset itself is
//! tiny (one row group, a few rows) so the numbers are dominated by
//! per-column setup.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{Bencher, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_datasource_parquet::apply_file_schema_type_coercions;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_pruning::{PruningPredicate, build_pruning_predicate};
use datafusion_physical_plan::metrics::Count;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::{ProjectionMask, parquet_to_arrow_schema_and_field_levels};
use parquet::file::properties::WriterProperties;

/// Build an arrow [`Schema`] of `width` columns: `[id INT64, ts INT64,
/// c0 STRING, c1 STRING, ..., cN STRING]`.
fn make_schema(width: usize) -> SchemaRef {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(width);
    fields.push(Arc::new(Field::new("id", DataType::Int64, false)));
    fields.push(Arc::new(Field::new("ts", DataType::Int64, true)));
    for i in 0..width.saturating_sub(2) {
        fields.push(Arc::new(Field::new(
            format!("c{i}"),
            DataType::Utf8,
            true,
        )));
    }
    Arc::new(Schema::new(fields))
}

/// Build a tiny parquet file with the given schema in memory and return
/// its bytes.
fn write_parquet(schema: SchemaRef) -> Vec<u8> {
    let n_rows = 4;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        match f.data_type() {
            DataType::Int64 => {
                cols.push(Arc::new(Int64Array::from(vec![0i64; n_rows])) as _)
            }
            DataType::Utf8 => {
                cols.push(Arc::new(StringArray::from(vec!["x"; n_rows])) as _)
            }
            other => panic!("unsupported in benchmark schema: {other:?}"),
        }
    }
    let batch = RecordBatch::try_new(schema.clone(), cols).unwrap();
    let mut buf = Vec::new();
    {
        let mut w = ArrowWriter::try_new(
            &mut buf,
            schema,
            Some(WriterProperties::builder().build()),
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    buf
}

/// Bench: build ArrowReaderMetadata::try_new vs from_field_levels for a
/// wide-schema file. Try_new walks every leaf; from_field_levels is the
/// "cached" path.
fn bench_arrow_reader_metadata(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_reader_metadata");
    for &width in &[8usize, 64, 256, 1024] {
        let schema = make_schema(width);
        let bytes = write_parquet(schema.clone());
        let bytes = bytes::Bytes::from(bytes);

        // Pre-load the metadata once.
        let arm = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new()).unwrap();
        let metadata = Arc::clone(arm.metadata());
        let parquet_schema = arm.parquet_schema().clone();

        group.bench_with_input(
            BenchmarkId::new("try_new", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    let _ = ArrowReaderMetadata::try_new(
                        Arc::clone(&metadata),
                        ArrowReaderOptions::new(),
                    )
                    .unwrap();
                });
            },
        );

        // Pre-compute field levels once for the from_field_levels bench
        let (arrow_schema, _) = parquet_to_arrow_schema_and_field_levels(
            &parquet_schema,
            ProjectionMask::all(),
            metadata.file_metadata().key_value_metadata(),
        )
        .unwrap();
        let arrow_schema = Arc::new(arrow_schema);
        let prebuilt = ArrowReaderMetadata::from_field_levels(
            Arc::clone(&metadata),
            Arc::clone(&arrow_schema),
            parquet_to_arrow_schema_and_field_levels(
                &parquet_schema,
                ProjectionMask::all(),
                metadata.file_metadata().key_value_metadata(),
            )
            .unwrap()
            .1,
        );

        group.bench_with_input(
            BenchmarkId::new("clone_cached", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    // What our DataFusion cache hit reduces to.
                    let _ = prebuilt.clone();
                });
            },
        );
    }
    group.finish();
}

/// Bench: apply_file_schema_type_coercions when the schemas already
/// match (no-op case).
fn bench_apply_coercions_noop(c: &mut Criterion) {
    let mut group = c.benchmark_group("apply_file_schema_type_coercions_noop");
    for &width in &[8usize, 64, 256, 1024] {
        let schema = make_schema(width);
        group.bench_with_input(
            BenchmarkId::new("identical", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    let _ = apply_file_schema_type_coercions(&schema, &schema);
                });
            },
        );
    }
    group.finish();
}

/// Bench: PruningPredicate::try_new with a small predicate against a
/// wide schema. The schema-walk cost should not scale with the schema
/// width when the predicate touches a constant number of columns.
fn bench_pruning_predicate(c: &mut Criterion) {
    let mut group = c.benchmark_group("pruning_predicate_try_new");
    for &width in &[8usize, 64, 256, 1024] {
        let schema = make_schema(width);
        // predicate: id = 12345
        let id_col: Arc<dyn PhysicalExpr> = Arc::new(Column::new("id", 0));
        let lit: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(datafusion_common::ScalarValue::Int64(Some(
                12345,
            ))));
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(
            datafusion_physical_expr::expressions::BinaryExpr::new(
                id_col,
                datafusion_expr::Operator::Eq,
                lit,
            ),
        );
        group.bench_with_input(
            BenchmarkId::new("eq", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    let _ = PruningPredicate::try_new(
                        Arc::clone(&predicate),
                        Arc::clone(&schema),
                    )
                    .unwrap();
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("build_pruning_predicate", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                let errors = Count::new();
                b.iter(|| {
                    let _ = build_pruning_predicate(
                        Arc::clone(&predicate),
                        &schema,
                        &errors,
                    );
                });
            },
        );
    }
    group.finish();
}

/// Bench: building one StatisticsConverter for a single column out of a
/// wide schema. This is the inner-loop cost in
/// `statistics_from_parquet_metadata`.
fn bench_statistics_converter(c: &mut Criterion) {
    let mut group = c.benchmark_group("statistics_converter");
    for &width in &[8usize, 64, 256, 1024] {
        let schema = make_schema(width);
        let bytes = bytes::Bytes::from(write_parquet(schema.clone()));
        let arm = ArrowReaderMetadata::load(&bytes, ArrowReaderOptions::new()).unwrap();
        let parquet_schema = arm.parquet_schema().clone();

        group.bench_with_input(
            BenchmarkId::new("try_new_one_col", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    let _ = StatisticsConverter::try_new(
                        "id",
                        &schema,
                        &parquet_schema,
                    )
                    .unwrap();
                });
            },
        );

        // Pre-resolve the field/index for the from_arrow_field bench.
        let arrow_field = schema.field_with_name("id").unwrap();
        let parquet_idx = parquet::arrow::parquet_column(&parquet_schema, &schema, "id")
            .map(|(i, _)| i);
        group.bench_with_input(
            BenchmarkId::new("from_arrow_field_one_col", width),
            &width,
            |b: &mut Bencher<'_>, _| {
                b.iter(|| {
                    let _ = StatisticsConverter::from_arrow_field(
                        arrow_field,
                        &parquet_schema,
                        parquet_idx,
                    );
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_arrow_reader_metadata,
    bench_apply_coercions_noop,
    bench_pruning_predicate,
    bench_statistics_converter,
);
criterion_main!(benches);
