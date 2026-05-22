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

//! Benchmarks for deriving DataFusion table statistics from Parquet metadata.
//!
//! This mirrors the structure of Arrow's `arrow_statistics` benchmark: build
//! Parquet metadata once, then repeatedly measure statistics extraction. The
//! benchmark targets the cold planning/statistics path used by listing tables.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::stats::Precision;
use datafusion_datasource_parquet::metadata::DFParquetMetadata;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Debug, Copy, Clone)]
struct BenchmarkSpec {
    name: &'static str,
    columns: usize,
    row_groups: usize,
    rows_per_group: usize,
}

struct BenchmarkCase {
    spec: BenchmarkSpec,
    schema: SchemaRef,
    metadata: ParquetMetaData,
}

fn parquet_metadata_statistics(c: &mut Criterion) {
    let specs = [
        BenchmarkSpec {
            name: "wide_one_row_group",
            columns: 1024,
            row_groups: 1,
            rows_per_group: 16,
        },
        BenchmarkSpec {
            name: "moderate_width_many_row_groups",
            columns: 64,
            row_groups: 128,
            rows_per_group: 8,
        },
        BenchmarkSpec {
            name: "wide_many_row_groups",
            columns: 256,
            row_groups: 32,
            rows_per_group: 8,
        },
    ];

    let cases: Vec<_> = specs.into_iter().map(BenchmarkCase::new).collect();
    let mut group = c.benchmark_group("parquet_metadata_statistics");
    group.sample_size(10);

    for case in &cases {
        group.bench_function(BenchmarkId::from_parameter(case.spec.name), |b| {
            b.iter(|| {
                let statistics = DFParquetMetadata::statistics_from_parquet_metadata(
                    black_box(&case.metadata),
                    black_box(&case.schema),
                )
                .expect("statistics extraction failed");
                black_box(statistics);
            });
        });
    }

    group.finish();
}

impl BenchmarkCase {
    fn new(spec: BenchmarkSpec) -> Self {
        let schema = make_schema(spec.columns);
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(spec.rows_per_group))
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let file = tempfile::Builder::new()
            .prefix("parquet_metadata_statistics")
            .suffix(".parquet")
            .tempfile()
            .expect("failed to create temporary parquet file");
        let mut writer = ArrowWriter::try_new(
            file.reopen().expect("failed to reopen temporary file"),
            Arc::clone(&schema),
            Some(props),
        )
        .expect("failed to create parquet writer");

        for row_group in 0..spec.row_groups {
            writer
                .write(&make_batch(&schema, row_group, spec.rows_per_group))
                .expect("failed to write benchmark row group");
        }

        let metadata = writer.close().expect("failed to close parquet writer");
        assert_eq!(metadata.row_groups().len(), spec.row_groups);

        let statistics =
            DFParquetMetadata::statistics_from_parquet_metadata(&metadata, &schema)
                .expect("failed to validate benchmark metadata");
        assert_eq!(statistics.column_statistics.len(), spec.columns);
        assert_eq!(
            statistics.num_rows,
            Precision::Exact(spec.row_groups * spec.rows_per_group)
        );

        Self {
            spec,
            schema,
            metadata,
        }
    }
}

fn make_schema(columns: usize) -> SchemaRef {
    let fields = (0..columns)
        .map(|idx| {
            let data_type = match idx % 4 {
                0 => DataType::Int64,
                1 => DataType::Float64,
                2 => DataType::Utf8,
                _ => DataType::Int64,
            };
            Field::new(format!("c{idx:04}"), data_type, true)
        })
        .collect::<Vec<_>>();

    Arc::new(Schema::new(fields))
}

fn make_batch(
    schema: &SchemaRef,
    row_group: usize,
    rows_per_group: usize,
) -> RecordBatch {
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(column_idx, field)| {
            make_array(field.data_type(), column_idx, row_group, rows_per_group)
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(Arc::clone(schema), columns)
        .expect("failed to create benchmark record batch")
}

fn make_array(
    data_type: &DataType,
    column_idx: usize,
    row_group: usize,
    rows_per_group: usize,
) -> ArrayRef {
    match data_type {
        DataType::Int64 => {
            Arc::new(Int64Array::from_iter((0..rows_per_group).map(|row| {
                nullable_value(row, value(column_idx, row_group, row))
            })))
        }
        DataType::Float64 => {
            Arc::new(Float64Array::from_iter((0..rows_per_group).map(|row| {
                nullable_value(row, value(column_idx, row_group, row) as f64 * 1.5)
            })))
        }
        DataType::Utf8 => {
            Arc::new(StringArray::from_iter((0..rows_per_group).map(|row| {
                nullable_value(row, format!("s{column_idx}_{row_group}_{row}"))
            })))
        }
        other => unreachable!("unsupported benchmark data type: {other:?}"),
    }
}

fn nullable_value<T>(row: usize, value: T) -> Option<T> {
    (!row.is_multiple_of(7)).then_some(value)
}

fn value(column_idx: usize, row_group: usize, row: usize) -> i64 {
    (column_idx as i64 * 10_000) + (row_group as i64 * 100) + row as i64
}

criterion_group!(benches, parquet_metadata_statistics);
criterion_main!(benches);
