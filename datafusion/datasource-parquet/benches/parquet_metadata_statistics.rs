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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_datasource_parquet::metadata::DFParquetMetadata;
use parquet::arrow::ArrowSchemaConverter;
use parquet::data_type::ByteArray;
use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::statistics::{Statistics as ParquetStatistics, ValueStatistics};

const ROWS_PER_GROUP: usize = 8;

#[derive(Debug, Copy, Clone)]
struct BenchmarkSpec {
    columns: usize,
    row_groups: usize,
    metadata: MetadataState,
}

#[derive(Debug, Copy, Clone)]
enum MetadataState {
    Full,
    Mixed,
    None,
}

impl std::fmt::Display for MetadataState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "full"),
            Self::Mixed => write!(f, "mixed"),
            Self::None => write!(f, "none"),
        }
    }
}

struct BenchmarkCase {
    schema: SchemaRef,
    metadata: ParquetMetaData,
}

fn parquet_metadata_statistics(c: &mut Criterion) {
    let metadata_states = [
        MetadataState::Full,
        MetadataState::Mixed,
        MetadataState::None,
    ];
    let column_counts = [8, 64, 256];
    let row_group_counts = [1, 32, 128];

    let mut group = c.benchmark_group("parquet_metadata_statistics");

    for metadata in metadata_states {
        for columns in column_counts {
            for row_groups in row_group_counts {
                let spec = BenchmarkSpec {
                    columns,
                    row_groups,
                    metadata,
                };
                group.bench_function(
                    BenchmarkId::from_parameter(format!(
                        "metadata_{}_col_{}_rg_{}",
                        spec.metadata, spec.columns, spec.row_groups,
                    )),
                    |b| {
                        b.iter_batched(
                            || BenchmarkCase::new(spec),
                            |case| {
                                let statistics =
                                    DFParquetMetadata::statistics_from_parquet_metadata(
                                        black_box(&case.metadata),
                                        black_box(&case.schema),
                                    )
                                    .expect("statistics extraction failed");
                                black_box(statistics);
                            },
                            BatchSize::PerIteration,
                        );
                    },
                );
            }
        }
    }

    group.finish();
}

impl BenchmarkCase {
    fn new(spec: BenchmarkSpec) -> Self {
        let schema = make_schema(spec.columns);
        let metadata = match spec.metadata {
            MetadataState::Full => {
                make_synthetic_metadata(&schema, spec, full_statistics)
            }
            MetadataState::Mixed => {
                make_synthetic_metadata(&schema, spec, mixed_statistics)
            }
            MetadataState::None => make_synthetic_metadata(&schema, spec, |_, _, _| None),
        };

        Self { schema, metadata }
    }
}

fn make_synthetic_metadata(
    schema: &SchemaRef,
    spec: BenchmarkSpec,
    statistics: fn(&DataType, usize, usize) -> Option<ParquetStatistics>,
) -> ParquetMetaData {
    let schema_descr = Arc::new(
        ArrowSchemaConverter::new()
            .convert(schema.as_ref())
            .expect("failed to convert arrow schema"),
    );
    let row_groups = (0..spec.row_groups)
        .map(|row_group| {
            let columns = schema
                .fields()
                .iter()
                .enumerate()
                .map(|(column_idx, field)| {
                    let mut builder =
                        ColumnChunkMetaData::builder(schema_descr.column(column_idx));
                    if let Some(statistics) =
                        statistics(field.data_type(), column_idx, row_group)
                    {
                        builder = builder.set_statistics(statistics);
                    }
                    builder
                        .set_num_values(ROWS_PER_GROUP as i64)
                        .build()
                        .expect("failed to build column metadata")
                })
                .collect::<Vec<_>>();

            RowGroupMetaData::builder(Arc::clone(&schema_descr))
                .set_num_rows(ROWS_PER_GROUP as i64)
                .set_total_byte_size((spec.columns * ROWS_PER_GROUP * 8) as i64)
                .set_column_metadata(columns)
                .build()
                .expect("failed to build row group metadata")
        })
        .collect::<Vec<_>>();

    let file_metadata = FileMetaData::new(
        1,
        (spec.row_groups * ROWS_PER_GROUP) as i64,
        Some("datafusion parquet metadata benchmark".to_string()),
        None,
        schema_descr,
        None,
    );

    ParquetMetaData::new(file_metadata, row_groups)
}

fn full_statistics(
    data_type: &DataType,
    column_idx: usize,
    row_group: usize,
) -> Option<ParquetStatistics> {
    Some(statistics(
        data_type,
        column_idx,
        row_group,
        true,
        true,
        Some(null_count_for_rows()),
    ))
}

fn mixed_statistics(
    data_type: &DataType,
    column_idx: usize,
    row_group: usize,
) -> Option<ParquetStatistics> {
    if column_idx.is_multiple_of(16) || row_group.is_multiple_of(5) {
        return None;
    }

    let min_exact = !row_group.is_multiple_of(3);
    let max_exact = !row_group.is_multiple_of(4);
    let null_count = (!row_group.is_multiple_of(7)).then(null_count_for_rows);

    Some(statistics(
        data_type, column_idx, row_group, min_exact, max_exact, null_count,
    ))
}

fn statistics(
    data_type: &DataType,
    column_idx: usize,
    row_group: usize,
    min_exact: bool,
    max_exact: bool,
    null_count: Option<u64>,
) -> ParquetStatistics {
    let min_row = first_non_null_row();
    let max_row = last_non_null_row();

    match data_type {
        DataType::Int64 => {
            let min = min_row.map(|row| value(column_idx, row_group, row));
            let max = max_row.map(|row| value(column_idx, row_group, row));
            ParquetStatistics::Int64(
                ValueStatistics::new(min, max, None, null_count, false)
                    .with_min_is_exact(min_exact)
                    .with_max_is_exact(max_exact),
            )
        }
        DataType::Float64 => {
            let min = min_row.map(|row| value(column_idx, row_group, row) as f64 * 1.5);
            let max = max_row.map(|row| value(column_idx, row_group, row) as f64 * 1.5);
            ParquetStatistics::Double(
                ValueStatistics::new(min, max, None, null_count, false)
                    .with_min_is_exact(min_exact)
                    .with_max_is_exact(max_exact),
            )
        }
        DataType::Utf8 => {
            let min = min_row.map(|row| {
                ByteArray::from(string_value(column_idx, row_group, row).into_bytes())
            });
            let max = max_row.map(|row| {
                ByteArray::from(string_value(column_idx, row_group, row).into_bytes())
            });
            ParquetStatistics::ByteArray(
                ValueStatistics::new(min, max, None, null_count, false)
                    .with_min_is_exact(min_exact)
                    .with_max_is_exact(max_exact),
            )
        }
        other => unreachable!("unsupported benchmark data type: {other:?}"),
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

fn first_non_null_row() -> Option<usize> {
    (0..ROWS_PER_GROUP).find(|row| !row.is_multiple_of(7))
}

fn last_non_null_row() -> Option<usize> {
    (0..ROWS_PER_GROUP).rev().find(|row| !row.is_multiple_of(7))
}

fn null_count_for_rows() -> u64 {
    (0..ROWS_PER_GROUP)
        .filter(|row| row.is_multiple_of(7))
        .count() as u64
}

fn value(column_idx: usize, row_group: usize, row: usize) -> i64 {
    (column_idx as i64 * 10_000) + (row_group as i64 * 100) + row as i64
}

fn string_value(column_idx: usize, row_group: usize, row: usize) -> String {
    format!("s{column_idx:04}_{row_group:04}_{row:04}")
}

criterion_group!(benches, parquet_metadata_statistics);
criterion_main!(benches);
