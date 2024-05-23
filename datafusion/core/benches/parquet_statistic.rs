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

//! Benchmarks of benchmark for extracting arrow statistics from parquet

use arrow::array::{ArrayRef, DictionaryArray, Float64Array, StringArray, UInt64Array};
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{
    DataType::{self, *},
    Field, Schema,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::physical_plan::parquet::{
    RequestedStatistics, StatisticsConverter,
};
use parquet::arrow::{arrow_reader::ArrowReaderBuilder, ArrowWriter};
use parquet::file::properties::WriterProperties;
use std::sync::Arc;
use tempfile::NamedTempFile;
#[derive(Debug, Clone)]
enum TestTypes {
    UInt64,
    F64,
    String,
    Dictionary,
}

use std::fmt;

impl fmt::Display for TestTypes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TestTypes::UInt64 => write!(f, "UInt64"),
            TestTypes::F64 => write!(f, "F64"),
            TestTypes::String => write!(f, "String"),
            TestTypes::Dictionary => write!(f, "Dictionary(Int32, String)"),
        }
    }
}

fn create_parquet_file(dtype: TestTypes, row_groups: usize) -> NamedTempFile {
    let schema = match dtype {
        TestTypes::UInt64 => {
            Arc::new(Schema::new(vec![Field::new("col", DataType::UInt64, true)]))
        }
        TestTypes::F64 => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            true,
        )])),
        TestTypes::String => {
            Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, true)]))
        }
        TestTypes::Dictionary => Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Dictionary(Box::new(Int32), Box::new(Utf8)),
            true,
        )])),
    };

    let props = WriterProperties::builder().build();
    let file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let mut writer =
        ArrowWriter::try_new(file.reopen().unwrap(), schema.clone(), Some(props))
            .unwrap();

    for _ in 0..row_groups {
        let batch = match dtype {
            TestTypes::UInt64 => make_uint64_batch(),
            TestTypes::F64 => make_f64_batch(),
            TestTypes::String => make_string_batch(),
            TestTypes::Dictionary => make_dict_batch(),
        };
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
    file
}

fn make_uint64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(UInt64Array::from(vec![
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
    ]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", UInt64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_f64_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Float64, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_string_batch() -> RecordBatch {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("col", Utf8, false),
        ])),
        vec![array],
    )
    .unwrap()
}

fn make_dict_batch() -> RecordBatch {
    let keys = Int32Array::from(vec![0, 1, 2, 3, 4]);
    let values = StringArray::from(vec!["a", "b", "c", "d", "e"]);
    let array: ArrayRef =
        Arc::new(DictionaryArray::try_new(keys, Arc::new(values)).unwrap());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "col",
            Dictionary(Box::new(Int32), Box::new(Utf8)),
            false,
        )])),
        vec![array],
    )
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let row_groups = 100;
    use TestTypes::*;
    let types = vec![UInt64, F64, String, Dictionary];

    for dtype in types {
        let file = create_parquet_file(dtype.clone(), row_groups);
        let file = file.reopen().unwrap();
        let reader = ArrowReaderBuilder::try_new(file).unwrap();
        let metadata = reader.metadata();

        let mut group =
            c.benchmark_group(format!("Extract statistics for {}", dtype.clone()));
        group.bench_function(
            BenchmarkId::new("extract_statistics", dtype.clone()),
            |b| {
                b.iter(|| {
                    let _ = StatisticsConverter::try_new(
                        "col",
                        RequestedStatistics::Min,
                        reader.schema(),
                    )
                    .unwrap()
                    .extract(metadata)
                    .unwrap();

                    let _ = StatisticsConverter::try_new(
                        "col",
                        RequestedStatistics::Max,
                        reader.schema(),
                    )
                    .unwrap()
                    .extract(reader.metadata())
                    .unwrap();

                    let _ = StatisticsConverter::try_new(
                        "col",
                        RequestedStatistics::NullCount,
                        reader.schema(),
                    )
                    .unwrap()
                    .extract(reader.metadata())
                    .unwrap();

                    let _ = StatisticsConverter::row_counts(reader.metadata()).unwrap();
                })
            },
        );
        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
