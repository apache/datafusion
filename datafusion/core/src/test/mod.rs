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

//! Common unit test utility methods

use crate::arrow::array::UInt32Array;
use crate::datasource::file_format::file_type::{FileCompressionType, FileType};
use crate::datasource::listing::PartitionedFile;
use crate::datasource::object_store::ObjectStoreUrl;
use crate::datasource::{MemTable, TableProvider};
use crate::error::Result;
use crate::from_slice::FromSlice;
use crate::logical_expr::LogicalPlan;
use crate::physical_plan::file_format::{CsvExec, FileScanConfig};
use crate::test::object_store::local_unpartitioned_file;
use crate::test_util::{aggr_test_schema, arrow_test_data};
use array::ArrayRef;
use arrow::array::{self, Array, Decimal128Builder, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "compression")]
use bzip2::write::BzEncoder;
#[cfg(feature = "compression")]
use bzip2::Compression as BzCompression;
#[cfg(feature = "compression")]
use flate2::write::GzEncoder;
#[cfg(feature = "compression")]
use flate2::Compression as GzCompression;
use futures::{Future, FutureExt};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::pin::Pin;
use std::sync::Arc;
use tempfile::TempDir;
#[cfg(feature = "compression")]
use xz2::write::XzEncoder;

pub fn create_table_dual() -> Arc<dyn TableProvider> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(array::Int32Array::from_slice([1])),
            Arc::new(array::StringArray::from_slice(["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Returns a [`CsvExec`] that scans "aggregate_test_100.csv" with `partitions` partitions
pub fn scan_partitioned_csv(partitions: usize) -> Result<Arc<CsvExec>> {
    let schema = aggr_test_schema();
    let filename = "aggregate_test_100.csv";
    let path = format!("{}/csv", arrow_test_data());
    let file_groups = partitioned_file_groups(
        path.as_str(),
        filename,
        partitions,
        FileType::CSV,
        FileCompressionType::UNCOMPRESSED,
    )?;
    let config = partitioned_csv_config(schema, file_groups)?;
    Ok(Arc::new(CsvExec::new(
        config,
        true,
        b',',
        FileCompressionType::UNCOMPRESSED,
    )))
}

/// Returns file groups [`Vec<Vec<PartitionedFile>>`] for scanning `partitions` of `filename`
pub fn partitioned_file_groups(
    path: &str,
    filename: &str,
    partitions: usize,
    file_type: FileType,
    file_compression_type: FileCompressionType,
) -> Result<Vec<Vec<PartitionedFile>>> {
    let path = format!("{path}/{filename}");

    let tmp_dir = TempDir::new()?.into_path();

    let mut writers = vec![];
    let mut files = vec![];
    for i in 0..partitions {
        let filename = format!(
            "partition-{}{}",
            i,
            file_type
                .to_owned()
                .get_ext_with_compression(file_compression_type.to_owned())
                .unwrap()
        );
        let filename = tmp_dir.join(filename);

        let file = File::create(&filename).unwrap();

        let encoder: Box<dyn Write + Send> = match file_compression_type.to_owned() {
            FileCompressionType::UNCOMPRESSED => Box::new(file),
            #[cfg(feature = "compression")]
            FileCompressionType::GZIP => {
                Box::new(GzEncoder::new(file, GzCompression::default()))
            }
            #[cfg(feature = "compression")]
            FileCompressionType::XZ => Box::new(XzEncoder::new(file, 9)),
            #[cfg(feature = "compression")]
            FileCompressionType::BZIP2 => {
                Box::new(BzEncoder::new(file, BzCompression::default()))
            }
            #[cfg(not(feature = "compression"))]
            FileCompressionType::GZIP
            | FileCompressionType::BZIP2
            | FileCompressionType::XZ => {
                panic!("GZIP compression is not supported in this build")
            }
        };

        let writer = BufWriter::new(encoder);
        writers.push(writer);
        files.push(filename);
    }

    let f = File::open(path)?;
    let f = BufReader::new(f);
    for (i, line) in f.lines().enumerate() {
        let line = line.unwrap();

        if i == 0 && file_type == FileType::CSV {
            // write header to all partitions
            for w in writers.iter_mut() {
                w.write_all(line.as_bytes()).unwrap();
                w.write_all(b"\n").unwrap();
            }
        } else {
            // write data line to single partition
            let partition = i % partitions;
            writers[partition].write_all(line.as_bytes()).unwrap();
            writers[partition].write_all(b"\n").unwrap();
        }
    }
    for w in writers.iter_mut() {
        w.flush().unwrap();
    }

    Ok(files
        .into_iter()
        .map(|f| vec![local_unpartitioned_file(f).into()])
        .collect::<Vec<_>>())
}

/// Returns a [`FileScanConfig`] for given `file_groups`
pub fn partitioned_csv_config(
    schema: SchemaRef,
    file_groups: Vec<Vec<PartitionedFile>>,
) -> Result<FileScanConfig> {
    Ok(FileScanConfig {
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_schema: schema,
        file_groups,
        statistics: Default::default(),
        projection: None,
        limit: None,
        table_partition_cols: vec![],
        output_ordering: None,
        infinite_source: false,
    })
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: Vec<&str>) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
}

/// returns a table with 3 columns of i32 in memory
pub fn build_table_i32(
    a: (&str, &Vec<i32>),
    b: (&str, &Vec<i32>),
    c: (&str, &Vec<i32>),
) -> RecordBatch {
    let schema = Schema::new(vec![
        Field::new(a.0, DataType::Int32, false),
        Field::new(b.0, DataType::Int32, false),
        Field::new(c.0, DataType::Int32, false),
    ]);

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(a.1.clone())),
            Arc::new(Int32Array::from(b.1.clone())),
            Arc::new(Int32Array::from(c.1.clone())),
        ],
    )
    .unwrap()
}

/// Returns the column names on the schema
pub fn columns(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

/// Return a new table provider that has a single Int32 column with
/// values between `seq_start` and `seq_end`
pub fn table_with_sequence(
    seq_start: i32,
    seq_end: i32,
) -> Result<Arc<dyn TableProvider>> {
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from((seq_start..=seq_end).collect::<Vec<_>>()));
    let partitions = vec![vec![RecordBatch::try_new(
        schema.clone(),
        vec![arr as ArrayRef],
    )?]];
    Ok(Arc::new(MemTable::try_new(schema, partitions)?))
}

/// Return a RecordBatch with a single Int32 array with values (0..sz)
pub fn make_partition(sz: i32) -> RecordBatch {
    let seq_start = 0;
    let seq_end = sz;
    let values = (seq_start..seq_end).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)]));
    let arr = Arc::new(Int32Array::from(values));
    let arr = arr as ArrayRef;

    RecordBatch::try_new(schema, vec![arr]).unwrap()
}

/// Return a new table which provide this decimal column
pub fn table_with_decimal() -> Arc<dyn TableProvider> {
    let batch_decimal = make_decimal();
    let schema = batch_decimal.schema();
    let partitions = vec![vec![batch_decimal]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

fn make_decimal() -> RecordBatch {
    let mut decimal_builder = Decimal128Builder::with_capacity(20);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128);
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128);
    }
    let array = decimal_builder
        .finish()
        .with_precision_and_scale(10, 3)
        .unwrap();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

/// Asserts that given future is pending.
pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
    let waker = futures::task::noop_waker();
    let mut cx = futures::task::Context::from_waker(&waker);
    let poll = fut.poll_unpin(&mut cx);

    assert!(poll.is_pending());
}

/// Create vector batches
pub fn create_vec_batches(schema: &Schema, n: usize) -> Vec<RecordBatch> {
    let batch = create_batch(schema);
    let mut vec = Vec::with_capacity(n);
    for _ in 0..n {
        vec.push(batch.clone());
    }
    vec
}

/// Create batch
fn create_batch(schema: &Schema) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(UInt32Array::from_slice([1, 2, 3, 4, 5, 6, 7, 8]))],
    )
    .unwrap()
}

pub mod exec;
pub mod object_store;
pub mod variable;
