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

#![allow(missing_docs)]

use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::sync::Arc;

use crate::datasource::file_format::csv::CsvFormat;
use crate::datasource::file_format::file_compression_type::FileCompressionType;
use crate::datasource::file_format::FileFormat;

use crate::datasource::physical_plan::CsvSource;
use crate::datasource::{MemTable, TableProvider};
use crate::error::Result;
use crate::logical_expr::LogicalPlan;
use crate::test_util::{aggr_test_schema, arrow_test_data};

use datafusion_common::config::CsvOptions;

use arrow::array::{self, Array, ArrayRef, Decimal128Builder, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
#[cfg(feature = "compression")]
use datafusion_common::DataFusionError;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource::TableSchema;

#[cfg(feature = "compression")]
use bzip2::write::BzEncoder;
#[cfg(feature = "compression")]
use bzip2::Compression as BzCompression;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource_csv::partitioned_csv_config;
#[cfg(feature = "compression")]
use flate2::write::GzEncoder;
#[cfg(feature = "compression")]
use flate2::Compression as GzCompression;
#[cfg(feature = "compression")]
use liblzma::write::XzEncoder;
use object_store::local_unpartitioned_file;
#[cfg(feature = "compression")]
use zstd::Encoder as ZstdEncoder;

pub fn create_table_dual() -> Arc<dyn TableProvider> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::<Schema>::clone(&dual_schema),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(array::StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Returns a [`DataSourceExec`] that scans "aggregate_test_100.csv" with `partitions` partitions
pub fn scan_partitioned_csv(
    partitions: usize,
    work_dir: &Path,
) -> Result<Arc<DataSourceExec>> {
    let schema = aggr_test_schema();
    let filename = "aggregate_test_100.csv";
    let path = format!("{}/csv", arrow_test_data());
    let csv_format: Arc<dyn FileFormat> = Arc::new(CsvFormat::default());

    let file_groups = partitioned_file_groups(
        path.as_str(),
        filename,
        partitions,
        &csv_format,
        FileCompressionType::UNCOMPRESSED,
        work_dir,
    )?;
    let options = CsvOptions {
        has_header: Some(true),
        delimiter: b',',
        quote: b'"',
        ..Default::default()
    };
    let table_schema = TableSchema::from_file_schema(schema);
    let source = Arc::new(CsvSource::new(table_schema.clone()).with_csv_options(options));
    let config =
        FileScanConfigBuilder::from(partitioned_csv_config(file_groups, source)?)
            .with_file_compression_type(FileCompressionType::UNCOMPRESSED)
            .build();
    Ok(DataSourceExec::from_data_source(config))
}

/// Returns file groups [`Vec<FileGroup>`] for scanning `partitions` of `filename`
pub fn partitioned_file_groups(
    path: &str,
    filename: &str,
    partitions: usize,
    file_format: &Arc<dyn FileFormat>,
    file_compression_type: FileCompressionType,
    work_dir: &Path,
) -> Result<Vec<FileGroup>> {
    let path = format!("{path}/{filename}");

    let mut writers = vec![];
    let mut files = vec![];
    for i in 0..partitions {
        let filename = format!(
            "partition-{}{}",
            i,
            file_format
                .get_ext_with_compression(&file_compression_type)
                .unwrap()
        );
        let filename = work_dir.join(filename);

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
            FileCompressionType::ZSTD => {
                let encoder = ZstdEncoder::new(file, 0)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?
                    .auto_finish();
                Box::new(encoder)
            }
            #[cfg(feature = "compression")]
            FileCompressionType::BZIP2 => {
                Box::new(BzEncoder::new(file, BzCompression::default()))
            }
            #[cfg(not(feature = "compression"))]
            FileCompressionType::GZIP
            | FileCompressionType::BZIP2
            | FileCompressionType::XZ
            | FileCompressionType::ZSTD => {
                panic!("Compression is not supported in this build")
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

        if i == 0 && file_format.get_ext() == CsvFormat::default().get_ext() {
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

    // Must drop the stream before creating ObjectMeta below as drop triggers
    // finish for ZstdEncoder/BzEncoder which writes additional data
    for mut w in writers.into_iter() {
        w.flush().unwrap();
    }

    Ok(files
        .into_iter()
        .map(|f| FileGroup::new(vec![local_unpartitioned_file(f).into()]))
        .collect::<Vec<_>>())
}

pub fn assert_fields_eq(plan: &LogicalPlan, expected: &[&str]) {
    let actual: Vec<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(actual, expected);
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
        Arc::<Schema>::clone(&schema),
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

pub mod object_store;
pub mod variable;
