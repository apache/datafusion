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

use crate::datasource::object_store::local::local_unpartitioned_file;
use crate::datasource::{MemTable, PartitionedFile, TableProvider};
use crate::error::Result;
use crate::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use array::{
    Array, ArrayRef, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::array::{self, DecimalBuilder, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::{Future, FutureExt};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::pin::Pin;
use std::sync::Arc;
use tempfile::TempDir;

pub fn create_table_dual() -> Arc<dyn TableProvider> {
    let dual_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        dual_schema.clone(),
        vec![
            Arc::new(array::Int32Array::from(vec![1])),
            Arc::new(array::StringArray::from(vec!["a"])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(dual_schema, vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Generated partitioned copy of a CSV file
pub fn create_partitioned_csv(
    filename: &str,
    partitions: usize,
) -> Result<(String, Vec<Vec<PartitionedFile>>)> {
    let testdata = crate::test_util::arrow_test_data();
    let path = format!("{}/csv/{}", testdata, filename);

    let tmp_dir = TempDir::new()?;

    let mut writers = vec![];
    let mut files = vec![];
    for i in 0..partitions {
        let filename = format!("partition-{}.csv", i);
        let filename = tmp_dir.path().join(&filename);

        let writer = BufWriter::new(File::create(&filename).unwrap());
        writers.push(writer);
        files.push(filename);
    }

    let f = File::open(&path)?;
    let f = BufReader::new(f);
    for (i, line) in f.lines().enumerate() {
        let line = line.unwrap();

        if i == 0 {
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

    let groups = files
        .into_iter()
        .map(|f| vec![local_unpartitioned_file(f.to_str().unwrap().to_owned())])
        .collect::<Vec<_>>();

    Ok((tmp_dir.into_path().to_str().unwrap().to_string(), groups))
}

/// some tests share a common table with different names
pub fn test_table_scan_with_name(name: &str) -> Result<LogicalPlan> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::UInt32, false),
        Field::new("b", DataType::UInt32, false),
        Field::new("c", DataType::UInt32, false),
    ]);
    LogicalPlanBuilder::scan_empty(Some(name), &schema, None)?.build()
}

/// some tests share a common table
pub fn test_table_scan() -> Result<LogicalPlan> {
    test_table_scan_with_name("test")
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

/// Return a new table provider containing all of the supported timestamp types
pub fn table_with_timestamps() -> Arc<dyn TableProvider> {
    let batch = make_timestamps();
    let schema = batch.schema();
    let partitions = vec![vec![batch]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

/// Return a new table which provide this decimal column
pub fn table_with_decimal() -> Arc<dyn TableProvider> {
    let batch_decimal = make_decimal();
    let schema = batch_decimal.schema();
    let partitions = vec![vec![batch_decimal]];
    Arc::new(MemTable::try_new(schema, partitions).unwrap())
}

fn make_decimal() -> RecordBatch {
    let mut decimal_builder = DecimalBuilder::new(20, 10, 3);
    for i in 110000..110010 {
        decimal_builder.append_value(i as i128).unwrap();
    }
    for i in 100000..100010 {
        decimal_builder.append_value(-i as i128).unwrap();
    }
    let array = decimal_builder.finish();
    let schema = Schema::new(vec![Field::new("c1", array.data_type().clone(), true)]);
    RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
}

/// Return  record batch with all of the supported timestamp types
/// values
///
/// Columns are named:
/// "nanos" --> TimestampNanosecondArray
/// "micros" --> TimestampMicrosecondArray
/// "millis" --> TimestampMillisecondArray
/// "secs" --> TimestampSecondArray
/// "names" --> StringArray
pub fn make_timestamps() -> RecordBatch {
    let ts_strings = vec![
        Some("2018-11-13T17:11:10.011375885995"),
        Some("2011-12-13T11:13:10.12345"),
        None,
        Some("2021-1-1T05:11:10.432"),
    ];

    let ts_nanos = ts_strings
        .into_iter()
        .map(|t| {
            t.map(|t| {
                t.parse::<chrono::NaiveDateTime>()
                    .unwrap()
                    .timestamp_nanos()
            })
        })
        .collect::<Vec<_>>();

    let ts_micros = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000))
        .collect::<Vec<_>>();

    let ts_millis = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000))
        .collect::<Vec<_>>();

    let ts_secs = ts_nanos
        .iter()
        .map(|t| t.as_ref().map(|ts_nanos| ts_nanos / 1000000000))
        .collect::<Vec<_>>();

    let names = ts_nanos
        .iter()
        .enumerate()
        .map(|(i, _)| format!("Row {}", i))
        .collect::<Vec<_>>();

    let arr_nanos = TimestampNanosecondArray::from_opt_vec(ts_nanos, None);
    let arr_micros = TimestampMicrosecondArray::from_opt_vec(ts_micros, None);
    let arr_millis = TimestampMillisecondArray::from_opt_vec(ts_millis, None);
    let arr_secs = TimestampSecondArray::from_opt_vec(ts_secs, None);

    let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
    let arr_names = StringArray::from(names);

    let schema = Schema::new(vec![
        Field::new("nanos", arr_nanos.data_type().clone(), true),
        Field::new("micros", arr_micros.data_type().clone(), true),
        Field::new("millis", arr_millis.data_type().clone(), true),
        Field::new("secs", arr_secs.data_type().clone(), true),
        Field::new("name", arr_names.data_type().clone(), true),
    ]);
    let schema = Arc::new(schema);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arr_nanos),
            Arc::new(arr_micros),
            Arc::new(arr_millis),
            Arc::new(arr_secs),
            Arc::new(arr_names),
        ],
    )
    .unwrap()
}

/// Asserts that given future is pending.
pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
    let waker = futures::task::noop_waker();
    let mut cx = futures::task::Context::from_waker(&waker);
    let poll = fut.poll_unpin(&mut cx);

    assert!(poll.is_pending());
}

pub mod exec;
pub mod object_store;
pub mod user_defined;
pub mod variable;
