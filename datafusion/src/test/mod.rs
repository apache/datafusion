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
use crate::datasource::object_store::local::local_unpartitioned_file;
use crate::datasource::{MemTable, PartitionedFile, TableProvider};
use crate::error::Result;
use crate::from_slice::FromSlice;
use crate::logical_plan::{LogicalPlan, LogicalPlanBuilder};
use array::{Array, ArrayRef};
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
            Arc::new(array::Int32Array::from_slice(&[1])),
            Arc::new(array::StringArray::from_slice(&["a"])),
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

/// Asserts that given future is pending.
pub fn assert_is_pending<'a, T>(fut: &mut Pin<Box<dyn Future<Output = T> + Send + 'a>>) {
    let waker = futures::task::noop_waker();
    let mut cx = futures::task::Context::from_waker(&waker);
    let poll = fut.poll_unpin(&mut cx);

    assert!(poll.is_pending());
}

/// Create vector batches
pub fn create_vec_batches(schema: &Arc<Schema>, n: usize) -> Vec<RecordBatch> {
    let batch = create_batch(schema);
    let mut vec = Vec::with_capacity(n);
    for _ in 0..n {
        vec.push(batch.clone());
    }
    vec
}

/// Create batch
fn create_batch(schema: &Arc<Schema>) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]))],
    )
    .unwrap()
}

pub mod exec;
pub mod object_store;
pub mod user_defined;
pub mod variable;
