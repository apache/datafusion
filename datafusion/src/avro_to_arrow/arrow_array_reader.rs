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

//! Avro to Arrow array readers

use crate::arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::physical_plan::coalesce_batches::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::io::avro::read::Reader as AvroReader;
use arrow::io::avro::{read, Compression};
use std::io::Read;

pub struct AvroBatchReader<R: Read> {
    reader: AvroReader<R>,
    schema: SchemaRef,
}

impl<'a, R: Read> AvroBatchReader<R> {
    pub fn try_new(
        reader: R,
        schema: SchemaRef,
        avro_schemas: Vec<avro_schema::Schema>,
        codec: Option<Compression>,
        file_marker: [u8; 16],
    ) -> Result<Self> {
        let reader = AvroReader::new(
            read::Decompressor::new(
                read::BlockStreamIterator::new(reader, file_marker),
                codec,
            ),
            avro_schemas,
            schema.clone(),
        );
        Ok(Self { reader, schema })
    }

    /// Read the next batch of records
    #[allow(clippy::should_implement_trait)]
    pub fn next_batch(&mut self, batch_size: usize) -> ArrowResult<Option<RecordBatch>> {
        if let Some(Ok(batch)) = self.reader.next() {
            let mut batch = batch;
            'batch: while batch.num_rows() < batch_size {
                if let Some(Ok(next_batch)) = self.reader.next() {
                    let num_rows = batch.num_rows() + next_batch.num_rows();
                    batch = concat_batches(&self.schema, &[batch, next_batch], num_rows)?
                } else {
                    break 'batch;
                }
            }
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::arrow::array::Array;
    use crate::arrow::datatypes::{Field, TimeUnit};
    use crate::avro_to_arrow::{Reader, ReaderBuilder};
    use arrow::array::{Int32Array, Int64Array, ListArray};
    use arrow::datatypes::DataType;
    use std::fs::File;

    fn build_reader(name: &str, batch_size: usize) -> Reader<File> {
        let testdata = crate::test_util::arrow_test_data();
        let filename = format!("{}/avro/{}", testdata, name);
        let builder = ReaderBuilder::new()
            .read_schema()
            .with_batch_size(batch_size);
        builder.build(File::open(filename).unwrap()).unwrap()
    }

    // TODO: Fixed, Enum, Dictionary

    #[test]
    fn test_time_avro_milliseconds() {
        let mut reader = build_reader("alltypes_plain.avro", 10);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(8, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema().clone();
        assert_eq!(schema, batch_schema);

        let timestamp_col = schema.column_with_name("timestamp_col").unwrap();
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("00:00".to_string())),
            timestamp_col.1.data_type()
        );
        let timestamp_array = batch
            .column(timestamp_col.0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..timestamp_array.len() {
            assert!(timestamp_array.is_valid(i));
        }
        assert_eq!(1235865600000000, timestamp_array.value(0));
        assert_eq!(1235865660000000, timestamp_array.value(1));
        assert_eq!(1238544000000000, timestamp_array.value(2));
        assert_eq!(1238544060000000, timestamp_array.value(3));
        assert_eq!(1233446400000000, timestamp_array.value(4));
        assert_eq!(1233446460000000, timestamp_array.value(5));
        assert_eq!(1230768000000000, timestamp_array.value(6));
        assert_eq!(1230768060000000, timestamp_array.value(7));
    }

    #[test]
    fn test_avro_read_list() {
        let mut reader = build_reader("list_columns.avro", 3);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("int64_list").unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
        let a_array = batch
            .column(col_id_index)
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        assert_eq!(
            *a_array.data_type(),
            DataType::List(Box::new(Field::new("item", DataType::Int64, true)))
        );
        let array = a_array.value(0);
        assert_eq!(*array.data_type(), DataType::Int64);

        assert_eq!(
            6,
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        );
    }
    #[test]
    fn test_avro_read_nested_list() {
        let mut reader = build_reader("nested_lists.snappy.avro", 3);
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_avro_iterator() {
        let reader = build_reader("alltypes_plain.avro", 5);
        let schema = reader.schema();
        let (col_id_index, _) = schema.column_with_name("id").unwrap();

        let mut sum_num_rows = 0;
        let mut num_batches = 0;
        let mut sum_id = 0;
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(11, batch.num_columns());
            sum_num_rows += batch.num_rows();
            num_batches += 1;
            let batch_schema = batch.schema().clone();
            assert_eq!(schema, batch_schema);
            let a_array = batch
                .column(col_id_index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            sum_id += (0..a_array.len()).map(|i| a_array.value(i)).sum::<i32>();
        }
        assert_eq!(8, sum_num_rows);
        assert_eq!(1, num_batches);
        assert_eq!(28, sum_id);
    }
}
