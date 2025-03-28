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

//! Execution plans that read file formats

mod arrow_file;
pub mod csv;
pub mod json;

#[cfg(feature = "parquet")]
pub mod parquet;

#[cfg(feature = "avro")]
pub mod avro;

#[allow(deprecated)]
#[cfg(feature = "avro")]
pub use avro::{AvroExec, AvroSource};

#[cfg(feature = "parquet")]
pub use datafusion_datasource_parquet::source::ParquetSource;
#[cfg(feature = "parquet")]
#[allow(deprecated)]
pub use datafusion_datasource_parquet::{
    ParquetExec, ParquetExecBuilder, ParquetFileMetrics, ParquetFileReaderFactory,
};

#[allow(deprecated)]
pub use arrow_file::ArrowExec;
pub use arrow_file::ArrowSource;

#[allow(deprecated)]
pub use json::NdJsonExec;

pub use json::{JsonOpener, JsonSource};

#[allow(deprecated)]
pub use csv::{CsvExec, CsvExecBuilder};

pub use csv::{CsvOpener, CsvSource};
pub use datafusion_datasource::file::FileSource;
pub use datafusion_datasource::file_groups::FileGroup;
pub use datafusion_datasource::file_groups::FileGroupPartitioner;
pub use datafusion_datasource::file_meta::FileMeta;
pub use datafusion_datasource::file_scan_config::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
    FileScanConfigBuilder,
};
pub use datafusion_datasource::file_sink_config::*;

pub use datafusion_datasource::file_stream::{
    FileOpenFuture, FileOpener, FileStream, OnError,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        cast::AsArray,
        types::{Float32Type, Float64Type, UInt32Type},
        BinaryArray, BooleanArray, Float32Array, Int32Array, Int64Array, RecordBatch,
        StringArray, UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SchemaRef;

    use crate::datasource::schema_adapter::{
        DefaultSchemaAdapterFactory, SchemaAdapterFactory,
    };

    #[test]
    fn schema_mapping_map_batch() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt32, true),
            Field::new("c3", DataType::Float64, true),
        ]));

        let adapter = DefaultSchemaAdapterFactory
            .create(table_schema.clone(), table_schema.clone());

        let file_schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::UInt64, true),
            Field::new("c3", DataType::Float32, true),
        ]);

        let (mapping, _) = adapter.map_schema(&file_schema).expect("map schema failed");

        let c1 = StringArray::from(vec!["hello", "world"]);
        let c2 = UInt64Array::from(vec![9_u64, 5_u64]);
        let c3 = Float32Array::from(vec![2.0_f32, 7.0_f32]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![Arc::new(c1), Arc::new(c2), Arc::new(c3)],
        )
        .unwrap();

        let mapped_batch = mapping.map_batch(batch).unwrap();

        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 3);
        assert_eq!(mapped_batch.num_rows(), 2);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<UInt32Type>();
        let c3 = mapped_batch.column(2).as_primitive::<Float64Type>();

        assert_eq!(c1.value(0), "hello");
        assert_eq!(c1.value(1), "world");
        assert_eq!(c2.value(0), 9_u32);
        assert_eq!(c2.value(1), 5_u32);
        assert_eq!(c3.value(0), 2.0_f64);
        assert_eq!(c3.value(1), 7.0_f64);
    }

    #[test]
    fn schema_adapter_map_schema_with_projection() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("c0", DataType::Utf8, true),
            Field::new("c1", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Int32, true),
            Field::new("c4", DataType::Float32, true),
        ]));

        let file_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("c1", DataType::Boolean, true),
            Field::new("c2", DataType::Float32, true),
            Field::new("c3", DataType::Binary, true),
            Field::new("c4", DataType::Int64, true),
        ]);

        let indices = vec![1, 2, 4];
        let schema = SchemaRef::from(table_schema.project(&indices).unwrap());
        let adapter = DefaultSchemaAdapterFactory.create(schema, table_schema.clone());
        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let c1 = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let c2 = Float32Array::from(vec![Some(2.0_f32), Some(7.0_f32), Some(3.0_f32)]);
        let c3 = BinaryArray::from_opt_vec(vec![
            Some(b"hallo"),
            Some(b"danke"),
            Some(b"super"),
        ]);
        let c4 = Int64Array::from(vec![1, 2, 3]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema),
            vec![
                Arc::new(id),
                Arc::new(c1),
                Arc::new(c2),
                Arc::new(c3),
                Arc::new(c4),
            ],
        )
        .unwrap();
        let rows_num = batch.num_rows();
        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        assert_eq!(
            mapped_batch.schema(),
            Arc::new(table_schema.project(&indices).unwrap())
        );
        assert_eq!(mapped_batch.num_columns(), indices.len());
        assert_eq!(mapped_batch.num_rows(), rows_num);

        let c1 = mapped_batch.column(0).as_string::<i32>();
        let c2 = mapped_batch.column(1).as_primitive::<Float64Type>();
        let c4 = mapped_batch.column(2).as_primitive::<Float32Type>();

        assert_eq!(c1.value(0), "true");
        assert_eq!(c1.value(1), "false");
        assert_eq!(c1.value(2), "true");

        assert_eq!(c2.value(0), 2.0_f64);
        assert_eq!(c2.value(1), 7.0_f64);
        assert_eq!(c2.value(2), 3.0_f64);

        assert_eq!(c4.value(0), 1.0_f32);
        assert_eq!(c4.value(1), 2.0_f32);
        assert_eq!(c4.value(2), 3.0_f32);
    }
}
