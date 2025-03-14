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
pub use datafusion_datasource::file_groups::FileGroupPartitioner;
pub use datafusion_datasource::file_meta::FileMeta;
pub use datafusion_datasource::file_scan_config::{
    wrap_partition_type_in_dict, wrap_partition_value_in_dict, FileScanConfig,
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

        let adapter = DefaultSchemaAdapterFactory::default()
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
        let adapter =
            DefaultSchemaAdapterFactory::default().create(schema, table_schema.clone());
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

    #[test]
    fn schema_adapter_with_column_generator() {
        use crate::datasource::schema_adapter::{
            DefaultSchemaAdapterFactory, MissingColumnGenerator, MissingColumnGeneratorFactory,
        };
        use arrow::array::{ArrayRef, Int32Array};
        use arrow::datatypes::Int32Type;
        use std::fmt;

        // A simple generator that produces a constant value
        #[derive(Debug)]
        struct ConstantGenerator(Int32Array);

        impl MissingColumnGenerator for ConstantGenerator {
            fn generate(&self, _batch: RecordBatch) -> datafusion_common::Result<ArrayRef> {
                Ok(Arc::new(self.0.clone()))
            }

            fn dependencies(&self) -> Vec<String> {
                vec![]
            }
        }

        // A factory that produces a constant generator for a specific field
        #[derive(Debug)]
        struct ConstantGeneratorFactory {
            field_name: String,
            value: i32,
        }

        impl fmt::Display for ConstantGeneratorFactory {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "ConstantGeneratorFactory({}={})", self.field_name, self.value)
            }
        }

        impl MissingColumnGeneratorFactory for ConstantGeneratorFactory {
            fn create(
                &self,
                field: &Field,
                _file_schema: &Schema,
            ) -> Option<Arc<dyn MissingColumnGenerator + Send + Sync>> {
                if field.name() == &self.field_name && field.data_type() == &DataType::Int32 {
                    let array = Int32Array::from(vec![self.value; 3]);
                    Some(Arc::new(ConstantGenerator(array)))
                } else {
                    None
                }
            }
        }

        // A generator that depends on another column
        #[derive(Debug)]
        struct MultiplyByTwoGenerator {
            dependency: String,
        }

        impl MissingColumnGenerator for MultiplyByTwoGenerator {
            fn generate(&self, batch: RecordBatch) -> datafusion_common::Result<ArrayRef> {
                let idx = batch
                    .schema()
                    .index_of(&self.dependency)
                    .expect("dependency should exist");
                let col = batch.column(idx);
                let col = col.as_primitive::<Int32Type>();

                let result: Int32Array = col.iter().map(|v| v.map(|x| x * 2)).collect();
                Ok(Arc::new(result))
            }

            fn dependencies(&self) -> Vec<String> {
                vec![self.dependency.clone()]
            }
        }

        #[derive(Debug)]
        struct MultiplyByTwoGeneratorFactory;

        impl fmt::Display for MultiplyByTwoGeneratorFactory {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "MultiplyByTwoGeneratorFactory")
            }
        }

        impl MissingColumnGeneratorFactory for MultiplyByTwoGeneratorFactory {
            fn create(
                &self,
                field: &Field,
                file_schema: &Schema,
            ) -> Option<Arc<dyn MissingColumnGenerator + Send + Sync>> {
                if field.name() == "doubled_id" && field.data_type() == &DataType::Int32 {
                    // Look for id column to use as our dependency
                    if file_schema.column_with_name("id").is_some() {
                        Some(Arc::new(MultiplyByTwoGenerator {
                            dependency: "id".to_string(),
                        }))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }

        // Test with a constant generator
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("c1", DataType::Boolean, true),
            Field::new("missing_column", DataType::Int32, true),
        ]));

        let file_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("c1", DataType::Boolean, true),
        ]);

        // Create a factory that will generate a constant value for the missing column
        let generator_factory = Arc::new(ConstantGeneratorFactory {
            field_name: "missing_column".to_string(),
            value: 42,
        });

        let adapter = DefaultSchemaAdapterFactory::default()
            .with_column_generator(generator_factory)
            .create(table_schema.clone(), table_schema.clone());

        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        // Create a batch to test
        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let c1 = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema.clone()),
            vec![Arc::new(id), Arc::new(c1)],
        )
        .unwrap();

        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        // Verify the result
        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 3);
        assert_eq!(mapped_batch.num_rows(), 3);

        // Check the missing column was generated with constant value
        let missing_col = mapped_batch.column(2).as_primitive::<Int32Type>();
        assert_eq!(missing_col.value(0), 42);
        assert_eq!(missing_col.value(1), 42);
        assert_eq!(missing_col.value(2), 42);

        // Test with a generator that depends on another column
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("doubled_id", DataType::Int32, true),
        ]));

        let file_schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

        // Set up the generator factory
        let adapter = DefaultSchemaAdapterFactory::default()
            .with_column_generator(Arc::new(MultiplyByTwoGeneratorFactory))
            .create(table_schema.clone(), table_schema.clone());

        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        // Create a batch with just an id column
        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema.clone()),
            vec![Arc::new(id)],
        )
        .unwrap();

        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        // Verify the result
        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 2);
        assert_eq!(mapped_batch.num_rows(), 3);

        // Check the doubled_id column was generated correctly
        let id_col = mapped_batch.column(0).as_primitive::<Int32Type>();
        let doubled_col = mapped_batch.column(1).as_primitive::<Int32Type>();

        assert_eq!(doubled_col.value(0), id_col.value(0) * 2);
        assert_eq!(doubled_col.value(1), id_col.value(1) * 2);
        assert_eq!(doubled_col.value(2), id_col.value(2) * 2);
    }

    #[test]
    fn schema_adapter_with_multiple_generators() {
        use crate::datasource::schema_adapter::{
            DefaultSchemaAdapterFactory, MissingColumnGenerator, MissingColumnGeneratorFactory,
        };
        use arrow::array::{ArrayRef, Int32Array, StringArray};
        use arrow::datatypes::Int32Type;
        use std::fmt;

        // A generator for creating a description from an id
        #[derive(Debug)]
        struct IdToDescriptionGenerator;

        impl MissingColumnGenerator for IdToDescriptionGenerator {
            fn generate(&self, batch: RecordBatch) -> datafusion_common::Result<ArrayRef> {
                let idx = batch.schema().index_of("id").expect("id should exist");
                let col = batch.column(idx);
                let col = col.as_primitive::<Int32Type>();

                let result: StringArray = col
                    .iter()
                    .map(|v| {
                        v.map(|id| match id {
                            1 => "Product One",
                            2 => "Product Two",
                            3 => "Product Three",
                            _ => "Unknown Product",
                        })
                    })
                    .collect();
                Ok(Arc::new(result))
            }

            fn dependencies(&self) -> Vec<String> {
                vec!["id".to_string()]
            }
        }

        #[derive(Debug)]
        struct IdToDescriptionGeneratorFactory;

        impl fmt::Display for IdToDescriptionGeneratorFactory {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "IdToDescriptionGeneratorFactory")
            }
        }

        impl MissingColumnGeneratorFactory for IdToDescriptionGeneratorFactory {
            fn create(
                &self,
                field: &Field,
                file_schema: &Schema,
            ) -> Option<Arc<dyn MissingColumnGenerator + Send + Sync>> {
                if field.name() == "description" && field.data_type() == &DataType::Utf8 {
                    if file_schema.column_with_name("id").is_some() {
                        Some(Arc::new(IdToDescriptionGenerator))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }

        // A generator for creating a score column with constant value
        #[derive(Debug)]
        struct ScoreGenerator(i32);

        impl MissingColumnGenerator for ScoreGenerator {
            fn generate(&self, batch: RecordBatch) -> datafusion_common::Result<ArrayRef> {
                let len = batch.num_rows();
                Ok(Arc::new(Int32Array::from(vec![self.0; len])))
            }

            fn dependencies(&self) -> Vec<String> {
                vec![]
            }
        }

        #[derive(Debug)]
        struct ScoreGeneratorFactory;

        impl fmt::Display for ScoreGeneratorFactory {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "ScoreGeneratorFactory")
            }
        }

        impl MissingColumnGeneratorFactory for ScoreGeneratorFactory {
            fn create(
                &self,
                field: &Field,
                _file_schema: &Schema,
            ) -> Option<Arc<dyn MissingColumnGenerator + Send + Sync>> {
                if field.name() == "score" && field.data_type() == &DataType::Int32 {
                    Some(Arc::new(ScoreGenerator(100)))
                } else {
                    None
                }
            }
        }

        // Set up the schema with multiple missing columns
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("score", DataType::Int32, true),
        ]));

        let file_schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);

        // Create factory that will generate multiple missing columns
        let adapter = DefaultSchemaAdapterFactory::default()
            .with_column_generator(Arc::new(IdToDescriptionGeneratorFactory))
            .with_column_generator(Arc::new(ScoreGeneratorFactory))
            .create(table_schema.clone(), table_schema.clone());

        let (mapping, projection) = adapter.map_schema(&file_schema).unwrap();

        // Create a batch to test
        let id = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let batch = RecordBatch::try_new(
            Arc::new(file_schema.clone()),
            vec![Arc::new(id)],
        )
        .unwrap();

        let projected = batch.project(&projection).unwrap();
        let mapped_batch = mapping.map_batch(projected).unwrap();

        // Verify the result
        assert_eq!(mapped_batch.schema(), table_schema);
        assert_eq!(mapped_batch.num_columns(), 3);
        assert_eq!(mapped_batch.num_rows(), 3);

        // Check both missing columns were generated correctly
        let id_col = mapped_batch.column(0).as_primitive::<Int32Type>();
        let description_col = mapped_batch.column(1).as_string::<i32>();
        let score_col = mapped_batch.column(2).as_primitive::<Int32Type>();

        // Verify id column
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        // Verify description column generated from id
        assert_eq!(description_col.value(0), "Product One");
        assert_eq!(description_col.value(1), "Product Two");
        assert_eq!(description_col.value(2), "Product Three");

        // Verify score column has constant value
        assert_eq!(score_col.value(0), 100);
        assert_eq!(score_col.value(1), 100);
        assert_eq!(score_col.value(2), 100);
    }
}
