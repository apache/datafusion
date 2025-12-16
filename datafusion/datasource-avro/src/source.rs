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

//! Execution plan for reading line-delimited Avro files

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_avro::reader::{Reader, ReaderBuilder};
use arrow_avro::schema::{AvroSchema, SCHEMA_METADATA_KEY};
use datafusion_common::error::Result;
use datafusion_common::DataFusionError;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_datasource::schema_adapter::SchemaAdapterFactory;
use datafusion_datasource::TableSchema;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExprs;

use crate::read_avro_schema_from_reader;
use object_store::ObjectStore;
use serde_json::Value;

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone)]
pub struct AvroSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
    schema_adapter_factory: Option<Arc<dyn SchemaAdapterFactory>>,
}

impl AvroSource {
    /// Initialize an AvroSource with the provided schema
    pub fn new(table_schema: impl Into<TableSchema>) -> Self {
        let table_schema = table_schema.into();
        Self {
            projection: SplitProjection::unprojected(&table_schema),
            table_schema,
            batch_size: None,
            metrics: ExecutionPlanMetricsSet::new(),
            schema_adapter_factory: None,
        }
    }

    fn open<R: std::io::BufRead>(&self, reader: R) -> Result<Reader<R>> {
        // TODO: Once `ReaderBuilder::with_projection` is available, we should use it instead.
        //  This should be an easy change. We'd simply need to:
        //      1. Use the full file schema to generate the reader `AvroSchema`.
        //      2. Pass `&self.projection` into `ReaderBuilder::with_projection`.
        //      3. Remove the `build_projected_reader_schema` methods.
        ReaderBuilder::new()
            .with_reader_schema(self.build_projected_reader_schema()?)
            .with_batch_size(self.batch_size.expect("Batch size must set before open"))
            .build(reader)
            .map_err(Into::into)
    }

    fn build_projected_reader_schema(&self) -> Result<AvroSchema> {
        let file_schema = self.table_schema.file_schema().as_ref();
        // Fast path: no projection. If we have the original writer schema JSON
        // in metadata, just reuse it as-is without parsing.
        if self.projection.file_indices.is_empty() {
            return if let Some(avro_json) =
                file_schema.metadata().get(SCHEMA_METADATA_KEY)
            {
                Ok(AvroSchema::new(avro_json.clone()))
            } else {
                // Fall back to deriving Avro from the full Arrow file schema, should be ok
                // if not using projection.
                Ok(AvroSchema::try_from(file_schema)
                    .map_err(Into::<DataFusionError>::into)?)
            };
        }
        // Use the writer Avro schema JSON tagged upstream to build a projected reader schema
        match file_schema.metadata().get(SCHEMA_METADATA_KEY) {
            Some(avro_json) => {
                let mut schema_json: Value =
                    serde_json::from_str(avro_json).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to parse Avro schema JSON from metadata: {e}"
                        ))
                    })?;
                let obj = schema_json.as_object_mut().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Top-level Avro schema JSON must be an object".to_string(),
                    )
                })?;
                let fields_val = obj.get_mut("fields").ok_or_else(|| {
                    DataFusionError::Execution(
                        "Top-level Avro schema JSON must contain a `fields` array"
                            .to_string(),
                    )
                })?;
                let fields_arr = fields_val.as_array_mut().ok_or_else(|| {
                    DataFusionError::Execution(
                        "Top-level Avro schema `fields` must be an array".to_string(),
                    )
                })?;
                // Move existing fields out so we can rebuild them in projected order.
                let original_fields = std::mem::take(fields_arr);
                let mut by_name: HashMap<String, Value> =
                    HashMap::with_capacity(original_fields.len());
                for field in original_fields {
                    if let Some(name) = field.get("name").and_then(|v| v.as_str()) {
                        by_name.insert(name.to_string(), field);
                    }
                }
                // Rebuild `fields` in the same order as the projected Arrow schema.
                let projection = self.projection.file_indices.as_ref();
                let projected_schema = file_schema.project(projection)?;
                let mut projected_fields =
                    Vec::with_capacity(projected_schema.fields().len());
                for arrow_field in projected_schema.fields() {
                    let name = arrow_field.name();
                    let field = by_name.remove(name).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "Projected field `{name}` not found in Avro writer schema"
                        ))
                    })?;
                    projected_fields.push(field);
                }
                *fields_val = Value::Array(projected_fields);
                let projected_json =
                    serde_json::to_string(&schema_json).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to serialize projected Avro schema JSON: {e}"
                        ))
                    })?;
                Ok(AvroSchema::new(projected_json))
            }
            None => Err(DataFusionError::Execution(format!(
                "Avro schema metadata ({SCHEMA_METADATA_KEY}) is missing from file schema, but is required for projection"
            ))),
        }
    }
}

impl FileSource for AvroSource {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        _base_config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        let mut opener = Arc::new(private::AvroOpener {
            config: Arc::new(self.clone()),
            object_store,
        }) as Arc<dyn FileOpener>;
        opener = ProjectionOpener::try_new(
            self.projection.clone(),
            Arc::clone(&opener),
            self.table_schema.file_schema(),
        )?;
        Ok(opener)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        let mut conf = self.clone();
        conf.batch_size = Some(batch_size);
        Arc::new(conf)
    }

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>> {
        let mut source = self.clone();
        let new_projection = self.projection.source.try_merge(projection)?;
        let split_projection =
            SplitProjection::new(self.table_schema.file_schema(), &new_projection);
        source.projection = split_projection;
        Ok(Some(Arc::new(source)))
    }

    fn projection(&self) -> Option<&ProjectionExprs> {
        Some(&self.projection.source)
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn file_type(&self) -> &str {
        "avro"
    }

    fn with_schema_adapter_factory(
        &self,
        schema_adapter_factory: Arc<dyn SchemaAdapterFactory>,
    ) -> Result<Arc<dyn FileSource>> {
        Ok(Arc::new(Self {
            schema_adapter_factory: Some(schema_adapter_factory),
            ..self.clone()
        }))
    }

    fn schema_adapter_factory(&self) -> Option<Arc<dyn SchemaAdapterFactory>> {
        self.schema_adapter_factory.clone()
    }
}

mod private {
    use super::*;
    use std::io::BufReader;

    use bytes::Buf;
    use datafusion_datasource::{file_stream::FileOpenFuture, PartitionedFile};
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore};

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let object_store = Arc::clone(&self.object_store);
            let config = Arc::clone(&self.config);

            Ok(Box::pin(async move {
                // check if schema should be inferred
                let r = object_store
                    .get(&partitioned_file.object_meta.location)
                    .await?;
                let config = Arc::new(match r.payload {
                    GetResultPayload::File(mut file, _) => {
                        let schema = match config
                            .table_schema
                            .file_schema()
                            .metadata
                            .get(SCHEMA_METADATA_KEY)
                        {
                            Some(_) => Arc::clone(config.table_schema.file_schema()),
                            None => {
                                Arc::new(read_avro_schema_from_reader(&mut file).unwrap())
                            } // if not inferred, read schema from file
                        };
                        AvroSource {
                            table_schema: TableSchema::new(
                                schema,
                                config.table_schema.table_partition_cols().clone(),
                            ),
                            batch_size: config.batch_size,
                            projection: config.projection.clone(),
                            metrics: config.metrics.clone(),
                            schema_adapter_factory: config.schema_adapter_factory.clone(),
                        }
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let schema = match config
                            .table_schema
                            .file_schema()
                            .metadata
                            .get(SCHEMA_METADATA_KEY)
                        {
                            Some(_) => Arc::clone(config.table_schema.file_schema()),
                            None => Arc::new(
                                read_avro_schema_from_reader(&mut bytes.reader())
                                    .unwrap(),
                            ), // if not inferred, read schema from file
                        };
                        AvroSource {
                            table_schema: TableSchema::new(
                                schema,
                                config.table_schema.table_partition_cols().clone(),
                            ),
                            batch_size: config.batch_size,
                            projection: config.projection.clone(),
                            metrics: config.metrics.clone(),
                            schema_adapter_factory: config.schema_adapter_factory.clone(),
                        }
                    }
                });

                let r = object_store
                    .get(&partitioned_file.object_meta.location)
                    .await?;
                match r.payload {
                    GetResultPayload::File(file, _) => {
                        let reader = config.open(BufReader::new(file))?;
                        Ok(futures::stream::iter(reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        let reader = config.open(BufReader::new(bytes.reader()))?;
                        Ok(futures::stream::iter(reader)
                            .map(|r| r.map_err(Into::into))
                            .boxed())
                    }
                }
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::array::{
        BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::{DataType, Field};
    use arrow::datatypes::{Schema, TimeUnit};
    use std::fs::File;
    use std::io::BufReader;

    fn build_reader(name: &'_ str, schema: Option<&Schema>) -> Reader<BufReader<File>> {
        let testdata = datafusion_common::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/{name}");
        let mut builder = ReaderBuilder::new().with_batch_size(64);
        if let Some(schema) = schema {
            builder = builder.with_reader_schema(AvroSchema::try_from(schema).unwrap());
        }
        builder
            .build(BufReader::new(File::open(filename).unwrap()))
            .unwrap()
    }

    fn get_col<'a, T: 'static>(
        batch: &'a RecordBatch,
        col: (usize, &Field),
    ) -> Option<&'a T> {
        batch.column(col.0).as_any().downcast_ref::<T>()
    }

    #[test]
    fn test_avro_basic() {
        let mut reader = build_reader("alltypes_dictionary.avro", None);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(11, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        let id = schema.column_with_name("id").unwrap();
        assert_eq!(0, id.0);
        assert_eq!(&DataType::Int32, id.1.data_type());
        let col = get_col::<Int32Array>(&batch, id).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let bool_col = schema.column_with_name("bool_col").unwrap();
        assert_eq!(1, bool_col.0);
        assert_eq!(&DataType::Boolean, bool_col.1.data_type());
        let col = get_col::<BooleanArray>(&batch, bool_col).unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
        let tinyint_col = schema.column_with_name("tinyint_col").unwrap();
        assert_eq!(2, tinyint_col.0);
        assert_eq!(&DataType::Int32, tinyint_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, tinyint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let smallint_col = schema.column_with_name("smallint_col").unwrap();
        assert_eq!(3, smallint_col.0);
        assert_eq!(&DataType::Int32, smallint_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, smallint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let int_col = schema.column_with_name("int_col").unwrap();
        assert_eq!(4, int_col.0);
        let col = get_col::<Int32Array>(&batch, int_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        assert_eq!(&DataType::Int32, int_col.1.data_type());
        let col = get_col::<Int32Array>(&batch, int_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1, col.value(1));
        let bigint_col = schema.column_with_name("bigint_col").unwrap();
        assert_eq!(5, bigint_col.0);
        let col = get_col::<Int64Array>(&batch, bigint_col).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(10, col.value(1));
        assert_eq!(&DataType::Int64, bigint_col.1.data_type());
        let float_col = schema.column_with_name("float_col").unwrap();
        assert_eq!(6, float_col.0);
        let col = get_col::<Float32Array>(&batch, float_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(1.1, col.value(1));
        assert_eq!(&DataType::Float32, float_col.1.data_type());
        let col = get_col::<Float32Array>(&batch, float_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(1.1, col.value(1));
        let double_col = schema.column_with_name("double_col").unwrap();
        assert_eq!(7, double_col.0);
        assert_eq!(&DataType::Float64, double_col.1.data_type());
        let col = get_col::<Float64Array>(&batch, double_col).unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(10.1, col.value(1));
        let date_string_col = schema.column_with_name("date_string_col").unwrap();
        assert_eq!(8, date_string_col.0);
        assert_eq!(&DataType::Binary, date_string_col.1.data_type());
        let col = get_col::<BinaryArray>(&batch, date_string_col).unwrap();
        assert_eq!("01/01/09".as_bytes(), col.value(0));
        assert_eq!("01/01/09".as_bytes(), col.value(1));
        let string_col = schema.column_with_name("string_col").unwrap();
        assert_eq!(9, string_col.0);
        assert_eq!(&DataType::Binary, string_col.1.data_type());
        let col = get_col::<BinaryArray>(&batch, string_col).unwrap();
        assert_eq!("0".as_bytes(), col.value(0));
        assert_eq!("1".as_bytes(), col.value(1));
        let timestamp_col = schema.column_with_name("timestamp_col").unwrap();
        assert_eq!(10, timestamp_col.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            timestamp_col.1.data_type()
        );
        let col = get_col::<TimestampMicrosecondArray>(&batch, timestamp_col).unwrap();
        assert_eq!(1230768000000000, col.value(0));
        assert_eq!(1230768060000000, col.value(1));
    }

    #[test]
    fn test_avro_with_projection() {
        // Test projection to filter and reorder columns
        let projected_schema = Schema::new(vec![
            Field::new("string_col", DataType::Binary, false),
            Field::new("double_col", DataType::Float64, false),
            Field::new("bool_col", DataType::Boolean, false),
        ]);

        let mut reader =
            build_reader("alltypes_dictionary.avro", Some(&projected_schema));
        let batch = reader.next().unwrap().unwrap();

        // Only 3 columns should be present (not all 11)
        assert_eq!(3, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let schema = reader.schema();
        let batch_schema = batch.schema();
        assert_eq!(schema, batch_schema);

        // Verify columns are in the order specified in projection
        // First column should be string_col (was at index 9 in original)
        assert_eq!("string_col", schema.field(0).name());
        assert_eq!(&DataType::Binary, schema.field(0).data_type());
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!("0".as_bytes(), col.value(0));
        assert_eq!("1".as_bytes(), col.value(1));

        // Second column should be double_col (was at index 7 in original)
        assert_eq!("double_col", schema.field(1).name());
        assert_eq!(&DataType::Float64, schema.field(1).data_type());
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(0.0, col.value(0));
        assert_eq!(10.1, col.value(1));

        // Third column should be bool_col (was at index 1 in original)
        assert_eq!("bool_col", schema.field(2).name());
        assert_eq!(&DataType::Boolean, schema.field(2).data_type());
        let col = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(col.value(0));
        assert!(!col.value(1));
    }
}
