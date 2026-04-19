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

use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow_avro::reader::{Reader, ReaderBuilder};
use datafusion_common::error::Result;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_datasource::TableSchema;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::file_stream::FileOpener;
use datafusion_datasource::projection::{ProjectionOpener, SplitProjection};
use datafusion_physical_expr_adapter::BatchAdapterFactory;
use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_physical_plan::projection::ProjectionExprs;

use object_store::ObjectStore;

/// AvroSource holds the extra configuration that is necessary for opening avro files
#[derive(Clone)]
pub struct AvroSource {
    table_schema: TableSchema,
    batch_size: Option<usize>,
    projection: SplitProjection,
    metrics: ExecutionPlanMetricsSet,
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
        }
    }

    fn open<R: std::io::BufRead>(
        &self,
        reader: R,
        projection: Option<Vec<usize>>,
    ) -> Result<Reader<R>> {
        let mut builder = ReaderBuilder::new()
            .with_batch_size(self.batch_size.expect("Batch size must set before open"));
        if let Some(projection) = projection {
            builder = builder.with_projection(projection);
        }
        builder.build(reader).map_err(Into::into)
    }

    fn projected_file_schema(&self) -> SchemaRef {
        let file_schema = self.table_schema.file_schema();
        if self.projection.file_indices.is_empty() {
            return Arc::clone(file_schema);
        }

        Arc::new(Schema::new(
            self.projection
                .file_indices
                .iter()
                .map(|idx| file_schema.field(*idx).clone())
                .collect::<Vec<_>>(),
        ))
    }

    fn writer_projection_for_schema(
        &self,
        writer_schema: &Schema,
        target_schema: &Schema,
    ) -> Option<Vec<usize>> {
        // `arrow-avro` accepts projection ordinals against the file's writer schema,
        // while DataFusion plans projection against the logical table schema. Remap
        // projected column names to writer ordinals so reader-level pushdown still
        // preserves DataFusion's existing name-based projection semantics.
        let projection = target_schema
            .fields()
            .iter()
            .filter_map(|field| {
                writer_schema
                    .column_with_name(field.name())
                    .map(|(idx, _)| idx)
            })
            .collect::<Vec<_>>();

        let identity_projection = projection.len() == writer_schema.fields().len()
            && projection
                .iter()
                .enumerate()
                .all(|(idx, value)| idx == *value);

        (!identity_projection).then_some(projection)
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

    fn supports_repartitioning(&self) -> bool {
        // Avro OCF does not support safe byte-range splitting in this reader path.
        false
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &dyn datafusion_physical_plan::PhysicalExpr,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        // Visit projection expressions
        let mut tnr = TreeNodeRecursion::Continue;
        for proj_expr in &self.projection.source {
            tnr = tnr.visit_sibling(|| f(proj_expr.expr.as_ref()))?;
        }
        Ok(tnr)
    }
}

mod private {
    use super::*;
    use std::io::BufReader;
    use std::io::Seek;

    use bytes::Buf;
    use datafusion_datasource::{PartitionedFile, file_stream::FileOpenFuture};
    use futures::StreamExt;
    use object_store::{GetResultPayload, ObjectStore, ObjectStoreExt};

    pub struct AvroOpener {
        pub config: Arc<AvroSource>,
        pub object_store: Arc<dyn ObjectStore>,
    }

    impl FileOpener for AvroOpener {
        fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let object_store = Arc::clone(&self.object_store);
            let config = Arc::clone(&self.config);
            let projected_file_schema = config.projected_file_schema();

            Ok(Box::pin(async move {
                let r = object_store
                    .get(&partitioned_file.object_meta.location)
                    .await?;
                match r.payload {
                    GetResultPayload::File(mut file, _) => {
                        // Probe the writer schema first so logical projected columns can be
                        // translated to the writer-schema ordinals expected by `arrow-avro`.
                        let probe_reader =
                            config.open(BufReader::new(file.try_clone()?), None)?;
                        let writer_projection = config.writer_projection_for_schema(
                            probe_reader.schema().as_ref(),
                            projected_file_schema.as_ref(),
                        );
                        file.rewind()?;
                        let reader =
                            config.open(BufReader::new(file), writer_projection)?;
                        let batch_adapter =
                            BatchAdapterFactory::new(Arc::clone(&projected_file_schema))
                                .make_adapter(&reader.schema())?;
                        Ok(futures::stream::iter(reader)
                            .map(move |r| {
                                r.map_err(Into::into)
                                    .and_then(|batch| batch_adapter.adapt_batch(&batch))
                            })
                            .boxed())
                    }
                    GetResultPayload::Stream(_) => {
                        let bytes = r.bytes().await?;
                        // As above, inspect the writer schema before constructing the real
                        // reader so `with_projection` can use writer-schema ordinals.
                        let probe_reader =
                            config.open(BufReader::new(bytes.clone().reader()), None)?;
                        let writer_projection = config.writer_projection_for_schema(
                            probe_reader.schema().as_ref(),
                            projected_file_schema.as_ref(),
                        );
                        let reader = config
                            .open(BufReader::new(bytes.reader()), writer_projection)?;
                        let batch_adapter =
                            BatchAdapterFactory::new(Arc::clone(&projected_file_schema))
                                .make_adapter(&reader.schema())?;
                        Ok(futures::stream::iter(reader)
                            .map(move |r| {
                                r.map_err(Into::into)
                                    .and_then(|batch| batch_adapter.adapt_batch(&batch))
                            })
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
        TimestampMicrosecondArray, TimestampMillisecondArray,
    };
    use arrow::datatypes::TimeUnit;
    use arrow::datatypes::{DataType, Field};
    use std::fs::File;
    use std::io::BufReader;

    fn build_reader(
        name: &'_ str,
        projection: Option<Vec<usize>>,
    ) -> Reader<BufReader<File>> {
        let testdata = datafusion_common::test_util::arrow_test_data();
        let filename = format!("{testdata}/avro/{name}");
        let mut builder = ReaderBuilder::new().with_batch_size(64);
        if let Some(proj) = projection {
            builder = builder.with_projection(proj);
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
        let projection = vec![9, 7, 1]; // string_col, double_col, bool_col

        let mut reader = build_reader("alltypes_dictionary.avro", Some(projection));
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

    #[test]
    fn test_avro_timestamp_logical_types() {
        let mut reader = build_reader("timestamp_logical_types.avro", None);
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(7, batch.num_columns());
        assert_eq!(2, batch.num_rows());

        let schema = reader.schema();
        let ts_millis = schema.column_with_name("ts_millis").unwrap();
        assert_eq!(1, ts_millis.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
            ts_millis.1.data_type()
        );
        let col = get_col::<TimestampMillisecondArray>(&batch, ts_millis).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000, col.value(1));

        let ts_micros = schema.column_with_name("ts_micros").unwrap();
        assert_eq!(2, ts_micros.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into())),
            ts_micros.1.data_type()
        );
        let col = get_col::<TimestampMicrosecondArray>(&batch, ts_micros).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000_000, col.value(1));

        let ts_nanos = schema.column_with_name("ts_nanos").unwrap();
        assert_eq!(3, ts_nanos.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
            ts_nanos.1.data_type()
        );
        let col = get_col::<TimestampNanosecondArray>(&batch, ts_nanos).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000_000_000, col.value(1));

        let local_ts_millis = schema.column_with_name("local_ts_millis").unwrap();
        assert_eq!(4, local_ts_millis.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Millisecond, None),
            local_ts_millis.1.data_type()
        );
        let col = get_col::<TimestampMillisecondArray>(&batch, local_ts_millis).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000, col.value(1));

        let local_ts_micros = schema.column_with_name("local_ts_micros").unwrap();
        assert_eq!(5, local_ts_micros.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Microsecond, None),
            local_ts_micros.1.data_type()
        );
        let col = get_col::<TimestampMicrosecondArray>(&batch, local_ts_micros).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000_000, col.value(1));

        let local_ts_nanos = schema.column_with_name("local_ts_nanos").unwrap();
        assert_eq!(6, local_ts_nanos.0);
        assert_eq!(
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
            local_ts_nanos.1.data_type()
        );
        let col = get_col::<TimestampNanosecondArray>(&batch, local_ts_nanos).unwrap();
        assert_eq!(0, col.value(0));
        assert_eq!(1_000_000_000, col.value(1));
    }
}
