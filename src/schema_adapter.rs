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

//! Custom schema adapter that uses Spark-compatible casts

use crate::cast::cast_supported;
use crate::{spark_cast, SparkCastOptions};
use arrow_array::{new_null_array, Array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};
use datafusion_common::plan_err;
use datafusion_expr::ColumnarValue;
use std::sync::Arc;

/// An implementation of DataFusion's `SchemaAdapterFactory` that uses a Spark-compatible
/// `cast` implementation.
#[derive(Clone, Debug)]
pub struct SparkSchemaAdapterFactory {
    /// Spark cast options
    cast_options: SparkCastOptions,
}

impl SparkSchemaAdapterFactory {
    pub fn new(options: SparkCastOptions) -> Self {
        Self {
            cast_options: options,
        }
    }
}

impl SchemaAdapterFactory for SparkSchemaAdapterFactory {
    /// Create a new factory for mapping batches from a file schema to a table
    /// schema.
    ///
    /// This is a convenience for [`DefaultSchemaAdapterFactory::create`] with
    /// the same schema for both the projected table schema and the table
    /// schema.
    fn create(
        &self,
        required_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(SparkSchemaAdapter {
            required_schema,
            table_schema,
            cast_options: self.cast_options.clone(),
        })
    }
}

/// This SchemaAdapter requires both the table schema and the projected table
/// schema. See  [`SchemaMapping`] for more details
#[derive(Clone, Debug)]
pub struct SparkSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetExec
    required_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    table_schema: SchemaRef,
    /// Spark cast options
    cast_options: SparkCastOptions,
}

impl SchemaAdapter for SparkSchemaAdapter {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.required_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    /// Creates a `SchemaMapping` for casting or mapping the columns from the
    /// file schema to the table schema.
    ///
    /// If the provided `file_schema` contains columns of a different type to
    /// the expected `table_schema`, the method will attempt to cast the array
    /// data from the file schema to the table schema where possible.
    ///
    /// Returns a [`SchemaMapping`] that can be applied to the output batch
    /// along with an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.required_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.required_schema.fields().find(file_field.name())
            {
                if cast_supported(
                    file_field.data_type(),
                    table_field.data_type(),
                    &self.cast_options,
                ) {
                    field_mappings[table_idx] = Some(projection.len());
                    projection.push(file_idx);
                } else {
                    return plan_err!(
                        "Cannot cast file schema field {} of type {:?} to required schema field of type {:?}",
                        file_field.name(),
                        file_field.data_type(),
                        table_field.data_type()
                    );
                }
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                required_schema: Arc::<Schema>::clone(&self.required_schema),
                field_mappings,
                table_schema: Arc::<Schema>::clone(&self.table_schema),
                cast_options: self.cast_options.clone(),
            }),
            projection,
        ))
    }
}

// TODO SchemaMapping is mostly copied from DataFusion but calls spark_cast
// instead of arrow cast - can we reduce the amount of code copied here and make
// the DataFusion version more extensible?

/// The SchemaMapping struct holds a mapping from the file schema to the table
/// schema and any necessary type conversions.
///
/// Note, because `map_batch` and `map_partial_batch` functions have different
/// needs, this struct holds two schemas:
///
/// 1. The projected **table** schema
/// 2. The full table schema
///
/// [`map_batch`] is used by the ParquetOpener to produce a RecordBatch which
/// has the projected schema, since that's the schema which is supposed to come
/// out of the execution of this query. Thus `map_batch` uses
/// `projected_table_schema` as it can only operate on the projected fields.
///
/// [`map_partial_batch`]  is used to create a RecordBatch with a schema that
/// can be used for Parquet predicate pushdown, meaning that it may contain
/// fields which are not in the projected schema (as the fields that parquet
/// pushdown filters operate can be completely distinct from the fields that are
/// projected (output) out of the ParquetExec). `map_partial_batch` thus uses
/// `table_schema` to create the resulting RecordBatch (as it could be operating
/// on any fields in the schema).
///
/// [`map_batch`]: Self::map_batch
/// [`map_partial_batch`]: Self::map_partial_batch
#[derive(Debug)]
pub struct SchemaMapping {
    /// The schema of the table. This is the expected schema after conversion
    /// and it should match the schema of the query result.
    required_schema: SchemaRef,
    /// Mapping from field index in `projected_table_schema` to index in
    /// projected file_schema.
    ///
    /// They are Options instead of just plain `usize`s because the table could
    /// have fields that don't exist in the file.
    field_mappings: Vec<Option<usize>>,
    /// The entire table schema, as opposed to the projected_table_schema (which
    /// only contains the columns that we are projecting out of this query).
    /// This contains all fields in the table, regardless of if they will be
    /// projected out or not.
    table_schema: SchemaRef,
    /// Spark cast options
    cast_options: SparkCastOptions,
}

impl SchemaMapper for SchemaMapping {
    /// Adapts a `RecordBatch` to match the `projected_table_schema` using the stored mapping and
    /// conversions. The produced RecordBatch has a schema that contains only the projected
    /// columns, so if one needs a RecordBatch with a schema that references columns which are not
    /// in the projected, it would be better to use `map_partial_batch`
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let batch_rows = batch.num_rows();
        let batch_cols = batch.columns().to_vec();

        let cols = self
            .required_schema
            // go through each field in the projected schema
            .fields()
            .iter()
            // and zip it with the index that maps fields from the projected table schema to the
            // projected file schema in `batch`
            .zip(&self.field_mappings)
            // and for each one...
            .map(|(field, file_idx)| {
                file_idx.map_or_else(
                    // If this field only exists in the table, and not in the file, then we know
                    // that it's null, so just return that.
                    || Ok(new_null_array(field.data_type(), batch_rows)),
                    // However, if it does exist in both, then try to cast it to the correct output
                    // type
                    |batch_idx| {
                        spark_cast(
                            ColumnarValue::Array(Arc::clone(&batch_cols[batch_idx])),
                            field.data_type(),
                            &self.cast_options,
                        )?
                        .into_array(batch_rows)
                    },
                )
            })
            .collect::<datafusion_common::Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = Arc::<Schema>::clone(&self.required_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    /// Adapts a [`RecordBatch`]'s schema into one that has all the correct output types and only
    /// contains the fields that exist in both the file schema and table schema.
    ///
    /// Unlike `map_batch` this method also preserves the columns that
    /// may not appear in the final output (`projected_table_schema`) but may
    /// appear in push down predicates
    fn map_partial_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let batch_cols = batch.columns().to_vec();
        let schema = batch.schema();

        // for each field in the batch's schema (which is based on a file, not a table)...
        let (cols, fields) = schema
            .fields()
            .iter()
            .zip(batch_cols.iter())
            .flat_map(|(field, batch_col)| {
                self.table_schema
                    // try to get the same field from the table schema that we have stored in self
                    .field_with_name(field.name())
                    // and if we don't have it, that's fine, ignore it. This may occur when we've
                    // created an external table whose fields are a subset of the fields in this
                    // file, then tried to read data from the file into this table. If that is the
                    // case here, it's fine to ignore because we don't care about this field
                    // anyways
                    .ok()
                    // but if we do have it,
                    .map(|table_field| {
                        // try to cast it into the correct output type. we don't want to ignore this
                        // error, though, so it's propagated.
                        spark_cast(
                            ColumnarValue::Array(Arc::clone(batch_col)),
                            table_field.data_type(),
                            &self.cast_options,
                        )?
                        .into_array(batch_col.len())
                        // and if that works, return the field and column.
                        .map(|new_col| (new_col, table_field.clone()))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

#[cfg(test)]
mod test {
    use crate::test_common::file_util::get_temp_filename;
    use crate::{EvalMode, SparkCastOptions, SparkSchemaAdapterFactory};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::UInt32Array;
    use arrow_schema::SchemaRef;
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion_common::DataFusionError;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc;

    #[tokio::test]
    async fn parquet_roundtrip_int_as_string() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let ids = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let names = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]))
            as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids, names])?;

        let required_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    #[tokio::test]
    async fn parquet_roundtrip_unsigned_int() -> Result<(), DataFusionError> {
        let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));

        let ids = Arc::new(UInt32Array::from(vec![1, 2, 3])) as Arc<dyn arrow::array::Array>;
        let batch = RecordBatch::try_new(Arc::clone(&file_schema), vec![ids])?;

        let required_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let _ = roundtrip(&batch, required_schema).await?;

        Ok(())
    }

    /// Create a Parquet file containing a single batch and then read the batch back using
    /// the specified required_schema. This will cause the SchemaAdapter code to be used.
    async fn roundtrip(
        batch: &RecordBatch,
        required_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        let filename = get_temp_filename();
        let filename = filename.as_path().as_os_str().to_str().unwrap().to_string();
        let file = File::create(&filename)?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&batch.schema()), None)?;
        writer.write(batch)?;
        writer.close()?;

        let object_store_url = ObjectStoreUrl::local_filesystem();
        let file_scan_config = FileScanConfig::new(object_store_url, required_schema)
            .with_file_groups(vec![vec![PartitionedFile::from_path(
                filename.to_string(),
            )?]]);

        let mut spark_cast_options = SparkCastOptions::new(EvalMode::Legacy, "UTC", false);
        spark_cast_options.allow_cast_unsigned_ints = true;

        let parquet_exec = ParquetExec::builder(file_scan_config)
            .with_schema_adapter_factory(Arc::new(SparkSchemaAdapterFactory::new(
                spark_cast_options,
            )))
            .build();

        let mut stream = parquet_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        stream.next().await.unwrap()
    }
}
