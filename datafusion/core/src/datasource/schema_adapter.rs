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

//! [`SchemaAdapter`] and [`SchemaAdapterFactory`] to adapt file-level record batches to a table schema.
//!
//! Adapter provides a method of translating the RecordBatches that come out of the
//! physical format into how they should be used by DataFusion.  For instance, a schema
//! can be stored external to a parquet file that maps parquet logical types to arrow types.

use arrow::compute::{can_cast_types, cast};
use arrow_array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::plan_err;
use std::fmt::Debug;
use std::sync::Arc;

/// Factory for creating [`SchemaAdapter`]
///
/// This interface provides a way to implement custom schema adaptation logic
/// for ParquetExec (for example, to fill missing columns with default value
/// other than null).
///
/// Most users should use [`DefaultSchemaAdapterFactory`]. See that struct for
/// more details and examples.
pub trait SchemaAdapterFactory: Debug + Send + Sync + 'static {
    /// Create a [`SchemaAdapter`]
    ///
    /// Arguments:
    ///
    /// * `projected_table_schema`: The schema for the table, projected to
    ///    include only the fields being output (projected) by the this mapping.
    ///
    /// * `table_schema`: The entire table schema for the table
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter>;
}

/// Creates [`SchemaMapper`]s to map file-level [`RecordBatch`]es to a table
/// schema, which may have a schema obtained from merging multiple file-level
/// schemas.
///
/// This is useful for implementing schema evolution in partitioned datasets.
///
/// See [`DefaultSchemaAdapterFactory`] for more details and examples.
pub trait SchemaAdapter: Send + Sync {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// This is used while reading a file to push down projections by mapping
    /// projected column indexes from the table schema to the file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize>;

    /// Creates a mapping for casting columns from the file schema to the table
    /// schema.
    ///
    /// This is used after reading a record batch. The returned [`SchemaMapper`]:
    ///
    /// 1. Maps columns to the expected columns indexes
    /// 2. Handles missing values (e.g. fills nulls or a default value) for
    ///    columns in the in the table schema not in the file schema
    /// 2. Handles different types: if the column in the file schema has a
    ///    different type than `table_schema`, the mapper will resolve this
    ///    difference (e.g. by casting to the appropriate type)
    ///
    /// Returns:
    /// * a [`SchemaMapper`]
    /// * an ordered list of columns to project from the file
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)>;
}

/// Maps, columns from a specific file schema to the table schema.
///
/// See [`DefaultSchemaAdapterFactory`] for more details and examples.
pub trait SchemaMapper: Debug + Send + Sync {
    /// Adapts a `RecordBatch` to match the `table_schema`
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch>;

    /// Adapts a [`RecordBatch`] that does not have all the columns from the
    /// file schema.
    ///
    /// This method is used, for example,  when applying a filter to a subset of
    /// the columns as part of `DataFusionArrowPredicate` when `filter_pushdown`
    /// is enabled.
    ///
    /// This method is slower than `map_batch` as it looks up columns by name.
    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch>;
}

/// Default  [`SchemaAdapterFactory`] for mapping schemas.
///
/// This can be used to adapt file-level record batches to a table schema and
/// implement schema evolution.
///
/// Given an input file schema and a table schema, this factory returns
/// [`SchemaAdapter`] that return [`SchemaMapper`]s that:
///
/// 1. Reorder columns
/// 2. Cast columns to the correct type
/// 3. Fill missing columns with nulls
///
/// # Errors:
///
/// * If a column in the table schema is non-nullable but is not present in the
///   file schema (i.e. it is missing), the returned mapper tries to fill it with
///   nulls resulting in a schema error.
///
/// # Illustration of Schema Mapping
///
/// ```text
/// ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///  ┌───────┐   ┌───────┐ │                  ┌───────┐   ┌───────┐   ┌───────┐ │
/// ││  1.0  │   │ "foo" │                   ││ NULL  │   │ "foo" │   │ "1.0" │
///  ├───────┤   ├───────┤ │ Schema mapping   ├───────┤   ├───────┤   ├───────┤ │
/// ││  2.0  │   │ "bar" │                   ││  NULL │   │ "bar" │   │ "2.0" │
///  └───────┘   └───────┘ │────────────────▶ └───────┘   └───────┘   └───────┘ │
/// │                                        │
///  column "c"  column "b"│                  column "a"  column "b"  column "c"│
/// │ Float64       Utf8                     │  Int32        Utf8        Utf8
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘                  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
///     Input Record Batch                         Output Record Batch
///
///     Schema {                                   Schema {
///      "c": Float64,                              "a": Int32,
///      "b": Utf8,                                 "b": Utf8,
///     }                                           "c": Utf8,
///                                                }
/// ```
///
/// # Example of using the `DefaultSchemaAdapterFactory` to map [`RecordBatch`]s
///
/// Note `SchemaMapping` also supports mapping partial batches, which is used as
/// part of predicate pushdown.
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{DataType, Field, Schema};
/// # use datafusion::datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory};
/// # use datafusion_common::record_batch;
/// // Table has fields "a",  "b" and "c"
/// let table_schema = Schema::new(vec![
///     Field::new("a", DataType::Int32, true),
///     Field::new("b", DataType::Utf8, true),
///     Field::new("c", DataType::Utf8, true),
/// ]);
///
/// // create an adapter to map the table schema to the file schema
/// let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
///
/// // The file schema has fields "c" and "b" but "b" is stored as an 'Float64'
/// // instead of 'Utf8'
/// let file_schema = Schema::new(vec![
///    Field::new("c", DataType::Utf8, true),
///    Field::new("b", DataType::Float64, true),
/// ]);
///
/// // Get a mapping from the file schema to the table schema
/// let (mapper, _indices) = adapter.map_schema(&file_schema).unwrap();
///
/// let file_batch = record_batch!(
///     ("c", Utf8, vec!["foo", "bar"]),
///     ("b", Float64, vec![1.0, 2.0])
/// ).unwrap();
///
/// let mapped_batch = mapper.map_batch(file_batch).unwrap();
///
/// // the mapped batch has the correct schema and the "b" column has been cast to Utf8
/// let expected_batch = record_batch!(
///    ("a", Int32, vec![None, None]),  // missing column filled with nulls
///    ("b", Utf8, vec!["1.0", "2.0"]), // b was cast to string and order was changed
///    ("c", Utf8, vec!["foo", "bar"])
/// ).unwrap();
/// assert_eq!(mapped_batch, expected_batch);
/// ```
#[derive(Clone, Debug, Default)]
pub struct DefaultSchemaAdapterFactory;

impl DefaultSchemaAdapterFactory {
    /// Create a new factory for mapping batches from a file schema to a table
    /// schema.
    ///
    /// This is a convenience for [`DefaultSchemaAdapterFactory::create`] with
    /// the same schema for both the projected table schema and the table
    /// schema.
    pub fn from_schema(table_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        Self.create(Arc::clone(&table_schema), table_schema)
    }
}

impl SchemaAdapterFactory for DefaultSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DefaultSchemaAdapter {
            projected_table_schema,
            table_schema,
        })
    }
}

/// This SchemaAdapter requires both the table schema and the projected table
/// schema. See  [`SchemaMapping`] for more details
#[derive(Clone, Debug)]
pub(crate) struct DefaultSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetExec
    projected_table_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    table_schema: SchemaRef,
}

impl SchemaAdapter for DefaultSchemaAdapter {
    /// Map a column index in the table schema to a column index in a particular
    /// file schema
    ///
    /// Panics if index is not in range for the table schema
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.projected_table_schema.field(index);
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
        let mut field_mappings = vec![None; self.projected_table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.projected_table_schema.fields().find(file_field.name())
            {
                match can_cast_types(file_field.data_type(), table_field.data_type()) {
                    true => {
                        field_mappings[table_idx] = Some(projection.len());
                        projection.push(file_idx);
                    }
                    false => {
                        return plan_err!(
                            "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                            file_field.name(),
                            file_field.data_type(),
                            table_field.data_type()
                        )
                    }
                }
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                projected_table_schema: self.projected_table_schema.clone(),
                field_mappings,
                table_schema: self.table_schema.clone(),
            }),
            projection,
        ))
    }
}

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
    projected_table_schema: SchemaRef,
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
            .projected_table_schema
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
                    |batch_idx| cast(&batch_cols[batch_idx], field.data_type()),
                )
            })
            .collect::<datafusion_common::Result<Vec<_>, _>>()?;

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema = self.projected_table_schema.clone();
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }

    /// Adapts a [`RecordBatch`]'s schema into one that has all the correct output types and only
    /// contains the fields that exist in both the file schema and table schema.
    ///
    /// Unlike `map_batch` this method also preserves the columns that
    /// may not appear in the final output (`projected_table_schema`) but may
    /// appear in push down predicates
    fn map_partial_batch(
        &self,
        batch: RecordBatch,
    ) -> datafusion_common::Result<RecordBatch> {
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
                        cast(batch_col, table_field.data_type())
                            // and if that works, return the field and column.
                            .map(|new_col| (new_col, table_field.clone()))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        // Necessary to handle empty batches
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

        let schema =
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use crate::assert_batches_sorted_eq;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, SchemaRef};
    use object_store::path::Path;
    use object_store::ObjectMeta;

    use crate::datasource::object_store::ObjectStoreUrl;
    use crate::datasource::physical_plan::{FileScanConfig, ParquetExec};
    use crate::physical_plan::collect;
    use crate::prelude::SessionContext;

    use crate::datasource::listing::PartitionedFile;
    use crate::datasource::schema_adapter::{
        DefaultSchemaAdapterFactory, SchemaAdapter, SchemaAdapterFactory, SchemaMapper,
    };
    use datafusion_common::record_batch;
    #[cfg(feature = "parquet")]
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    #[tokio::test]
    async fn can_override_schema_adapter() {
        // Test shows that SchemaAdapter can add a column that doesn't existing in the
        // record batches returned from parquet.  This can be useful for schema evolution
        // where older files may not have all columns.
        let tmp_dir = TempDir::new().unwrap();
        let table_dir = tmp_dir.path().join("parquet_test");
        fs::DirBuilder::new().create(table_dir.as_path()).unwrap();
        let f1 = Field::new("id", DataType::Int32, true);

        let file_schema = Arc::new(Schema::new(vec![f1.clone()]));
        let filename = "part.parquet".to_string();
        let path = table_dir.as_path().join(filename.clone());
        let file = fs::File::create(path.clone()).unwrap();
        let mut writer = ArrowWriter::try_new(file, file_schema.clone(), None).unwrap();

        let ids = Arc::new(Int32Array::from(vec![1i32]));
        let rec_batch = RecordBatch::try_new(file_schema.clone(), vec![ids]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();

        let location = Path::parse(path.to_str().unwrap()).unwrap();
        let metadata = fs::metadata(path.as_path()).expect("Local file metadata");
        let meta = ObjectMeta {
            location,
            last_modified: metadata.modified().map(chrono::DateTime::from).unwrap(),
            size: metadata.len() as usize,
            e_tag: None,
            version: None,
        };

        let partitioned_file = PartitionedFile {
            object_meta: meta,
            partition_values: vec![],
            range: None,
            statistics: None,
            extensions: None,
        };

        let f1 = Field::new("id", DataType::Int32, true);
        let f2 = Field::new("extra_column", DataType::Utf8, true);

        let schema = Arc::new(Schema::new(vec![f1.clone(), f2.clone()]));

        // prepare the scan
        let parquet_exec = ParquetExec::builder(
            FileScanConfig::new(ObjectStoreUrl::local_filesystem(), schema)
                .with_file(partitioned_file),
        )
        .build()
        .with_schema_adapter_factory(Arc::new(TestSchemaAdapterFactory {}));

        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let read = collect(Arc::new(parquet_exec), task_ctx).await.unwrap();

        let expected = [
            "+----+--------------+",
            "| id | extra_column |",
            "+----+--------------+",
            "| 1  | foo          |",
            "+----+--------------+",
        ];

        assert_batches_sorted_eq!(expected, &read);
    }

    #[test]
    fn default_schema_adapter() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        // file has a subset of the table schema fields and different type
        let file_schema = Schema::new(vec![
            Field::new("c", DataType::Float64, true), // not in table schema
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![1]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        let mapped_batch = mapper.map_batch(file_batch).unwrap();

        // the mapped batch has the correct schema and the "b" column has been cast to Utf8
        let expected_batch = record_batch!(
            ("a", Int32, vec![None, None]), // missing column filled with nulls
            ("b", Utf8, vec!["1.0", "2.0"])  // b was cast to string and order was changed
        )
        .unwrap();
        assert_eq!(mapped_batch, expected_batch);
    }

    #[test]
    fn default_schema_adapter_non_nullable_columns() {
        let table_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false), // "a"" is declared non nullable
            Field::new("b", DataType::Utf8, true),
        ]);
        let file_schema = Schema::new(vec![
            // since file doesn't have "a" it will be filled with nulls
            Field::new("b", DataType::Float64, true),
        ]);

        let adapter = DefaultSchemaAdapterFactory::from_schema(Arc::new(table_schema));
        let (mapper, indices) = adapter.map_schema(&file_schema).unwrap();
        assert_eq!(indices, vec![0]);

        let file_batch = record_batch!(("b", Float64, vec![1.0, 2.0])).unwrap();

        // Mapping fails because it tries to fill in a non-nullable column with nulls
        let err = mapper.map_batch(file_batch).unwrap_err().to_string();
        assert!(err.contains("Invalid argument error: Column 'a' is declared as non-nullable but contains null values"), "{err}");
    }

    #[derive(Debug)]
    struct TestSchemaAdapterFactory;

    impl SchemaAdapterFactory for TestSchemaAdapterFactory {
        fn create(
            &self,
            projected_table_schema: SchemaRef,
            _table_schema: SchemaRef,
        ) -> Box<dyn SchemaAdapter> {
            Box::new(TestSchemaAdapter {
                table_schema: projected_table_schema,
            })
        }
    }

    struct TestSchemaAdapter {
        /// Schema for the table
        table_schema: SchemaRef,
    }

    impl SchemaAdapter for TestSchemaAdapter {
        fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
            let field = self.table_schema.field(index);
            Some(file_schema.fields.find(field.name())?.0)
        }

        fn map_schema(
            &self,
            file_schema: &Schema,
        ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
            let mut projection = Vec::with_capacity(file_schema.fields().len());

            for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
                if self.table_schema.fields().find(file_field.name()).is_some() {
                    projection.push(file_idx);
                }
            }

            Ok((Arc::new(TestSchemaMapping {}), projection))
        }
    }

    #[derive(Debug)]
    struct TestSchemaMapping {}

    impl SchemaMapper for TestSchemaMapping {
        fn map_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            let f1 = Field::new("id", DataType::Int32, true);
            let f2 = Field::new("extra_column", DataType::Utf8, true);

            let schema = Arc::new(Schema::new(vec![f1, f2]));

            let extra_column = Arc::new(StringArray::from(vec!["foo"]));
            let mut new_columns = batch.columns().to_vec();
            new_columns.push(extra_column);

            Ok(RecordBatch::try_new(schema, new_columns).unwrap())
        }

        fn map_partial_batch(
            &self,
            batch: RecordBatch,
        ) -> datafusion_common::Result<RecordBatch> {
            self.map_batch(batch)
        }
    }
}
