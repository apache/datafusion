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

use arrow::array::{new_null_array, RecordBatch, RecordBatchOptions};
use arrow::compute::{can_cast_types, cast};
use arrow::datatypes::{Schema, SchemaRef};
use datafusion_common::plan_err;
use std::fmt::Debug;
use std::sync::Arc;

/// Factory for creating [`SchemaAdapter`]
///
/// This interface provides a way to implement custom schema adaptation logic
/// for DataSourceExec (for example, to fill missing columns with default value
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
/// # use datafusion_datasource::schema_adapter::{DefaultSchemaAdapterFactory, SchemaAdapterFactory};
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
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DefaultSchemaAdapter {
            projected_table_schema,
        })
    }
}

/// This SchemaAdapter requires both the table schema and the projected table
/// schema. See  [`SchemaMapping`] for more details
#[derive(Clone, Debug)]
pub(crate) struct DefaultSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetSource
    projected_table_schema: SchemaRef,
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
                projected_table_schema: Arc::clone(&self.projected_table_schema),
                field_mappings,
            }),
            projection,
        ))
    }
}

/// The SchemaMapping struct holds a mapping from the file schema to the table
/// schema and any necessary type conversions.
///
/// [`map_batch`] is used by the ParquetOpener to produce a RecordBatch which
/// has the projected schema, since that's the schema which is supposed to come
/// out of the execution of this query. Thus `map_batch` uses
/// `projected_table_schema` as it can only operate on the projected fields.
///
/// [`map_batch`]: Self::map_batch
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
}

impl SchemaMapper for SchemaMapping {
    /// Adapts a `RecordBatch` to match the `projected_table_schema` using the stored mapping and
    /// conversions.
    /// The produced RecordBatch has a schema that contains only the projected columns.
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

        let schema = Arc::clone(&self.projected_table_schema);
        let record_batch = RecordBatch::try_new_with_options(schema, cols, &options)?;
        Ok(record_batch)
    }
}
