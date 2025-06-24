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
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{plan_err, ColumnStatistics};
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
    ///   include only the fields being output (projected) by the this mapping.
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

    /// Adapts file-level column `Statistics` to match the `table_schema`
    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> datafusion_common::Result<Vec<ColumnStatistics>>;
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

/// Checks if a file field can be cast to a table field
///
/// Returns Ok(true) if casting is possible, or an error explaining why casting is not possible
pub(crate) fn can_cast_field(
    file_field: &Field,
    table_field: &Field,
) -> datafusion_common::Result<bool> {
    if can_cast_types(file_field.data_type(), table_field.data_type()) {
        Ok(true)
    } else {
        plan_err!(
            "Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
            file_field.name(),
            file_field.data_type(),
            table_field.data_type()
        )
    }
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
        let (field_mappings, projection) = create_field_mapping(
            file_schema,
            &self.projected_table_schema,
            can_cast_field,
        )?;

        Ok((
            Arc::new(SchemaMapping::new(
                Arc::clone(&self.projected_table_schema),
                field_mappings,
            )),
            projection,
        ))
    }
}

/// Helper function that creates field mappings between file schema and table schema
///
/// Maps columns from the file schema to their corresponding positions in the table schema,
/// applying type compatibility checking via the provided predicate function.
///
/// Returns field mappings (for column reordering) and a projection (for field selection).
pub(crate) fn create_field_mapping<F>(
    file_schema: &Schema,
    projected_table_schema: &SchemaRef,
    can_map_field: F,
) -> datafusion_common::Result<(Vec<Option<usize>>, Vec<usize>)>
where
    F: Fn(&Field, &Field) -> datafusion_common::Result<bool>,
{
    let mut projection = Vec::with_capacity(file_schema.fields().len());
    let mut field_mappings = vec![None; projected_table_schema.fields().len()];

    for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
        if let Some((table_idx, table_field)) =
            projected_table_schema.fields().find(file_field.name())
        {
            if can_map_field(file_field, table_field)? {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }
    }

    Ok((field_mappings, projection))
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

impl SchemaMapping {
    /// Creates a new SchemaMapping instance
    ///
    /// Initializes the field mappings needed to transform file data to the projected table schema
    pub fn new(
        projected_table_schema: SchemaRef,
        field_mappings: Vec<Option<usize>>,
    ) -> Self {
        Self {
            projected_table_schema,
            field_mappings,
        }
    }
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

    /// Adapts file-level column `Statistics` to match the `table_schema`
    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> datafusion_common::Result<Vec<ColumnStatistics>> {
        let mut table_col_statistics = vec![];

        // Map the statistics for each field in the file schema to the corresponding field in the
        // table schema, if a field is not present in the file schema, we need to fill it with `ColumnStatistics::new_unknown`
        for (_, file_col_idx) in self
            .projected_table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
        {
            if let Some(file_col_idx) = file_col_idx {
                table_col_statistics.push(
                    file_col_statistics
                        .get(*file_col_idx)
                        .cloned()
                        .unwrap_or_default(),
                );
            } else {
                table_col_statistics.push(ColumnStatistics::new_unknown());
            }
        }

        Ok(table_col_statistics)
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::{stats::Precision, Statistics};

    use super::*;

    #[test]
    fn test_schema_mapping_map_statistics_basic() {
        // Create table schema (a, b, c)
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]));

        // Create file schema (b, a) - different order, missing c
        let file_schema = Schema::new(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, true),
        ]);

        // Create SchemaAdapter
        let adapter = DefaultSchemaAdapter {
            projected_table_schema: Arc::clone(&table_schema),
        };

        // Get mapper and projection
        let (mapper, projection) = adapter.map_schema(&file_schema).unwrap();

        // Should project columns 0,1 from file
        assert_eq!(projection, vec![0, 1]);

        // Create file statistics
        let mut file_stats = Statistics::default();

        // Statistics for column b (index 0 in file)
        let b_stats = ColumnStatistics {
            null_count: Precision::Exact(5),
            ..Default::default()
        };

        // Statistics for column a (index 1 in file)
        let a_stats = ColumnStatistics {
            null_count: Precision::Exact(10),
            ..Default::default()
        };

        file_stats.column_statistics = vec![b_stats, a_stats];

        // Map statistics
        let table_col_stats = mapper
            .map_column_statistics(&file_stats.column_statistics)
            .unwrap();

        // Verify stats
        assert_eq!(table_col_stats.len(), 3);
        assert_eq!(table_col_stats[0].null_count, Precision::Exact(10)); // a from file idx 1
        assert_eq!(table_col_stats[1].null_count, Precision::Exact(5)); // b from file idx 0
        assert_eq!(table_col_stats[2].null_count, Precision::Absent); // c (unknown)
    }

    #[test]
    fn test_schema_mapping_map_statistics_empty() {
        // Create schemas
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        let file_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]);

        let adapter = DefaultSchemaAdapter {
            projected_table_schema: Arc::clone(&table_schema),
        };
        let (mapper, _) = adapter.map_schema(&file_schema).unwrap();

        // Empty file statistics
        let file_stats = Statistics::default();
        let table_col_stats = mapper
            .map_column_statistics(&file_stats.column_statistics)
            .unwrap();

        // All stats should be unknown
        assert_eq!(table_col_stats.len(), 2);
        assert_eq!(table_col_stats[0], ColumnStatistics::new_unknown(),);
        assert_eq!(table_col_stats[1], ColumnStatistics::new_unknown(),);
    }

    #[test]
    fn test_can_cast_field() {
        // Same type should work
        let from_field = Field::new("col", DataType::Int32, true);
        let to_field = Field::new("col", DataType::Int32, true);
        assert!(can_cast_field(&from_field, &to_field).unwrap());

        // Casting Int32 to Float64 is allowed
        let from_field = Field::new("col", DataType::Int32, true);
        let to_field = Field::new("col", DataType::Float64, true);
        assert!(can_cast_field(&from_field, &to_field).unwrap());

        // Casting Float64 to Utf8 should work (converts to string)
        let from_field = Field::new("col", DataType::Float64, true);
        let to_field = Field::new("col", DataType::Utf8, true);
        assert!(can_cast_field(&from_field, &to_field).unwrap());

        // Binary to Utf8 is not supported - this is an example of a cast that should fail
        // Note: We use Binary instead of Utf8->Int32 because Arrow actually supports that cast
        let from_field = Field::new("col", DataType::Binary, true);
        let to_field = Field::new("col", DataType::Decimal128(10, 2), true);
        let result = can_cast_field(&from_field, &to_field);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast file schema field col"));
    }

    #[test]
    fn test_create_field_mapping() {
        // Define the table schema
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Float64, true),
        ]));

        // Define file schema: different order, missing column c, and b has different type
        let file_schema = Schema::new(vec![
            Field::new("b", DataType::Float64, true), // Different type but castable to Utf8
            Field::new("a", DataType::Int32, true),   // Same type
            Field::new("d", DataType::Boolean, true), // Not in table schema
        ]);

        // Custom can_map_field function that allows all mappings for testing
        let allow_all = |_: &Field, _: &Field| Ok(true);

        // Test field mapping
        let (field_mappings, projection) =
            create_field_mapping(&file_schema, &table_schema, allow_all).unwrap();

        // Expected:
        // - field_mappings[0] (a) maps to projection[1]
        // - field_mappings[1] (b) maps to projection[0]
        // - field_mappings[2] (c) is None (not in file)
        assert_eq!(field_mappings, vec![Some(1), Some(0), None]);
        assert_eq!(projection, vec![0, 1]); // Projecting file columns b, a

        // Test with a failing mapper
        let fails_all = |_: &Field, _: &Field| Ok(false);
        let (field_mappings, projection) =
            create_field_mapping(&file_schema, &table_schema, fails_all).unwrap();

        // Should have no mappings or projections if all cast checks fail
        assert_eq!(field_mappings, vec![None, None, None]);
        assert_eq!(projection, Vec::<usize>::new());

        // Test with error-producing mapper
        let error_mapper = |_: &Field, _: &Field| plan_err!("Test error");
        let result = create_field_mapping(&file_schema, &table_schema, error_mapper);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Test error"));
    }

    #[test]
    fn test_schema_mapping_new() {
        // Define the projected table schema
        let projected_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
        ]));

        // Define field mappings from table to file
        let field_mappings = vec![Some(1), Some(0)];

        // Create SchemaMapping manually
        let mapping =
            SchemaMapping::new(Arc::clone(&projected_schema), field_mappings.clone());

        // Check that fields were set correctly
        assert_eq!(*mapping.projected_table_schema, *projected_schema);
        assert_eq!(mapping.field_mappings, field_mappings);

        // Test with a batch to ensure it works properly
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("b_file", DataType::Utf8, true),
                Field::new("a_file", DataType::Int32, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["hello", "world"])),
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        // Test that map_batch works with our manually created mapping
        let mapped_batch = mapping.map_batch(batch).unwrap();

        // Verify the mapped batch has the correct schema and data
        assert_eq!(*mapped_batch.schema(), *projected_schema);
        assert_eq!(mapped_batch.num_columns(), 2);
        assert_eq!(mapped_batch.column(0).len(), 2); // a column
        assert_eq!(mapped_batch.column(1).len(), 2); // b column
    }

    #[test]
    fn test_map_schema_error_path() {
        // Define the table schema
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Decimal128(10, 2), true), // Use Decimal which has stricter cast rules
        ]));

        // Define file schema with incompatible type for column c
        let file_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Float64, true), // Different but castable
            Field::new("c", DataType::Binary, true),  // Not castable to Decimal128
        ]);

        // Create DefaultSchemaAdapter
        let adapter = DefaultSchemaAdapter {
            projected_table_schema: Arc::clone(&table_schema),
        };

        // map_schema should error due to incompatible types
        let result = adapter.map_schema(&file_schema);
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Cannot cast file schema field c"));
    }

    #[test]
    fn test_map_schema_happy_path() {
        // Define the table schema
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Decimal128(10, 2), true),
        ]));

        // Create DefaultSchemaAdapter
        let adapter = DefaultSchemaAdapter {
            projected_table_schema: Arc::clone(&table_schema),
        };

        // Define compatible file schema (missing column c)
        let compatible_file_schema = Schema::new(vec![
            Field::new("a", DataType::Int64, true), // Can be cast to Int32
            Field::new("b", DataType::Float64, true), // Can be cast to Utf8
        ]);

        // Test successful schema mapping
        let (mapper, projection) = adapter.map_schema(&compatible_file_schema).unwrap();

        // Verify field_mappings and projection created correctly
        assert_eq!(projection, vec![0, 1]); // Projecting a and b

        // Verify the SchemaMapping works with actual data
        let file_batch = RecordBatch::try_new(
            Arc::new(compatible_file_schema.clone()),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
                Arc::new(arrow::array::Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .unwrap();

        let mapped_batch = mapper.map_batch(file_batch).unwrap();

        // Verify correct schema mapping
        assert_eq!(*mapped_batch.schema(), *table_schema);
        assert_eq!(mapped_batch.num_columns(), 3); // a, b, c

        // Column c should be null since it wasn't in the file schema
        let c_array = mapped_batch.column(2);
        assert_eq!(c_array.len(), 2);
        assert_eq!(c_array.null_count(), 2);
    }
}
