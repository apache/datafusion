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

//! Helper struct to manage table schemas with partition columns

use arrow::datatypes::{FieldRef, Fields, SchemaBuilder, SchemaRef};
use std::sync::Arc;

/// The overall schema for potentially partitioned data sources.
///
/// When reading partitioned data (such as Hive-style partitioning), a [`TableSchema`]
/// consists of two parts:
/// 1. **File schema**: The schema of the actual data files on disk
/// 2. **Partition columns**: Columns whose values are encoded in the directory structure,
///    but not stored in the files themselves
///
/// # Example: Partitioned Table
///
/// Consider a table with the following directory structure:
/// ```text
/// /data/date=2025-10-10/region=us-west/data.parquet
/// /data/date=2025-10-11/region=us-east/data.parquet
/// ```
///
/// In this case:
/// - **File schema**: The schema of `data.parquet` files (e.g., `[user_id, amount]`)
/// - **Partition columns**: `[date, region]` extracted from the directory path
/// - **Table schema**: The full schema combining both (e.g., `[user_id, amount, date, region]`)
///
/// # When to Use
///
/// Use `TableSchema` when:
/// - Reading partitioned data sources (Parquet, CSV, etc. with Hive-style partitioning)
/// - You need to efficiently access different schema representations without reconstructing them
/// - You want to avoid repeatedly concatenating file and partition schemas
///
/// For non-partitioned data or when working with a single schema representation,
/// working directly with Arrow's `Schema` or `SchemaRef` is simpler.
///
/// # Performance
///
/// This struct pre-computes and caches the full table schema, allowing cheap references
/// to any representation without repeated allocations or reconstructions.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// The schema of the data files themselves, without partition columns.
    ///
    /// For example, if your Parquet files contain `[user_id, amount]`,
    /// this field holds that schema.
    file_schema: SchemaRef,

    /// Columns that are derived from the directory structure (partitioning scheme).
    ///
    /// For Hive-style partitioning like `/date=2025-10-10/region=us-west/`,
    /// this contains the `date` and `region` fields.
    ///
    /// These columns are NOT present in the data files but are appended to each
    /// row during query execution based on the file's location.
    ///
    /// Stored as [`Fields`] (an immutable `Arc<[FieldRef]>`) so that cloning a
    /// `TableSchema` is cheap and the partition columns can be shared zero-copy
    /// with an existing schema.
    table_partition_cols: Fields,

    /// The complete table schema: file_schema columns followed by partition columns.
    ///
    /// This is pre-computed during construction by concatenating `file_schema`
    /// and `table_partition_cols`, so it can be returned as a cheap reference.
    table_schema: SchemaRef,
}

impl TableSchema {
    /// Start building a [`TableSchema`] from its (required) file schema.
    ///
    /// Partition columns are optional and added with
    /// [`TableSchemaBuilder::with_table_partition_cols`]; the full table schema
    /// is computed once by [`TableSchemaBuilder::build`]. This is the preferred
    /// way to construct a `TableSchema`.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::{Schema, Field, DataType};
    /// # use datafusion_datasource::TableSchema;
    /// let file_schema = Arc::new(Schema::new(vec![
    ///     Field::new("user_id", DataType::Int64, false),
    ///     Field::new("amount", DataType::Float64, false),
    /// ]));
    ///
    /// let table_schema = TableSchema::builder(file_schema)
    ///     .with_table_partition_cols(vec![
    ///         Arc::new(Field::new("date", DataType::Utf8, false)),
    ///         Arc::new(Field::new("region", DataType::Utf8, false)),
    ///     ])
    ///     .build();
    ///
    /// // Table schema will have 4 columns: user_id, amount, date, region
    /// assert_eq!(table_schema.table_schema().fields().len(), 4);
    /// ```
    pub fn builder(file_schema: SchemaRef) -> TableSchemaBuilder {
        TableSchemaBuilder::new(file_schema)
    }

    /// Create a new TableSchema from a file schema and partition columns.
    ///
    /// This is a convenience for
    /// `TableSchema::builder(file_schema).with_table_partition_cols(cols).build()`.
    #[deprecated(
        since = "55.0.0",
        note = "use TableSchema::builder(file_schema).with_table_partition_cols(cols).build() (or TableSchema::from(file_schema) for no partition columns)"
    )]
    pub fn new(file_schema: SchemaRef, table_partition_cols: Vec<FieldRef>) -> Self {
        TableSchemaBuilder::new(file_schema)
            .with_table_partition_cols(table_partition_cols)
            .build()
    }

    /// Create a new TableSchema with no partition columns.
    #[deprecated(
        since = "55.0.0",
        note = "use TableSchema::from(file_schema) / file_schema.into()"
    )]
    pub fn from_file_schema(file_schema: SchemaRef) -> Self {
        TableSchemaBuilder::new(file_schema).build()
    }

    /// Return a new `TableSchema` with `partition_cols` as its partition columns,
    /// replacing any existing ones.
    #[deprecated(
        since = "55.0.0",
        note = "use TableSchema::builder(file_schema).with_table_partition_cols(cols).build()"
    )]
    pub fn with_table_partition_cols(self, partition_cols: Vec<FieldRef>) -> Self {
        TableSchemaBuilder::new(self.file_schema)
            .with_table_partition_cols(partition_cols)
            .build()
    }

    /// Get the file schema (without partition columns).
    ///
    /// This is the schema of the actual data files on disk.
    pub fn file_schema(&self) -> &SchemaRef {
        &self.file_schema
    }

    /// Get the table partition columns.
    ///
    /// These are the columns derived from the directory structure that
    /// will be appended to each row during query execution.
    pub fn table_partition_cols(&self) -> &Fields {
        &self.table_partition_cols
    }

    /// Get the full table schema (file schema + partition columns).
    ///
    /// This is the complete schema that will be seen by queries, combining
    /// both the columns from the files and the partition columns.
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }
}

impl From<SchemaRef> for TableSchema {
    fn from(schema: SchemaRef) -> Self {
        TableSchemaBuilder::new(schema).build()
    }
}

impl From<&SchemaRef> for TableSchema {
    fn from(schema: &SchemaRef) -> Self {
        TableSchemaBuilder::new(Arc::clone(schema)).build()
    }
}

/// Builder for [`TableSchema`].
///
/// The file schema is the only required input; partition columns are optional.
/// Unlike calling [`TableSchema`]'s setters repeatedly, the builder computes the
/// concatenated table schema exactly once, in [`TableSchemaBuilder::build`].
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::datatypes::{Schema, Field, DataType};
/// # use datafusion_datasource::TableSchemaBuilder;
/// # let file_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
/// let table_schema = TableSchemaBuilder::new(file_schema)
///     .with_table_partition_cols(vec![Arc::new(Field::new("date", DataType::Utf8, false))])
///     .build();
/// assert_eq!(table_schema.table_partition_cols().len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct TableSchemaBuilder {
    file_schema: SchemaRef,
    table_partition_cols: Fields,
}

impl TableSchemaBuilder {
    /// Create a builder for a `TableSchema` over the given file schema, with no
    /// partition columns yet.
    pub fn new(file_schema: SchemaRef) -> Self {
        Self {
            file_schema,
            table_partition_cols: Fields::empty(),
        }
    }

    /// Set the partition columns, replacing any previously set.
    ///
    /// Accepts anything convertible into [`Fields`] (e.g. `Vec<FieldRef>` or an
    /// existing schema's `Fields`, which is shared zero-copy).
    pub fn with_table_partition_cols(
        mut self,
        table_partition_cols: impl Into<Fields>,
    ) -> Self {
        self.table_partition_cols = table_partition_cols.into();
        self
    }

    /// Build the [`TableSchema`], computing the full `file + partition` schema once.
    pub fn build(self) -> TableSchema {
        let mut builder = SchemaBuilder::from(self.file_schema.as_ref());
        builder.extend(self.table_partition_cols.iter().cloned());
        TableSchema {
            file_schema: self.file_schema,
            table_partition_cols: self.table_partition_cols,
            table_schema: Arc::new(builder.finish()),
        }
    }
}

impl From<SchemaRef> for TableSchemaBuilder {
    fn from(schema: SchemaRef) -> Self {
        TableSchemaBuilder::new(schema)
    }
}

impl From<&SchemaRef> for TableSchemaBuilder {
    fn from(schema: &SchemaRef) -> Self {
        TableSchemaBuilder::new(Arc::clone(schema))
    }
}

#[cfg(test)]
mod tests {
    use super::{TableSchema, TableSchemaBuilder};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_table_schema_creation() {
        let file_schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]));

        let partition_cols = vec![
            Arc::new(Field::new("date", DataType::Utf8, false)),
            Arc::new(Field::new("region", DataType::Utf8, false)),
        ];

        let table_schema = TableSchema::builder(file_schema.clone())
            .with_table_partition_cols(partition_cols.clone())
            .build();

        // Verify file schema
        assert_eq!(table_schema.file_schema().as_ref(), file_schema.as_ref());

        // Verify partition columns
        assert_eq!(table_schema.table_partition_cols().len(), 2);
        assert_eq!(table_schema.table_partition_cols()[0], partition_cols[0]);
        assert_eq!(table_schema.table_partition_cols()[1], partition_cols[1]);

        // Verify full table schema
        let expected_fields = vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("date", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ];
        let expected_schema = Schema::new(expected_fields);
        assert_eq!(table_schema.table_schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_builder_with_partition_cols() {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let table_schema = TableSchemaBuilder::new(Arc::clone(&file_schema))
            .with_table_partition_cols(vec![
                Arc::new(Field::new("country", DataType::Utf8, false)),
                Arc::new(Field::new("year", DataType::Int32, false)),
            ])
            .build();

        // File schema is preserved and the partition columns are appended.
        assert_eq!(table_schema.file_schema().as_ref(), file_schema.as_ref());
        assert_eq!(table_schema.table_partition_cols().len(), 2);
        assert_eq!(table_schema.table_partition_cols()[0].name(), "country");
        assert_eq!(table_schema.table_partition_cols()[1].name(), "year");

        let expected_schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("country", DataType::Utf8, false),
            Field::new("year", DataType::Int32, false),
        ]);
        assert_eq!(table_schema.table_schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_builder_with_table_partition_cols_replaces() {
        // Calling the setter more than once replaces rather than appends.
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let table_schema = TableSchemaBuilder::new(file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "country",
                DataType::Utf8,
                false,
            ))])
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "city",
                DataType::Utf8,
                false,
            ))])
            .build();

        assert_eq!(table_schema.table_partition_cols().len(), 1);
        assert_eq!(table_schema.table_partition_cols()[0].name(), "city");
    }

    #[test]
    fn test_builder_accepts_fields_zero_copy() {
        // `with_table_partition_cols` accepts an existing schema's `Fields`
        // directly (shared via `Arc`, no `Vec` round-trip).
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let partition_schema =
            Schema::new(vec![Field::new("date", DataType::Utf8, false)]);

        let table_schema = TableSchemaBuilder::new(file_schema)
            .with_table_partition_cols(partition_schema.fields().clone())
            .build();

        assert_eq!(table_schema.table_partition_cols().len(), 1);
        assert_eq!(table_schema.table_partition_cols()[0].name(), "date");
    }

    #[test]
    #[expect(deprecated)]
    fn test_deprecated_with_table_partition_cols_replaces() {
        // The deprecated setter still works and replaces the partition columns.
        // It is safe on a shared clone because partition columns are immutable.
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let original = TableSchema::builder(file_schema)
            .with_table_partition_cols(vec![Arc::new(Field::new(
                "country",
                DataType::Utf8,
                false,
            ))])
            .build();

        let replaced =
            original
                .clone()
                .with_table_partition_cols(vec![Arc::new(Field::new(
                    "city",
                    DataType::Utf8,
                    false,
                ))]);

        assert_eq!(replaced.table_partition_cols().len(), 1);
        assert_eq!(replaced.table_partition_cols()[0].name(), "city");

        // The original is untouched.
        assert_eq!(original.table_partition_cols().len(), 1);
        assert_eq!(original.table_partition_cols()[0].name(), "country");
    }
}
