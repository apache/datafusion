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

use arrow::datatypes::{FieldRef, SchemaBuilder, SchemaRef};
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
    table_partition_cols: Arc<Vec<FieldRef>>,

    /// The complete table schema: file_schema columns followed by partition columns.
    ///
    /// This is pre-computed during construction by concatenating `file_schema`
    /// and `table_partition_cols`, so it can be returned as a cheap reference.
    table_schema: SchemaRef,
}

impl TableSchema {
    /// Create a new TableSchema from a file schema and partition columns.
    ///
    /// The table schema is automatically computed by appending the partition columns
    /// to the file schema.
    ///
    /// You should prefer calling this method over
    /// chaining [`TableSchema::from_file_schema`] and [`TableSchema::with_table_partition_cols`]
    /// if you have both the file schema and partition columns available at construction time
    /// since it avoids re-computing the table schema.
    ///
    /// # Arguments
    ///
    /// * `file_schema` - Schema of the data files (without partition columns)
    /// * `table_partition_cols` - Partition columns to append to each row
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
    /// let partition_cols = vec![
    ///     Arc::new(Field::new("date", DataType::Utf8, false)),
    ///     Arc::new(Field::new("region", DataType::Utf8, false)),
    /// ];
    ///
    /// let table_schema = TableSchema::new(file_schema, partition_cols);
    ///
    /// // Table schema will have 4 columns: user_id, amount, date, region
    /// assert_eq!(table_schema.table_schema().fields().len(), 4);
    /// ```
    pub fn new(file_schema: SchemaRef, table_partition_cols: Vec<FieldRef>) -> Self {
        let mut builder = SchemaBuilder::from(file_schema.as_ref());
        builder.extend(table_partition_cols.iter().cloned());
        Self {
            file_schema,
            table_partition_cols: Arc::new(table_partition_cols),
            table_schema: Arc::new(builder.finish()),
        }
    }

    /// Create a new TableSchema with no partition columns.
    ///
    /// You should prefer calling [`TableSchema::new`] if you have partition columns at
    /// construction time since it avoids re-computing the table schema.
    pub fn from_file_schema(file_schema: SchemaRef) -> Self {
        Self::new(file_schema, vec![])
    }

    /// Add partition columns to an existing TableSchema, returning a new instance.
    ///
    /// You should prefer calling [`TableSchema::new`] instead of chaining [`TableSchema::from_file_schema`]
    /// into [`TableSchema::with_table_partition_cols`] if you have partition columns at construction time
    /// since it avoids re-computing the table schema.
    pub fn with_table_partition_cols(mut self, partition_cols: Vec<FieldRef>) -> Self {
        if self.table_partition_cols.is_empty() {
            self.table_partition_cols = Arc::new(partition_cols);
        } else {
            // Append to existing partition columns
            let table_partition_cols = Arc::get_mut(&mut self.table_partition_cols).expect(
                "Expected to be the sole owner of table_partition_cols since this function accepts mut self",
            );
            table_partition_cols.extend(partition_cols);
        }
        let mut builder = SchemaBuilder::from(self.file_schema.as_ref());
        builder.extend(self.table_partition_cols.iter().cloned());
        self.table_schema = Arc::new(builder.finish());
        self
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
    pub fn table_partition_cols(&self) -> &Vec<FieldRef> {
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
        Self::from_file_schema(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::TableSchema;
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

        let table_schema = TableSchema::new(file_schema.clone(), partition_cols.clone());

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
    fn test_add_multiple_partition_columns() {
        let file_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let initial_partition_cols =
            vec![Arc::new(Field::new("country", DataType::Utf8, false))];

        let table_schema = TableSchema::new(file_schema.clone(), initial_partition_cols);

        let additional_partition_cols = vec![
            Arc::new(Field::new("city", DataType::Utf8, false)),
            Arc::new(Field::new("year", DataType::Int32, false)),
        ];

        let updated_table_schema =
            table_schema.with_table_partition_cols(additional_partition_cols);

        // Verify file schema remains unchanged
        assert_eq!(
            updated_table_schema.file_schema().as_ref(),
            file_schema.as_ref()
        );

        // Verify partition columns
        assert_eq!(updated_table_schema.table_partition_cols().len(), 3);
        assert_eq!(
            updated_table_schema.table_partition_cols()[0].name(),
            "country"
        );
        assert_eq!(
            updated_table_schema.table_partition_cols()[1].name(),
            "city"
        );
        assert_eq!(
            updated_table_schema.table_partition_cols()[2].name(),
            "year"
        );

        // Verify full table schema
        let expected_fields = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("country", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("year", DataType::Int32, false),
        ];
        let expected_schema = Schema::new(expected_fields);
        assert_eq!(
            updated_table_schema.table_schema().as_ref(),
            &expected_schema
        );
    }
}
