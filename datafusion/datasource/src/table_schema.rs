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

/// Helper to hold table schema information for partitioned data sources.
///
/// When reading partitioned data (such as Hive-style partitioning), a table's schema
/// consists of two parts:
/// 1. **File schema**: The schema of the actual data files on disk
/// 2. **Partition columns**: Columns that are encoded in the directory structure,
///    not stored in the files themselves
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
    table_partition_cols: Vec<FieldRef>,

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
            table_partition_cols,
            table_schema: Arc::new(builder.finish()),
        }
    }

    /// Create a new TableSchema from a file schema with no partition columns.
    pub fn from_file_schema(file_schema: SchemaRef) -> Self {
        Self::new(file_schema, vec![])
    }

    /// Set the table partition columns and rebuild the table schema.
    pub fn with_table_partition_cols(
        mut self,
        table_partition_cols: Vec<FieldRef>,
    ) -> TableSchema {
        self.table_partition_cols = table_partition_cols;
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
