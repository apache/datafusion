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

//! Deprecated: [`SchemaAdapter`] and [`SchemaAdapterFactory`] have been removed.
//!
//! Use [`PhysicalExprAdapterFactory`] instead. See `upgrading.md` for more details.
//!
//! [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory

#![allow(deprecated)]

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion_common::{ColumnStatistics, Result, not_impl_err};
use log::warn;
use std::fmt::Debug;
use std::sync::Arc;

/// Deprecated: Function type for casting columns.
///
/// This type has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "SchemaAdapter has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
pub type CastColumnFn = dyn Fn(&ArrayRef, &Field, &arrow::compute::CastOptions) -> Result<ArrayRef>
    + Send
    + Sync;

/// Deprecated: Factory for creating [`SchemaAdapter`].
///
/// This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "SchemaAdapter has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
pub trait SchemaAdapterFactory: Debug + Send + Sync + 'static {
    /// Create a [`SchemaAdapter`]
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter>;

    /// Create a [`SchemaAdapter`] using only the projected table schema.
    fn create_with_projected_schema(
        &self,
        projected_table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        self.create(Arc::clone(&projected_table_schema), projected_table_schema)
    }
}

/// Deprecated: Creates [`SchemaMapper`]s to map file-level [`RecordBatch`]es to a table schema.
///
/// This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "SchemaAdapter has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
pub trait SchemaAdapter: Send + Sync {
    /// Map a column index in the table schema to a column index in a particular file schema.
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize>;

    /// Creates a mapping for casting columns from the file schema to the table schema.
    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)>;
}

/// Deprecated: Maps columns from a specific file schema to the table schema.
///
/// This trait has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "SchemaMapper has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
pub trait SchemaMapper: Debug + Send + Sync {
    /// Adapts a `RecordBatch` to match the `table_schema`.
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Adapts file-level column `Statistics` to match the `table_schema`.
    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>>;
}

/// Deprecated: Default [`SchemaAdapterFactory`] for mapping schemas.
///
/// This struct has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "DefaultSchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
#[derive(Clone, Debug, Default)]
pub struct DefaultSchemaAdapterFactory;

impl SchemaAdapterFactory for DefaultSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeprecatedSchemaAdapter {
            _projected_table_schema: projected_table_schema,
        })
    }
}

impl DefaultSchemaAdapterFactory {
    /// Deprecated: Create a new factory for mapping batches from a file schema to a table schema.
    #[deprecated(
        since = "52.0.0",
        note = "DefaultSchemaAdapterFactory has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
    )]
    pub fn from_schema(table_schema: SchemaRef) -> Box<dyn SchemaAdapter> {
        // Note: this method did not return an error thus the errors are raised from the returned adapter
        warn!(
            "DefaultSchemaAdapterFactory::from_schema is deprecated. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
        );
        Box::new(DeprecatedSchemaAdapter {
            _projected_table_schema: table_schema,
        })
    }
}

/// Internal deprecated adapter that returns errors when methods are called.
struct DeprecatedSchemaAdapter {
    _projected_table_schema: SchemaRef,
}

impl SchemaAdapter for DeprecatedSchemaAdapter {
    fn map_column_index(&self, _index: usize, _file_schema: &Schema) -> Option<usize> {
        None // Safe no-op
    }

    fn map_schema(
        &self,
        _file_schema: &Schema,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        not_impl_err!(
            "SchemaAdapter has been removed. Use PhysicalExprAdapterFactory instead. \
            See upgrading.md for more details."
        )
    }
}

/// Deprecated: The SchemaMapping struct held a mapping from the file schema to the table schema.
///
/// This struct has been removed. Use [`PhysicalExprAdapterFactory`] instead.
/// See `upgrading.md` for more details.
///
/// [`PhysicalExprAdapterFactory`]: datafusion_physical_expr_adapter::PhysicalExprAdapterFactory
#[deprecated(
    since = "52.0.0",
    note = "SchemaMapping has been removed. Use PhysicalExprAdapterFactory instead. See upgrading.md for more details."
)]
#[derive(Debug)]
pub struct SchemaMapping {
    // Private fields removed - this is a skeleton for deprecation purposes only
    _private: (),
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, _batch: RecordBatch) -> Result<RecordBatch> {
        not_impl_err!(
            "SchemaMapping has been removed. Use PhysicalExprAdapterFactory instead. \
            See upgrading.md for more details."
        )
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        not_impl_err!(
            "SchemaMapping has been removed. Use PhysicalExprAdapterFactory instead. \
            See upgrading.md for more details."
        )
    }
}
