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

//! Table source

use crate::{Expr, LogicalPlan};

use arrow::datatypes::SchemaRef;
use datafusion_common::{Constraints, Result};

use std::{any::Any, borrow::Cow};

/// Indicates how a filter expression is handled by
/// [`TableProvider::scan`].
///
/// Filter expressions are boolean expressions used to reduce the number of
/// rows that are read from a table. Only rows that evaluate to `true` ("pass
/// the filter") are returned. Rows that evaluate to `false` or `NULL` are
/// omitted.
///
/// [`TableProvider::scan`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html#tymethod.scan
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableProviderFilterPushDown {
    /// The filter cannot be used by the provider and will not be pushed down.
    Unsupported,
    /// The filter can be used, but the provider might still return some tuples
    /// that do not pass the filter.
    ///
    /// In this case, DataFusion applies an additional `Filter` operation
    /// after the scan to ensure all rows are filtered correctly.
    Inexact,
    /// The provider **guarantees** that it will omit **only** tuples which
    /// pass the filter.
    ///
    /// In this case, DataFusion will not apply additional filtering.
    Exact,
}

/// Indicates the type of this table for metadata/catalog purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialized table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

impl std::fmt::Display for TableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableType::Base => write!(f, "Base"),
            TableType::View => write!(f, "View"),
            TableType::Temporary => write!(f, "Temporary"),
        }
    }
}

/// Planning time information about a table.
///
/// This trait is used during logical query planning and optimizations, and
/// provides a subset of the [`TableProvider`] trait, such as schema information
/// and filter push-down capabilities. The [`TableProvider`] trait provides
/// additional information needed for physical query execution, such as the
/// ability to perform a scan or insert data.
///
/// # See Also:
///
/// [`DefaultTableSource`]  to go from [`TableProvider`], to `TableSource`
///
/// # Rationale
///
/// The reason for having two separate traits is to avoid having the logical
/// plan code be dependent on the DataFusion execution engine. Some projects use
/// DataFusion's logical plans and have their own execution engine.
///
/// [`TableProvider`]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
/// [`DefaultTableSource`]: https://docs.rs/datafusion/latest/datafusion/datasource/default_table_source/struct.DefaultTableSource.html
pub trait TableSource: Sync + Send {
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Get primary key indices, if any
    fn constraints(&self) -> Option<&Constraints> {
        None
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimize data retrieval. Only non-volatile expressions are passed to this function.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok((0..filters.len())
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    /// Get the Logical plan of this table provider, if available.
    ///
    /// For example, a view may have a logical plan, but a CSV file does not.
    fn get_logical_plan(&self) -> Option<Cow<LogicalPlan>> {
        None
    }

    /// Get the default value for a column, if available.
    fn get_column_default(&self, _column: &str) -> Option<&Expr> {
        None
    }
}
