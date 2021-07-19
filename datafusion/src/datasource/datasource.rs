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

//! Data source traits

use std::any::Any;
use std::sync::Arc;

use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::ExecutionPlan;
use crate::{arrow::datatypes::SchemaRef, scalar::ScalarValue};

/// This table statistics are estimates.
/// It can not be used directly in the precise compute
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Statistics {
    /// The number of table rows
    pub num_rows: Option<usize>,
    /// total byte of the table rows
    pub total_byte_size: Option<usize>,
    /// Statistics on a column level
    pub column_statistics: Option<Vec<ColumnStatistics>>,
}
/// This table statistics are estimates about column
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnStatistics {
    /// Number of null values on column
    pub null_count: Option<usize>,
    /// Maximum value of column
    pub max_value: Option<ScalarValue>,
    /// Minimum value of column
    pub min_value: Option<ScalarValue>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
}

/// Indicates whether and how a filter expression can be handled by a
/// TableProvider for table scans.
#[derive(Debug, Clone, PartialEq)]
pub enum TableProviderFilterPushDown {
    /// The expression cannot be used by the provider.
    Unsupported,
    /// The expression can be used to help minimise the data retrieved,
    /// but the provider cannot guarantee that all returned tuples
    /// satisfy the filter. The Filter plan node containing this expression
    /// will be preserved.
    Inexact,
    /// The provider guarantees that all returned data satisfies this
    /// filter expression. The Filter plan node containing this expression
    /// will be removed.
    Exact,
}

/// Indicates the type of this table for metadata/catalog purposes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TableType {
    /// An ordinary physical table.
    Base,
    /// A non-materialised table that itself uses a query internally to provide data.
    View,
    /// A transient table.
    Temporary,
}

/// Source table
pub trait TableProvider: Sync + Send {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef;

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Create an ExecutionPlan that will scan the table.
    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        filters: &[Expr],
        // limit can be used to reduce the amount scanned
        // from the datasource as a performance optimization.
        // If set, it contains the amount of rows needed by the `LogicalPlan`,
        // The datasource should return *at least* this number of rows if available.
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the table Statistics
    /// Statistics should be optional because not all data sources can provide statistics.
    fn statistics(&self) -> Statistics;

    /// Returns whether statistics provided are exact values or estimates
    fn has_exact_statistics(&self) -> bool {
        false
    }

    /// Tests whether the table provider can make use of a filter expression
    /// to optimise data retrieval.
    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
