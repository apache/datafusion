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

//! [`AccessPlanOptimizer`] trait and implementations for optimizing
//! row group access order during parquet scans.
//!
//! Applied after row group pruning but before building the decoder,
//! these optimizers reorder (or reverse) the row groups to improve
//! query performance — e.g., placing the "best" row groups first
//! so TopK's dynamic filter threshold tightens quickly.

use crate::access_plan::PreparedAccessPlan;
use arrow::datatypes::Schema;
use datafusion_common::Result;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
use parquet::file::metadata::ParquetMetaData;
use std::fmt::Debug;

/// Optimizes the row group access order for a prepared access plan.
///
/// Implementations can reorder, reverse, or otherwise transform the
/// row group read order to improve scan performance. The optimizer
/// is applied once per file, after all pruning passes are complete.
///
/// # Examples
///
/// - [`ReverseRowGroups`]: simple O(n) reversal for DESC on ASC-sorted data
/// - [`ReorderByStatistics`]: sort row groups by min/max statistics
///   so TopK queries find optimal values first
pub(crate) trait AccessPlanOptimizer: Send + Sync + Debug {
    /// Transform the prepared access plan.
    ///
    /// Implementations should return the plan unchanged if they cannot
    /// apply their optimization (e.g., missing statistics).
    fn optimize(
        &self,
        plan: PreparedAccessPlan,
        file_metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<PreparedAccessPlan>;
}

/// Reverse the row group order — simple O(n) reversal.
///
/// Used as a fallback when the sort column has no statistics available.
/// For ASC-sorted files with a DESC query, reversing row groups places
/// the highest-value row groups first.
#[derive(Debug)]
pub(crate) struct ReverseRowGroups;

impl AccessPlanOptimizer for ReverseRowGroups {
    fn optimize(
        &self,
        plan: PreparedAccessPlan,
        file_metadata: &ParquetMetaData,
        _arrow_schema: &Schema,
    ) -> Result<PreparedAccessPlan> {
        plan.reverse(file_metadata)
    }
}

/// Reorder row groups by min/max statistics of the sort column.
///
/// For ASC sort: row groups with the smallest min come first.
/// For DESC sort: row groups with the largest max come first.
///
/// This is more effective than [`ReverseRowGroups`] when row groups
/// are out of order (e.g., append-heavy workloads), because it uses
/// actual statistics rather than assuming the original order is sorted.
///
/// Gracefully falls back to the original order when statistics are
/// unavailable, the sort expression is not a simple column, etc.
#[derive(Debug)]
pub(crate) struct ReorderByStatistics {
    sort_order: LexOrdering,
}

impl ReorderByStatistics {
    pub(crate) fn new(sort_order: LexOrdering) -> Self {
        Self { sort_order }
    }
}

impl AccessPlanOptimizer for ReorderByStatistics {
    fn optimize(
        &self,
        plan: PreparedAccessPlan,
        file_metadata: &ParquetMetaData,
        arrow_schema: &Schema,
    ) -> Result<PreparedAccessPlan> {
        plan.reorder_by_statistics(&self.sort_order, file_metadata, arrow_schema)
    }
}
