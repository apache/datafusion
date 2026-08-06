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

//! Scan-local cache for reusable Parquet pruning setup.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, ScalarValue};
use datafusion_functions::core::input_file_name::InputFileNameFunc;
use datafusion_physical_expr::expressions::DynamicFilterTracking;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_adapter::rewrite::expr_references_scalar_udf;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_pruning::PruningPredicate;
use parking_lot::Mutex;

/// Scan-local cache for CPU-only pruning setup that can be reused across files
/// with the same adapted expression inputs and physical schema.
#[derive(Debug, Default)]
pub(crate) struct ParquetPruningSetupCache {
    entries: Mutex<HashMap<ParquetPruningSetupCacheKey, ParquetPruningSetup>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ParquetPruningSetupCacheKey {
    // Schema coercions such as INT96 resolution and file-schema type coercions
    // are included through the final physical schema used for adaptation.
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
    // Page-index options are intentionally not part of this key because page
    // pruning predicates are built after this cache entry is applied.
    predicate_ptr: Option<usize>,
    // The projection and predicate are scan-level inputs once literal column
    // replacement has been ruled out, so pointer identity is stable within the
    // scan and avoids structural expression hashing.
    projection_expr_ptrs: Vec<usize>,
}

impl ParquetPruningSetupCacheKey {
    fn new(
        logical_file_schema: &SchemaRef,
        physical_file_schema: &SchemaRef,
        projection: &ProjectionExprs,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Self {
        Self {
            logical_file_schema: Arc::clone(logical_file_schema),
            physical_file_schema: Arc::clone(physical_file_schema),
            predicate_ptr: predicate.map(physical_expr_ptr),
            projection_expr_ptrs: projection
                .iter()
                .map(|expr| physical_expr_ptr(&expr.expr))
                .collect(),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct ParquetPruningSetup {
    pub(super) projection: ProjectionExprs,
    pub(super) predicate: Option<Arc<dyn PhysicalExpr>>,
    pub(super) pruning_predicate: Option<Arc<PruningPredicate>>,
}

impl ParquetPruningSetupCache {
    /// Return whether the original scan expressions can produce a setup shared
    /// by multiple files.
    ///
    /// Literal replacement is file-local: partition values and constant-column
    /// statistics change the expression and pruning predicate but are not in
    /// the cache key. Dynamic filters and `input_file_name()` are likewise
    /// file-specific, so each bypasses the cache.
    pub(super) fn is_pruning_setup_reusable(
        projection: &ProjectionExprs,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
        literal_columns: &HashMap<String, ScalarValue>,
    ) -> bool {
        let has_dynamic_predicate = predicate.is_some_and(|predicate| {
            DynamicFilterTracking::classify(predicate).contains_dynamic_filter()
        });
        let has_input_file_name_projection = projection
            .iter()
            .any(|expr| expr_references_scalar_udf::<InputFileNameFunc>(&expr.expr));

        literal_columns.is_empty()
            && !has_dynamic_predicate
            && !has_input_file_name_projection
    }

    pub(super) fn get_or_insert_with(
        &self,
        logical_file_schema: &SchemaRef,
        physical_file_schema: &SchemaRef,
        projection: &ProjectionExprs,
        predicate: Option<&Arc<dyn PhysicalExpr>>,
        make_setup: impl FnOnce() -> Result<ParquetPruningSetup>,
    ) -> Result<ParquetPruningSetup> {
        let key = ParquetPruningSetupCacheKey::new(
            logical_file_schema,
            physical_file_schema,
            projection,
            predicate,
        );
        if let Some(setup) = self.entries.lock().get(&key) {
            return Ok(setup.clone());
        }

        // Compute outside the cache lock. Concurrent first misses for the same
        // key may duplicate this CPU-only setup, but the first completed insert
        // still makes subsequent files reuse the cached entry. Reintroduce
        // single-flight coordination only if profiling shows duplicate setup is
        // material.
        let setup = make_setup()?;
        self.entries.lock().insert(key, setup.clone());
        Ok(setup)
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

fn physical_expr_ptr(expr: &Arc<dyn PhysicalExpr>) -> usize {
    Arc::as_ptr(expr) as *const () as usize
}
