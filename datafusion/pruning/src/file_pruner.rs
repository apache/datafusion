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

//! File-level pruning based on partition values and file-level statistics

use std::sync::Arc;

use arrow::datatypes::{FieldRef, SchemaRef};
use datafusion_common::{Result, internal_datafusion_err, pruning::PrunableStatistics};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr_common::physical_expr::{PhysicalExpr, snapshot_generation};
use datafusion_physical_plan::metrics::Count;
use log::debug;

use crate::build_pruning_predicate;

/// Prune based on file-level statistics.
///
/// Note: Partition column pruning is handled earlier via `replace_columns_with_literals`
/// which substitutes partition column references with their literal values before
/// the predicate reaches this pruner.
pub struct FilePruner {
    predicate_generation: Option<u64>,
    predicate: Arc<dyn PhysicalExpr>,
    /// Schema used for pruning (the logical file schema).
    file_schema: SchemaRef,
    file_stats_pruning: PrunableStatistics,
    predicate_creation_errors: Count,
}

impl FilePruner {
    #[deprecated(
        since = "52.0.0",
        note = "Use `try_new` instead which returns None if no statistics are available"
    )]
    #[expect(clippy::needless_pass_by_value)]
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        logical_file_schema: &SchemaRef,
        _partition_fields: Vec<FieldRef>,
        partitioned_file: PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Result<Self> {
        Self::try_new(
            predicate,
            logical_file_schema,
            &partitioned_file,
            predicate_creation_errors,
        )
        .ok_or_else(|| {
            internal_datafusion_err!(
                "FilePruner::new called on a file without statistics: {:?}",
                partitioned_file
            )
        })
    }

    /// Create a new file pruner if statistics are available.
    /// Returns None if this file does not have statistics.
    pub fn try_new(
        predicate: Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        partitioned_file: &PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Option<Self> {
        let file_stats = partitioned_file.statistics.as_ref()?;
        let file_stats_pruning =
            PrunableStatistics::new(vec![file_stats.clone()], Arc::clone(file_schema));
        Some(Self {
            predicate_generation: None,
            predicate,
            file_schema: Arc::clone(file_schema),
            file_stats_pruning,
            predicate_creation_errors,
        })
    }

    pub fn should_prune(&mut self) -> Result<bool> {
        // Check if the predicate has changed since last invocation by tracking
        // its "generation". Dynamic filter expressions can change their values
        // during query execution, so we use generation tracking to detect when
        // the predicate has been updated and needs to be rebuilt.
        //
        // If the generation hasn't changed, we can skip rebuilding the pruning
        // predicate, which is an expensive operation involving expression analysis.
        let new_generation = snapshot_generation(&self.predicate);
        if let Some(current_generation) = self.predicate_generation.as_mut() {
            if *current_generation == new_generation {
                return Ok(false);
            }
            *current_generation = new_generation;
        } else {
            self.predicate_generation = Some(new_generation);
        }
        let pruning_predicate = build_pruning_predicate(
            Arc::clone(&self.predicate),
            &self.file_schema,
            &self.predicate_creation_errors,
        );
        let Some(pruning_predicate) = pruning_predicate else {
            return Ok(false);
        };
        match pruning_predicate.prune(&self.file_stats_pruning) {
            Ok(values) => {
                assert!(values.len() == 1);
                // We expect a single container -> if all containers are false skip this file
                if values.into_iter().all(|v| !v) {
                    return Ok(true);
                }
            }
            // Stats filter array could not be built, so we can't prune
            Err(e) => {
                debug!("Ignoring error building pruning predicate for file: {e}");
                self.predicate_creation_errors.add(1);
            }
        }

        Ok(false)
    }
}
