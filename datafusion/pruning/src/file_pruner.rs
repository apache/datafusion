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

use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use datafusion_common::{
    pruning::{
        CompositePruningStatistics, PartitionPruningStatistics, PrunableStatistics,
        PruningStatistics,
    },
    Result,
};
use datafusion_datasource::PartitionedFile;
use datafusion_physical_expr_common::physical_expr::{snapshot_generation, PhysicalExpr};
use datafusion_physical_plan::metrics::Count;
use itertools::Itertools;
use log::debug;

use crate::build_pruning_predicate;

/// Prune based on partition values and file-level statistics.
pub struct FilePruner {
    predicate_generation: Option<u64>,
    predicate: Arc<dyn PhysicalExpr>,
    /// Schema used for pruning, which combines the file schema and partition fields.
    /// Partition fields are always at the end, as they are during scans.
    pruning_schema: Arc<Schema>,
    file: PartitionedFile,
    partition_fields: Vec<FieldRef>,
    predicate_creation_errors: Count,
}

impl FilePruner {
    pub fn new(
        predicate: Arc<dyn PhysicalExpr>,
        logical_file_schema: &SchemaRef,
        partition_fields: Vec<FieldRef>,
        file: PartitionedFile,
        predicate_creation_errors: Count,
    ) -> Result<Self> {
        // Build a pruning schema that combines the file fields and partition fields.
        // Partition fileds are always at the end.
        let pruning_schema = Arc::new(
            Schema::new(
                logical_file_schema
                    .fields()
                    .iter()
                    .cloned()
                    .chain(partition_fields.iter().cloned())
                    .collect_vec(),
            )
            .with_metadata(logical_file_schema.metadata().clone()),
        );
        Ok(Self {
            // Initialize the predicate generation to None so that the first time we call `should_prune` we actually check the predicate
            // Subsequent calls will only do work if the predicate itself has changed.
            // See `snapshot_generation` for more info.
            predicate_generation: None,
            predicate,
            pruning_schema,
            file,
            partition_fields,
            predicate_creation_errors,
        })
    }

    pub fn should_prune(&mut self) -> Result<bool> {
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
            &self.pruning_schema,
            &self.predicate_creation_errors,
        );
        if let Some(pruning_predicate) = pruning_predicate {
            // The partition column schema is the schema of the table - the schema of the file
            let mut pruning = Box::new(PartitionPruningStatistics::try_new(
                vec![self.file.partition_values.clone()],
                self.partition_fields.clone(),
            )?) as Box<dyn PruningStatistics>;
            if let Some(stats) = &self.file.statistics {
                let stats_pruning = Box::new(PrunableStatistics::new(
                    vec![Arc::clone(stats)],
                    Arc::clone(&self.pruning_schema),
                ));
                pruning = Box::new(CompositePruningStatistics::new(vec![
                    pruning,
                    stats_pruning,
                ]));
            }
            match pruning_predicate.prune(pruning.as_ref()) {
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
        }

        Ok(false)
    }
}
