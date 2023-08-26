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

//! [`PartitionedFileFinder`] to scan [`ExecutionPlan`]` for input
//! partitioned file sources.

use std::sync::Arc;

use datafusion_common::{
    tree_node::{TreeNode, VisitRecursion},
    Result,
};

use crate::{datasource::listing::PartitionedFile, physical_plan::ExecutionPlan};

use super::{AvroExec, CsvExec, NdJsonExec, ParquetExec};

pub type FinderFunction =
    Box<dyn Fn(&dyn ExecutionPlan) -> Option<Vec<Vec<PartitionedFile>>>>;

/// Get all of the [`PartitionedFile`] to be scanned for an [`ExecutionPlan`]
///
/// This structure will find all `ScanFileConfig` in any built in
/// [`ExecutionPlan`] and allows finding user defined nodes as well
pub struct PartitionedFileFinder {
    custom_finder: FinderFunction,
}

impl Default for PartitionedFileFinder {
    fn default() -> Self {
        Self {
            // default custom finding function does nothing
            custom_finder: Box::new(|_plan| None),
        }
    }
}

impl PartitionedFileFinder {
    /// Create a new file finder
    pub fn new() -> Self {
        Default::default()
    }

    /// Get all [`PartitionedFile`]s that are scanned for an
    /// [`ExecutionPlan`],  by recursively checking all children
    pub fn find(&self, plan: Arc<dyn ExecutionPlan>) -> Vec<Vec<Vec<PartitionedFile>>> {
        let mut collector: Vec<Vec<Vec<PartitionedFile>>> = vec![];
        plan.apply(&mut |plan| {
            if let Some(files) = self.get_files(plan.as_ref()) {
                collector.push(files);
            }
            Ok(VisitRecursion::Continue)
        })
        .expect("infallable");
        collector
    }

    /// Provide a custom method to find `PartitionedFiles` for
    /// `ExecutionPlans`
    ///
    /// Called on all [`ExecutionPlan`]s other than built ins such as
    /// [`ParquetExec`] and can be used to extract
    /// [`PartitionedFile`]s from user defined nodes
    pub fn with_finder<F>(mut self, custom_finder: F) -> Self
    where
        F: Fn(&dyn ExecutionPlan) -> Option<Vec<Vec<PartitionedFile>>> + 'static,
    {
        self.custom_finder = Box::new(custom_finder);
        self
    }

    /// Return the [`PartitionedFile`] scanned by this plan node, or
    /// `None` if the plan does not can files
    fn get_files(&self, plan: &dyn ExecutionPlan) -> Option<Vec<Vec<PartitionedFile>>> {
        let plan_any = plan.as_any();
        if let Some(parquet_exec) = plan_any.downcast_ref::<ParquetExec>() {
            Some(parquet_exec.base_config().file_groups.clone())
        } else if let Some(avro_exec) = plan_any.downcast_ref::<AvroExec>() {
            Some(avro_exec.base_config().file_groups.clone())
        } else if let Some(json_exec) = plan_any.downcast_ref::<NdJsonExec>() {
            Some(json_exec.base_config().file_groups.clone())
        } else if let Some(csv_exec) = plan_any.downcast_ref::<CsvExec>() {
            Some(csv_exec.base_config().file_groups.clone())
        } else {
            (self.custom_finder)(plan)
        }
    }
}

/// Find all `[PartitionedFile]` scanned by any node in this plan.
///
/// There is one element in the returned array for each node in the
/// plan that scans files. Each element is represents the groups of
/// files that [`ExecutionPlan`] scans.
pub fn get_scan_files(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Vec<Vec<Vec<PartitionedFile>>>> {
    let files = PartitionedFileFinder::new().find(plan);
    Ok(files)
}
