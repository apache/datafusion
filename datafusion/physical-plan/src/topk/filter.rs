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

use std::sync::{Arc, RwLock};

use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_physical_expr::expressions::lit;
use datafusion_physical_expr::PhysicalExpr;

use crate::dynamic_filters::{DynamicFilterPhysicalExpr, DynamicFilterSource};

/// Pushdown of dynamic fitlers from TopK operators is used to speed up queries
/// such as `SELECT * FROM table ORDER BY col DESC LIMIT 10` by pushing down the
/// threshold values for the sort columns to the data source.
/// That is, the TopK operator will keep track of the top 10 values for the sort
/// and before a new file is opened it's statitics will be checked against the
/// threshold values to determine if the file can be skipped and predicate pushdown
/// will use these to skip rows during the scan.
///
/// For example, imagine this data gets created if multiple sources with clock skews,
/// network delays, etc. are writing data and you don't do anything fancy to guarantee
/// perfect sorting by `timestamp` (i.e. you naively write out the data to Parquet, maybe do some compaction, etc.).
/// The point is that 99% of yesterday's files have a `timestamp` smaller than 99% of today's files
/// but there may be a couple seconds of overlap between files.
/// To be concrete, let's say this is our data:
//
// | file | min | max |
// |------|-----|-----|
// | 1    | 1   | 10  |
// | 2    | 9   | 19  |
// | 3    | 20  | 31  |
// | 4    | 30  | 35  |
//
// Ideally a [`TableProvider`] is able to use file level stats or other methods to roughly order the files
// within each partition / file group such that we start with the newest / largest `timestamp`s.
// If this is not possible the optimization still works but is less efficient and harder to visualize,
// so for this example let's assume that we process 1 file at a time and we started with file 4.
// After processing file 4 let's say we have 10 values in our TopK heap, the smallest of which is 30.
// The TopK operator will then push down the filter `timestamp < 30` down the tree of [`ExecutionPlan`]s
// and if the data source supports dynamic filter pushdown it will accept a reference to this [`DynamicPhysicalExprSource`]
// and when it goes to open file 3 it will ask the [`DynamicPhysicalExprSource`] for the current filters.
// Since file 3 may contain values larger than 30 we cannot skip it entirely,
// but scanning it may still be more efficient due to page pruning and other optimizations.
// Once we get to file 2 however we can skip it entirely because we know that all values in file 2 are smaller than 30.
// The same goes for file 1.
// So this optimization just saved us 50% of the work of scanning the data.
#[derive(Debug, Clone)]
pub struct TopKDynamicFilterSource {
    /// The children of the dynamic filters produced by this TopK.
    /// In particular, this is the columns that are being sorted, derived from the sorting expressions.
    children: Vec<Arc<dyn PhysicalExpr>>,
    /// The current filters derived from this TopK
    predicate: Arc<RwLock<Arc<dyn PhysicalExpr>>>,
}

impl TopKDynamicFilterSource {
    pub fn new(children: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            children,
            predicate: Arc::new(RwLock::new(lit(true))),
        }
    }

    pub fn update_filters(&self, predicate: Arc<dyn PhysicalExpr>) -> Result<()> {
        let mut current_predicate = self.predicate.write().map_err(|_| {
            DataFusionError::Internal(
                "Failed to acquire write lock on TopKDynamicPhysicalExprSource"
                    .to_string(),
            )
        })?;
        *current_predicate = predicate;
        Ok(())
    }
}

impl TopKDynamicFilterSource {
    pub fn as_dynamic_physical_expr(&self) -> Arc<dyn PhysicalExpr> {
        let new = self.clone();
        // Transform the sort expresions into referenced columns
        let children = self.children.clone();
        Arc::new(DynamicFilterPhysicalExpr::new(children, Arc::new(new)))
    }
}

impl DynamicFilterSource for TopKDynamicFilterSource {
    fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let predicate = self
            .predicate
            .read()
            .map_err(|_| {
                DataFusionError::Internal(
                    "Failed to acquire read lock on TopKDynamicPhysicalExprSource"
                        .to_string(),
                )
            })?
            .clone();
        Ok(vec![predicate])
    }
}
