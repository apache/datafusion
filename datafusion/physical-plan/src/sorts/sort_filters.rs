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

use arrow_schema::SortOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    expressions::{is_not_null, is_null, lit, BinaryExpr},
    LexOrdering, PhysicalExpr,
};

use crate::dynamic_filters::{DynamicFilterPhysicalExpr, DynamicFilterSource};

/// Holds threshold value and sort order information for a column
#[derive(Debug, Clone)]
struct ColumnThreshold {
    /// The current threshold value
    pub value: Option<ScalarValue>,
    /// The column expression
    pub expr: Arc<dyn PhysicalExpr>,
    /// Sort options
    pub sort_options: SortOptions,
}

/// Pushdown of dynamic fitlers from sort + limit operators (aka `TopK`) is used to speed up queries
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
#[derive(Debug)]
pub struct SortDynamicFilterSource {
    thresholds: Arc<RwLock<Vec<ColumnThreshold>>>,
}

impl SortDynamicFilterSource {
    pub fn new(ordering: &LexOrdering) -> Self {
        let thresholds = ordering
            .iter()
            .map(|sort_expr| ColumnThreshold {
                value: None,
                expr: Arc::clone(&sort_expr.expr),
                sort_options: sort_expr.options,
            })
            .collect();

        let thresholds = Arc::new(RwLock::new(thresholds));

        Self { thresholds }
    }

    pub fn update_values(&self, new_values: &[ScalarValue]) -> Result<()> {
        let mut thresholds = self.thresholds.write().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Failed to acquire write lock on thresholds".to_string(),
            )
        })?;
        if new_values.len() != thresholds.len() {
            return Err(datafusion_common::DataFusionError::Execution(
                "The number of new values does not match the number of thresholds"
                    .to_string(),
            ));
        }
        for (i, new_value) in new_values.iter().enumerate() {
            let threshold = &mut thresholds[i];
            let descending = threshold.sort_options.descending;
            let nulls_first = threshold.sort_options.nulls_first;
            let current_value = &threshold.value;
            // Check if the new value is more or less selective than the current value given the sorting
            if let Some(current_value) = current_value {
                let new_value_is_greater = new_value > current_value;
                let new_value_is_null = new_value.is_null();
                let current_value_is_null = current_value.is_null();
                if (nulls_first && new_value_is_null && !current_value_is_null)
                    || (descending && new_value_is_greater)
                    || (!descending && !new_value_is_greater)
                {
                    // *current_value = new_value.clone();
                    threshold.value = Some(new_value.clone());
                }
            } else {
                threshold.value = Some(new_value.clone());
            }
        }
        Ok(())
    }

    pub fn as_physical_expr(self: &Arc<Self>) -> Result<Arc<dyn PhysicalExpr>> {
        let children = self
            .thresholds
            .read()
            .map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Failed to acquire read lock on thresholds".to_string(),
                )
            })?
            .iter()
            .map(|threshold| Arc::clone(&threshold.expr))
            .collect::<Vec<_>>();
        Ok(Arc::new(DynamicFilterPhysicalExpr::new(
            children,
            Arc::clone(self) as Arc<dyn DynamicFilterSource>,
        )))
    }
}

impl DynamicFilterSource for SortDynamicFilterSource {
    fn snapshot_current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
        let thresholds = self.thresholds.read().map_err(|_| {
            datafusion_common::DataFusionError::Execution(
                "Failed to acquire read lock on thresholds".to_string(),
            )
        })?;
        // Create filter expressions for each threshold
        let mut filters: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(thresholds.len());

        let mut prev_sort_expr: Option<Arc<dyn PhysicalExpr>> = None;
        for threshold in thresholds.iter() {
            let value = &threshold.value;

            let Some(value) = value else {
                // If the value is None, we cannot create a filter for this threshold
                // This means we skip this column for filtering
                continue;
            };

            // Create the appropriate operator based on sort order
            let op = if threshold.sort_options.descending {
                // For descending sort, we want col > threshold (exclude smaller values)
                Operator::Gt
            } else {
                // For ascending sort, we want col < threshold (exclude larger values)
                Operator::Lt
            };

            let value_null = value.is_null();

            let comparison = Arc::new(BinaryExpr::new(
                Arc::clone(&threshold.expr),
                op,
                lit(value.clone()),
            ));

            let comparison_with_null =
                match (threshold.sort_options.nulls_first, value_null) {
                    // For nulls first, transform to (threshold.value is not null) and (threshold.expr is null or comparison)
                    (true, true) => lit(false),
                    (true, false) => Arc::new(BinaryExpr::new(
                        is_null(Arc::clone(&threshold.expr))?,
                        Operator::Or,
                        comparison,
                    )),
                    // For nulls last, transform to (threshold.value is null and threshold.expr is not null)
                    // or (threshold.value is not null and comparison)
                    (false, true) => is_not_null(Arc::clone(&threshold.expr))?,
                    (false, false) => comparison,
                };

            let mut eq_expr = Arc::new(BinaryExpr::new(
                Arc::clone(&threshold.expr),
                Operator::Eq,
                lit(value.clone()),
            ));

            if value_null {
                eq_expr = Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&threshold.expr))?,
                    Operator::Or,
                    eq_expr,
                ));
            }

            // For a query like order by a, b, the filter for column `b` is only applied if
            // the condition a = threshold.value (considering null equality) is met.
            // Therefore, we add equality predicates for all preceding fields to the filter logic of the current field,
            // and include the current field's equality predicate in `prev_sort_expr` for use with subsequent fields.
            match prev_sort_expr.take() {
                None => {
                    prev_sort_expr = Some(eq_expr);
                    filters.push(comparison_with_null);
                }
                Some(p) => {
                    filters.push(Arc::new(BinaryExpr::new(
                        Arc::clone(&p),
                        Operator::And,
                        comparison_with_null,
                    )));

                    prev_sort_expr =
                        Some(Arc::new(BinaryExpr::new(p, Operator::And, eq_expr)));
                }
            }
        }

        let dynamic_predicate = filters
            .into_iter()
            .reduce(|a, b| Arc::new(BinaryExpr::new(a, Operator::Or, b)));

        if let Some(predicate) = dynamic_predicate {
            if !predicate.eq(&lit(true)) {
                return Ok(vec![predicate]);
            }
        }
        Ok(vec![])
    }
}
