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

use std::{
    fmt::Display,
    hash::{Hash, Hasher},
    sync::{Arc, RwLock},
};

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::{
    expressions::{is_not_null, is_null, lit, BinaryExpr},
    LexOrdering, PhysicalExpr,
};

use crate::dynamic_filters::{DynamicFilterPhysicalExpr, DynamicFilterSource};

/// Pushdown of dynamic filters from sort + limit operators (aka `TopK`) is used to speed up queries
/// such as `SELECT * FROM table ORDER BY col DESC LIMIT 10` by pushing down the
/// threshold values for the sort columns to the data source.
/// That is, the TopK operator will keep track of the top 10 values for the sort
/// and before a new file is opened its statistics will be checked against the
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
    /// Sort expressions
    expr: LexOrdering,
    /// Current threshold values
    thresholds: Arc<RwLock<Vec<Option<ScalarValue>>>>,
}

impl Hash for SortDynamicFilterSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the pointers to the thresholds
        let thresholds = Arc::as_ptr(&self.thresholds) as usize;
        thresholds.hash(state);
    }
}

impl PartialEq for SortDynamicFilterSource {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.thresholds, &other.thresholds)
    }
}

impl Eq for SortDynamicFilterSource {}

impl SortDynamicFilterSource {
    pub fn new(expr: LexOrdering) -> Self {
        let thresholds = Arc::new(RwLock::new(vec![None; expr.len()]));
        Self { expr, thresholds }
    }

    pub fn update_values(&self, new_values: &[ScalarValue]) -> Result<()> {
        let replace = {
            let thresholds = self.thresholds.read().map_err(|_| {
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
            // We need to decide if these values replace our current values or not.
            // They only replace our current values if they would sort before them given our sorting expression.
            // Importantly, since this may be a multi-expressions sort, we need to check that **the entire expression**
            // sorts before the current set of values, not just one column.
            // This means that if we have a sort expression like `a, b` and the new value is `a = 1, b = 2`
            // and the current value is `a = 1, b = 3` we need to check that `a = 1, b = 2` sorts before `a = 1, b = 3`
            // and not just that `a = 1` sorts before `a = 1`.
            // We also have to handle ASC/DESC and NULLS FIRST/LAST for each column.
            let mut replace = true;
            for (i, new_value) in new_values.iter().enumerate() {
                let current_value = &thresholds[i];
                let sort_expr = &self.expr[i];
                let descending = sort_expr.options.descending;
                let nulls_first = sort_expr.options.nulls_first;
                if let Some(current_value) = current_value {
                    let new_value_is_greater_than_current = new_value.gt(current_value);
                    let new_value_is_null = new_value.is_null();
                    let current_value_is_null = current_value.is_null();
                    // Handle the null cases
                    if current_value_is_null && !new_value_is_null && nulls_first {
                        replace = false;
                        break;
                    }
                    if new_value_is_null && !current_value_is_null && !nulls_first {
                        replace = false;
                        break;
                    }
                    // Handle the descending case
                    if descending {
                        if new_value_is_greater_than_current {
                            replace = false;
                            break;
                        }
                    } else {
                        if !new_value_is_greater_than_current {
                            replace = false;
                            break;
                        }
                    }
                    // Handle the equality case
                    if new_value.eq(current_value) {
                        replace = false;
                        break;
                    }
                }
            }
            replace
        };
        if replace {
            let mut thresholds = self.thresholds.write().map_err(|_| {
                datafusion_common::DataFusionError::Execution(
                    "Failed to acquire write lock on thresholds".to_string(),
                )
            })?;
            for (i, new_value) in new_values.iter().enumerate() {
                thresholds[i] = Some(new_value.clone());
            }
        }
        Ok(())
    }

    pub fn as_physical_expr(self: &Arc<Self>) -> Result<Arc<dyn PhysicalExpr>> {
        let children = self
            .expr
            .iter()
            .map(|sort_expr| Arc::clone(&sort_expr.expr))
            .collect::<Vec<_>>();
        Ok(Arc::new(DynamicFilterPhysicalExpr::new(
            children,
            Arc::clone(self) as Arc<dyn DynamicFilterSource>,
        )))
    }
}

impl Display for SortDynamicFilterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let thresholds = self
            .snapshot_current_filters()
            .map_err(|_| std::fmt::Error)?
            .iter()
            .map(|p| format!("{p}"))
            .collect::<Vec<String>>();
        let inner = thresholds.join(",");
        write!(f, "SortDynamicFilterSource[ {} ]", inner,)
    }
}

impl DynamicFilterSource for SortDynamicFilterSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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
        for (sort_expr, value) in self.expr.iter().zip(thresholds.iter()) {
            let Some(value) = value else {
                // If the value is None, we cannot create a filter for this threshold
                // This means we skip this column for filtering
                continue;
            };

            // Create the appropriate operator based on sort order
            let op = if sort_expr.options.descending {
                // For descending sort, we want col > threshold (exclude smaller values)
                Operator::Gt
            } else {
                // For ascending sort, we want col < threshold (exclude larger values)
                Operator::Lt
            };

            let value_null = value.is_null();

            let comparison = Arc::new(BinaryExpr::new(
                Arc::clone(&sort_expr.expr),
                op,
                lit(value.clone()),
            ));

            let comparison_with_null = match (sort_expr.options.nulls_first, value_null) {
                // For nulls first, transform to (threshold.value is not null) and (threshold.expr is null or comparison)
                (true, true) => lit(false),
                (true, false) => Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&sort_expr.expr))?,
                    Operator::Or,
                    comparison,
                )),
                // For nulls last, transform to (threshold.value is null and threshold.expr is not null)
                // or (threshold.value is not null and comparison)
                (false, true) => is_not_null(Arc::clone(&sort_expr.expr))?,
                (false, false) => comparison,
            };

            let mut eq_expr = Arc::new(BinaryExpr::new(
                Arc::clone(&sort_expr.expr),
                Operator::Eq,
                lit(value.clone()),
            ));

            if value_null {
                eq_expr = Arc::new(BinaryExpr::new(
                    is_null(Arc::clone(&sort_expr.expr))?,
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
