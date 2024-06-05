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

//! [`EliminateFilter`] replaces `where false` or `where null` with an empty relation.

use datafusion_common::tree_node::Transformed;
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::logical_plan::tree_node::unwrap_arc;
use datafusion_expr::{EmptyRelation, Expr, Filter, LogicalPlan};

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

/// Optimization rule that eliminate the scalar value (true/false/null) filter
/// with an [LogicalPlan::EmptyRelation]
///
/// This saves time in planning and executing the query.
/// Note that this rule should be applied after simplify expressions optimizer rule.
#[derive(Default)]
pub struct EliminateFilter;

impl EliminateFilter {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateFilter {
    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        internal_err!("Should have called EliminateFilter::rewrite")
    }

    fn name(&self) -> &str {
        "eliminate_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(Filter {
                predicate: Expr::Literal(ScalarValue::Boolean(v)),
                input,
                ..
            }) => match v {
                Some(true) => Ok(Transformed::yes(unwrap_arc(input))),
                Some(false) | None => Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                    EmptyRelation {
                        produce_one_row: false,
                        schema: input.schema().clone(),
                    },
                ))),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}
