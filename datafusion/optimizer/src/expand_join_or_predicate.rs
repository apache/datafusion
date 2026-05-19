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

//! [`ExpandJoinOrPredicate`] rewrites inner joins with OR filters into a UNION ALL
//! of mutually exclusive hashjoin-capable inner joins.

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use std::sync::Arc;

use datafusion_common::tree_node::Transformed;
use datafusion_common::Result;
use datafusion_expr::logical_plan::{Join, LogicalPlan, Projection, Union};
use datafusion_expr::utils::{can_hash, find_valid_equijoin_key_pair, split_binary_owned, split_conjunction_owned};
use datafusion_expr::{Expr, ExprSchemable, JoinType, Operator};

#[derive(Default, Debug)]
pub struct ExpandJoinOrPredicate;

impl ExpandJoinOrPredicate {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for ExpandJoinOrPredicate {
    fn name(&self) -> &str {
        "expand_join_or_predicate"
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Join(join) => rewrite_join(join),
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn rewrite_join(join: Join) -> Result<Transformed<LogicalPlan>> {
    let original_schema = Arc::clone(&join.schema);

    if join.join_type != JoinType::Inner || join.null_aware {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let Some(filter) = join.filter.clone() else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };

    if filter.is_volatile() {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let disjuncts = split_binary_owned(filter, Operator::Or);
    if disjuncts.len() < 2 {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    }

    let left_schema = join.left.schema();
    let right_schema = join.right.schema();

    let Some(branch_keys) = disjuncts
        .iter()
        .map(|expr| extract_hashjoin_keys(expr, left_schema, right_schema))
        .collect::<Result<Option<Vec<Vec<(Expr, Expr)>>>>>()?
    else {
        return Ok(Transformed::no(LogicalPlan::Join(join)));
    };

    let mut guards = Vec::with_capacity(disjuncts.len().saturating_sub(1));
    let mut branches = Vec::with_capacity(disjuncts.len());

    for (disjunct, keys) in disjuncts.into_iter().zip(branch_keys.into_iter()) {
        let branch_filter = guards
            .iter()
            .cloned()
            .reduce(Expr::and);

        let mut on = join.on.clone();
        on.extend(keys);

        let branch = LogicalPlan::Join(Join::try_new(
            Arc::clone(&join.left),
            Arc::clone(&join.right),
            on,
            branch_filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
            join.null_aware,
        )?);

        let branch = LogicalPlan::Projection(Projection::new_from_schema(
            Arc::new(branch),
            Arc::clone(&original_schema),
        ));

        branches.push(Arc::new(branch));

        guards.push(disjunct.is_not_true());
    }

    let rewritten = LogicalPlan::Union(Union {
        inputs: branches,
        schema: original_schema,
    });

    Ok(Transformed::yes(rewritten))
}

fn extract_hashjoin_keys(
    expr: &Expr,
    left_schema: &datafusion_common::DFSchema,
    right_schema: &datafusion_common::DFSchema,
) -> Result<Option<Vec<(Expr, Expr)>>> {
    let conjuncts = split_conjunction_owned(expr.clone());
    let mut keys = Vec::with_capacity(conjuncts.len());

    for conjunct in conjuncts {
        let Expr::BinaryExpr(binary) = conjunct else {
            return Ok(None);
        };

        if binary.op != Operator::Eq {
            return Ok(None);
        }

        let Some((left, right)) = find_valid_equijoin_key_pair(
            &binary.left,
            &binary.right,
            left_schema,
            right_schema,
        )? else {
            return Ok(None);
        };

        let left_type = left.get_type(left_schema)?;
        let right_type = right.get_type(right_schema)?;
        if !can_hash(&left_type) || !can_hash(&right_type) {
            return Ok(None);
        }

        keys.push((left, right));
    }

    Ok(Some(keys))
}
