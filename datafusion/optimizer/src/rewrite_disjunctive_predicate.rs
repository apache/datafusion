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

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_expr::logical_plan::{
    Aggregate, Analyze, CreateMemoryTable, CreateView, CrossJoin, Distinct, Explain,
    Filter, Join, Limit, Projection, Repartition, Sort, Subquery, SubqueryAlias, Union,
    Window,
};
use datafusion_expr::Expr::BinaryExpr;
use datafusion_expr::{Expr, LogicalPlan, Operator};
use std::sync::Arc;

#[derive(Clone, PartialEq, Debug)]
enum Predicate {
    And { args: Vec<Predicate> },
    Or { args: Vec<Predicate> },
    Other { expr: Box<Expr> },
}

fn predicate(expr: &Expr) -> Result<Predicate> {
    match expr {
        BinaryExpr { left, op, right } => match op {
            Operator::And => {
                let args = vec![predicate(left)?, predicate(right)?];
                Ok(Predicate::And { args })
            }
            Operator::Or => {
                let args = vec![predicate(left)?, predicate(right)?];
                Ok(Predicate::Or { args })
            }
            _ => Ok(Predicate::Other {
                expr: Box::new(BinaryExpr {
                    left: left.clone(),
                    op: *op,
                    right: right.clone(),
                }),
            }),
        },
        _ => Ok(Predicate::Other {
            expr: Box::new(expr.clone()),
        }),
    }
}

fn normalize_predicate(predicate: &Predicate) -> Expr {
    match predicate {
        Predicate::And { args } => {
            assert!(args.len() >= 2);
            let left = normalize_predicate(&args[0]);
            let right = normalize_predicate(&args[1]);
            let mut and_expr = BinaryExpr {
                left: Box::new(left),
                op: Operator::And,
                right: Box::new(right),
            };
            for arg in args.iter().skip(2) {
                and_expr = BinaryExpr {
                    left: Box::new(and_expr),
                    op: Operator::And,
                    right: Box::new(normalize_predicate(arg)),
                };
            }
            and_expr
        }
        Predicate::Or { args } => {
            assert!(args.len() >= 2);
            let left = normalize_predicate(&args[0]);
            let right = normalize_predicate(&args[1]);
            let mut or_expr = BinaryExpr {
                left: Box::new(left),
                op: Operator::Or,
                right: Box::new(right),
            };
            for arg in args.iter().skip(2) {
                or_expr = BinaryExpr {
                    left: Box::new(or_expr),
                    op: Operator::Or,
                    right: Box::new(normalize_predicate(arg)),
                };
            }
            or_expr
        }
        Predicate::Other { expr } => *expr.clone(),
    }
}

fn rewrite_predicate(predicate: &Predicate) -> Predicate {
    match predicate {
        Predicate::And { args } => {
            let mut rewritten_args = Vec::with_capacity(args.len());
            for arg in args.iter() {
                rewritten_args.push(rewrite_predicate(arg));
            }
            rewritten_args = flatten_and_predicates(&rewritten_args);
            Predicate::And {
                args: rewritten_args,
            }
        }
        Predicate::Or { args } => {
            let mut rewritten_args = vec![];
            for arg in args.iter() {
                rewritten_args.push(rewrite_predicate(arg));
            }
            rewritten_args = flatten_or_predicates(&rewritten_args);
            delete_duplicate_predicates(&rewritten_args)
        }
        Predicate::Other { expr } => Predicate::Other {
            expr: Box::new(*expr.clone()),
        },
    }
}

fn flatten_and_predicates(and_predicates: &[Predicate]) -> Vec<Predicate> {
    let mut flattened_predicates = vec![];
    for predicate in and_predicates {
        match predicate {
            Predicate::And { args } => {
                flattened_predicates
                    .extend_from_slice(flatten_and_predicates(args).as_slice());
            }
            _ => {
                flattened_predicates.push(predicate.clone());
            }
        }
    }
    flattened_predicates
}

fn flatten_or_predicates(or_predicates: &[Predicate]) -> Vec<Predicate> {
    let mut flattened_predicates = vec![];
    for predicate in or_predicates {
        match predicate {
            Predicate::Or { args } => {
                flattened_predicates
                    .extend_from_slice(flatten_or_predicates(args).as_slice());
            }
            _ => {
                flattened_predicates.push(predicate.clone());
            }
        }
    }
    flattened_predicates
}

fn delete_duplicate_predicates(or_predicates: &[Predicate]) -> Predicate {
    let mut shortest_exprs: Vec<Predicate> = vec![];
    let mut shortest_exprs_len = 0;
    // choose the shortest AND predicate
    for or_predicate in or_predicates.iter() {
        match or_predicate {
            Predicate::And { args } => {
                let args_num = args.len();
                if shortest_exprs.is_empty() || args_num < shortest_exprs_len {
                    shortest_exprs = (*args).clone();
                    shortest_exprs_len = args_num;
                }
            }
            _ => {
                // if there is no AND predicate, it must be the shortest expression.
                shortest_exprs = vec![or_predicate.clone()];
                break;
            }
        }
    }

    // dedup shortest_exprs
    shortest_exprs.dedup();

    // Check each element in shortest_exprs to see if it's in all the OR arguments.
    let mut exist_exprs: Vec<Predicate> = vec![];
    for expr in shortest_exprs.iter() {
        let mut found = true;
        for or_predicate in or_predicates.iter() {
            match or_predicate {
                Predicate::And { args } => {
                    if !args.contains(expr) {
                        found = false;
                        break;
                    }
                }
                _ => {
                    if or_predicate != expr {
                        found = false;
                        break;
                    }
                }
            }
        }
        if found {
            exist_exprs.push((*expr).clone());
        }
    }
    if exist_exprs.is_empty() {
        return Predicate::Or {
            args: or_predicates.to_vec(),
        };
    }

    // Rebuild the OR predicate.
    // (A AND B) OR A will be optimized to A.
    let mut new_or_predicates = vec![];
    for or_predicate in or_predicates.iter() {
        match or_predicate {
            Predicate::And { args } => {
                let mut new_args = (*args).clone();
                new_args.retain(|expr| !exist_exprs.contains(expr));
                if !new_args.is_empty() {
                    if new_args.len() == 1 {
                        new_or_predicates.push(new_args[0].clone());
                    } else {
                        new_or_predicates.push(Predicate::And { args: new_args });
                    }
                } else {
                    new_or_predicates.clear();
                    break;
                }
            }
            _ => {
                if exist_exprs.contains(or_predicate) {
                    new_or_predicates.clear();
                    break;
                }
            }
        }
    }
    if !new_or_predicates.is_empty() {
        if new_or_predicates.len() == 1 {
            exist_exprs.push(new_or_predicates[0].clone());
        } else {
            exist_exprs.push(Predicate::Or {
                args: flatten_or_predicates(&new_or_predicates),
            });
        }
    }

    if exist_exprs.len() == 1 {
        exist_exprs[0].clone()
    } else {
        Predicate::And {
            args: flatten_and_predicates(&exist_exprs),
        }
    }
}

#[derive(Default)]
pub struct RewriteDisjunctivePredicate;

impl RewriteDisjunctivePredicate {
    pub fn new() -> Self {
        Self::default()
    }
    fn rewrite_disjunctive_predicate(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(filter) => {
                let predicate = predicate(&filter.predicate)?;
                let rewritten_predicate = rewrite_predicate(&predicate);
                let rewritten_expr = normalize_predicate(&rewritten_predicate);
                Ok(LogicalPlan::Filter(Filter {
                    predicate: rewritten_expr,
                    input: Arc::new(self.rewrite_disjunctive_predicate(
                        &filter.input,
                        _optimizer_config,
                    )?),
                }))
            }
            LogicalPlan::Projection(project) => {
                Ok(LogicalPlan::Projection(Projection {
                    expr: project.expr.clone(),
                    input: Arc::new(self.rewrite_disjunctive_predicate(
                        &project.input,
                        _optimizer_config,
                    )?),
                    schema: project.schema.clone(),
                    alias: project.alias.clone(),
                }))
            }
            LogicalPlan::Window(window) => Ok(LogicalPlan::Window(Window {
                input: Arc::new(
                    self.rewrite_disjunctive_predicate(&window.input, _optimizer_config)?,
                ),
                window_expr: window.window_expr.clone(),
                schema: window.schema.clone(),
            })),
            LogicalPlan::Aggregate(aggregate) => Ok(LogicalPlan::Aggregate(Aggregate {
                input: Arc::new(self.rewrite_disjunctive_predicate(
                    &aggregate.input,
                    _optimizer_config,
                )?),
                group_expr: aggregate.group_expr.clone(),
                aggr_expr: aggregate.aggr_expr.clone(),
                schema: aggregate.schema.clone(),
            })),
            LogicalPlan::Sort(sort) => Ok(LogicalPlan::Sort(Sort {
                expr: sort.expr.clone(),
                input: Arc::new(
                    self.rewrite_disjunctive_predicate(&sort.input, _optimizer_config)?,
                ),
            })),
            LogicalPlan::Join(join) => Ok(LogicalPlan::Join(Join {
                left: Arc::new(
                    self.rewrite_disjunctive_predicate(&join.left, _optimizer_config)?,
                ),
                right: Arc::new(
                    self.rewrite_disjunctive_predicate(&join.right, _optimizer_config)?,
                ),
                on: join.on.clone(),
                filter: join.filter.clone(),
                join_type: join.join_type,
                join_constraint: join.join_constraint,
                schema: join.schema.clone(),
                null_equals_null: join.null_equals_null,
            })),
            LogicalPlan::CrossJoin(cross_join) => Ok(LogicalPlan::CrossJoin(CrossJoin {
                left: Arc::new(self.rewrite_disjunctive_predicate(
                    &cross_join.left,
                    _optimizer_config,
                )?),
                right: Arc::new(self.rewrite_disjunctive_predicate(
                    &cross_join.right,
                    _optimizer_config,
                )?),
                schema: cross_join.schema.clone(),
            })),
            LogicalPlan::Repartition(repartition) => {
                Ok(LogicalPlan::Repartition(Repartition {
                    input: Arc::new(self.rewrite_disjunctive_predicate(
                        &repartition.input,
                        _optimizer_config,
                    )?),
                    partitioning_scheme: repartition.partitioning_scheme.clone(),
                }))
            }
            LogicalPlan::Union(union) => {
                let inputs = union
                    .inputs
                    .iter()
                    .map(|input| {
                        self.rewrite_disjunctive_predicate(input, _optimizer_config)
                    })
                    .collect::<Result<Vec<LogicalPlan>>>()?;
                Ok(LogicalPlan::Union(Union {
                    inputs,
                    schema: union.schema.clone(),
                    alias: union.alias.clone(),
                }))
            }
            LogicalPlan::TableScan(table_scan) => {
                Ok(LogicalPlan::TableScan(table_scan.clone()))
            }
            LogicalPlan::EmptyRelation(empty_relation) => {
                Ok(LogicalPlan::EmptyRelation(empty_relation.clone()))
            }
            LogicalPlan::Subquery(subquery) => Ok(LogicalPlan::Subquery(Subquery {
                subquery: Arc::new(self.rewrite_disjunctive_predicate(
                    &subquery.subquery,
                    _optimizer_config,
                )?),
            })),
            LogicalPlan::SubqueryAlias(subquery_alias) => {
                Ok(LogicalPlan::SubqueryAlias(SubqueryAlias {
                    input: Arc::new(self.rewrite_disjunctive_predicate(
                        &subquery_alias.input,
                        _optimizer_config,
                    )?),
                    alias: subquery_alias.alias.clone(),
                    schema: subquery_alias.schema.clone(),
                }))
            }
            LogicalPlan::Limit(limit) => Ok(LogicalPlan::Limit(Limit {
                skip: limit.skip,
                fetch: limit.fetch,
                input: Arc::new(
                    self.rewrite_disjunctive_predicate(&limit.input, _optimizer_config)?,
                ),
            })),
            LogicalPlan::CreateExternalTable(plan) => {
                Ok(LogicalPlan::CreateExternalTable(plan.clone()))
            }
            LogicalPlan::CreateMemoryTable(plan) => {
                Ok(LogicalPlan::CreateMemoryTable(CreateMemoryTable {
                    name: plan.name.clone(),
                    input: Arc::new(
                        self.rewrite_disjunctive_predicate(
                            &plan.input,
                            _optimizer_config,
                        )?,
                    ),
                    if_not_exists: plan.if_not_exists,
                    or_replace: plan.or_replace,
                }))
            }
            LogicalPlan::CreateView(plan) => Ok(LogicalPlan::CreateView(CreateView {
                name: plan.name.clone(),
                input: Arc::new(
                    self.rewrite_disjunctive_predicate(&plan.input, _optimizer_config)?,
                ),
                or_replace: plan.or_replace,
                definition: plan.definition.clone(),
            })),
            LogicalPlan::CreateCatalogSchema(plan) => {
                Ok(LogicalPlan::CreateCatalogSchema(plan.clone()))
            }
            LogicalPlan::CreateCatalog(plan) => {
                Ok(LogicalPlan::CreateCatalog(plan.clone()))
            }
            LogicalPlan::DropTable(plan) => Ok(LogicalPlan::DropTable(plan.clone())),
            LogicalPlan::Values(plan) => Ok(LogicalPlan::Values(plan.clone())),
            LogicalPlan::Explain(explain) => Ok(LogicalPlan::Explain(Explain {
                verbose: explain.verbose,
                plan: Arc::new(
                    self.rewrite_disjunctive_predicate(&explain.plan, _optimizer_config)?,
                ),
                stringified_plans: explain.stringified_plans.clone(),
                schema: explain.schema.clone(),
            })),
            LogicalPlan::Analyze(analyze) => {
                Ok(LogicalPlan::Analyze(Analyze {
                    verbose: analyze.verbose,
                    input: Arc::new(self.rewrite_disjunctive_predicate(
                        &analyze.input,
                        _optimizer_config,
                    )?),
                    schema: analyze.schema.clone(),
                }))
            }
            LogicalPlan::Extension(plan) => Ok(LogicalPlan::Extension(plan.clone())),
            LogicalPlan::Distinct(plan) => Ok(LogicalPlan::Distinct(Distinct {
                input: Arc::new(
                    self.rewrite_disjunctive_predicate(&plan.input, _optimizer_config)?,
                ),
            })),
        }
    }
}

impl OptimizerRule for RewriteDisjunctivePredicate {
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &OptimizerConfig,
    ) -> Result<LogicalPlan> {
        self.rewrite_disjunctive_predicate(plan, optimizer_config)
    }

    fn name(&self) -> &str {
        "rewrite_disjunctive_predicate"
    }
}
