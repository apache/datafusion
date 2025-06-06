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

//! [`DependentJoinRewriter`] converts correlated subqueries to `DependentJoin`

use std::ops::Deref;
use std::sync::Arc;
use std::collections::HashMap as StdHashMap;
use crate::rewrite_dependent_join::DependentJoinRewriter;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

use arrow::datatypes::{DataType, Field};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, 
};
use datafusion_common::{internal_err, Column, DFSchema,  Result};
use datafusion_expr::expr::{self, Exists, InSubquery};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    binary_expr,  col,  lit, not, when, Aggregate, BinaryExpr,
    DependentJoin,  Expr,   JoinType, LogicalPlan,
    LogicalPlanBuilder, Operator, Projection,
};

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

#[allow(dead_code)]
#[derive(Clone)]
struct UnnestingInfo {
    // join: DependentJoin,
    domain: LogicalPlan,
    parent: Option<Unnesting>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct Unnesting {
    original_subquery: LogicalPlan,
    info: Arc<UnnestingInfo>,
}

#[derive(Clone, Debug, Eq, PartialOrd, PartialEq, Hash)]
struct CorrelatedColumnInfo {
    col: Column,
    data_type: DataType,
}
#[derive(Clone, Debug)]
pub struct DependentJoinDecorrelator {
    // immutable, defined when this object is constructed
    domains: IndexSet<CorrelatedColumnInfo>,
    pub delim_types: Vec<DataType>,
    is_initial: bool,

    // top-most subquery DecorrelateDependentJoin has depth 1 and so on
    depth: usize,
    // hashmap of correlated column by depth
    correlated_map: IndexMap<usize, Vec<CorrelatedColumnInfo>>,
    // check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
    // store a mapping between a expr and its original index in the loglan output
    replacement_map: IndexMap<String, Expr>,
    // if during the top down traversal, we observe any operator that requires
    // joining all rows from the lhs with nullable rows on the rhs
    any_join: bool,
    delim_scan_id: usize,
}

// normal join, but remove redundant columns
// i.e if we join two table with equi joins left=right
// only take the matching table on the right;
fn natural_join(
    mut builder: LogicalPlanBuilder,
    right: LogicalPlan,
    join_type: JoinType,
    delim_join_conditions: Vec<(Column, Column)>,
) -> Result<LogicalPlanBuilder> {
    let mut exclude_cols = IndexSet::new();
    let join_exprs: Vec<_> = delim_join_conditions
        .iter()
        .map(|(lhs, rhs)| {
            exclude_cols.insert(rhs);
            binary_expr(
                Expr::Column(lhs.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(rhs.clone()),
            )
        })
        .collect();
    let require_dedup = !join_exprs.is_empty();

    builder = builder.join(
        right,
        join_type,
        (Vec::<Column>::new(), Vec::<Column>::new()),
        conjunction(join_exprs).or(Some(lit(true))),
    )?;
    if require_dedup {
        let remain_cols = builder.schema().columns().into_iter().filter_map(|c| {
            if exclude_cols.contains(&c) {
                None
            } else {
                Some(Expr::Column(c))
            }
        });
        builder.project(remain_cols)
    } else {
        Ok(builder)
    }
}

impl DependentJoinDecorrelator {
    fn init(&mut self, dependent_join_node: &DependentJoin) {
        let correlated_columns_of_current_level = dependent_join_node
            .correlated_columns
            .iter()
            .map(|(_, col, data_type)| CorrelatedColumnInfo {
                col: col.clone(),
                data_type: data_type.clone(),
            });

        self.domains = correlated_columns_of_current_level.unique().collect();
        self.delim_types = self
            .domains
            .iter()
            .map(|CorrelatedColumnInfo { data_type, .. }| data_type.clone())
            .collect();

        dependent_join_node.correlated_columns.iter().for_each(
            |(depth, col, data_type)| {
                let cols = self.correlated_map.entry(*depth).or_default();
                let to_insert = CorrelatedColumnInfo {
                    col: col.clone(),
                    data_type: data_type.clone(),
                };
                if !cols.contains(&to_insert) {
                    cols.push(CorrelatedColumnInfo {
                        col: col.clone(),
                        data_type: data_type.clone(),
                    });
                }
            },
        );
    }
    fn new_root() -> Self {
        Self {
            domains: IndexSet::new(),
            delim_types: vec![],
            is_initial: true,
            correlated_map: IndexMap::new(),
            replacement_map: IndexMap::new(),
            any_join: true,
            delim_scan_id: 0,
            depth: 0,
        }
    }
    fn new(
        correlated_columns: &Vec<(usize, Column, DataType)>,
        parent_correlated_columns: &IndexMap<usize, Vec<CorrelatedColumnInfo>>,
        is_initial: bool,
        any_join: bool,
        delim_scan_id: usize,
        depth: usize,
    ) -> Self {
        let correlated_columns_of_current_level =
            correlated_columns
                .iter()
                .map(|(_, col, data_type)| CorrelatedColumnInfo {
                    col: col.clone(),
                    data_type: data_type.clone(),
                });

        let domains: IndexSet<_> = correlated_columns_of_current_level
            .chain(
                parent_correlated_columns
                    .iter()
                    .map(|(_, correlated_columns)| correlated_columns.clone())
                    .flatten(),
            )
            .unique()
            .collect();

        let delim_types = domains
            .iter()
            .map(|CorrelatedColumnInfo { data_type, .. }| data_type.clone())
            .collect();
        let mut merged_correlated_map = parent_correlated_columns.clone();
        merged_correlated_map.retain(|columns_depth, _| *columns_depth >= depth);

        correlated_columns
            .iter()
            .for_each(|(depth, col, data_type)| {
                let cols = merged_correlated_map.entry(*depth).or_default();
                let to_insert = CorrelatedColumnInfo {
                    col: col.clone(),
                    data_type: data_type.clone(),
                };
                if !cols.contains(&to_insert) {
                    cols.push(CorrelatedColumnInfo {
                        col: col.clone(),
                        data_type: data_type.clone(),
                    });
                }
            });

        Self {
            domains,
            delim_types,
            is_initial,
            correlated_map: merged_correlated_map,
            replacement_map: IndexMap::new(),
            any_join,
            delim_scan_id,
            depth,
        }
    }

    #[allow(dead_code)]
    fn subquery_dependent_filter(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if *op == Operator::And {
                    if Self::subquery_dependent_filter(left)
                        || Self::subquery_dependent_filter(right)
                    {
                        return true;
                    }
                }
            }
            Expr::InSubquery(_) | Expr::ScalarSubquery(_) | Expr::Exists(_) => {
                return true;
            }
            _ => {}
        };
        false
    }

    fn decorrelate(
        &mut self,
        node: &DependentJoin,
        parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        let correlated_columns = node.correlated_columns.clone();
        let perform_delim = true;
        let left = node.left.as_ref();
        let new_left = if !self.is_initial {
            // TODO: revisit this check
            // because after DecorrelateDependentJoin at parent level
            // this correlated_columns list are not mutated yet
            let new_left = if node.correlated_columns.is_empty() {
                self.pushdown_independent(left)?
            } else {
                self.push_down_dependent_join(
                    left,
                    parent_propagate_nulls,
                    lateral_depth,
                )?
            };

            // if the pushdown happens, it means
            // the DELIM join has happend somewhere
            // and the new correlated columns now has new name
            // using the delim_join side's name
            // Self::rewrite_correlated_columns(
            //     &mut correlated_columns,
            //     self.delim_scan_relation_name(),
            // );
            new_left
        } else {
            self.init(node);
            self.decorrelate_plan(left.clone())?
        };
        let lateral_depth = 0;
        // let propagate_null_values = node.propagate_null_value();
        let _propagate_null_values = true;

        let mut decorrelator = DependentJoinDecorrelator::new(
            &correlated_columns,
            &self.correlated_map,
            false,
            false,
            self.delim_scan_id,
            self.depth + 1,
        );
        let right = decorrelator.push_down_dependent_join(
            &node.right,
            parent_propagate_nulls,
            lateral_depth,
        )?;
        let (join_condition, join_type, post_join_expr) = self.delim_join_conditions(
            node,
            right.schema().columns(),
            decorrelator.delim_scan_relation_name(),
            perform_delim,
        )?;

        let mut builder = LogicalPlanBuilder::new(new_left).join(
            right,
            join_type,
            (Vec::<Column>::new(), Vec::<Column>::new()),
            Some(join_condition),
        )?;
        if let Some(subquery_proj_expr) = post_join_expr {
            let new_exprs: Vec<Expr> = builder
                .schema()
                .columns()
                .into_iter()
                // remove any "mark" columns output by the markjoin
                .filter_map(|c| {
                    if c.name == "mark" {
                        None
                    } else {
                        Some(Expr::Column(c))
                    }
                })
                .chain(std::iter::once(subquery_proj_expr))
                .collect();
            builder = builder.project(new_exprs)?;
        }

        let _debug = builder.clone().build()?;
        let new_plan = Self::rewrite_outer_ref_columns(
            builder.build()?,
            &self.domains,
            decorrelator.delim_scan_relation_name(),
            true,
        )?;

        self.delim_scan_id = decorrelator.delim_scan_id;
        return Ok(new_plan);
    }

    // TODO: support lateral join
    // convert dependent join into delim join
    fn delim_join_conditions(
        &self,
        node: &DependentJoin,
        right_columns: Vec<Column>,
        delim_join_relation_name_on_right: String,
        perform_delim: bool,
    ) -> Result<(Expr, JoinType, Option<Expr>)> {
        if node.lateral_join_condition.is_some() {
            unimplemented!()
        }

        let _col_count = if perform_delim {
            node.correlated_columns.len()
        } else {
            unimplemented!()
        };
        let mut join_conditions = vec![];
        // if this is set, a new expr will be added to the parent projection
        // after delimJoin
        // this is because some expr cannot be evaluated during the join, for example
        // binary_expr(subquery_1,subquery_2)
        // this will result into 2 consecutive delim_join
        // project(binary_expr(result_subquery_1, result_subquery_2))
        //  delim_join on subquery1
        //   delim_join on subquery2
        let mut extra_expr_after_join = None;
        let mut join_type = JoinType::Inner;
        if let Some(ref expr) = node.subquery_expr {
            match expr {
                Expr::ScalarSubquery(_) => {
                    // TODO: support JoinType::Single
                    // That works similar to left outer join
                    // But having extra check that only for each entry on the LHS
                    // only at most 1 parter on the RHS matches
                    join_type = JoinType::Left;

                    // The reason we does not make this as a condition inside the delim join
                    // is because the evaluation of scalar_subquery expr may be needed
                    // somewhere above
                    extra_expr_after_join = Some(
                        Expr::Column(right_columns.first().unwrap().clone())
                            .alias(format!("{}.output", node.subquery_name)),
                    );
                }
                Expr::Exists(Exists { negated, .. }) => {
                    join_type = JoinType::LeftMark;
                    if *negated {
                        extra_expr_after_join = Some(
                            not(col("mark"))
                                .alias(format!("{}.output", node.subquery_name)),
                        );
                    } else {
                        extra_expr_after_join = Some(
                            col("mark").alias(format!("{}.output", node.subquery_name)),
                        );
                    }
                }
                Expr::InSubquery(InSubquery { expr, negated, .. }) => {
                    // TODO: looks like there is a comment that
                    // markjoin does not support fully null semantic for ANY/IN subquery
                    join_type = JoinType::LeftMark;
                    extra_expr_after_join =
                        Some(col("mark").alias(format!("{}.output", node.subquery_name)));
                    let op = if *negated {
                        Operator::NotEq
                    } else {
                        Operator::Eq
                    };
                    join_conditions.push(binary_expr(
                        expr.deref().clone(),
                        op,
                        Expr::Column(right_columns.first().unwrap().clone()),
                    ));
                }
                _ => {
                    unreachable!()
                }
            }
        }

        for col in node
            .correlated_columns
            .iter()
            .map(|(_, col, _)| col)
            .unique()
        {
            let raw_name = col.flat_name().replace('.', "_");
            join_conditions.push(binary_expr(
                Expr::Column(col.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(Column::from(format!(
                    "{delim_join_relation_name_on_right}.{raw_name}"
                ))),
            ));
        }
        Ok((
            conjunction(join_conditions).or(Some(lit(true))).unwrap(),
            join_type,
            extra_expr_after_join,
        ))
    }
    fn pushdown_independent(&mut self, _node: &LogicalPlan) -> Result<LogicalPlan> {
        unimplemented!()
    }

    #[allow(dead_code)]
    fn rewrite_correlated_columns(
        correlated_columns: &mut Vec<(usize, Column, DataType)>,
        delim_scan_name: String,
    ) {
        for (_, col, _) in correlated_columns.iter_mut() {
            *col = Column::from(format!("{}.{}", delim_scan_name, col.name));
        }
    }

    // equivalent to RewriteCorrelatedExpressions of DuckDB
    // but with our current context we may not need this
    fn rewrite_outer_ref_columns(
        plan: LogicalPlan,
        domains: &IndexSet<CorrelatedColumnInfo>,
        delim_scan_relation_name: String,
        recursive: bool,
    ) -> Result<LogicalPlan> {
        if !recursive {
            return plan
                .map_expressions(|e| {
                    e.transform(|e| {
                        if let Expr::OuterReferenceColumn(data_type, outer_col) = &e {
                            let cmp_col = CorrelatedColumnInfo {
                                col: outer_col.clone(),
                                data_type: data_type.clone(),
                            };
                            if domains.contains(&cmp_col) {
                                return Ok(Transformed::yes(col(
                                    Self::rewrite_into_delim_column(
                                        &delim_scan_relation_name,
                                        outer_col,
                                    ),
                                )));
                            }
                        }
                        Ok(Transformed::no(e))
                    })
                })?
                .data
                .recompute_schema();
        }
        plan.transform_up(|p| {
            if let LogicalPlan::DependentJoin(_) = &p {
                return internal_err!(
                    "calling rewrite_correlated_exprs while some of \
                    the plan is still dependent join plan"
                );
            }
            if !p.contains_outer_reference() {
                return Ok(Transformed::no(p));
            }
            p.map_expressions(|e| {
                e.transform(|e| {
                    if let Expr::OuterReferenceColumn(data_type, outer_col) = &e {
                        let cmp_col = CorrelatedColumnInfo {
                            col: outer_col.clone(),
                            data_type: data_type.clone(),
                        };
                        if domains.contains(&cmp_col) {
                            return Ok(Transformed::yes(col(
                                Self::rewrite_into_delim_column(
                                    &delim_scan_relation_name,
                                    outer_col,
                                ),
                            )));
                        }
                    }
                    Ok(Transformed::no(e))
                })
            })
        })?
        .data
        .recompute_schema()
    }
    fn delim_scan_relation_name(&self) -> String {
        format!("delim_scan_{}", self.delim_scan_id)
    }
    fn rewrite_into_delim_column(delim_relation: &String, original: &Column) -> Column {
        let field_name = original.flat_name().replace('.', "_");
        return Column::from(format!("{delim_relation}.{field_name}"));
    }
    fn build_delim_scan(&mut self) -> Result<(LogicalPlan, String)> {
        self.delim_scan_id += 1;
        let id = self.delim_scan_id;
        let delim_scan_relation_name = format!("delim_scan_{id}");
        let fields = self
            .domains
            .iter()
            .map(|c| {
                let field_name = c.col.flat_name().replace('.', "_");
                Field::new(field_name, c.data_type.clone(), true)
            })
            .collect();
        let schema = DFSchema::from_unqualified_fields(fields, StdHashMap::new())?;
        Ok((
            LogicalPlanBuilder::delim_get(
                self.delim_scan_id,
                &self.delim_types,
                self.domains
                    .iter()
                    .map(|c| c.col.clone())
                    .unique()
                    .collect(),
                schema.into(),
            )
            .alias(&delim_scan_relation_name)?
            .build()?,
            delim_scan_relation_name,
        ))
    }
    fn rewrite_expr_from_replacement_map(
        replacement: &IndexMap<String, Expr>,
        plan: LogicalPlan,
    ) -> Result<LogicalPlan> {
        // TODO: not sure if rewrite should stop once found replacement expr
        plan.transform_down(|p| {
            if let LogicalPlan::DependentJoin(_) = &p {
                return internal_err!(
                    "calling rewrite_correlated_exprs while some of \
                    the plan is still dependent join plan"
                );
            }
            if let LogicalPlan::Projection(_proj) = &p {
                p.map_expressions(|e| {
                    e.transform(|e| {
                        if let Some(to_replace) = replacement.get(&e.to_string()) {
                            Ok(Transformed::yes(to_replace.clone()))
                        } else {
                            Ok(Transformed::no(e))
                        }
                    })
                })
            } else {
                Ok(Transformed::no(p))
                // unimplemented!()
            }
        })?
        .data
        .recompute_schema()
    }

    // on recursive rewrite, make sure to update any correlated_column
    // TODO: make all of the delim join natural join
    fn push_down_dependent_join_internal(
        &mut self,
        node: &LogicalPlan,
        parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        let mut has_correlated_expr = false;
        let has_correlated_expr_ref = &mut has_correlated_expr;
        // TODO: is there any way to do this more efficiently
        // TODO: this lookup must be associated with a list of correlated_columns
        // (from current DecorrelateDependentJoin context and its parent)
        // and check if the correlated expr (if any) exists in the correlated_columns
        node.apply(|p| {
            match p {
                LogicalPlan::DependentJoin(join) => {
                    if !join.correlated_columns.is_empty() {
                        *has_correlated_expr_ref = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
                any => {
                    if any.contains_outer_reference() {
                        *has_correlated_expr_ref = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            };
            Ok(TreeNodeRecursion::Continue)
        })?;

        if !*has_correlated_expr_ref {
            match node {
                LogicalPlan::Projection(old_proj) => {
                    let mut proj = old_proj.clone();
                    // TODO: define logical plan for delim scan
                    let (delim_scan, delim_scan_relation_name) =
                        self.build_delim_scan()?;
                    let left = self.decorrelate_plan(proj.input.deref().clone())?;
                    let cross_join = LogicalPlanBuilder::new(left)
                        .join(
                            delim_scan,
                            JoinType::Inner,
                            (Vec::<Column>::new(), Vec::<Column>::new()),
                            None,
                        )?
                        .build()?;

                    for domain_col in self.domains.iter() {
                        proj.expr.push(col(Self::rewrite_into_delim_column(
                            &delim_scan_relation_name,
                            &domain_col.col,
                        )));
                    }

                    let proj = Projection::try_new(proj.expr, cross_join.into())?;

                    return Self::rewrite_outer_ref_columns(
                        LogicalPlan::Projection(proj),
                        &self.domains,
                        delim_scan_relation_name,
                        false,
                    );
                }
                LogicalPlan::RecursiveQuery(_) => {
                    // duckdb support this
                    unimplemented!("")
                }
                any => {
                    let (delim_scan, _) = self.build_delim_scan()?;
                    let left = self.decorrelate_plan(any.clone())?;

                    let _dedup_cols = delim_scan.schema().columns();
                    let cross_join = natural_join(
                        LogicalPlanBuilder::new(left),
                        delim_scan,
                        JoinType::Inner,
                        vec![],
                    )?
                    .build()?;
                    return Ok(cross_join);
                }
            }
        }
        match node {
            LogicalPlan::Projection(old_proj) => {
                let mut proj = old_proj.clone();
                // for (auto &expr : plan->expressions) {
                // 	parent_propagate_null_values &= expr->PropagatesNullValues();
                // }
                // bool child_is_dependent_join = plan->children[0]->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
                // parent_propagate_null_values &= !child_is_dependent_join;
                let new_input = self.push_down_dependent_join(
                    proj.input.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                for domain_col in self.domains.iter() {
                    proj.expr.push(col(Self::rewrite_into_delim_column(
                        &self.delim_scan_relation_name(),
                        &domain_col.col,
                    )));
                }
                let proj = Projection::try_new(proj.expr, new_input.into())?;
                return Self::rewrite_outer_ref_columns(
                    LogicalPlan::Projection(proj),
                    &self.domains,
                    self.delim_scan_relation_name(),
                    false,
                );
            }
            LogicalPlan::Filter(old_filter) => {
                // todo: define if any join is need
                let new_input = self.push_down_dependent_join(
                    old_filter.input.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                let mut filter = old_filter.clone();
                filter.input = Arc::new(new_input);
                let new_plan = Self::rewrite_outer_ref_columns(
                    LogicalPlan::Filter(filter),
                    &self.domains,
                    self.delim_scan_relation_name(),
                    false,
                )?;

                return Ok(new_plan);
            }
            LogicalPlan::Aggregate(old_agg) => {
                let (delim_scan_above_agg, _) = self.build_delim_scan()?;
                let new_input = self.push_down_dependent_join_internal(
                    old_agg.input.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                // to differentiate between the delim scan above the aggregate
                // i.e
                // Delim -> Above agg
                //   Agg
                //     Join
                //       Delim -> Delim below agg
                //       Filter
                //       ..
                let delim_scan_under_agg_rela = self.delim_scan_relation_name();

                let mut new_agg = old_agg.clone();
                new_agg.input = Arc::new(new_input);
                let new_plan = Self::rewrite_outer_ref_columns(
                    LogicalPlan::Aggregate(new_agg),
                    &self.domains,
                    delim_scan_under_agg_rela.clone(),
                    false,
                )?;

                let (agg_expr, mut group_expr, input) = match new_plan {
                    LogicalPlan::Aggregate(Aggregate {
                        aggr_expr,
                        group_expr,
                        input,
                        ..
                    }) => (aggr_expr, group_expr, input),
                    _ => {
                        unreachable!()
                    }
                };
                // TODO: only false in case one of the correlated columns are of type
                // List or a struct with a subfield of type List
                let _perform_delim = true;
                // let new_group_count = if perform_delim { self.domains.len() } else { 1 };
                // TODO: support grouping set
                // select count(*)
                let mut extra_group_columns = vec![];
                for c in self.domains.iter() {
                    let delim_col = Self::rewrite_into_delim_column(
                        &delim_scan_under_agg_rela,
                        &c.col,
                    );
                    group_expr.push(col(delim_col.clone()));
                    extra_group_columns.push(delim_col);
                }
                // perform a join of this agg (group by correlated columns added)
                // with the same delimScan of the set same of correlated columns
                // for now ungorup_join is always true
                // let ungroup_join = agg.group_expr.len() == new_group_count;
                let ungroup_join = true;
                if ungroup_join {
                    let mut join_type = JoinType::Inner;
                    if self.any_join || !parent_propagate_nulls {
                        join_type = JoinType::Left;
                    }

                    let mut delim_conditions = vec![];
                    for (lhs, rhs) in extra_group_columns
                        .iter()
                        .zip(delim_scan_above_agg.schema().columns().iter())
                    {
                        delim_conditions.push((lhs.clone(), rhs.clone()));
                    }

                    for agg_expr in agg_expr.iter() {
                        match agg_expr {
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func,
                                ..
                            }) => {
                                // Transformed::yes(Expr::Literal(ScalarValue::Int64(Some(0))))
                                if func.name() == "count" {
                                    let expr_name = agg_expr.to_string();
                                    let expr_to_replace =
                                        when(agg_expr.clone().is_null(), lit(0))
                                            .otherwise(agg_expr.clone())?;
                                    self.replacement_map
                                        .insert(expr_name, expr_to_replace);
                                    continue;
                                }
                            }
                            _ => {}
                        }
                    }

                    let new_agg = Aggregate::try_new(input, group_expr, agg_expr)?;
                    let agg_output_cols = new_agg
                        .schema
                        .columns()
                        .into_iter()
                        .map(|c| Expr::Column(c));
                    let builder =
                        LogicalPlanBuilder::new(LogicalPlan::Aggregate(new_agg))
                            // TODO: a hack to ensure aggregated expr are ordered first in the output
                            .project(agg_output_cols.rev())?;
                    natural_join(
                        builder,
                        delim_scan_above_agg,
                        join_type,
                        delim_conditions,
                    )?
                    .build()
                } else {
                    unimplemented!()
                }
            }
            LogicalPlan::DependentJoin(djoin) => {
                return self.decorrelate(djoin, parent_propagate_nulls, lateral_depth);
            }
            plan_ => {
                unimplemented!("implement pushdown dependent join for node {plan_}")
            }
        }
    }
    fn push_down_dependent_join(
        &mut self,
        node: &LogicalPlan,
        parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        let mut new_plan = self.push_down_dependent_join_internal(
            node,
            parent_propagate_nulls,
            lateral_depth,
        )?;
        if !self.replacement_map.is_empty() {
            new_plan =
                Self::rewrite_expr_from_replacement_map(&self.replacement_map, new_plan)?;
        }

        // let projected_expr = new_plan.schema().columns().into_iter().map(|c| {
        //     if let Some(alt_expr) = self.replacement_map.swap_remove(&c.name) {
        //         return alt_expr;
        //     }
        //     Expr::Column(c.clone())
        // });
        // new_plan = LogicalPlanBuilder::new(new_plan)
        //     .project(projected_expr)?
        //     .build()?;
        Ok(new_plan)
    }
    fn decorrelate_plan(&mut self, node: LogicalPlan) -> Result<LogicalPlan> {
        match node {
            LogicalPlan::DependentJoin(mut djoin) => {
                self.decorrelate(&mut djoin, true, 0)
            }
            _ => Ok(node
                .map_children(|n| Ok(Transformed::yes(self.decorrelate_plan(n)?)))?
                .data),
        }
    }
}

/// Optimizer rule for rewriting any arbitrary subqueries
#[allow(dead_code)]
#[derive(Debug)]
pub struct DecorrelateDependentJoin {}

impl DecorrelateDependentJoin {
    pub fn new() -> Self {
        return DecorrelateDependentJoin {};
    }
}

impl OptimizerRule for DecorrelateDependentJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    // There will be 2 rewrites going on
    // - Convert all subqueries (maybe including lateral join in the future) to temporary
    // LogicalPlan node called DependentJoin
    // - Decorrelate DependentJoin following top-down approach recursively
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let mut transformer =
            DependentJoinRewriter::new(Arc::clone(config.alias_generator()));
        let rewrite_result = transformer.rewrite_subqueries_into_dependent_joins(plan)?;
        if rewrite_result.transformed {
            println!("dependent join plan {}", rewrite_result.data);
            let mut decorrelator = DependentJoinDecorrelator::new_root();
            return Ok(Transformed::yes(
                decorrelator.decorrelate_plan(rewrite_result.data)?,
            ));
        }
        Ok(rewrite_result)
    }

    fn name(&self) -> &str {
        "decorrelate_subquery"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

#[cfg(test)]
mod tests {

    use crate::decorrelate_dependent_join::DecorrelateDependentJoin;
    use crate::test::test_table_scan_with_name;
    use crate::Optimizer;
    use crate::{
        assert_optimized_plan_eq_display_indent_snapshot, OptimizerConfig,
        OptimizerContext, OptimizerRule,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common:: Result;
    use datafusion_expr::{
         exists,  expr_fn::col, in_subquery, lit,
        out_ref_col, scalar_subquery, Expr,  LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use std::sync::Arc;
    fn print_graphviz(plan: &LogicalPlan) {
        let rule: Arc<dyn OptimizerRule + Send + Sync> = Arc::new(DecorrelateDependentJoin::new());
        let optimizer = Optimizer::with_rules(vec![rule]);
        let optimized_plan = optimizer
            .optimize(plan.clone(), &OptimizerContext::new(), |_, _| {})
            .expect("failed to optimize plan");
        let _formatted_plan = optimized_plan.display_indent_schema();
        println!("{}", optimized_plan.display_graphviz());
    }

    macro_rules! assert_decorrelate {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(DecorrelateDependentJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )?;
        }};
    }

    #[test]
    fn decorrelated_two_nested_subqueries() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 =
            Arc::new(
                LogicalPlanBuilder::from(inner_table_lv2)
                    .filter(
                        col("inner_table_lv2.a")
                            .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                            .and(col("inner_table_lv2.b").eq(out_ref_col(
                                ArrowDataType::UInt32,
                                "inner_table_lv1.b",
                            ))),
                    )?
                    .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv2.a"))])?
                    .build()?,
            );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.c"))
                        .and(scalar_subquery(scalar_sq_level2).eq(lit(1))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(scalar_subquery(scalar_sq_level1).eq(col("outer_table.a"))),
            )?
            .build()?;
        print_graphviz(&plan);

        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a
        //     DependentJoin on [outer_table.a lvl 2, outer_table.c lvl 1] with expr (<subquery>) depth 1
        //       TableScan: outer_table
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c
        //           Filter: inner_table_lv1.c = outer_ref(outer_table.c) AND __scalar_sq_1.output = Int32(1)
        //             DependentJoin on [inner_table_lv1.b lvl 2] with expr (<subquery>) depth 2
        //               TableScan: inner_table_lv1
        //               Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv2.a)]]
        //                 Filter: inner_table_lv2.a = outer_ref(outer_table.a) AND inner_table_lv2.b = outer_ref(inner_table_lv1.b)
        //                   TableScan: inner_table_lv2
        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
              Left Join:  Filter: outer_table.a IS NOT DISTINCT FROM delim_scan_4.outer_table_a AND outer_table.c IS NOT DISTINCT FROM delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join:  Filter: delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_1.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                        Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                          Filter: inner_table_lv1.c = delim_scan_4.outer_table_c AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_2.outer_table_a, delim_scan_2.outer_table_c, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                              Left Join:  Filter: inner_table_lv1.b IS NOT DISTINCT FROM delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                Inner Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                  TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                  SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    DelimGet: outer_table.a, outer_table.c [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                  Inner Join:  Filter: delim_scan_4.inner_table_lv1_b IS NOT DISTINCT FROM delim_scan_3.inner_table_lv1_b AND delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_3.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_3.outer_table_c [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                      Aggregate: groupBy=[[delim_scan_4.inner_table_lv1_b, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv2.a)]] [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv2.a):Int64]
                                        Filter: inner_table_lv2.a = delim_scan_4.outer_table_a AND inner_table_lv2.b = delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                          Inner Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                            TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                            SubqueryAlias: delim_scan_4 [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                              DelimGet: inner_table_lv1.b, outer_table.a, outer_table.c [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    SubqueryAlias: delim_scan_3 [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                      DelimGet: inner_table_lv1.b, outer_table.a, outer_table.c [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                    SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                      DelimGet: outer_table.a, outer_table.c [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
        ");
        Ok(())
    }

    #[test]
    fn decorrelate_join_in_subquery_with_count_depth_1() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("inner_table_lv1.a"))])?
                // TODO: if uncomment this the test fail
                // .project(vec![count(col("inner_table_lv1.a")).alias("count_a")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __in_sq_1.output
        //     DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1
        //       TableScan: outer_table
        //       Aggregate: groupBy=[[]], aggr=[[count(inner_table_lv1.a)]]
        //         Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //           TableScan: inner_table_lv1

        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, delim_scan_2.mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join:  Filter: outer_table.c = CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AND outer_table.a IS NOT DISTINCT FROM delim_scan_2.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_2.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join:  Filter: delim_scan_2.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_2.outer_table_b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_2.outer_table_a, delim_scan_2.outer_table_b]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64]
                        Filter: inner_table_lv1.a = delim_scan_2.outer_table_a AND delim_scan_2.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND delim_scan_2.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                          Inner Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                            SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                              DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");
        Ok(())
    }

    #[test]
    fn decorrelate_two_subqueries_at_the_same_level() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let in_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(col("inner_table_lv1.c").eq(lit(2)))?
                .project(vec![col("inner_table_lv1.a")])?
                .build()?,
        );
        let exist_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a").and(col("inner_table_lv1.b").eq(lit(1))),
                )?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(exists(exist_sq_level1))
                    .and(in_subquery(col("outer_table.b"), in_sq_level1)),
            )?
            .build()?;
        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __exists_sq_1.output AND __in_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __in_sq_2.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1.output, inner_table_lv1.mark AS __in_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __in_sq_2.output:Boolean]
              LeftMark Join:  Filter: outer_table.b = inner_table_lv1.a [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, mark:Boolean]
                Projection: outer_table.a, outer_table.b, outer_table.c, inner_table_lv1.mark AS __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean]
                  LeftMark Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                    TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                    Inner Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
                      Filter: inner_table_lv1.a AND inner_table_lv1.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
                        TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      SubqueryAlias: delim_scan_1 []
                        DelimGet: []
                Projection: inner_table_lv1.a [a:UInt32]
                  Cross Join:  [a:UInt32, b:UInt32, c:UInt32]
                    Filter: inner_table_lv1.c = Int32(2) [a:UInt32, b:UInt32, c:UInt32]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                    SubqueryAlias: delim_scan_2 []
                      DelimGet: []
        ");
        Ok(())
    }

    #[test]
    fn decorrelate_with_in_subquery_has_dependent_column() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .filter(
                    col("inner_table_lv1.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a"))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.a")
                                .gt(col("inner_table_lv1.c")),
                        )
                        .and(col("inner_table_lv1.b").eq(lit(1)))
                        .and(
                            out_ref_col(ArrowDataType::UInt32, "outer_table.b")
                                .eq(col("inner_table_lv1.b")),
                        ),
                )?
                .project(vec![col("inner_table_lv1.b")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), sq_level1)),
            )?
            .build()?;
        let dec = DecorrelateDependentJoin::new();
        let ctx: Box<dyn OptimizerConfig> = Box::new(OptimizerContext::new());
        let plan = dec.rewrite(plan, ctx.as_ref())?.data;

        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __in_sq_1.output
        //     DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1
        //       TableScan: outer_table
        //       Projection: inner_table_lv1.b
        //         Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b
        //           TableScan: inner_table_lv1
        assert_decorrelate!(plan,       @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join:  Filter: outer_table.c = inner_table_lv1.b AND outer_table.a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table_lv1.b, delim_scan_1.outer_table_a, delim_scan_1.outer_table_b [b:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Filter: inner_table_lv1.a = delim_scan_1.outer_table_a AND delim_scan_1.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND delim_scan_1.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Inner Join:  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");

        Ok(())
    }
}
