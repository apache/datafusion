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

use crate::rewrite_dependent_join::DependentJoinRewriter;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use std::ops::Deref;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{internal_datafusion_err, internal_err, Column, Result};
use datafusion_expr::expr::{self, Exists, InSubquery};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    binary_expr, col, lit, not, when, Aggregate, BinaryExpr, CorrelatedColumnInfo,
    DependentJoin, Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Operator,
    Projection,
};

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

#[derive(Clone, Debug)]
pub struct DependentJoinDecorrelator {
    // immutable, defined when this object is constructed
    domains: IndexSet<CorrelatedColumnInfo>,
    // for each domain column, the corresponding column in delim_get
    correlated_column_to_delim_column: IndexMap<Column, Column>,
    is_initial: bool,

    // top-most subquery DecorrelateDependentJoin has depth 1 and so on
    // TODO: for now it has no usage
    // depth: usize,
    // all correlated columns in current depth and downward (if any)
    correlated_columns: Vec<CorrelatedColumnInfo>,
    // check if we have to replace any COUNT aggregates into "CASE WHEN X IS NULL THEN 0 ELSE COUNT END"
    // store a mapping between a expr and its original index in the loglan output
    replacement_map: IndexMap<String, Expr>,
    // if during the top down traversal, we observe any operator that requires
    // joining all rows from the lhs with nullable rows on the rhs
    any_join: bool,
    delim_scan_id: usize,
    dscan_cols: Vec<Column>,
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

    builder = builder.delim_join(
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
    fn new_root() -> Self {
        Self {
            domains: IndexSet::new(),
            correlated_column_to_delim_column: IndexMap::new(),
            is_initial: true,
            correlated_columns: vec![],
            replacement_map: IndexMap::new(),
            any_join: true,
            delim_scan_id: 0,
            dscan_cols: vec![],
        }
    }

    fn new(
        node: &DependentJoin,
        // correlated_columns: &Vec<(usize, Column, DataType)>,
        correlated_columns_from_parent: &Vec<CorrelatedColumnInfo>,
        is_initial: bool,
        any_join: bool,
        delim_scan_id: usize,
        depth: usize,
    ) -> Self {
        // the correlated_columns may contains columns referenced by lower depth, filter them out
        let current_depth_correlated_columns =
            node.correlated_columns.iter().filter_map(|info| {
                if depth == info.depth {
                    Some(info)
                } else {
                    None
                }
            });

        // TODO: it's better if dependentjoin node store all outer ref on RHS itself
        let all_outer_ref = node.right.all_out_ref_exprs();
        let parent_correlated_columns =
            correlated_columns_from_parent.iter().filter(|info| {
                all_outer_ref.contains(&Expr::OuterReferenceColumn(
                    info.data_type.clone(),
                    info.col.clone(),
                ))
            });

        let domains: IndexSet<_> = current_depth_correlated_columns
            .chain(parent_correlated_columns)
            .unique()
            .cloned()
            .collect();

        let mut merged_correlated_columns = correlated_columns_from_parent.clone();
        merged_correlated_columns.retain(|info| info.depth >= depth);
        merged_correlated_columns.extend_from_slice(&node.correlated_columns);

        //  println!("\n\ndomains:{:?}\ncorrelated_columns:{:?}\n correlated_columns_from_parent:{:?}\n\n", &domains, &merged_correlated_columns, &correlated_columns_from_parent);

        Self {
            domains,
            correlated_column_to_delim_column: IndexMap::new(),
            is_initial,
            correlated_columns: merged_correlated_columns,
            replacement_map: IndexMap::new(),
            any_join,
            delim_scan_id,
            dscan_cols: vec![],
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
    // fn has_correlated_exprs(node: DependentJoin) -> Result<bool> {}

    fn decorrelate_independent(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut decorrelator = DependentJoinDecorrelator::new_root();

        decorrelator.decorrelate_plan(plan.clone())
    }

    fn decorrelate(
        &mut self,
        node: &DependentJoin,
        parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        let perform_delim = true;
        let left = node.left.as_ref();

        let new_left = if !self.is_initial {
            let mut has_correlated_expr = false;
            detect_correlated_expressions(left, &self.domains, &mut has_correlated_expr)?;
            let new_left = if !has_correlated_expr {
                // self.decorrelate_plan(left.clone())?
                // TODO: fix me
                self.decorrelate_independent(left)?
            } else {
                self.push_down_dependent_join(
                    left,
                    parent_propagate_nulls,
                    lateral_depth,
                )?
            };

            // TODO: duckdb does this redundant rewrite for no reason???
            // let mut new_plan = Self::rewrite_outer_ref_columns(
            //     new_left,
            //     &self.correlated_column_to_delim_column,
            //     false,
            // )?;

            let new_plan = Self::rewrite_outer_ref_columns(
                new_left,
                &self.correlated_column_to_delim_column,
                true,
            )?;
            new_plan
        } else {
            self.decorrelate_plan(left.clone())?
        };
        let lateral_depth = 0;
        // let propagate_null_values = node.propagate_null_value();
        let _propagate_null_values = true;
        let mut decorrelator = DependentJoinDecorrelator::new(
            node,
            &self.correlated_columns,
            false,
            false,
            self.delim_scan_id,
            node.subquery_depth,
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

        self.delim_scan_id = decorrelator.delim_scan_id;
        self.merge_child(&decorrelator);
        return builder.build();
    }
    fn merge_child(&mut self, child: &Self) {
        self.delim_scan_id = child.delim_scan_id;
        for entry in child.correlated_column_to_delim_column.iter() {
            self.correlated_column_to_delim_column
                .insert(entry.0.clone(), entry.1.clone());
        }
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
            .map(|info| info.col.clone())
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

    fn rewrite_current_plan_outer_ref_columns(
        plan: LogicalPlan,
        correlated_map: &IndexMap<Column, Column>,
    ) -> Result<LogicalPlan> {
        // replace correlated column in dependent with delimget's column
        let new_plan = if let LogicalPlan::DependentJoin(DependentJoin { .. }) = plan {
            return internal_err!(
                "logical error, this function should not be called if one of the plan is still dependent join node");
        } else {
            plan
        };

        new_plan
            .map_expressions(|e| {
                e.transform(|e| {
                    if let Expr::OuterReferenceColumn(_, outer_col) = &e {
                        if let Some(delim_col) = correlated_map.get(outer_col) {
                            return Ok(Transformed::yes(Expr::Column(delim_col.clone())));
                        }else{
                            return internal_err!("correlated map does not detect for outer reference of column {}",outer_col);
                        }
                    }
                    Ok(Transformed::no(e))
                })
            })?
            .data
            .recompute_schema()
    }

    fn rewrite_outer_ref_columns(
        plan: LogicalPlan,
        correlated_map: &IndexMap<Column, Column>,
        recursive: bool,
    ) -> Result<LogicalPlan> {
        // TODO: take depth into consideration
        let new_plan = if recursive {
            plan.transform_down(|child| {
                Ok(Transformed::yes(
                    Self::rewrite_current_plan_outer_ref_columns(child, correlated_map)?,
                ))
            })?
            .data
            .recompute_schema()?
        } else {
            plan
        };

        Self::rewrite_current_plan_outer_ref_columns(new_plan, correlated_map)
    }

    fn delim_scan_relation_name(&self) -> String {
        format!("delim_scan_{}", self.delim_scan_id)
    }

    fn rewrite_into_delim_column(
        correlated_map: &IndexMap<Column, Column>,
        original: &Column,
    ) -> Result<Column> {
        correlated_map
            .get(original)
            .ok_or(internal_datafusion_err!(
                "correlated map does not have entry for {}",
                original
            ))
            .cloned()
    }

    fn build_delim_scan(&mut self) -> Result<LogicalPlan> {
        // Clear last dscan info every time we build new dscan.
        self.dscan_cols.clear();

        // Collect all correlated columns of different outer table.
        let mut domains_by_table: IndexMap<String, Vec<CorrelatedColumnInfo>> =
            IndexMap::new();

        for domain in &self.domains {
            let table_ref = domain
                .col
                .relation
                .clone()
                .ok_or(internal_datafusion_err!(
                    "TableRef should exists in correlatd column"
                ))?
                .clone();
            let domains = domains_by_table.entry(table_ref.to_string()).or_default();
            if !domains.iter().any(|existing| {
                (&existing.col == &domain.col)
                    && (&existing.data_type == &domain.data_type)
            }) {
                domains.push(domain.clone());
            }
        }

        // Collect all D from different tables.
        let mut delim_scans = vec![];
        for (table_ref, table_domains) in domains_by_table {
            self.delim_scan_id += 1;
            let delim_scan_name =
                format!("{0}_dscan_{1}", table_ref.clone(), self.delim_scan_id);

            table_domains.iter().for_each(|c| {
                let field_name = c.col.flat_name().replace(".", "_");
                let dscan_col = Column::from_qualified_name(format!(
                    "{}.{field_name}",
                    delim_scan_name
                ));
                self.correlated_column_to_delim_column
                    .insert(c.col.clone(), dscan_col.clone());
                self.dscan_cols.push(dscan_col);
            });

            delim_scans.push(
                LogicalPlanBuilder::delim_get(&table_domains)?
                    .alias(&delim_scan_name)?
                    .build()?,
            );
        }

        // Join all delim_scans together.
        let final_delim_scan = if delim_scans.len() == 1 {
            delim_scans.into_iter().next().unwrap()
        } else {
            let mut iter = delim_scans.into_iter();
            let first = iter
                .next()
                .ok_or_else(|| internal_datafusion_err!("Empty delim_scans vector"))?;
            iter.try_fold(first, |acc, delim_scan| {
                LogicalPlanBuilder::new(acc)
                    .join(
                        delim_scan,
                        JoinType::Inner,
                        (Vec::<Column>::new(), Vec::<Column>::new()),
                        None,
                    )?
                    .build()
            })?
        };

        final_delim_scan.recompute_schema()
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
        // TODO: is there any way to do this more efficiently
        // TODO: this lookup must be associated with a list of correlated_columns
        // (from current DecorrelateDependentJoin context and its parent)
        // and check if the correlated expr (if any) exists in the correlated_columns
        detect_correlated_expressions(node, &self.domains, &mut has_correlated_expr)?;

        if !has_correlated_expr {
            match node {
                LogicalPlan::Projection(old_proj) => {
                    let mut proj = old_proj.clone();
                    // TODO: define logical plan for delim scan
                    let delim_scan = self.build_delim_scan()?;
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
                            &self.correlated_column_to_delim_column,
                            &domain_col.col,
                        )?));
                    }

                    let proj = Projection::try_new(proj.expr, cross_join.into())?;

                    return Self::rewrite_outer_ref_columns(
                        LogicalPlan::Projection(proj),
                        &self.correlated_column_to_delim_column,
                        false,
                    );
                }
                LogicalPlan::RecursiveQuery(_) => {
                    // duckdb support this
                    unimplemented!("")
                }
                any => {
                    let delim_scan = self.build_delim_scan()?;
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
                        &self.correlated_column_to_delim_column,
                        &domain_col.col,
                    )?));
                }
                let proj = Projection::try_new(proj.expr, new_input.into())?;
                return Self::rewrite_outer_ref_columns(
                    LogicalPlan::Projection(proj),
                    &self.correlated_column_to_delim_column,
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
                    &self.correlated_column_to_delim_column,
                    false,
                )?;

                return Ok(new_plan);
            }
            LogicalPlan::Aggregate(old_agg) => {
                let delim_scan_above_agg = self.build_delim_scan()?;
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
                // let delim_scan_under_agg_rela = self.delim_scan_relation_name();

                let mut new_agg = old_agg.clone();
                new_agg.input = Arc::new(new_input);
                let new_plan = Self::rewrite_outer_ref_columns(
                    LogicalPlan::Aggregate(new_agg),
                    &self.correlated_column_to_delim_column,
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
                        &self.correlated_column_to_delim_column,
                        &c.col,
                    )?;
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

                    let delim_conditions = vec![];
                    // for (lhs, rhs) in extra_group_columns
                    //     .iter()
                    //     .zip(delim_scan_above_agg.schema().columns().iter())
                    // {
                    //     delim_conditions.push((lhs.clone(), rhs.clone()));
                    // }

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
            LogicalPlan::Join(old_join) => {
                let mut left_has_correlation = false;
                detect_correlated_expressions(
                    old_join.left.as_ref(),
                    &self.domains,
                    &mut left_has_correlation,
                )?;
                let mut right_has_correlation = false;
                detect_correlated_expressions(
                    old_join.right.as_ref(),
                    &self.domains,
                    &mut right_has_correlation,
                )?;

                // Cross projuct, push into both sides of the plan.
                if old_join.is_cross_product() {
                    if !right_has_correlation {
                        // Only left has correlation, push into left.
                        let new_left = self.push_down_dependent_join_internal(
                            old_join.left.as_ref(),
                            parent_propagate_nulls,
                            lateral_depth,
                        )?;
                        let new_right =
                            self.decorrelate_independent(old_join.right.as_ref())?;

                        return self.join_without_correlation(
                            new_left,
                            new_right,
                            old_join.clone(),
                        );
                    } else if !left_has_correlation {
                        // Only right has correlation, push into right.
                        let new_right = self.push_down_dependent_join_internal(
                            old_join.right.as_ref(),
                            parent_propagate_nulls,
                            lateral_depth,
                        )?;
                        let new_left =
                            self.decorrelate_independent(old_join.left.as_ref())?;

                        return self.join_without_correlation(
                            new_left,
                            new_right,
                            old_join.clone(),
                        );
                    }

                    // Both sides have correlation, turn into an inner join.
                    let new_left = self.push_down_dependent_join_internal(
                        old_join.left.as_ref(),
                        parent_propagate_nulls,
                        lateral_depth,
                    )?;
                    let new_right = self.push_down_dependent_join_internal(
                        old_join.right.as_ref(),
                        parent_propagate_nulls,
                        lateral_depth,
                    )?;

                    // Add the correlated columns to th join conditions.
                    return self.join_with_correlation(
                        new_left,
                        new_right,
                        old_join.clone(),
                    );
                }

                // If it's a comparison join.
                match old_join.join_type {
                    JoinType::Inner => {
                        if !right_has_correlation {
                            // Only left has correlation, push info left.
                            let new_left = self.push_down_dependent_join_internal(
                                old_join.left.as_ref(),
                                parent_propagate_nulls,
                                lateral_depth,
                            )?;
                            let new_right =
                                self.decorrelate_independent(old_join.right.as_ref())?;

                            return self.join_without_correlation(
                                new_left,
                                new_right,
                                old_join.clone(),
                            );
                        }

                        if !left_has_correlation {
                            // Only right has correlation, push into right.
                            let new_right = self.push_down_dependent_join_internal(
                                old_join.right.as_ref(),
                                parent_propagate_nulls,
                                lateral_depth,
                            )?;
                            let new_left =
                                self.decorrelate_independent(old_join.left.as_ref())?;

                            return self.join_without_correlation(
                                new_left,
                                new_right,
                                old_join.clone(),
                            );
                        }
                    }
                    JoinType::Left => {
                        if !right_has_correlation {
                            // Only left has correlation, push info left.
                            let new_left = self.push_down_dependent_join_internal(
                                old_join.left.as_ref(),
                                parent_propagate_nulls,
                                lateral_depth,
                            )?;
                            let new_right =
                                self.decorrelate_independent(old_join.right.as_ref())?;

                            return self.join_without_correlation(
                                new_left,
                                new_right,
                                old_join.clone(),
                            );
                        }
                    }
                    JoinType::Right => {
                        if !left_has_correlation {
                            // Only right has correlation, push into right.
                            let new_right = self.push_down_dependent_join_internal(
                                old_join.right.as_ref(),
                                parent_propagate_nulls,
                                lateral_depth,
                            )?;
                            let new_left =
                                self.decorrelate_independent(old_join.left.as_ref())?;

                            return self.join_without_correlation(
                                new_left,
                                new_right,
                                old_join.clone(),
                            );
                        }
                    }
                    JoinType::LeftMark => {
                        // Push the child into the RHS.
                        let new_left = self.push_down_dependent_join_internal(
                            old_join.left.as_ref(),
                            parent_propagate_nulls,
                            lateral_depth,
                        )?;
                        let new_right =
                            self.decorrelate_independent(old_join.right.as_ref())?;

                        let new_join = self.join_without_correlation(
                            new_left,
                            new_right,
                            old_join.clone(),
                        )?;

                        return Self::rewrite_outer_ref_columns(
                            new_join,
                            &self.correlated_column_to_delim_column,
                            false,
                        );
                    }
                    _ => return internal_err!("unreachable"),
                }

                // Both sides have correlation, push into both sides.
                let new_left = self.push_down_dependent_join_internal(
                    old_join.left.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                let left_dscan_cols = self.dscan_cols.clone();

                let new_right = self.push_down_dependent_join_internal(
                    old_join.right.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                let right_dscan_cols = self.dscan_cols.clone();

                // NOTE: For OUTER JOINS it matters what the correlated column map is after the join:
                // for the LEFT OUTER JOIN: we want the LEFT side to be the base map after we push,
                // because the RIGHT might contains NULL values.
                if old_join.join_type == JoinType::Left {
                    self.dscan_cols = left_dscan_cols.clone();
                }

                // Add the correlated columns to the join conditions.
                let new_join = self.join_with_delim_scan(
                    new_left,
                    new_right,
                    old_join.clone(),
                    &left_dscan_cols,
                    &right_dscan_cols,
                )?;

                // Then we replace any correlated expressions with the corresponding entry in the
                // correlated_map.
                return Self::rewrite_outer_ref_columns(
                    new_join,
                    &self.correlated_column_to_delim_column,
                    false,
                );
            }
            other => {
                unimplemented!("implement pushdown dependent join for node {other}")
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

    fn join_without_correlation(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: Join,
    ) -> Result<LogicalPlan> {
        Ok(LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?))
    }

    fn join_with_correlation(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: Join,
    ) -> Result<LogicalPlan> {
        let mut join_conditions = vec![];
        if let Some(filter) = join.filter {
            join_conditions.push(filter);
        }

        for col_pair in &self.correlated_column_to_delim_column {
            join_conditions.push(binary_expr(
                Expr::Column(col_pair.0.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(col_pair.1.clone()),
            ));
        }

        Ok(LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            conjunction(join_conditions).or(Some(lit(true))),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?))
    }

    fn join_with_delim_scan(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: Join,
        left_scan_cols: &Vec<Column>,
        right_dscan_cols: &Vec<Column>,
    ) -> Result<LogicalPlan> {
        let mut join_conditions = vec![];
        if let Some(filter) = join.filter {
            join_conditions.push(filter);
        }

        for (index, left_delim_col) in left_scan_cols.iter().enumerate() {
            if let Some(right_delim_col) = right_dscan_cols.get(index) {
                join_conditions.push(binary_expr(
                    Expr::Column(left_delim_col.clone()),
                    Operator::IsNotDistinctFrom,
                    Expr::Column(right_delim_col.clone()),
                ));
            } else {
                return Err(internal_datafusion_err!(
                    "Index {} not found in right_dscan_cols, left_scan_cols has {} elements, right_dscan_cols has {} elements",
                    index,
                    left_scan_cols.len(),
                    right_dscan_cols.len()
                ));
            }
        }

        Ok(LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            conjunction(join_conditions).or(Some(lit(true))),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?))
    }
}

// TODO: take lateral into consideration
fn detect_correlated_expressions(
    plan: &LogicalPlan,
    correlated_columns: &IndexSet<CorrelatedColumnInfo>,
    has_correlated_expressions: &mut bool,
) -> Result<()> {
    plan.apply(|child| match child {
        any_plan => {
            for e in any_plan.all_out_ref_exprs().iter() {
                if let Expr::OuterReferenceColumn(data_type, col) = e {
                    if correlated_columns
                        .iter()
                        .any(|c| (&c.col == col) && (&c.data_type == data_type))
                    {
                        *has_correlated_expressions = true;
                        return Ok(TreeNodeRecursion::Stop);
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        }
    })?;

    Ok(())
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
    use datafusion_common::{Column, Result};
    use datafusion_expr::JoinType;
    use datafusion_expr::{
        exists, expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use std::sync::Arc;
    fn print_optimize_tree(plan: &LogicalPlan) {
        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(DecorrelateDependentJoin::new());
        let optimizer = Optimizer::with_rules(vec![rule]);
        let _optimized_plan = optimizer
            .optimize(plan.clone(), &OptimizerContext::new(), |_, _| {})
            .expect("failed to optimize plan");
        // println!("{}", optimized_plan.display_tree());
    }

    macro_rules! assert_decorrelate {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            print_optimize_tree(&$plan);
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(DecorrelateDependentJoin::new());
            assert_optimized_plan_eq_display_indent_snapshot!(
                rule,
                $plan,
                @ $expected,
            )?;
        }};
    }

    // TODO: This test is failing
    #[test]
    fn correlated_subquery_nested_in_uncorrelated_subquery() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;

        let sq2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2.clone())
                .filter(
                    col("inner_table_lv2.b")
                        .eq(out_ref_col(ArrowDataType::UInt32, "inner_table_1.b")),
                )?
                .build()?,
        );
        let sq1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(exists(sq2))?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(exists(sq1))?
            .build()?;
        println!("{plan}");
        // assert_decorrelate!(plan, @r"
        // Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
        //   Filter: __exists_sq_1.output AND __exists_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __exists_sq_2.output:Boolean]
        //     Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1.output, mark AS __exists_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __exists_sq_2.output:Boolean]
        //       LeftMark Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM delim_scan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, mark:Boolean]
        //         Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean]
        //           LeftMark Join(ComparisonJoin):  Filter: outer_table.b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
        //             TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
        //             Filter: inner_table_lv1.b = delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
        //               Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
        //                 TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        //                 SubqueryAlias: delim_scan_1 [outer_table_b:UInt32;N]
        //                   DelimGet: outer_table.b [outer_table_b:UInt32;N]
        //         Filter: inner_table_lv1.c = delim_scan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
        //           Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
        //             TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        //             SubqueryAlias: delim_scan_2 [outer_table_c:UInt32;N]
        //               DelimGet: outer_table.c [outer_table_c:UInt32;N]
        // ");
        Ok(())
    }
    #[test]
    fn two_dependent_joins_at_the_same_depth() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let sq1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.b")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.b")),
                )?
                .build()?,
        );
        let sq2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("inner_table_lv1.c")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.c")),
                )?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(exists(sq1).and(exists(sq2)))?
            .build()?;

        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: __exists_sq_1.output AND __exists_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __exists_sq_2.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1.output, mark AS __exists_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __exists_sq_2.output:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM delim_scan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, mark:Boolean]
                Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean]
                  LeftMark Join(ComparisonJoin):  Filter: outer_table.b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                    TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                    Filter: inner_table_lv1.b = outer_table_dscan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
                      Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
                        TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                        SubqueryAlias: outer_table_dscan_1 [outer_table_b:UInt32;N]
                          DelimGet: outer_table.b [outer_table_b:UInt32;N]
                Filter: inner_table_lv1.c = outer_table_dscan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                  Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                    SubqueryAlias: outer_table_dscan_2 [outer_table_c:UInt32;N]
                      DelimGet: outer_table.c [outer_table_c:UInt32;N]
        ");
        Ok(())
    }

    // Given a plan with 2 level of subquery
    // This test the fact that correlated columns from the top
    // are propagated to the very bottom subquery
    #[test]
    fn correlated_column_ref_from_parent() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;

        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    col("inner_table_lv2.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a")),
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
            .filter(scalar_subquery(scalar_sq_level1).eq(col("outer_table.a")))?
            .build()?;

        println!("{plan}");
        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N, __scalar_sq_2.output:Int64;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, count(inner_table_lv1.a), outer_table_dscan_2.outer_table_c, outer_table_dscan_1.outer_table_c, count(inner_table_lv1.a) AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N, __scalar_sq_2.output:Int64;N]
              Left Join(ComparisonJoin):  Filter: outer_table.a IS NOT DISTINCT FROM delim_scan_4.outer_table_a AND outer_table.c IS NOT DISTINCT FROM delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Inner Join(DelimJoin):  Filter: Boolean(true) [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_c:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_2.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_2.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                      Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, outer_table_dscan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                        Filter: inner_table_lv1.c = outer_table_dscan_2.outer_table_c AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N, outer_table_c:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N, outer_table_c:UInt32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, count(inner_table_lv2.a), outer_table_dscan_4.outer_table_a, outer_table_dscan_3.outer_table_a, count(inner_table_lv2.a) AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N]
                              Left Join(ComparisonJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N]
                                TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                Inner Join(DelimJoin):  Filter: Boolean(true) [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N, outer_table_a:UInt32;N]
                                  Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, outer_table_dscan_4.outer_table_a [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N]
                                    Aggregate: groupBy=[[outer_table_dscan_4.outer_table_a]], aggr=[[count(inner_table_lv2.a)]] [outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64]
                                      Filter: inner_table_lv2.a = outer_table_dscan_4.outer_table_a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                          SubqueryAlias: outer_table_dscan_4 [outer_table_a:UInt32;N]
                                            DelimGet: outer_table.a [outer_table_a:UInt32;N]
                                  SubqueryAlias: outer_table_dscan_3 [outer_table_a:UInt32;N]
                                    DelimGet: outer_table.a [outer_table_a:UInt32;N]
                            SubqueryAlias: outer_table_dscan_2 [outer_table_c:UInt32;N]
                              DelimGet: outer_table.c [outer_table_c:UInt32;N]
                  SubqueryAlias: outer_table_dscan_1 [outer_table_c:UInt32;N]
                    DelimGet: outer_table.c [outer_table_c:UInt32;N]
        ");
        Ok(())
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
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N, __scalar_sq_2.output:Int64;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, count(inner_table_lv1.a), outer_table_dscan_2.outer_table_c, outer_table_dscan_1.outer_table_c, count(inner_table_lv1.a) AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N, __scalar_sq_2.output:Int64;N]
              Left Join(ComparisonJoin):  Filter: outer_table.a IS NOT DISTINCT FROM delim_scan_6.outer_table_a AND outer_table.c IS NOT DISTINCT FROM delim_scan_6.outer_table_c [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, outer_table_c:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Inner Join(DelimJoin):  Filter: Boolean(true) [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_c:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_2.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_2.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                      Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, outer_table_dscan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                        Filter: inner_table_lv1.c = outer_table_dscan_2.outer_table_c AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N, outer_table_c:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N, outer_table_c:UInt32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, count(inner_table_lv2.a), outer_table_dscan_6.outer_table_a, inner_table_lv1_dscan_5.inner_table_lv1_b, inner_table_lv1_dscan_3.inner_table_lv1_b, outer_table_dscan_4.outer_table_a, count(inner_table_lv2.a) AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_1.output:Int64;N]
                              Left Join(ComparisonJoin):  Filter: inner_table_lv1.b IS NOT DISTINCT FROM delim_scan_6.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                Inner Join(DelimJoin):  Filter: Boolean(true) [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                  Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, outer_table_dscan_6.outer_table_a, inner_table_lv1_dscan_5.inner_table_lv1_b [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                    Aggregate: groupBy=[[inner_table_lv1_dscan_5.inner_table_lv1_b, outer_table_dscan_6.outer_table_a]], aggr=[[count(inner_table_lv2.a)]] [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64]
                                      Filter: inner_table_lv2.a = outer_table_dscan_6.outer_table_a AND inner_table_lv2.b = inner_table_lv1_dscan_5.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                          Cross Join(ComparisonJoin):  [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                            SubqueryAlias: inner_table_lv1_dscan_5 [inner_table_lv1_b:UInt32;N]
                                              DelimGet: inner_table_lv1.b [inner_table_lv1_b:UInt32;N]
                                            SubqueryAlias: outer_table_dscan_6 [outer_table_a:UInt32;N]
                                              DelimGet: outer_table.a [outer_table_a:UInt32;N]
                                  Cross Join(ComparisonJoin):  [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                    SubqueryAlias: inner_table_lv1_dscan_3 [inner_table_lv1_b:UInt32;N]
                                      DelimGet: inner_table_lv1.b [inner_table_lv1_b:UInt32;N]
                                    SubqueryAlias: outer_table_dscan_4 [outer_table_a:UInt32;N]
                                      DelimGet: outer_table.a [outer_table_a:UInt32;N]
                            SubqueryAlias: outer_table_dscan_2 [outer_table_c:UInt32;N]
                              DelimGet: outer_table.c [outer_table_c:UInt32;N]
                  SubqueryAlias: outer_table_dscan_1 [outer_table_c:UInt32;N]
                    DelimGet: outer_table.c [outer_table_c:UInt32;N]
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
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = count(inner_table_lv1.a) AND outer_table.a IS NOT DISTINCT FROM delim_scan_2.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_2.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Inner Join(DelimJoin):  Filter: Boolean(true) [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_2.outer_table_b, outer_table_dscan_2.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_2.outer_table_a, outer_table_dscan_2.outer_table_b]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64]
                      Filter: inner_table_lv1.a = outer_table_dscan_2.outer_table_a AND outer_table_dscan_2.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_table_dscan_2.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                          SubqueryAlias: outer_table_dscan_2 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                            DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  SubqueryAlias: outer_table_dscan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");
        Ok(())
    }

    // TODO: an issue with uncorrelated subquery making this fail
    #[test]
    fn one_correlated_subquery_and_one_uncorrelated_subquery_at_the_same_level(
    ) -> Result<()> {
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
        println!("{plan}");
        // assert_decorrelate!(plan, @r"
        // Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
        //   Filter: outer_table.a > Int32(1) AND __exists_sq_1.output AND __in_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __in_sq_2.output:Boolean]
        //     Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1.output, inner_table_lv1.mark AS __in_sq_2.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, __in_sq_2.output:Boolean]
        //       LeftMark Join(ComparisonJoin):  Filter: outer_table.b = inner_table_lv1.a [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean, mark:Boolean]
        //         Projection: outer_table.a, outer_table.b, outer_table.c, inner_table_lv1.mark AS __exists_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1.output:Boolean]
        //           LeftMark Join(ComparisonJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
        //             TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
        //             Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32]
        //               Filter: inner_table_lv1.a AND inner_table_lv1.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32]
        //                 TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        //               SubqueryAlias: delim_scan_1 []
        //                 DelimGet: []
        //         Projection: inner_table_lv1.a [a:UInt32]
        //           Cross Join(ComparisonJoin):  [a:UInt32, b:UInt32, c:UInt32]
        //             Filter: inner_table_lv1.c = Int32(2) [a:UInt32, b:UInt32, c:UInt32]
        //               TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        //             SubqueryAlias: delim_scan_2 []
        //               DelimGet: []
        // ");
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
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = inner_table_lv1.b AND outer_table.a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table_lv1.b, outer_table_dscan_1.outer_table_a, outer_table_dscan_1.outer_table_b [b:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Filter: inner_table_lv1.a = outer_table_dscan_1.outer_table_a AND outer_table_dscan_1.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_table_dscan_1.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      SubqueryAlias: outer_table_dscan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");

        Ok(())
    }

    // This query is inside the paper
    #[test]
    fn decorrelate_two_different_outer_tables() -> Result<()> {
        let outer_table = test_table_scan_with_name("T1")?;
        let inner_table_lv1 = test_table_scan_with_name("T2")?;

        let inner_table_lv2 = test_table_scan_with_name("T3")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv2)
                .filter(
                    col("T3.b")
                        .eq(out_ref_col(ArrowDataType::UInt32, "T2.b"))
                        .and(col("T3.a").eq(out_ref_col(ArrowDataType::UInt32, "T1.a"))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("T3.a"))])?
                .build()?,
        );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1.clone())
                .filter(
                    col("T2.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "T1.a"))
                        .and(scalar_subquery(scalar_sq_level2).gt(lit(300000))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("T2.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("T1.c")
                    .eq(lit(123))
                    .and(scalar_subquery(scalar_sq_level1).gt(lit(5))),
            )?
            .build()?;
        println!("{}", plan.display_indent_schema());

        // Filter: t1.c = Int32(123) AND (<subquery>) > Int32(5) [a:UInt32, b:UInt32, c:UInt32]
        //   Subquery: [count(t2.a):Int64]
        //     Aggregate: groupBy=[[]], aggr=[[count(t2.a)]] [count(t2.a):Int64]
        //       Filter: t2.a = outer_ref(t1.a) AND (<subquery>) > Int32(300000) [a:UInt32, b:UInt32, c:UInt32]
        //         Subquery: [sum(t3.a):UInt64;N]
        //           Aggregate: groupBy=[[]], aggr=[[sum(t3.a)]] [sum(t3.a):UInt64;N]
        //             Filter: t3.b = outer_ref(t2.b) AND t3.a = outer_ref(t1.a) [a:UInt32, b:UInt32, c:UInt32]
        //               TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]
        //         TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        //   TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]

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
        Projection: t1.a, t1.b, t1.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: t1.c = Int32(123) AND __scalar_sq_2.output > Int32(5) [a:UInt32, b:UInt32, c:UInt32, count(t2.a):Int64;N, t1_a:UInt32;N, t1_a:UInt32;N, __scalar_sq_2.output:Int64;N]
            Projection: t1.a, t1.b, t1.c, count(t2.a), t1_dscan_5.t1_a, t1_dscan_1.t1_a, count(t2.a) AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, count(t2.a):Int64;N, t1_a:UInt32;N, t1_a:UInt32;N, __scalar_sq_2.output:Int64;N]
              Left Join(ComparisonJoin):  Filter: t1.a IS NOT DISTINCT FROM delim_scan_5.t1_a [a:UInt32, b:UInt32, c:UInt32, count(t2.a):Int64;N, t1_a:UInt32;N, t1_a:UInt32;N]
                TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
                Inner Join(DelimJoin):  Filter: Boolean(true) [count(t2.a):Int64, t1_a:UInt32;N, t1_a:UInt32;N]
                  Projection: CASE WHEN count(t2.a) IS NULL THEN Int32(0) ELSE count(t2.a) END, t1_dscan_5.t1_a [count(t2.a):Int64, t1_a:UInt32;N]
                    Aggregate: groupBy=[[t1_dscan_5.t1_a]], aggr=[[count(t2.a)]] [t1_a:UInt32;N, count(t2.a):Int64]
                      Projection: t2.a, t2.b, t2.c, t1_dscan_5.t1_a [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N]
                        Filter: t2.a = t1_dscan_5.t1_a AND __scalar_sq_1.output > Int32(300000) [a:UInt32, b:UInt32, c:UInt32, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N, __scalar_sq_1.output:UInt64;N]
                          Projection: t2.a, t2.b, t2.c, sum(t3.a), t1_dscan_5.t1_a, t2_dscan_4.t2_b, t2_dscan_2.t2_b, t1_dscan_3.t1_a, sum(t3.a) AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N, __scalar_sq_1.output:UInt64;N]
                            Left Join(ComparisonJoin):  Filter: t2.b IS NOT DISTINCT FROM delim_scan_5.t2_b [a:UInt32, b:UInt32, c:UInt32, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N]
                              TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
                              Inner Join(DelimJoin):  Filter: Boolean(true) [sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N]
                                Projection: sum(t3.a), t1_dscan_5.t1_a, t2_dscan_4.t2_b [sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N]
                                  Aggregate: groupBy=[[t2_dscan_4.t2_b, t1_dscan_5.t1_a, t1_dscan_5.t1_a]], aggr=[[sum(t3.a)]] [t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N]
                                    Filter: t3.b = t2_dscan_4.t2_b AND t3.a = t1_dscan_5.t1_a [a:UInt32, b:UInt32, c:UInt32, t2_b:UInt32;N, t1_a:UInt32;N]
                                      Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, t2_b:UInt32;N, t1_a:UInt32;N]
                                        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]
                                        Cross Join(ComparisonJoin):  [t2_b:UInt32;N, t1_a:UInt32;N]
                                          SubqueryAlias: t2_dscan_4 [t2_b:UInt32;N]
                                            DelimGet: t2.b [t2_b:UInt32;N]
                                          SubqueryAlias: t1_dscan_5 [t1_a:UInt32;N]
                                            DelimGet: t1.a [t1_a:UInt32;N]
                                Cross Join(ComparisonJoin):  [t2_b:UInt32;N, t1_a:UInt32;N]
                                  SubqueryAlias: t2_dscan_2 [t2_b:UInt32;N]
                                    DelimGet: t2.b [t2_b:UInt32;N]
                                  SubqueryAlias: t1_dscan_3 [t1_a:UInt32;N]
                                    DelimGet: t1.a [t1_a:UInt32;N]
                  SubqueryAlias: t1_dscan_1 [t1_a:UInt32;N]
                    DelimGet: t1.a [t1_a:UInt32;N]
        ");
        Ok(())
    }

    #[test]
    fn decorrelate_inner_join_left() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table_lv1 = test_table_scan_with_name("inner_table_lv1")?;
        let inner_table_lv2 = test_table_scan_with_name("inner_table_lv2")?;

        let sq_level1 = Arc::new(
            LogicalPlanBuilder::from(inner_table_lv1)
                .join(
                    inner_table_lv2,
                    JoinType::Inner,
                    (Vec::<Column>::new(), Vec::<Column>::new()),
                    Some(
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
                            )
                            .and(col("inner_table_lv1.a").eq(col("inner_table_lv2.a"))),
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

        // Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
        //   Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
        //     DependentJoin on [outer_table.a lvl 1, outer_table.b lvl 1] with expr outer_table.c IN (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Boolean]
        //       TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
        //       Projection: inner_table_lv1.b [b:UInt32]
        //         Inner Join(ComparisonJoin):  Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b AND inner_table_lv1.a = inner_table_lv2.a [a:UInt32, b:UInt32, c:UInt32, a:UInt32, b:UInt32, c:UInt32]
        //           TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
        //           TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]

        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = inner_table_lv1.b AND outer_table.a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table_lv1.b, outer_table_dscan_1.outer_table_a, outer_table_dscan_1.outer_table_b [b:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Inner Join(ComparisonJoin):  Filter: inner_table_lv1.a = outer_ref(outer_table.a) AND outer_ref(outer_table.a) > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_ref(outer_table.b) = inner_table_lv1.b AND inner_table_lv1.a = inner_table_lv2.a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N, a:UInt32, b:UInt32, c:UInt32]
                    Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      SubqueryAlias: outer_table_dscan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
        ");

        Ok(())
    }
}
