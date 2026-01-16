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

use crate::analyzer::type_coercion::TypeCoercionRewriter;
use crate::deliminator::Deliminator;
use crate::rewrite_dependent_join::DependentJoinRewriter;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};
use std::ops::Deref;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{
    internal_datafusion_err, internal_err, not_impl_datafusion_err, Column, Result,
};
use datafusion_expr::expr::{
    self, Exists, InSubquery, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::utils::conjunction;
use datafusion_expr::{
    binary_expr, col, lit, not, when, Aggregate, CorrelatedColumnInfo, DependentJoin,
    Expr, FetchType, GroupingSet, Join, JoinType, LogicalPlan, LogicalPlanBuilder,
    Operator, SkipType, WindowFrame, WindowFunctionDefinition,
};

use datafusion_functions_window::row_number::row_number_udwf;
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

#[derive(Clone, Debug)]
pub struct DependentJoinDecorrelator {
    // immutable, defined when this object is constructed
    domains: IndexSet<CorrelatedColumnInfo>,
    // for each domain column, the corresponding column in delim_get
    correlated_map: IndexMap<Column, Column>,
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
    // this is true for the case of exists and in subquery
    any_join: bool,
    delim_scan_id: usize,
    dscan_cols: Vec<Column>,
    // TODO: check if delim_join should be disable if there is a unnest operator
    // in the subquery
    // https://github.com/duckdb/duckdb/pull/4044
    // perform_delim: bool
}

// normal join, but remove redundant columns
// i.e if we join two table with equi joins left=right
// only take the matching table on the right;
fn natural_join(
    mut builder: LogicalPlanBuilder,
    right: LogicalPlan,
    join_type: JoinType,
    conditions: Vec<(Column, Column)>,
) -> Result<LogicalPlanBuilder> {
    // let mut exclude_cols = IndexSet::new();
    let join_exprs: Vec<_> = conditions
        .iter()
        .map(|(lhs, rhs)| {
            // exclude_cols.insert(rhs);
            binary_expr(
                Expr::Column(lhs.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(rhs.clone()),
            )
        })
        .collect();
    // let require_dedup = !join_exprs.is_empty();

    builder = builder.delim_join(
        right,
        join_type,
        (Vec::<Column>::new(), Vec::<Column>::new()),
        conjunction(join_exprs).or(Some(lit(true))),
    )?;
    //if require_dedup {
    //    let remain_cols = builder.schema().columns().into_iter().filter_map(|c| {
    //        if exclude_cols.contains(&c) {
    //            None
    //        } else {
    //            Some(Expr::Column(c))
    //        }
    //    });
    //    builder.project(remain_cols)
    //} else {
    Ok(builder)
    //}
}

impl DependentJoinDecorrelator {
    fn new_root() -> Self {
        Self {
            domains: IndexSet::new(),
            correlated_map: IndexMap::new(),
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
                    info.field.clone(),
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

        Self {
            domains,
            correlated_map: IndexMap::new(),
            is_initial,
            correlated_columns: merged_correlated_columns,
            replacement_map: IndexMap::new(),
            any_join,
            delim_scan_id,
            dscan_cols: vec![],
        }
    }

    fn decorrelate_independent(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        let mut decorrelator = DependentJoinDecorrelator::new_root();

        decorrelator.decorrelate(plan, true, 0)
    }

    fn decorrelate(
        &mut self,
        plan: &LogicalPlan,
        parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        if let LogicalPlan::DependentJoin(djoin) = plan {
            self.any_join = djoin.any_join;
            let perform_delim = true;
            let left = djoin.left.as_ref();

            let new_left = if !self.is_initial {
                let mut has_correlated_expr = false;
                detect_correlated_expressions(
                    plan,
                    &self.domains,
                    &mut has_correlated_expr,
                )?;
                let new_left = if !has_correlated_expr {
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
                //     &self.correlated_map,
                //     false,
                // )?;

                let new_plan = Self::rewrite_outer_ref_columns(
                    new_left,
                    &self.correlated_map,
                    true,
                )?;
                new_plan
            } else {
                self.decorrelate(left, true, 0)?
            };
            let lateral_depth = 0;
            // let propagate_null_values = node.propagate_null_value();
            let _propagate_null_values = true;
            let mut decorrelator = DependentJoinDecorrelator::new(
                djoin,
                &self.correlated_columns,
                false,
                self.any_join,
                self.delim_scan_id,
                djoin.subquery_depth,
            );
            let right = decorrelator.push_down_dependent_join(
                &djoin.right,
                parent_propagate_nulls,
                lateral_depth,
            )?;

            let (join_condition, join_type, post_join_expr) = self
                .delim_join_conditions(
                    djoin,
                    &decorrelator,
                    right.schema().columns(),
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

            builder.build()
        } else {
            Ok(plan
                .clone()
                .map_children(|n| Ok(Transformed::yes(self.decorrelate(&n, true, 0)?)))?
                .data)
        }
    }

    fn merge_child(&mut self, child: &Self) {
        self.delim_scan_id = child.delim_scan_id;
        for entry in child.correlated_map.iter() {
            self.correlated_map.insert(entry.0.clone(), entry.1.clone());
        }
    }

    // TODO: support lateral join
    // convert dependent join into delim join
    fn delim_join_conditions(
        &self,
        node: &DependentJoin,
        decorrelator: &DependentJoinDecorrelator,
        right_columns: Vec<Column>,
        _perform_delim: bool,
    ) -> Result<(Expr, JoinType, Option<Expr>)> {
        if node.lateral_join_condition.is_some() {
            return Err(not_impl_datafusion_err!("lateral join not supported"));
        }

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
                    join_type = JoinType::LeftSingle;

                    // The reason we does not make this as a condition inside the delim join
                    // is because the evaluation of scalar_subquery expr may be needed
                    // somewhere above
                    extra_expr_after_join = Some(
                        Expr::Column(right_columns.first().unwrap().clone())
                            .alias(format!("{}", node.subquery_name)),
                    );
                }
                Expr::Exists(Exists { negated, .. }) => {
                    join_type = JoinType::LeftMark;
                    if *negated {
                        extra_expr_after_join = Some(
                            not(col("mark")).alias(format!("{}", node.subquery_name)),
                        );
                    } else {
                        extra_expr_after_join =
                            Some(col("mark").alias(format!("{}", node.subquery_name)));
                    }
                }
                Expr::InSubquery(InSubquery { expr, negated, .. }) => {
                    // TODO: looks like there is a comment that
                    // markjoin does not support fully null semantic for ANY/IN subquery
                    join_type = JoinType::LeftMark;
                    extra_expr_after_join =
                        Some(col("mark").alias(format!("{}", node.subquery_name)));
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

        let curr_lv_correlated_cols = if self.is_initial {
            node.correlated_columns
                .iter()
                .filter_map(|info| {
                    if node.subquery_depth == info.depth {
                        Some(info.clone())
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            decorrelator.domains.clone()
        };

        // TODO: natural join?
        for corr_col in curr_lv_correlated_cols.iter().unique() {
            let right_col = Self::fetch_dscan_col_from_correlated_col(
                &decorrelator.correlated_map,
                &corr_col.col,
            )?;

            join_conditions.push(binary_expr(
                col(corr_col.col.clone()),
                Operator::IsNotDistinctFrom,
                col(right_col.clone()),
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

    fn fetch_dscan_col_from_correlated_col(
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
                (&existing.col == &domain.col) && (&existing.field == &domain.field)
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

            let mut projection_exprs = vec![];
            table_domains.iter().for_each(|c| {
                let dcol_name = c.col.flat_name().replace(".", "_");
                let dscan_col = Column::from_qualified_name(format!(
                    "{}.{dcol_name}",
                    delim_scan_name.clone(),
                ));

                self.correlated_map.insert(c.col.clone(), dscan_col.clone());
                self.dscan_cols.push(dscan_col);

                // Construct alias for projection.
                projection_exprs.push(
                    col(c.col.clone())
                        .alias_qualified(delim_scan_name.clone().into(), dcol_name),
                );
            });

            // Apply projection to rename columns and then alias the entire plan.
            delim_scans.push(
                LogicalPlanBuilder::delim_get(&table_domains)?
                    .project(projection_exprs)?
                    .build()?,
            );
        }

        if delim_scans.is_empty() {
            return internal_err!("Empty delim_scans vector");
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
        // if a expr found in projection has been rewritten
        // then the upper expr no longer needs to be transformed
        plan.transform_down(|p| {
            if let LogicalPlan::DependentJoin(_) = &p {
                return internal_err!(
                    "calling rewrite_correlated_exprs while some of \
                    the plan is still dependent join plan"
                );
            }
            match &p {
                LogicalPlan::Projection(_) | LogicalPlan::Filter(_) => {
                    p.map_expressions(|e| {
                        e.transform(|e| {
                            if let Some(to_replace) = replacement.get(&e.to_string()) {
                                return Ok(Transformed::yes(to_replace.clone()));
                            }

                            return Ok(Transformed::no(e));
                        })
                    })
                }
                _ => Ok(Transformed::no(p)),
            }
        })?
        .data
        .recompute_schema()
    }

    // on recursive rewrite, make sure to update any correlated_column
    // TODO: make all of the delim join natural join
    fn push_down_dependent_join_internal(
        &mut self,
        plan: &LogicalPlan,
        parent_propagate_nulls: &mut bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        // First check if the logical plan has correlated expressions.
        let mut has_correlated_expr = false;
        detect_correlated_expressions(plan, &self.domains, &mut has_correlated_expr)?;

        let mut exit_projection = false;

        if !has_correlated_expr {
            // We reached a node without correlated expressions.
            // We can eliminate the dependent join now and create a simple cross product.
            // Now create the duplicate eliminated scan for this node.
            match plan {
                LogicalPlan::Projection(_) => {
                    // We want to keep the logical projection for positionality.
                    exit_projection = true;
                }
                LogicalPlan::RecursiveQuery(_) => {
                    // TODO: Add cte support.
                    unimplemented!("")
                }
                other => {
                    if self.domains.is_empty() {
                        // No correlated columns, nothing to do.
                        return Ok(other.clone());
                    }

                    let delim_scan = self.build_delim_scan()?;
                    let left = self.decorrelate(other, true, 0)?;
                    return Ok(natural_join(
                        LogicalPlanBuilder::new(left),
                        delim_scan,
                        JoinType::Inner,
                        vec![],
                    )?
                    .build()?);
                }
            }
        }
        match plan {
            LogicalPlan::Filter(old_filter) => {
                // TODO: any join support

                let new_input = self.push_down_dependent_join_internal(
                    old_filter.input.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;
                let mut filter = old_filter.clone();
                filter.input = Arc::new(new_input);

                return Ok(Self::rewrite_outer_ref_columns(
                    LogicalPlan::Filter(filter),
                    &self.correlated_map,
                    false,
                )?);
            }
            LogicalPlan::Projection(old_proj) => {
                // TODO: Take propagate_null_value into consideration.
                for expr in old_proj.expr.iter() {
                    *parent_propagate_nulls &= expr.propagate_null_values()?;
                }

                // If the node has no correlated expressions, push the cross product with the
                // delim scan only below the projection. This will preserve positionality of the
                // columns and prevent errors when reordering of delim scans is enabled.
                let mut proj = old_proj.clone();
                proj.input = Arc::new(if exit_projection {
                    if self.domains.is_empty() {
                        return Ok(LogicalPlan::Projection(proj));
                    }

                    let delim_scan = self.build_delim_scan()?;
                    let new_left = self.decorrelate(proj.input.deref(), true, 0)?;
                    LogicalPlanBuilder::new(new_left)
                        .join(
                            delim_scan,
                            JoinType::Inner,
                            (Vec::<Column>::new(), Vec::<Column>::new()),
                            None,
                        )?
                        .build()?
                } else {
                    self.push_down_dependent_join_internal(
                        proj.input.as_ref(),
                        parent_propagate_nulls,
                        lateral_depth,
                    )?
                });

                for domain_col in self.domains.iter() {
                    proj.expr
                        .push(col(Self::fetch_dscan_col_from_correlated_col(
                            &self.correlated_map,
                            &domain_col.col,
                        )?));
                }

                // Then we replace any correlated expressions with the corresponding entry in the
                // correlated_map.
                proj = match Self::rewrite_outer_ref_columns(
                    LogicalPlan::Projection(proj),
                    &self.correlated_map,
                    false,
                )? {
                    LogicalPlan::Projection(projection) => projection,
                    _ => {
                        return internal_err!(
                            "Expected Projection after rewrite_outer_ref_columns"
                        )
                    }
                };

                return Ok(LogicalPlan::Projection(proj));
            }
            LogicalPlan::Aggregate(old_agg) => {
                // TODO: support propagates null values
                for expr in old_agg.aggr_expr.iter() {
                    *parent_propagate_nulls &= expr.propagate_null_values()?;
                }
                for expr in old_agg.group_expr.iter() {
                    *parent_propagate_nulls &= expr.propagate_null_values()?;
                }
                // parent_propagate_null_values &= expr->PropagatesNullValues();

                // First we flatten the dependent join in the child of the projection.
                let new_input = self.push_down_dependent_join_internal(
                    old_agg.input.as_ref(),
                    parent_propagate_nulls,
                    lateral_depth,
                )?;

                // Then we replace any correlated expressions with the corresponding entry
                // in the correlated_map.
                let mut new_agg = old_agg.clone();
                new_agg.input = Arc::new(new_input);
                let new_plan = Self::rewrite_outer_ref_columns(
                    LogicalPlan::Aggregate(new_agg),
                    &self.correlated_map,
                    false,
                )?;

                // TODO: take perform_delim into consideration.
                // Now we add all the correlated columns of current level's dependent join
                // to the grouping operators AND the projection list.
                let (mut group_expr, aggr_expr, input) =
                    if let LogicalPlan::Aggregate(Aggregate {
                        group_expr,
                        aggr_expr,
                        input,
                        ..
                    }) = &new_plan
                    {
                        (group_expr.clone(), aggr_expr.clone(), input.clone())
                    } else {
                        return Err(internal_datafusion_err!(
                            "Expected LogicalPlan::Aggregate"
                        ));
                    };

                for c in self.domains.iter() {
                    let dcol = Self::fetch_dscan_col_from_correlated_col(
                        &self.correlated_map,
                        &c.col,
                    )?;

                    for expr in &mut group_expr {
                        if let Expr::GroupingSet(grouping_set) = expr {
                            if let GroupingSet::GroupingSets(sets) = grouping_set {
                                for set in sets {
                                    set.push(col(dcol.clone()))
                                }
                            }
                        }
                    }
                }

                for c in self.domains.iter() {
                    group_expr.push(col(Self::fetch_dscan_col_from_correlated_col(
                        &self.correlated_map,
                        &c.col,
                    )?));
                }

                let ungroup_join = true;
                if ungroup_join {
                    // We have to perform an INNER or LEFT OUTER JOIN between the result of this
                    // aggregate and the delim scan.
                    // This does not always have to be a LEFt OUTER JOIN, depending on whether
                    // aggr func return NULL or a value.
                    let join_type = if self.any_join || !*parent_propagate_nulls {
                        JoinType::Left
                    } else {
                        JoinType::Inner
                    };

                    // Construct delim join condition.
                    // let mut join_conditions = vec![];
                    let mut join_left_side = vec![];
                    for corr in self.domains.iter() {
                        let delim_col = Self::fetch_dscan_col_from_correlated_col(
                            &self.correlated_map,
                            &corr.col,
                        )?;
                        join_left_side.push(delim_col);
                    }

                    let dscan = self.build_delim_scan()?;
                    let mut join_right_side = vec![];
                    for corr in self.domains.iter() {
                        let delim_col = Self::fetch_dscan_col_from_correlated_col(
                            &self.correlated_map,
                            &corr.col,
                        )?;
                        join_right_side.push(delim_col);
                    }

                    let mut join_conditions = vec![];
                    for (left_col, right_col) in
                        join_left_side.iter().zip(join_right_side.iter())
                    {
                        join_conditions.push((left_col.clone(), right_col.clone()));
                    }

                    // For any COUNT aggregate we replace reference to the column with:
                    // CASE WHTN COUNT (*) IS NULL THEN 0 ELSE COUNT(*) END.
                    for agg_expr in &aggr_expr {
                        match agg_expr {
                            Expr::AggregateFunction(expr::AggregateFunction {
                                func,
                                ..
                            }) => {
                                if func.name() == "count" {
                                    let expr_name = agg_expr.to_string();
                                    let expr_to_replace =
                                        when(col(&expr_name).is_null(), lit(0))
                                            .otherwise(col(&expr_name))?;
                                    // type_coerce to ensure int-typed exprs are correct
                                    let mut expr_rewrite = TypeCoercionRewriter {
                                        schema: new_plan.schema(),
                                    };
                                    let typed_expr =
                                        expr_to_replace.rewrite(&mut expr_rewrite)?.data;
                                    // any above expr which reference this count(1) will be replaced by "case"
                                    self.replacement_map.insert(expr_name, typed_expr);

                                    continue;
                                }
                            }
                            _ => {}
                        }
                    }

                    let new_agg = Aggregate::try_new(input, group_expr, aggr_expr)?;
                    let right = LogicalPlanBuilder::new(LogicalPlan::Aggregate(new_agg))
                        .build()?;
                    natural_join(
                        LogicalPlanBuilder::new(dscan),
                        right,
                        join_type,
                        join_conditions,
                    )?
                    .build()
                } else {
                    // TODO: handle this case
                    unimplemented!()
                }
            }
            LogicalPlan::DependentJoin(_) => {
                return self.decorrelate(plan, *parent_propagate_nulls, lateral_depth);
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
                            &self.correlated_map,
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
                    &self.correlated_map,
                    false,
                );
            }
            LogicalPlan::Limit(old_limit) => {
                let mut sort = None;

                // Check if the direct child of this LIMIT node is an ORDER BY node, if so, keep it
                // separate. This is done for an optimization to avoid having to compute the total
                // order.
                let new_input = if let LogicalPlan::Sort(child) = old_limit.input.as_ref()
                {
                    sort = Some(old_limit.input.as_ref().clone());
                    self.push_down_dependent_join_internal(
                        &child.input,
                        parent_propagate_nulls,
                        lateral_depth,
                    )?
                } else {
                    self.push_down_dependent_join_internal(
                        &old_limit.input,
                        parent_propagate_nulls,
                        lateral_depth,
                    )?
                };

                let new_input_cols = new_input.schema().columns().clone();

                // We push a row_number() OVER (PARTITION BY [correlated columns])
                // TODO: take perform delim into consideration
                let mut partition_by = vec![];
                for corr_col in self.domains.iter() {
                    let delim_col = Self::fetch_dscan_col_from_correlated_col(
                        &self.correlated_map,
                        &corr_col.col,
                    )?;
                    partition_by.push(Expr::Column(delim_col));
                }

                let order_by = if let Some(LogicalPlan::Sort(sort)) = &sort {
                    // Optimization: if there is an ORDER BY node followed by a LIMIT rather than
                    // computing the entire order, we push the ORDER BY expressions into the
                    // row_num computation. This way the order only needs to be computed per
                    // partition.
                    sort.expr.clone()
                } else {
                    vec![]
                };

                // Create row_number() window function.
                let row_number_expr = Expr::WindowFunction(Box::new(WindowFunction {
                    fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
                    params: WindowFunctionParams {
                        args: vec![],
                        partition_by,
                        order_by,
                        window_frame: WindowFrame::new(Some(false)),
                        filter: None,
                        null_treatment: None,
                        distinct: false,
                    },
                }))
                .alias("row_number");
                let mut window_exprs = vec![];
                window_exprs.push(row_number_expr);

                let window = LogicalPlanBuilder::new(new_input)
                    .window(window_exprs)?
                    .build()?;

                // Add filter based on row_number
                // the filter we add is "row_number > offset AND row_number <= offset + limit"
                let mut filter_conditions = vec![];

                if let FetchType::Literal(Some(fetch)) = old_limit.get_fetch_type()? {
                    let upper_bound =
                        if let SkipType::Literal(skip) = old_limit.get_skip_type()? {
                            // Both offset and limit specified - upper bound is offset + limit.
                            fetch + skip
                        } else {
                            // No offset - upper bound is not only the limit.
                            fetch
                        };

                    filter_conditions
                        .push(col("row_number").lt_eq(lit(upper_bound as u64)));
                }

                // We only need to add "row_number > offset" if offset is bigger than 0.
                if let SkipType::Literal(skip) = old_limit.get_skip_type()? {
                    if skip > 0 {
                        filter_conditions.push(col("row_number").gt(lit(skip as u64)));
                    }
                }

                let mut result_plan = window;
                if !filter_conditions.is_empty() {
                    let filter_expr = filter_conditions
                        .into_iter()
                        .reduce(|acc, expr| acc.and(expr))
                        .unwrap();

                    result_plan = LogicalPlanBuilder::new(result_plan)
                        .filter(filter_expr)?
                        .build()?;
                }

                // Project away the row_number column, keeping only original columns
                let final_exprs = new_input_cols
                    .iter()
                    .map(|c| col(c.clone()))
                    .collect::<Vec<_>>();
                result_plan = LogicalPlanBuilder::new(result_plan)
                    .project(final_exprs)?
                    .build()?;

                return Ok(result_plan);
            }
            LogicalPlan::Distinct(old_distinct) => {
                // Push down into child.
                let new_input = self.push_down_dependent_join(
                    old_distinct.input().as_ref(),
                    *parent_propagate_nulls,
                    lateral_depth,
                )?;
                // Add all correlated columns to the DISTINCT targets.
                let mut distinct_exprs = old_distinct
                    .input()
                    .schema()
                    .columns()
                    .into_iter()
                    .map(|c| col(c.clone()))
                    .collect::<Vec<_>>();

                // Add correlated columns as additional columns for grouping
                for domain_col in self.domains.iter() {
                    let delim_col = Self::fetch_dscan_col_from_correlated_col(
                        &self.correlated_map,
                        &domain_col.col,
                    )?;
                    distinct_exprs.push(col(delim_col));
                }

                // Create new distinct plan with additional correlated columns
                let distinct = LogicalPlanBuilder::new(new_input)
                    .distinct_on(distinct_exprs, vec![], None)?
                    .build()?;

                return Ok(distinct);
            }
            LogicalPlan::Sort(old_sort) => {
                let new_input = self.push_down_dependent_join(
                    old_sort.input.as_ref(),
                    *parent_propagate_nulls,
                    lateral_depth,
                )?;
                let mut sort = old_sort.clone();
                sort.input = Arc::new(new_input);
                Ok(LogicalPlan::Sort(sort))
            }
            LogicalPlan::TableScan(old_table_scan) => {
                let delim_scan = self.build_delim_scan()?;

                // Add correlated columns to the table scan output
                let mut projection_exprs: Vec<Expr> = old_table_scan
                    .projected_schema
                    .columns()
                    .into_iter()
                    .map(|c| Expr::Column(c))
                    .collect();

                // Add delim columns to projection
                for domain_col in self.domains.iter() {
                    let delim_col = Self::fetch_dscan_col_from_correlated_col(
                        &self.correlated_map,
                        &domain_col.col,
                    )?;
                    projection_exprs.push(Expr::Column(delim_col));
                }

                // Cross join with delim scan and project
                let cross_join = LogicalPlanBuilder::new(LogicalPlan::TableScan(
                    old_table_scan.clone(),
                ))
                .join(
                    delim_scan,
                    JoinType::Inner,
                    (Vec::<Column>::new(), Vec::<Column>::new()),
                    None,
                )?
                .project(projection_exprs)?
                .build()?;

                // Rewrite correlated expressions
                Self::rewrite_outer_ref_columns(cross_join, &self.correlated_map, false)
            }
            LogicalPlan::Window(old_window) => {
                // Push into children.
                let new_input = self.push_down_dependent_join_internal(
                    &old_window.input,
                    parent_propagate_nulls,
                    lateral_depth,
                )?;

                // Create new window expressions with updated partition clauses
                let mut new_window_exprs = old_window.window_expr.clone();

                // Add correlated columns to PARTITION BY clauses in each window expression
                for window_expr in &mut new_window_exprs {
                    // Handle both direct window functions and aliased window functions
                    let window_func = match window_expr {
                        Expr::WindowFunction(ref mut window_func) => window_func,
                        Expr::Alias(alias) => {
                            if let Expr::WindowFunction(ref mut window_func) =
                                alias.expr.as_mut()
                            {
                                window_func
                            } else {
                                continue; // Skip if alias doesn't contain a window function
                            }
                        }
                        _ => continue, // Skip non-window expressions
                    };

                    // Add correlated columns to the partition by clause
                    for domain_col in self.domains.iter() {
                        let delim_col = Self::fetch_dscan_col_from_correlated_col(
                            &self.correlated_map,
                            &domain_col.col,
                        )?;
                        window_func
                            .params
                            .partition_by
                            .push(Expr::Column(delim_col));
                    }
                }

                // Create new window plan with updated expressions and input
                let mut window = old_window.clone();
                window.input = Arc::new(new_input);
                window.window_expr = new_window_exprs;

                // We replace any correlated expressions with the corresponding entry in the
                // correlated_map.
                Self::rewrite_outer_ref_columns(
                    LogicalPlan::Window(window),
                    &self.correlated_map,
                    false,
                )
            }
            plan_ => Err(not_impl_datafusion_err!(
                "implement pushdown dependent join for node {plan_}"
            ))?,
        }
    }

    fn push_down_dependent_join(
        &mut self,
        node: &LogicalPlan,
        mut parent_propagate_nulls: bool,
        lateral_depth: usize,
    ) -> Result<LogicalPlan> {
        let mut new_plan = self.push_down_dependent_join_internal(
            node,
            &mut parent_propagate_nulls,
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

    fn join_without_correlation(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: Join,
    ) -> Result<LogicalPlan> {
        let new_join = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            join.filter,
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?);

        Self::rewrite_outer_ref_columns(new_join, &self.correlated_map, false)
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

        for col_pair in &self.correlated_map {
            join_conditions.push(binary_expr(
                Expr::Column(col_pair.0.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(col_pair.1.clone()),
            ));
        }

        let new_join = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            conjunction(join_conditions).or(Some(lit(true))),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?);

        Self::rewrite_outer_ref_columns(new_join, &self.correlated_map, false)
    }

    fn join_with_delim_scan(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join: Join,
        left_dscan_cols: &Vec<Column>,
        right_dscan_cols: &Vec<Column>,
    ) -> Result<LogicalPlan> {
        let mut join_conditions = vec![];
        if let Some(filter) = join.filter {
            join_conditions.push(filter);
        }

        // Ensure left_dscan_cols and right_dscan_cols have the same length
        if left_dscan_cols.len() != right_dscan_cols.len() {
            return Err(internal_datafusion_err!(
                "Mismatched dscan columns length: left_dscan_cols has {} elements, right_dscan_cols has {} elements",
                left_dscan_cols.len(),
                right_dscan_cols.len()
            ));
        }

        for (left_delim_col, right_delim_col) in
            left_dscan_cols.iter().zip(right_dscan_cols.iter())
        {
            join_conditions.push(binary_expr(
                Expr::Column(left_delim_col.clone()),
                Operator::IsNotDistinctFrom,
                Expr::Column(right_delim_col.clone()),
            ));
        }

        let new_join = LogicalPlan::Join(Join::try_new(
            Arc::new(left),
            Arc::new(right),
            join.on,
            conjunction(join_conditions).or(Some(lit(true))),
            join.join_type,
            join.join_constraint,
            join.null_equality,
        )?);

        Self::rewrite_outer_ref_columns(new_join, &self.correlated_map, false)
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
                if let Expr::OuterReferenceColumn(field, col) = e {
                    if correlated_columns
                        .iter()
                        .any(|c| (&c.col == col) && (&c.field == field))
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
macro_rules! debug_println {
    ($($arg:tt)*) => {
        if std::env::var("PLAN_DEBUG").is_ok() {
            println!($($arg)*);
        }
    };
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
        let rewrite_result =
            match transformer.rewrite_subqueries_into_dependent_joins(plan.clone()) {
                Err(e) => Transformed::no(plan),
                Ok(transformed) => transformed,
            };

        // Only print debug info if PLAN_DEBUG env var is exactly "1"

        if rewrite_result.transformed {
            debug_println!(
                "dependent join plan\n{}",
                rewrite_result.data.display_indent()
            );
            let mut decorrelator = DependentJoinDecorrelator::new_root();
            let ret = decorrelator.decorrelate(&rewrite_result.data, true, 0)?;

            debug_println!("decorrelated plan\n{}", ret.display_indent_schema(),);
            let deliminator = Deliminator::new();
            let ret = deliminator.rewrite(ret, config)?;
            if ret.transformed{
                debug_println!("deliminated plan\n{}", ret.data.display_indent_schema());
            }


            return Ok(ret);
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
    use crate::test::{test_table_scan_with_name, test_table_with_columns};
    use crate::Optimizer;
    use crate::{
        assert_optimized_plan_eq_display_indent_snapshot, OptimizerConfig,
        OptimizerContext, OptimizerRule,
    };
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::{Column, Result};
    use datafusion_expr::expr::{WindowFunction, WindowFunctionParams};
    use datafusion_expr::{
        binary_expr, not_in_subquery, JoinType, Operator, WindowFrame,
        WindowFunctionDefinition,
    };
    use datafusion_expr::{
        exists, expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlan, LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::{count::count, sum::sum};
    use datafusion_functions_window::row_number::row_number_udwf;
    use std::sync::Arc;
    fn print_optimize_tree(plan: &LogicalPlan) {
        let rule: Arc<dyn OptimizerRule + Send + Sync> =
            Arc::new(DecorrelateDependentJoin::new());
        let optimizer = Optimizer::with_rules(vec![rule]);
        let _optimized_plan = optimizer
            .optimize(plan.clone(), &OptimizerContext::new(), |_, _| {})
            .expect("failed to optimize plan");
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

        let _plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(exists(sq1))?
            .build()?;
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
          Filter: __exists_sq_1 AND __exists_sq_2 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, __exists_sq_2:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1, mark AS __exists_sq_2 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, __exists_sq_2:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM outer_table_dscan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, mark:Boolean]
                Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __exists_sq_1 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean]
                  LeftMark Join(ComparisonJoin):  Filter: outer_table.b IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                    TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                    Filter: inner_table_lv1.b = outer_table_dscan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
                      Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_b:UInt32;N]
                        TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                        Projection: outer_table.b AS outer_table_dscan_1.outer_table_b [outer_table_b:UInt32;N]
                          DelimGet: outer_table.b [b:UInt32;N]
                Filter: inner_table_lv1.c = outer_table_dscan_2.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                  Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                    Projection: outer_table.c AS outer_table_dscan_2.outer_table_c [outer_table_c:UInt32;N]
                      DelimGet: outer_table.c [c:UInt32;N]
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

        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: __scalar_sq_2 = outer_table.a [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, __scalar_sq_2:UInt32;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, outer_table_dscan_4.outer_table_c, count(inner_table_lv1.a), outer_table_dscan_1.outer_table_c, outer_table_dscan_4.outer_table_c AS __scalar_sq_2 [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, __scalar_sq_2:UInt32;N]
              LeftSingle Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM outer_table_dscan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Left Join(DelimJoin):  Filter: outer_table_dscan_1.outer_table_c IS NOT DISTINCT FROM outer_table_dscan_4.outer_table_c [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N]
                  Projection: outer_table.c AS outer_table_dscan_4.outer_table_c [outer_table_c:UInt32;N]
                    DelimGet: outer_table.c [c:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_1.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_1.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                      Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, outer_table_dscan_1.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                        Filter: inner_table_lv1.c = outer_table_dscan_1.outer_table_c AND __scalar_sq_1 = Int32(1) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, __scalar_sq_1:UInt32;N, outer_table_c:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, __scalar_sq_1:UInt32;N, outer_table_c:UInt32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, outer_table_dscan_3.outer_table_a, count(inner_table_lv2.a), outer_table_dscan_2.outer_table_a, outer_table_dscan_3.outer_table_a AS __scalar_sq_1 [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, __scalar_sq_1:UInt32;N]
                              LeftSingle Join(ComparisonJoin):  Filter: outer_table.a IS NOT DISTINCT FROM outer_table_dscan_3.outer_table_a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N]
                                TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                Left Join(DelimJoin):  Filter: outer_table_dscan_2.outer_table_a IS NOT DISTINCT FROM outer_table_dscan_3.outer_table_a [outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N]
                                  Projection: outer_table.a AS outer_table_dscan_3.outer_table_a [outer_table_a:UInt32;N]
                                    DelimGet: outer_table.a [a:UInt32;N]
                                  Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, outer_table_dscan_2.outer_table_a [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N]
                                    Aggregate: groupBy=[[outer_table_dscan_2.outer_table_a]], aggr=[[count(inner_table_lv2.a)]] [outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64]
                                      Filter: inner_table_lv2.a = outer_table_dscan_2.outer_table_a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                          Projection: outer_table.a AS outer_table_dscan_2.outer_table_a [outer_table_a:UInt32;N]
                                            DelimGet: outer_table.a [a:UInt32;N]
                            Projection: outer_table.c AS outer_table_dscan_1.outer_table_c [outer_table_c:UInt32;N]
                              DelimGet: outer_table.c [c:UInt32;N]
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
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2 = outer_table.a [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, __scalar_sq_2:UInt32;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, outer_table_dscan_6.outer_table_c, count(inner_table_lv1.a), outer_table_dscan_1.outer_table_c, outer_table_dscan_6.outer_table_c AS __scalar_sq_2 [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N, __scalar_sq_2:UInt32;N]
              LeftSingle Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM outer_table_dscan_6.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Left Join(DelimJoin):  Filter: outer_table_dscan_1.outer_table_c IS NOT DISTINCT FROM outer_table_dscan_6.outer_table_c [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_c:UInt32;N]
                  Projection: outer_table.c AS outer_table_dscan_6.outer_table_c [outer_table_c:UInt32;N]
                    DelimGet: outer_table.c [c:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_1.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_1.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                      Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, outer_table_dscan_1.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:UInt32;N]
                        Filter: inner_table_lv1.c = outer_table_dscan_1.outer_table_c AND __scalar_sq_1 = Int32(1) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1:UInt32;N, outer_table_c:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1:UInt32;N, outer_table_c:UInt32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, inner_table_lv1_dscan_4.inner_table_lv1_b, outer_table_dscan_5.outer_table_a, count(inner_table_lv2.a), outer_table_dscan_3.outer_table_a, inner_table_lv1_dscan_2.inner_table_lv1_b, inner_table_lv1_dscan_4.inner_table_lv1_b AS __scalar_sq_1 [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1:UInt32;N]
                              LeftSingle Join(ComparisonJoin):  Filter: inner_table_lv1.b IS NOT DISTINCT FROM inner_table_lv1_dscan_4.inner_table_lv1_b AND outer_table.a IS NOT DISTINCT FROM outer_table_dscan_5.outer_table_a [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                Left Join(DelimJoin):  Filter: inner_table_lv1_dscan_2.inner_table_lv1_b IS NOT DISTINCT FROM inner_table_lv1_dscan_4.inner_table_lv1_b AND outer_table_dscan_3.outer_table_a IS NOT DISTINCT FROM outer_table_dscan_5.outer_table_a [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                  Cross Join(ComparisonJoin):  [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                    Projection: inner_table_lv1.b AS inner_table_lv1_dscan_4.inner_table_lv1_b [inner_table_lv1_b:UInt32;N]
                                      DelimGet: inner_table_lv1.b [b:UInt32;N]
                                    Projection: outer_table.a AS outer_table_dscan_5.outer_table_a [outer_table_a:UInt32;N]
                                      DelimGet: outer_table.a [a:UInt32;N]
                                  Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, outer_table_dscan_3.outer_table_a, inner_table_lv1_dscan_2.inner_table_lv1_b [count(inner_table_lv2.a):Int64, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                    Aggregate: groupBy=[[inner_table_lv1_dscan_2.inner_table_lv1_b, outer_table_dscan_3.outer_table_a]], aggr=[[count(inner_table_lv2.a)]] [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, count(inner_table_lv2.a):Int64]
                                      Filter: inner_table_lv2.a = outer_table_dscan_3.outer_table_a AND inner_table_lv2.b = inner_table_lv1_dscan_2.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                          TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
                                          Cross Join(ComparisonJoin):  [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N]
                                            Projection: inner_table_lv1.b AS inner_table_lv1_dscan_2.inner_table_lv1_b [inner_table_lv1_b:UInt32;N]
                                              DelimGet: inner_table_lv1.b [b:UInt32;N]
                                            Projection: outer_table.a AS outer_table_dscan_3.outer_table_a [outer_table_a:UInt32;N]
                                              DelimGet: outer_table.a [a:UInt32;N]
                            Projection: outer_table.c AS outer_table_dscan_1.outer_table_c [outer_table_c:UInt32;N]
                              DelimGet: outer_table.c [c:UInt32;N]
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
          Filter: outer_table.a > Int32(1) AND __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = outer_table_dscan_2.outer_table_a AND outer_table.a IS NOT DISTINCT FROM outer_table_dscan_2.outer_table_a AND outer_table.b IS NOT DISTINCT FROM outer_table_dscan_2.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Left Join(DelimJoin):  Filter: outer_table_dscan_1.outer_table_a IS NOT DISTINCT FROM outer_table_dscan_2.outer_table_a AND outer_table_dscan_1.outer_table_b IS NOT DISTINCT FROM outer_table_dscan_2.outer_table_b [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64;N, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                  Projection: outer_table.a AS outer_table_dscan_2.outer_table_a, outer_table.b AS outer_table_dscan_2.outer_table_b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    DelimGet: outer_table.a, outer_table.b [a:UInt32;N, b:UInt32;N]
                  Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, outer_table_dscan_1.outer_table_b, outer_table_dscan_1.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                    Aggregate: groupBy=[[outer_table_dscan_1.outer_table_a, outer_table_dscan_1.outer_table_b]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64]
                      Filter: inner_table_lv1.a = outer_table_dscan_1.outer_table_a AND outer_table_dscan_1.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_table_dscan_1.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                          TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                          Projection: outer_table.a AS outer_table_dscan_1.outer_table_a, outer_table.b AS outer_table_dscan_1.outer_table_b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                            DelimGet: outer_table.a, outer_table.b [a:UInt32;N, b:UInt32;N]
        ");
        Ok(())
    }

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
                    col("inner_table_lv1.a").and(
                        col("inner_table_lv1.b")
                            .eq(out_ref_col(ArrowDataType::Int32, "outer_table.c")),
                    ),
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
          Filter: outer_table.a > Int32(1) AND __exists_sq_1 AND __in_sq_2 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, __in_sq_2:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, __exists_sq_1, inner_table_lv1.mark AS __in_sq_2 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, __in_sq_2:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.b = inner_table_lv1.a [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean, mark:Boolean]
                Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __exists_sq_1 [a:UInt32, b:UInt32, c:UInt32, __exists_sq_1:Boolean]
                  LeftMark Join(ComparisonJoin):  Filter: outer_table.c IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_c [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                    TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                    Filter: inner_table_lv1.a AND inner_table_lv1.b = outer_table_dscan_1.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_c:Int32;N]
                      Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_c:Int32;N]
                        TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                        Projection: outer_table.c AS outer_table_dscan_1.outer_table_c [outer_table_c:Int32;N]
                          DelimGet: outer_table.c [c:Int32;N]
                Projection: inner_table_lv1.a [a:UInt32]
                  Filter: inner_table_lv1.c = Int32(2) [a:UInt32, b:UInt32, c:UInt32]
                    TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
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
          Filter: outer_table.a > Int32(1) AND __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = inner_table_lv1.b AND outer_table.a IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_a AND outer_table.b IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table_lv1.b, outer_table_dscan_1.outer_table_a, outer_table_dscan_1.outer_table_b [b:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Filter: inner_table_lv1.a = outer_table_dscan_1.outer_table_a AND outer_table_dscan_1.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_table_dscan_1.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      Projection: outer_table.a AS outer_table_dscan_1.outer_table_a, outer_table.b AS outer_table_dscan_1.outer_table_b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        DelimGet: outer_table.a, outer_table.b [a:UInt32;N, b:UInt32;N]
        ");

        Ok(())
    }

    // This query is inside the paper
    #[test]
    fn decorrelate_two_different_outer_tables() -> Result<()> {
        let t1 = test_table_scan_with_name("T1")?;
        let t2 = test_table_scan_with_name("T2")?;

        let t3 = test_table_scan_with_name("T3")?;
        let scalar_sq_level2 = Arc::new(
            LogicalPlanBuilder::from(t3)
                .filter(
                    col("T3.b")
                        .eq(out_ref_col(ArrowDataType::UInt32, "T2.b"))
                        .and(col("T3.a").eq(out_ref_col(ArrowDataType::UInt32, "T1.a"))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("T3.a"))])?
                .build()?,
        );
        let scalar_sq_level1 = Arc::new(
            LogicalPlanBuilder::from(t2.clone())
                .filter(
                    col("T2.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "T1.a"))
                        .and(scalar_subquery(scalar_sq_level2).gt(lit(300000))),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(col("T2.a"))])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(t1.clone())
            .filter(
                col("T1.c")
                    .eq(lit(123))
                    .and(scalar_subquery(scalar_sq_level1).gt(lit(5))),
            )?
            .build()?;

        // Projection: t1.a, t1.b, t1.c [a:UInt32, b:UInt32, c:UInt32]
        //   Filter: t1.c = Int32(123) AND __scalar_sq_2.output > Int32(5) [a:UInt32, b:UInt32, c:UInt32, output:Int64]
        //     DependentJoin on [t1.a lvl 1, t1.a lvl 2] with expr (<subquery>) depth 1 [a:UInt32, b:UInt32, c:UInt32, output:Int64]
        //       TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
        //       Aggregate: groupBy=[[]], aggr=[[count(t2.a)]] [count(t2.a):Int64]
        //         Projection: t2.a, t2.b, t2.c [a:UInt32, b:UInt32, c:UInt32]
        //           Filter: t2.a = outer_ref(t1.a) AND __scalar_sq_1.output > Int32(300000) [a:UInt32, b:UInt32, c:UInt32, output:UInt64]
        //             DependentJoin on [t2.b lvl 2] with expr (<subquery>) depth 2 [a:UInt32, b:UInt32, c:UInt32, output:UInt64]
        //               TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
        //               Aggregate: groupBy=[[]], aggr=[[sum(t3.a)]] [sum(t3.a):UInt64;N]
        //                 Filter: t3.b = outer_ref(t2.b) AND t3.a = outer_ref(t1.a) [a:UInt32, b:UInt32, c:UInt32]
        //                   TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]

        assert_decorrelate!(plan, @r"
        Projection: t1.a, t1.b, t1.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: t1.c = Int32(123) AND __scalar_sq_2 > Int32(5) [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, count(t2.a):Int64;N, t1_a:UInt32;N, __scalar_sq_2:UInt32;N]
            Projection: t1.a, t1.b, t1.c, t1_dscan_6.t1_a, count(t2.a), t1_dscan_5.t1_a, t1_dscan_6.t1_a AS __scalar_sq_2 [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, count(t2.a):Int64;N, t1_a:UInt32;N, __scalar_sq_2:UInt32;N]
              LeftSingle Join(ComparisonJoin):  Filter: t1.a IS NOT DISTINCT FROM t1_dscan_6.t1_a [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, count(t2.a):Int64;N, t1_a:UInt32;N]
                TableScan: t1 [a:UInt32, b:UInt32, c:UInt32]
                Left Join(DelimJoin):  Filter: t1_dscan_5.t1_a IS NOT DISTINCT FROM t1_dscan_6.t1_a [t1_a:UInt32;N, count(t2.a):Int64;N, t1_a:UInt32;N]
                  Projection: t1.a AS t1_dscan_6.t1_a [t1_a:UInt32;N]
                    DelimGet: t1.a [a:UInt32;N]
                  Projection: CASE WHEN count(t2.a) IS NULL THEN Int32(0) ELSE count(t2.a) END, t1_dscan_5.t1_a [count(t2.a):Int64, t1_a:UInt32;N]
                    Aggregate: groupBy=[[t1_dscan_5.t1_a]], aggr=[[count(t2.a)]] [t1_a:UInt32;N, count(t2.a):Int64]
                      Projection: t2.a, t2.b, t2.c, t1_dscan_5.t1_a [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N]
                        Filter: t2.a = t1_dscan_5.t1_a AND __scalar_sq_1 > Int32(300000) [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, __scalar_sq_1:UInt32;N]
                          Projection: t2.a, t2.b, t2.c, t1_dscan_1.t1_a, t2_dscan_4.t2_b, t1_dscan_5.t1_a, sum(t3.a), t1_dscan_3.t1_a, t2_dscan_2.t2_b, t2_dscan_4.t2_b AS __scalar_sq_1 [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N, __scalar_sq_1:UInt32;N]
                            LeftSingle Join(ComparisonJoin):  Filter: t2.b IS NOT DISTINCT FROM t2_dscan_4.t2_b AND t1.a IS NOT DISTINCT FROM t1_dscan_5.t1_a AND t1.a IS NOT DISTINCT FROM t1_dscan_5.t1_a [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N, t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N]
                              Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, t1_a:UInt32;N]
                                TableScan: t2 [a:UInt32, b:UInt32, c:UInt32]
                                Projection: t1.a AS t1_dscan_1.t1_a [t1_a:UInt32;N]
                                  DelimGet: t1.a [a:UInt32;N]
                              Left Join(DelimJoin):  Filter: t2_dscan_2.t2_b IS NOT DISTINCT FROM t2_dscan_4.t2_b AND t1_dscan_3.t1_a IS NOT DISTINCT FROM t1_dscan_5.t1_a AND t1_dscan_3.t1_a IS NOT DISTINCT FROM t1_dscan_5.t1_a [t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N]
                                Cross Join(ComparisonJoin):  [t2_b:UInt32;N, t1_a:UInt32;N]
                                  Projection: t2.b AS t2_dscan_4.t2_b [t2_b:UInt32;N]
                                    DelimGet: t2.b [b:UInt32;N]
                                  Projection: t1.a AS t1_dscan_5.t1_a [t1_a:UInt32;N]
                                    DelimGet: t1.a [a:UInt32;N]
                                Projection: sum(t3.a), t1_dscan_3.t1_a, t2_dscan_2.t2_b [sum(t3.a):UInt64;N, t1_a:UInt32;N, t2_b:UInt32;N]
                                  Aggregate: groupBy=[[t2_dscan_2.t2_b, t1_dscan_3.t1_a, t1_dscan_3.t1_a]], aggr=[[sum(t3.a)]] [t2_b:UInt32;N, t1_a:UInt32;N, sum(t3.a):UInt64;N]
                                    Filter: t3.b = t2_dscan_2.t2_b AND t3.a = t1_dscan_3.t1_a [a:UInt32, b:UInt32, c:UInt32, t2_b:UInt32;N, t1_a:UInt32;N]
                                      Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, t2_b:UInt32;N, t1_a:UInt32;N]
                                        TableScan: t3 [a:UInt32, b:UInt32, c:UInt32]
                                        Cross Join(ComparisonJoin):  [t2_b:UInt32;N, t1_a:UInt32;N]
                                          Projection: t2.b AS t2_dscan_2.t2_b [t2_b:UInt32;N]
                                            DelimGet: t2.b [b:UInt32;N]
                                          Projection: t1.a AS t1_dscan_3.t1_a [t1_a:UInt32;N]
                                            DelimGet: t1.a [a:UInt32;N]
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
          Filter: outer_table.a > Int32(1) AND __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = inner_table_lv1.b AND outer_table.a IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_a AND outer_table.b IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table_lv1.b, outer_table_dscan_1.outer_table_a, outer_table_dscan_1.outer_table_b [b:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                  Inner Join(ComparisonJoin):  Filter: inner_table_lv1.a = outer_table_dscan_1.outer_table_a AND outer_table_dscan_1.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND outer_table_dscan_1.outer_table_b = inner_table_lv1.b AND inner_table_lv1.a = inner_table_lv2.a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N, a:UInt32, b:UInt32, c:UInt32]
                    Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                      Projection: outer_table.a AS outer_table_dscan_1.outer_table_a, outer_table.b AS outer_table_dscan_1.outer_table_b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                        DelimGet: outer_table.a, outer_table.b [a:UInt32;N, b:UInt32;N]
                    TableScan: inner_table_lv2 [a:UInt32, b:UInt32, c:UInt32]
        ");

        Ok(())
    }

    #[test]
    fn decorrelate_in_subquery_with_sort_limit() -> Result<()> {
        let outer_table = test_table_scan_with_name("customers")?;
        let inner_table = test_table_scan_with_name("orders")?;

        let in_subquery_plan = Arc::new(
            LogicalPlanBuilder::from(inner_table)
                .filter(
                    col("orders.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "customers.a"))
                        .and(col("orders.b").eq(lit(1))), // status = 'completed' simplified as b = 1
                )?
                .sort(vec![col("orders.c").sort(false, true)])? // ORDER BY order_amount DESC
                .limit(0, Some(3))? // LIMIT 3
                .project(vec![col("orders.c")])?
                .build()?,
        );

        // Outer query
        let plan = LogicalPlanBuilder::from(outer_table.clone())
            .filter(
                col("customers.a")
                    .gt(lit(100))
                    .and(in_subquery(col("customers.a"), in_subquery_plan)),
            )?
            .build()?;

        // Projection: customers.a, customers.b, customers.c
        //       Filter: customers.a > Int32(100) AND __in_sq_1.output
        //         DependentJoin on [customers.a lvl 1] with expr customers.a IN (<subquery>) depth 1
        //           TableScan: customers
        //           Projection: orders.c
        //             Limit: skip=0, fetch=3
        //               Sort: orders.c DESC NULLS FIRST
        //                 Filter: orders.a = outer_ref(customers.a) AND orders.b = Int32(1)
        //                   TableScan: orders

        assert_decorrelate!(plan, @r"
        Projection: customers.a, customers.b, customers.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: customers.a > Int32(100) AND __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
            Projection: customers.a, customers.b, customers.c, mark AS __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: customers.a = orders.c AND customers.a IS NOT DISTINCT FROM customers_dscan_1.customers_a [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: customers [a:UInt32, b:UInt32, c:UInt32]
                Projection: orders.c, customers_dscan_1.customers_a [c:UInt32, customers_a:UInt32;N]
                  Projection: orders.a, orders.b, orders.c, customers_dscan_1.customers_a [a:UInt32, b:UInt32, c:UInt32, customers_a:UInt32;N]
                    Filter: row_number <= UInt64(3) [a:UInt32, b:UInt32, c:UInt32, customers_a:UInt32;N, row_number:UInt64]
                      WindowAggr: windowExpr=[[row_number() PARTITION BY [customers_dscan_1.customers_a] ORDER BY [orders.c DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_number]] [a:UInt32, b:UInt32, c:UInt32, customers_a:UInt32;N, row_number:UInt64]
                        Filter: orders.a = customers_dscan_1.customers_a AND orders.b = Int32(1) [a:UInt32, b:UInt32, c:UInt32, customers_a:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, customers_a:UInt32;N]
                            TableScan: orders [a:UInt32, b:UInt32, c:UInt32]
                            Projection: customers.a AS customers_dscan_1.customers_a [customers_a:UInt32;N]
                              DelimGet: customers.a [a:UInt32;N]
        ");

        Ok(())
    }

    #[test]
    fn decorrelate_subquery_with_window_function() -> Result<()> {
        let outer_table = test_table_scan_with_name("outer_table")?;
        let inner_table = test_table_scan_with_name("inner_table")?;

        // Create a subquery with window function
        let window_expr = Expr::WindowFunction(Box::new(WindowFunction {
            fun: WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            params: WindowFunctionParams {
                args: vec![],
                partition_by: vec![col("inner_table.b")],
                order_by: vec![col("inner_table.c").sort(false, true)],
                window_frame: WindowFrame::new(Some(false)),
                filter: None,
                null_treatment: None,
                distinct: false,
            },
        }))
        .alias("row_num");

        let subquery = Arc::new(
            LogicalPlanBuilder::from(inner_table)
                .filter(
                    col("inner_table.a")
                        .eq(out_ref_col(ArrowDataType::UInt32, "outer_table.a")),
                )?
                .window(vec![window_expr])?
                .filter(col("row_num").eq(lit(1)))?
                .project(vec![col("inner_table.b")])?
                .build()?,
        );

        let plan = LogicalPlanBuilder::from(outer_table)
            .filter(
                col("outer_table.a")
                    .gt(lit(1))
                    .and(in_subquery(col("outer_table.c"), subquery)),
            )?
            .build()?;

        // Projection: outer_table.a, outer_table.b, outer_table.c
        //   Filter: outer_table.a > Int32(1) AND __in_sq_1.output
        //     DependentJoin on [outer_table.a lvl 1] with expr outer_table.c IN (<subquery>) depth 1
        //       TableScan: outer_table
        //       Projection: inner_table.b
        //         Filter: row_num = Int32(1)
        //           WindowAggr: windowExpr=[[row_number() PARTITION BY [inner_table.b] ORDER BY [inner_table.c DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_num]]
        //             Filter: inner_table.a = outer_ref(outer_table.a)
        //               TableScan: inner_table

        assert_decorrelate!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, mark AS __in_sq_1 [a:UInt32, b:UInt32, c:UInt32, __in_sq_1:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = inner_table.b AND outer_table.a IS NOT DISTINCT FROM outer_table_dscan_1.outer_table_a [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: inner_table.b, outer_table_dscan_1.outer_table_a [b:UInt32, outer_table_a:UInt32;N]
                  Filter: row_num = Int32(1) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, row_num:UInt64]
                    WindowAggr: windowExpr=[[row_number() PARTITION BY [inner_table.b, outer_table_dscan_1.outer_table_a] ORDER BY [inner_table.c DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_num]] [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, row_num:UInt64]
                      Filter: inner_table.a = outer_table_dscan_1.outer_table_a [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                        Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N]
                          TableScan: inner_table [a:UInt32, b:UInt32, c:UInt32]
                          Projection: outer_table.a AS outer_table_dscan_1.outer_table_a [outer_table_a:UInt32;N]
                            DelimGet: outer_table.a [a:UInt32;N]
        ");

        Ok(())
    }

    // TODO: support uncorrelated subquery
    #[test]
    fn subquery_slt_test1() -> Result<()> {
        // Create test tables with custom column names
        let t1 = test_table_with_columns(
            "t1",
            &[
                ("t1_id", ArrowDataType::UInt32),
                ("t1_name", ArrowDataType::Utf8),
                ("t1_int", ArrowDataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[
                ("t2_id", ArrowDataType::UInt32),
                ("t2_value", ArrowDataType::Utf8),
            ],
        )?;

        // Create the subquery plan (SELECT t2_id FROM t2)
        let subquery = Arc::new(
            LogicalPlanBuilder::from(t2)
                .project(vec![col("t2_id")])?
                .build()?,
        );

        // Create the main query plan
        // SELECT t1_id, t1_name, t1_int FROM t1 WHERE t1_id IN (SELECT t2_id FROM t2)
        let _plan = LogicalPlanBuilder::from(t1)
            .filter(in_subquery(col("t1_id"), subquery))?
            .project(vec![col("t1_id"), col("t1_name"), col("t1_int")])?
            .build()?;

        // Test the decorrelation transformation
        // assert_decorrelate!(plan, @r"");

        Ok(())
    }

    #[test]
    fn subquery_slt_test2() -> Result<()> {
        // Create test tables with custom column names
        let t1 = test_table_with_columns(
            "t1",
            &[
                ("t1_id", ArrowDataType::UInt32),
                ("t1_name", ArrowDataType::Utf8),
                ("t1_int", ArrowDataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[
                ("t2_id", ArrowDataType::UInt32),
                ("t2_value", ArrowDataType::Utf8),
            ],
        )?;

        // Create the subquery plan
        // SELECT t2_id + 1 FROM t2 WHERE t1.t1_int > 0
        let subquery = Arc::new(
            LogicalPlanBuilder::from(t2)
                .filter(out_ref_col(ArrowDataType::Int32, "t1.t1_int").gt(lit(0)))?
                .project(vec![binary_expr(col("t2_id"), Operator::Plus, lit(1))])?
                .build()?,
        );

        // Create the main query plan
        // SELECT t1_id, t1_name, t1_int FROM t1 WHERE t1_id + 12 NOT IN (SELECT t2_id + 1 FROM t2 WHERE t1.t1_int > 0)
        let plan = LogicalPlanBuilder::from(t1)
            .filter(not_in_subquery(
                binary_expr(col("t1_id"), Operator::Plus, lit(12)),
                subquery,
            ))?
            .project(vec![col("t1_id"), col("t1_name"), col("t1_int")])?
            .build()?;

        // select t1.t1_id,
        //        t1.t1_name,
        //        t1.t1_int
        // from t1
        // where t1.t1_id + 12 not in (select t2.t2_id + 1 from t2 where t1.t1_int > 0);

        // Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //   Filter: t1.t1_id + Int32(12) NOT IN (<subquery>) [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //     Subquery: [t2.t2_id + Int32(1):Int64]
        //       Projection: t2.t2_id + Int32(1) [t2.t2_id + Int32(1):Int64]
        //         Filter: outer_ref(t1.t1_int) > Int32(0) [t2_id:UInt32, t2_value:Utf8]
        //           TableScan: t2 [t2_id:UInt32, t2_value:Utf8]
        //     TableScan: t1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]

        // Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //   Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //     Filter: __in_sq_1.output [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, output:Boolean]
        //       DependentJoin on [t1.t1_int lvl 1] with expr t1.t1_id + Int32(12) NOT IN (<subquery>) depth 1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, output:Boolean]
        //         TableScan: t1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //         Projection: t2.t2_id + Int32(1) [t2.t2_id + Int32(1):Int64]
        //           Filter: outer_ref(t1.t1_int) > Int32(0) [t2_id:UInt32, t2_value:Utf8]
        //             TableScan: t2 [t2_id:UInt32, t2_value:Utf8]

        assert_decorrelate!(plan, @r"
        Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
          Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
            Filter: NOT __in_sq_1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, __in_sq_1:Boolean]
              Projection: t1.t1_id, t1.t1_name, t1.t1_int, t1_dscan_1.mark AS __in_sq_1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, __in_sq_1:Boolean]
                LeftMark Join(ComparisonJoin):  Filter: t1.t1_id + Int32(12) = t2.t2_id + Int32(1) AND t1.t1_int IS NOT DISTINCT FROM t1_dscan_1.t1_t1_int [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, mark:Boolean]
                  TableScan: t1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
                  Projection: t2.t2_id + Int32(1), t1_dscan_1.t1_t1_int [t2.t2_id + Int32(1):Int64, t1_t1_int:Int32;N]
                    Filter: t1_dscan_1.t1_t1_int > Int32(0) [t2_id:UInt32, t2_value:Utf8, t1_t1_int:Int32;N]
                      Inner Join(DelimJoin):  Filter: Boolean(true) [t2_id:UInt32, t2_value:Utf8, t1_t1_int:Int32;N]
                        TableScan: t2 [t2_id:UInt32, t2_value:Utf8]
                        Projection: t1.t1_int AS t1_dscan_1.t1_t1_int [t1_t1_int:Int32;N]
                          DelimGet: t1.t1_int [t1_int:Int32;N]
        ");

        Ok(())
    }

    #[test]
    fn subquery_slt_test3() -> Result<()> {
        // Test case for: SELECT t1_id, (SELECT sum(t2_int) FROM t2 WHERE t2.t2_id = t1.t1_id) as t2_sum from t1

        // Create test tables
        let t1 = test_table_with_columns(
            "t1",
            &[
                ("t1_id", ArrowDataType::UInt32),
                ("t1_name", ArrowDataType::Utf8),
                ("t1_int", ArrowDataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[
                ("t2_id", ArrowDataType::UInt32),
                ("t2_int", ArrowDataType::Int32),
                ("t2_value", ArrowDataType::Utf8),
            ],
        )?;

        // Create the scalar subquery: SELECT sum(t2_int) FROM t2 WHERE t2.t2_id = t1.t1_id
        let scalar_sq = Arc::new(
            LogicalPlanBuilder::from(t2)
                .filter(
                    col("t2.t2_id").eq(out_ref_col(ArrowDataType::UInt32, "t1.t1_id")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![sum(col("t2_int"))])?
                .build()?,
        );

        // Create the main query plan: SELECT t1_id, (subquery) as t2_sum FROM t1
        let plan = LogicalPlanBuilder::from(t1)
            .project(vec![
                col("t1_id"),
                scalar_subquery(scalar_sq).alias("t2_sum"),
            ])?
            .build()?;

        // Projection: t1.t1_id, __scalar_sq_1.output AS t2_sum [t1_id:UInt32, t2_sum:Int64]
        //   DependentJoin on [t1.t1_id lvl 1] with expr (<subquery>) depth 1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, output:Int64]
        //     TableScan: t1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
        //     Aggregate: groupBy=[[]], aggr=[[sum(t2.t2_int)]] [sum(t2.t2_int):Int64;N]
        //       Filter: t2.t2_id = outer_ref(t1.t1_id) [t2_id:UInt32, t2_int:Int32, t2_value:Utf8]
        //         TableScan: t2 [t2_id:UInt32, t2_int:Int32, t2_value:Utf8]

        assert_decorrelate!(plan, @r"
        Projection: t1.t1_id, __scalar_sq_1 AS t2_sum [t1_id:UInt32, t2_sum:Int64]
          Projection: t1.t1_id, t1.t1_name, t1.t1_int, t1_dscan_2.t1_t1_id, sum(t2.t2_int), t1_dscan_1.t1_t1_id, t1_dscan_2.t1_t1_id AS __scalar_sq_1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, t1_t1_id:UInt32;N, sum(t2.t2_int):Int64;N, t1_t1_id:UInt32;N, __scalar_sq_1:UInt32;N]
            LeftSingle Join(ComparisonJoin):  Filter: t1.t1_id IS NOT DISTINCT FROM t1_dscan_2.t1_t1_id [t1_id:UInt32, t1_name:Utf8, t1_int:Int32, t1_t1_id:UInt32;N, sum(t2.t2_int):Int64;N, t1_t1_id:UInt32;N]
              TableScan: t1 [t1_id:UInt32, t1_name:Utf8, t1_int:Int32]
              Inner Join(DelimJoin):  Filter: t1_dscan_1.t1_t1_id IS NOT DISTINCT FROM t1_dscan_2.t1_t1_id [t1_t1_id:UInt32;N, sum(t2.t2_int):Int64;N, t1_t1_id:UInt32;N]
                Projection: t1.t1_id AS t1_dscan_2.t1_t1_id [t1_t1_id:UInt32;N]
                  DelimGet: t1.t1_id [t1_id:UInt32;N]
                Projection: sum(t2.t2_int), t1_dscan_1.t1_t1_id [sum(t2.t2_int):Int64;N, t1_t1_id:UInt32;N]
                  Aggregate: groupBy=[[t1_dscan_1.t1_t1_id]], aggr=[[sum(t2.t2_int)]] [t1_t1_id:UInt32;N, sum(t2.t2_int):Int64;N]
                    Filter: t2.t2_id = t1_dscan_1.t1_t1_id [t2_id:UInt32, t2_int:Int32, t2_value:Utf8, t1_t1_id:UInt32;N]
                      Inner Join(DelimJoin):  Filter: Boolean(true) [t2_id:UInt32, t2_int:Int32, t2_value:Utf8, t1_t1_id:UInt32;N]
                        TableScan: t2 [t2_id:UInt32, t2_int:Int32, t2_value:Utf8]
                        Projection: t1.t1_id AS t1_dscan_1.t1_t1_id [t1_t1_id:UInt32;N]
                          DelimGet: t1.t1_id [t1_id:UInt32;N]
        ");

        Ok(())
    }

    #[test]
    fn subquery_slt_test4() -> Result<()> {
        // Test case for: SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2_id = t1_id LIMIT 1);

        // Create test tables matching the SQL schema
        let t1 = test_table_with_columns(
            "t1",
            &[
                ("t1_id", ArrowDataType::Int32),
                ("t1_name", ArrowDataType::Utf8),
                ("t1_int", ArrowDataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[
                ("t2_id", ArrowDataType::Int32),
                ("t2_name", ArrowDataType::Utf8),
                ("t2_int", ArrowDataType::Int32),
            ],
        )?;

        // Create the EXISTS subquery: SELECT * FROM t2 WHERE t2_id = t1_id LIMIT 1
        let exists_subquery = Arc::new(
            LogicalPlanBuilder::from(t2)
                .filter(
                    col("t2.t2_id").eq(out_ref_col(ArrowDataType::Int32, "t1.t1_id")),
                )?
                .limit(0, Some(1))? // LIMIT 1
                .build()?,
        );

        // Create the main query plan: SELECT t1_id, t1_name FROM t1 WHERE EXISTS (subquery)
        let plan = LogicalPlanBuilder::from(t1)
            .filter(exists(exists_subquery))?
            .project(vec![col("t1_id"), col("t1_name")])?
            .build()?;

        assert_decorrelate!(plan, @r"
        Projection: t1.t1_id, t1.t1_name [t1_id:Int32, t1_name:Utf8]
          Projection: t1.t1_id, t1.t1_name, t1.t1_int [t1_id:Int32, t1_name:Utf8, t1_int:Int32]
            Filter: __exists_sq_1 [t1_id:Int32, t1_name:Utf8, t1_int:Int32, __exists_sq_1:Boolean]
              Projection: t1.t1_id, t1.t1_name, t1.t1_int, mark AS __exists_sq_1 [t1_id:Int32, t1_name:Utf8, t1_int:Int32, __exists_sq_1:Boolean]
                LeftMark Join(ComparisonJoin):  Filter: t1.t1_id IS NOT DISTINCT FROM t1_dscan_1.t1_t1_id [t1_id:Int32, t1_name:Utf8, t1_int:Int32, mark:Boolean]
                  TableScan: t1 [t1_id:Int32, t1_name:Utf8, t1_int:Int32]
                  Projection: t2.t2_id, t2.t2_name, t2.t2_int, t1_dscan_1.t1_t1_id [t2_id:Int32, t2_name:Utf8, t2_int:Int32, t1_t1_id:Int32;N]
                    Filter: row_number <= UInt64(1) [t2_id:Int32, t2_name:Utf8, t2_int:Int32, t1_t1_id:Int32;N, row_number:UInt64]
                      WindowAggr: windowExpr=[[row_number() PARTITION BY [t1_dscan_1.t1_t1_id] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS row_number]] [t2_id:Int32, t2_name:Utf8, t2_int:Int32, t1_t1_id:Int32;N, row_number:UInt64]
                        Filter: t2.t2_id = t1_dscan_1.t1_t1_id [t2_id:Int32, t2_name:Utf8, t2_int:Int32, t1_t1_id:Int32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [t2_id:Int32, t2_name:Utf8, t2_int:Int32, t1_t1_id:Int32;N]
                            TableScan: t2 [t2_id:Int32, t2_name:Utf8, t2_int:Int32]
                            Projection: t1.t1_id AS t1_dscan_1.t1_t1_id [t1_t1_id:Int32;N]
                              DelimGet: t1.t1_id [t1_id:Int32;N]
        ");

        Ok(())
    }

    #[test]
    fn subquery_slt_test5() -> Result<()> {
        // Test case for: SELECT t1_id, (SELECT count(*) FROM t2 WHERE t2.t2_int = t1.t1_int) from t1;

        // Create test tables matching the SQL schema
        let t1 = test_table_with_columns(
            "t1",
            &[
                ("t1_id", ArrowDataType::Int32),
                ("t1_name", ArrowDataType::Utf8),
                ("t1_int", ArrowDataType::Int32),
            ],
        )?;

        let t2 = test_table_with_columns(
            "t2",
            &[
                ("t2_id", ArrowDataType::Int32),
                ("t2_name", ArrowDataType::Utf8),
                ("t2_int", ArrowDataType::Int32),
            ],
        )?;

        // Create the scalar subquery: SELECT count(*) FROM t2 WHERE t2.t2_int = t1.t1_int
        let scalar_sq = Arc::new(
            LogicalPlanBuilder::from(t2)
                .filter(
                    col("t2.t2_int").eq(out_ref_col(ArrowDataType::Int32, "t1.t1_int")),
                )?
                .aggregate(Vec::<Expr>::new(), vec![count(lit(1))])? // count(*) is represented as count(1)
                .build()?,
        );

        // Create the main query plan: SELECT t1_id, (subquery) FROM t1
        let plan = LogicalPlanBuilder::from(t1)
            .project(vec![col("t1_id"), scalar_subquery(scalar_sq)])?
            .build()?;

        // Projection: t1.t1_id, __scalar_sq_1 [t1_id:Int32, __scalar_sq_1:Int64]
        //   DependentJoin on [t1.t1_int lvl 1] with expr (<subquery>) depth 1 [t1_id:Int32, t1_name:Utf8, t1_int:Int32, __scalar_sq_1:Int64]
        //     TableScan: t1 [t1_id:Int32, t1_name:Utf8, t1_int:Int32]
        //     Aggregate: groupBy=[[]], aggr=[[count(Int32(1))]] [count(Int32(1)):Int64]
        //       Filter: t2.t2_int = outer_ref(t1.t1_int) [t2_id:Int32, t2_name:Utf8, t2_int:Int32]
        //         TableScan: t2 [t2_id:Int32, t2_name:Utf8, t2_int:Int32]

        // t1_id, count(1)
        assert_decorrelate!(plan, @r"");

        Ok(())
    }
}
