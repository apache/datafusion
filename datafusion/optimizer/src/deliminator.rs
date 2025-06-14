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

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::{Join, JoinKind, JoinType, LogicalPlan};

use crate::decorrelate_dependent_join::DecorrelateDependentJoin;
use crate::{ApplyOrder, OptimizerConfig, OptimizerRule};

/// The Deliminator optimizer traverses the logical operator tree and removes any
/// redundant DelimScan/DelimJoins.
#[derive(Debug)]
pub struct Deliminator {}

impl Deliminator {
    pub fn new() -> Self {
        return Deliminator {};
    }
}

impl OptimizerRule for Deliminator {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let transformer = DecorrelateDependentJoin::new();
        let rewrite_result = transformer.rewrite(plan, config)?;

        let mut visitor = DelimCandidateVisitor::new();
        let _ = rewrite_result.data.visit(&mut visitor)?;
        for candidate in &visitor.candidates {
            println!("=== DelimCandidate ===");
            println!("  plan: {}", candidate.plan.display());
            println!("  delim_get_count: {}", candidate.delim_get_count);
            println!("  joins: [");
            for join in &candidate.joins {
                println!("    JoinWithDelimGet {{");
                println!("      depth: {}", join.depth);
                println!("      join: {}", join.join.display());
                println!("    }},");
            }
            println!("  ]");
            println!("==================\n");
        }

        if visitor.candidates.is_empty() {
            return Ok(rewrite_result);
        }

        for candidate in visitor.candidates.iter_mut() {
            let _delim_join = &candidate.delim_join;
            let plan = &candidate.plan;

            // Sort these so the deepest are first.
            candidate.joins.sort_by(|a, b| b.depth.cmp(&a.depth));

            let mut all_removed = true;
            if !candidate.joins.is_empty() {
                let mut has_selection = false;
                plan.apply(|plan| {
                    match plan {
                        LogicalPlan::TableScan(_) => {
                            has_selection = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        LogicalPlan::Filter(_) => {
                            has_selection = true;
                            return Ok(TreeNodeRecursion::Stop);
                        }
                        _ => {}
                    }

                    Ok(TreeNodeRecursion::Continue)
                })?;

                if has_selection {
                    // Keey the deepest join with DelimScan in these cases,
                    // as the selection can greatly reduce the cost of the RHS child of the
                    // DelimJoin.
                    candidate.joins.remove(0);
                    all_removed = false;
                }

                let _all_equality_conditions = true;
                for _join in &candidate.joins {
                    // TODO remove join with delim scan.
                }

                // Change type if there are no more duplicate-eliminated columns.
                if candidate.joins.len() == candidate.delim_get_count && all_removed {
                    // TODO: how we can change it.
                    // delim_join.join_kind = JoinKind::ComparisonJoin;
                }

                // Only DelimJoins are ever created as SINGLE joins, and we can switch from SINGLE
                // to LEFT if the RHS is de-deuplicated by an aggr.
                // TODO: add single join support.
            }
        }

        Ok(rewrite_result)
    }

    fn name(&self) -> &str {
        "deliminator"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }
}

#[allow(unused_mut)]
#[allow(dead_code)]
fn remove_join_with_delim_scan(
    _delim_join: &Join,
    _delim_get_count: usize,
    join: &LogicalPlan,
    _all_equality_conditions: &mut bool,
) -> Result<bool> {
    if let LogicalPlan::Join(join) = join {
        if !child_join_type_can_be_deliminated(join.join_type) {
            return Ok(false);
        }

        // Fetch delim scan.
        let mut plan_pair = fetch_delim_scan(join.left.as_ref());
        if plan_pair.1.is_none() {
            plan_pair = fetch_delim_scan(join.right.as_ref());
        }

        let delim_scan = plan_pair
            .1
            .ok_or_else(|| DataFusionError::Plan("No delim scan found".to_string()))?;
        let delim_scan = if let LogicalPlan::DelimGet(delim_scan) = delim_scan {
            delim_scan
        } else {
            return internal_err!("unreachable");
        };

        if join.on.len() != delim_scan.delim_types.len() {
            // Joining with delim scan adds new information.
            return Ok(false);
        }

        // Check if joining with the delim scan is redundant, and collect relevant column
        // information.
    } else {
        return internal_err!("current plan must be join in remove_join_with_delim_scan");
    }

    todo!()
}

fn child_join_type_can_be_deliminated(join_type: JoinType) -> bool {
    match join_type {
        JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => true,
        _ => false,
    }
}

// fetch filter (if any) and delim scan
fn fetch_delim_scan(plan: &LogicalPlan) -> (Option<&LogicalPlan>, Option<&LogicalPlan>) {
    match plan {
        LogicalPlan::Filter(filter) => {
            if let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref() {
                if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                    return (Some(plan), Some(alias.input.as_ref()));
                };
            };
        }
        LogicalPlan::SubqueryAlias(alias) => {
            if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
                return (None, Some(alias.input.as_ref()));
            }
        }
        _ => return (None, None),
    }

    todo!()
}

#[allow(dead_code)]
fn remove_inequality_join_with_delim_scan(
    delim_join: &Join,
    _delim_get_count: usize,
    join: &LogicalPlan,
) -> Result<bool> {
    if let LogicalPlan::Join(_) = join {
        let _delim_on = &delim_join.on;
    } else {
        return internal_err!(
            "current plan must be join in remove_inequality_join_with_delim_scan"
        );
    }

    todo!()
}

struct JoinWithDelimGet {
    join: LogicalPlan,
    depth: usize,
}

impl JoinWithDelimGet {
    fn new(join: LogicalPlan, depth: usize) -> Self {
        Self { join, depth }
    }
}

#[allow(dead_code)]
struct DelimCandidate {
    plan: LogicalPlan,
    delim_join: Join,
    joins: Vec<JoinWithDelimGet>,
    delim_get_count: usize,
}

impl DelimCandidate {
    fn new(plan: LogicalPlan, delim_join: Join) -> Self {
        Self {
            plan,
            delim_join,
            joins: vec![],
            delim_get_count: 0,
        }
    }
}

struct DelimCandidateVisitor {
    candidates: Vec<DelimCandidate>,
}

impl DelimCandidateVisitor {
    fn new() -> Self {
        Self { candidates: vec![] }
    }
}

impl TreeNodeVisitor<'_> for DelimCandidateVisitor {
    type Node = LogicalPlan;

    fn f_down(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, plan: &Self::Node) -> Result<TreeNodeRecursion> {
        if let LogicalPlan::Join(join) = plan {
            if join.join_kind == JoinKind::DelimJoin {
                self.candidates
                    .push(DelimCandidate::new(plan.clone(), join.clone()));

                if let Some(candidate) = self.candidates.last_mut() {
                    // DelimScans are in the RHS.
                    find_join_with_delim_scan(join.right.as_ref(), candidate, 0);
                } else {
                    unreachable!()
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    }
}

fn find_join_with_delim_scan(
    plan: &LogicalPlan,
    candidate: &mut DelimCandidate,
    depth: usize,
) {
    if let LogicalPlan::Join(join) = plan {
        if join.join_kind == JoinKind::DelimJoin {
            find_join_with_delim_scan(join.left.as_ref(), candidate, depth + 1);
        } else {
            for child in plan.inputs() {
                find_join_with_delim_scan(child, candidate, depth + 1);
            }
        }
    } else if let LogicalPlan::DelimGet(_) = plan {
        candidate.delim_get_count += 1;
    } else {
        for child in plan.inputs() {
            find_join_with_delim_scan(child, candidate, depth + 1);
        }
    }

    if let LogicalPlan::Join(join) = plan {
        if join.join_kind == JoinKind::DelimJoin
            && (is_delim_scan(join.left.as_ref()) || is_delim_scan(join.right.as_ref()))
        {
            candidate
                .joins
                .push(JoinWithDelimGet::new(plan.clone(), depth));
        }
    }
}

fn is_delim_scan(plan: &LogicalPlan) -> bool {
    if let LogicalPlan::SubqueryAlias(alias) = plan {
        if let LogicalPlan::DelimGet(_) = alias.input.as_ref() {
            true
        } else {
            false
        }
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_optimized_plan_eq_display_indent_snapshot;
    use crate::deliminator::Deliminator;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType as ArrowDataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        expr_fn::col, in_subquery, lit, out_ref_col, scalar_subquery, Expr,
        LogicalPlanBuilder,
    };
    use datafusion_functions_aggregate::count::count;
    use std::sync::Arc;

    macro_rules! assert_deliminator{
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rule: Arc<dyn crate::OptimizerRule + Send + Sync> = Arc::new(Deliminator::new());
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
        assert_deliminator!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __scalar_sq_2.output = outer_table.a [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
            Projection: outer_table.a, outer_table.b, outer_table.c, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AS __scalar_sq_2.output [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, __scalar_sq_2.output:Int32;N]
              Left Join(ComparisonJoin):  Filter: outer_table.a IS NOT DISTINCT FROM delim_scan_4.outer_table_a AND outer_table.c IS NOT DISTINCT FROM delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join(DelimJoin):  Filter: delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_1.outer_table_c [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv1.a):Int64]
                        Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                          Filter: inner_table_lv1.c = delim_scan_4.outer_table_c AND __scalar_sq_1.output = Int32(1) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                            Projection: inner_table_lv1.a, inner_table_lv1.b, inner_table_lv1.c, delim_scan_2.outer_table_a, delim_scan_2.outer_table_c, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END AS __scalar_sq_1.output [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, __scalar_sq_1.output:Int32;N]
                              Left Join(ComparisonJoin):  Filter: inner_table_lv1.b IS NOT DISTINCT FROM delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N, CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32;N, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                  TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                                  SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    DelimGet: outer_table.a, outer_table.c [outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END:Int32, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                  Inner Join(DelimJoin):  Filter: delim_scan_4.inner_table_lv1_b IS NOT DISTINCT FROM delim_scan_3.inner_table_lv1_b AND delim_scan_4.outer_table_a IS NOT DISTINCT FROM delim_scan_3.outer_table_a AND delim_scan_4.outer_table_c IS NOT DISTINCT FROM delim_scan_3.outer_table_c [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                    Projection: CASE WHEN count(inner_table_lv2.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv2.a) END, delim_scan_4.outer_table_c, delim_scan_4.outer_table_a, delim_scan_4.inner_table_lv1_b [count(inner_table_lv2.a):Int64, outer_table_c:UInt32;N, outer_table_a:UInt32;N, inner_table_lv1_b:UInt32;N]
                                      Aggregate: groupBy=[[delim_scan_4.inner_table_lv1_b, delim_scan_4.outer_table_a, delim_scan_4.outer_table_c]], aggr=[[count(inner_table_lv2.a)]] [inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N, count(inner_table_lv2.a):Int64]
                                        Filter: inner_table_lv2.a = delim_scan_4.outer_table_a AND inner_table_lv2.b = delim_scan_4.inner_table_lv1_b [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
                                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, inner_table_lv1_b:UInt32;N, outer_table_a:UInt32;N, outer_table_c:UInt32;N]
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

        assert_deliminator!(plan, @r"
        Projection: outer_table.a, outer_table.b, outer_table.c [a:UInt32, b:UInt32, c:UInt32]
          Filter: outer_table.a > Int32(1) AND __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
            Projection: outer_table.a, outer_table.b, outer_table.c, delim_scan_2.mark AS __in_sq_1.output [a:UInt32, b:UInt32, c:UInt32, __in_sq_1.output:Boolean]
              LeftMark Join(ComparisonJoin):  Filter: outer_table.c = CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END AND outer_table.a IS NOT DISTINCT FROM delim_scan_2.outer_table_a AND outer_table.b IS NOT DISTINCT FROM delim_scan_2.outer_table_b [a:UInt32, b:UInt32, c:UInt32, mark:Boolean]
                TableScan: outer_table [a:UInt32, b:UInt32, c:UInt32]
                Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END:Int32, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                  Inner Join(DelimJoin):  Filter: delim_scan_2.outer_table_a IS NOT DISTINCT FROM delim_scan_1.outer_table_a AND delim_scan_2.outer_table_b IS NOT DISTINCT FROM delim_scan_1.outer_table_b [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    Projection: CASE WHEN count(inner_table_lv1.a) IS NULL THEN Int32(0) ELSE count(inner_table_lv1.a) END, delim_scan_2.outer_table_b, delim_scan_2.outer_table_a [count(inner_table_lv1.a):Int64, outer_table_b:UInt32;N, outer_table_a:UInt32;N]
                      Aggregate: groupBy=[[delim_scan_2.outer_table_a, delim_scan_2.outer_table_b]], aggr=[[count(inner_table_lv1.a)]] [outer_table_a:UInt32;N, outer_table_b:UInt32;N, count(inner_table_lv1.a):Int64]
                        Filter: inner_table_lv1.a = delim_scan_2.outer_table_a AND delim_scan_2.outer_table_a > inner_table_lv1.c AND inner_table_lv1.b = Int32(1) AND delim_scan_2.outer_table_b = inner_table_lv1.b [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                          Inner Join(DelimJoin):  Filter: Boolean(true) [a:UInt32, b:UInt32, c:UInt32, outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                            TableScan: inner_table_lv1 [a:UInt32, b:UInt32, c:UInt32]
                            SubqueryAlias: delim_scan_2 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                              DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                    SubqueryAlias: delim_scan_1 [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
                      DelimGet: outer_table.a, outer_table.b [outer_table_a:UInt32;N, outer_table_b:UInt32;N]
        ");
        Ok(())
    }
}
