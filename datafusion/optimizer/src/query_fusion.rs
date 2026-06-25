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

//! Experimental query fusion rewrites.
//!
//! This proof of concept implements the UNION ALL branch-tag rewrite from
//! "Computation reuse via fusion in Amazon Athena" for a deliberately narrow
//! shape: UNION ALL branches that differ only by safe filters over the same
//! source and compatible wrappers.

use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRewriter};
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::{
    Expr, Filter, LogicalPlan, Projection, SubqueryAlias, Union, col, lit,
};
use log::debug;

const BRANCH_TAG_COLUMN: &str = "__datafusion_query_fusion_branch";

#[derive(Default, Debug)]
pub struct QueryFusion;

impl QueryFusion {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for QueryFusion {
    fn name(&self) -> &str {
        "query_fusion"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_query_fusion {
            return Ok(Transformed::no(plan));
        }

        if !plan.exists(|p| Ok(matches!(p, LogicalPlan::Union(_))))? {
            return Ok(Transformed::no(plan));
        }

        plan.rewrite_with_subqueries(&mut QueryFusionRewriter)
    }
}

struct QueryFusionRewriter;

impl TreeNodeRewriter for QueryFusionRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::Union(_) => match try_rewrite_union_all(plan.clone())? {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            },
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn try_rewrite_union_all(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Union(Union { inputs, schema }) = plan else {
        return Ok(None);
    };

    if inputs.len() < 2 {
        debug!("query_fusion skipped: UNION has fewer than two inputs");
        return Ok(None);
    }

    let mut branches = Vec::with_capacity(inputs.len());
    for input in inputs {
        let Some(branch) = extract_branch(Arc::unwrap_or_clone(input))? else {
            return Ok(None);
        };
        branches.push(branch);
    }

    let Some(first) = branches.first() else {
        return Ok(None);
    };
    if !branches
        .iter()
        .all(|branch| branch.source == first.source && branch.wrappers == first.wrappers)
    {
        debug!("query_fusion skipped: UNION branches do not share one source");
        return Ok(None);
    }

    let fused_source = first.source.clone();
    let tag_values = branches
        .iter()
        .enumerate()
        .map(|(idx, _)| vec![lit(idx as i32)])
        .collect::<Vec<_>>();
    let tags = LogicalPlanBuilder::values(tag_values)?
        .project(vec![col("column1").alias(BRANCH_TAG_COLUMN)])?
        .build()?;

    let tagged = LogicalPlanBuilder::from(fused_source)
        .cross_join(tags)?
        .build()?;

    let tag_col = col(BRANCH_TAG_COLUMN);
    let filter = branches
        .iter()
        .enumerate()
        .map(|(idx, branch)| {
            tag_col
                .clone()
                .eq(lit(idx as i32))
                .and(branch.predicate.clone())
        })
        .reduce(|left, right| left.or(right))
        .expect("at least two UNION inputs");

    let filtered = LogicalPlanBuilder::from(tagged).filter(filter)?.build()?;
    let projected = wrap_branch(filtered, &first.wrappers)?;
    align_plan_to_schema(projected, schema).map(Some)
}

struct UnionBranch {
    source: LogicalPlan,
    predicate: Expr,
    wrappers: Vec<Wrapper>,
}

fn extract_branch(plan: LogicalPlan) -> Result<Option<UnionBranch>> {
    let (wrappers, plan) = peel_wrappers(plan);

    if !wrapper_projections_are_safe(&wrappers) {
        debug!("query_fusion skipped: wrapper projection is unsafe");
        return Ok(None);
    }

    match plan {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                debug!("query_fusion skipped: branch predicate is unsafe");
                return Ok(None);
            }
            Ok(Some(UnionBranch {
                source: strip_passthrough_nodes(Arc::unwrap_or_clone(input)),
                predicate,
                wrappers,
            }))
        }
        LogicalPlan::Limit(_) => {
            debug!("query_fusion skipped: branch contains LIMIT");
            Ok(None)
        }
        LogicalPlan::Sort(_) => {
            debug!("query_fusion skipped: branch contains SORT");
            Ok(None)
        }
        other => Ok(Some(UnionBranch {
            source: strip_passthrough_nodes(other),
            predicate: lit(true),
            wrappers,
        })),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Wrapper {
    Projection {
        expr: Vec<Expr>,
        schema: datafusion_common::DFSchemaRef,
    },
    SubqueryAlias {
        alias: datafusion_common::TableReference,
        schema: datafusion_common::DFSchemaRef,
    },
}

fn peel_wrappers(mut plan: LogicalPlan) -> (Vec<Wrapper>, LogicalPlan) {
    let mut wrappers = vec![];
    loop {
        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::Projection { expr, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::SubqueryAlias { alias, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            other => return (wrappers, other),
        }
    }
}

fn wrap_branch(mut plan: LogicalPlan, wrappers: &[Wrapper]) -> Result<LogicalPlan> {
    for wrapper in wrappers.iter().rev() {
        plan = match wrapper {
            Wrapper::Projection { expr, schema } => {
                LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr.clone(),
                    Arc::new(plan),
                    Arc::clone(schema),
                )?)
            }
            Wrapper::SubqueryAlias { alias, .. } => LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), alias.clone())?,
            ),
        };
    }
    Ok(plan)
}

fn strip_passthrough_nodes(mut plan: LogicalPlan) -> LogicalPlan {
    loop {
        plan = match plan {
            LogicalPlan::Projection(Projection { input, .. }) => {
                Arc::unwrap_or_clone(input)
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                Arc::unwrap_or_clone(input)
            }
            other => return other,
        };
    }
}

fn align_plan_to_schema(
    plan: LogicalPlan,
    schema: datafusion_common::DFSchemaRef,
) -> Result<LogicalPlan> {
    if plan.schema() == &schema {
        return Ok(plan);
    }

    let expr = plan
        .schema()
        .iter()
        .take(schema.fields().len())
        .enumerate()
        .map(|(i, _)| {
            Expr::Column(datafusion_common::Column::from(
                plan.schema().qualified_field(i),
            ))
        })
        .collect::<Vec<_>>();

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expr,
        Arc::new(plan),
        schema,
    )?))
}

fn is_mergeable_predicate(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_subquery(expr)
}

fn wrapper_projections_are_safe(wrappers: &[Wrapper]) -> bool {
    wrappers.iter().all(|w| match w {
        Wrapper::Projection { expr, .. } => expr
            .iter()
            .all(|e| !e.is_volatile() && !expr_contains_subquery(e)),
        Wrapper::SubqueryAlias { .. } => true,
    })
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => Ok(true),
        _ => Ok(false),
    })
    .expect("boolean expression walk is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType;
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    };

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut options = datafusion_common::config::ConfigOptions::default();
            options.optimizer.enable_query_fusion = true;
            let optimizer_ctx = OptimizerContext::new_with_config_options(Arc::new(options))
                .with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(QueryFusion::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileTestUdf;

    impl ScalarUDFImpl for VolatileTestUdf {
        fn name(&self) -> &str {
            "volatile_test"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
            &SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("VolatileTestUdf is not intended for execution")
        }
    }

    fn volatile_expr() -> Expr {
        ScalarUDF::new_from_impl(VolatileTestUdf).call(vec![])
    }

    #[test]
    fn rewrite_union_all_same_source_filters() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: t.a, t.b, t.c
          Filter: __datafusion_query_fusion_branch = Int32(0) AND t.a = Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND t.a = Int32(2)
            Cross Join:
              TableScan: t
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_all_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Filter: t1.a = Int32(1)
            TableScan: t1
          Filter: t2.a = Int32(2)
            TableScan: t2
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_all_preserves_overlap_duplicates() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").gt(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").lt(lit(10)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: t.a, t.b, t.c
          Filter: __datafusion_query_fusion_branch = Int32(0) AND t.a > Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND t.a < Int32(10)
            Cross Join:
              TableScan: t
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_all_with_matching_projection() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: emp.a AS mgr, emp.b AS comm
          Filter: __datafusion_query_fusion_branch = Int32(0) AND emp.a = Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND emp.a = Int32(2)
            Cross Join:
              TableScan: emp
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_all_with_volatile_projection() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: volatile_test() AS v, t.a
            Filter: t.a = Int32(1)
              TableScan: t
          Projection: volatile_test() AS v, t.a
            Filter: t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }
}
