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

use crate::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult};
use datafusion_common::{plan_err, Result};
use datafusion_expr::utils::{expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{Expr, LogicalPlan, Projection};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub struct ExpandWildcardRule {}

impl ExpandWildcardRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for ExpandWildcardRule {
    fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
        // Because the wildcard expansion is based on the schema of the input plan,
        // using `transform_up_with_subqueries` here.
        plan.transform_up_with_subqueries(analyzer_internal).data()
    }

    fn name(&self) -> &str {
        "expand_wildcard_rule"
    }
}

fn analyzer_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let mut projected_expr = vec![];
            for e in expr {
                match e {
                    Expr::Wildcard { qualifier } => {
                        if let Some(qualifier) = qualifier {
                            projected_expr.extend(expand_qualified_wildcard(
                                &qualifier,
                                input.schema(),
                                None,
                            )?);
                        } else {
                            projected_expr.extend(expand_wildcard(
                                input.schema(),
                                &input,
                                None,
                            )?);
                        }
                    }
                    _ => projected_expr.push(e),
                }
            }
            validate_unique_names("Projections", projected_expr.iter())?;
            Ok(Transformed::yes(
                Projection::try_new(projected_expr, Arc::clone(&input))
                    .map(LogicalPlan::Projection)?,
            ))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn validate_unique_names<'a>(
    node_name: &str,
    expressions: impl IntoIterator<Item = &'a Expr>,
) -> Result<()> {
    let mut unique_names = HashMap::new();

    expressions.into_iter().enumerate().try_for_each(|(position, expr)| {
        let name = expr.display_name()?;
        match unique_names.get(&name) {
            None => {
                unique_names.insert(name, (position, expr));
                Ok(())
            },
            Some((existing_position, existing_expr)) => {
                plan_err!("{node_name} require unique expression names \
                             but the expression \"{existing_expr}\" at position {existing_position} and \"{expr}\" \
                             at position {position} have the same name. Consider aliasing (\"AS\") one of them."
                            )
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::{assert_analyzed_plan_eq_display_indent, test_table_scan};
    use datafusion_common::TableReference;
    use datafusion_expr::{
        col, in_subquery, qualified_wildcard, wildcard, LogicalPlanBuilder,
    };

    fn assert_plan_eq(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_analyzed_plan_eq_display_indent(
            Arc::new(ExpandWildcardRule::new()),
            plan,
            expected,
        )
    }

    #[test]
    fn test_expand_wildcard() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![wildcard()])?
            .build()?;
        let expected =
            "Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_expand_qualified_wildcard() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![qualified_wildcard(TableReference::bare("test"))])?
            .build()?;
        let expected =
            "Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_expand_qualified_wildcard_in_subquery() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![qualified_wildcard(TableReference::bare("test"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(plan)
            .project(vec![wildcard()])?
            .build()?;
        let expected =
            "Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]\
        \n  Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_expand_wildcard_in_subquery() -> Result<()> {
        let projection_a = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![col("a")])?
            .build()?;
        let subquery = LogicalPlanBuilder::from(projection_a)
            .project(vec![wildcard()])?
            .build()?;
        let plan = LogicalPlanBuilder::from(test_table_scan()?)
            .filter(in_subquery(col("a"), Arc::new(subquery)))?
            .project(vec![wildcard()])?
            .build()?;
        let expected = "\
        Projection: test.a, test.b, test.c [a:UInt32, b:UInt32, c:UInt32]\
        \n  Filter: test.a IN (<subquery>) [a:UInt32, b:UInt32, c:UInt32]\
        \n    Subquery: [a:UInt32]\
        \n      Projection: test.a [a:UInt32]\
        \n        Projection: test.a [a:UInt32]\
        \n          TableScan: test [a:UInt32, b:UInt32, c:UInt32]\
        \n    TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }
}
