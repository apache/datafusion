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

use std::sync::Arc;

use crate::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult};
use datafusion_common::{Column, Result};
use datafusion_expr::builder::validate_unique_names;
use datafusion_expr::expr::PlannedReplaceSelectItem;
use datafusion_expr::utils::{
    expand_qualified_wildcard, expand_wildcard, find_base_plan,
};
use datafusion_expr::{
    Distinct, DistinctOn, Expr, LogicalPlan, Projection, SubqueryAlias,
};

#[derive(Default, Debug)]
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
        plan.transform_up_with_subqueries(expand_internal).data()
    }

    fn name(&self) -> &str {
        "expand_wildcard_rule"
    }
}

fn expand_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    match plan {
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let projected_expr = expand_exprlist(&input, expr)?;
            validate_unique_names("Projections", projected_expr.iter())?;
            Ok(Transformed::yes(
                Projection::try_new(projected_expr, Arc::clone(&input))
                    .map(LogicalPlan::Projection)?,
            ))
        }
        // The schema of the plan should also be updated if the child plan is transformed.
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
            Ok(Transformed::yes(
                SubqueryAlias::try_new(input, alias).map(LogicalPlan::SubqueryAlias)?,
            ))
        }
        LogicalPlan::Distinct(Distinct::On(distinct_on)) => {
            let projected_expr =
                expand_exprlist(&distinct_on.input, distinct_on.select_expr)?;
            validate_unique_names("Distinct", projected_expr.iter())?;
            Ok(Transformed::yes(LogicalPlan::Distinct(Distinct::On(
                DistinctOn::try_new(
                    distinct_on.on_expr,
                    projected_expr,
                    distinct_on.sort_expr,
                    distinct_on.input,
                )?,
            ))))
        }
        _ => Ok(Transformed::no(plan)),
    }
}

fn expand_exprlist(input: &LogicalPlan, expr: Vec<Expr>) -> Result<Vec<Expr>> {
    let mut projected_expr = vec![];
    let input = find_base_plan(input);
    for e in expr {
        match e {
            Expr::Wildcard { qualifier, options } => {
                if let Some(qualifier) = qualifier {
                    let expanded = expand_qualified_wildcard(
                        &qualifier,
                        input.schema(),
                        Some(&options),
                    )?;
                    // If there is a REPLACE statement, replace that column with the given
                    // replace expression. Column name remains the same.
                    let replaced = if let Some(replace) = options.replace {
                        replace_columns(expanded, &replace)?
                    } else {
                        expanded
                    };
                    projected_expr.extend(replaced);
                } else {
                    let expanded =
                        expand_wildcard(input.schema(), input, Some(&options))?;
                    // If there is a REPLACE statement, replace that column with the given
                    // replace expression. Column name remains the same.
                    let replaced = if let Some(replace) = options.replace {
                        replace_columns(expanded, &replace)?
                    } else {
                        expanded
                    };
                    projected_expr.extend(replaced);
                }
            }
            // A workaround to handle the case when the column name is "*".
            // We transform the expression to a Expr::Column through [Column::from_name] in many places.
            // It would also convert the wildcard expression to a column expression with name "*".
            Expr::Column(Column {
                ref relation,
                ref name,
            }) => {
                if name.eq("*") {
                    if let Some(qualifier) = relation {
                        projected_expr.extend(expand_qualified_wildcard(
                            qualifier,
                            input.schema(),
                            None,
                        )?);
                    } else {
                        projected_expr.extend(expand_wildcard(
                            input.schema(),
                            input,
                            None,
                        )?);
                    }
                } else {
                    projected_expr.push(e.clone());
                }
            }
            _ => projected_expr.push(e),
        }
    }
    Ok(projected_expr)
}

/// If there is a REPLACE statement in the projected expression in the form of
/// "REPLACE (some_column_within_an_expr AS some_column)", this function replaces
/// that column with the given replace expression. Column name remains the same.
/// Multiple REPLACEs are also possible with comma separations.
fn replace_columns(
    mut exprs: Vec<Expr>,
    replace: &PlannedReplaceSelectItem,
) -> Result<Vec<Expr>> {
    for expr in exprs.iter_mut() {
        if let Expr::Column(Column { name, .. }) = expr {
            if let Some((_, new_expr)) = replace
                .items()
                .iter()
                .zip(replace.expressions().iter())
                .find(|(item, _)| item.column_name.value == *name)
            {
                *expr = new_expr.clone().alias(name.clone())
            }
        }
    }
    Ok(exprs)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::test::{assert_analyzed_plan_eq_display_indent, test_table_scan};
    use crate::Analyzer;
    use datafusion_common::{JoinType, TableReference};
    use datafusion_expr::{
        col, in_subquery, qualified_wildcard, table_scan, wildcard, LogicalPlanBuilder,
    };

    use super::*;

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

    #[test]
    fn test_expand_wildcard_in_distinct_on() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .distinct_on(vec![col("a")], vec![wildcard()], None)?
            .build()?;
        let expected = "\
        DistinctOn: on_expr=[[test.a]], select_expr=[[test.a, test.b, test.c]], sort_expr=[[]] [a:UInt32, b:UInt32, c:UInt32]\
        \n  TableScan: test [a:UInt32, b:UInt32, c:UInt32]";
        assert_plan_eq(plan, expected)
    }

    #[test]
    fn test_subquery_schema() -> Result<()> {
        let analyzer = Analyzer::with_rules(vec![Arc::new(ExpandWildcardRule::new())]);
        let options = ConfigOptions::default();
        let subquery = LogicalPlanBuilder::from(test_table_scan()?)
            .project(vec![wildcard()])?
            .build()?;
        let plan = LogicalPlanBuilder::from(subquery)
            .alias("sub")?
            .project(vec![wildcard()])?
            .build()?;
        let analyzed_plan = analyzer.execute_and_check(plan, &options, |_, _| {})?;
        for x in analyzed_plan.inputs() {
            for field in x.schema().fields() {
                assert_ne!(field.name(), "*");
            }
        }
        Ok(())
    }

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn plan_using_join_wildcard_projection() -> Result<()> {
        let t2 = table_scan(Some("t2"), &employee_schema(), None)?.build()?;

        let plan = table_scan(Some("t1"), &employee_schema(), None)?
            .join_using(t2, JoinType::Inner, vec!["id"])?
            .project(vec![wildcard()])?
            .build()?;

        let expected = "Projection: *\
        \n  Inner Join: Using t1.id = t2.id\
        \n    TableScan: t1\
        \n    TableScan: t2";

        assert_eq!(expected, format!("{plan}"));

        let analyzer = Analyzer::with_rules(vec![Arc::new(ExpandWildcardRule::new())]);
        let options = ConfigOptions::default();

        let analyzed_plan = analyzer.execute_and_check(plan, &options, |_, _| {})?;

        // id column should only show up once in projection
        let expected = "Projection: t1.id, t1.first_name, t1.last_name, t1.state, t1.salary, t2.first_name, t2.last_name, t2.state, t2.salary\
        \n  Inner Join: Using t1.id = t2.id\
        \n    TableScan: t1\
        \n    TableScan: t2";
        assert_eq!(expected, format!("{analyzed_plan}"));

        Ok(())
    }
}
