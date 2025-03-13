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
use datafusion_common::DataFusionError;
use datafusion_common::{Column, DataFusionError::SchemaError, Result};
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
    let (has_group_by, group_by_columns, aggregate_columns) = check_group_by_info(&input);
    let input = find_base_plan(input);
    for e in expr {
        match e {
            #[expect(deprecated)]
            Expr::Wildcard { qualifier, options } => {
                let expanded = if let Some(qualifier) = qualifier {
                    let expanded = expand_qualified_wildcard(
                        &qualifier,
                        input.schema(),
                        Some(&options),
                    )?;
                    // If there is a REPLACE statement, replace that column with the given
                    // replace expression. Column name remains the same.
                    if let Some(replace) = options.replace {
                        replace_columns(expanded, &replace)?
                    } else {
                        expanded
                    }
                } else {
                    let expanded =
                        expand_wildcard(input.schema(), input, Some(&options))?;
                    // If there is a REPLACE statement, replace that column with the given
                    // replace expression. Column name remains the same.
                    if let Some(replace) = options.replace {
                        replace_columns(expanded, &replace)?
                    } else {
                        expanded
                    }
                };
                // check if the columns in the SELECT list are in the GROUP BY clause
                // or are part of an aggregate function, if not, throw an error.
                validate_columns_in_group_by_or_aggregate(
                    has_group_by,
                    &expanded,
                    &group_by_columns,
                    &aggregate_columns,
                )?;
                projected_expr.extend(expanded);
            }
            // A workaround to handle the case when the column name is "*".
            // We transform the expression to a Expr::Column through [Column::from_name] in many places.
            // It would also convert the wildcard expression to a column expression with name "*".
            Expr::Column(Column {
                ref relation,
                ref name,
                // TODO Should we use these spans?
                spans: _,
            }) => {
                if name.eq("*") {
                    let expanded = if let Some(qualifier) = relation {
                        expand_qualified_wildcard(qualifier, input.schema(), None)?
                    } else {
                        expand_wildcard(input.schema(), input, None)?
                    };

                    validate_columns_in_group_by_or_aggregate(
                        has_group_by,
                        &expanded,
                        &group_by_columns,
                        &aggregate_columns,
                    )?;

                    projected_expr.extend(expanded);
                } else {
                    projected_expr.push(e.clone());
                }
            }
            _ => projected_expr.push(e),
        }
    }
    Ok(projected_expr)
}

/// This function is for checking if query has a GROUP BY clause
/// If it does, extract the columns in the GROUP BY clause
/// and the columns in the aggregate functions for future use.
/// This function is return a tuple of three values:
/// 1. A boolean value indicating if the query has a GROUP BY clause
/// 2. A vector of columns in the GROUP BY clause
/// 3. A vector of columns in the aggregate functions
fn check_group_by_info(input: &LogicalPlan) -> (bool, Vec<Expr>, Vec<Expr>) {
    // check if the input plan has a GROUP BY clause
    let has_group_by = if let LogicalPlan::Aggregate(agg) = input {
        !agg.group_expr.is_empty()
    } else {
        false
    };
    // get the columns that are in the GROUP BY clause
    let group_by_columns = if let LogicalPlan::Aggregate(agg) = input {
        agg.group_expr.clone()
    } else {
        vec![]
    };
    // get the columns that are in the Aggregare functions
    let mut aggregate_columns = vec![];
    if let LogicalPlan::Aggregate(agg) = input {
        for expr in &agg.aggr_expr {
            if let Expr::AggregateFunction(agg_func) = expr {
                for arg in &agg_func.params.args {
                    if let Expr::Column(_) = arg {
                        aggregate_columns.push(arg.clone());
                    }
                }
            } else {
                aggregate_columns.push(expr.clone());
            }
        }
    }

    (has_group_by, group_by_columns, aggregate_columns)
}

/// This function is for checking if the columns in the SELECT list are
/// in the GROUP BY clause or are part of an aggregate function.
/// If not, throw an SchemaError::GroupByColumnInvalid and return an error.
///
fn validate_columns_in_group_by_or_aggregate(
    has_group_by: bool,
    expanded: &Vec<Expr>,
    group_by_columns: &Vec<Expr>,
    aggregate_columns: &Vec<Expr>,
) -> Result<(), DataFusionError> {
    if has_group_by {
        for e in expanded {
            if let Expr::Column(col) = e {
                let name = &col.name;
                if !group_by_columns.contains(e) && !aggregate_columns.contains(e) {
                    return Err(SchemaError(
                        datafusion_common::SchemaError::GroupByOrAggregateColumnInvalid {
                            column: (name.clone().to_string()),
                        },
                        Box::new(None),
                    ));
                }
            }
        }
    }
    Ok(())
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
    use datafusion_functions_aggregate::sum::sum;

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

    #[test]
    fn test_wildcard_with_group_by_error_message() -> Result<()> {
        // Create a simple schema
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int8, false),
        ]);

        // Build a plan with GROUP BY and wildcard projection
        let plan = table_scan(Some("foo"), &schema, None)?
            .aggregate(
                vec![Expr::Column(Column::from_name("a"))],
                vec![] as Vec<Expr>,
            )? // GROUP BY a
            .project(vec![wildcard()])? // SELECT *
            .build()?;

        // Set up the analyzer with our rule
        let analyzer = Analyzer::with_rules(vec![Arc::new(ExpandWildcardRule::new())]);
        let options = ConfigOptions::default();

        // Run the analyzer and expect an error
        let result = analyzer.execute_and_check(plan, &options, |_, _| {});

        // Check that it fails with the right error message
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        println!("Actual error message: {}", &error_message);
        let expected_msg = "expand_wildcard_rule\ncaused by\nSchema error: While expanding wildcard, column 'b' must appear in the GROUP BY clause or must be part of an aggregate function";
        assert_eq!(
            expected_msg, error_message,
            "Error message didn't match. Got: {}",
            error_message
        );

        Ok(())
    }

    #[test]
    fn test_columns_in_group_by_and_aggregates() -> Result<()> {
        // Schema with multiple columns
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int8, false),
        ]);

        // Some columns in GROUP BY, others in aggregate functions
        let plan = table_scan(Some("foo"), &schema, None)?
            .aggregate(
                // test group by a with sum b and max c
                // we can use new_udf to create a custom aggregate function
                vec![Expr::Column(Column::from_name("a"))],
                vec![sum(col("c"))],
            )?
            .project(vec![wildcard()])? // SELECT *
            .build()?;

        let analyzer = Analyzer::with_rules(vec![Arc::new(ExpandWildcardRule::new())]);
        let options = ConfigOptions::default();

        // Should succeed as all columns are properly handled
        let result = analyzer.execute_and_check(plan, &options, |_, _| {});
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        // Print the actual error message for debugging
        println!("Actual error message: {}", &error_message);
        let expected_msg = "expand_wildcard_rule\ncaused by\nSchema error: While expanding wildcard, column 'b' must appear in the GROUP BY clause or must be part of an aggregate function";
        assert_eq!(
            expected_msg, error_message,
            "Error message didn't match. Got: {}",
            error_message
        );

        Ok(())
    }
}
