<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Query Optimizer

[DataFusion][df] is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory
format.

DataFusion has modular design, allowing individual crates to be re-used in other projects.

This crate is a submodule of DataFusion that provides a query optimizer for logical plans, and
contains an extensive set of OptimizerRules that may rewrite the plan and/or its expressions so
they execute more quickly while still computing the same result.

## Running the Optimizer

The following code demonstrates the basic flow of creating the optimizer with a default set of optimization rules
and applying it to a logical plan to produce an optimized logical plan.

```fixed

use std::sync::Arc;
use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerRule, OptimizerContext, Optimizer};

// We need a logical plan as the starting point. There are many ways to build a logical plan:
//
// The `datafusion-expr` crate provides a LogicalPlanBuilder
// The `datafusion-sql` crate provides a SQL query planner that can create a LogicalPlan from SQL
// The `datafusion` crate provides a DataFrame API that can create a LogicalPlan

let initial_logical_plan = LogicalPlanBuilder::empty(false).build().unwrap();

// use builtin rules or customized rules
let rules: Vec<Arc<dyn OptimizerRule + Send + Sync>> = vec![];

let optimizer = Optimizer::with_rules(rules);

let config = OptimizerContext::new().with_max_passes(16);

let optimized_plan = optimizer.optimize(initial_logical_plan.clone(), &config, observer);

fn observer(plan: &LogicalPlan, rule: &dyn OptimizerRule) {
    println!(
        "After applying rule '{}':\n{}",
        rule.name(),
        plan.display_indent()
    )
}
```


## Writing Optimization Rules

Please refer to the
[optimizer_rule.rs](../../../datafusion-examples/examples/optimizer_rule.rs)
example to learn more about the general approach to writing optimizer rules and
then move onto studying the existing rules.

`OptimizerRule` transforms one ['LogicalPlan'] into another which
computes the same results, but in a potentially more efficient
way. If there are no suitable transformations for the input plan,
the optimizer can simply return it as is.

All rules must implement the `OptimizerRule` trait.

```fixed
# use datafusion::common::tree_node::Transformed;
# use datafusion::common::Result;
# use datafusion::logical_expr::LogicalPlan;
# use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
#

#[derive(Default, Debug)]
struct MyOptimizerRule {}

impl OptimizerRule for MyOptimizerRule {
    fn name(&self) -> &str {
        "my_optimizer_rule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        unimplemented!()
    }
}
```

## Providing Custom Rules

The optimizer can be created with a custom set of rules.

```fixed
# use std::sync::Arc;
# use datafusion::logical_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
# use datafusion::optimizer::{OptimizerRule, OptimizerConfig, OptimizerContext, Optimizer};
# use datafusion::common::tree_node::Transformed;
# use datafusion::common::Result;
#
# #[derive(Default, Debug)]
# struct MyOptimizerRule {}
#
# impl OptimizerRule for MyOptimizerRule {
#     fn name(&self) -> &str {
#         "my_optimizer_rule"
#     }
#
#     fn rewrite(
#         &self,
#         plan: LogicalPlan,
#         _config: &dyn OptimizerConfig,
#     ) -> Result<Transformed<LogicalPlan>> {
#         unimplemented!()
#     }
# }

let optimizer = Optimizer::with_rules(vec![
    Arc::new(MyOptimizerRule {})
]);
```

### General Guidelines

Rules typical walk the logical plan and walk the expression trees inside operators and selectively mutate
individual operators or expressions.

Sometimes there is an initial pass that visits the plan and builds state that is used in a second pass that performs
the actual optimization. This approach is used in projection push down and filter push down.

### Expression Naming

Every expression in DataFusion has a name, which is used as the column name. For example, in this example the output
contains a single column with the name `"COUNT(aggregate_test_100.c9)"`:

```text
> select count(c9) from aggregate_test_100;
+------------------------------+
| COUNT(aggregate_test_100.c9) |
+------------------------------+
| 100                          |
+------------------------------+
```

These names are used to refer to the columns in both subqueries as well as internally from one stage of the LogicalPlan
to another. For example:

```text
> select "COUNT(aggregate_test_100.c9)" + 1 from (select count(c9) from aggregate_test_100) as sq;
+--------------------------------------------+
| sq.COUNT(aggregate_test_100.c9) + Int64(1) |
+--------------------------------------------+
| 101                                        |
+--------------------------------------------+
```

### Implication

Because DataFusion identifies columns using a string name, it means it is critical that the names of expressions are
not changed by the optimizer when it rewrites expressions. This is typically accomplished by renaming a rewritten
expression by adding an alias.

Here is a simple example of such a rewrite. The expression `1 + 2` can be internally simplified to 3 but must still be
displayed the same as `1 + 2`:

```text
> select 1 + 2;
+---------------------+
| Int64(1) + Int64(2) |
+---------------------+
| 3                   |
+---------------------+
```

Looking at the `EXPLAIN` output we can see that the optimizer has effectively rewritten `1 + 2` into effectively
`3 as "1 + 2"`:

```text
> explain select 1 + 2;
+---------------+-------------------------------------------------+
| plan_type     | plan                                            |
+---------------+-------------------------------------------------+
| logical_plan  | Projection: Int64(3) AS Int64(1) + Int64(2)     |
|               |   EmptyRelation                                 |
| physical_plan | ProjectionExec: expr=[3 as Int64(1) + Int64(2)] |
|               |   PlaceholderRowExec                            |
|               |                                                 |
+---------------+-------------------------------------------------+
```

If the expression name is not preserved, bugs such as [#3704](https://github.com/apache/datafusion/issues/3704)
and [#3555](https://github.com/apache/datafusion/issues/3555) occur where the expected columns can not be found.

### Building Expression Names

There are currently two ways to create a name for an expression in the logical plan.

```fixed
# use datafusion::common::Result;
# struct Expr;

impl Expr {
    /// Returns the name of this expression as it should appear in a schema. This name
    /// will not include any CAST expressions.
    pub fn display_name(&self) -> Result<String> {
        Ok("display_name".to_string())
    }

    /// Returns a full and complete string representation of this expression.
    pub fn canonical_name(&self) -> String {
        "canonical_name".to_string()
    }
}
```

When comparing expressions to determine if they are equivalent, `canonical_name` should be used, and when creating a
name to be used in a schema, `display_name` should be used.

### Utilities

There are a number of [utility methods][util] provided that take care of some common tasks.

[util]: https://github.com/apache/datafusion/blob/main/datafusion/expr/src/utils.rs

### Recursively walk an expression tree

Coming soon

### Rewriting expressions

Coming soon

### Optimize children


Coming soon

### Writing Tests

There should be unit tests in the same file as the new rule that test the effect of the rule being applied to a plan
in isolation (without any other rule being applied).

There should also be a test in `integration-tests.rs` that tests the rule as part of the overall optimization process.

### Debugging

The `EXPLAIN VERBOSE` command can be used to show the effect of each optimization rule on a query.

In the following example, the `type_coercion` and `simplify_expressions` passes have simplified the plan so that it returns the constant `"3.2"` rather than doing a computation at execution time.

```text
> explain verbose select cast(1 + 2.2 as string) as foo;
+------------------------------------------------------------+---------------------------------------------------------------------------+
| plan_type                                                  | plan                                                                      |
+------------------------------------------------------------+---------------------------------------------------------------------------+
| initial_logical_plan                                       | Projection: CAST(Int64(1) + Float64(2.2) AS Utf8) AS foo                  |
|                                                            |   EmptyRelation                                                           |
| logical_plan after type_coercion                           | Projection: CAST(CAST(Int64(1) AS Float64) + Float64(2.2) AS Utf8) AS foo |
|                                                            |   EmptyRelation                                                           |
| logical_plan after simplify_expressions                    | Projection: Utf8("3.2") AS foo                                            |
|                                                            |   EmptyRelation                                                           |
| logical_plan after unwrap_cast_in_comparison               | SAME TEXT AS ABOVE                                                        |
| logical_plan after decorrelate_where_exists                | SAME TEXT AS ABOVE                                                        |
| logical_plan after decorrelate_where_in                    | SAME TEXT AS ABOVE                                                        |
| logical_plan after scalar_subquery_to_join                 | SAME TEXT AS ABOVE                                                        |
| logical_plan after subquery_filter_to_join                 | SAME TEXT AS ABOVE                                                        |
| logical_plan after simplify_expressions                    | SAME TEXT AS ABOVE                                                        |
| logical_plan after eliminate_filter                        | SAME TEXT AS ABOVE                                                        |
| logical_plan after reduce_cross_join                       | SAME TEXT AS ABOVE                                                        |
| logical_plan after common_sub_expression_eliminate         | SAME TEXT AS ABOVE                                                        |
| logical_plan after eliminate_limit                         | SAME TEXT AS ABOVE                                                        |
| logical_plan after projection_push_down                    | SAME TEXT AS ABOVE                                                        |
| logical_plan after rewrite_disjunctive_predicate           | SAME TEXT AS ABOVE                                                        |
| logical_plan after reduce_outer_join                       | SAME TEXT AS ABOVE                                                        |
| logical_plan after filter_push_down                        | SAME TEXT AS ABOVE                                                        |
| logical_plan after limit_push_down                         | SAME TEXT AS ABOVE                                                        |
| logical_plan after single_distinct_aggregation_to_group_by | SAME TEXT AS ABOVE                                                        |
| logical_plan                                               | Projection: Utf8("3.2") AS foo                                            |
|                                                            |   EmptyRelation                                                           |
| initial_physical_plan                                      | ProjectionExec: expr=[3.2 as foo]                                         |
|                                                            |   PlaceholderRowExec                                                      |
|                                                            |                                                                           |
| physical_plan after aggregate_statistics                   | SAME TEXT AS ABOVE                                                        |
| physical_plan after join_selection                         | SAME TEXT AS ABOVE                                                        |
| physical_plan after coalesce_batches                       | SAME TEXT AS ABOVE                                                        |
| physical_plan after repartition                            | SAME TEXT AS ABOVE                                                        |
| physical_plan after add_merge_exec                         | SAME TEXT AS ABOVE                                                        |
| physical_plan                                              | ProjectionExec: expr=[3.2 as foo]                                         |
|                                                            |   PlaceholderRowExec                                                      |
|                                                            |                                                                           |
+------------------------------------------------------------+---------------------------------------------------------------------------+
```

[df]: https://crates.io/crates/datafusion
