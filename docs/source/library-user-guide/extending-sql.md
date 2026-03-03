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

# Extending SQL Syntax

DataFusion provides a flexible extension system that allows you to customize SQL
parsing and planning without modifying the core codebase. This is useful when you
need to:

- Support custom operators from other SQL dialects (e.g., PostgreSQL's `->` for JSON)
- Add custom data types not natively supported
- Implement SQL constructs like `TABLESAMPLE`, `PIVOT`/`UNPIVOT`, or `MATCH_RECOGNIZE`

You can read more about this topic in the [Extending SQL in DataFusion: from ->>
to TABLESAMPLE] blog.

[extending sql in datafusion: from ->> to tablesample]: https://datafusion.apache.org/blog/2026/01/12/extending-sql

## Architecture Overview

When DataFusion processes a SQL query, it goes through these stages:

```text
┌─────────────┐    ┌─────────┐    ┌──────────────────────┐    ┌─────────────┐
│ SQL String  │───▶│ Parser  │───▶│      SqlToRel        │───▶│ LogicalPlan │
└─────────────┘    └─────────┘    │ (SQL to LogicalPlan) │    └─────────────┘
                                  └──────────────────────┘
                                              │
                                              │ uses
                                              ▼
                                  ┌───────────────────────┐
                                  │  Extension Planners   │
                                  │  • ExprPlanner        │
                                  │  • TypePlanner        │
                                  │  • RelationPlanner    │
                                  └───────────────────────┘
```

The extension planners intercept specific parts of the SQL AST during the
`SqlToRel` phase and allow you to customize how they are converted to DataFusion's
logical plan.

## Extension Points

DataFusion provides three planner traits for extending SQL:

| Trait               | Purpose                                 | Registration Method                        |
| ------------------- | --------------------------------------- | ------------------------------------------ |
| [`ExprPlanner`]     | Custom expressions and operators        | `ctx.register_expr_planner()`              |
| [`TypePlanner`]     | Custom SQL data types                   | `SessionStateBuilder::with_type_planner()` |
| [`RelationPlanner`] | Custom FROM clause elements (relations) | `ctx.register_relation_planner()`          |

**Planner Precedence**: Multiple [`ExprPlanner`]s and [`RelationPlanner`]s can be
registered; they are invoked in reverse registration order (last registered wins).
Return `Original(...)` to delegate to the next planner. Only one `TypePlanner`
can be active at a time.

### ExprPlanner: Custom Expressions and Operators

Use [`ExprPlanner`] to customize how SQL expressions are converted to DataFusion
logical expressions. This is useful for:

- Custom binary operators (e.g., `->`, `->>`, `@>`, `?`)
- Custom field access patterns
- Custom aggregate or window function handling

#### Available Methods

| Category           | Methods                                                                            |
| ------------------ | ---------------------------------------------------------------------------------- |
| Operators          | `plan_binary_op`, `plan_any`                                                       |
| Literals           | `plan_array_literal`, `plan_dictionary_literal`, `plan_struct_literal`             |
| Functions          | `plan_extract`, `plan_substring`, `plan_overlay`, `plan_position`, `plan_make_map` |
| Identifiers        | `plan_field_access`, `plan_compound_identifier`                                    |
| Aggregates/Windows | `plan_aggregate`, `plan_window`                                                    |

See the [ExprPlanner API documentation] for full method signatures.

#### Example: Custom Arrow Operator

This example maps the `->` operator to string concatenation:

```rust
# use std::sync::Arc;
# use datafusion::common::DFSchema;
# use datafusion::error::Result;
# use datafusion::logical_expr::Operator;
# use datafusion::prelude::*;
# use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
# use datafusion_expr::BinaryExpr;

#[derive(Debug)]
struct MyCustomPlanner;

impl ExprPlanner for MyCustomPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        match &expr.op {
            // Map `->` to string concatenation
            BinaryOperator::Arrow => {
                Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(expr.left.clone()),
                    right: Box::new(expr.right.clone()),
                    op: Operator::StringConcat,
                })))
            }
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Use postgres dialect to enable `->` operator parsing
    let config = SessionConfig::new()
        .set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);

    // Register the custom planner
    ctx.register_expr_planner(Arc::new(MyCustomPlanner))?;

    // Now `->` works as string concatenation
    let results = ctx.sql("SELECT 'hello'->'world'").await?.collect().await?;
    // Returns: "helloworld"
    Ok(())
}
```

For more details, see the [ExprPlanner API documentation] and the
[expr_planner test examples].

### TypePlanner: Custom Data Types

Use [`TypePlanner`] to map SQL data types to Arrow/DataFusion types. This is useful
when you need to support SQL types that aren't natively recognized.

#### Example: Custom DATETIME Type

```rust
# use std::sync::Arc;
# use arrow::datatypes::{DataType, TimeUnit};
# use datafusion::error::Result;
# use datafusion::prelude::*;
# use datafusion::execution::SessionStateBuilder;
use datafusion_expr::planner::TypePlanner;
# use sqlparser::ast;

#[derive(Debug)]
struct MyTypePlanner;

impl TypePlanner for MyTypePlanner {
    fn plan_type(&self, sql_type: &ast::DataType) -> Result<Option<DataType>> {
        match sql_type {
            // Map DATETIME(precision) to Arrow Timestamp
            ast::DataType::Datetime(precision) => {
                let time_unit = match precision {
                    Some(0) => TimeUnit::Second,
                    Some(3) => TimeUnit::Millisecond,
                    Some(6) => TimeUnit::Microsecond,
                    None | Some(9) => TimeUnit::Nanosecond,
                    _ => return Ok(None), // Let default handling take over
                };
                Ok(Some(DataType::Timestamp(time_unit, None)))
            }
            _ => Ok(None), // Return None for types we don't handle
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_type_planner(Arc::new(MyTypePlanner))
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Now DATETIME type is recognized
    ctx.sql("CREATE TABLE events (ts DATETIME(3))").await?;
    Ok(())
}
```

For more details, see the [TypePlanner API documentation].

### RelationPlanner: Custom FROM Clause Elements

Use [`RelationPlanner`] to handle custom relations in the FROM clause. This
enables you to implement SQL constructs like:

- `TABLESAMPLE` for sampling data
- `PIVOT` / `UNPIVOT` for data reshaping
- `MATCH_RECOGNIZE` for pattern matching
- Any custom relation syntax parsed by sqlparser

#### The RelationPlannerContext

When implementing [`RelationPlanner`], you receive a [`RelationPlannerContext`] that
provides utilities for planning:

| Method                      | Purpose                                         |
| --------------------------- | ----------------------------------------------- |
| `plan(relation)`            | Recursively plan a nested relation              |
| `sql_to_expr(expr, schema)` | Convert SQL expression to DataFusion Expr       |
| `context_provider()`        | Access session configuration, tables, functions |

See the [RelationPlanner API documentation] for additional methods like
`normalize_ident()` and `object_name_to_table_reference()`.

#### Implementation Strategies

There are two main approaches when implementing a [`RelationPlanner`]:

1. **Rewrite to Standard SQL**: Transform custom syntax into equivalent standard
   operations that DataFusion already knows how to execute (e.g., PIVOT → GROUP BY
   with CASE expressions). This is the simplest approach when possible.

2. **Custom Logical and Physical Nodes**: Create a [`UserDefinedLogicalNode`] to
   represent the operation in the logical plan, along with a custom [`ExecutionPlan`]
   to execute it. Both are required for end-to-end execution.

#### Example: Basic RelationPlanner Structure

```rust
# use std::sync::Arc;
# use datafusion::error::Result;
# use datafusion::prelude::*;
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct MyRelationPlanner;

impl RelationPlanner for MyRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        ctx: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            // Handle your custom relation
            TableFactor::Pivot { table, alias, .. } => {
                // Plan the input table
                let input = ctx.plan(*table)?;

                // Transform or wrap the plan as needed
                // ...

                Ok(RelationPlanning::Planned(PlannedRelation::new(input, alias)))
            }

            // Return Original for relations you don't handle
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register the custom planner
    ctx.register_relation_planner(Arc::new(MyRelationPlanner))?;

    Ok(())
}
```

## Complete Examples

The DataFusion repository includes comprehensive examples demonstrating each
approach:

### TABLESAMPLE (Custom Logical and Physical Nodes)

The [table_sample.rs] example shows a complete end-to-end implementation of how to
support queries such as:

```sql
SELECT * FROM table TABLESAMPLE BERNOULLI(10 PERCENT) REPEATABLE(42)
```

### PIVOT/UNPIVOT (Rewrite Strategy)

The [pivot_unpivot.rs] example demonstrates rewriting custom syntax to standard SQL
for queries such as:

```sql
SELECT * FROM sales
  PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
```

## Recap

1. Use [`ExprPlanner`] for custom operators and expression handling
2. Use [`TypePlanner` for custom SQL data types
3. Use [`RelationPlanner`] for custom FROM clause syntax (TABLESAMPLE, PIVOT, etc.)
4. Register planners via [`SessionContext`] or [`SessionStateBuilder`]

## See Also

- API Documentation: [`ExprPlanner`], [`TypePlanner`], [`RelationPlanner`]
- [relation_planner examples] - Complete TABLESAMPLE, PIVOT/UNPIVOT implementations
- [expr_planner test examples] - Custom operator examples
- [Custom Expression Planning](functions/adding-udfs.md#custom-expression-planning) in the UDF guide

[`exprplanner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.ExprPlanner.html
[`typeplanner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.TypePlanner.html
[`relationplanner`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlanner.html
[`userdefinedlogicalnode`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNode.html
[`executionplan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`sessioncontext`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html
[`sessionstatebuilder`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html
[`relationplannercontext`]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlannerContext.html
[exprplanner api documentation]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.ExprPlanner.html
[typeplanner api documentation]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.TypePlanner.html
[relationplanner api documentation]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlanner.html
[expr_planner test examples]: https://github.com/apache/datafusion/blob/main/datafusion/core/tests/user_defined/expr_planner.rs
[relation_planner examples]: https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/relation_planner
[table_sample.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/relation_planner/table_sample.rs
[pivot_unpivot.rs]: https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/relation_planner/pivot_unpivot.rs
