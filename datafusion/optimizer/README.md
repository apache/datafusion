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

[DataFusion](df) is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory
format.

DataFusion has modular design, allowing individual crates to be re-used in other projects.

This crate is a submodule of DataFusion that provides a query optimizer for logical plans.

## Running the Optimizer

The following code demonstrates the basic flow of creating the optimizer with a default set of optimization rules
and applying it to a logical plan to produce an optimized logical plan.

```rust

// We need a logical plan as the starting point. There are many ways to build a logical plan:
//
// The `datafusion-expr` crate provides a LogicalPlanBuilder
// The `datafusion-sql` crate provides a SQL query planner that can create a LogicalPlan from SQL
// The `datafusion` crate provides a DataFrame API that can create a LogicalPlan
let logical_plan = ...

let mut config = OptimizerConfig::default();
let optimizer = Optimizer::new(&config);
let optimized_plan = optimizer.optimize(&logical_plan, &mut config, observe)?;

fn observe(plan: &LogicalPlan, rule: &dyn OptimizerRule) {
    println!(
        "After applying rule '{}':\n{}",
        rule.name(),
        plan.display_indent()
    )
}
```

## Providing Custom Rules

The optimizer can be created with a custom set of rules.

```rust
let optimizer = Optimizer::with_rules(vec![
    Arc::new(MyRule {})
]);
```

## Writing Optimization Rules

Please refer to the [examples](examples) to learn more about the general approach to writing optimizer rules and
then move onto studying the existing rules.

All rules must implement the `OptimizerRule` trait.

```rust
/// `OptimizerRule` transforms one ['LogicalPlan'] into another which
/// computes the same results, but in a potentially more efficient
/// way.
pub trait OptimizerRule {
    /// Rewrite `plan` to an optimized form
    fn optimize(
        &self,
        plan: &LogicalPlan,
        optimizer_config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan>;

    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;
}
```

### General Guidelines

Rules typical walk the logical plan and walk the expression trees inside operators and selectively mutate
individual operators or expressions.

Sometimes there is an initial pass that visits the plan and builds state that is used in a second pass that performs
the actual optimization. This approach is used in projection push down and filter push down.

### Utilities

There are a number of utility methods provided that take care of some common tasks.

### ExprVisitor

The `ExprVisitor` and `ExprVisitable` traits provide a mechanism for applying a visitor pattern to an expression tree.

Here is an example that demonstrates this.

```rust
fn extract_subquery_filters(expression: &Expr, extracted: &mut Vec<Expr>) -> Result<()> {
    struct InSubqueryVisitor<'a> {
        accum: &'a mut Vec<Expr>,
    }

    impl ExpressionVisitor for InSubqueryVisitor<'_> {
        fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
            if let Expr::InSubquery { .. } = expr {
                self.accum.push(expr.to_owned());
            }
            Ok(Recursion::Continue(self))
        }
    }

    expression.accept(InSubqueryVisitor { accum: extracted })?;
    Ok(())
}
```

### Rewriting Expressions

The `MyExprRewriter` trait can be implemented to provide a way to rewrite expressions. This rule can then be applied
to an expression by calling `Expr::rewrite` (from the `ExprRewritable` trait).

The `rewrite` method will perform a depth first walk of the expression and its children to rewrite an expression,
consuming `self` producing a new expression.

```rust
let mut expr_rewriter = MyExprRewriter {};
let expr = expr.rewrite(&mut expr_rewriter)?;
```

Here is an example implementation which will rewrite `expr BETWEEN a AND b` as `expr >= a AND expr <= b`. Note that the
implementation does not need to perform any recursion since this is handled by the `rewrite` method.

```rust
struct MyExprRewriter {}

impl ExprRewriter for MyExprRewriter {
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::Between {
                negated,
                expr,
                low,
                high,
            } => {
                let expr: Expr = expr.as_ref().clone();
                let low: Expr = low.as_ref().clone();
                let high: Expr = high.as_ref().clone();
                if negated {
                    Ok(expr.clone().lt(low).or(expr.clone().gt(high)))
                } else {
                    Ok(expr.clone().gt_eq(low).and(expr.clone().lt_eq(high)))
                }
            }
            _ => Ok(expr.clone()),
        }
    }
}
```

### optimize_children

It is quite typical for a rule to be applied recursively to all operators within a query plan. Rather than duplicate
that logic in each rule, an `optimize_children` method is provided. This recursively invokes the `optimize` method on
the plan's children and then returns a node of the same type.

```rust
fn optimize(
    &self,
    plan: &LogicalPlan,
    _config: &mut OptimizerConfig,
) -> Result<LogicalPlan> {
    // recurse down and optimize children first
    let plan = utils::optimize_children(self, plan, _config)?;

    ...
}
```

### Writing Tests

There should be unit tests in the same file as the new rule that test the effect of the rule being applied to a plan
in isolation (without any other rule being applied).

There should also be a test in `integration-tests.rs` that tests the rule as part of the overall optimization process.

[df]: https://crates.io/crates/datafusion
