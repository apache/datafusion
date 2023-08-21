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

# Working with Exprs

<!-- https://github.com/apache/arrow-datafusion/issues/7304 -->

`Expr` is short for "expression". It is a core abstraction in DataFusion for representing a computation.

For example, the SQL expression `a + b` would be represented as an `Expr` with a `BinaryExpr` variant. A `BinaryExpr` has a left and right `Expr` and an operator.

As another example, the SQL expression `a + b * c` would be represented as an `Expr` with a `BinaryExpr` variant. The left `Expr` would be `a` and the right `Expr` would be another `BinaryExpr` with a left `Expr` of `b` and a right `Expr` of `c`.

As the writer of a library, you may want to use or create `Expr`s to represent computations that you want to perform. This guide will walk you through how to make your own scalar UDF as an `Expr` and how to rewrite `Expr`s to inline the simple UDF.

## A Scalar UDF Example

Let's start by creating our own `Expr` in the form of a Scalar UDF. This UDF will simply add 1 to the input value.

```rust
pub fn add_one(args: &[ArrayRef]) -> Result<ArrayRef> {
    let i32s = as_int64_array(&args[0])?;

    let array = i32s
        .iter()
        .map(|sequence| match sequence {
            Some(value) => Some(value + 1),
            None => None,
        })
        .collect::<Int64Array>();

    Ok(Arc::new(array))
}
```

And our `ScalarUDF` would look like this. Please see the section on [adding UDFs](./adding-udfs.md) for more information on how to create a `ScalarUDF`.

```rust
let add_one = create_udf(
    "add_one",
    vec![DataType::Int64],
    Arc::new(DataType::Int64),
    Volatility::Immutable,
    make_scalar_function(add_one),
);
```

## Creating Exprs Programmatically

In addition to SQL strings, you can create `Expr`s programatically. This is common if you're working with a DataFrame vs. a SQL string. A simple example is:

```rust
use datafusion::prelude::*;

let expr = lit(5) + lit(5);
```

This is obviously a very simple example, but it shows how you can create an `Expr` from a literal value and sets us up later for how to simplify `Expr`s. You can also create `Expr`s from column references and operators:

```rust
use datafusion::prelude::*;

let expr = col("a") + col("b");
```

In fact, the `add_one` we created earlier is also an `Expr`. And because it's so simple, we'll use it as fodder for how to rewrite `Expr`s.

## Rewriting Exprs

Rewriting Expressions is the process of taking an `Expr` and transforming it into another `Expr`. This is useful for a number of reasons, including:

- Simplifying `Expr`s to make them easier to evaluate
- Optimizing `Expr`s to make them faster to evaluate
- Converting `Expr`s to other forms, e.g. converting a `BinaryExpr` to a `CastExpr`

In our example, we'll use rewriting to update our `add_one` UDF, to be rewritten as a `BinaryExpr` with a `Literal` of 1. We're effectively inlining the UDF.

### Rewriting with `transform`

To implement the inlining, we'll need to write a function that takes an `Expr` and returns a `Result<Expr>`. If the expression is _not_ to be rewritten `Transformed::No` is used to wrap the original `Expr`. If the expression _is_ to be rewritten, `Transformed::Yes` is used to wrap the new `Expr`.

```rust
fn rewrite_add_one(expr: Expr) -> Result<Expr> {
    expr.transform(&|expr| {
        Ok(match expr {
            Expr::ScalarUDF(scalar_fun) => {
                // rewrite the expression if the function is "add_one", otherwise return the original expression
                if scalar_fun.fun.name == "add_one" {
                    let input_arg = scalar_fun.args[0].clone();

                    let new_expression = BinaryExpr::new(
                        Box::new(input_arg),
                        datafusion::logical_expr::Operator::Plus,
                        Box::new(Expr::Literal(datafusion::scalar::ScalarValue::Int64(Some(
                            1,
                        )))),
                    );

                    Transformed::Yes(Expr::BinaryExpr(new_expression))
                } else {
                    // a scalar function that is not "add_one" is not rewritten
                    Transformed::No(Expr::ScalarUDF(scalar_fun))
                }
            }
            _ => Transformed::No(expr),  // not a scalar function, so not rewritten
        })
    })
}
```

### Creating an `OptimizerRule`

In DataFusion, an `OptimizerRule` is a trait that supports rewriting `Expr`s. It follows DataFusion's general mantra of trait implementations to drive behavior.

We'll call our rule `AddOneInliner` and implement the `OptimizerRule` trait. The `OptimizerRule` trait has two methods:

- `name` - returns the name of the rule
- `try_optimize` - takes a `LogicalPlan` and returns an `Option<LogicalPlan>`. If the rule is able to optimize the plan, it returns `Some(LogicalPlan)` with the optimized plan. If the rule is not able to optimize the plan, it returns `None`.

```rust
struct AddOneInliner {}

impl OptimizerRule for AddOneInliner {
    fn name(&self) -> &str {
        "add_one_inliner"
    }

    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // recurse down and optimize children first
        let optimized_plan = utils::optimize_children(self, plan, config)?;

        match optimized_plan {
            Some(LogicalPlan::Projection(projection)) => {
                let proj_expression = projection
                    .expr
                    .iter()
                    .map(|expr| rewrite_add_one(expr.clone()))
                    .collect::<Result<Vec<_>>>()?;

                let proj = Projection::try_new(proj_expression, projection.input)?;

                Ok(Some(LogicalPlan::Projection(proj)))
            }
            Some(optimized_plan) => Ok(Some(optimized_plan)),
            None => match plan {
                LogicalPlan::Projection(projection) => {
                    let proj_expression = projection
                        .expr
                        .iter()
                        .map(|expr| rewrite_add_one(expr.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    let proj = Projection::try_new(proj_expression, projection.input.clone())?;

                    Ok(Some(LogicalPlan::Projection(proj)))
                }
                _ => Ok(None),
            },
        }
    }
}
```

Note the use of `rewrite_add_one` which is mapped over the `expr` of the `Projection`. This is the function we wrote earlier that takes an `Expr` and returns a `Result<Expr>`.

We're almost there. Let's just test our rule works properly.

## Testing the Rule

Testing the rule is fairly simple, we can create a SessionState with our rule and then create a DataFrame and run a query. The logical plan will be optimized by our rule.

```rust
use datafusion::prelude::*;

let rules = Arc::new(AddOneInliner {});
let state = ctx.state().with_optimizer_rules(vec![rules]);

let ctx = SessionContext::with_state(state);
ctx.register_udf(add_one);

let sql = "SELECT add_one(1) AS added_one";
let plan = ctx.sql(sql).await?.logical_plan();

println!("{:?}", plan);
```

This results in the following output:

```text
Projection: Int64(1) + Int64(1) AS added_one
  EmptyRelation
```

I.e. the `add_one` UDF has been inlined into the projection.

## Conclusion

In this guide, we've seen how to create `Expr`s programmatically and how to rewrite them. This is useful for simplifying and optimizing `Expr`s. We've also seen how to test our rule to ensure it works properly.
