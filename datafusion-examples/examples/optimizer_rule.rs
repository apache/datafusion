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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::DataType;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, Result, ScalarValue};
use datafusion_expr::{
    BinaryExpr, ColumnarValue, Expr, LogicalPlan, Operator, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};
use std::any::Any;
use std::sync::Arc;

/// This example demonstrates how to add your own [`OptimizerRule`]
/// to DataFusion.
///
/// [`OptimizerRule`]s transform [`LogicalPlan`]s into an equivalent (but
/// hopefully faster) form.
///
/// See [analyzer_rule.rs] for an example of AnalyzerRules, which are for
/// changing plan semantics.
#[tokio::main]
pub async fn main() -> Result<()> {
    // DataFusion includes many built in OptimizerRules for tasks such as outer
    // to inner join conversion and constant folding. To modify the list of
    // optimizer rules, we must use the lower level SessionState API
    let state = SessionContext::new().state();
    let state = state.add_optimizer_rule(Arc::new(MyOptimizerRule {}));

    // To plan and run queries with the new rule, create a SessionContext with
    // the modified SessionState
    let ctx = SessionContext::from(state);
    ctx.register_batch("person", person_batch())?;

    // Plan a SQL statement as normal
    let sql = "SELECT * FROM person WHERE age = 22";
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;

    println!("Logical Plan:\n\n{}\n", plan.display_indent());

    // We can see the effect of our rewrite on the output plan that the filter
    // has been rewritten to my_eq

    // Filter: my_eq(person.age, Int32(22))
    //   TableScan: person projection=[name, age]

    ctx.sql(sql).await?.show().await?;

    // And the output verifies the predicates have been changed (as the my_eq
    // always returns true)

    // +--------+-----+
    // | name   | age |
    // +--------+-----+
    // | Andy   | 11  |
    // | Andrew | 22  |
    // | Oleks  | 33  |
    // +--------+-----+

    // however we can see the rule doesn't trigger for queries with not equal
    // predicates
    ctx.sql("SELECT * FROM person WHERE age <> 22")
        .await?
        .show()
        .await?;

    // +-------+-----+
    // | name  | age |
    // +-------+-----+
    // | Andy  | 11  |
    // | Oleks | 33  |
    // +-------+-----+

    Ok(())
}

/// An example optimizer rule that looks for col = <const> and replaces it with
/// a user defined function
struct MyOptimizerRule {}

impl OptimizerRule for MyOptimizerRule {
    fn name(&self) -> &str {
        "my_optimizer_rule"
    }

    // New OptimizerRules should use the "rewrite" api as it is more efficient
    fn supports_rewrite(&self) -> bool {
        true
    }

    /// Ask the optimizer to handle the plan recursion. `rewrite` will be called
    /// on each plan node.
    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr| {
            // This closure is called for all expressions in the current plan
            //
            // For example, given a plan like `SELECT a + b, 5 + 10`
            //
            // The closure would be called twice, once for `a + b` and once for `5 + 10`
            self.rewrite_expr(expr)
        })
    }

    fn try_optimize(
        &self,
        _plan: &LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        // since this rule uses the rewrite API, return an error if the old API is called
        return internal_err!("Should have called rewrite");
    }
}

impl MyOptimizerRule {
    /// Rewrites an Expr replacing all `<col> = <const>` expressions with
    /// a call to my_eq udf
    fn rewrite_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
        // do a bottom up rewrite of the expression tree
        expr.transform_up(|expr| {
            // Closure called for each sub tree
            match expr {
                Expr::BinaryExpr(binary_expr) if is_binary_eq(&binary_expr) => {
                    // destruture the expression
                    let BinaryExpr { left, op: _, right } = binary_expr;
                    // rewrite to `my_eq(left, right)`
                    let udf = ScalarUDF::new_from_impl(MyEq::new());
                    let call = udf.call(vec![*left, *right]);
                    Ok(Transformed::yes(call))
                }
                _ => return Ok(Transformed::no(expr)),
            }
        })
        // Note that the TreeNode API handles propagating the transformed flag
        // and errors up the call chain
    }
}

/// return true of the expression is an equality expression for a literal or
/// column reference
fn is_binary_eq(binary_expr: &BinaryExpr) -> bool {
    binary_expr.op == Operator::Eq
        && is_lit_or_col(binary_expr.left.as_ref())
        && is_lit_or_col(binary_expr.right.as_ref())
}

/// Return true if the expression is a literal or column reference
fn is_lit_or_col(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(_) | Expr::Literal(_))
}

/// A simple user defined filter function
#[derive(Debug, Clone)]
struct MyEq {
    signature: Signature,
}

impl MyEq {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Stable),
        }
    }
}

impl ScalarUDFImpl for MyEq {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "my_eq"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke(&self, _args: &[ColumnarValue]) -> Result<ColumnarValue> {
        // this example simply returns "true" which is not what a real
        // implementation would do.
        return Ok(ColumnarValue::Scalar(ScalarValue::from(true)));
    }
}

/// Return a RecordBatch with made up data
fn person_batch() -> RecordBatch {
    let name: ArrayRef =
        Arc::new(StringArray::from_iter_values(["Andy", "Andrew", "Oleks"]));
    let age: ArrayRef = Arc::new(Int32Array::from(vec![11, 22, 33]));
    RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
}
