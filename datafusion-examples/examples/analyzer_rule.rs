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
use datafusion::prelude::SessionContext;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{lit, Expr, LogicalPlan};
use datafusion_optimizer::analyzer::AnalyzerRule;
use std::sync::Arc;

/// This example demonstrates how to add your own [`AnalyzerRule`]
/// to DataFusion.
///
/// [`AnalyzerRule`]s transform [`LogicalPlan`]s prior to the rest of the
/// DataFusion optimization process, and are allowed to change the plan's
/// semantics (e.g. output types).
///
#[tokio::main]
pub async fn main() -> Result<()> {
    // DataFusion includes several built in AnalyzerRules for tasks such as type
    // coercion. To modify the list of rules, we must use the lower level
    // SessionState API
    let state = SessionContext::new().state();
    let state = state.add_analyzer_rule(Arc::new(MyAnalyzerRule {}));

    // To plan and run queries with the new rule, create a SessionContext with
    // the modified SessionState
    let ctx = SessionContext::from(state);
    ctx.register_batch("person", person_batch())?;

    // Plan a SQL statement as normal
    let sql = "SELECT * FROM person WHERE age BETWEEN 21 AND 32";
    let plan = ctx.sql(sql).await?.into_optimized_plan()?;

    println!("Logical Plan:\n\n{}\n", plan.display_indent());

    // We can see the effect of our rewrite on the output plan. Even though the
    // input query was between 21 and 32, the plan is between 31 and 42

    // Filter: person.age >= Int32(31) AND person.age <= Int32(42)
    //   TableScan: person projection=[name, age]

    ctx.sql(sql).await?.show().await?;

    // And the output verifies the predicates have been changed

    // +-------+-----+
    // | name  | age |
    // +-------+-----+
    // | Oleks | 33  |
    // +-------+-----+

    Ok(())
}

/// An example analyzer rule that changes adds 10 to all Int64 literals in the plan
struct MyAnalyzerRule {}

impl AnalyzerRule for MyAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // use the TreeNode API to recursively walk the LogicalPlan tree
        // and all of its children (inputs)
        plan.transform(|plan| {
            // This closure is called for each LogicalPlan node
            plan.map_expressions(|expr| {
                // This closure is called for all expressions in the current plan
                //
                // For example, given a plan like `SELECT a + b, 5 + 10`
                //
                // The closure would be called twice, once for `a + b` and once for `5 + 10`
                self.rewrite_expr(expr)
            })
        })
        // the result of calling transform is a `Transformed` structure that
        // contains a flag signalling if any rewrite took place as well as
        // if the recursion stopped early.
        //
        // This example does not need either of that information, so simply
        // extract the LogicalPlan "data"
        .data()
    }

    fn name(&self) -> &str {
        "my_analyzer_rule"
    }
}

impl MyAnalyzerRule {
    /// rewrites an idividual expression
    fn rewrite_expr(&self, expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform(|expr| {
            // closure is invoked for all sub expressions

            // Transformed is used to transfer the "was this rewritten"
            // information back up the stack.
            if let Expr::Literal(ScalarValue::Int64(Some(i))) = expr {
                Ok(Transformed::yes(lit(i + 10)))
            } else {
                Ok(Transformed::no(expr))
            }
        })
    }
}

/// Return a RecordBatch with made up date
fn person_batch() -> RecordBatch {
    let name: ArrayRef =
        Arc::new(StringArray::from_iter_values(["Andy", "Andrew", "Oleks"]));
    let age: ArrayRef = Arc::new(Int32Array::from(vec![11, 22, 33]));
    RecordBatch::try_from_iter(vec![("name", name), ("age", age)]).unwrap()
}
