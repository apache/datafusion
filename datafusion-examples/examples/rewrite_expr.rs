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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::expr_rewriter::{ExprRewritable, ExprRewriter};
use datafusion_expr::{
    AggregateUDF, Between, Expr, Filter, LogicalPlan, ScalarUDF, TableSource,
};
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{utils, OptimizerConfig, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::sync::Arc;

pub fn main() -> Result<()> {
    // produce a logical plan using the datafusion-sql crate
    let dialect = PostgreSqlDialect {};
    let sql = "SELECT * FROM person WHERE age BETWEEN 21 AND 32";
    let statements = Parser::parse_sql(&dialect, sql)?;

    // produce a logical plan using the datafusion-sql crate
    let context_provider = MyContextProvider {};
    let sql_to_rel = SqlToRel::new(&context_provider);
    let logical_plan = sql_to_rel.sql_statement_to_plan(statements[0].clone())?;
    println!(
        "Unoptimized Logical Plan:\n\n{}\n",
        logical_plan.display_indent()
    );

    // now run the optimizer with our custom rule
    let optimizer = Optimizer::with_rules(vec![Arc::new(MyRule {})]);
    let mut optimizer_config = OptimizerConfig::default().with_skip_failing_rules(false);
    let optimized_plan =
        optimizer.optimize(&logical_plan, &mut optimizer_config, observe)?;
    println!(
        "Optimized Logical Plan:\n\n{}\n",
        optimized_plan.display_indent()
    );

    Ok(())
}

fn observe(plan: &LogicalPlan, rule: &dyn OptimizerRule) {
    println!(
        "After applying rule '{}':\n{}\n",
        rule.name(),
        plan.display_indent()
    )
}

struct MyRule {}

impl OptimizerRule for MyRule {
    fn name(&self) -> &str {
        "my_rule"
    }

    fn optimize(
        &self,
        plan: &LogicalPlan,
        _config: &mut OptimizerConfig,
    ) -> Result<LogicalPlan> {
        // recurse down and optimize children first
        let plan = utils::optimize_children(self, plan, _config)?;

        match plan {
            LogicalPlan::Filter(filter) => {
                let mut expr_rewriter = MyExprRewriter {};
                let predicate = filter.predicate().clone();
                let predicate = predicate.rewrite(&mut expr_rewriter)?;
                Ok(LogicalPlan::Filter(Filter::try_new(
                    predicate,
                    filter.input().clone(),
                )?))
            }
            _ => Ok(plan.clone()),
        }
    }
}

struct MyExprRewriter {}

impl ExprRewriter for MyExprRewriter {
    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        match expr {
            Expr::Between(Between {
                expr,
                negated,
                low,
                high,
            }) => {
                let expr: Expr = expr.as_ref().clone();
                let low: Expr = low.as_ref().clone();
                let high: Expr = high.as_ref().clone();
                if negated {
                    Ok(expr.clone().lt(low).or(expr.gt(high)))
                } else {
                    Ok(expr.clone().gt_eq(low).and(expr.lt_eq(high)))
                }
            }
            _ => Ok(expr.clone()),
        }
    }
}

struct MyContextProvider {}

impl ContextProvider for MyContextProvider {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name.table() == "person" {
            Ok(Arc::new(MyTableSource {
                schema: Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::UInt8, false),
                ])),
            }))
        } else {
            Err(DataFusionError::Plan("table not found".to_string()))
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_config_option(
        &self,
        _variable: &str,
    ) -> Option<datafusion_common::ScalarValue> {
        None
    }
}

struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
