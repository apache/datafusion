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
use datafusion_common::config::ConfigOptions;
use datafusion_common::{plan_err, Result};
use datafusion_expr::{AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF};
use datafusion_optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion_sql::sqlparser::parser::Parser;
use datafusion_sql::TableReference;
use std::any::Any;
use std::sync::Arc;

/// This example shows how to use DataFusion's SQL planner to parse SQL text and
/// build `LogicalPlan`s without executing them.
///
/// For example, if you need a SQL planner and optimizer like Apache Calcite, but
/// do not want a Java runtime dependency for some reason, you can use
/// DataFusion as a SQL frontend.
pub fn main() -> Result<()> {
    // Normally, users interact with DataFusion via SessionContext. However,
    // using SessionContext requires depending on the full `datafusion` crate.
    //
    // In this example, we demonstrate how to use the lower level APIs directly,
    // which only requires the `datafusion-sql` dependencies.

    // First, we parse the SQL string. Note that we use the DataFusion
    // Parser, which wraps the `sqlparser-rs` SQL parser and adds DataFusion
    // specific syntax such as `CREATE EXTERNAL TABLE`
    let dialect = PostgreSqlDialect {};
    let sql = "SELECT * FROM person WHERE age BETWEEN 21 AND 32";
    let statements = Parser::parse_sql(&dialect, sql)?;

    // Now, use DataFusion's SQL planner, called `SqlToRel` to create a
    // `LogicalPlan` from the parsed statement
    //
    // To invoke SqlToRel we must provide it schema and function information
    // via an object that implements the `ContextProvider` trait
    let context_provider = MyContextProvider::default();
    let sql_to_rel = SqlToRel::new(&context_provider);
    let logical_plan = sql_to_rel.sql_statement_to_plan(statements[0].clone())?;
    println!(
        "Unoptimized Logical Plan:\n\n{}\n",
        logical_plan.display_indent()
    );

    // Projection: person.name, person.age
    //   Filter: person.age BETWEEN Int64(21) AND Int64(32)
    //     TableScan: person

    // The initial plan is a mechanical translation from the parsed SQL and
    // often can not run. In this example, `person.age` is actually a different
    // data type (Int32) than the values to which it is compared to which are
    // Int64. Most execution engines, including DataFusion's, will fail if you
    // provide such a plan.
    //
    // To prepare it to run, we must apply type coercion to align types, and
    // check for other semantic errors. In DataFusion this is done by a
    // component called the Analyzer.
    let config = OptimizerContext::default().with_skip_failing_rules(false);
    let analyzed_plan =Analyzer::new()
        .execute_and_check(logical_plan, config.options(), observe_analyzer)?;
    println!(
        "Analyzed Logical Plan:\n\n{}\n",
        analyzed_plan.display_indent()
    );

    // Finally we must invoke the DataFusion optimizer to improve the plans
    // performance by applying various rewrite rules.
    let optimized_plan = Optimizer::new().optimize(analyzed_plan, &config, observe_optimizer)?;
    println!(
        "Optimized Logical Plan:\n\n{}\n",
        optimized_plan.display_indent()
    );

    Ok(())
}

// Both the optimizer and the analyzer take a callback, called an "observer"
// that is invoked after each pass. We do not do anything with these callbacks
// in this example

fn observe_analyzer(_plan: &LogicalPlan, _rule: &dyn AnalyzerRule) {
}
fn observe_optimizer(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {
}


/// Implements the `ContextProvider` trait required to plan SQL
#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name.table() == "person" {
            Ok(Arc::new(MyTableSource {
                schema: Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, false),
                    Field::new("age", DataType::UInt8, false),
                ])),
            }))
        } else {
            plan_err!("table not found")
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

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// TableSource is the part of TableProvider needed for creating a LogicalPlan.
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
