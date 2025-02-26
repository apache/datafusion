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

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::{config::ConfigOptions, plan_err};
use datafusion_expr::{planner::ExprPlanner, AggregateUDF, LogicalTableSource, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::{planner::{ContextProvider, ParserOptions, SqlToRel}, TableReference};
use sqlparser::{dialect::GenericDialect, parser::Parser};

fn main() {
    let sql = "SELECT 2.0 + 2";

    // configure the parser
    let options = ParserOptions::new()
        .with_enable_ident_normalization(true)
        .with_parse_float_as_decimal(false);

    // parse the SQL
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];
    
    // create a logical query plan
    let context_provider = MyContextProvider::new();
    let sql_to_rel = SqlToRel::new_with_options(&context_provider, options);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    println!("{plan}");
}

struct MyContextProvider {
    options: ConfigOptions,
    tables: HashMap<String, Arc<dyn TableSource>>,
    udafs: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

impl MyContextProvider {
    fn with_udaf(mut self, udaf: Arc<AggregateUDF>) -> Self {
        self.udafs.insert(udaf.name().to_string(), udaf);
        self
    }

    fn with_expr_planner(mut self, planner: Arc<dyn ExprPlanner>) -> Self {
        self.expr_planners.push(planner);
        self
    }

    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            options: Default::default(),
            udafs: Default::default(),
            expr_planners: vec![],
        }
    }
}

fn create_table_source(fields: Vec<Field>) -> Arc<dyn TableSource> {
    Arc::new(LogicalTableSource::new(Arc::new(
        Schema::new_with_metadata(fields, HashMap::new()),
    )))
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> datafusion_common::Result<Arc<dyn TableSource>> {
        match self.tables.get(name.table()) {
            Some(table) => Ok(Arc::clone(table)),
            _ => plan_err!("Table not found: {}", name.table()),
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.udafs.get(name).cloned()
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

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
    
    fn get_file_type(&self, _ext: &str) -> datafusion_common::Result<Arc<dyn datafusion_common::file_options::file_type::FileType>> {
        datafusion_common::not_impl_err!("Registered file types are not supported")
    }
    
    fn get_table_function_source(
        &self,
        _name: &str,
        _args: Vec<datafusion_expr::Expr>,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        datafusion_common::not_impl_err!("Table Functions are not supported")
    }
    
    fn create_cte_work_table(
        &self,
        _name: &str,
        _schema: arrow::datatypes::SchemaRef,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        datafusion_common::not_impl_err!("Recursive CTE is not implemented")
    }
    
    fn get_type_planner(&self) -> Option<Arc<dyn datafusion_expr::planner::TypePlanner>> {
        None
    }
}
