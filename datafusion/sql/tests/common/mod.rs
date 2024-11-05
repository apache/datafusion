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

use std::any::Any;
#[cfg(test)]
use std::collections::HashMap;
use std::fmt::Display;
use std::{sync::Arc, vec};

use arrow_schema::*;
use datafusion_common::config::ConfigOptions;
use datafusion_common::file_options::file_type::FileType;
use datafusion_common::{plan_err, GetExt, Result, TableReference};
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion_sql::planner::ContextProvider;

struct MockCsvType {}

impl GetExt for MockCsvType {
    fn get_ext(&self) -> String {
        "csv".to_string()
    }
}

impl FileType for MockCsvType {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for MockCsvType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_ext())
    }
}

#[derive(Default)]
pub(crate) struct MockSessionState {
    scalar_functions: HashMap<String, Arc<ScalarUDF>>,
    aggregate_functions: HashMap<String, Arc<AggregateUDF>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
    window_functions: HashMap<String, Arc<WindowUDF>>,
    pub config_options: ConfigOptions,
}

impl MockSessionState {
    pub fn with_expr_planner(mut self, expr_planner: Arc<dyn ExprPlanner>) -> Self {
        self.expr_planners.push(expr_planner);
        self
    }

    pub fn with_scalar_function(mut self, scalar_function: Arc<ScalarUDF>) -> Self {
        self.scalar_functions
            .insert(scalar_function.name().to_string(), scalar_function);
        self
    }

    pub fn with_aggregate_function(
        mut self,
        aggregate_function: Arc<AggregateUDF>,
    ) -> Self {
        // TODO: change to to_string() if all the function name is converted to lowercase
        self.aggregate_functions.insert(
            aggregate_function.name().to_string().to_lowercase(),
            aggregate_function,
        );
        self
    }

    pub fn with_window_function(mut self, window_function: Arc<WindowUDF>) -> Self {
        self.window_functions
            .insert(window_function.name().to_string(), window_function);
        self
    }
}

pub(crate) struct MockContextProvider {
    pub(crate) state: MockSessionState,
}

impl ContextProvider for MockContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let schema = match name.table() {
            "test" => Ok(Schema::new(vec![
                Field::new("t_date32", DataType::Date32, false),
                Field::new("t_date64", DataType::Date64, false),
            ])),
            "j1" => Ok(Schema::new(vec![
                Field::new("j1_id", DataType::Int32, false),
                Field::new("j1_string", DataType::Utf8, false),
            ])),
            "j2" => Ok(Schema::new(vec![
                Field::new("j2_id", DataType::Int32, false),
                Field::new("j2_string", DataType::Utf8, false),
            ])),
            "j3" => Ok(Schema::new(vec![
                Field::new("j3_id", DataType::Int32, false),
                Field::new("j3_string", DataType::Utf8, false),
            ])),
            "test_decimal" => Ok(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("price", DataType::Decimal128(10, 2), false),
            ])),
            "person" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("first_name", DataType::Utf8, false),
                Field::new("last_name", DataType::Utf8, false),
                Field::new("age", DataType::Int32, false),
                Field::new("state", DataType::Utf8, false),
                Field::new("salary", DataType::Float64, false),
                Field::new(
                    "birth_date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("😀", DataType::Int32, false),
            ])),
            "person_quoted_cols" => Ok(Schema::new(vec![
                Field::new("id", DataType::UInt32, false),
                Field::new("First Name", DataType::Utf8, false),
                Field::new("Last Name", DataType::Utf8, false),
                Field::new("Age", DataType::Int32, false),
                Field::new("State", DataType::Utf8, false),
                Field::new("Salary", DataType::Float64, false),
                Field::new(
                    "Birth Date",
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                ),
                Field::new("😀", DataType::Int32, false),
            ])),
            "orders" => Ok(Schema::new(vec![
                Field::new("order_id", DataType::UInt32, false),
                Field::new("customer_id", DataType::UInt32, false),
                Field::new("o_item_id", DataType::Utf8, false),
                Field::new("qty", DataType::Int32, false),
                Field::new("price", DataType::Float64, false),
                Field::new("delivered", DataType::Boolean, false),
            ])),
            "array" => Ok(Schema::new(vec![
                Field::new(
                    "left",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
                Field::new(
                    "right",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
            ])),
            "lineitem" => Ok(Schema::new(vec![
                Field::new("l_item_id", DataType::UInt32, false),
                Field::new("l_description", DataType::Utf8, false),
                Field::new("price", DataType::Float64, false),
            ])),
            "aggregate_test_100" => Ok(Schema::new(vec![
                Field::new("c1", DataType::Utf8, false),
                Field::new("c2", DataType::UInt32, false),
                Field::new("c3", DataType::Int8, false),
                Field::new("c4", DataType::Int16, false),
                Field::new("c5", DataType::Int32, false),
                Field::new("c6", DataType::Int64, false),
                Field::new("c7", DataType::UInt8, false),
                Field::new("c8", DataType::UInt16, false),
                Field::new("c9", DataType::UInt32, false),
                Field::new("c10", DataType::UInt64, false),
                Field::new("c11", DataType::Float32, false),
                Field::new("c12", DataType::Float64, false),
                Field::new("c13", DataType::Utf8, false),
            ])),
            "UPPERCASE_test" => Ok(Schema::new(vec![
                Field::new("Id", DataType::UInt32, false),
                Field::new("lower", DataType::UInt32, false),
            ])),
            "unnest_table" => Ok(Schema::new(vec![
                Field::new(
                    "array_col",
                    DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                    false,
                ),
                Field::new(
                    "struct_col",
                    DataType::Struct(Fields::from(vec![
                        Field::new("field1", DataType::Int64, true),
                        Field::new("field2", DataType::Utf8, true),
                    ])),
                    false,
                ),
            ])),
            _ => plan_err!("No table named: {} found", name.table()),
        };

        match schema {
            Ok(t) => Ok(Arc::new(EmptyTable::new(Arc::new(t)))),
            Err(e) => Err(e),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions.get(name).cloned()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions.get(name).cloned()
    }

    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        unimplemented!()
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions.get(name).cloned()
    }

    fn options(&self) -> &ConfigOptions {
        &self.state.config_options
    }

    fn get_file_type(&self, _ext: &str) -> Result<Arc<dyn FileType>> {
        Ok(Arc::new(MockCsvType {}))
    }

    fn create_cte_work_table(
        &self,
        _name: &str,
        schema: SchemaRef,
    ) -> Result<Arc<dyn TableSource>> {
        Ok(Arc::new(EmptyTable::new(schema)))
    }

    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions.keys().cloned().collect()
    }

    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions.keys().cloned().collect()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.state.expr_planners
    }
}

struct EmptyTable {
    table_schema: SchemaRef,
}

impl EmptyTable {
    fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl TableSource for EmptyTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }
}
