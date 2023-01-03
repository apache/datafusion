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

use arrow::datatypes::DataType;
use datafusion_common::{ScalarValue, TableReference};
use datafusion_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion_sql::planner::ContextProvider;
use std::sync::Arc;

pub struct LogicTestContextProvider {}

// Only a mock, don't need to implement
impl ContextProvider for LogicTestContextProvider {
    fn get_table_provider(
        &self,
        _name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        todo!()
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        todo!()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        todo!()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        todo!()
    }

    fn get_config_option(&self, _variable: &str) -> Option<ScalarValue> {
        todo!()
    }
}
