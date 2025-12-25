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

//! [`MemorySchemaProvider`]: In-memory implementations of [`SchemaProvider`].

use crate::{SchemaProvider, TableFunction, TableProvider};
use async_trait::async_trait;
use dashmap::DashMap;
use datafusion_common::{DataFusionError, exec_err};
use std::any::Any;
use std::sync::Arc;

/// Simple in-memory implementation of a schema.
#[derive(Debug)]
pub struct MemorySchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
    table_functions: DashMap<String, Arc<TableFunction>>,
}

impl MemorySchemaProvider {
    /// Instantiates a new MemorySchemaProvider with an empty collection of tables.
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
            table_functions: DashMap::new(),
        }
    }
}

impl Default for MemorySchemaProvider {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SchemaProvider for MemorySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).map(|table| Arc::clone(table.value())))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return exec_err!("The table {name} already exists");
        }
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    fn udtf_names(&self) -> Vec<String> {
        self.table_functions
            .iter()
            .map(|f| f.key().clone())
            .collect()
    }

    fn udtf(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<TableFunction>>, DataFusionError> {
        Ok(self
            .table_functions
            .get(name)
            .map(|f| Arc::clone(f.value())))
    }

    fn register_udtf(
        &self,
        name: String,
        function: Arc<TableFunction>,
    ) -> datafusion_common::Result<Option<Arc<TableFunction>>> {
        if self.udtf_exist(name.as_str()) {
            return exec_err!("The table function {name} already exists");
        }
        Ok(self.table_functions.insert(name, function))
    }

    fn deregister_udtf(
        &self,
        name: &str,
    ) -> datafusion_common::Result<Option<Arc<TableFunction>>> {
        Ok(self.table_functions.remove(name).map(|(_, f)| f))
    }

    fn udtf_exist(&self, name: &str) -> bool {
        self.table_functions.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableFunctionImpl;
    use crate::Session;
    use arrow::datatypes::Schema;
    use datafusion_common::Result;
    use datafusion_expr::{Expr, TableType};
    use datafusion_physical_plan::ExecutionPlan;

    #[derive(Debug)]
    struct DummyTableFunc;

    #[derive(Debug)]
    struct DummyTable {
        schema: arrow::datatypes::SchemaRef,
    }

    #[async_trait::async_trait]
    impl TableProvider for DummyTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.schema.clone()
        }

        fn table_type(&self) -> TableType {
            TableType::Base
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            datafusion_common::plan_err!("DummyTable does not support scanning")
        }
    }

    impl TableFunctionImpl for DummyTableFunc {
        fn call(&self, _args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
            Ok(Arc::new(DummyTable {
                schema: Arc::new(Schema::empty()),
            }))
        }
    }

    #[test]
    fn test_register_and_retrieve_udtf() {
        let schema = MemorySchemaProvider::new();
        let func = Arc::new(TableFunction::new(
            "my_func".to_string(),
            Arc::new(DummyTableFunc),
        ));

        let result = schema.register_udtf("my_func".to_string(), func.clone());
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        assert!(schema.udtf_exist("my_func"));
        assert_eq!(schema.udtf_names(), vec!["my_func"]);

        let retrieved = schema.udtf("my_func").unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name(), "my_func");
    }

    #[test]
    fn test_duplicate_udtf_registration_fails() {
        let schema = MemorySchemaProvider::new();
        let func = Arc::new(TableFunction::new(
            "my_func".to_string(),
            Arc::new(DummyTableFunc),
        ));

        schema
            .register_udtf("my_func".to_string(), func.clone())
            .unwrap();

        let result = schema.register_udtf("my_func".to_string(), func.clone());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already exists"));
    }

    #[test]
    fn test_deregister_udtf() {
        let schema = MemorySchemaProvider::new();
        let func = Arc::new(TableFunction::new(
            "my_func".to_string(),
            Arc::new(DummyTableFunc),
        ));

        schema.register_udtf("my_func".to_string(), func).unwrap();
        assert!(schema.udtf_exist("my_func"));

        let removed = schema.deregister_udtf("my_func").unwrap();
        assert!(removed.is_some());
        assert!(!schema.udtf_exist("my_func"));
        assert_eq!(schema.udtf_names(), Vec::<String>::new());

        let removed = schema.deregister_udtf("my_func").unwrap();
        assert!(removed.is_none());
    }

    #[test]
    fn test_udtf_not_found() {
        let schema = MemorySchemaProvider::new();

        assert!(!schema.udtf_exist("nonexistent"));
        let result = schema.udtf("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_multiple_udtfs() {
        let schema = MemorySchemaProvider::new();
        let func1 = Arc::new(TableFunction::new(
            "func1".to_string(),
            Arc::new(DummyTableFunc),
        ));
        let func2 = Arc::new(TableFunction::new(
            "func2".to_string(),
            Arc::new(DummyTableFunc),
        ));

        schema.register_udtf("func1".to_string(), func1).unwrap();
        schema.register_udtf("func2".to_string(), func2).unwrap();

        let mut names = schema.udtf_names();
        names.sort();
        assert_eq!(names, vec!["func1", "func2"]);

        assert!(schema.udtf_exist("func1"));
        assert!(schema.udtf_exist("func2"));
    }
}
