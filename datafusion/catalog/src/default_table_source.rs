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

//! Default TableSource implementation used in DataFusion physical plans

use std::sync::Arc;
use std::{any::Any, borrow::Cow};

use crate::{BatchedTableFunctionImpl, TableProvider};

use arrow::datatypes::SchemaRef;
use datafusion_common::{internal_err, Constraints};
use datafusion_expr::{
    BatchedTableFunctionSource, Expr, TableProviderFilterPushDown, TableSource, TableType,
};

/// Implements [`TableSource`] for a [`TableProvider`]
///
/// This structure adapts a [`TableProvider`] (a physical plan trait) to the
/// [`TableSource`] (logical plan trait).
///
/// It is used so logical plans in the `datafusion_expr` crate do not have a
/// direct dependency on physical plans, such as [`TableProvider`]s.
pub struct DefaultTableSource {
    /// table provider
    pub table_provider: Arc<dyn TableProvider>,
}

impl DefaultTableSource {
    /// Create a new DefaultTableSource to wrap a TableProvider
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self { table_provider }
    }
}

impl TableSource for DefaultTableSource {
    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        self.table_provider.schema()
    }

    /// Get a reference to applicable constraints, if any exists.
    fn constraints(&self) -> Option<&Constraints> {
        self.table_provider.constraints()
    }

    /// Get the type of this table for metadata/catalog purposes.
    fn table_type(&self) -> TableType {
        self.table_provider.table_type()
    }

    /// Tests whether the table provider can make use of any or all filter expressions
    /// to optimize data retrieval.
    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.table_provider.supports_filters_pushdown(filter)
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, datafusion_expr::LogicalPlan>> {
        self.table_provider.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.table_provider.get_column_default(column)
    }
}

/// Wrap TableProvider in TableSource
pub fn provider_as_source(
    table_provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableSource> {
    Arc::new(DefaultTableSource::new(table_provider))
}

/// Attempt to downcast a TableSource to DefaultTableSource and access the
/// TableProvider. This will only work with a TableSource created by DataFusion.
pub fn source_as_provider(
    source: &Arc<dyn TableSource>,
) -> datafusion_common::Result<Arc<dyn TableProvider>> {
    match source
        .as_ref()
        .as_any()
        .downcast_ref::<DefaultTableSource>()
    {
        Some(source) => Ok(Arc::clone(&source.table_provider)),
        _ => internal_err!("TableSource was not DefaultTableSource"),
    }
}

#[test]
fn preserves_table_type() {
    use async_trait::async_trait;
    use datafusion_common::DataFusionError;

    #[derive(Debug)]
    struct TestTempTable;

    #[async_trait]
    impl TableProvider for TestTempTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_type(&self) -> TableType {
            TableType::Temporary
        }

        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _: &dyn crate::Session,
            _: Option<&Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn datafusion_physical_plan::ExecutionPlan>, DataFusionError>
        {
            unimplemented!()
        }
    }

    let table_source = DefaultTableSource::new(Arc::new(TestTempTable));
    assert_eq!(table_source.table_type(), TableType::Temporary);
}

/// Implements [`BatchedTableFunctionSource`] for a [`BatchedTableFunctionImpl`]
///
/// This structure adapts a [`BatchedTableFunctionImpl`] (a physical plan trait) to the
/// [`BatchedTableFunctionSource`] (logical plan trait).
///
/// It is used so logical plans in the `datafusion_expr` crate do not have a
/// direct dependency on physical plans, such as [`BatchedTableFunctionImpl`]s.
///
/// This source is created per call site, storing the specific argument types
/// for that invocation. This allows the `schema()` method to correctly return
/// the output schema based on the actual argument types.
pub struct DefaultBatchedTableFunctionSource {
    /// batched table function implementation
    pub function_impl: Arc<dyn BatchedTableFunctionImpl>,
    /// The argument types for this specific call site
    pub arg_types: Vec<arrow::datatypes::DataType>,
}

impl DefaultBatchedTableFunctionSource {
    pub fn new(
        function_impl: Arc<dyn BatchedTableFunctionImpl>,
        arg_types: Vec<arrow::datatypes::DataType>,
    ) -> Self {
        Self {
            function_impl,
            arg_types,
        }
    }
}

impl BatchedTableFunctionSource for DefaultBatchedTableFunctionSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.function_impl.name()
    }

    fn schema(&self) -> SchemaRef {
        match self.function_impl.return_type(&self.arg_types) {
            Ok(schema) => Arc::new(schema),
            Err(_) => Arc::new(arrow::datatypes::Schema::empty()),
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion_common::Result<Vec<TableProviderFilterPushDown>> {
        self.function_impl.supports_filters_pushdown(filters)
    }
}
