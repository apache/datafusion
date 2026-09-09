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

//! [`EmptyTable`] useful for testing.

use futures::future::BoxFuture;
use std::sync::Arc;

use arrow::datatypes::*;
use datafusion_common::{Result, project_schema};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;

use crate::Session;
use crate::TableProvider;

/// An empty plan that is useful for testing and generating plans
/// without mapping them to actual data.
#[derive(Debug)]
pub struct EmptyTable {
    schema: SchemaRef,
    partitions: usize,
}

impl EmptyTable {
    /// Initialize a new `EmptyTable` from a schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            partitions: 1,
        }
    }

    /// Creates a new EmptyTable with specified partition number.
    pub fn with_partitions(mut self, partitions: usize) -> Self {
        self.partitions = partitions;
        self
    }
}
impl TableProvider for EmptyTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn scan<'a>(
        &'a self,
        _state: &'a dyn Session,
        projection: Option<&'a [usize]>,
        _filters: &'a [Expr],
        _limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(async move {
            // even though there is no data, projections apply
            let projected_schema = project_schema(&self.schema, projection)?;
            Ok(
                Arc::new(
                    EmptyExec::new(projected_schema).with_partitions(self.partitions),
                ) as Arc<dyn ExecutionPlan>,
            )
        })
    }
}
