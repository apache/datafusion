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

//! CteWorkTable implementation used for recursive queries

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::not_impl_err;

use crate::{
    error::Result,
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use datafusion_common::DataFusionError;

use crate::datasource::{TableProvider, TableType};
use crate::execution::context::SessionState;

/// TODO: add docs
pub struct CteWorkTable {
    name: String,
    table_schema: SchemaRef,
}

impl CteWorkTable {
    /// TODO: add doc
    pub fn new(name: &str, table_schema: SchemaRef) -> Self {
        Self {
            name: name.to_owned(),
            table_schema,
        }
    }
}

#[async_trait]
impl TableProvider for CteWorkTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        None
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("scan not implemented for CteWorkTable yet")
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        // TODO: should we support filter pushdown?
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
