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
use std::borrow::Cow;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::error::Result;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::work_table::WorkTableExec;

use crate::{ScanArgs, ScanResult, Session, TableProvider};

/// The temporary working table where the previous iteration of a recursive query is stored
/// Naming is based on PostgreSQL's implementation.
/// See here for more details: www.postgresql.org/docs/11/queries-with.html#id-1.5.6.12.5.4
#[derive(Debug)]
pub struct CteWorkTable {
    /// The name of the CTE work table
    name: String,
    /// This schema must be shared across both the static and recursive terms of a recursive query
    table_schema: SchemaRef,
}

impl CteWorkTable {
    /// construct a new CteWorkTable with the given name and schema
    /// This schema must match the schema of the recursive term of the query
    /// Since the scan method will contain an physical plan that assumes this schema
    pub fn new(name: &str, table_schema: SchemaRef) -> Self {
        Self {
            name: name.to_owned(),
            table_schema,
        }
    }

    /// The user-provided name of the CTE
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The schema of the recursive term of the query
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }
}

#[async_trait]
impl TableProvider for CteWorkTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = ScanArgs::default()
            .with_projection(projection.map(|p| p.as_slice()))
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(self.scan_with_args(state, options).await?.into_inner())
    }

    async fn scan_with_args<'a>(
        &self,
        _state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        Ok(ScanResult::new(Arc::new(WorkTableExec::new(
            self.name.clone(),
            Arc::clone(&self.table_schema),
            args.projection().map(|p| p.to_vec()),
        )?)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // TODO: should we support filter pushdown?
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}
