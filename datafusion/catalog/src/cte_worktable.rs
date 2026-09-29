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

use std::borrow::Cow;
use std::future::ready;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion_common::error::Result;
use datafusion_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::work_table::WorkTableExec;
use futures::future::BoxFuture;

use crate::{ScanArgs, ScanResult, Session, TableProvider};

/// The temporary working table where the previous iteration of a recursive query is stored
/// Naming is based on PostgreSQL's implementation.
/// See here for more details: www.postgresql.org/docs/11/queries-with.html#id-1.5.6.12.5.4
#[derive(Debug)]
pub struct CteWorkTable {
    /// The name of the CTE work table
    name: String,
    /// Schema exposed by recursive self-references while planning the recursive term.
    ///
    /// This is a conservative work-table schema, not the final recursive query output
    /// schema. For example, the SQL planner may mark fields nullable here so recursive
    /// references do not inherit unsound anchor-term nullability assumptions.
    table_schema: SchemaRef,
}

impl CteWorkTable {
    /// Construct a new CteWorkTable with the given name and self-reference schema.
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

    /// The schema exposed by scans of the recursive self-reference.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }
}

#[async_trait]
impl TableProvider for CteWorkTable {
    fn get_logical_plan(&'_ self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn scan<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        projection: Option<&'life2 [usize]>,
        filters: &'life3 [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'async_trait, Result<Arc<dyn ExecutionPlan>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        self.scan_boxed(state, projection, filters, limit)
    }

    // Hand-written `#[async_trait]` expansion to reduce compile time. See
    // <https://github.com/apache/datafusion/issues/13814#issuecomment-5292709677>
    fn scan_with_args<'a, 'life0, 'life1, 'async_trait>(
        &'life0 self,
        state: &'life1 dyn Session,
        args: ScanArgs<'a>,
    ) -> BoxFuture<'async_trait, Result<ScanResult>>
    where
        'a: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(ready(self.scan_with_args_inner(state, &args)))
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

impl CteWorkTable {
    fn scan_with_args_inner(
        &self,
        _state: &dyn Session,
        args: &ScanArgs<'_>,
    ) -> Result<ScanResult> {
        Ok(ScanResult::new(Arc::new(WorkTableExec::new(
            self.name.clone(),
            Arc::clone(&self.table_schema),
            args.projection().map(|p| p.to_vec()),
        )?)))
    }

    fn scan_boxed<'a>(
        &'a self,
        state: &'a dyn Session,
        projection: Option<&'a [usize]>,
        filters: &'a [Expr],
        limit: Option<usize>,
    ) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
        Box::pin(self.scan_inner(state, projection, filters, limit))
    }

    async fn scan_inner(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let options = ScanArgs::default()
            .with_projection(projection)
            .with_filters(Some(filters))
            .with_limit(limit);
        Ok(self.scan_with_args(state, options).await?.into_inner())
    }
}
