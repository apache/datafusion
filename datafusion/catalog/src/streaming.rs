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

//! A simplified [`TableProvider`] for streaming partitioned datasets

use std::any::Any;
use std::sync::Arc;

use crate::Session;
use crate::TableProvider;

use arrow::datatypes::SchemaRef;
use datafusion_common::{plan_err, DFSchema, Result};
use datafusion_expr::{Expr, SortExpr, TableType};
use datafusion_physical_expr::{create_physical_sort_exprs, LexOrdering};
use datafusion_physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion_physical_plan::ExecutionPlan;

use async_trait::async_trait;
use log::debug;

/// A [`TableProvider`] that streams a set of [`PartitionStream`]
#[derive(Debug)]
pub struct StreamingTable {
    schema: SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
    infinite: bool,
    sort_order: Vec<SortExpr>,
}

impl StreamingTable {
    /// Try to create a new [`StreamingTable`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.contains(partition_schema) {
                debug!(
                    "target schema does not contain partition schema. \
                        Target_schema: {schema:?}. Partition Schema: {partition_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        Ok(Self {
            schema,
            partitions,
            infinite: false,
            sort_order: vec![],
        })
    }

    /// Sets streaming table can be infinite.
    pub fn with_infinite_table(mut self, infinite: bool) -> Self {
        self.infinite = infinite;
        self
    }

    /// Sets the existing ordering of streaming table.
    pub fn with_sort_order(mut self, sort_order: Vec<SortExpr>) -> Self {
        self.sort_order = sort_order;
        self
    }
}

#[async_trait]
impl TableProvider for StreamingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_sort = if !self.sort_order.is_empty() {
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;
            let eqp = state.execution_props();

            create_physical_sort_exprs(&self.sort_order, &df_schema, eqp)?
        } else {
            vec![]
        };

        Ok(Arc::new(StreamingTableExec::try_new(
            Arc::clone(&self.schema),
            self.partitions.clone(),
            projection,
            LexOrdering::new(physical_sort),
            self.infinite,
            limit,
        )?))
    }
}
