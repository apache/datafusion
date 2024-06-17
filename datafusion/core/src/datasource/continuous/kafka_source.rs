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

use async_trait::async_trait;
use std::{any::Any, sync::Arc};

use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion_common::{plan_err, Result};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::{
    kafka_source::{KafkaStreamConfig, KafkaStreamRead},
    streaming::StreamingTableExec,
    ExecutionPlan,
};

// Used to createa kafka source
pub struct KafkaSource(pub Arc<KafkaStreamConfig>);

impl KafkaSource {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<KafkaStreamConfig>) -> Self {
        Self(config)
    }

    pub async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => {
                let projected = self.0.schema.project(p)?;
                create_ordering(&projected, &self.0.order)?
            }
            None => create_ordering(self.0.schema.as_ref(), &self.0.order)?,
        };
        let mut partition_streams = Vec::with_capacity(self.0.partitions as usize);

        for part in 0..self.0.partitions {
            let read_stream = Arc::new(KafkaStreamRead {
                config: self.0.clone(),
                assigned_partitions: vec![part],
            });
            partition_streams.push(read_stream as _);
        }

        Ok(Arc::new(StreamingTableExec::try_new(
            self.0.schema.clone(),
            partition_streams,
            projection,
            projected_schema,
            true,
        )?))
    }
}

#[async_trait]
impl TableProvider for KafkaSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection).await;
    }
}

fn create_ordering(
    schema: &Schema,
    sort_order: &[Vec<Expr>],
) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for exprs in sort_order {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = vec![];
        for expr in exprs {
            match expr {
                Expr::Sort(sort) => match sort.expr.as_ref() {
                    Expr::Column(col) => match expressions::col(&col.name, schema) {
                        Ok(expr) => {
                            sort_exprs.push(PhysicalSortExpr {
                                expr,
                                options: SortOptions {
                                    descending: !sort.asc,
                                    nulls_first: sort.nulls_first,
                                },
                            });
                        }
                        // Cannot find expression in the projected_schema, stop iterating
                        // since rest of the orderings are violated
                        Err(_) => break,
                    },
                    expr => {
                        return plan_err!(
                            "Expected single column references in output_ordering, got {expr}"
                        )
                    }
                },
                expr => return plan_err!("Expected Expr::Sort in output_ordering, but got {expr}"),
            }
        }
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}
