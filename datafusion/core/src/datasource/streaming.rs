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

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, TableType};

use crate::datasource::TableProvider;
use crate::execution::context::{SessionState, TaskContext};
use crate::physical_plan::streaming::StreamingTableExec;
use crate::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

/// A partition that can be converted into a [`SendableRecordBatchStream`]
pub trait PartitionStream: Send + Sync {
    /// Returns the schema of this partition
    fn schema(&self) -> &SchemaRef;

    /// Returns a stream yielding this partitions values
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream;
}

/// A [`TableProvider`] that streams a set of [`PartitionStream`]
pub struct StreamingTable {
    schema: SchemaRef,
    partitions: Vec<Arc<dyn PartitionStream>>,
}

impl StreamingTable {
    /// Try to create a new [`StreamingTable`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
    ) -> Result<Self> {
        if !partitions.iter().all(|x| schema.contains(x.schema())) {
            return Err(DataFusionError::Plan(
                "Mismatch between schema and batches".to_string(),
            ));
        }

        Ok(Self { schema, partitions })
    }
}

#[async_trait]
impl TableProvider for StreamingTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        // TODO: push limit down
        Ok(Arc::new(StreamingTableExec::try_new(
            self.schema.clone(),
            self.partitions.clone(),
            projection,
        )?))
    }
}
