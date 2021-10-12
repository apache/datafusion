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

//! Line delimited JSON format abstractions

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

use super::FileFormat;
use crate::datasource::PartitionedFile;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// New line delimited JSON `FileFormat` implementation.
pub struct JsonFormat {
    /// If no schema was provided for the table, it will be
    /// infered from the data itself, this limits the number
    /// of lines used in the process.
    pub schema_infer_max_rec: Option<u64>,
}

#[async_trait]
impl FileFormat for JsonFormat {
    async fn infer_schema(&self, _path: &str) -> Result<SchemaRef> {
        todo!()
    }

    async fn infer_stats(&self, _path: &str) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_executor(
        &self,
        _schema: SchemaRef,
        _files: Vec<Vec<PartitionedFile>>,
        _statistics: Statistics,
        _projection: &Option<Vec<usize>>,
        _batch_size: usize,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
