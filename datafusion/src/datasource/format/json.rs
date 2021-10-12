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

use std::io::BufReader;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::datatypes::SchemaRef;
use arrow::json::reader::infer_json_schema_from_iterator;
use arrow::json::reader::ValueIter;
use async_trait::async_trait;
use futures::StreamExt;
use std::fs::File;

use super::{FileFormat, StringStream};
use crate::datasource::PartitionedFile;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::format::NdJsonExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// New line delimited JSON `FileFormat` implementation.
pub struct JsonFormat {
    /// If no schema was provided for the table, it will be
    /// infered from the data itself, this limits the number
    /// of lines used in the process.
    pub schema_infer_max_rec: Option<usize>,
}

#[async_trait]
impl FileFormat for JsonFormat {
    async fn infer_schema(&self, mut paths: StringStream) -> Result<SchemaRef> {
        let mut schemas = Vec::new();
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(usize::MAX);
        while let Some(file) = paths.next().await {
            let file = File::open(file)?;
            let mut reader = BufReader::new(file);
            let iter = ValueIter::new(&mut reader, None);
            let schema = infer_json_schema_from_iterator(iter.take_while(|_| {
                let should_take = records_to_read > 0;
                records_to_read -= 1;
                should_take
            }))?;
            if records_to_read == 0 {
                break;
            }
            schemas.push(schema);
        }

        let schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(schema))
    }

    async fn infer_stats(&self, _path: &str) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_executor(
        &self,
        schema: SchemaRef,
        files: Vec<Vec<PartitionedFile>>,
        statistics: Statistics,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = NdJsonExec::try_new(
            // flattening this for now because NdJsonExec does not support partitioning yet
            files.into_iter().flatten().map(|f| f.path).collect(),
            statistics,
            schema,
            projection.clone(),
            batch_size,
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}
