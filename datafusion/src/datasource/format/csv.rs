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

//! CSV format abstractions

use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;
use std::fs::File;

use super::{FileFormat, StringStream};
use crate::datasource::PartitionedFile;
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::format::CsvExec;
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// Character Separated Value `FileFormat` implementation.
pub struct CsvFormat {
    /// Set true to indicate that the first line is a header.
    pub has_header: bool,
    /// The character seprating values within a row.
    pub delimiter: u8,
    /// If no schema was provided for the table, it will be
    /// infered from the data itself, this limits the number
    /// of lines used in the process.
    pub schema_infer_max_rec: Option<usize>,
}

#[async_trait]
impl FileFormat for CsvFormat {
    async fn infer_schema(&self, mut paths: StringStream) -> Result<SchemaRef> {
        let mut schemas = vec![];
        let mut records_to_read = self.schema_infer_max_rec.unwrap_or(std::usize::MAX);

        while let Some(fname) = paths.next().await {
            let (schema, records_read) = arrow::csv::reader::infer_file_schema(
                &mut File::open(fname)?,
                self.delimiter,
                Some(records_to_read),
                self.has_header,
            )?;
            if records_read == 0 {
                continue;
            }
            schemas.push(schema.clone());
            records_to_read -= records_read;
            if records_to_read == 0 {
                break;
            }
        }

        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
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
        let exec = CsvExec::try_new(
            files.into_iter().flatten().map(|f| f.path).collect(),
            statistics,
            schema,
            self.has_header,
            self.delimiter,
            projection.clone(),
            batch_size,
            limit,
        )?;
        Ok(Arc::new(exec))
    }
}
