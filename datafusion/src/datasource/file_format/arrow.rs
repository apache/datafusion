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

//! Apache Avro format abstractions

use std::any::Any;
use std::io::Read;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::reader::StreamReader;
use arrow::{self, datatypes::SchemaRef};
use async_trait::async_trait;
use futures::StreamExt;

use super::FileFormat;
use crate::datasource::object_store::{ObjectReader, ObjectReaderStream};
use crate::error::Result;
use crate::logical_plan::Expr;
use crate::physical_plan::file_format::{ArrowExec, FileScanConfig};
use crate::physical_plan::ExecutionPlan;
use crate::physical_plan::Statistics;

/// The default file extension of arrow files
pub const DEFAULT_ARROW_EXTENSION: &str = ".arrow";
/// Arrow `FileFormat` implementation.
#[derive(Default, Debug)]
pub struct ArrowFormat;

#[async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(&self, mut readers: ObjectReaderStream) -> Result<SchemaRef> {
        let mut schemas = vec![];
        while let Some(obj_reader) = readers.next().await {
            let mut reader = obj_reader?.sync_reader()?;
            let schema = read_arrow_schema_from_reader(&mut reader)?;
            schemas.push(schema.as_ref().clone());
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(&self, _reader: Arc<dyn ObjectReader>) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        _filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = ArrowExec::new(conf);
        Ok(Arc::new(exec))
    }
}

fn read_arrow_schema_from_reader<R: Read>(reader: R) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(reader)?;
    Ok(reader.schema())
}

#[cfg(test)]
mod tests {}
