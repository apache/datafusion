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

//! Apache Arrow format abstractions
//!
//! Works with files following the [Arrow IPC format](https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format)

use std::any::Any;
use std::io::{Read, Seek};
use std::sync::Arc;

use crate::datasource::file_format::FileFormat;
use crate::datasource::physical_plan::{ArrowExec, FileScanConfig};
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::ExecutionPlan;

use arrow::ipc::reader::FileReader;
use arrow_schema::{Schema, SchemaRef};

use datafusion_common::{FileType, Statistics};
use datafusion_physical_expr::PhysicalExpr;

use async_trait::async_trait;
use object_store::{GetResultPayload, ObjectMeta, ObjectStore};

/// Arrow `FileFormat` implementation.
#[derive(Default, Debug)]
pub struct ArrowFormat;

#[async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let r = store.as_ref().get(&object.location).await?;
            let schema = match r.payload {
                GetResultPayload::File(mut file, _) => {
                    read_arrow_schema_from_reader(&mut file)?
                }
                GetResultPayload::Stream(_) => {
                    // TODO: Fetching entire file to get schema is potentially wasteful
                    let data = r.bytes().await?;
                    let mut cursor = std::io::Cursor::new(&data);
                    read_arrow_schema_from_reader(&mut cursor)?
                }
            };
            schemas.push(schema.as_ref().clone());
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = ArrowExec::new(conf);
        Ok(Arc::new(exec))
    }

    fn file_type(&self) -> FileType {
        FileType::ARROW
    }
}

fn read_arrow_schema_from_reader<R: Read + Seek>(reader: R) -> Result<SchemaRef> {
    let reader = FileReader::try_new(reader, None)?;
    Ok(reader.schema())
}
