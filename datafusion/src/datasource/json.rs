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

//! Line-delimited JSON data source
//!
//! This data source allows Line-delimited JSON string or files to be used as input for queries.
//!

use std::{
    any::Any,
    io::{BufReader, Read, Seek},
    sync::{Arc, Mutex},
};

use crate::{
    datasource::{Source, TableProvider},
    error::{DataFusionError, Result},
    physical_plan::{
        common,
        json::{NdJsonExec, NdJsonReadOptions},
        ExecutionPlan,
    },
};
use arrow::{datatypes::SchemaRef, json::reader::infer_json_schema_from_seekable};

use super::datasource::Statistics;

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}

/// Represents a  line-delimited JSON file with a provided schema
pub struct NdJsonFile {
    source: Source<Box<dyn SeekRead + Send + Sync + 'static>>,
    schema: SchemaRef,
    file_extension: String,
    statistics: Statistics,
}

impl NdJsonFile {
    /// Attempt to initialize a `NdJsonFile` from a path. The schema can be inferred automatically.
    pub fn try_new(path: &str, options: NdJsonReadOptions) -> Result<Self> {
        let schema = if let Some(schema) = options.schema {
            schema
        } else {
            let filenames = common::build_file_list(path, options.file_extension)?;
            if filenames.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "No files found at {path} with file extension {file_extension}",
                    path = path,
                    file_extension = options.file_extension
                )));
            }

            NdJsonExec::try_infer_schema(
                filenames,
                Some(options.schema_infer_max_records),
            )?
            .into()
        };

        Ok(Self {
            source: Source::Path(path.to_string()),
            schema,
            file_extension: options.file_extension.to_string(),
            statistics: Statistics::default(),
        })
    }

    /// Attempt to initialize a `NdJsonFile` from a reader impls `Seek`. The schema can be inferred automatically.
    pub fn try_new_from_reader<R: Read + Seek + Send + Sync + 'static>(
        mut reader: R,
        options: NdJsonReadOptions,
    ) -> Result<Self> {
        let schema = if let Some(schema) = options.schema {
            schema
        } else {
            let mut bufr = BufReader::new(reader);
            let schema = infer_json_schema_from_seekable(
                &mut bufr,
                Some(options.schema_infer_max_records),
            )?
            .into();
            reader = bufr.into_inner();
            schema
        };
        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            statistics: Statistics::default(),
            file_extension: String::new(),
        })
    }
}
impl TableProvider for NdJsonFile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        batch_size: usize,
        _filters: &[crate::logical_plan::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let opts = NdJsonReadOptions {
            schema: Some(self.schema.clone()),
            schema_infer_max_records: 0, // schema will always be provided, so it's unnecessary to infer schema
            file_extension: self.file_extension.as_str(),
        };
        let batch_size = limit
            .map(|l| std::cmp::min(l, batch_size))
            .unwrap_or(batch_size);

        let exec = match &self.source {
            Source::Reader(maybe_reader) => {
                if let Some(rdr) = maybe_reader.lock().unwrap().take() {
                    NdJsonExec::try_new_from_reader(
                        rdr,
                        opts,
                        projection.clone(),
                        batch_size,
                        limit,
                    )?
                } else {
                    return Err(DataFusionError::Execution(
                        "You can only read once if the data comes from a reader"
                            .to_string(),
                    ));
                }
            }
            Source::Path(p) => {
                NdJsonExec::try_new(p, opts, projection.clone(), batch_size, limit)?
            }
        };
        Ok(Arc::new(exec))
    }

    fn statistics(&self) -> Statistics {
        self.statistics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::*;
    const TEST_DATA_BASE: &str = "tests/jsons";

    #[tokio::test]
    async fn csv_file_from_reader() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        let path = format!("{}/2.json", TEST_DATA_BASE);
        ctx.register_table(
            "ndjson",
            Arc::new(NdJsonFile::try_new(&path, Default::default())?),
        )?;
        let df = ctx.sql("select sum(a) from ndjson")?;
        let batches = df.collect().await?;
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            100000000000011
        );
        Ok(())
    }
}
