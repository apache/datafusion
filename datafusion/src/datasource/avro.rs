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

//! Line-delimited Avro data source
//!
//! This data source allows Line-delimited Avro records or files to be used as input for queries.
//!

use std::{
    any::Any,
    io::{Read, Seek},
    sync::{Arc, Mutex},
};

use crate::{
    datasource::{Source, TableProvider},
    error::{DataFusionError, Result},
    physical_plan::{common, ExecutionPlan},
};
use arrow::datatypes::SchemaRef;

use super::datasource::Statistics;
use crate::physical_plan::avro::{AvroExec, AvroReadOptions};

trait SeekRead: Read + Seek {}

impl<T: Seek + Read> SeekRead for T {}

/// Represents a  line-delimited JSON file with a provided schema
pub struct AvroFile {
    source: Source<Box<dyn SeekRead + Send + Sync + 'static>>,
    schema: SchemaRef,
    file_extension: String,
    statistics: Statistics,
}

impl AvroFile {
    /// Attempt to initialize a `AvroFile` from a path. The schema can be inferred automatically.
    pub fn try_new(path: &str, options: AvroReadOptions) -> Result<Self> {
        let schema = if let Some(schema) = options.schema {
            schema.clone()
        } else {
            let filenames =
                common::build_checked_file_list(path, options.file_extension)?;
            Arc::new(AvroExec::try_infer_schema(&filenames)?)
        };

        Ok(Self {
            source: Source::Path(path.to_string()),
            schema,
            file_extension: options.file_extension.to_string(),
            statistics: Statistics::default(),
        })
    }

    /// Attempt to initialize a `AvroFile` from a reader. The schema MUST be provided in options
    pub fn try_new_from_reader<R: Read + Seek + Send + Sync + 'static>(
        reader: R,
        options: AvroReadOptions,
    ) -> Result<Self> {
        let schema = match options.schema {
            Some(s) => s.clone(),
            None => {
                return Err(DataFusionError::Execution(
                    "Schema must be provided to CsvRead".to_string(),
                ));
            }
        };
        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            statistics: Statistics::default(),
            file_extension: String::new(),
        })
    }

    /// Attempt to initialize an AvroFile from a reader impls Seek. The schema can be inferred automatically.
    pub fn try_new_from_reader_infer_schema<R: Read + Seek + Send + Sync + 'static>(
        mut reader: R,
        options: AvroReadOptions,
    ) -> Result<Self> {
        let schema = {
            if let Some(schema) = options.schema {
                schema
            } else {
                Arc::new(AvroExec::infer_avro_schema_from_reader(&mut reader)?)
            }
        };

        Ok(Self {
            source: Source::Reader(Mutex::new(Some(Box::new(reader)))),
            schema,
            statistics: Statistics::default(),
            file_extension: String::new(),
        })
    }

    /// Get the path for Avro file(s) represented by this AvroFile instance
    pub fn path(&self) -> &str {
        match &self.source {
            Source::Reader(_) => "",
            Source::Path(path) => path,
        }
    }

    /// Get the file extension for the Avro file(s) represented by this AvroFile instance
    pub fn file_extension(&self) -> &str {
        &self.file_extension
    }
}

impl TableProvider for AvroFile {
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
        let opts = AvroReadOptions {
            schema: Some(self.schema.clone()),
            file_extension: self.file_extension.as_str(),
        };
        let batch_size = limit
            .map(|l| std::cmp::min(l, batch_size))
            .unwrap_or(batch_size);

        let exec = match &self.source {
            Source::Reader(maybe_reader) => {
                if let Some(rdr) = maybe_reader.lock().unwrap().take() {
                    AvroExec::try_new_from_reader(
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
                AvroExec::try_new(p, opts, projection.clone(), batch_size, limit)?
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
            Arc::new(AvroFile::try_new(&path, Default::default())?),
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
