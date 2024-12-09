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

//! Helpers for writing csv files and reading them back

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crate::arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use crate::error::Result;

use arrow::csv::WriterBuilder;

///  a CSV file that has been created for testing.
pub struct TestCsvFile {
    path: PathBuf,
    schema: SchemaRef,
}

impl TestCsvFile {
    /// Creates a new csv file at the specified location
    pub fn try_new(
        path: PathBuf,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Result<Self> {
        let file = File::create(&path).unwrap();
        let builder = WriterBuilder::new().with_header(true);
        let mut writer = builder.build(file);

        let mut batches = batches.into_iter();
        let first_batch = batches.next().expect("need at least one record batch");
        let schema = first_batch.schema();

        let mut num_rows = 0;
        for batch in batches {
            writer.write(&batch)?;
            num_rows += batch.num_rows();
        }

        println!("Generated test dataset with {num_rows} rows");

        Ok(Self { path, schema })
    }

    /// The schema of this csv file
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The path to the csv file
    pub fn path(&self) -> &std::path::Path {
        self.path.as_path()
    }
}
