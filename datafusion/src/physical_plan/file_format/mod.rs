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

//! Execution plans that read file formats

mod avro;
mod csv;
mod json;
mod parquet;

pub use self::parquet::ParquetExec;
pub use avro::AvroExec;
pub use csv::CsvExec;
pub use json::NdJsonExec;

use crate::datasource::PartitionedFile;
use std::fmt::{Display, Formatter, Result};

/// A wrapper to customize partitioned file display
#[derive(Debug)]
struct FileGroupsDisplay<'a>(&'a [Vec<PartitionedFile>]);

impl<'a> Display for FileGroupsDisplay<'a> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let parts: Vec<_> = self
            .0
            .iter()
            .map(|pp| {
                pp.iter()
                    .map(|pf| pf.file_meta.path())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .collect();
        write!(f, "[{}]", parts.join(", "))
    }
}
