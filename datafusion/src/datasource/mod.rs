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

//! DataFusion data sources

pub mod csv;
pub mod datasource;
pub mod empty;
pub mod json;
pub mod memory;
pub mod parquet;

pub use self::csv::{CsvFile, CsvReadOptions};
pub use self::datasource::{TableProvider, TableType};
pub use self::memory::MemTable;

pub(crate) enum Source<R = Box<dyn std::io::Read + Send + Sync + 'static>> {
    /// Path to a single file or a directory containing one of more files
    Path(String),

    /// Read data from a reader
    Reader(std::sync::Mutex<Option<R>>),
}
