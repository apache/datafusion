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

//! File type abstraction

use std::any::Any;
use std::fmt::Display;

/// The default file extension of arrow files
pub const DEFAULT_ARROW_EXTENSION: &str = ".arrow";
/// The default file extension of avro files
pub const DEFAULT_AVRO_EXTENSION: &str = ".avro";
/// The default file extension of csv files
pub const DEFAULT_CSV_EXTENSION: &str = ".csv";
/// The default file extension of json files
pub const DEFAULT_JSON_EXTENSION: &str = ".json";
/// The default file extension of parquet files
pub const DEFAULT_PARQUET_EXTENSION: &str = ".parquet";

/// Define each `FileType`/`FileCompressionType`'s extension
pub trait GetExt {
    /// File extension getter
    fn get_ext(&self) -> String;
}

/// Defines the functionality needed for logical planning for
/// a type of file which will be read or written to storage.
pub trait FileType: GetExt + Display + Send + Sync {
    /// Returns the table source as [`Any`] so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any;
}
