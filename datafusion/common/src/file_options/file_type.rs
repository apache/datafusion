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

use crate::error::{DataFusionError, Result};
use crate::parsers::CompressionTypeVariant;

use core::fmt;
use std::fmt::Display;
use std::hash::Hasher;
use std::str::FromStr;

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

/// A trait which provides information during planning time about a type of file which may be defined
/// externally. Use SessionContext::register_file_type to add new implementations.
pub trait FileType:
    std::fmt::Debug + FileTypeClone + Send + Sync
{
    /// Returns the default file extension for this type, e.g. CSV would return ".csv".to_owned()
    /// The default_extension is also used to uniquely identify a specific FileType::Extension variant,
    /// so ensure this String is unique from any built in FileType and any other ExtensionFileTypes
    /// defined.
    fn default_extension(&self) -> String;

    /// Returns the file extension when it is compressed with a given [CompressionTypeVariant]
    fn extension_with_compression(
        &self,
        compression: CompressionTypeVariant,
    ) -> Result<String>;
}

pub trait FileTypeClone {
    fn clone_box(&self) -> Box<dyn FileType>;
}

impl Clone for Box<dyn FileType> {
    fn clone(&self) -> Box<dyn FileType> {
        self.clone_box()
    }
}

impl std::hash::Hash for Box<dyn FileType> {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.default_extension().hash(state)
    }
}
