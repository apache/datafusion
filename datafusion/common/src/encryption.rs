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

// Support optional features for encryption in Parquet files.
//! This module provides types and functions related to encryption in Parquet files.

#[cfg(feature = "parquet_encryption")]
pub use parquet::encryption::decrypt::FileDecryptionProperties;
#[cfg(feature = "parquet_encryption")]
pub use parquet::encryption::encrypt::FileEncryptionProperties;

#[cfg(not(feature = "parquet_encryption"))]
#[derive(Default, Clone, Debug)]
pub struct FileDecryptionProperties;
#[cfg(not(feature = "parquet_encryption"))]
#[derive(Default, Clone, Debug)]
pub struct FileEncryptionProperties;

pub use crate::config::{ConfigFileDecryptionProperties, ConfigFileEncryptionProperties};
