// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
