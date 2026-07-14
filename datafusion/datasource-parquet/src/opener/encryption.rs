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

//! Encryption context used during Parquet file open.
//!
//! Isolated here so the `#[cfg(feature = "parquet_encryption")]` gating does
//! not pollute the rest of the opener module.

#[cfg(feature = "parquet_encryption")]
use std::sync::Arc;

use datafusion_common::Result;
#[cfg(feature = "parquet_encryption")]
use datafusion_common::config::EncryptionFactoryOptions;
#[cfg(feature = "parquet_encryption")]
use datafusion_common::encryption::FileDecryptionProperties;
#[cfg(feature = "parquet_encryption")]
use datafusion_execution::parquet_encryption::EncryptionFactory;

use super::ParquetMorselizer;

#[derive(Default)]
pub(super) struct EncryptionContext {
    #[cfg(feature = "parquet_encryption")]
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    #[cfg(feature = "parquet_encryption")]
    encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
}

#[cfg(feature = "parquet_encryption")]
impl EncryptionContext {
    fn new(
        file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
        encryption_factory: Option<(
            Arc<dyn EncryptionFactory>,
            EncryptionFactoryOptions,
        )>,
    ) -> Self {
        Self {
            file_decryption_properties,
            encryption_factory,
        }
    }

    pub(super) async fn get_file_decryption_properties(
        &self,
        file_location: &object_store::path::Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        match &self.file_decryption_properties {
            Some(file_decryption_properties) => {
                Ok(Some(Arc::clone(file_decryption_properties)))
            }
            None => match &self.encryption_factory {
                Some((encryption_factory, encryption_config)) => Ok(encryption_factory
                    .get_file_decryption_properties(encryption_config, file_location)
                    .await?),
                None => Ok(None),
            },
        }
    }
}

#[cfg(not(feature = "parquet_encryption"))]
#[expect(dead_code)]
impl EncryptionContext {
    pub(super) async fn get_file_decryption_properties(
        &self,
        _file_location: &object_store::path::Path,
    ) -> Result<
        Option<std::sync::Arc<datafusion_common::encryption::FileDecryptionProperties>>,
    > {
        Ok(None)
    }
}

impl ParquetMorselizer {
    #[cfg(feature = "parquet_encryption")]
    pub(super) fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::new(
            self.file_decryption_properties.clone(),
            self.encryption_factory.clone(),
        )
    }

    #[cfg(not(feature = "parquet_encryption"))]
    #[expect(dead_code)]
    pub(super) fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::default()
    }
}
