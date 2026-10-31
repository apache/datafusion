// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Encryption context used during Parquet file open.
//!
//! This module is only compiled with the `parquet_encryption` feature. It is
//! isolated here so the feature gating does not pollute the rest of the
//! opener module.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::EncryptionFactoryOptions;
use datafusion_common::encryption::FileDecryptionProperties;
use datafusion_execution::parquet_encryption::EncryptionFactory;

use super::ParquetMorselizer;

pub(super) struct EncryptionContext {
    file_decryption_properties: Option<Arc<FileDecryptionProperties>>,
    encryption_factory: Option<(Arc<dyn EncryptionFactory>, EncryptionFactoryOptions)>,
}

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

impl ParquetMorselizer {
    pub(super) fn get_encryption_context(&self) -> EncryptionContext {
        EncryptionContext::new(
            self.file_decryption_properties.clone(),
            self.encryption_factory.clone(),
        )
    }
}
