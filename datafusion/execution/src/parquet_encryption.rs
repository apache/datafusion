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

use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion_common::config::{EncryptionFactoryOptions, ExtensionOptions};
use datafusion_common::error::Result;
use datafusion_common::DataFusionError;
use object_store::path::Path;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use std::sync::Arc;

/// Trait for types that generate file encryption and decryption properties to
/// write and read encrypted Parquet files.
/// This allows flexibility in how encryption keys are managed, for example, to
/// integrate with a user's key management service (KMS).
pub trait EncryptionFactory: Send + Sync + std::fmt::Debug + 'static {
    /// The type to hold configuration options for this factory
    type Options: ExtensionOptions + Default;

    /// Generate file encryption properties to use when writing a Parquet file.
    fn get_file_encryption_properties(
        &self,
        config: &Self::Options,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>>;

    /// Generate file decryption properties to use when reading a Parquet file.
    fn get_file_decryption_properties(
        &self,
        config: &Self::Options,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>>;
}

/// Dyn-compatible version of the [`EncryptionFactory`] trait
pub trait DynEncryptionFactory: Send + Sync + std::fmt::Debug + 'static {
    /// Generate file encryption properties to use when writing a Parquet file.
    fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>>;

    /// Generate file decryption properties to use when reading a Parquet file.
    fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>>;
}

impl<T: EncryptionFactory> DynEncryptionFactory for T {
    fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>> {
        let mut options = T::Options::default();
        for (key, value) in &config.options {
            options.set(key, value)?;
        }
        self.get_file_encryption_properties(&options, schema, file_path)
    }

    fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>> {
        let mut options = T::Options::default();
        for (key, value) in &config.options {
            options.set(key, value)?;
        }
        self.get_file_decryption_properties(&options, file_path)
    }
}

/// Stores [`EncryptionFactory`] implementations that can be retrieved by a unique string identifier
#[derive(Clone, Debug, Default)]
pub struct EncryptionFactoryRegistry {
    factories: DashMap<String, Arc<dyn DynEncryptionFactory>>,
}

impl EncryptionFactoryRegistry {
    /// Register a [`DynEncryptionFactory`] with an associated identifier that can be later
    /// used to configure encryption when reading or writing Parquet.
    /// If an encryption factory with the same identifier was already registered, it is replaced and returned.
    pub fn register_factory(
        &self,
        id: &str,
        factory: Arc<dyn DynEncryptionFactory>,
    ) -> Option<Arc<dyn DynEncryptionFactory>> {
        self.factories.insert(id.to_owned(), factory)
    }

    /// Retrieve a [`DynEncryptionFactory`] by its identifier
    pub fn get_factory(&self, id: &str) -> Result<Arc<dyn DynEncryptionFactory>> {
        self.factories
            .get(id)
            .map(|f| Arc::clone(f.value()))
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "No Parquet encryption factory found for id '{id}'"
                ))
            })
    }
}
