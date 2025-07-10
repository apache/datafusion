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

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::config::TableParquetOptions;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::physical_plan::FileSinkConfig;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet_key_management::crypto_factory::{
    CryptoFactory, DecryptionConfiguration, EncryptionConfiguration,
};
use parquet_key_management::kms::KmsConnectionConfig;
use parquet_key_management::test_kms::TestKmsClientFactory;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

/// This example demonstrates reading and writing Parquet files that
/// are encrypted using Parquet Modular Encryption, and uses the
/// parquet-key-management crate to integrate with a Key Management Server (KMS).

const ENCRYPTION_FACTORY_ID: &'static str = "example.inmem_kms_encryption";

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register an `EncryptionFactory` implementation to be used for Parquet encryption
    // in the session context.
    // This example uses an in-memory test KMS from the `parquet_key_management` crate with
    // a custom `KmsEncryptionFactory` wrapper type to integrate with DataFusion.
    // `EncryptionFactory` instances are registered with a name to identify them so
    // they can be later referenced in configuration options, and it's possible to register
    // multiple different factories to handle different ways of encrypting Parquet.
    // In future it could be possible to have built-in implementations in DataFusion.
    let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());
    let encryption_factory = KmsEncryptionFactory { crypto_factory };
    ctx.register_parquet_encryption_factory(
        ENCRYPTION_FACTORY_ID,
        Arc::new(encryption_factory),
    );

    let tmpdir = TempDir::new()?;
    write_encrypted(&ctx, &tmpdir).await?;
    read_encrypted(&ctx, &tmpdir).await?;
    Ok(())
}

/// Write an encrypted Parquet file
async fn write_encrypted(ctx: &SessionContext, tmpdir: &TempDir) -> Result<()> {
    let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 10, 10, 100]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])?;

    ctx.register_batch("test_data", batch)?;
    let df = ctx.table("test_data").await?;

    let mut parquet_options = TableParquetOptions::new();
    // We specify that we want to use Parquet encryption by setting the identifier of the
    // encryption factory to use.
    parquet_options.encryption.factory_id = ENCRYPTION_FACTORY_ID.to_owned();
    // Our encryption factory requires specifying the master key identifier to
    // use for encryption. To support arbitrary configuration options for different encryption factories,
    // DataFusion could use a HashMap<String, String> field for encryption options.
    let encryption_config = EncryptionConfig::new("kf1".to_owned());
    parquet_options.encryption.factory_options = encryption_config.to_config_map();

    df.write_parquet(
        tmpdir.path().to_str().unwrap(),
        DataFrameWriteOptions::new(),
        Some(parquet_options),
    )
    .await?;

    println!("Encrypted Parquet written to {:?}", tmpdir.path());

    Ok(())
}

/// Read from an encrypted Parquet file
async fn read_encrypted(ctx: &SessionContext, tmpdir: &TempDir) -> Result<()> {
    let mut parquet_options = TableParquetOptions::new();
    // Specify the encryption factory to use for decrypting Parquet.
    // In this example, we don't require any additional configuration options when reading
    // as key identifiers are stored in the key metadata.
    parquet_options.encryption.factory_id = ENCRYPTION_FACTORY_ID.to_owned();

    let file_format = ParquetFormat::default().with_options(parquet_options);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let file_name = std::fs::read_dir(tmpdir)?.next().unwrap()?;
    let table_path = format!("file://{}", file_name.path().as_os_str().to_str().unwrap());

    let _ = ctx
        .register_listing_table(
            "encrypted_parquet_table",
            &table_path,
            listing_options.clone(),
            None,
            None,
        )
        .await?;

    let df = ctx.sql("SELECT * FROM encrypted_parquet_table").await?;
    let plan = df.create_physical_plan().await?;

    let mut batch_stream = execute_stream(plan.clone(), ctx.task_ctx())?;
    println!("Reading encrypted Parquet as a RecordBatch stream");
    while let Some(batch) = batch_stream.next().await {
        let batch = batch?;
        println!("Read batch with {} rows", batch.num_rows());
    }
    println!("Finished reading");
    Ok(())
}

// Options used to configure our example encryption factory
struct EncryptionConfig {
    pub footer_key_id: String,
}

impl EncryptionConfig {
    pub fn new(footer_key_id: String) -> Self {
        Self { footer_key_id }
    }

    pub fn to_config_map(self) -> HashMap<String, String> {
        let mut config = HashMap::new();
        config.insert("footer_key_id".to_string(), self.footer_key_id);
        config
    }
}

/// Wrapper type around `CryptoFactory` to allow implementing the `EncryptionFactory` trait
struct KmsEncryptionFactory {
    crypto_factory: CryptoFactory,
}

/// `EncryptionFactory` is a trait defined by DataFusion that allows generating
/// file encryption and decryption properties.
impl EncryptionFactory for KmsEncryptionFactory {
    /// Generate file encryption properties to use when writing a Parquet file.
    /// The `FileSinkConfig` is provided so that the schema may be used to dynamically configure
    /// per-column encryption keys.
    /// Because `FileSinkConfig` can represent multiple output files, we also provide a
    /// single file path so that external key material may be used (where key metadata is
    /// stored in a JSON file alongside Parquet files).
    fn get_file_encryption_properties(
        &self,
        options: &TableParquetOptions,
        sink_config: &FileSinkConfig,
        file_path: &str,
    ) -> Result<FileEncryptionProperties> {
        let config: &HashMap<String, String> = options.encryption.factory_options;
        let footer_key_id = config.get("footer_key_id").cloned().ok_or_else(|| {
            DataFusionError::Configuration(
                "Footer key id for encryption is not set".to_owned(),
            )
        })?;
        // We could configure per-column keys using the provided schema,
        // but for simplicity this example uses uniform encryption.
        let config = EncryptionConfiguration::builder(footer_key_id).build()?;
        // Similarly, the KMS connection could be configured from the options if needed, but this
        // example just uses the default options.
        let kms_config = Arc::new(KmsConnectionConfig::default());
        Ok(self
            .crypto_factory
            .file_encryption_properties(kms_config, &config)?)
    }

    /// Generate file decryption properties to use when reading a Parquet file.
    /// The `file_path` needs to be known to support encryption factories that use external key material.
    fn get_file_decryption_properties(
        &self,
        options: &TableParquetOptions,
        file_path: &str,
    ) -> Result<FileDecryptionProperties> {
        let config = DecryptionConfiguration::builder().build()?;
        let kms_config = Arc::new(KmsConnectionConfig::default());
        Ok(self
            .crypto_factory
            .file_decryption_properties(kms_config, config)?)
    }
}
