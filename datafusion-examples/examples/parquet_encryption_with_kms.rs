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
use datafusion::common::extensions_options;
use datafusion::config::{ConfigExtension, TableParquetOptions};
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
use std::sync::Arc;
use tempfile::TempDir;

/// This example demonstrates reading and writing Parquet files that
/// are encrypted using Parquet Modular Encryption, and uses the
/// parquet-key-management crate to integrate with a Key Management Server (KMS).

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register an `EncryptionFactory` implementation to be used for Parquet encryption
    // in the session context.
    // This uses an in-memory test KMS from the `parquet_key_management` crate with
    // a custom `KmsEncryptionFactory` wrapper type to integrate with DataFusion.
    // `EncryptionFactory` instances are registered with a name to identify them so
    // they can be later referenced in configuration options, and it's possible to register
    // multiple different factories to handle different ways of encrypting Parquet.
    // In future it could be possible to have built-in implementations in DataFusion.
    let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());
    let encryption_factory = KmsEncryptionFactory { crypto_factory };
    ctx.register_parquet_encryption_factory(
        "example.inmem_kms_encryption",
        Arc::new(encryption_factory),
    );

    // Register options required for our encryption factory.
    // This works similarly to register_table_options_extension but registers extension options
    // specific to Parquet, so they can be set in `TableParquetOptions`.
    ctx.register_parquet_options_extension(EncryptionConfig::default());

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
    // We specify that we want to use Parquet encryption by setting the name of the
    // encryption factory to use.
    parquet_options.global.encryption_factory = "example.inmem_kms_encryption".to_owned();
    // Our encryption factory requires specifying the master key identifier to
    // use for encryption:
    let encryption_options = parquet_options
        .extensions
        .get_mut::<EncryptionConfig>()
        .ok_or_else(|| {
            DataFusionError::Configuration(
                "EncryptionConfig options not registered".to_owned(),
            )
        });
    encryption_options.footer_key_id = Some("kf1".to_owned());
    // Question: I don't think extensions.get will work when creating new `TableParquetOptions`,
    // we probably need to register the extension options here but also allow registering them
    // on the session context for compatibility with setting options via strings?.

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
    // We don't require setting any additional configuration options when reading.
    parquet_options.global.encryption_factory = "example.inmem_kms_encryption".to_owned();

    let file_format = ParquetFormat::default().with_options(parquet_options);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    let file_name = std::fs::read_dir(tmpdir)?.next().unwrap()?;
    let table_path = format!("file://{}", file_name.path().as_os_str().to_str().unwrap());

    let _ = ctx
        .register_listing_table(
            "parquet_table",
            &table_path,
            listing_options.clone(),
            None,
            None,
        )
        .await?;

    let df = ctx.sql("SELECT * FROM parquet_table").await?;
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

// Custom options used to configure our encryption factory
extensions_options! {
   pub struct EncryptionConfig {
       pub footer_key_id: Option<String>, default = None
   }
}

impl ConfigExtension for EncryptionConfig {
    const PREFIX: &'static str = "kms_encryption";
}

/// Wrapper type for `CryptoFactory` to allow implementing the `EncryptionFactory` trait
struct KmsEncryptionFactory {
    crypto_factory: CryptoFactory,
}

/// `EncryptionFactory` is a trait defined by DataFusion that allows generating
/// file encryption and decryption properties.
/// The `FileSinkConfig is provided so that the schema may be used to dynamically specify
/// per-column encryption.
impl EncryptionFactory for KmsEncryptionFactory {
    fn get_file_encryption_properties(
        &self,
        options: &TableParquetOptions,
        sink_config: &FileSinkConfig,
    ) -> Result<FileEncryptionProperties> {
        let config: &EncryptionConfig = options.extensions.get().ok_or_else(|| {
            DataFusionError::Configuration(
                "EncryptionConfig options not registered".to_owned(),
            )
        })?;
        let footer_key_id = config.footer_key_id.as_ref().cloned().ok_or_else(|| {
            DataFusionError::Configuration("Footer key id not configured".to_owned())
        })?;
        // We could configure per-column keys using the provided schema,
        // but for simplicity this example uses uniform encryption.
        let config =
            EncryptionConfiguration::builder(footer_key_id.to_owned()).build()?;
        // Similarly, KMS connection could be configured from the options.
        let kms_config = Arc::new(KmsConnectionConfig::default());
        Ok(self
            .crypto_factory
            .file_encryption_properties(kms_config, &config)?)
    }

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
