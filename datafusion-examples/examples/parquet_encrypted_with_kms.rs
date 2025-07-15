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
use arrow_schema::SchemaRef;
use datafusion::common::{extensions_options, DataFusionError};
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::parquet::encryption::{
    decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::path::Path;
use parquet_key_management::crypto_factory::{
    CryptoFactory, DecryptionConfiguration, EncryptionConfiguration,
};
use parquet_key_management::kms::KmsConnectionConfig;
use parquet_key_management::test_kms::TestKmsClientFactory;
use std::collections::HashSet;
use std::fmt::Formatter;
use std::sync::Arc;
use tempfile::TempDir;

const ENCRYPTION_FACTORY_ID: &str = "example.memory_kms_encryption";

/// This example demonstrates reading and writing Parquet files that
/// are encrypted using Parquet Modular Encryption, and uses the
/// parquet-key-management crate to integrate with a Key Management Server (KMS).
///
/// Compared to the `parquet_encrypted` example, where AES keys
/// are specified directly, this example uses an `EncryptionFactory` so that
/// encryption keys can be dynamically generated per file,
/// and the encryption key metadata stored in files can be used to determine
/// the decryption keys when reading.
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
    let crypto_factory = CryptoFactory::new(TestKmsClientFactory::with_default_keys());
    let encryption_factory = KmsEncryptionFactory { crypto_factory };
    ctx.register_parquet_encryption_factory(
        ENCRYPTION_FACTORY_ID,
        Arc::new(encryption_factory),
    );

    // Register some simple test data
    let a: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![1, 10, 10, 100]));
    let c: ArrayRef = Arc::new(Int32Array::from(vec![2, 20, 20, 200]));
    let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)])?;
    ctx.register_batch("test_data", batch)?;

    {
        // Write and read with the programmatic API
        let tmpdir = TempDir::new()?;
        let table_path = format!("{}/", tmpdir.path().to_str().unwrap());
        write_encrypted(&ctx, &table_path).await?;
        read_encrypted(&ctx, &table_path).await?;
    }

    {
        // Write and read with the SQL API
        let tmpdir = TempDir::new()?;
        let table_path = format!("{}/", tmpdir.path().to_str().unwrap());
        write_encrypted_with_sql(&ctx, &table_path).await?;
        read_encrypted_with_sql(&ctx, &table_path).await?;
    }

    Ok(())
}

/// Write an encrypted Parquet file
async fn write_encrypted(ctx: &SessionContext, table_path: &str) -> Result<()> {
    let df = ctx.table("test_data").await?;

    let mut parquet_options = TableParquetOptions::new();
    // We specify that we want to use Parquet encryption by setting the identifier of the
    // encryption factory to use and providing the factory specific configuration.
    // Our encryption factory requires specifying the master key identifier to
    // use for encryption, and we can optionally configure which columns are encrypted.
    let encryption_config = KmsEncryptionConfig {
        key_id: "kf".to_owned(),
        encrypted_columns: "b,c".to_owned(),
    };
    parquet_options
        .crypto
        .configure_factory(ENCRYPTION_FACTORY_ID, &encryption_config);

    df.write_parquet(
        table_path,
        DataFrameWriteOptions::new(),
        Some(parquet_options),
    )
    .await?;

    println!("Encrypted Parquet written to {table_path}");
    Ok(())
}

/// Read from an encrypted Parquet file
async fn read_encrypted(ctx: &SessionContext, table_path: &str) -> Result<()> {
    let mut parquet_options = TableParquetOptions::new();
    // Specify the encryption factory to use for decrypting Parquet.
    // In this example, we don't require any additional configuration options when reading
    // as master key identifiers are stored in the key metadata within Parquet files.
    parquet_options
        .crypto
        .configure_factory(ENCRYPTION_FACTORY_ID, &KmsEncryptionConfig::default());

    let file_format = ParquetFormat::default().with_options(parquet_options);
    let listing_options = ListingOptions::new(Arc::new(file_format));

    ctx.register_listing_table(
        "encrypted_parquet_table",
        &table_path,
        listing_options.clone(),
        None,
        None,
    )
    .await?;

    let mut batch_stream = ctx
        .table("encrypted_parquet_table")
        .await?
        .execute_stream()
        .await?;
    println!("Reading encrypted Parquet as a RecordBatch stream");
    while let Some(batch) = batch_stream.next().await {
        let batch = batch?;
        println!("Read batch with {} rows", batch.num_rows());
    }

    println!("Finished reading");
    Ok(())
}

/// Write an encrypted Parquet file using only SQL syntax with string configuration
async fn write_encrypted_with_sql(ctx: &SessionContext, table_path: &str) -> Result<()> {
    let query = format!(
        "COPY test_data \
        TO '{table_path}' \
        STORED AS parquet
        OPTIONS (\
            'format.crypto.factory_id' '{ENCRYPTION_FACTORY_ID}', \
            'format.crypto.factory_options.key_id' 'kf', \
            'format.crypto.factory_options.encrypted_columns' 'b,c' \
        )"
    );
    let _ = ctx.sql(&query).await?.collect().await?;

    println!("Encrypted Parquet written to {table_path}");
    Ok(())
}

/// Read from an encrypted Parquet file using only the SQL API and string based configuration
async fn read_encrypted_with_sql(ctx: &SessionContext, table_path: &str) -> Result<()> {
    let ddl = format!(
        "CREATE EXTERNAL TABLE encrypted_parquet_table_2 \
        STORED AS PARQUET LOCATION '{table_path}' OPTIONS (\
        'format.crypto.factory_id' '{ENCRYPTION_FACTORY_ID}' \
        )"
    );
    ctx.sql(&ddl).await?;
    let df = ctx.sql("SELECT * FROM encrypted_parquet_table_2").await?;
    let mut batch_stream = df.execute_stream().await?;

    println!("Reading encrypted Parquet as a RecordBatch stream");
    while let Some(batch) = batch_stream.next().await {
        let batch = batch?;
        println!("Read batch with {} rows", batch.num_rows());
    }
    println!("Finished reading");
    Ok(())
}

// Options used to configure our example encryption factory
extensions_options! {
    struct KmsEncryptionConfig {
        /// Identifier of the encryption key to use
        pub key_id: String, default = "".to_owned()
        /// Comma separated list of columns to encrypt
        pub encrypted_columns: String, default = "".to_owned()
    }
}

/// Wrapper type around `CryptoFactory` to allow implementing the `EncryptionFactory` trait
struct KmsEncryptionFactory {
    crypto_factory: CryptoFactory,
}

impl std::fmt::Debug for KmsEncryptionFactory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsEncryptionFactory")
            .finish_non_exhaustive()
    }
}

/// `EncryptionFactory` is DataFusion trait for types that generate
/// file encryption and decryption properties.
impl EncryptionFactory for KmsEncryptionFactory {
    /// Generate file encryption properties to use when writing a Parquet file.
    /// The `schema` is provided so that it may be used to dynamically configure
    /// per-column encryption keys.
    /// The file path is also provided, so that it may be used to set an
    /// AAD prefix for the file, or to allow use of external key material
    /// (where key metadata is stored in a JSON file alongside Parquet files).
    fn get_file_encryption_properties(
        &self,
        options: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        _file_path: &Path,
    ) -> Result<Option<FileEncryptionProperties>> {
        let config: KmsEncryptionConfig = options.to_extension_options()?;
        if config.key_id.is_empty() {
            return Err(DataFusionError::Configuration(
                "Key id for encryption is not set".to_owned(),
            ));
        };
        // Configure encryption key to use
        let mut encryption_config_builder =
            EncryptionConfiguration::builder(config.key_id.clone());

        // Set up per-column encryption.
        let encrypted_columns: HashSet<&str> =
            config.encrypted_columns.split(",").collect();
        if !encrypted_columns.is_empty() {
            let encrypted_columns: Vec<String> = schema
                .fields
                .iter()
                .filter(|f| encrypted_columns.contains(f.name().as_str()))
                .map(|f| f.name().clone())
                .collect();
            encryption_config_builder = encryption_config_builder
                .add_column_key(config.key_id.clone(), encrypted_columns);
        }
        let encryption_config = encryption_config_builder.build()?;

        // The KMS connection could be configured from the options if needed,
        // but this example just uses the default options.
        let kms_config = Arc::new(KmsConnectionConfig::default());

        // Use the `CryptoFactory` to generate file encryption properties
        Ok(Some(self.crypto_factory.file_encryption_properties(
            kms_config,
            &encryption_config,
        )?))
    }

    /// Generate file decryption properties to use when reading a Parquet file.
    fn get_file_decryption_properties(
        &self,
        _options: &EncryptionFactoryOptions,
        _file_path: &Path,
    ) -> Result<Option<FileDecryptionProperties>> {
        let decryption_config = DecryptionConfiguration::builder().build();
        let kms_config = Arc::new(KmsConnectionConfig::default());
        Ok(Some(self.crypto_factory.file_decryption_properties(
            kms_config,
            decryption_config,
        )?))
    }
}
