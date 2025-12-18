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

//! See `main.rs` for how to run it.

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use base64::Engine;
use datafusion::common::extensions_options;
use datafusion::config::{EncryptionFactoryOptions, TableParquetOptions};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::execution::parquet_encryption::EncryptionFactory;
use datafusion::parquet::encryption::decrypt::KeyRetriever;
use datafusion::parquet::encryption::{
    decrypt::FileDecryptionProperties, encrypt::FileEncryptionProperties,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::path::Path;
use rand::rand_core::{OsRng, TryRngCore};
use std::collections::HashSet;
use std::sync::Arc;
use tempfile::TempDir;

const ENCRYPTION_FACTORY_ID: &str = "example.mock_kms_encryption";

/// This example demonstrates reading and writing Parquet files that
/// are encrypted using Parquet Modular Encryption.
///
/// Compared to the `parquet_encrypted` example, where AES keys
/// are specified directly, this example implements an `EncryptionFactory` that
/// generates encryption keys dynamically per file.
/// Encryption key metadata is stored inline in the Parquet files and is used to determine
/// the decryption keys when reading the files.
///
/// In this example, encryption keys are simply stored base64 encoded in the Parquet metadata,
/// which is not a secure way to store encryption keys.
/// For production use, it is recommended to use a key-management service (KMS) to encrypt
/// data encryption keys.
pub async fn parquet_encrypted_with_kms() -> Result<()> {
    let ctx = SessionContext::new();

    // Register an `EncryptionFactory` implementation to be used for Parquet encryption
    // in the runtime environment.
    // `EncryptionFactory` instances are registered with a name to identify them so
    // they can be later referenced in configuration options, and it's possible to register
    // multiple different factories to handle different ways of encrypting Parquet.
    let encryption_factory = TestEncryptionFactory::default();
    ctx.runtime_env().register_parquet_encryption_factory(
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
        // Write and read encrypted Parquet with the programmatic API
        let tmpdir = TempDir::new()?;
        let table_path = format!("{}/", tmpdir.path().to_str().unwrap());
        write_encrypted(&ctx, &table_path).await?;
        read_encrypted(&ctx, &table_path).await?;
    }

    {
        // Write and read encrypted Parquet with the SQL API
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
    // encryption factory to use and providing the factory-specific configuration.
    // Our encryption factory only requires specifying the columns to encrypt.
    let encryption_config = EncryptionConfig {
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
    // as we only need the key metadata from the Parquet files to determine the decryption keys.
    parquet_options
        .crypto
        .configure_factory(ENCRYPTION_FACTORY_ID, &EncryptionConfig::default());

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
            'format.crypto.factory_options.encrypted_columns' 'b,c' \
        )"
    );
    let _ = ctx.sql(&query).await?.collect().await?;

    println!("Encrypted Parquet written to {table_path}");
    Ok(())
}

/// Read from an encrypted Parquet file using only the SQL API and string-based configuration
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
    struct EncryptionConfig {
        /// Comma-separated list of columns to encrypt
        pub encrypted_columns: String, default = "".to_owned()
    }
}

/// Mock implementation of an `EncryptionFactory` that stores encryption keys
/// base64 encoded in the Parquet encryption metadata.
/// For production use, integrating with a key-management service to encrypt
/// data encryption keys is recommended.
#[derive(Default, Debug)]
struct TestEncryptionFactory {}

/// `EncryptionFactory` is a DataFusion trait for types that generate
/// file encryption and decryption properties.
#[async_trait]
impl EncryptionFactory for TestEncryptionFactory {
    /// Generate file encryption properties to use when writing a Parquet file.
    /// The `schema` is provided so that it may be used to dynamically configure
    /// per-column encryption keys.
    /// The file path is also available. We don't use the path in this example,
    /// but other implementations may want to use this to compute an
    /// AAD prefix for the file, or to allow use of external key material
    /// (where key metadata is stored in a JSON file alongside Parquet files).
    async fn get_file_encryption_properties(
        &self,
        options: &EncryptionFactoryOptions,
        schema: &SchemaRef,
        _file_path: &Path,
    ) -> Result<Option<Arc<FileEncryptionProperties>>> {
        let config: EncryptionConfig = options.to_extension_options()?;

        // Generate a random encryption key for this file.
        let mut key = vec![0u8; 16];
        OsRng.try_fill_bytes(&mut key).unwrap();

        // Generate the key metadata that allows retrieving the key when reading the file.
        let key_metadata = wrap_key(&key);

        let mut builder = FileEncryptionProperties::builder(key.to_vec())
            .with_footer_key_metadata(key_metadata.clone());

        let encrypted_columns: HashSet<&str> =
            config.encrypted_columns.split(",").collect();
        if !encrypted_columns.is_empty() {
            // Set up per-column encryption.
            for field in schema.fields().iter() {
                if encrypted_columns.contains(field.name().as_str()) {
                    // Here we re-use the same key for all encrypted columns,
                    // but new keys could also be generated per column.
                    builder = builder.with_column_key_and_metadata(
                        field.name().as_str(),
                        key.clone(),
                        key_metadata.clone(),
                    );
                }
            }
        }

        let encryption_properties = builder.build()?;

        Ok(Some(encryption_properties))
    }

    /// Generate file decryption properties to use when reading a Parquet file.
    /// Rather than provide the AES keys directly for decryption, we set a `KeyRetriever`
    /// that can determine the keys using the encryption metadata.
    async fn get_file_decryption_properties(
        &self,
        _options: &EncryptionFactoryOptions,
        _file_path: &Path,
    ) -> Result<Option<Arc<FileDecryptionProperties>>> {
        let decryption_properties =
            FileDecryptionProperties::with_key_retriever(Arc::new(TestKeyRetriever {}))
                .build()?;
        Ok(Some(decryption_properties))
    }
}

/// Mock implementation of encrypting a key that simply base64 encodes the key.
/// Note that this is not a secure way to store encryption keys,
/// and for production use keys should be encrypted with a KMS.
fn wrap_key(key: &[u8]) -> Vec<u8> {
    base64::prelude::BASE64_STANDARD
        .encode(key)
        .as_bytes()
        .to_vec()
}

struct TestKeyRetriever {}

impl KeyRetriever for TestKeyRetriever {
    /// Get a data encryption key using the metadata stored in the Parquet file.
    fn retrieve_key(
        &self,
        key_metadata: &[u8],
    ) -> datafusion::parquet::errors::Result<Vec<u8>> {
        let key_metadata = std::str::from_utf8(key_metadata)?;
        let key = base64::prelude::BASE64_STANDARD
            .decode(key_metadata)
            .unwrap();
        Ok(key)
    }
}
