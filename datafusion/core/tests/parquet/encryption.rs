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

//! Tests for reading and writing Parquet files that use Parquet modular encryption

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_common::config::{EncryptionFactoryOptions, TableParquetOptions};
use datafusion_common::{assert_batches_sorted_eq, DataFusionError};
use datafusion_datasource_parquet::ParquetFormat;
use datafusion_execution::parquet_encryption::EncryptionFactory;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::ArrowWriter;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::column_crypto_metadata::ColumnCryptoMetaData;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

async fn read_parquet_test_data<'a, T: Into<String>>(
    path: T,
    ctx: &SessionContext,
    options: ParquetReadOptions<'a>,
) -> Vec<RecordBatch> {
    ctx.read_parquet(path.into(), options)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

pub fn write_batches(
    path: PathBuf,
    props: WriterProperties,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> datafusion_common::Result<usize> {
    let mut batches = batches.into_iter();
    let first_batch = batches.next().expect("need at least one record batch");
    let schema = first_batch.schema();

    let file = File::create(&path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))?;

    writer.write(&first_batch)?;
    let mut num_rows = first_batch.num_rows();

    for batch in batches {
        writer.write(&batch)?;
        num_rows += batch.num_rows();
    }
    writer.close()?;
    Ok(num_rows)
}

#[tokio::test]
async fn round_trip_encryption() {
    let ctx: SessionContext = SessionContext::new();

    let options = ParquetReadOptions::default();
    let batches = read_parquet_test_data(
        "tests/data/filter_pushdown/single_file.gz.parquet",
        &ctx,
        options,
    )
    .await;

    let schema = batches[0].schema();
    let footer_key = b"0123456789012345".to_vec(); // 128bit/16
    let column_key = b"1234567890123450".to_vec(); // 128bit/16

    let mut encrypt = FileEncryptionProperties::builder(footer_key.clone());
    let mut decrypt = FileDecryptionProperties::builder(footer_key.clone());

    for field in schema.fields.iter() {
        encrypt = encrypt.with_column_key(field.name().as_str(), column_key.clone());
        decrypt = decrypt.with_column_key(field.name().as_str(), column_key.clone());
    }
    let encrypt = encrypt.build().unwrap();
    let decrypt = decrypt.build().unwrap();

    // Write encrypted parquet
    let props = WriterProperties::builder()
        .with_file_encryption_properties(encrypt)
        .build();

    let tempdir = TempDir::new_in(Path::new(".")).unwrap();
    let tempfile = tempdir.path().join("data.parquet");
    let num_rows_written = write_batches(tempfile.clone(), props, batches).unwrap();

    // Read encrypted parquet
    let ctx: SessionContext = SessionContext::new();
    let options =
        ParquetReadOptions::default().file_decryption_properties((&decrypt).into());

    let encrypted_batches = read_parquet_test_data(
        tempfile.into_os_string().into_string().unwrap(),
        &ctx,
        options,
    )
    .await;

    let num_rows_read = encrypted_batches
        .iter()
        .fold(0, |acc, x| acc + x.num_rows());

    assert_eq!(num_rows_written, num_rows_read);
}

#[tokio::test]
async fn round_trip_parquet_with_encryption_factory() {
    let ctx = SessionContext::new();
    let encryption_factory = Arc::new(MockEncryptionFactory::default());
    ctx.runtime_env().register_parquet_encryption_factory(
        "test_encryption_factory",
        Arc::clone(&encryption_factory) as Arc<dyn EncryptionFactory>,
    );

    let tmpdir = TempDir::new().unwrap();

    // Register some simple test data
    let strings: ArrayRef =
        Arc::new(StringArray::from(vec!["a", "b", "c", "a", "b", "c"]));
    let x1: ArrayRef = Arc::new(Int32Array::from(vec![1, 10, 11, 100, 101, 111]));
    let x2: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
    let batch =
        RecordBatch::try_from_iter(vec![("string", strings), ("x1", x1), ("x2", x2)])
            .unwrap();
    let test_data_schema = batch.schema();
    ctx.register_batch("test_data", batch).unwrap();
    let df = ctx.table("test_data").await.unwrap();

    // Write encrypted Parquet, partitioned by string column into separate files
    let mut parquet_options = TableParquetOptions::new();
    parquet_options.crypto.factory_id = Some("test_encryption_factory".to_string());
    parquet_options
        .crypto
        .factory_options
        .options
        .insert("test_key".to_string(), "test value".to_string());

    let df_write_options =
        DataFrameWriteOptions::default().with_partition_by(vec!["string".to_string()]);
    df.write_parquet(
        tmpdir.path().to_str().unwrap(),
        df_write_options,
        Some(parquet_options.clone()),
    )
    .await
    .unwrap();

    // Crypto factory should have generated one key per partition file
    assert_eq!(encryption_factory.encryption_keys.lock().unwrap().len(), 3);

    verify_table_encrypted(tmpdir.path(), &encryption_factory)
        .await
        .unwrap();

    // Registering table without decryption properties should fail
    let table_path = format!("file://{}/", tmpdir.path().to_str().unwrap());
    let without_decryption_register = ctx
        .register_listing_table(
            "parquet_missing_decryption",
            &table_path,
            ListingOptions::new(Arc::new(ParquetFormat::default())),
            None,
            None,
        )
        .await;
    assert!(matches!(
        without_decryption_register.unwrap_err(),
        DataFusionError::ParquetError(_)
    ));

    // Registering table succeeds if schema is provided
    ctx.register_listing_table(
        "parquet_missing_decryption",
        &table_path,
        ListingOptions::new(Arc::new(ParquetFormat::default())),
        Some(test_data_schema),
        None,
    )
    .await
    .unwrap();

    // But trying to read from the table should fail
    let without_decryption_read = ctx
        .table("parquet_missing_decryption")
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(
        without_decryption_read.unwrap_err(),
        DataFusionError::ParquetError(_)
    ));

    // Register table with encryption factory specified
    let listing_options = ListingOptions::new(Arc::new(
        ParquetFormat::default().with_options(parquet_options),
    ))
    .with_table_partition_cols(vec![("string".to_string(), DataType::Utf8)]);
    ctx.register_listing_table(
        "parquet_with_decryption",
        &table_path,
        listing_options,
        None,
        None,
    )
    .await
    .unwrap();

    // Can read correct data when encryption factory has been specified
    let table = ctx
        .table("parquet_with_decryption")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+-----+----+--------+",
        "| x1  | x2 | string |",
        "+-----+----+--------+",
        "| 1   | 1  | a      |",
        "| 100 | 4  | a      |",
        "| 10  | 2  | b      |",
        "| 101 | 5  | b      |",
        "| 11  | 3  | c      |",
        "| 111 | 6  | c      |",
        "+-----+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &table);
}

async fn verify_table_encrypted(
    table_path: &Path,
    encryption_factory: &Arc<MockEncryptionFactory>,
) -> datafusion_common::Result<()> {
    let mut directories = vec![table_path.to_path_buf()];
    let mut files_visited = 0;
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory)? {
            let path = entry?.path();
            if path.is_dir() {
                directories.push(path);
            } else {
                verify_file_encrypted(&path, encryption_factory).await?;
                files_visited += 1;
            }
        }
    }
    assert!(files_visited > 0);
    Ok(())
}

async fn verify_file_encrypted(
    file_path: &Path,
    encryption_factory: &Arc<MockEncryptionFactory>,
) -> datafusion_common::Result<()> {
    let mut options = EncryptionFactoryOptions::default();
    options
        .options
        .insert("test_key".to_string(), "test value".to_string());

    let file_path_str = if cfg!(target_os = "windows") {
        // Windows backslashes are eventually converted to slashes when writing the Parquet files,
        // through `ListingTableUrl::parse`, making `encryption_factory.encryption_keys` store them
        // it that format. So we also replace backslashes here to ensure they match.
        file_path.to_str().unwrap().replace("\\", "/")
    } else {
        file_path.to_str().unwrap().to_owned()
    };

    let object_path = object_store::path::Path::from(file_path_str);
    let decryption_properties = encryption_factory
        .get_file_decryption_properties(&options, &object_path)
        .await?
        .unwrap();

    let reader_options =
        ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
    let file = File::open(file_path)?;
    let reader_metadata = ArrowReaderMetadata::load(&file, reader_options)?;
    let metadata = reader_metadata.metadata();
    assert!(metadata.num_row_groups() > 0);
    for row_group in metadata.row_groups() {
        assert!(row_group.num_columns() > 0);
        for col in row_group.columns() {
            assert!(matches!(
                col.crypto_metadata(),
                Some(ColumnCryptoMetaData::ENCRYPTION_WITH_FOOTER_KEY)
            ));
        }
    }
    Ok(())
}

/// Encryption factory implementation for use in tests,
/// which generates encryption keys in a sequence
#[derive(Debug, Default)]
struct MockEncryptionFactory {
    pub encryption_keys: Mutex<HashMap<object_store::path::Path, Vec<u8>>>,
    pub counter: AtomicU8,
}

#[async_trait]
impl EncryptionFactory for MockEncryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        _schema: &SchemaRef,
        file_path: &object_store::path::Path,
    ) -> datafusion_common::Result<Option<FileEncryptionProperties>> {
        assert_eq!(
            config.options.get("test_key"),
            Some(&"test value".to_string())
        );
        let file_idx = self.counter.fetch_add(1, Ordering::Relaxed);
        let key = vec![file_idx, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut keys = self.encryption_keys.lock().unwrap();
        keys.insert(file_path.clone(), key.clone());
        let encryption_properties = FileEncryptionProperties::builder(key).build()?;
        Ok(Some(encryption_properties))
    }

    async fn get_file_decryption_properties(
        &self,
        config: &EncryptionFactoryOptions,
        file_path: &object_store::path::Path,
    ) -> datafusion_common::Result<Option<FileDecryptionProperties>> {
        assert_eq!(
            config.options.get("test_key"),
            Some(&"test value".to_string())
        );
        let keys = self.encryption_keys.lock().unwrap();
        let key = keys.get(file_path).ok_or_else(|| {
            DataFusionError::Execution(format!("No key for file {file_path:?}"))
        })?;
        let decryption_properties =
            FileDecryptionProperties::builder(key.clone()).build()?;
        Ok(Some(decryption_properties))
    }
}
