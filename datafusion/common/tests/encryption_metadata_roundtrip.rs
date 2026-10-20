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

//! Preserve the association between column keys and optional key metadata.

#![cfg(feature = "parquet_encryption")]

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use datafusion_common::config::ConfigFileEncryptionProperties;
use datafusion_common::encryption::{FileDecryptionProperties, FileEncryptionProperties};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::column_crypto_metadata::ColumnCryptoMetaData;
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

const FOOTER_KEY: &[u8] = b"0123456789abcdef";
const COLUMN_NAMES: [&str; 4] = ["first", "second", "third", "fourth"];

fn properties(metadata: &[Option<&[u8]>]) -> Arc<FileEncryptionProperties> {
    let mut builder = FileEncryptionProperties::builder(FOOTER_KEY.to_vec())
        .with_footer_key_metadata(b"footer-key-id".to_vec());
    for (i, metadata) in metadata.iter().enumerate() {
        let key = vec![i as u8; 16];
        builder = match metadata {
            Some(metadata) => builder.with_column_key_and_metadata(
                COLUMN_NAMES[i],
                key,
                metadata.to_vec(),
            ),
            None => builder.with_column_key(COLUMN_NAMES[i], key),
        };
    }
    builder.build().unwrap()
}

fn assert_roundtrip(metadata: &[Option<&[u8]>]) {
    // Each newly built map has its own iteration order. Associations must not
    // depend on where a column happens to fall in that order.
    for _ in 0..32 {
        let original = properties(metadata);
        let config = ConfigFileEncryptionProperties::from(&original);
        for (i, expected_metadata) in metadata.iter().enumerate() {
            let column = &config.column_encryption_properties[COLUMN_NAMES[i]];
            assert_eq!(column.column_key_as_hex, hex::encode(vec![i as u8; 16]));
            assert_eq!(
                column.column_metadata_as_hex,
                expected_metadata.map(hex::encode),
                "metadata moved away from column {}",
                COLUMN_NAMES[i]
            );
        }

        let restored = FileEncryptionProperties::try_from(config).unwrap();
        assert_eq!(restored.footer_key(), FOOTER_KEY);
        let (names, keys, _) = restored.column_keys();
        let keys: HashMap<_, _> = names.into_iter().zip(keys).collect();
        for i in 0..metadata.len() {
            assert_eq!(keys[COLUMN_NAMES[i]], vec![i as u8; 16]);
        }
        assert_eq!(original.as_ref(), &restored);
    }
}

#[test]
fn mixed_column_metadata_preserves_each_association() {
    assert_roundtrip(&[Some(b"first-key-id"), None, Some(b""), None]);
}

#[test]
fn all_column_metadata_preserves_each_association() {
    assert_roundtrip(&[
        Some(b"first-key-id"),
        Some(b"second-key-id"),
        Some(b"third-key-id"),
        Some(b"fourth-key-id"),
    ]);
}

#[test]
fn absent_column_metadata_remains_absent() {
    assert_roundtrip(&[None, None, None, None]);
}

struct RemoveOnDrop(PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn assert_encrypted_file_metadata(
    properties: Arc<FileEncryptionProperties>,
    expected_metadata: &[Option<&[u8]>],
) {
    let first: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
    let second: ArrayRef = Arc::new(Int32Array::from(vec![Some(4), Some(5), None]));
    let batch =
        RecordBatch::try_from_iter(vec![("first", first), ("second", second)]).unwrap();

    // Write entirely in memory. The temporary file supplies the reader's public
    // File-based ChunkReader without adding a byte-buffer dependency to this crate.
    let writer_properties = WriterProperties::builder()
        .with_file_encryption_properties(properties)
        .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), batch.schema(), Some(writer_properties))
            .unwrap();
    writer.write(&batch).unwrap();
    let bytes = writer.into_inner().unwrap();
    let path = std::env::temp_dir().join(format!(
        "datafusion-encryption-metadata-{}.parquet",
        Uuid::new_v4()
    ));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let cleanup = RemoveOnDrop(path);
    file.write_all(&bytes).unwrap();

    let mut decrypt = FileDecryptionProperties::builder(FOOTER_KEY.to_vec());
    for (i, column_name) in COLUMN_NAMES[..2].iter().enumerate() {
        decrypt = decrypt.with_column_key(column_name, vec![i as u8; 16]);
    }
    let options = ArrowReaderOptions::new()
        .with_file_decryption_properties(decrypt.build().unwrap());
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
    for row_group in builder.metadata().row_groups() {
        for (i, column) in row_group.columns().iter().enumerate() {
            let Some(ColumnCryptoMetaData::ENCRYPTION_WITH_COLUMN_KEY(crypto)) =
                column.crypto_metadata()
            else {
                panic!("expected a column-specific encryption key");
            };
            assert_eq!(crypto.path_in_schema, vec![COLUMN_NAMES[i].to_string()]);
            assert_eq!(
                crypto.key_metadata.as_deref(),
                expected_metadata[i],
                "file metadata changed for column {}",
                COLUMN_NAMES[i]
            );
        }
    }
    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches, vec![batch]);
    // The reader (and its File) has been consumed before removing the file.
    drop(cleanup);
}

#[test]
fn encrypted_files_preserve_optional_column_metadata_after_conversion() {
    for metadata in [
        [Some(b"first-key-id".as_slice()), None],
        [None, Some(b"".as_slice())],
    ] {
        // Repeat with independent maps so metadata placement is not tested with
        // only one possible column iteration order.
        for _ in 0..16 {
            let original = properties(&metadata);
            let config = ConfigFileEncryptionProperties::from(&original);
            let restored = Arc::new(FileEncryptionProperties::try_from(config).unwrap());
            assert_encrypted_file_metadata(original, &metadata);
            assert_encrypted_file_metadata(restored, &metadata);
        }
    }
}
