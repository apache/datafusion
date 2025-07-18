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

//! non trivial integration testing for parquet predicate pushdown
//!
//! Testing hints: If you run this test with --nocapture it will tell you where
//! the generated parquet file went. You can then test it and try out various queries
//! datafusion-cli like:
//!
//! ```sql
//! create external table data stored as parquet location 'data.parquet';
//! select * from data limit 10;
//! ```

use arrow::record_batch::RecordBatch;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parquet::arrow::ArrowWriter;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterProperties;
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

#[cfg(feature = "parquet_encryption")]
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
