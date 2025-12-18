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

use datafusion::common::DataFusionError;
use datafusion::config::{ConfigFileEncryptionProperties, TableParquetOptions};
use datafusion::dataframe::{DataFrame, DataFrameWriteOptions};
use datafusion::logical_expr::{col, lit};
use datafusion::parquet::encryption::decrypt::FileDecryptionProperties;
use datafusion::parquet::encryption::encrypt::FileEncryptionProperties;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use std::sync::Arc;
use tempfile::TempDir;

/// Read and write encrypted Parquet files using DataFusion
pub async fn parquet_encrypted() -> datafusion::common::Result<()> {
    // The SessionContext is the main high level API for interacting with DataFusion
    let ctx = SessionContext::new();

    // Find the local path of "alltypes_plain.parquet"
    let testdata = datafusion::test_util::parquet_test_data();
    let filename = &format!("{testdata}/alltypes_plain.parquet");

    // Read the sample parquet file
    let parquet_df = ctx
        .read_parquet(filename, ParquetReadOptions::default())
        .await?;

    // Show information from the dataframe
    println!(
        "==============================================================================="
    );
    println!("Original Parquet DataFrame:");
    query_dataframe(&parquet_df).await?;

    // Setup encryption and decryption properties
    let (encrypt, decrypt) = setup_encryption(&parquet_df)?;

    // Create a temporary file location for the encrypted parquet file
    let tmp_dir = TempDir::new()?;
    let tempfile = tmp_dir.path().join("alltypes_plain-encrypted.parquet");
    let tempfile_str = tempfile.into_os_string().into_string().unwrap();

    // Write encrypted parquet
    let mut options = TableParquetOptions::default();
    options.crypto.file_encryption = Some(ConfigFileEncryptionProperties::from(&encrypt));
    parquet_df
        .write_parquet(
            tempfile_str.as_str(),
            DataFrameWriteOptions::new().with_single_file_output(true),
            Some(options),
        )
        .await?;

    // Read encrypted parquet
    let ctx: SessionContext = SessionContext::new();
    let read_options =
        ParquetReadOptions::default().file_decryption_properties((&decrypt).into());

    let encrypted_parquet_df = ctx.read_parquet(tempfile_str, read_options).await?;

    // Show information from the dataframe
    println!(
        "\n\n==============================================================================="
    );
    println!("Encrypted Parquet DataFrame:");
    query_dataframe(&encrypted_parquet_df).await?;

    Ok(())
}

// Show information from the dataframe
async fn query_dataframe(df: &DataFrame) -> Result<(), DataFusionError> {
    // show its schema using 'describe'
    println!("Schema:");
    df.clone().describe().await?.show().await?;

    // Select three columns and filter the results
    // so that only rows where id > 1 are returned
    println!("\nSelected rows and columns:");
    df.clone()
        .select_columns(&["id", "bool_col", "timestamp_col"])?
        .filter(col("id").gt(lit(5)))?
        .show()
        .await?;

    Ok(())
}

// Setup encryption and decryption properties
fn setup_encryption(
    parquet_df: &DataFrame,
) -> Result<(Arc<FileEncryptionProperties>, Arc<FileDecryptionProperties>), DataFusionError>
{
    let schema = parquet_df.schema();
    let footer_key = b"0123456789012345".to_vec(); // 128bit/16
    let column_key = b"1234567890123450".to_vec(); // 128bit/16

    let mut encrypt = FileEncryptionProperties::builder(footer_key.clone());
    let mut decrypt = FileDecryptionProperties::builder(footer_key.clone());

    for field in schema.fields().iter() {
        encrypt = encrypt.with_column_key(field.name().as_str(), column_key.clone());
        decrypt = decrypt.with_column_key(field.name().as_str(), column_key.clone());
    }

    let encrypt = encrypt.build()?;
    let decrypt = decrypt.build()?;
    Ok((encrypt, decrypt))
}
