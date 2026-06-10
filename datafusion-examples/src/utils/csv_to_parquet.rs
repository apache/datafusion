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

use std::path::{Path, PathBuf};

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{CsvReadOptions, SessionContext};
use tempfile::TempDir;
use tokio::fs::create_dir_all;

/// Temporary Parquet directory that is deleted when dropped.
#[derive(Debug)]
pub struct ParquetTemp {
    pub tmp_dir: TempDir,
    pub parquet_dir: PathBuf,
}

impl ParquetTemp {
    pub fn path(&self) -> &Path {
        &self.parquet_dir
    }

    pub fn path_str(&self) -> Result<&str> {
        self.parquet_dir.to_str().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "Parquet directory path is not valid UTF-8: {}",
                self.parquet_dir.display()
            ))
        })
    }

    pub fn file_uri(&self) -> Result<String> {
        Ok(format!("file://{}", self.path_str()?))
    }
}

/// Helper for examples: load a CSV file and materialize it as Parquet
/// in a temporary directory.
///
/// # Example
/// ```
/// use std::path::PathBuf;
/// use datafusion::prelude::*;
/// use datafusion_examples::utils::write_csv_to_parquet;
/// # use datafusion::assert_batches_eq;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
///     .join("data")
///     .join("csv")
///     .join("cars.csv");
/// let parquet_dir = write_csv_to_parquet(&ctx, &csv_path).await?;
/// let df = ctx.read_parquet(parquet_dir.path_str()?, ParquetReadOptions::default()).await?;
/// let rows = df
///    .sort(vec![col("speed").sort(true, true)])?
///    .limit(0, Some(5))?;
/// assert_batches_eq!(
///     &[
///        "+-------+-------+---------------------+",
///        "| car   | speed | time                |",
///        "+-------+-------+---------------------+",
///        "| red   | 0.0   | 1996-04-12T12:05:15 |",
///        "| red   | 1.0   | 1996-04-12T12:05:14 |",
///        "| green | 2.0   | 1996-04-12T12:05:14 |",
///        "| red   | 3.0   | 1996-04-12T12:05:13 |",
///        "| red   | 7.0   | 1996-04-12T12:05:10 |",
///        "+-------+-------+---------------------+",
///      ],
///        &rows.collect().await?
/// );
/// # Ok(())
/// # }
/// ```
pub async fn write_csv_to_parquet(
    ctx: &SessionContext,
    csv_path: &Path,
) -> Result<ParquetTemp> {
    if !csv_path.is_file() {
        return Err(DataFusionError::Execution(format!(
            "CSV file does not exist: {}",
            csv_path.display()
        )));
    }

    let csv_path = csv_path.to_str().ok_or_else(|| {
        DataFusionError::Execution("CSV path is not valid UTF-8".to_string())
    })?;

    let csv_df = ctx.read_csv(csv_path, CsvReadOptions::default()).await?;

    let tmp_dir = TempDir::new()?;
    let parquet_dir = tmp_dir.path().join("parquet_source");
    create_dir_all(&parquet_dir).await?;

    let path = parquet_dir.to_str().ok_or_else(|| {
        DataFusionError::Execution("Failed processing tmp directory path".to_string())
    })?;

    csv_df
        .write_parquet(path, DataFrameWriteOptions::default(), None)
        .await?;

    Ok(ParquetTemp {
        tmp_dir,
        parquet_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use datafusion::assert_batches_eq;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_write_csv_to_parquet_with_cars_data() -> Result<()> {
        let ctx = SessionContext::new();
        let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join("csv")
            .join("cars.csv");

        let parquet_dir = write_csv_to_parquet(&ctx, &csv_path).await?;
        let df = ctx
            .read_parquet(parquet_dir.path_str()?, ParquetReadOptions::default())
            .await?;

        let rows = df.sort(vec![col("speed").sort(true, true)])?;
        assert_batches_eq!(
            &[
                "+-------+-------+---------------------+",
                "| car   | speed | time                |",
                "+-------+-------+---------------------+",
                "| red   | 0.0   | 1996-04-12T12:05:15 |",
                "| red   | 1.0   | 1996-04-12T12:05:14 |",
                "| green | 2.0   | 1996-04-12T12:05:14 |",
                "| red   | 3.0   | 1996-04-12T12:05:13 |",
                "| red   | 7.0   | 1996-04-12T12:05:10 |",
                "| red   | 7.1   | 1996-04-12T12:05:11 |",
                "| red   | 7.2   | 1996-04-12T12:05:12 |",
                "| green | 8.0   | 1996-04-12T12:05:13 |",
                "| green | 10.0  | 1996-04-12T12:05:03 |",
                "| green | 10.3  | 1996-04-12T12:05:04 |",
                "| green | 10.4  | 1996-04-12T12:05:05 |",
                "| green | 10.5  | 1996-04-12T12:05:06 |",
                "| green | 11.0  | 1996-04-12T12:05:07 |",
                "| green | 12.0  | 1996-04-12T12:05:08 |",
                "| green | 14.0  | 1996-04-12T12:05:09 |",
                "| green | 15.0  | 1996-04-12T12:05:10 |",
                "| green | 15.1  | 1996-04-12T12:05:11 |",
                "| green | 15.2  | 1996-04-12T12:05:12 |",
                "| red   | 17.0  | 1996-04-12T12:05:09 |",
                "| red   | 18.0  | 1996-04-12T12:05:08 |",
                "| red   | 19.0  | 1996-04-12T12:05:07 |",
                "| red   | 20.0  | 1996-04-12T12:05:03 |",
                "| red   | 20.3  | 1996-04-12T12:05:04 |",
                "| red   | 21.4  | 1996-04-12T12:05:05 |",
                "| red   | 21.5  | 1996-04-12T12:05:06 |",
                "+-------+-------+---------------------+",
            ],
            &rows.collect().await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_to_parquet_with_regex_data() -> Result<()> {
        let ctx = SessionContext::new();
        let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join("csv")
            .join("regex.csv");

        let parquet_dir = write_csv_to_parquet(&ctx, &csv_path).await?;
        let df = ctx
            .read_parquet(parquet_dir.path_str()?, ParquetReadOptions::default())
            .await?;

        let rows = df.sort(vec![col("values").sort(true, true)])?;
        assert_batches_eq!(
            &[
                "+------------+--------------------------------------+-------------+-------+",
                "| values     | patterns                             | replacement | flags |",
                "+------------+--------------------------------------+-------------+-------+",
                "| 4000       | \\b4([1-9]\\d\\d|\\d[1-9]\\d|\\d\\d[1-9])\\b | xyz         |       |",
                "| 4010       | \\b4([1-9]\\d\\d|\\d[1-9]\\d|\\d\\d[1-9])\\b | xyz         |       |",
                "| ABC        | ^(A).*                               | B           | i     |",
                "| AbC        | (B|D)                                | e           |       |",
                "| Düsseldorf | [\\p{Letter}-]+                       | München     |       |",
                "| Köln       | [a-zA-Z]ö[a-zA-Z]{2}                 | Koln        |       |",
                "| aBC        | ^(b|c)                               | d           |       |",
                "| aBc        | (b|d)                                | e           | i     |",
                "| abc        | ^(a)                                 | bb\\1bb      | i     |",
                "| Москва     | [\\p{L}-]+                            | Moscow      |       |",
                "| اليوم      | ^\\p{Arabic}+$                        | Today       |       |",
                "+------------+--------------------------------------+-------------+-------+",
            ],
            &rows.collect().await?
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_write_csv_to_parquet_error() {
        let ctx = SessionContext::new();
        let csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("data")
            .join("csv")
            .join("file-does-not-exist.csv");

        let err = write_csv_to_parquet(&ctx, &csv_path).await.unwrap_err();
        match err {
            DataFusionError::Execution(msg) => {
                assert!(
                    msg.contains("CSV file does not exist"),
                    "unexpected error message: {msg}"
                );
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }
}
