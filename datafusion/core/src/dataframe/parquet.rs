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

use super::{
    DataFrame, DataFrameWriteOptions, DataFusionError, LogicalPlanBuilder, RecordBatch,
};

use datafusion_common::config::{FormatOptions, TableParquetOptions};

impl DataFrame {
    /// Execute the `DataFrame` and write the results to Parquet file(s).
    ///
    /// # Example
    /// ```
    /// # use datafusion::prelude::*;
    /// # use datafusion::error::Result;
    /// # use std::fs;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// use datafusion::dataframe::DataFrameWriteOptions;
    /// let ctx = SessionContext::new();
    /// // Sort the data by column "b" and write it to a new location
    /// ctx.read_csv("tests/data/example.csv", CsvReadOptions::new()).await?
    ///   .sort(vec![col("b").sort(true, true)])? // sort by b asc, nulls first
    ///   .write_parquet(
    ///     "output.parquet",
    ///     DataFrameWriteOptions::new(),
    ///     None, // can also specify parquet writing options here
    /// ).await?;
    /// # fs::remove_file("output.parquet")?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_parquet(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_options: Option<TableParquetOptions>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_parquet.".to_owned(),
            ));
        }

        let props = writer_options
            .unwrap_or_else(|| self.session_state.default_table_options().parquet);

        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FormatOptions::PARQUET(props),
            Default::default(),
            options.partition_by,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::super::Result;
    use super::*;
    use crate::arrow::util::pretty;
    use crate::execution::context::SessionContext;
    use crate::execution::options::ParquetReadOptions;
    use crate::test_util::{self, register_aggregate_csv};

    use datafusion_common::file_options::parquet_writer::parse_compression_string;
    use datafusion_execution::config::SessionConfig;
    use datafusion_expr::{col, lit};

    use object_store::local::LocalFileSystem;
    use parquet::file::reader::FileReader;
    use tempfile::TempDir;
    use url::Url;

    #[tokio::test]
    async fn filter_pushdown_dataframe() -> Result<()> {
        let ctx = SessionContext::new();

        ctx.register_parquet(
            "test",
            &format!(
                "{}/alltypes_plain.snappy.parquet",
                test_util::parquet_test_data()
            ),
            ParquetReadOptions::default(),
        )
        .await?;

        ctx.register_table("t1", ctx.table("test").await?.into_view())?;

        let df = ctx
            .table("t1")
            .await?
            .filter(col("id").eq(lit(1)))?
            .select_columns(&["bool_col", "int_col"])?;

        let plan = df.explain(false, false)?.collect().await?;
        // Filters all the way to Parquet
        let formatted = pretty::pretty_format_batches(&plan)?.to_string();
        assert!(formatted.contains("FilterExec: id@0 = 1"));

        Ok(())
    }

    #[tokio::test]
    async fn write_parquet_with_compression() -> Result<()> {
        let test_df = test_util::test_table().await?;
        let output_path = "file://local/test.parquet";
        let test_compressions = vec![
            "snappy",
            "brotli(1)",
            "lz4",
            "lz4_raw",
            "gzip(6)",
            "zstd(1)",
        ];
        for compression in test_compressions.into_iter() {
            let df = test_df.clone();
            let tmp_dir = TempDir::new()?;
            let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
            let local_url = Url::parse("file://local").unwrap();
            let ctx = &test_df.session_state;
            ctx.runtime_env().register_object_store(&local_url, local);
            let mut options = TableParquetOptions::default();
            options.global.compression = Some(compression.to_string());
            df.write_parquet(
                output_path,
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(options),
            )
            .await?;

            // Check that file actually used the specified compression
            let file = std::fs::File::open(tmp_dir.path().join("test.parquet"))?;

            let reader =
                parquet::file::serialized_reader::SerializedFileReader::new(file)
                    .unwrap();

            let parquet_metadata = reader.metadata();

            let written_compression =
                parquet_metadata.row_group(0).column(0).compression();

            assert_eq!(written_compression, parse_compression_string(compression)?);
        }

        Ok(())
    }

    #[tokio::test]
    async fn write_parquet_with_small_rg_size() -> Result<()> {
        // This test verifies writing a parquet file with small rg size
        // relative to datafusion.execution.batch_size does not panic
        let mut ctx = SessionContext::new_with_config(
            SessionConfig::from_string_hash_map(HashMap::from_iter(
                [("datafusion.execution.batch_size", "10")]
                    .iter()
                    .map(|(s1, s2)| (s1.to_string(), s2.to_string())),
            ))?,
        );
        register_aggregate_csv(&mut ctx, "aggregate_test_100").await?;
        let test_df = ctx.table("aggregate_test_100").await?;

        let output_path = "file://local/test.parquet";

        for rg_size in 1..10 {
            let df = test_df.clone();
            let tmp_dir = TempDir::new()?;
            let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
            let local_url = Url::parse("file://local").unwrap();
            let ctx = &test_df.session_state;
            ctx.runtime_env().register_object_store(&local_url, local);
            let mut options = TableParquetOptions::default();
            options.global.max_row_group_size = rg_size;
            options.global.allow_single_file_parallelism = true;
            df.write_parquet(
                output_path,
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(options),
            )
            .await?;

            // Check that file actually used the correct rg size
            let file = std::fs::File::open(tmp_dir.path().join("test.parquet"))?;

            let reader =
                parquet::file::serialized_reader::SerializedFileReader::new(file)
                    .unwrap();

            let parquet_metadata = reader.metadata();

            let written_rows = parquet_metadata.row_group(0).num_rows();

            assert_eq!(written_rows as usize, rg_size);
        }

        Ok(())
    }
}
