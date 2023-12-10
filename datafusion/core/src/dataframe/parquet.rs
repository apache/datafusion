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

use datafusion_common::file_options::parquet_writer::{
    default_builder, ParquetWriterOptions,
};
use parquet::file::properties::WriterProperties;

use super::{
    CompressionTypeVariant, CopyOptions, DataFrame, DataFrameWriteOptions,
    DataFusionError, FileType, FileTypeWriterOptions, LogicalPlanBuilder, RecordBatch,
};

impl DataFrame {
    /// Write a `DataFrame` to a Parquet file.
    pub async fn write_parquet(
        self,
        path: &str,
        options: DataFrameWriteOptions,
        writer_properties: Option<WriterProperties>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        if options.overwrite {
            return Err(DataFusionError::NotImplemented(
                "Overwrites are not implemented for DataFrame::write_parquet.".to_owned(),
            ));
        }
        match options.compression{
            CompressionTypeVariant::UNCOMPRESSED => (),
            _ => return Err(DataFusionError::Configuration("DataFrame::write_parquet method does not support compression set via DataFrameWriteOptions. Set parquet compression via writer_properties instead.".to_owned()))
        }
        let props = match writer_properties {
            Some(props) => props,
            None => default_builder(self.session_state.config_options())?.build(),
        };
        let file_type_writer_options =
            FileTypeWriterOptions::Parquet(ParquetWriterOptions::new(props));
        let copy_options = CopyOptions::WriterOptions(Box::new(file_type_writer_options));
        let plan = LogicalPlanBuilder::copy_to(
            self.plan,
            path.into(),
            FileType::PARQUET,
            options.single_file_output,
            copy_options,
        )?
        .build()?;
        DataFrame::new(self.session_state, plan).collect().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use parquet::basic::{BrotliLevel, GzipLevel, ZstdLevel};
    use parquet::file::reader::FileReader;
    use tempfile::TempDir;
    use url::Url;

    use datafusion_expr::{col, lit};

    use crate::arrow::util::pretty;
    use crate::execution::context::SessionContext;
    use crate::execution::options::ParquetReadOptions;
    use crate::test_util;

    use super::super::Result;
    use super::*;

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
            parquet::basic::Compression::SNAPPY,
            parquet::basic::Compression::LZ4,
            parquet::basic::Compression::LZ4_RAW,
            parquet::basic::Compression::GZIP(GzipLevel::default()),
            parquet::basic::Compression::BROTLI(BrotliLevel::default()),
            parquet::basic::Compression::ZSTD(ZstdLevel::default()),
        ];
        for compression in test_compressions.into_iter() {
            let df = test_df.clone();
            let tmp_dir = TempDir::new()?;
            let local = Arc::new(LocalFileSystem::new_with_prefix(&tmp_dir)?);
            let local_url = Url::parse("file://local").unwrap();
            let ctx = &test_df.session_state;
            ctx.runtime_env().register_object_store(&local_url, local);
            df.write_parquet(
                output_path,
                DataFrameWriteOptions::new().with_single_file_output(true),
                Some(
                    WriterProperties::builder()
                        .set_compression(compression)
                        .build(),
                ),
            )
            .await?;

            // Check that file actually used the specified compression
            let file = std::fs::File::open(tmp_dir.into_path().join("test.parquet"))?;

            let reader =
                parquet::file::serialized_reader::SerializedFileReader::new(file)
                    .unwrap();

            let parquet_metadata = reader.metadata();

            let written_compression =
                parquet_metadata.row_group(0).column(0).compression();

            assert_eq!(written_compression, compression);
        }

        Ok(())
    }
}
