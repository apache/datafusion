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

//! Factory for creating ListingTables with default options

use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_expr::CreateExternalTable;
use object_store::aws::AmazonS3Builder;
use url::Url;

use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::file_format::avro::AvroFormat;
use crate::datasource::file_format::csv::CsvFormat;
use crate::datasource::file_format::file_type::{FileCompressionType, FileType};
use crate::datasource::file_format::json::JsonFormat;
use crate::datasource::file_format::parquet::ParquetFormat;
use crate::datasource::file_format::FileFormat;
use crate::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
pub struct ListingTableFactory {}

impl ListingTableFactory {
    /// Creates a new `ListingTableFactory`
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for ListingTableFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TableProviderFactory for ListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        cmd: &CreateExternalTable,
    ) -> datafusion_common::Result<Arc<dyn TableProvider>> {
        let file_compression_type = FileCompressionType::from(cmd.file_compression_type);
        let file_type = FileType::from_str(cmd.file_type.as_str()).map_err(|_| {
            DataFusionError::Execution(format!("Unknown FileType {}", cmd.file_type))
        })?;

        let file_extension =
            file_type.get_ext_with_compression(file_compression_type.to_owned())?;

        let file_format: Arc<dyn FileFormat> = match file_type {
            FileType::CSV => Arc::new(
                CsvFormat::default()
                    .with_has_header(cmd.has_header)
                    .with_delimiter(cmd.delimiter as u8)
                    .with_file_compression_type(file_compression_type),
            ),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat::default()),
            FileType::JSON => Arc::new(
                JsonFormat::default().with_file_compression_type(file_compression_type),
            ),
        };

        let (provided_schema, table_partition_cols) = if cmd.schema.fields().is_empty() {
            (
                None,
                cmd.table_partition_cols
                    .iter()
                    .map(|x| {
                        (
                            x.clone(),
                            DataType::Dictionary(
                                Box::new(DataType::UInt16),
                                Box::new(DataType::Utf8),
                            ),
                        )
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            let schema: SchemaRef = Arc::new(cmd.schema.as_ref().to_owned().into());
            let table_partition_cols = cmd
                .table_partition_cols
                .iter()
                .map(|col| {
                    schema
                        .field_with_name(col)
                        .map_err(DataFusionError::ArrowError)
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?
                .into_iter()
                .map(|f| (f.name().to_owned(), f.data_type().to_owned()))
                .collect();
            // exclude partition columns to support creating partitioned external table
            // with a specified column definition like
            // `create external table a(c0 int, c1 int) stored as csv partitioned by (c1)...`
            let mut project_idx = Vec::new();
            for i in 0..schema.fields().len() {
                if !cmd.table_partition_cols.contains(schema.field(i).name()) {
                    project_idx.push(i);
                }
            }
            let schema = Arc::new(schema.project(&project_idx)?);
            (Some(schema), table_partition_cols)
        };

        let file_sort_order = if cmd.order_exprs.is_empty() {
            None
        } else {
            Some(cmd.order_exprs.clone())
        };

        let options = ListingOptions::new(file_format)
            .with_collect_stat(state.config().collect_statistics())
            .with_file_extension(file_extension)
            .with_target_partitions(state.config().target_partitions())
            .with_table_partition_cols(table_partition_cols)
            .with_file_sort_order(file_sort_order);

        let table_path = ListingTableUrl::parse(&cmd.location)?;

        // try obtaining all relevant information of object store from cmd.options
        match table_path.scheme() {
            "s3" => {
                let url: &Url = table_path.as_ref();
                let bucket_name = url
                    .host_str()
                    .ok_or(DataFusionError::External("invaild bucket name".into()))?;
                let mut builder =
                    AmazonS3Builder::from_env().with_bucket_name(bucket_name);

                if let (Some(access_key_id), Some(secret_access_key)) = (
                    cmd.options.get("access_key_id"),
                    cmd.options.get("secret_access_key"),
                ) {
                    builder = builder
                        .with_access_key_id(access_key_id)
                        .with_secret_access_key(secret_access_key);
                }

                if let Some(session_token) = cmd.options.get("session_token") {
                    builder = builder.with_token(session_token);
                }

                if let Some(region) = cmd.options.get("region") {
                    builder = builder.with_region(region);
                }

                let store = Arc::new(builder.build()?);

                state
                    .runtime_env()
                    .register_object_store(table_path.as_ref(), store);
            }
            "oss" => {
                let url: &Url = table_path.as_ref();
                let bucket_name = url
                    .host_str()
                    .ok_or(DataFusionError::External("invaild bucket name".into()))?;
                let mut builder = AmazonS3Builder::from_env()
                    .with_virtual_hosted_style_request(true)
                    .with_bucket_name(bucket_name)
                    // oss don't care about the "region" field
                    .with_region("do_not_care");

                if let (Some(access_key_id), Some(secret_access_key)) = (
                    cmd.options.get("access_key_id"),
                    cmd.options.get("secret_access_key"),
                ) {
                    builder = builder
                        .with_access_key_id(access_key_id)
                        .with_secret_access_key(secret_access_key);
                }

                if let Some(endpoint) = cmd.options.get("endpoint") {
                    builder = builder.with_endpoint(endpoint);
                }

                let store = Arc::new(builder.build()?);

                state
                    .runtime_env()
                    .register_object_store(table_path.as_ref(), store);
            }
            _ => {}
        };

        let resolved_schema = match provided_schema {
            None => options.infer_schema(state, &table_path).await?,
            Some(s) => s,
        };
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table =
            ListingTable::try_new(config)?.with_definition(cmd.definition.clone());
        Ok(Arc::new(table))
    }
}
