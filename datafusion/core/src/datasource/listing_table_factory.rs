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

use crate::datasource::datasource::TableProviderFactory;
use crate::datasource::file_format::avro::AvroFormat;
use crate::datasource::file_format::csv::CsvFormat;
use crate::datasource::file_format::file_type::{FileType, GetExt};
use crate::datasource::file_format::json::JsonFormat;
use crate::datasource::file_format::parquet::ParquetFormat;
use crate::datasource::file_format::FileFormat;
use crate::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use crate::datasource::TableProvider;
use crate::execution::context::SessionState;
use async_trait::async_trait;
use std::sync::Arc;

/// A `TableProviderFactory` capable of creating new `ListingTable`s
pub struct ListingTableFactory {
    file_type: FileType,
}

impl ListingTableFactory {
    /// Creates a new `ListingTableFactory`
    pub fn new(file_type: FileType) -> Self {
        Self { file_type }
    }
}

#[async_trait]
impl TableProviderFactory for ListingTableFactory {
    async fn create(
        &self,
        state: &SessionState,
        url: &str,
    ) -> datafusion_common::Result<Arc<dyn TableProvider>> {
        let file_extension = self.file_type.get_ext();

        let file_format: Arc<dyn FileFormat> = match self.file_type {
            FileType::CSV => Arc::new(CsvFormat::default()),
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::AVRO => Arc::new(AvroFormat::default()),
            FileType::JSON => Arc::new(JsonFormat::default()),
        };

        let options = ListingOptions {
            format: file_format,
            collect_stat: true,
            file_extension: file_extension.to_owned(),
            target_partitions: 1,
            table_partition_cols: vec![],
        };

        let table_path = ListingTableUrl::parse(url)?;
        let resolved_schema = options.infer_schema(state, &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }
}
