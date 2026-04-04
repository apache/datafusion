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

//! `read_parquet` table function: reads Parquet files as a table source.
//!
//! ```sql
//! SELECT * FROM read_parquet('/path/to/file.parquet')
//! SELECT * FROM read_parquet('/path/to/dir/*.parquet')
//! SELECT count(*) FROM read_parquet('s3://bucket/prefix/*.parquet')
//! ```

use std::sync::Arc;

use datafusion_catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{plan_err, Result};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;

use crate::extract_path;

use tokio::runtime::Handle;
use tokio::task::block_in_place;

/// Table function that reads Parquet files.
#[derive(Debug, Default)]
pub struct ReadParquetFunc;

impl TableFunctionImpl for ReadParquetFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.len() != 1 {
            return plan_err!(
                "read_parquet requires exactly 1 argument (path), got {}",
                exprs.len()
            );
        }

        let path = extract_path(&exprs[0], "read_parquet")?;
        let session = args.session();

        let table_path = ListingTableUrl::parse(&path)?;

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_session_config_options(session.config());

        let schema = block_in_place(|| {
            Handle::current()
                .block_on(listing_options.infer_schema(session, &table_path))
        })?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }
}
