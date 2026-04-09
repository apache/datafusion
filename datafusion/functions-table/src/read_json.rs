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

//! `read_json` table function: reads JSON files as a table source.
//!
//! ```sql
//! SELECT * FROM read_json('/path/to/file.json')
//! SELECT * FROM read_json('/path/to/dir/*.json')
//! ```

use std::sync::Arc;

use datafusion_catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{Result, plan_err};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_json::file_format::JsonFormat;

use crate::{extract_path, infer_schema_blocking};

/// Table function that reads JSON files.
#[derive(Debug, Default)]
pub struct ReadJsonFunc;

impl TableFunctionImpl for ReadJsonFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.len() != 1 {
            return plan_err!(
                "read_json requires exactly 1 argument (path), got {}",
                exprs.len()
            );
        }

        let path = extract_path(&exprs[0], "read_json")?;
        let session = args.session();

        let table_path = ListingTableUrl::parse(&path)?;

        let json_format =
            JsonFormat::default().with_options(session.default_table_options().json);

        let listing_options = ListingOptions::new(Arc::new(json_format))
            .with_file_extension(".json")
            .with_session_config_options(session.config());

        let schema = infer_schema_blocking(&listing_options, session, &table_path)?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }
}
