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

//! `read_csv` table function: reads CSV files as a table source.
//!
//! ```sql
//! SELECT * FROM read_csv('/path/to/file.csv')
//! SELECT * FROM read_csv('/path/to/dir/*.csv')
//! ```

use std::sync::Arc;

use datafusion_catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{plan_err, Result};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_csv::file_format::CsvFormat;

use crate::{extract_path, infer_schema_blocking};

/// Table function that reads CSV files.
#[derive(Debug, Default)]
pub struct ReadCsvFunc;

impl TableFunctionImpl for ReadCsvFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        if exprs.len() != 1 {
            return plan_err!(
                "read_csv requires exactly 1 argument (path), got {}",
                exprs.len()
            );
        }

        let path = extract_path(&exprs[0], "read_csv")?;
        let session = args.session();

        let table_path = ListingTableUrl::parse(&path)?;

        let csv_format = CsvFormat::default()
            .with_options(session.default_table_options().csv);

        let listing_options = ListingOptions::new(Arc::new(csv_format))
            .with_file_extension(".csv")
            .with_session_config_options(session.config());

        let schema = infer_schema_blocking(&listing_options, session, &table_path)?;

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        let table = ListingTable::try_new(config)?;
        Ok(Arc::new(table))
    }
}
