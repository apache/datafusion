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

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

pub mod generate_series;
#[cfg(feature = "avro")]
pub mod read_avro;
// CSV and JSON are always available — no heavy optional dependencies
pub mod read_csv;
pub mod read_json;
#[cfg(feature = "parquet")]
pub mod read_parquet;

use arrow::datatypes::SchemaRef;
use datafusion_catalog::{Session, TableFunction};
use datafusion_catalog_listing::ListingOptions;
use datafusion_common::{plan_err, Result};
use datafusion_datasource::ListingTableUrl;
use datafusion_expr::Expr;
use std::sync::Arc;

/// Extract a string path from a literal expression.
pub(crate) fn extract_path(expr: &Expr, func_name: &str) -> Result<String> {
    match expr {
        Expr::Literal(scalar, _) => match scalar.try_as_str() {
            Some(Some(s)) => Ok(s.to_string()),
            _ => plan_err!(
                "{func_name} requires a string literal path argument, got {scalar:?}"
            ),
        },
        _ => plan_err!(
            "{func_name} requires a string literal path argument, got {expr:?}"
        ),
    }
}

/// Bridge async `infer_schema` into the sync `call_with_args` context.
///
/// Spawns a scoped OS thread so the Tokio executor thread is never blocked.
/// Inside the spawned thread, [`tokio::runtime::Handle::block_on`] drives
/// the future to completion.  This works on **both** multi-thread and
/// current-thread Tokio runtimes (unlike `block_in_place`, which panics on
/// single-threaded runtimes).
pub(crate) fn infer_schema_blocking(
    listing_options: &ListingOptions,
    session: &dyn Session,
    table_path: &ListingTableUrl,
) -> Result<SchemaRef> {
    let handle = tokio::runtime::Handle::current();
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                handle
                    .block_on(listing_options.infer_schema(session, table_path))
            })
            .join()
            .expect("infer_schema thread panicked")
    })
}

/// Returns all default table functions
pub fn all_default_table_functions() -> Vec<Arc<TableFunction>> {
    #[cfg(any(feature = "parquet", feature = "avro"))]
    let mut funcs = vec![generate_series(), range(), read_csv(), read_json()];
    #[cfg(not(any(feature = "parquet", feature = "avro")))]
    let funcs = vec![generate_series(), range(), read_csv(), read_json()];

    #[cfg(feature = "parquet")]
    funcs.push(read_parquet());

    #[cfg(feature = "avro")]
    funcs.push(read_avro());

    funcs
}

/// Creates a singleton instance of a table function
/// - `$module`: A struct implementing `TableFunctionImpl` to create the function from
/// - `$name`: The name to give to the created function
/// - `$func_name`: The name of the function to be called
///   This is used to ensure creating the list of `TableFunction` only happens once.
#[macro_export]
macro_rules! create_udtf_function {
    ($module:expr, $func_name:ident, $name:expr) => {
        pub fn $func_name() -> Arc<TableFunction> {
            static INSTANCE: std::sync::LazyLock<Arc<TableFunction>> =
                std::sync::LazyLock::new(|| {
                    std::sync::Arc::new(TableFunction::new(
                        $name.to_string(),
                        Arc::new($module),
                    ))
                });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}

create_udtf_function!(
    generate_series::GenerateSeriesFunc {},
    generate_series,
    "generate_series"
);
create_udtf_function!(generate_series::RangeFunc {}, range, "range");
create_udtf_function!(read_csv::ReadCsvFunc {}, read_csv, "read_csv");
create_udtf_function!(read_json::ReadJsonFunc {}, read_json, "read_json");

#[cfg(feature = "parquet")]
create_udtf_function!(
    read_parquet::ReadParquetFunc {},
    read_parquet,
    "read_parquet"
);

#[cfg(feature = "avro")]
create_udtf_function!(read_avro::ReadAvroFunc {}, read_avro, "read_avro");
