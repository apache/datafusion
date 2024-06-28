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

//! This file contains several examples of how to run SQL queries using DataFusion
//!
//! * [`parquet`]: run SQL query against a single Parquet file
//! * [`parquet_multi_files`]: run SQL query against a table backed by multiple Parquet files
//! * [`regexp`]: regular expression functions

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::{assert_batches_eq, assert_contains};
use object_store::local::LocalFileSystem;
use std::path::Path;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    parquet().await?;
    parquet_multi_files().await?;
    regexp().await?;
    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (Parquet) and fetching results
async fn parquet() -> Result<()> {
    // create local session context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::parquet_test_data();

    // register parquet file with the execution context
    ctx.register_parquet(
        "alltypes_plain",
        &format!("{testdata}/alltypes_plain.parquet"),
        ParquetReadOptions::default(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain \
        WHERE id > 1 AND tinyint_col < double_col",
        )
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (a directory with multiple Parquet files) and fetching results.
///
/// The query is run twice, once showing how to used `register_listing_table`
/// with an absolute path, and once registering an ObjectStore to use a relative
/// path.
async fn parquet_multi_files() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let test_data = datafusion::test_util::parquet_test_data();

    // Configure listing options
    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        // This is a workaround for this example since `test_data` contains
        // many different parquet different files,
        // in practice use FileType::PARQUET.get_ext().
        .with_file_extension("alltypes_plain.parquet");

    // First example were we use an absolute path, which requires no additional setup.
    ctx.register_listing_table(
        "my_table",
        &format!("file://{test_data}/"),
        listing_options.clone(),
        None,
        None,
    )
    .await
    .unwrap();

    // execute the query
    let df = ctx
        .sql(
            "SELECT * \
        FROM my_table \
        LIMIT 1",
        )
        .await?;

    // print the results
    df.show().await?;

    // Second example were we temporarily move into the test data's parent directory and
    // simulate a relative path, this requires registering an ObjectStore.
    let cur_dir = std::env::current_dir()?;

    let test_data_path = Path::new(&test_data);
    let test_data_path_parent = test_data_path.parent().unwrap();

    std::env::set_current_dir(test_data_path_parent)?;

    let local_fs = Arc::new(LocalFileSystem::default());

    let u = url::Url::parse("file://./").unwrap();
    ctx.register_object_store(&u, local_fs);

    // Register a listing table - this will use all files in the directory as data sources
    // for the query
    ctx.register_listing_table(
        "relative_table",
        "./data",
        listing_options.clone(),
        None,
        None,
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT * \
        FROM relative_table \
        LIMIT 1",
        )
        .await?;

    // print the results
    df.show().await?;

    // Reset the current directory
    std::env::set_current_dir(cur_dir)?;

    Ok(())
}

/// This example demonstrates how to use the regexp_* functions
///
/// the full list of supported features and
/// syntax can be found at
/// https://docs.rs/regex/latest/regex/#syntax
///
/// Supported flags can be found at
/// https://docs.rs/regex/latest/regex/#grouping-and-flags
async fn regexp() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "examples",
        "../../datafusion/physical-expr/tests/data/regex.csv",
        CsvReadOptions::new(),
    )
    .await?;

    //
    //
    //regexp_like examples
    //
    //
    // regexp_like format is (regexp_like(text, regex[, flags])
    //

    // use sql and regexp_like function to test col 'values', against patterns in col 'patterns' without flags
    let result = ctx
        .sql("select regexp_like(values, patterns) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------------------------+",
            "| regexp_like(examples.values,examples.patterns) |",
            "+------------------------------------------------+",
            "| true                                           |",
            "| true                                           |",
            "| false                                          |",
            "| false                                          |",
            "| false                                          |",
            "| false                                          |",
            "| true                                           |",
            "| true                                           |",
            "| true                                           |",
            "| true                                           |",
            "| true                                           |",
            "+------------------------------------------------+",
        ],
        &result
    );

    // use sql and regexp_like function to test col 'values', against patterns in col 'patterns' with flags
    let result = ctx
        .sql("select regexp_like(values, patterns, flags) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+---------------------------------------------------------------+",
            "| regexp_like(examples.values,examples.patterns,examples.flags) |",
            "+---------------------------------------------------------------+",
            "| true                                                          |",
            "| true                                                          |",
            "| true                                                          |",
            "| false                                                         |",
            "| false                                                         |",
            "| false                                                         |",
            "| true                                                          |",
            "| true                                                          |",
            "| true                                                          |",
            "| true                                                          |",
            "| true                                                          |",
            "+---------------------------------------------------------------+",
        ],
        &result
    );

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let result = ctx
        .sql("select regexp_like('John Smith', '^.*Smith$'), regexp_like('Smith Jones', '^Smith.*$')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
    "+---------------------------------------------------+----------------------------------------------------+",
    "| regexp_like(Utf8(\"John Smith\"),Utf8(\"^.*Smith$\")) | regexp_like(Utf8(\"Smith Jones\"),Utf8(\"^Smith.*$\")) |",
    "+---------------------------------------------------+----------------------------------------------------+",
    "| true                                              | true                                               |",
    "+---------------------------------------------------+----------------------------------------------------+",
        ],
        &result
    );

    // look-around and back references are not supported for performance
    // reasons.
    // Note that an error may not always be returned but the result
    // if returned will always be false
    let result = ctx
        .sql(r"select regexp_like('(?<=[A-Z]\w )Smith', 'John Smith', 'i') as a")
        .await?
        .collect()
        .await;

    assert!(result.is_ok());
    let result = result.unwrap();
    assert_eq!(result.len(), 1);

    assert_batches_eq!(
        &[
            "+-------+",
            "| a     |",
            "+-------+",
            "| false |",
            "+-------+",
        ],
        &result
    );

    // invalid flags will result in an error
    let result = ctx
        .sql(r"select regexp_like('\b4(?!000)\d\d\d\b', 4010, 'g')")
        .await?
        .collect()
        .await;

    let expected = "regexp_like() does not support the \"global\" option";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // there is a size limit on the regex during regex compilation
    let result = ctx
        .sql("select regexp_like('aaaaa', 'a{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}{5}')")
        .await?
        .collect()
        .await;

    let expected = "Regular expression did not compile: CompiledTooBig";
    assert_contains!(result.unwrap_err().to_string(), expected);

    //
    //
    //regexp_match examples
    //
    //
    // regexp_match format is (regexp_match(text, regex[, flags])
    //

    let _ = ctx.table("examples").await?;

    // use sql and regexp_match function to test col 'values', against patterns in col 'patterns' without flags
    let result = ctx
        .sql("select regexp_match(values, patterns) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------------------------------------------------+",
            "| regexp_match(examples.values,examples.patterns) |",
            "+-------------------------------------------------+",
            "| [a]                                             |",
            "| [A]                                             |",
            "|                                                 |",
            "|                                                 |",
            "|                                                 |",
            "|                                                 |",
            "| [010]                                           |",
            "| [Düsseldorf]                                    |",
            "| [Москва]                                        |",
            "| [Köln]                                          |",
            "| [اليوم]                                         |",
            "+-------------------------------------------------+",
        ],
        &result
    );

    // use dataframe and regexp_match function to test col 'values', against patterns in col 'patterns' with flags
    let result = ctx
        .sql("select regexp_match(values, patterns, flags) from examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------+",
            "| regexp_match(examples.values,examples.patterns,examples.flags) |",
            "+----------------------------------------------------------------+",
            "| [a]                                                            |",
            "| [A]                                                            |",
            "| [B]                                                            |",
            "|                                                                |",
            "|                                                                |",
            "|                                                                |",
            "| [010]                                                          |",
            "| [Düsseldorf]                                                   |",
            "| [Москва]                                                       |",
            "| [Köln]                                                         |",
            "| [اليوم]                                                        |",
            "+----------------------------------------------------------------+",
        ],
        &result
    );

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let result = ctx
        .sql("select regexp_match('John Smith', '^.*Smith$'), regexp_match('Smith Jones', '^Smith.*$')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
    "+----------------------------------------------------+-----------------------------------------------------+",
    "| regexp_match(Utf8(\"John Smith\"),Utf8(\"^.*Smith$\")) | regexp_match(Utf8(\"Smith Jones\"),Utf8(\"^Smith.*$\")) |",
    "+----------------------------------------------------+-----------------------------------------------------+",
    "| [John Smith]                                       | [Smith Jones]                                       |",
    "+----------------------------------------------------+-----------------------------------------------------+",
        ],
        &result
    );

    //
    //
    //regexp_replace examples
    //
    //
    // regexp_replace format is (regexp_replace(text, regex, replace[, flags])
    //

    // use regexp_replace function against tables
    let result = ctx
        .sql("SELECT regexp_replace(values, patterns, replacement, concat('g', flags)) FROM examples")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
    "+---------------------------------------------------------------------------------------------------------+",
    "| regexp_replace(examples.values,examples.patterns,examples.replacement,concat(Utf8(\"g\"),examples.flags)) |",
    "+---------------------------------------------------------------------------------------------------------+",
    "| bbabbbc                                                                                                 |",
    "| B                                                                                                       |",
    "| aec                                                                                                     |",
    "| AbC                                                                                                     |",
    "| aBC                                                                                                     |",
    "| 4000                                                                                                    |",
    "| xyz                                                                                                     |",
    "| München                                                                                                 |",
    "| Moscow                                                                                                  |",
    "| Koln                                                                                                    |",
    "| Today                                                                                                   |",
    "+---------------------------------------------------------------------------------------------------------+",
        ],
        &result
    );

    // global flag example
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
    "+------------------------------------------------------------------------+",
    "| regexp_replace(Utf8(\"foobarbaz\"),Utf8(\"b(..)\"),Utf8(\"X\\1Y\"),Utf8(\"g\")) |",
    "+------------------------------------------------------------------------+",
    "| fooXarYXazY                                                            |",
    "+------------------------------------------------------------------------+",
        ],
        &result
    );

    // without global flag
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+--------------------------------------------------------------+",
            "| regexp_replace(Utf8(\"foobarbaz\"),Utf8(\"b(..)\"),Utf8(\"X\\1Y\")) |",
            "+--------------------------------------------------------------+",
            "| fooXarYbaz                                                   |",
            "+--------------------------------------------------------------+",
        ],
        &result
    );

    // null regex means null result
    let result = ctx
        .sql("SELECT regexp_replace('foobarbaz', NULL, 'X\\1Y', 'g')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+---------------------------------------------------------------+",
            "| regexp_replace(Utf8(\"foobarbaz\"),NULL,Utf8(\"X\\1Y\"),Utf8(\"g\")) |",
            "+---------------------------------------------------------------+",
            "|                                                               |",
            "+---------------------------------------------------------------+",
        ],
        &result
    );

    Ok(())
}
