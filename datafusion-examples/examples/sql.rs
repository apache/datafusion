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
//! * [`parquet_demo`]: run SQL query against a single Parquet file
//! * [`avro_demo`]: run SQL query against a single Avro file
//! * [`csv_demo`]: run SQL query against a single CSV file
//! * [`parquet_multi_files_demo`]: run SQL query against a table backed by multiple Parquet files
//! * [`regexp_demo`]: regular expression functions to manipulate strings
//! * [`to_char_demo`]: to_char function to convert strings to date, time, timestamp and durations
//! * [`to_timestamp_demo`]: to_timestamp function to convert strings to timestamps
//! * [`make_date_demo`]: make_date function to convert year, month and day to a date

use arrow::array::{Date32Array, Int32Array, RecordBatch, StringArray};
use arrow::util::pretty;
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
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
    parquet_demo().await?;
    avro_demo().await?;
    csv_demo().await?;
    parquet_multi_files_demo().await?;
    regexp_demo().await?;
    to_char_demo().await?;
    to_timestamp_demo().await?;
    make_date_demo().await?;
    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (Parquet) and fetching results
async fn parquet_demo() -> Result<()> {
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
/// source (Avro) and fetching results
async fn avro_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::arrow_test_data();

    // register avro file with the execution context
    let avro_file = &format!("{testdata}/avro/alltypes_plain.avro");
    ctx.register_avro("alltypes_plain", avro_file, AvroReadOptions::default())
        .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT int_col, double_col, CAST(date_string_col as VARCHAR) \
        FROM alltypes_plain \
        WHERE id > 1 AND tinyint_col < double_col",
        )
        .await?;
    let results = df.collect().await?;

    // print the results
    pretty::print_batches(&results)?;

    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (CSV) and fetching results
async fn csv_demo() -> Result<()> {
    // create local execution context
    let ctx = SessionContext::new();

    let testdata = datafusion::test_util::arrow_test_data();

    // register csv file with the execution context
    ctx.register_csv(
        "aggregate_test_100",
        &format!("{testdata}/csv/aggregate_test_100.csv"),
        CsvReadOptions::new(),
    )
    .await?;

    // execute the query
    let df = ctx
        .sql(
            "SELECT c1, MIN(c12), MAX(c12) \
        FROM aggregate_test_100 \
        WHERE c11 > 0.1 AND c11 < 0.9 \
        GROUP BY c1",
        )
        .await?;

    // print the results
    df.show().await?;

    // query compressed CSV with specific options
    let csv_options = CsvReadOptions::default()
        .has_header(true)
        .file_compression_type(FileCompressionType::GZIP)
        .file_extension("csv.gz");
    let df = ctx
        .read_csv(
            &format!("{testdata}/csv/aggregate_test_100.csv.gz"),
            csv_options,
        )
        .await?;
    let df = df
        .filter(col("c1").eq(lit("a")))?
        .select_columns(&["c2", "c3"])?;

    df.show().await?;

    Ok(())
}

/// This example demonstrates executing a simple query against an Arrow data
/// source (a directory with multiple Parquet files) and fetching results.
///
/// The query is run twice, once showing how to used `register_listing_table`
/// with an absolute path, and once registering an ObjectStore to use a relative
/// path.
async fn parquet_multi_files_demo() -> Result<()> {
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
async fn regexp_demo() -> Result<()> {
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

/// This example demonstrates how to use the to_char function via sql
///
/// This function accepts date, time, timestamp and duration values
/// in the first argument and string values for the second
async fn to_char_demo() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", DataType::Date32, false),
        Field::new("patterns", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![18506, 18507, 18508, 18509])),
            Arc::new(StringArray::from(vec![
                "%Y-%m-%d", "%Y:%m:%d", "%Y%m%d", "%d-%m-%Y",
            ])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let _ = ctx.table("t").await?;

    // use to_char function to convert col 'values' to timestamp type using
    // patterns stored in col 'patterns'
    let result = ctx
        .sql("SELECT to_char(values, patterns) from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------+",
            "| to_char(t.values,t.patterns) |",
            "+------------------------------+",
            "| 2020-09-01                   |",
            "| 2020:09:02                   |",
            "| 20200903                     |",
            "| 04-09-2020                   |",
            "+------------------------------+",
        ],
        &result
    );

    // the date_format alias for the to_char function can be used as well
    let result = ctx
        .sql("SELECT date_format(values, patterns) from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------+",
            "| to_char(t.values,t.patterns) |",
            "+------------------------------+",
            "| 2020-09-01                   |",
            "| 2020:09:02                   |",
            "| 20200903                     |",
            "| 04-09-2020                   |",
            "+------------------------------+",
        ],
        &result
    );

    // use to_char function to convert col 'values' with a fixed format
    let result = ctx
        .sql("SELECT to_char(values, '%m-%d-%Y') FROM t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+------------------------------------+",
            "| to_char(t.values,Utf8(\"%m-%d-%Y\")) |",
            "+------------------------------------+",
            "| 09-01-2020                         |",
            "| 09-02-2020                         |",
            "| 09-03-2020                         |",
            "| 09-04-2020                         |",
            "+------------------------------------+",
        ],
        &result
    );

    // if you want to just use the default format cast to a string
    let result = ctx
        .sql("SELECT arrow_cast(values, 'Utf8') from t")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------+",
            "| arrow_cast(t.values,Utf8(\"Utf8\")) |",
            "+-----------------------------------+",
            "| 2020-09-01                        |",
            "| 2020-09-02                        |",
            "| 2020-09-03                        |",
            "| 2020-09-04                        |",
            "+-----------------------------------+",
        ],
        &result
    );

    // use can use literals as well (note the use of timestamp here)
    let result = ctx
        .sql("SELECT to_char(arrow_cast(TIMESTAMP '2023-08-03 14:38:50Z', 'Timestamp(Second, None)'), '%d-%m-%Y %H:%M:%S')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-------------------------------------------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Utf8(\"2023-08-03 14:38:50Z\"),Utf8(\"Timestamp(Second, None)\")),Utf8(\"%d-%m-%Y %H:%M:%S\")) |",
            "+-------------------------------------------------------------------------------------------------------------+",
            "| 03-08-2023 14:38:50                                                                                         |",
            "+-------------------------------------------------------------------------------------------------------------+",
        ],
        &result
    );

    // durations are supported though the output format is limited to two formats
    // 'pretty' and 'ISO8601'
    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), 'pretty')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+----------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Int64(123456),Utf8(\"Duration(Second)\")),Utf8(\"pretty\")) |",
            "+----------------------------------------------------------------------------+",
            "| 1 days 10 hours 17 mins 36 secs                                            |",
            "+----------------------------------------------------------------------------+",
        ],
        &result
    );

    // durations are supported though the output format is limited to two formats
    // 'pretty' and 'ISO8601'
    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), 'iso8601')")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+-----------------------------------------------------------------------------+",
            "| to_char(arrow_cast(Int64(123456),Utf8(\"Duration(Second)\")),Utf8(\"iso8601\")) |",
            "+-----------------------------------------------------------------------------+",
            "| PT123456S                                                                   |",
            "+-----------------------------------------------------------------------------+",
        ],
        &result
    );

    // output format is null

    let result = ctx
        .sql("SELECT to_char(arrow_cast(123456, 'Duration(Second)'), null) as result")
        .await?
        .collect()
        .await?;

    assert_batches_eq!(
        &[
            "+--------+",
            "| result |",
            "+--------+",
            "|        |",
            "+--------+",
        ],
        &result
    );

    Ok(())
}

/// This example demonstrates how to use the to_timestamp series
/// of functions via sql.
async fn to_timestamp_demo() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "2020-09-08T13:42:29Z",
                "2020-09-08T13:42:29.190855-05:00",
                "2020-08-09 12:13:29",
                "2020-01-02",
            ])),
            Arc::new(StringArray::from(vec![
                "2020-09-08T13:42:29Z",
                "2020-09-08T13:42:29.190855-05:00",
                "08-09-2020 13/42/29",
                "09-27-2020 13:42:29-05:30",
            ])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    // use sql to convert col 'a' to timestamp using the default parsing
    let df = ctx.sql("select to_timestamp(a) from t").await?;

    // print the results
    df.show().await?;

    // use sql to convert col 'b' to timestamp using a list of chrono formats to try
    let df = ctx.sql("select to_timestamp(b, '%+', '%d-%m-%Y %H/%M/%S', '%m-%d-%Y %H:%M:%S%#z') from t").await?;

    // print the results
    df.show().await?;

    // use sql to convert a static string to a timestamp using a list of chrono formats to try
    // note that one of the formats is invalid ('%q') but since DataFusion will try all the
    // formats until it encounters one that parses the timestamp expression successfully
    // no error will be returned
    let df = ctx.sql("select to_timestamp_micros('01-14-2023 01:01:30+05:30', '%q', '%d-%m-%Y %H/%M/%S', '%+', '%m-%d-%Y %H:%M:%S%#z')").await?;

    // print the results
    df.show().await?;

    // casting a string to TIMESTAMP will also work for RFC3339 timestamps
    let df = ctx
        .sql("select to_timestamp_millis(TIMESTAMP '2022-08-03T14:38:50Z')")
        .await?;

    // print the results
    df.show().await?;

    // unix timestamps (in seconds) are also supported
    let df = ctx.sql("select to_timestamp(1926632005)").await?;

    // print the results
    df.show().await?;

    // use sql to convert a static string to a timestamp using a non-matching chrono format to try
    let result = ctx
        .sql("select to_timestamp_nanos('01-14-2023 01/01/30', '%d-%m-%Y %H:%M:%S')")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Error parsing timestamp from '01-14-2023 01/01/30' using format '%d-%m-%Y %H:%M:%S': input is out of range";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // note that using arrays for the chrono formats is not supported
    let result = ctx
        .sql("SELECT to_timestamp('2022-08-03T14:38:50+05:30', make_array('%s', '%q', '%d-%m-%Y %H:%M:%S%#z', '%+'))")
        .await?
        .collect()
        .await;

    let expected = "to_timestamp function unsupported data type at index 1: List";
    assert_contains!(result.unwrap_err().to_string(), expected);

    Ok(())
}

/// This example demonstrates how to use the make_date
/// function via sql.
async fn make_date_demo() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("y", DataType::Int32, false),
        Field::new("m", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![2020, 2021, 2022, 2023, 2024])),
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int32Array::from(vec![15, 16, 17, 18, 19])),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;

    // use sql to convert col 'y' & 'm' with a static string day to a date
    let df = ctx.sql("select make_date(y, m, '22') from t").await?;

    // print the results
    df.show().await?;

    // math expressions work
    let df = ctx.sql("select make_date(y + 1, m, d) from t").await?;

    // print the results
    df.show().await?;

    // you can cast to supported types (int, bigint, varchar) if required
    let df = ctx
        .sql("select make_date(2024::bigint, 01::bigint, 27::varchar(3))")
        .await?;

    // print the results
    df.show().await?;

    // arrow casts also work
    let df = ctx
        .sql("select make_date(arrow_cast(2024, 'Int64'), arrow_cast(1, 'Int64'), arrow_cast(27, 'Int64'))")
        .await?;

    // print the results
    df.show().await?;

    // invalid column values will result in an error
    let result = ctx
        .sql("select make_date(2024, null, 23)")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Unable to parse date from null/empty value";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // invalid date values will also result in an error
    let result = ctx
        .sql("select make_date(2024, 01, 32)")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Unable to parse date from 2024, 1, 32";
    assert_contains!(result.unwrap_err().to_string(), expected);

    Ok(())
}
