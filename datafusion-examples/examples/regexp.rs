// Licensed to the Apache Software Foundation (ASF) under one
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

use std::sync::Arc;

use arrow::array::{BooleanArray, LargeStringArray, StringArray, StringBuilder};
use log::info;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::assert_contains;

/// This example demonstrates how to use the regexp_*
/// functions in the DataFrame API as well as via sql.
#[tokio::main]
async fn main() -> Result<()> {
    // define a schema. Regex are restricted to Utf8 and largeutf8 data
    let schema = Arc::new(Schema::new(vec![
        Field::new("values", DataType::Utf8, false),
        Field::new("patterns", DataType::LargeUtf8, false),
        Field::new("flags", DataType::Utf8, true),
    ]));

    let mut sb = StringBuilder::new();
    sb.append_value("i");
    sb.append_value("i");
    sb.append_value("i");
    sb.append_null();
    sb.append_null();
    sb.append_null();
    sb.append_null();
    sb.append_null();
    sb.append_null();
    sb.append_null();
    sb.append_null();

    // define data for our examples
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                "abc",
                "ABC",
                "aBc",
                "AbC",
                "aBC",
                "4000",
                "4010",
                "Düsseldorf",
                "Москва",
                "Köln",
                "إسرائيل",
            ])),
            // the full list of supported features and
            // syntax can be found at
            // https://docs.rs/regex/latest/regex/#syntax

            // NOTE: double slashes are required to escape the slash character
            // NOTE: when not using the r"" syntax
            Arc::new(LargeStringArray::from(vec![
                // simple regex examples
                "^(a)",
                "^(A).*",
                "(b|d)",
                "(B|D)",
                "^(b|c)",
                // word boundaries, grouping, etc
                r"\b4([1-9]\d\d|\d[1-9]\d|\d\d[1-9])\b",
                r"\b4([1-9]\d\d|\d[1-9]\d|\d\d[1-9])\b",
                // unicode is supported
                r"[\p{Letter}-]+",
                r"[\p{L}-]+",
                "[a-zA-Z]ö[a-zA-Z]{2}",
                // unicode character classes work
                r"^\p{Arabic}+$",
            ])),
            // supported flags can be found at
            // https://docs.rs/regex/latest/regex/#grouping-and-flags
            Arc::new(sb.finish()),
        ],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("examples", batch)?;
    let df = ctx.table("examples").await?;

    //
    //
    //regexp_like examples
    //
    //
    // regexp_like format is (regexp_replace(text, regex[, flags])
    //

    // use dataframe and regexp_like function to test col 'values', against patterns in col 'patterns' without flags
    let df = df.with_column("a", regexp_like(vec![col("values"), col("patterns")]))?;
    // use dataframe and regexp_like function to test col 'values', against patterns in col 'patterns' with flags
    let df = df.with_column(
        "b",
        regexp_like(vec![col("values"), col("patterns"), col("flags")]),
    )?;

    // you can  use literals as well with dataframe calls
    let df = df.with_column(
        "c",
        regexp_like(vec![lit("foobarbequebaz"), lit("(bar)(beque)")]),
    )?;

    let df = df.select_columns(&["a", "b", "c"])?;

    // print the results
    df.show().await?;

    // use sql and regexp_like function to test col 'values', against patterns in col 'patterns' without flags
    let df = ctx
        .sql("select regexp_like(values, patterns) from examples")
        .await?;

    // print the results
    df.show().await?;

    // use dataframe and regexp_like function to test col 'values', against patterns in col 'patterns' with flags
    let df = ctx
        .sql("select regexp_like(values, patterns, flags) from examples")
        .await?;

    // print the results
    df.show().await?;

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let df = ctx.sql("select regexp_like('John Smith', '^.*Smith$'), regexp_like('Smith Jones', '^Smith.*$')").await?;

    // print the results
    df.show().await?;

    // look-around and back references are not supported for performance
    // reasons.
    // Note that an error may not always be returned but the result
    // if returned will always be false
    let df = ctx.read_empty()?.with_column(
        "a",
        regexp_like(vec![
            lit(r"(?<=[A-Z]\w* )Smith"),
            lit("John Smith"),
            lit("i"),
        ]),
    )?;
    let df = df.select_columns(&["a"])?;

    // print the results
    df.show().await?;

    let result = ctx
        .sql(r"select regexp_like('(?<=[A-Z]\w )Smith', 'John Smith', 'i') as a")
        .await?
        .collect()
        .await;

    let expected = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, false)])),
        vec![Arc::new(BooleanArray::from(vec![false]))],
    )
    .unwrap();

    assert!(result.is_ok());
    let result = result.unwrap();

    assert_eq!(result.len(), 1);
    info!("{:?}", result[0]);
    info!("{expected:?}");

    assert_eq!(format!("{:?}", result[0]), format!("{expected:?}"));

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

    let expected = "Regular expression did not compile: CompiledTooBig(";
    assert_contains!(result.unwrap_err().to_string(), expected);

    //
    //
    //regexp_match examples
    //
    //
    // regexp_match format is (regexp_replace(text, regex[, flags])
    //

    let df = ctx.table("examples").await?;

    // use dataframe and regexp_match function to test col 'values', against patterns in col 'patterns' without flags
    let df = df.with_column("a", regexp_match(vec![col("values"), col("patterns")]))?;
    // use dataframe and regexp_match function to test col 'values', against patterns in col 'patterns' with flags
    let df = df.with_column(
        "b",
        regexp_match(vec![col("values"), col("patterns"), col("flags")]),
    )?;

    // you can  use literals as well with dataframe calls
    let df = df.with_column(
        "c",
        regexp_match(vec![lit("foobarbequebaz"), lit("(bar)(beque)")]),
    )?;

    let df = df.select_columns(&["a", "b", "c"])?;

    // print the results
    df.show().await?;

    // use sql and regexp_match function to test col 'values', against patterns in col 'patterns' without flags
    let df = ctx
        .sql("select regexp_match(values, patterns) from examples")
        .await?;

    // print the results
    df.show().await?;

    // use dataframe and regexp_match function to test col 'values', against patterns in col 'patterns' with flags
    let df = ctx
        .sql("select regexp_match(values, patterns, flags) from examples")
        .await?;

    // print the results
    df.show().await?;

    // literals work as well
    // to match against the entire input use ^ and $ in the regex
    let df = ctx.sql("select regexp_match('John Smith', '^.*Smith$'), regexp_match('Smith Jones', '^Smith.*$')").await?;

    // print the results
    df.show().await?;

    //
    //
    //regexp_replace examples
    //
    //
    // regexp_replace format is (regexp_replace(text, regex, replace, flags)
    //

    // global flag example
    let df = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', 'g')")
        .await?;

    // print the results
    df.show().await?;

    // without global flag
    let df = ctx
        .sql("SELECT regexp_replace('foobarbaz', 'b(..)', 'X\\1Y', null)")
        .await?;

    // print the results
    df.show().await?;

    // null regex means null result
    let df = ctx
        .sql("SELECT regexp_replace('foobarbaz', NULL, 'X\\1Y', 'g')")
        .await?;

    // print the results
    df.show().await?;

    Ok(())
}
