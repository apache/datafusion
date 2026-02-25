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

//! See `main.rs` for how to run it.

use std::sync::Arc;

use arrow::array::{Date32Array, Int32Array};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::assert_batches_eq;
use datafusion::common::assert_contains;
use datafusion::error::Result;
use datafusion::prelude::*;

/// Example: Working with Date and Time Functions
///
/// This example demonstrates how to work with various date and time
/// functions in DataFusion using both the DataFrame API and SQL queries.
///
/// It includes:
/// - `make_date`: building `DATE` values from year, month, and day columns
/// - `to_date`: converting string expressions into `DATE` values
/// - `to_timestamp`: parsing strings or numeric values into `TIMESTAMP`s
/// - `to_char`: formatting dates, timestamps, and durations as strings
///
/// Together, these examples show how to create, convert, and format temporal
/// data using DataFusion’s built-in functions.
pub async fn date_time() -> Result<()> {
    query_make_date().await?;
    query_to_date().await?;
    query_to_timestamp().await?;
    query_to_char().await?;
    Ok(())
}

/// This example demonstrates how to use the make_date
/// function in the DataFrame API as well as via sql.
async fn query_make_date() -> Result<()> {
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
    let df = ctx.table("t").await?;

    // use make_date function to convert col 'y', 'm' & 'd' to a date
    let df = df.with_column("a", make_date(col("y"), col("m"), col("d")))?;
    // use make_date function to convert col 'y' & 'm' with a static day to a date
    let df = df.with_column("b", make_date(col("y"), col("m"), lit(22)))?;
    let df = df.select_columns(&["a", "b"])?;

    let expected = [
        "+------------+------------+",
        "| a          | b          |",
        "+------------+------------+",
        "| 2020-01-15 | 2020-01-22 |",
        "| 2021-02-16 | 2021-02-22 |",
        "| 2022-03-17 | 2022-03-22 |",
        "| 2023-04-18 | 2023-04-22 |",
        "| 2024-05-19 | 2024-05-22 |",
        "+------------+------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);
    // print the results
    // df.show().await?;

    // use sql to convert col 'y', 'm' & 'd' to a date
    let df = ctx.sql("select make_date(y, m, d) from t").await?;

    let expected = [
        "+------------------------+",
        "| make_date(t.y,t.m,t.d) |",
        "+------------------------+",
        "| 2020-01-15             |",
        "| 2021-02-16             |",
        "| 2022-03-17             |",
        "| 2023-04-18             |",
        "| 2024-05-19             |",
        "+------------------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);

    // use sql to convert col 'y' & 'm' with a static string day to a date
    let df = ctx.sql("select make_date(y, m, '22') from t").await?;

    let expected = [
        "+-------------------------------+",
        "| make_date(t.y,t.m,Utf8(\"22\")) |",
        "+-------------------------------+",
        "| 2020-01-22                    |",
        "| 2021-02-22                    |",
        "| 2022-03-22                    |",
        "| 2023-04-22                    |",
        "| 2024-05-22                    |",
        "+-------------------------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);

    // math expressions work
    let df = ctx.sql("select make_date(y + 1, m, d) from t").await?;

    let expected = [
        "+-----------------------------------+",
        "| make_date(t.y + Int64(1),t.m,t.d) |",
        "+-----------------------------------+",
        "| 2021-01-15                        |",
        "| 2022-02-16                        |",
        "| 2023-03-17                        |",
        "| 2024-04-18                        |",
        "| 2025-05-19                        |",
        "+-----------------------------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);

    // you can cast to supported types (int, bigint, varchar) if required
    let df = ctx
        .sql("select make_date(2024::bigint, 01::bigint, 27::varchar(3))")
        .await?;

    let expected = [
        "+-------------------------------------------+",
        "| make_date(Int64(2024),Int64(1),Int64(27)) |",
        "+-------------------------------------------+",
        "| 2024-01-27                                |",
        "+-------------------------------------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);

    // arrow casts also work
    let df = ctx
        .sql("select make_date(arrow_cast(2024, 'Int64'), arrow_cast(1, 'Int64'), arrow_cast(27, 'Int64'))")
        .await?;

    let expected = [
        "+-------------------------------------------------------------------------------------------------------------------------+",
        "| make_date(arrow_cast(Int64(2024),Utf8(\"Int64\")),arrow_cast(Int64(1),Utf8(\"Int64\")),arrow_cast(Int64(27),Utf8(\"Int64\"))) |",
        "+-------------------------------------------------------------------------------------------------------------------------+",
        "| 2024-01-27                                                                                                              |",
        "+-------------------------------------------------------------------------------------------------------------------------+",
    ];

    assert_batches_eq!(expected, &df.collect().await?);

    // invalid column values will result in an error
    let result = ctx
        .sql("select make_date(2024, '', 23)")
        .await?
        .collect()
        .await;

    let expected =
        "Arrow error: Cast error: Cannot cast string '' to value of Int32 type";
    assert_contains!(result.unwrap_err().to_string(), expected);

    // invalid date values will also result in an error
    let result = ctx
        .sql("select make_date(2024, 01, 32)")
        .await?
        .collect()
        .await;

    let expected = "Execution error: Day value '32' is out of range";
    assert_contains!(result.unwrap_err().to_string(), expected);

    Ok(())
}

/// This example demonstrates how to use the to_date series
/// of functions in the DataFrame API as well as via sql.
async fn query_to_date() -> Result<()> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, false)]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(StringArray::from(vec![
            "2020-09-08T13:42:29Z",
            "2020-09-08T13:42:29.190855-05:00",
            "2020-08-09 12:13:29",
            "2020-01-02",
        ]))],
    )?;

    // declare a new context. In spark API, this corresponds to a new spark SQLsession
    let ctx = SessionContext::new();

    // declare a table in memory. In spark API, this corresponds to createDataFrame(...).
    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    // use to_date function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_date(vec![col("a")]))?;

    let df = df.select_columns(&["a"])?.collect().await?;

    let expected = [
        "+------------+",
        "| a          |",
        "+------------+",
        "| 2020-09-08 |",
        "| 2020-09-08 |",
        "| 2020-08-09 |",
        "| 2020-01-02 |",
        "+------------+",
    ];
    assert_batches_eq!(&expected, &df);

    Ok(())
}

/// This example demonstrates how to use the to_timestamp series
/// of functions in the DataFrame API as well as via sql.
async fn query_to_timestamp() -> Result<()> {
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
    let df = ctx.table("t").await?;

    // use to_timestamp function to convert col 'a' to timestamp type using the default parsing
    let df = df.with_column("a", to_timestamp(vec![col("a")]))?;
    // use to_timestamp_seconds function to convert col 'b' to timestamp(Seconds) type using a list
    // of chrono formats (https://docs.rs/chrono/latest/chrono/format/strftime/index.html) to try
    let df = df.with_column(
        "b",
        to_timestamp_seconds(vec![
            col("b"),
            lit("%+"),
            lit("%d-%m-%Y %H/%M/%S"),
            lit("%m-%d-%Y %H:%M:%S%#z"),
        ]),
    )?;

    let df = df.select_columns(&["a", "b"])?.collect().await?;
    assert_batches_eq!(
        &[
            "+----------------------------+---------------------+",
            "| a                          | b                   |",
            "+----------------------------+---------------------+",
            "| 2020-09-08T13:42:29        | 2020-09-08T13:42:29 |",
            "| 2020-09-08T18:42:29.190855 | 2020-09-08T18:42:29 |",
            "| 2020-08-09T12:13:29        | 2020-09-08T13:42:29 |",
            "| 2020-01-02T00:00:00        | 2020-09-27T19:12:29 |",
            "+----------------------------+---------------------+",
        ],
        &df
    );

    // use sql to convert col 'a' to timestamp using the default parsing
    let df = ctx
        .sql("select to_timestamp(a) from t")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+----------------------------+",
            "| to_timestamp(t.a)          |",
            "+----------------------------+",
            "| 2020-09-08T13:42:29        |",
            "| 2020-09-08T18:42:29.190855 |",
            "| 2020-08-09T12:13:29        |",
            "| 2020-01-02T00:00:00        |",
            "+----------------------------+",
        ],
        &df
    );

    let df = ctx.sql("select to_char(to_timestamp_seconds(b,  '%+', '%d-%m-%Y %H/%M/%S', '%m-%d-%Y %H:%M:%S%#z'), '%Y-%m-%d %H:%M:%S') from t").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+--------------------------------------------------------------------------------------------------------------------------------+",
            "| to_char(to_timestamp_seconds(t.b,Utf8(\"%+\"),Utf8(\"%d-%m-%Y %H/%M/%S\"),Utf8(\"%m-%d-%Y %H:%M:%S%#z\")),Utf8(\"%Y-%m-%d %H:%M:%S\")) |",
            "+--------------------------------------------------------------------------------------------------------------------------------+",
            "| 2020-09-08 13:42:29                                                                                                            |",
            "| 2020-09-08 18:42:29                                                                                                            |",
            "| 2020-09-08 13:42:29                                                                                                            |",
            "| 2020-09-27 19:12:29                                                                                                            |",
            "+--------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &df
    );

    // use sql to convert col 'b' to timestamp using a list of chrono formats to try
    let df = ctx.sql("select to_timestamp(b, '%+', '%d-%m-%Y %H/%M/%S', '%m-%d-%Y %H:%M:%S%#z') from t").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+-------------------------------------------------------------------------------------+",
            "| to_timestamp(t.b,Utf8(\"%+\"),Utf8(\"%d-%m-%Y %H/%M/%S\"),Utf8(\"%m-%d-%Y %H:%M:%S%#z\")) |",
            "+-------------------------------------------------------------------------------------+",
            "| 2020-09-08T13:42:29                                                                 |",
            "| 2020-09-08T18:42:29.190855                                                          |",
            "| 2020-09-08T13:42:29                                                                 |",
            "| 2020-09-27T19:12:29                                                                 |",
            "+-------------------------------------------------------------------------------------+",
        ],
        &df
    );

    // use sql to convert a static string to a timestamp using a list of chrono formats to try
    // note that one of the formats is invalid ('%q') but since DataFusion will try all the
    // formats until it encounters one that parses the timestamp expression successfully
    // no error will be returned
    let df = ctx.sql("select to_timestamp_micros('01-14-2023 01:01:30+05:30', '%q', '%d-%m-%Y %H/%M/%S', '%+', '%m-%d-%Y %H:%M:%S%#z')").await?.collect().await?;
    assert_batches_eq!(
        &[
            "+-------------------------------------------------------------------------------------------------------------------------------------+",
            "| to_timestamp_micros(Utf8(\"01-14-2023 01:01:30+05:30\"),Utf8(\"%q\"),Utf8(\"%d-%m-%Y %H/%M/%S\"),Utf8(\"%+\"),Utf8(\"%m-%d-%Y %H:%M:%S%#z\")) |",
            "+-------------------------------------------------------------------------------------------------------------------------------------+",
            "| 2023-01-13T19:31:30                                                                                                                 |",
            "+-------------------------------------------------------------------------------------------------------------------------------------+",
        ],
        &df
    );

    let df = ctx
        .sql("select to_timestamp_millis(TIMESTAMP '2022-08-03T14:38:50Z')")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+---------------------------------------------------+",
            "| to_timestamp_millis(Utf8(\"2022-08-03T14:38:50Z\")) |",
            "+---------------------------------------------------+",
            "| 2022-08-03T14:38:50                               |",
            "+---------------------------------------------------+",
        ],
        &df
    );

    let df = ctx
        .sql("select to_timestamp(1926632005)")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+---------------------------------+",
            "| to_timestamp(Int64(1926632005)) |",
            "+---------------------------------+",
            "| 2031-01-19T23:33:25             |",
            "+---------------------------------+",
        ],
        &df
    );

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

/// This function accepts date, time, timestamp and duration values
/// in the first argument and string values for the second
async fn query_to_char() -> Result<()> {
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
    let result = ctx
        .sql("SELECT to_timestamp(to_char(values, patterns),patterns) from t")
        .await?
        .collect()
        .await?;
    assert_batches_eq!(
        &[
            "+-------------------------------------------------------+",
            "| to_timestamp(to_char(t.values,t.patterns),t.patterns) |",
            "+-------------------------------------------------------+",
            "| 2020-09-01T00:00:00                                   |",
            "| 2020-09-02T00:00:00                                   |",
            "| 2020-09-03T00:00:00                                   |",
            "| 2020-09-04T00:00:00                                   |",
            "+-------------------------------------------------------+",
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
            "+----------------------------------+",
            "| date_format(t.values,t.patterns) |",
            "+----------------------------------+",
            "| 2020-09-01                       |",
            "| 2020:09:02                       |",
            "| 20200903                         |",
            "| 04-09-2020                       |",
            "+----------------------------------+",
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
