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

use super::*;

#[tokio::test]
async fn set_variable_to_value() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.execution.batch_size to 1")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 1       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn set_variable_to_value_with_equal_sign() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.execution.batch_size = 1")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 1       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn set_variable_to_value_with_single_quoted_string() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.execution.batch_size to '1'")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 1       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn set_variable_to_value_case_insensitive() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.EXECUTION.batch_size to '1'")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 1       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}

#[tokio::test]
async fn set_variable_unknown_variable() {
    let ctx = SessionContext::new();

    let err = plan_and_collect(&ctx, "SET aabbcc to '1'")
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "Execution error: Can not SET variable: Unknown Variable aabbcc"
    );
}

#[tokio::test]
async fn set_bool_variable() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.execution.coalesce_batches to true")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.coalesce_batches")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------------+---------+",
        "| name                                  | setting |",
        "+---------------------------------------+---------+",
        "| datafusion.execution.coalesce_batches | true    |",
        "+---------------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);

    ctx.sql("SET datafusion.execution.coalesce_batches to 'false'")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.coalesce_batches")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------------+---------+",
        "| name                                  | setting |",
        "+---------------------------------------+---------+",
        "| datafusion.execution.coalesce_batches | false   |",
        "+---------------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);
}

#[tokio::test]
async fn set_bool_variable_bad_value() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let err = plan_and_collect(&ctx, "SET datafusion.execution.coalesce_batches to 1")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: Failed to parse 1 as bool"
    );

    let err = plan_and_collect(&ctx, "SET datafusion.execution.coalesce_batches to abc")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: Failed to parse abc as bool"
    );
}

#[tokio::test]
async fn set_u64_variable() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    ctx.sql("SET datafusion.execution.batch_size to 0")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 0       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);

    ctx.sql("SET datafusion.execution.batch_size to '1'")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 1       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);

    ctx.sql("SET datafusion.execution.batch_size to +2")
        .await
        .unwrap();
    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.batch_size")
        .await
        .unwrap();
    let expected = vec![
        "+---------------------------------+---------+",
        "| name                            | setting |",
        "+---------------------------------+---------+",
        "| datafusion.execution.batch_size | 2       |",
        "+---------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);
}

#[tokio::test]
async fn set_u64_variable_bad_value() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    let err = plan_and_collect(&ctx, "SET datafusion.execution.batch_size to -1")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: Failed to parse -1 as u64"
    );

    let err = plan_and_collect(&ctx, "SET datafusion.execution.batch_size to abc")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: Failed to parse abc as u64"
    );

    let err = plan_and_collect(&ctx, "SET datafusion.execution.batch_size to 0.1")
        .await
        .unwrap_err();

    assert_eq!(
        err.to_string(),
        "Execution error: Failed to parse 0.1 as u64"
    );
}

#[tokio::test]
async fn set_time_zone() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    plan_and_collect(&ctx, "SET datafusion.execution.time_zone = '+08:00'")
        .await
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW datafusion.execution.time_zone")
        .await
        .unwrap();
    let expected = vec![
        "+--------------------------------+---------+",
        "| name                           | setting |",
        "+--------------------------------+---------+",
        "| datafusion.execution.time_zone | +08:00  |",
        "+--------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);
}

#[tokio::test]
async fn set_time_zone_with_alias_variable_name() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    // TIME ZONE with space
    plan_and_collect(&ctx, "SET TIME ZONE = '+08:00'")
        .await
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW TIME ZONE").await.unwrap();
    let expected = vec![
        "+--------------------------------+---------+",
        "| name                           | setting |",
        "+--------------------------------+---------+",
        "| datafusion.execution.time_zone | +08:00  |",
        "+--------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);

    // TIMEZONE without space
    plan_and_collect(&ctx, "SET TIMEZONE = '+07:00'")
        .await
        .unwrap();

    let result = plan_and_collect(&ctx, "SHOW TIMEZONE").await.unwrap();
    let expected = vec![
        "+--------------------------------+---------+",
        "| name                           | setting |",
        "+--------------------------------+---------+",
        "| datafusion.execution.time_zone | +07:00  |",
        "+--------------------------------+---------+",
    ];
    assert_batches_eq!(expected, &result);
}

#[tokio::test]
async fn set_time_zone_good_time_zone_format() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    plan_and_collect(&ctx, "SET TIME ZONE = '+08:00'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-----------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\") |",
        "+-----------------------------+",
        "| 2000-01-01T08:00:00+08:00   |",
        "+-----------------------------+",
    ];
    // this might break once https://github.com/apache/arrow-rs/issues/1936 fixed
    assert_batches_eq!(expected, &result);

    plan_and_collect(&ctx, "SET TIME ZONE = '-08:00'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-----------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\") |",
        "+-----------------------------+",
        "| 1999-12-31T16:00:00-08:00   |",
        "+-----------------------------+",
    ];
    // this might break once https://github.com/apache/arrow-rs/issues/1936 fixed
    assert_batches_eq!(expected, &result);

    plan_and_collect(&ctx, "SET TIME ZONE = '+0800'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-----------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\") |",
        "+-----------------------------+",
        "| 2000-01-01T08:00:00+08:00   |",
        "+-----------------------------+",
    ];
    // this might break once https://github.com/apache/arrow-rs/issues/1936 fixed
    assert_batches_eq!(expected, &result);

    plan_and_collect(&ctx, "SET TIME ZONE = '+08'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-----------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\") |",
        "+-----------------------------+",
        "| 2000-01-01T08:00:00+08:00   |",
        "+-----------------------------+",
    ];
    // this might break once https://github.com/apache/arrow-rs/issues/1936 fixed
    assert_batches_eq!(expected, &result);
}

#[tokio::test]
async fn set_time_zone_bad_time_zone_format() {
    let ctx =
        SessionContext::with_config(SessionConfig::new().with_information_schema(true));

    plan_and_collect(&ctx, "SET TIME ZONE = '+08:00:00'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-----------------------------------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\")                         |",
        "+-----------------------------------------------------+",
        "| 2000-01-01T00:00:00 (Unknown Time Zone '+08:00:00') |",
        "+-----------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &result);

    plan_and_collect(&ctx, "SET TIME ZONE = '08:00'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-------------------------------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\")                     |",
        "+-------------------------------------------------+",
        "| 2000-01-01T00:00:00 (Unknown Time Zone '08:00') |",
        "+-------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &result);

    plan_and_collect(&ctx, "SET TIME ZONE = '08'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+----------------------------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\")                  |",
        "+----------------------------------------------+",
        "| 2000-01-01T00:00:00 (Unknown Time Zone '08') |",
        "+----------------------------------------------+",
    ];
    assert_batches_eq!(expected, &result);

    // we dont support named time zone yet
    plan_and_collect(&ctx, "SET TIME ZONE = 'Asia/Taipei'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+-------------------------------------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\")                           |",
        "+-------------------------------------------------------+",
        "| 2000-01-01T00:00:00 (Unknown Time Zone 'Asia/Taipei') |",
        "+-------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &result);

    // this is invalid even after we support named time zone
    plan_and_collect(&ctx, "SET TIME ZONE = 'Asia/Taipei2'")
        .await
        .unwrap();

    // casting UTF-8 to TimestampTZ isn't supported yet, add Timestamp as the middle layer for now
    let result =
        plan_and_collect(&ctx, "SELECT '2000-01-01T00:00:00'::TIMESTAMP::TIMESTAMPTZ")
            .await
            .unwrap();
    let expected = vec![
        "+--------------------------------------------------------+",
        "| Utf8(\"2000-01-01T00:00:00\")                            |",
        "+--------------------------------------------------------+",
        "| 2000-01-01T00:00:00 (Unknown Time Zone 'Asia/Taipei2') |",
        "+--------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &result);
}
