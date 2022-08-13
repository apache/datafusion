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
async fn arrow_typeof_null() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(null)";
    let actual = execute(&ctx, sql).await;
    let expected = "Null";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_boolean() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(true)";
    let actual = execute(&ctx, sql).await;
    let expected = "Boolean";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_i64() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(1)";
    let actual = execute(&ctx, sql).await;
    let expected = "Int64";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_i32() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(1::int)";
    let actual = execute(&ctx, sql).await;
    let expected = "Int32";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_f64() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(1.0)";
    let actual = execute(&ctx, sql).await;
    let expected = "Float64";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_f32() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(1.0::float)";
    let actual = execute(&ctx, sql).await;
    let expected = "Float32";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_decimal() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(1::Decimal)";
    let actual = execute(&ctx, sql).await;
    let expected = "Decimal128(38, 10)";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_timestamp() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(now()::timestamp)";
    let actual = execute(&ctx, sql).await;
    let expected = "Timestamp(Nanosecond, None)";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_timestamp_utc() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(now())";
    let actual = execute(&ctx, sql).await;
    let expected = "Timestamp(Nanosecond, Some(\"UTC\"))";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_timestamp_date32() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof(now()::date)";
    let actual = execute(&ctx, sql).await;
    let expected = "Date32";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}

#[tokio::test]
async fn arrow_typeof_utf8() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "SELECT arrow_typeof('1')";
    let actual = execute(&ctx, sql).await;
    let expected = "Utf8";
    assert_eq!(expected, &actual[0][0]);

    Ok(())
}
