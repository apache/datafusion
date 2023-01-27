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
async fn decimal_cast_precision() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select cast(1.23 as decimal(10,4))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal128(10, 4),
        actual[0].schema().field(0).data_type()
    );

    let sql = "select cast(cast(1.23 as decimal(10,3)) as decimal(10,4))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal128(10, 4),
        actual[0].schema().field(0).data_type()
    );

    Ok(())
}

#[tokio::test]
async fn decimal_filter_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1, c5 from decimal_simple where c1 > c5";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(10, 6),
        actual[0].schema().field(0).data_type()
    );
    assert_eq!(
        &DataType::Decimal128(12, 7),
        actual[0].schema().field(1).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn min_function_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select min(c1) from decimal_simple where c4=false";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(10, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn max_function_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select max(c1) from decimal_simple where c4=false";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(10, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn sum_function_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select sum(c1) from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    // inferred precision is 10+10
    assert_eq!(
        &DataType::Decimal128(20, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn avg_function_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select avg(c1) from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    // inferred precision is original precision + 4
    // inferred scale is original scale + 4
    assert_eq!(
        &DataType::Decimal128(14, 10),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn add_scalar_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1+1 from decimal_simple"; // add scalar

    let actual = execute_to_batches(&ctx, sql).await;

    // array decimal(10,6) + scalar decimal(20,0) => decimal(21,6)
    assert_eq!(
        &DataType::Decimal128(27, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn add_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1+c5 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    // array decimal(10,6) + array decimal(12,7) => decimal(13,7)
    assert_eq!(
        &DataType::Decimal128(13, 7),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn subtract_scalar_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1-1 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(27, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn subtract_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1-c5 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(13, 7),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn multiply_scalar_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1*20 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(31, 6),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn multiply_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1*c5 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(23, 13),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn divide_scalar_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1/cast(0.00001 as decimal(5,5)) from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(21, 12),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn divide_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1/c5 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(30, 19),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn modulo_scalar_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c5%cast(0.00001 as decimal(5,5)) from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(7, 7),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn modulo_precision() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1%c5 from decimal_simple";

    let actual = execute_to_batches(&ctx, sql).await;

    assert_eq!(
        &DataType::Decimal128(11, 7),
        actual[0].schema().field(0).data_type()
    );
    Ok(())
}

#[tokio::test]
async fn decimal_null_scalar_array_comparison() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select a < null from (values (1.1::decimal)) as t(a)";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(1, actual.len());
    assert_eq!(1, actual[0].num_columns());
    assert_eq!(1, actual[0].num_rows());
    assert!(actual[0].column(0).is_null(0));
    assert_eq!(&DataType::Boolean, actual[0].column(0).data_type());
    Ok(())
}

#[tokio::test]
async fn decimal_null_array_scalar_comparison() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select null <= a from (values (1.1::decimal)) as t(a);";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(1, actual.len());
    assert_eq!(1, actual[0].num_columns());
    assert_eq!(1, actual[0].num_rows());
    assert!(actual[0].column(0).is_null(0));
    assert_eq!(&DataType::Boolean, actual[0].column(0).data_type());
    Ok(())
}
