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
async fn decimal_cast() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select cast(1.23 as decimal(10,4))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 4),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+---------------------------------------+",
        "| CAST(Float64(1.23) AS Decimal(10, 4)) |",
        "+---------------------------------------+",
        "| 1.2300                                |",
        "+---------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select cast(cast(1.23 as decimal(10,3)) as decimal(10,4))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 4),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+---------------------------------------------------------------+",
        "| CAST(CAST(Float64(1.23) AS Decimal(10, 3)) AS Decimal(10, 4)) |",
        "+---------------------------------------------------------------+",
        "| 1.2300                                                        |",
        "+---------------------------------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select cast(1.2345 as decimal(24,2))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(24, 2),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+-----------------------------------------+",
        "| CAST(Float64(1.2345) AS Decimal(24, 2)) |",
        "+-----------------------------------------+",
        "| 1.23                                    |",
        "+-----------------------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_by_sql() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "SELECT c1 from decimal_simple";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+",
        "| c1       |",
        "+----------+",
        "| 0.000010 |",
        "| 0.000020 |",
        "| 0.000020 |",
        "| 0.000030 |",
        "| 0.000030 |",
        "| 0.000030 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn decimal_by_filter() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select c1 from decimal_simple where c1 > 0.000030";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+",
        "| c1       |",
        "+----------+",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000040 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "| 0.000050 |",
        "+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from decimal_simple where c1 > c5";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    assert_eq!(
        &DataType::Decimal(12, 7),
        actual[0].schema().field(4).data_type()
    );
    let expected = vec![
        "+----------+----------------+----+-------+-----------+",
        "| c1       | c2             | c3 | c4    | c5        |",
        "+----------+----------------+----+-------+-----------+",
        "| 0.000020 | 0.000000000002 | 3  | false | 0.0000190 |",
        "| 0.000030 | 0.000000000003 | 5  | true  | 0.0000110 |",
        "| 0.000050 | 0.000000000005 | 8  | false | 0.0000330 |",
        "+----------+----------------+----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn decimal_agg_function() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    // min
    let sql = "select min(c1) from decimal_simple where c4=false";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+------------------------+",
        "| MIN(decimal_simple.c1) |",
        "+------------------------+",
        "| 0.000020               |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    // max
    let sql = "select max(c1) from decimal_simple where c4=false";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+------------------------+",
        "| MAX(decimal_simple.c1) |",
        "+------------------------+",
        "| 0.000050               |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // sum
    let sql = "select sum(c1) from decimal_simple";
    let actual = execute_to_batches(&ctx, sql).await;
    // inferred precision is 10+10
    assert_eq!(
        &DataType::Decimal(20, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+------------------------+",
        "| SUM(decimal_simple.c1) |",
        "+------------------------+",
        "| 0.000550               |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // avg
    // inferred precision is original precision + 4
    // inferred scale is original scale + 4
    let sql = "select avg(c1) from decimal_simple";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(14, 10),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+------------------------+",
        "| AVG(decimal_simple.c1) |",
        "+------------------------+",
        "| 0.0000366666           |",
        "+------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_logic_op() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    // logic operation: eq
    let sql = "select * from decimal_simple where c1=CAST(0.00002 as Decimal(10,8))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+----+-------+-----------+",
        "| c1       | c2             | c3 | c4    | c5        |",
        "+----------+----------------+----+-------+-----------+",
        "| 0.000020 | 0.000000000002 | 2  | true  | 0.0000250 |",
        "| 0.000020 | 0.000000000002 | 3  | false | 0.0000190 |",
        "+----------+----------------+----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: not eq
    let sql = "select c2,c3 from decimal_simple where c1!=0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+-----+",
        "| c2             | c3  |",
        "+----------------+-----+",
        "| 0.000000000001 | 1   |",
        "| 0.000000000003 | 4   |",
        "| 0.000000000003 | 5   |",
        "| 0.000000000003 | 5   |",
        "| 0.000000000004 | 5   |",
        "| 0.000000000004 | 12  |",
        "| 0.000000000004 | 14  |",
        "| 0.000000000004 | 8   |",
        "| 0.000000000005 | 9   |",
        "| 0.000000000005 | 4   |",
        "| 0.000000000005 | 8   |",
        "| 0.000000000005 | 100 |",
        "| 0.000000000005 | 1   |",
        "+----------------+-----+",
    ];
    assert_batches_eq!(expected, &actual);
    // logic operation: lt
    let sql = "select * from decimal_simple where 0.00002 > c1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+----+------+-----------+",
        "| c1       | c2             | c3 | c4   | c5        |",
        "+----------+----------------+----+------+-----------+",
        "| 0.000010 | 0.000000000001 | 1  | true | 0.0000140 |",
        "+----------+----------------+----+------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: lteq
    let sql = "select * from decimal_simple where c1 <= 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+----+-------+-----------+",
        "| c1       | c2             | c3 | c4    | c5        |",
        "+----------+----------------+----+-------+-----------+",
        "| 0.000010 | 0.000000000001 | 1  | true  | 0.0000140 |",
        "| 0.000020 | 0.000000000002 | 2  | true  | 0.0000250 |",
        "| 0.000020 | 0.000000000002 | 3  | false | 0.0000190 |",
        "+----------+----------------+----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: gt
    let sql = "select * from decimal_simple where c1 > 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+-----+-------+-----------+",
        "| c1       | c2             | c3  | c4    | c5        |",
        "+----------+----------------+-----+-------+-----------+",
        "| 0.000030 | 0.000000000003 | 4   | true  | 0.0000320 |",
        "| 0.000030 | 0.000000000003 | 5   | false | 0.0000350 |",
        "| 0.000030 | 0.000000000003 | 5   | true  | 0.0000110 |",
        "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
        "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
        "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
        "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
        "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
        "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
        "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
        "+----------+----------------+-----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: gteq
    let sql = "select * from decimal_simple where c1 >= 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+-----+-------+-----------+",
        "| c1       | c2             | c3  | c4    | c5        |",
        "+----------+----------------+-----+-------+-----------+",
        "| 0.000020 | 0.000000000002 | 2   | true  | 0.0000250 |",
        "| 0.000020 | 0.000000000002 | 3   | false | 0.0000190 |",
        "| 0.000030 | 0.000000000003 | 4   | true  | 0.0000320 |",
        "| 0.000030 | 0.000000000003 | 5   | false | 0.0000350 |",
        "| 0.000030 | 0.000000000003 | 5   | true  | 0.0000110 |",
        "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
        "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
        "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
        "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
        "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
        "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
        "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
        "+----------+----------------+-----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_sort() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select * from decimal_simple where c1 >= 0.00004 order by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+-----+-------+-----------+",
        "| c1       | c2             | c3  | c4    | c5        |",
        "+----------+----------------+-----+-------+-----------+",
        "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
        "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
        "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
        "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
        "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
        "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
        "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
        "+----------+----------------+-----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from decimal_simple where c1 >= 0.00004 order by c1 desc";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+-----+-------+-----------+",
        "| c1       | c2             | c3  | c4    | c5        |",
        "+----------+----------------+-----+-------+-----------+",
        "| 0.000050 | 0.000000000005 | 9   | true  | 0.0000520 |",
        "| 0.000050 | 0.000000000005 | 4   | true  | 0.0000780 |",
        "| 0.000050 | 0.000000000005 | 8   | false | 0.0000330 |",
        "| 0.000050 | 0.000000000005 | 100 | true  | 0.0000680 |",
        "| 0.000050 | 0.000000000005 | 1   | false | 0.0001000 |",
        "| 0.000040 | 0.000000000004 | 5   | true  | 0.0000440 |",
        "| 0.000040 | 0.000000000004 | 12  | false | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 14  | true  | 0.0000400 |",
        "| 0.000040 | 0.000000000004 | 8   | false | 0.0000440 |",
        "+----------+----------------+-----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from decimal_simple where c1 < 0.00003 order by c1 desc,c4";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(0).data_type()
    );
    let expected = vec![
        "+----------+----------------+----+-------+-----------+",
        "| c1       | c2             | c3 | c4    | c5        |",
        "+----------+----------------+----+-------+-----------+",
        "| 0.000020 | 0.000000000002 | 3  | false | 0.0000190 |",
        "| 0.000020 | 0.000000000002 | 2  | true  | 0.0000250 |",
        "| 0.000010 | 0.000000000001 | 1  | true  | 0.0000140 |",
        "+----------+----------------+----+-------+-----------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_group_function() -> Result<()> {
    let ctx = SessionContext::new();
    register_decimal_csv_table_by_sql(&ctx).await;
    let sql = "select count(*),c1 from decimal_simple group by c1 order by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(1).data_type()
    );
    let expected = vec![
        "+-----------------+----------+",
        "| COUNT(UInt8(1)) | c1       |",
        "+-----------------+----------+",
        "| 1               | 0.000010 |",
        "| 2               | 0.000020 |",
        "| 3               | 0.000030 |",
        "| 4               | 0.000040 |",
        "| 5               | 0.000050 |",
        "+-----------------+----------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select count(*),c1,c4 from decimal_simple group by c1,c4 order by c1,c4";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(
        &DataType::Decimal(10, 6),
        actual[0].schema().field(1).data_type()
    );
    let expected = vec![
        "+-----------------+----------+-------+",
        "| COUNT(UInt8(1)) | c1       | c4    |",
        "+-----------------+----------+-------+",
        "| 1               | 0.000010 | true  |",
        "| 1               | 0.000020 | false |",
        "| 1               | 0.000020 | true  |",
        "| 1               | 0.000030 | false |",
        "| 2               | 0.000030 | true  |",
        "| 2               | 0.000040 | false |",
        "| 2               | 0.000040 | true  |",
        "| 2               | 0.000050 | false |",
        "| 3               | 0.000050 | true  |",
        "+-----------------+----------+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
