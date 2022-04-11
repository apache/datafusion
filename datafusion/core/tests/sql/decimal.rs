use super::*;

#[tokio::test]
async fn decimal_cast() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "select cast(1.23 as decimal(10,4))";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(&DataType::Decimal(10, 4), actual[0].schema().field(0).data_type());
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
    assert_eq!(&DataType::Decimal(10, 4), actual[0].schema().field(0).data_type());
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
    assert_eq!(&DataType::Decimal(24, 2), actual[0].schema().field(0).data_type());
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
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    let sql = "SELECT c1 from aggregate_simple";
    let actual = execute_to_batches(&ctx, sql).await;
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
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    let sql = "select c1 from aggregate_simple where c1 > 0.000030";
    let actual = execute_to_batches(&ctx, sql).await;
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
    Ok(())
}

#[tokio::test]
async fn decimal_agg_function() -> Result<()> {
    let ctx = SessionContext::new();
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    // min
    let sql = "select min(c1) from aggregate_simple where c3=false";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(&DataType::Decimal(10, 6), actual[0].schema().field(0).data_type());
    let expected = vec![
        "+--------------------------+",
        "| MIN(aggregate_simple.c1) |",
        "+--------------------------+",
        "| 0.000020                 |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);
    // max
    let sql = "select max(c1) from aggregate_simple where c3=false";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(&DataType::Decimal(10, 6), actual[0].schema().field(0).data_type());
    let expected = vec![
        "+--------------------------+",
        "| MAX(aggregate_simple.c1) |",
        "+--------------------------+",
        "| 0.000040                 |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // sum
    let sql = "select sum(c1) from aggregate_simple";
    let actual = execute_to_batches(&ctx, sql).await;
    // inferred precision is 10+10
    assert_eq!(&DataType::Decimal(20, 6), actual[0].schema().field(0).data_type());
    let expected = vec![
        "+--------------------------+",
        "| SUM(aggregate_simple.c1) |",
        "+--------------------------+",
        "| 0.000550                 |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    // avg
    let sql = "select avg(c1) from aggregate_simple";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(&DataType::Decimal(14, 10), actual[0].schema().field(0).data_type());
    let expected = vec![
        "+--------------------------+",
        "| AVG(aggregate_simple.c1) |",
        "+--------------------------+",
        "| 0.0000366666             |",
        "+--------------------------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_logic_op() -> Result<()> {
    let ctx = SessionContext::new();
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    // logic operation: eq
    let sql = "select * from aggregate_simple where c1=0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    assert_eq!(&DataType::Decimal(10, 6), actual[0].schema().field(0).data_type());
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000020 | 0.000000000002 | false |",
        "| 0.000020 | 0.000000000002 | false |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: not eq
    let sql = "select c2,c3 from aggregate_simple where c1!=0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------------+-------+",
        "| c2             | c3    |",
        "+----------------+-------+",
        "| 0.000000000001 | true  |",
        "| 0.000000000003 | true  |",
        "| 0.000000000003 | true  |",
        "| 0.000000000003 | true  |",
        "| 0.000000000004 | false |",
        "| 0.000000000004 | false |",
        "| 0.000000000004 | false |",
        "| 0.000000000004 | false |",
        "| 0.000000000005 | true  |",
        "| 0.000000000005 | true  |",
        "| 0.000000000005 | true  |",
        "| 0.000000000005 | true  |",
        "| 0.000000000005 | true  |",
        "+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    // logic operation: lt
    let sql = "select * from aggregate_simple where c1 < 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+------+",
        "| c1       | c2             | c3   |",
        "+----------+----------------+------+",
        "| 0.000010 | 0.000000000001 | true |",
        "+----------+----------------+------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: lteq
    let sql = "select * from aggregate_simple where c1 <= 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+------+",
        "| c1       | c2             | c3   |",
        "+----------+----------------+------+",
        "| 0.000010 | 0.000000000001 | true |",
        "+----------+----------------+------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: gt
    let sql = "select * from aggregate_simple where c1 > 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+------+",
        "| c1       | c2             | c3   |",
        "+----------+----------------+------+",
        "| 0.000010 | 0.000000000001 | true |",
        "+----------+----------------+------+",
    ];
    assert_batches_eq!(expected, &actual);

    // logic operation: gteq
    let sql = "select * from aggregate_simple where c1 >= 0.00002";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000010 | 0.000000000001 | true  |",
        "| 0.000020 | 0.000000000002 | false |",
        "| 0.000020 | 0.000000000002 | false |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}

#[tokio::test]
async fn decimal_arithmetic_op() {

}

#[tokio::test]
async fn decimal_sort() -> Result<()>{
    let ctx = SessionContext::new();
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    let sql = "select * from aggregate_simple where c1 >= 0.00004 order by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from aggregate_simple where c1 >= 0.00004 order by c1 desc";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    let sql = "select * from aggregate_simple where c1 < 0.00003 order by c1 desc,c2 ";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000020 | 0.000000000002 | false |",
        "| 0.000020 | 0.000000000002 | false |",
        "| 0.000010 | 0.000000000001 | true  |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);

    Ok(())
}


#[tokio::test]
async fn decimal_group_function() -> Result<()>{
    let ctx = SessionContext::new();
    register_simple_aggregate_csv_with_decimal_by_sql(&ctx).await;
    let sql = "select count(*) from aggregate_simple group by c1";
    let actual = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+----------+----------------+-------+",
        "| c1       | c2             | c3    |",
        "+----------+----------------+-------+",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000040 | 0.000000000004 | false |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "| 0.000050 | 0.000000000005 | true  |",
        "+----------+----------------+-------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())

}