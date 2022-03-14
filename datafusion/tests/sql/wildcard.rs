use super::*;

#[tokio::test]
async fn select_qualified_wildcard() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_simple_csv(&mut ctx).await?;

    let sql = "SELECT agg.* FROM aggregate_simple as agg order by c1";
    let results = execute_to_batches(&mut ctx, sql).await;

    let expected = vec![
        "+---------+----------------+-------+",
        "| c1      | c2             | c3    |",
        "+---------+----------------+-------+",
        "| 0.00001 | 0.000000000001 | true  |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00002 | 0.000000000002 | false |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00003 | 0.000000000003 | true  |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00004 | 0.000000000004 | false |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "| 0.00005 | 0.000000000005 | true  |",
        "+---------+----------------+-------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn select_qualified_wildcard_join() -> Result<()> {
    let mut ctx = create_join_context("t1_id", "t2_id")?;
    let sql =
        "SELECT tb1.*, tb2.* FROM t1 tb1 JOIN t2 tb2 ON t2_id = t1_id ORDER BY t1_id";
    let expected = vec![
        "+-------+---------+-------+---------+",
        "| t1_id | t1_name | t2_id | t2_name |",
        "+-------+---------+-------+---------+",
        "| 11    | a       | 11    | z       |",
        "| 22    | b       | 22    | y       |",
        "| 44    | d       | 44    | x       |",
        "+-------+---------+-------+---------+",
    ];

    let results = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &results);

    Ok(())
}
