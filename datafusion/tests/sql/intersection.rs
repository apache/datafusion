use super::*;

#[tokio::test]
async fn intersect_with_null_not_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 2 AS id2) t2";

    let expected = vec!["++", "++"];
    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;
    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn intersect_with_null_equal() {
    let sql = "SELECT * FROM (SELECT null AS id1, 1 AS id2) t1
            INTERSECT SELECT * FROM (SELECT null AS id1, 1 AS id2) t2";

    let expected = vec![
        "+-----+-----+",
        "| id1 | id2 |",
        "+-----+-----+",
        "|     | 1   |",
        "+-----+-----+",
    ];

    let mut ctx = create_join_context_qualified().unwrap();
    let actual = execute_to_batches(&mut ctx, sql).await;

    assert_batches_eq!(expected, &actual);
}

#[tokio::test]
async fn test_intersect_all() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT ALL SELECT int_col, double_col FROM alltypes_plain LIMIT 4";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_intersect_distinct() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT int_col, double_col FROM alltypes_plain where int_col > 0 INTERSECT SELECT int_col, double_col FROM alltypes_plain";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+---------+------------+",
        "| int_col | double_col |",
        "+---------+------------+",
        "| 1       | 10.1       |",
        "+---------+------------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
