use super::*;
#[tokio::test]
async fn test_sort_unprojected_col() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_alltypes_parquet(&mut ctx).await;
    // execute the query
    let sql = "SELECT id FROM alltypes_plain ORDER BY int_col, double_col";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| id |", "+----+", "| 4  |", "| 6  |", "| 2  |", "| 0  |", "| 5  |",
        "| 7  |", "| 3  |", "| 1  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_asc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 1   | one    |",
        "| 2   | two    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_nulls_first_desc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 2   | two    |",
        "| 1   | one    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_last_desc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num DESC NULLS LAST";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "| 2   | two    |",
        "| 1   | one    |",
        "|     | three  |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn test_specific_nulls_first_asc() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    let sql = "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (null, 'three')) AS t (num,letter) ORDER BY num ASC NULLS FIRST";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+-----+--------+",
        "| num | letter |",
        "+-----+--------+",
        "|     | three  |",
        "| 1   | one    |",
        "| 2   | two    |",
        "+-----+--------+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
