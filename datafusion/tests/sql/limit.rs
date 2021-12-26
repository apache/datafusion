use super::*;

#[tokio::test]
async fn csv_query_limit() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 2";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["+----+", "| c1 |", "+----+", "| c  |", "| d  |", "+----+"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_bigger_than_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 200";
    let actual = execute_to_batches(&mut ctx, sql).await;
    // println!("{}", pretty_format_batches(&a).unwrap());
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_with_same_nbr_of_rows() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c2 FROM aggregate_test_100 LIMIT 100";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec![
        "+----+", "| c2 |", "+----+", "| 2  |", "| 5  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 3  |", "| 3  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 3  |",
        "| 2  |", "| 1  |", "| 1  |", "| 2  |", "| 1  |", "| 3  |", "| 2  |", "| 4  |",
        "| 1  |", "| 5  |", "| 4  |", "| 2  |", "| 1  |", "| 4  |", "| 5  |", "| 2  |",
        "| 3  |", "| 4  |", "| 2  |", "| 1  |", "| 5  |", "| 3  |", "| 1  |", "| 2  |",
        "| 3  |", "| 3  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |", "| 2  |",
        "| 5  |", "| 2  |", "| 1  |", "| 4  |", "| 1  |", "| 4  |", "| 2  |", "| 5  |",
        "| 4  |", "| 2  |", "| 3  |", "| 4  |", "| 4  |", "| 4  |", "| 5  |", "| 4  |",
        "| 2  |", "| 1  |", "| 2  |", "| 4  |", "| 2  |", "| 3  |", "| 5  |", "| 1  |",
        "| 1  |", "| 4  |", "| 2  |", "| 1  |", "| 2  |", "| 1  |", "| 1  |", "| 5  |",
        "| 4  |", "| 5  |", "| 2  |", "| 3  |", "| 2  |", "| 4  |", "| 1  |", "| 3  |",
        "| 4  |", "| 3  |", "| 2  |", "| 5  |", "| 3  |", "| 3  |", "| 2  |", "| 5  |",
        "| 5  |", "| 4  |", "| 1  |", "| 3  |", "| 3  |", "| 4  |", "| 4  |", "+----+",
    ];
    assert_batches_eq!(expected, &actual);
    Ok(())
}

#[tokio::test]
async fn csv_query_limit_zero() -> Result<()> {
    let mut ctx = ExecutionContext::new();
    register_aggregate_csv(&mut ctx).await?;
    let sql = "SELECT c1 FROM aggregate_test_100 LIMIT 0";
    let actual = execute_to_batches(&mut ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &actual);
    Ok(())
}
