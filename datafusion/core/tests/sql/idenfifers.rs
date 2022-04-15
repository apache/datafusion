use datafusion::{assert_batches_sorted_eq, prelude::*};

use crate::sql::plan_and_collect;

#[tokio::test]
async fn normalized_column_identifiers() {
    // create local execution context
    let ctx = SessionContext::new();

    // register csv file with the execution context
    ctx.register_csv(
        "case_insensitive_test",
        "tests/example.csv",
        CsvReadOptions::new(),
    )
    .await
    .unwrap();

    let sql = "SELECT A, b FROM case_insensitive_test";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Aliases

    let sql = "SELECT t.A as x, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A AS X, b FROM case_insensitive_test AS t";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t"#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Order by

    let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY x";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = "SELECT t.A AS x, b FROM case_insensitive_test AS t ORDER BY X";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT t.A AS "X", b FROM case_insensitive_test AS t ORDER BY "X""#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Where

    let sql = "SELECT a, b FROM case_insensitive_test where A IS NOT null";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 2 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    // Group by

    let sql = "SELECT a as x, count(*) as c FROM case_insensitive_test GROUP BY X";
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| x | c |",
        "+---+---+",
        "| 1 | 1 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);

    let sql = r#"SELECT a as "X", count(*) as c FROM case_insensitive_test GROUP BY "X""#;
    let result = plan_and_collect(&ctx, sql)
        .await
        .expect("ran plan correctly");
    let expected = vec![
        "+---+---+",
        "| X | c |",
        "+---+---+",
        "| 1 | 1 |",
        "+---+---+",
    ];
    assert_batches_sorted_eq!(expected, &result);
}
