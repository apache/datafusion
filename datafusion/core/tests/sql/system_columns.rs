use super::*;
use datafusion_common::FieldExt;

#[tokio::test]
async fn test_basic_select() {
    let ctx = setup_test_context().await;

    let select = "SELECT * FROM test order by id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------------+",
        "| id | bank_account |",
        "+----+--------------+",
        "| 1  | 9000         |",
        "| 2  | 100          |",
        "| 3  | 1000         |",
        "+----+--------------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_select() {
    let ctx = setup_test_context().await;

    // Basic _rowid select
    let select = "SELECT _rowid FROM test order by _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+",
        "| _rowid |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // _rowid with regular column
    let select = "SELECT _rowid, id FROM test order by _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 0      | 1  |",
        "| 1      | 2  |",
        "| 2      | 3  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_filtering() {
    let ctx = setup_test_context().await;

    // Filter by exact _rowid
    let select = "SELECT _rowid, id FROM test WHERE _rowid = 0";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 0      | 1  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Filter by _rowid with and operator
    let select = "SELECT _rowid, id FROM test WHERE _rowid % 2 = 1";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+--------+----+",
        "| _rowid | id |",
        "+--------+----+",
        "| 1      | 2  |",
        "+--------+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Filter without selecting
    let select = "SELECT id FROM test WHERE _rowid = 0";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 1  |",
        "+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_ordering() {
    let ctx = setup_test_context().await;

    let select = "SELECT id FROM test order by _rowid asc";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+",
        "| id |",
        "+----+",
        "| 1  |",
        "| 2  |",
        "| 3  |",
        "+----+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_joins() {
    let ctx = setup_test_context().await;
    setup_test2_for_joins(&ctx).await;

    // Join with non-system _rowid column and select _row_id
    // Normally this would result in an AmbiguousReference error because both tables have a _rowid column
    // But when this conflict is between a system column and a regular column, the regular column is chosen
    // to resolve the conflict without error.
    let select =
        "SELECT id, other_id, _rowid FROM test INNER JOIN test2 ON id = other_id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+---------+",
        "| id | other_id | _rowid |",
        "+----+----------+---------+",
        "| 1  | 1        | 10      |",
        "| 2  | 2        | 11      |",
        "| 3  | 3        | 12      |",
        "+----+----------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
    // Same in this case, `test._rowid` is discarded because it is a system column
    let select =
        "SELECT test.*, test2._row_id FROM test INNER JOIN test2 ON id = other_id";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+---------+",
        "| id | other_id | _rowid |",
        "+----+----------+---------+",
        "| 1  | 1        | 10      |",
        "| 2  | 2        | 11      |",
        "| 3  | 3        | 12      |",
        "+----+----------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);

    // Join on system _rowid columns
    setup_test2_with_system_rowid(&ctx).await;
    let select = "SELECT id, other_id, _rowid FROM test JOIN test2 ON _rowid = _rowid";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+----------+---------+",
        "| id | other_id | _rowid |",
        "+----+----------+---------+",
        "| 2  | 2        | 2       |",
        "+----+----------+---------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

#[tokio::test]
async fn test_system_column_with_cte() {
    let ctx = setup_test_context().await;

    // System columns not available after CTE
    let select = r"
        WITH cte AS (SELECT * FROM test)
        SELECT _rowid FROM cte
    ";
    assert!(ctx.sql(select).await.is_err());

    // Explicitly selected system columns become regular columns
    let select = r"
        WITH cte AS (SELECT id, _rowid FROM test)
        SELECT * FROM cte
    ";
    let df = ctx.sql(select).await.unwrap();
    let batches = df.collect().await.unwrap();
    #[rustfmt::skip]
    let expected = [
        "+----+--------+",
        "| id | _rowid |",
        "+----+--------+",
        "| 1  | 0      |",
        "| 2  | 1      |",
        "| 3  | 2      |",
        "+----+--------+",
    ];
    assert_batches_sorted_eq!(expected, &batches);
}

async fn setup_test_context() -> SessionContext {
    let batch = record_batch!(
        ("id", UInt8, [1, 2, 3]),
        ("bank_account", UInt64, [9000, 100, 1000]),
        ("_rowid", UInt32, [0, 1, 2]),
        ("_file", Utf8, ["file-0", "file-1", "file-2"])
    )
    .unwrap();
    let batch = batch
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt8, true),
            Field::new("bank_account", DataType::UInt64, true),
            Field::new("_rowid", DataType::UInt32, true).to_system_column(),
            Field::new("_file", DataType::Utf8, true).to_system_column(),
        ])))
        .unwrap();

    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_information_schema(true),
    );
    let _ = ctx.register_batch("test", batch);
    ctx
}

async fn setup_test2_for_joins(ctx: &SessionContext) {
    let batch = record_batch!(
        ("other_id", UInt8, [1, 2, 3]),
        ("bank_account", UInt64, [9, 10, 11]),
        ("_rowid", UInt32, [10, 11, 12])
    )
    .unwrap();
    let _ = ctx.register_batch("test2", batch);
}

async fn setup_test2_with_system_rowid(ctx: &SessionContext) {
    let batch = record_batch!(
        ("other_id", UInt8, [2, 3, 4]),
        ("_rowid", UInt32, [2, 3, 4])
    )
    .unwrap();
    let batch = batch
        .with_schema(Arc::new(Schema::new(vec![
            Field::new("other_id", DataType::UInt8, true),
            Field::new("_rowid", DataType::UInt32, true).to_system_column(),
        ])))
        .unwrap();
    let _ = ctx.register_batch("test2", batch);
}
