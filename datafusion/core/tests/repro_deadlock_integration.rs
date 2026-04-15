use std::sync::Arc;
use datafusion::prelude::*;
use datafusion::error::Result;
use datafusion::assert_batches_eq;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::array::{Int32Array, StringArray};

#[tokio::test]
async fn test_hash_join_dynamic_filter_deadlock_with_empty_partition() -> Result<()> {
    // We use 2 partitions and 2 rows, but we use a filter that puts all rows in one partition
    // and zero rows in the other partition.
    let config = SessionConfig::new()
        .with_target_partitions(2)
        .set_bool("datafusion.optimizer.enable_round_robin_repartition", false)
        .set_bool("datafusion.optimizer.repartition_joins", true)
        .set_bool("datafusion.optimizer.prefer_hash_join", true)
        .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 0); // Force Partitioned mode
    let ctx = SessionContext::new_with_config(config);

    // Create customer table with 2 rows that will both end up in the same partition
    // if the join key is the same (or hashes to the same partition).
    let customer_schema = Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
    ]));
    // Both rows have c_custkey = 1, so they MUST go to the same partition in Partitioned join
    let customer_batch = RecordBatch::try_new(
        customer_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec!["A", "B"])),
        ],
    )?;
    // Register as a table provider with 2 partitions to force Partitioned mode
    let customer_source = datafusion::datasource::MemTable::try_new(customer_schema, vec![vec![customer_batch.clone()], vec![]])?;
    ctx.register_table("customer", Arc::new(customer_source))?;

    // Create orders table
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("o_orderkey", DataType::Int32, false),
        Field::new("o_custkey", DataType::Int32, false),
    ]));
    let orders_batch = RecordBatch::try_new(
        orders_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int32Array::from(vec![1, 1])),
        ],
    )?;
    let orders_source = datafusion::datasource::MemTable::try_new(orders_schema, vec![vec![orders_batch.clone()], vec![]])?;
    ctx.register_table("orders", Arc::new(orders_source))?;

    // Query with dynamic filter (IN subquery)
    // The build side (right side of join) will have 2 partitions.
    // Since all o_custkey are 1, one partition will have 2 rows, and the other will have 0 rows.
    let sql = "
        SELECT c_name, o_orderkey
        FROM customer
        JOIN orders ON c_custkey = o_custkey
        WHERE o_orderkey IN (SELECT o_orderkey FROM orders WHERE o_orderkey > 15)
    ";

    let dataframe = ctx.sql(sql).await?;
    let plan = dataframe.create_physical_plan().await?;
    
    // Verify Partitioned mode
    let plan_str = format!("{:?}", plan);
    println!("Plan: {}", plan_str);
    assert!(plan_str.contains("HashJoinExec"), "Plan should use a hash join but got: {}", plan_str);

    // Wrap the collection in a timeout to detect deadlock
    let task_ctx = ctx.task_ctx();
    let results = tokio::time::timeout(std::time::Duration::from_secs(60), async move {
        datafusion::physical_plan::collect(plan, task_ctx).await
    }).await.expect("DEADLOCK DETECTED: Query timed out after 1 minute")?;
    
    let expected = vec![
        "+--------+------------+",
        "| c_name | o_orderkey |",
        "+--------+------------+",
        "| A      | 20         |",
        "| B      | 20         |",
        "+--------+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
