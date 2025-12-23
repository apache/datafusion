use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::logical_expr::col;
use datafusion::prelude::*;

#[tokio::main]
async fn main() {
    let ctx = SessionContext::default();

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    // create an empty but multi-partitioned MemTable
    let mem_table = MemTable::try_new(schema.clone(), vec![vec![], vec![]]).unwrap();
    ctx.register_table("metrics", Arc::new(mem_table)).unwrap();

    // aggregate and sort twice
    let data_frame = ctx
        .table("metrics")
        .await
        .unwrap()
        .aggregate(
            vec![col("region"), col("ts")],
            vec![count_udaf().call(vec![col("value")])],
        )
        .unwrap()
        .sort(vec![
            col("region").sort(true, true),
            col("ts").sort(true, true),
        ])
        .unwrap()
        .aggregate(
            vec![col("ts")],
            vec![count_udaf().call(vec![col("count(metrics.value)")])],
        )
        .unwrap()
        .sort(vec![col("ts").sort(true, true)])
        .unwrap();

    println!(
        "Logical Plan:\n{}",
        data_frame.logical_plan().display_indent()
    );

    data_frame.show().await.unwrap();
    println!("should not panic and print data_frame")
}
