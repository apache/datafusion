use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;

#[tokio::test]
async fn tpch_q4_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
    #orders.o_orderpriority ASC NULLS LAST
        Projection: #orders.o_orderpriority, #COUNT(UInt8(1)) AS order_count
            Aggregate: groupBy=[[#orders.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
                Filter: EXISTS (                                                         -- plan
                    Subquery: Projection: *                                              -- proj
                        Filter: #lineitem.l_orderkey = #orders.o_orderkey                -- filter
                            TableScan: lineitem projection=None                          -- filter.input
                )
                    TableScan: orders projection=None                                    -- plan.inputs
                 */
    let sql = r#"
        select o_orderpriority, count(*) as order_count
        from orders
        where exists (
            select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
        group by o_orderpriority
        order by o_orderpriority;
        "#;

    // assert plan
    let plan = ctx
        .create_logical_plan(sql)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #orders.o_orderpriority ASC NULLS LAST
  Projection: #orders.o_orderpriority, #COUNT(UInt8(1)) AS order_count
    Aggregate: groupBy=[[#orders.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
      Inner Join: #orders.o_orderkey = #lineitem.l_orderkey
        TableScan: orders projection=[o_orderkey, o_orderpriority]
        Projection: #lineitem.l_orderkey
          Aggregate: groupBy=[[#lineitem.l_orderkey]], aggr=[[]]
            Filter: #lineitem.l_commitdate < #lineitem.l_receiptdate
              TableScan: lineitem projection=[l_orderkey, l_commitdate, l_receiptdate], partial_filters=[#lineitem.l_commitdate < #lineitem.l_receiptdate]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------------+-------------+",
        "| o_orderpriority | order_count |",
        "+-----------------+-------------+",
        "| 1-URGENT        | 1           |",
        "| 5-LOW           | 1           |",
        "+-----------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
