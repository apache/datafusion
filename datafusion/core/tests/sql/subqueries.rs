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
        TableScan: orders
        Projection: #lineitem.l_orderkey
          Aggregate: groupBy=[[#lineitem.l_orderkey]], aggr=[[]]
            Filter: #lineitem.l_commitdate < #lineitem.l_receiptdate
              TableScan: lineitem, partial_filters=[#lineitem.l_commitdate < #lineitem.l_receiptdate]"#
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

#[tokio::test]
async fn tpch_q17_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
Projection: #SUM(lineitem.l_extendedprice) / Float64(7) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
        AND #lineitem.l_quantity < (
            Subquery: Projection: Float64(0.2) * #AVG(lineitem.l_quantity)
                Aggregate: groupBy=[[]], aggr=[[AVG(#lineitem.l_quantity)]]
                    Filter: #lineitem.l_partkey = #part.p_partkey
                        TableScan: lineitem
        )
      Inner Join: #lineitem.l_partkey = #part.p_partkey
        TableScan: lineitem
        TableScan: part
     */
    let sql = r#"select sum(l_extendedprice) / 7.0 as avg_yearly
        from lineitem, part
        where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
        and l_quantity < (
            select 0.2 * avg(l_quantity)
            from lineitem where l_partkey = p_partkey);"#;

    // assert plan
    let plan = ctx
        .create_logical_plan(sql)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    println!("before:\n{}", plan.display_indent());
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Projection: #SUM(lineitem.l_extendedprice) / Float64(7) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: #lineitem.l_quantity < #__sq_1.__value
      Inner Join: #part.p_partkey = #__sq_1.l_partkey
        Inner Join: #lineitem.l_partkey = #part.p_partkey
          TableScan: lineitem
          Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
            TableScan: part, partial_filters=[#part.p_brand = Utf8("Brand#23"), #part.p_container = Utf8("MED BOX")]
        Projection: #lineitem.l_partkey, Float64(0.2) * #AVG(lineitem.l_quantity) AS __value, alias=__sq_1
          Aggregate: groupBy=[[#lineitem.l_partkey]], aggr=[[AVG(#lineitem.l_quantity)]]
            TableScan: lineitem"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+--------------------+",
        "| avg_yearly         |",
        "+--------------------+",
        "| 1901.3714285714286 |",
        "+--------------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

