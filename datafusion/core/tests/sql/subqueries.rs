use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;
use datafusion_optimizer::utils::reset_id;

#[tokio::test]
async fn tpch_q4_correlated() -> Result<()> {
    reset_id();
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
      Filter: Boolean(true) AND #__sq_1.l_orderkey IS NOT NULL
        Left Join: #orders.o_orderkey = #__sq_1.l_orderkey
          TableScan: orders
          Projection: #lineitem.l_orderkey, alias=__sq_1
            Aggregate: groupBy=[[#lineitem.l_orderkey]], aggr=[[]]
              Filter: #lineitem.l_commitdate < #lineitem.l_receiptdate
                TableScan: lineitem"#
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
    reset_id();
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
      Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
        Inner Join: #part.p_partkey = #__sq_1.l_partkey
          Inner Join: #lineitem.l_partkey = #part.p_partkey
            TableScan: lineitem
            TableScan: part
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

#[tokio::test]
async fn tpch_q20_correlated() -> Result<()> {
    reset_id();
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
Sort: #supplier.s_name ASC NULLS LAST
  Projection: #supplier.s_name, #supplier.s_address
    Filter: #supplier.s_suppkey IN (Subquery: Projection: #partsupp.ps_suppkey
  Filter: #partsupp.ps_partkey IN (Subquery: Projection: #part.p_partkey
  Filter: #part.p_name LIKE Utf8("forest%")
    TableScan: part) AND #partsupp.ps_availqty > (Subquery: Projection: Float64(0.5) * #SUM(lineitem.l_quantity)
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_quantity)]]
    Filter: #lineitem.l_partkey = #partsupp.ps_partkey AND #lineitem.l_suppkey = #partsupp.ps_suppkey AND #lineitem.l_shipdate >= CAST(Utf8("1994-01-01") AS Date32) AND #lineitem.l_shipdate < CAST(Utf8("1994-01-01") AS Date32) + IntervalYearMonth("12")
      TableScan: lineitem)
    TableScan: partsupp) AND #nation.n_name = Utf8("CANADA")
      Inner Join: #supplier.s_nationkey = #nation.n_nationkey
        TableScan: supplier
        TableScan: nation
     */
    let sql = r#"select s_name, s_address
from supplier, nation
where s_suppkey in (
    select ps_suppkey from partsupp
    where ps_partkey in ( select p_partkey from part where p_name like 'forest%' )
      and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem
        where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01'
    )
)
and s_nationkey = n_nationkey and n_name = 'CANADA'
order by s_name;
"#;

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
    println!("after:\n{}", actual);
    let expected = r#"Projection: #SUM(lineitem.l_extendedprice) / Float64(7) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: #lineitem.l_quantity < #__sq_1.__value
      Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
        Inner Join: #part.p_partkey = #__sq_1.l_partkey
          Inner Join: #lineitem.l_partkey = #part.p_partkey
            TableScan: lineitem
            TableScan: part
          Projection: #lineitem.l_partkey, Float64(0.2) * #AVG(lineitem.l_quantity) AS __value, alias=__sq_1
            Aggregate: groupBy=[[#lineitem.l_partkey]], aggr=[[AVG(#lineitem.l_quantity)]]
              TableScan: lineitem"#
        .to_string();
    // assert_eq!(actual, expected);

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

