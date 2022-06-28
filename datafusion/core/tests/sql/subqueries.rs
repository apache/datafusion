use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;

/// https://github.com/apache/arrow-datafusion/issues/171
#[tokio::test]
async fn tpch_q20() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
        select s_name, s_address
        from supplier, nation
        where s_suppkey in (
            select ps_suppkey from partsupp
            where ps_partkey in ( select p_partkey from part where p_name like 'forest%' )
              and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem
                where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1994-01-01' + interval '1' year
            )
        )
        and s_nationkey = n_nationkey and n_name = 'CANADA'
        order by s_name;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q20_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
    #partsupp.ps_suppkey ASC NULLS LAST
          Projection: #partsupp.ps_suppkey
            Filter: #partsupp.ps_availqty > (
                Subquery: Projection: Float64(0.5) * #SUM(lineitem.l_quantity)
                  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_quantity)]]
                    Filter: #lineitem.l_partkey = #partsupp.ps_partkey AND #lineitem.l_suppkey = #partsupp.ps_suppkey
                      TableScan: lineitem projection=None
              )
              TableScan: partsupp projection=None
             */
    let sql = r#"
        select ps_suppkey from partsupp
        where ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem
            where l_partkey = ps_partkey and l_suppkey = ps_suppkey
        ) order by ps_suppkey;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| suppkey |",
        "+---------+",
        "| 7311    |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

// 0. recurse down to most deeply nested subquery
// 1. find references to outer scope (ps_partkey, ps_suppkey), if none, bail - not correlated
// 2. remove correlated fields from filter
// 3. add correlated fields as group by & to projection
#[tokio::test]
async fn tpch_q20_decorrelated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
    #suppkey
      Sort: #ps.ps_suppkey ASC NULLS LAST
        Projection: #ps.ps_suppkey AS suppkey, #ps.ps_suppkey
          Inner Join: #ps.ps_suppkey = #av.l_suppkey, #ps.ps_partkey = #av.l_partkey Filter: #ps.ps_availqty > #av.threshold
            SubqueryAlias: ps
              TableScan: partsupp projection=Some([ps_partkey, ps_suppkey, ps_availqty])
            Projection: #av.l_partkey, #av.l_suppkey, #av.threshold, alias=av
              Projection: #lineitem.l_partkey, #lineitem.l_suppkey, Float64(0.5) * #SUM(lineitem.l_quantity) AS threshold, alias=av
                Aggregate: groupBy=[[#lineitem.l_partkey, #lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_quantity)]]
                  TableScan: lineitem projection=Some([l_partkey, l_suppkey, l_quantity])
         */
    let sql = r#"
        select ps_suppkey as suppkey
        from partsupp ps
        inner join (
            select l_partkey, l_suppkey, 0.5 * sum(l_quantity) as threshold from lineitem
            group by l_partkey, l_suppkey
        ) av on av.l_suppkey=ps.ps_suppkey and av.l_partkey=ps.ps_partkey and ps.ps_availqty > av.threshold
        order by ps_suppkey;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| suppkey |",
        "+---------+",
        "| 7311    |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

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
                Filter: #lineitem.l_orderkey = #orders.o_orderkey                    -- filter
                TableScan: lineitem projection=None                                  -- filter.input
            )
                TableScan: orders projection=None                                    -- plan.inputs
             */
    let sql = r#"
        select o_orderpriority, count(*) as order_count
        from orders
        where exists ( select * from lineitem where l_orderkey = o_orderkey )
        group by o_orderpriority
        order by o_orderpriority;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| suppkey |",
        "+---------+",
        "| 7311    |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q4_decorrelated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    /*
#o.o_orderpriority ASC NULLS LAST
  Projection: #o.o_orderpriority, #COUNT(UInt8(1)) AS order_count
    Aggregate: groupBy=[[#o.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
      Inner Join: #o.o_orderkey = #l.l_orderkey
        SubqueryAlias: o
          TableScan: orders projection=Some([o_orderkey, o_orderpriority])
        Projection: #l.l_orderkey, alias=l
          Projection: #lineitem.l_orderkey, alias=l
            Aggregate: groupBy=[[#lineitem.l_orderkey]], aggr=[[]]
              TableScan: lineitem projection=Some([l_orderkey])
             */
    let sql = r#"
        select o_orderpriority, count(*) as order_count
        from orders o
        inner join ( select l_orderkey from lineitem group by l_orderkey ) l on l.l_orderkey = o_orderkey
        group by o_orderpriority
        order by o_orderpriority;
        "#;
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
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "part").await?;

    /*
#SUM(lineitem.l_extendedprice) / Float64(7) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX") AND #lineitem.l_quantity < (
        Subquery: Projection: Float64(0.2) * #AVG(lineitem.l_quantity)
            Aggregate: groupBy=[[]], aggr=[[AVG(#lineitem.l_quantity)]]
            Filter: #lineitem.l_partkey = #part.p_partkey
              TableScan: lineitem projection=None
      )
      Inner Join: #lineitem.l_partkey = #part.p_partkey
        TableScan: lineitem projection=None
        TableScan: part projection=None
        */
    let sql = r#"
        select sum(l_extendedprice) / 7.0 as avg_yearly
        from lineitem, part
        where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
          and l_quantity < ( select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey
        );
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| suppkey |",
        "+---------+",
        "| 7311    |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q17_decorrelated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "part").await?;

    /*
    #SUM(lineitem.l_extendedprice) / Float64(7) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: #lineitem.l_quantity < #li.qty
      Inner Join: #part.p_partkey = #li.l_partkey
        Inner Join: #lineitem.l_partkey = #part.p_partkey
          TableScan: lineitem projection=Some([l_partkey, l_quantity, l_extendedprice])
          Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
            TableScan: part projection=Some([p_partkey, p_brand, p_container]), partial_filters=[#part.p_brand = Utf8("Brand#23"), #part.p_container = Utf8("MED BOX")]
        Projection: #li.l_partkey, #li.qty, alias=li
          Projection: #lineitem.l_partkey, Float64(0.2) * #AVG(lineitem.l_quantity) AS qty, alias=li
            Aggregate: groupBy=[[#lineitem.l_partkey]], aggr=[[AVG(#lineitem.l_quantity)]]
              TableScan: lineitem projection=Some([l_partkey, l_quantity, l_extendedprice])
             */
    let sql = r#"
        select sum(l_extendedprice) / 7.0 as avg_yearly
        from lineitem
        inner join part on p_partkey = l_partkey
        inner join ( select l_partkey, 0.2 * avg(l_quantity) as qty from lineitem group by l_partkey
            ) li on li.l_partkey = p_partkey
        where p_brand = 'Brand#23' and p_container = 'MED BOX' and l_quantity < li.qty;
        "#;
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+------------+",
        "| avg_yearly |",
        "+------------+",
        "|            |",
        "+------------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn scalar_subquery() -> Result<()> {
    let ctx = SessionContext::new();

    let sql = "select * from (values (1)) where column1 > ( select 0.5 );";
    let results = execute_to_batches(&ctx, sql).await;

    let expected = vec![
        "+---------+",
        "| c1      |",
        "+---------+",
        "| 0.00005 |",
        "+---------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn filter_to_join() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "nation").await?;

    /*
    Sort: #customer.c_custkey ASC NULLS LAST
      Projection: #customer.c_custkey
        Filter: #customer.c_nationkey IN (
                Subquery: Projection: #nation.n_nationkey TableScan: nation projection=None
          )
          TableScan: customer projection=None
         */
    let sql = r#"
        select c_custkey from customer
        where c_nationkey in (select n_nationkey from nation)
        order by c_custkey;
        "#;
    let results = execute_to_batches(&ctx, sql).await;
    /*
    Sort: #customer.c_custkey ASC NULLS LAST
      Projection: #customer.c_custkey
        Semi Join: #customer.c_nationkey = #nation.n_nationkey
          TableScan: customer projection=Some([c_custkey, c_nationkey])
          Projection: #nation.n_nationkey
            TableScan: nation projection=Some([n_nationkey])
         */

    let expected = vec![
        "+-----------+",
        "| c_custkey |",
        "+-----------+",
        "| 3         |",
        "| 4         |",
        "| 5         |",
        "| 9         |",
        "| 10        |",
        "+-----------+",
    ];

    assert_batches_eq!(expected, &results);

    Ok(())
}
