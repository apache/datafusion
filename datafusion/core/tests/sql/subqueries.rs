use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;

#[tokio::test(flavor = "current_thread")]
async fn tpch_q2_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "region").await?;

    let sql = r#"select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment
from part, supplier, partsupp, nation, region
where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS'
    and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'EUROPE'
    and ps_supplycost = (
        select min(ps_supplycost) from partsupp, supplier, nation, region
        where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey and r_name = 'EUROPE'
    )
order by s_acctbal desc, n_name, s_name, p_partkey;"#;

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
    let expected = r#"Sort: #supplier.s_acctbal DESC NULLS FIRST, #nation.n_name ASC NULLS LAST, #supplier.s_name ASC NULLS LAST, #part.p_partkey ASC NULLS LAST
  Projection: #supplier.s_acctbal, #supplier.s_name, #nation.n_name, #part.p_partkey, #part.p_mfgr, #supplier.s_address, #supplier.s_phone, #supplier.s_comment
    Filter: #partsupp.ps_supplycost = #__sq_1.__value
      Filter: #part.p_size = Int64(15) AND #part.p_type LIKE Utf8("%BRASS") AND #region.r_name = Utf8("EUROPE")
        Inner Join: #part.p_partkey = #__sq_1.ps_partkey
          Inner Join: #nation.n_regionkey = #region.r_regionkey
            Inner Join: #supplier.s_nationkey = #nation.n_nationkey
              Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                Inner Join: #part.p_partkey = #partsupp.ps_partkey
                  TableScan: part
                  TableScan: partsupp
                TableScan: supplier
              TableScan: nation
            TableScan: region
          Projection: #partsupp.ps_partkey, #MIN(partsupp.ps_supplycost) AS __value, alias=__sq_1
            Aggregate: groupBy=[[#partsupp.ps_partkey]], aggr=[[MIN(#partsupp.ps_supplycost)]]
              Filter: #region.r_name = Utf8("EUROPE")
                Inner Join: #nation.n_regionkey = #region.r_regionkey
                  Inner Join: #supplier.s_nationkey = #nation.n_nationkey
                    Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                      TableScan: partsupp
                      TableScan: supplier
                    TableScan: nation
                  TableScan: region"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "++",
        "++",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tpch_q4_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

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
      Semi Join: #orders.o_orderkey = #lineitem.l_orderkey
        TableScan: orders
        Projection: #lineitem.l_orderkey
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
        "| 4-NOT SPECIFIED | 1           |",
        "| 5-LOW           | 1           |",
        "+-----------------+-------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tpch_q17_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"select sum(l_extendedprice) / 7.0 as avg_yearly
        from lineitem, part
        where p_partkey = l_partkey and p_brand = 'Brand#23' and p_container = 'MED BOX'
        and l_quantity < (
            select 0.2 * avg(l_quantity)
            from lineitem where l_partkey = p_partkey
        );"#;

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

#[tokio::test(flavor = "current_thread")]
async fn tpch_q20_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "part").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

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
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #supplier.s_name ASC NULLS LAST
  Projection: #supplier.s_name, #supplier.s_address
    Filter: #nation.n_name = Utf8("CANADA")
      Semi Join: #supplier.s_suppkey = #__sq_2.ps_suppkey
        Inner Join: #supplier.s_nationkey = #nation.n_nationkey
          TableScan: supplier
          TableScan: nation
        Projection: #partsupp.ps_suppkey, alias=__sq_2
          Aggregate: groupBy=[[#partsupp.ps_suppkey]], aggr=[[]]
            Filter: #partsupp.ps_availqty > #__sq_3.__value
              Inner Join: #partsupp.ps_partkey = #__sq_3.l_partkey, #partsupp.ps_suppkey = #__sq_3.l_suppkey
                Semi Join: #partsupp.ps_partkey = #__sq_1.p_partkey
                  TableScan: partsupp
                  Projection: #part.p_partkey, alias=__sq_1
                    Aggregate: groupBy=[[#part.p_partkey]], aggr=[[]]
                      Filter: #part.p_name LIKE Utf8("forest%")
                        TableScan: part
                Projection: #lineitem.l_partkey, #lineitem.l_suppkey, Float64(0.5) * #SUM(lineitem.l_quantity) AS __value, alias=__sq_3
                  Aggregate: groupBy=[[#lineitem.l_partkey, #lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_quantity)]]
                    Filter: #lineitem.l_shipdate >= CAST(Utf8("1994-01-01") AS Date32)
                      TableScan: lineitem"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "++",
        "++",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tpch_q22_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;

    let sql = r#"select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
from (
        select substring(c_phone from 1 for 2) as cntrycode, c_acctbal from customer
        where substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
          and c_acctbal > (
            select avg(c_acctbal) from customer where c_acctbal > 0.00
              and substring(c_phone from 1 for 2) in ('13', '31', '23', '29', '30', '18', '17')
        )
          and not exists ( select * from orders where o_custkey = c_custkey )
    ) as custsale
group by cntrycode
order by cntrycode;"#;

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
    let expected = r#"Sort: #custsale.cntrycode ASC NULLS LAST
  Projection: #custsale.cntrycode, #COUNT(UInt8(1)) AS numcust, #SUM(custsale.c_acctbal) AS totacctbal
    Aggregate: groupBy=[[#custsale.cntrycode]], aggr=[[COUNT(UInt8(1)), SUM(#custsale.c_acctbal)]]
      Projection: #custsale.cntrycode, #custsale.c_acctbal, alias=custsale
        Projection: substr(#customer.c_phone, Int64(1), Int64(2)) AS cntrycode, #customer.c_acctbal, alias=custsale
          Filter: #customer.c_acctbal > #__sq_2.__value
            Filter: substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
              CrossJoin:
                Anti Join: #customer.c_custkey = #orders.o_custkey
                  TableScan: customer
                  Projection: #orders.o_custkey
                    Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[]]
                      TableScan: orders
                Projection: #AVG(customer.c_acctbal) AS __value, alias=__sq_2
                  Aggregate: groupBy=[[]], aggr=[[AVG(#customer.c_acctbal)]]
                    Filter: #customer.c_acctbal > Float64(0) AND substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
                      TableScan: customer"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+-----------+---------+------------+",
        "| cntrycode | numcust | totacctbal |",
        "+-----------+---------+------------+",
        "| 18        | 1       | 8324.07    |",
        "| 30        | 1       | 7638.57    |",
        "+-----------+---------+------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn tpch_q11_correlated() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "partsupp").await?;
    register_tpch_csv(&ctx, "supplier").await?;
    register_tpch_csv(&ctx, "nation").await?;

    let sql = r#"select ps_partkey, sum(ps_supplycost * ps_availqty) as value
from partsupp, supplier, nation
where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
group by ps_partkey having
    sum(ps_supplycost * ps_availqty) > (
        select sum(ps_supplycost * ps_availqty) * 0.0001
        from partsupp, supplier, nation
        where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'GERMANY'
    )
order by value desc;
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
    let expected = r#"Sort: #value DESC NULLS FIRST
  Projection: #partsupp.ps_partkey, #SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS value
    Filter: #SUM(partsupp.ps_supplycost * partsupp.ps_availqty) > #__sq_1.__value
      CrossJoin:
        Aggregate: groupBy=[[#partsupp.ps_partkey]], aggr=[[SUM(#partsupp.ps_supplycost * #partsupp.ps_availqty)]]
          Filter: #nation.n_name = Utf8("GERMANY")
            Inner Join: #supplier.s_nationkey = #nation.n_nationkey
              Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                TableScan: partsupp
                TableScan: supplier
              TableScan: nation
        Projection: #SUM(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001) AS __value, alias=__sq_1
          Aggregate: groupBy=[[]], aggr=[[SUM(#partsupp.ps_supplycost * #partsupp.ps_availqty)]]
            Filter: #nation.n_name = Utf8("GERMANY")
              Inner Join: #supplier.s_nationkey = #nation.n_nationkey
                Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                  TableScan: partsupp
                  TableScan: supplier
                TableScan: nation"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "++",
        "++",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}
