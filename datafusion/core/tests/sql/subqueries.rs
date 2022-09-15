// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use crate::sql::execute_to_batches;
use datafusion::assert_batches_eq;
use datafusion::prelude::SessionContext;
use log::debug;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    let _ = env_logger::try_init();
}

#[tokio::test]
async fn correlated_recursive_scalar_subquery() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"
select c_custkey from customer
where c_acctbal < (
    select sum(o_totalprice) from orders
    where o_custkey = c_custkey
    and o_totalprice < (
            select sum(l_extendedprice) as price from lineitem where l_orderkey = o_orderkey
    )
) order by c_custkey;"#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    debug!("input:\n{}", plan.display_indent());

    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #customer.c_custkey ASC NULLS LAST
  Projection: #customer.c_custkey
    Filter: CAST(#customer.c_acctbal AS Decimal128(25, 2)) < #__sq_2.__value
      Inner Join: #customer.c_custkey = #__sq_2.o_custkey
        TableScan: customer projection=[c_custkey, c_acctbal]
        Projection: #orders.o_custkey, #SUM(orders.o_totalprice) AS __value, alias=__sq_2
          Aggregate: groupBy=[[#orders.o_custkey]], aggr=[[SUM(#orders.o_totalprice)]]
            Filter: CAST(#orders.o_totalprice AS Decimal128(25, 2)) < #__sq_1.__value
              Inner Join: #orders.o_orderkey = #__sq_1.l_orderkey
                TableScan: orders projection=[o_orderkey, o_custkey, o_totalprice]
                Projection: #lineitem.l_orderkey, #SUM(lineitem.l_extendedprice) AS price AS __value, alias=__sq_1
                  Aggregate: groupBy=[[#lineitem.l_orderkey]], aggr=[[SUM(#lineitem.l_extendedprice)]]
                    TableScan: lineitem projection=[l_orderkey, l_extendedprice]"#
        .to_string();
    assert_eq!(actual, expected);

    Ok(())
}

#[tokio::test]
async fn correlated_where_in() -> Result<()> {
    let orders = r#"1,3691,O,194029.55,1996-01-02,5-LOW,Clerk#000000951,0,
65,1627,P,99763.79,1995-03-18,1-URGENT,Clerk#000000632,0,
"#;
    let lineitems = r#"1,15519,785,1,17,24386.67,0.04,0.02,N,O,1996-03-13,1996-02-12,1996-03-22,DELIVER IN PERSON,TRUCK,
1,6731,732,2,36,58958.28,0.09,0.06,N,O,1996-04-12,1996-02-28,1996-04-20,TAKE BACK RETURN,MAIL,
65,5970,481,1,26,48775.22,0.03,0.03,A,F,1995-04-20,1995-04-25,1995-05-13,NONE,TRUCK,
65,7382,897,2,22,28366.36,0,0.05,N,O,1995-07-17,1995-06-04,1995-07-19,COLLECT COD,FOB,
"#;

    let ctx = SessionContext::new();
    register_tpch_csv_data(&ctx, "orders", orders).await?;
    register_tpch_csv_data(&ctx, "lineitem", lineitems).await?;

    let sql = r#"select o_orderkey from orders
where o_orderstatus in (
    select l_linestatus from lineitem where l_orderkey = orders.o_orderkey
);"#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Projection: #orders.o_orderkey
  Semi Join: #orders.o_orderstatus = #__sq_1.l_linestatus, #orders.o_orderkey = #__sq_1.l_orderkey
    TableScan: orders projection=[o_orderkey, o_orderstatus]
    Projection: #lineitem.l_linestatus AS l_linestatus, #lineitem.l_orderkey AS l_orderkey, alias=__sq_1
      TableScan: lineitem projection=[l_orderkey, l_linestatus]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec![
        "+------------+",
        "| o_orderkey |",
        "+------------+",
        "| 1          |",
        "+------------+",
    ];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
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
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #supplier.s_acctbal DESC NULLS FIRST, #nation.n_name ASC NULLS LAST, #supplier.s_name ASC NULLS LAST, #part.p_partkey ASC NULLS LAST
  Projection: #supplier.s_acctbal, #supplier.s_name, #nation.n_name, #part.p_partkey, #part.p_mfgr, #supplier.s_address, #supplier.s_phone, #supplier.s_comment
    Filter: #partsupp.ps_supplycost = #__sq_1.__value
      Inner Join: #part.p_partkey = #__sq_1.ps_partkey
        Inner Join: #nation.n_regionkey = #region.r_regionkey
          Inner Join: #supplier.s_nationkey = #nation.n_nationkey
            Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
              Inner Join: #part.p_partkey = #partsupp.ps_partkey
                Filter: #part.p_size = Int32(15) AND #part.p_type LIKE Utf8("%BRASS")
                  TableScan: part projection=[p_partkey, p_mfgr, p_type, p_size], partial_filters=[#part.p_size = Int32(15), #part.p_type LIKE Utf8("%BRASS")]
                TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_supplycost]
              TableScan: supplier projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]
            TableScan: nation projection=[n_nationkey, n_name, n_regionkey]
          Filter: #region.r_name = Utf8("EUROPE")
            TableScan: region projection=[r_regionkey, r_name], partial_filters=[#region.r_name = Utf8("EUROPE")]
        Projection: #partsupp.ps_partkey, #MIN(partsupp.ps_supplycost) AS __value, alias=__sq_1
          Aggregate: groupBy=[[#partsupp.ps_partkey]], aggr=[[MIN(#partsupp.ps_supplycost)]]
            Inner Join: #nation.n_regionkey = #region.r_regionkey
              Inner Join: #supplier.s_nationkey = #nation.n_nationkey
                Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                  TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_supplycost]
                  TableScan: supplier projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]
                TableScan: nation projection=[n_nationkey, n_name, n_regionkey]
              Filter: #region.r_name = Utf8("EUROPE")
                TableScan: region projection=[r_regionkey, r_name], partial_filters=[#region.r_name = Utf8("EUROPE")]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn tpch_q4_correlated() -> Result<()> {
    let orders = r#"4,13678,O,53829.87,1995-10-11,5-LOW,Clerk#000000124,0,
35,12760,O,192885.43,1995-10-23,4-NOT SPECIFIED,Clerk#000000259,0,
65,1627,P,99763.79,1995-03-18,1-URGENT,Clerk#000000632,0,
"#;
    let lineitems = r#"4,8804,579,1,30,51384,0.03,0.08,N,O,1996-01-10,1995-12-14,1996-01-18,DELIVER IN PERSON,REG AIR,
35,45,296,1,24,22680.96,0.02,0,N,O,1996-02-21,1996-01-03,1996-03-18,TAKE BACK RETURN,FOB,
65,5970,481,1,26,48775.22,0.03,0.03,A,F,1995-04-20,1995-04-25,1995-05-13,NONE,TRUCK,
"#;

    let ctx = SessionContext::new();
    register_tpch_csv_data(&ctx, "orders", orders).await?;
    register_tpch_csv_data(&ctx, "lineitem", lineitems).await?;

    let sql = r#"
        select o_orderpriority, count(*) as order_count
        from orders
        where exists (
            select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate)
        group by o_orderpriority
        order by o_orderpriority;
        "#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #orders.o_orderpriority ASC NULLS LAST
  Projection: #orders.o_orderpriority, #COUNT(UInt8(1)) AS order_count
    Aggregate: groupBy=[[#orders.o_orderpriority]], aggr=[[COUNT(UInt8(1))]]
      Semi Join: #orders.o_orderkey = #lineitem.l_orderkey
        TableScan: orders projection=[o_orderkey, o_orderpriority]
        Filter: #lineitem.l_commitdate < #lineitem.l_receiptdate
          TableScan: lineitem projection=[l_orderkey, l_commitdate, l_receiptdate]"#
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

#[ignore] // https://github.com/apache/arrow-datafusion/issues/3437
#[tokio::test]
async fn tpch_q17_correlated() -> Result<()> {
    let parts = r#"63700,goldenrod lavender spring chocolate lace,Manufacturer#1,Brand#23,PROMO BURNISHED COPPER,7,MED BOX,901.00,ly. slyly ironi
"#;
    let lineitems = r#"1,63700,7311,2,36.0,45983.16,0.09,0.06,N,O,1996-04-12,1996-02-28,1996-04-20,TAKE BACK RETURN,MAIL,ly final dependencies: slyly bold
1,63700,3701,3,1.0,13309.6,0.1,0.02,N,O,1996-01-29,1996-03-05,1996-01-31,TAKE BACK RETURN,REG AIR,"riously. regular, express dep"
"#;

    let ctx = SessionContext::new();
    register_tpch_csv_data(&ctx, "part", parts).await?;
    register_tpch_csv_data(&ctx, "lineitem", lineitems).await?;

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
    let expected = r#"Projection: CAST(#SUM(lineitem.l_extendedprice) AS Decimal128(38, 33)) / CAST(Float64(7) AS Decimal128(38, 33)) AS avg_yearly
  Aggregate: groupBy=[[]], aggr=[[SUM(#lineitem.l_extendedprice)]]
    Filter: CAST(#lineitem.l_quantity AS Decimal128(38, 21)) < #__sq_1.__value
      Inner Join: #part.p_partkey = #__sq_1.l_partkey
        Inner Join: #lineitem.l_partkey = #part.p_partkey
          TableScan: lineitem projection=[l_partkey, l_quantity, l_extendedprice]
          Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
            TableScan: part projection=[p_partkey, p_brand, p_container]
        Projection: #lineitem.l_partkey, CAST(Float64(0.2) AS Decimal128(38, 21)) * CAST(#AVG(lineitem.l_quantity) AS Decimal128(38, 21)) AS __value, alias=__sq_1
          Aggregate: groupBy=[[#lineitem.l_partkey]], aggr=[[AVG(#lineitem.l_quantity)]]
            TableScan: lineitem projection=[l_partkey, l_quantity, l_extendedprice]"#
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
    Semi Join: #supplier.s_suppkey = #__sq_2.ps_suppkey
      Inner Join: #supplier.s_nationkey = #nation.n_nationkey
        TableScan: supplier projection=[s_suppkey, s_name, s_address, s_nationkey]
        Filter: #nation.n_name = Utf8("CANADA")
          TableScan: nation projection=[n_nationkey, n_name], partial_filters=[#nation.n_name = Utf8("CANADA")]
      Projection: #partsupp.ps_suppkey AS ps_suppkey, alias=__sq_2
        Filter: CAST(#partsupp.ps_availqty AS Decimal128(38, 17)) > #__sq_3.__value
          Inner Join: #partsupp.ps_partkey = #__sq_3.l_partkey, #partsupp.ps_suppkey = #__sq_3.l_suppkey
            Semi Join: #partsupp.ps_partkey = #__sq_1.p_partkey
              TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_availqty]
              Projection: #part.p_partkey AS p_partkey, alias=__sq_1
                Filter: #part.p_name LIKE Utf8("forest%")
                  TableScan: part projection=[p_partkey, p_name], partial_filters=[#part.p_name LIKE Utf8("forest%")]
            Projection: #lineitem.l_partkey, #lineitem.l_suppkey, Decimal128(Some(50000000000000000),38,17) * CAST(#SUM(lineitem.l_quantity) AS Decimal128(38, 17)) AS __value, alias=__sq_3
              Aggregate: groupBy=[[#lineitem.l_partkey, #lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_quantity)]]
                Filter: #lineitem.l_shipdate >= Date32("8766")
                  TableScan: lineitem projection=[l_partkey, l_suppkey, l_quantity, l_shipdate], partial_filters=[#lineitem.l_shipdate >= Date32("8766")]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
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
          Filter: CAST(#customer.c_acctbal AS Decimal128(19, 6)) > #__sq_1.__value
            CrossJoin:
              Anti Join: #customer.c_custkey = #orders.o_custkey
                Filter: substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
                  TableScan: customer projection=[c_custkey, c_phone, c_acctbal], partial_filters=[substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])]
                TableScan: orders projection=[o_custkey]
              Projection: #AVG(customer.c_acctbal) AS __value, alias=__sq_1
                Aggregate: groupBy=[[]], aggr=[[AVG(#customer.c_acctbal)]]
                  Filter: CAST(#customer.c_acctbal AS Decimal128(30, 15)) > Decimal128(Some(0),30,15) AND substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
                    TableScan: customer projection=[c_phone, c_acctbal], partial_filters=[CAST(#customer.c_acctbal AS Decimal128(30, 15)) > Decimal128(Some(0),30,15), substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])]"#
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

#[tokio::test]
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
    let plan = ctx
        .optimize(&plan)
        .map_err(|e| format!("{:?} at {}", e, "error"))
        .unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Sort: #value DESC NULLS FIRST
  Projection: #partsupp.ps_partkey, #SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS value
    Filter: CAST(#SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS Decimal128(38, 17)) > #__sq_1.__value
      CrossJoin:
        Aggregate: groupBy=[[#partsupp.ps_partkey]], aggr=[[SUM(CAST(#partsupp.ps_supplycost AS Decimal128(26, 2)) * CAST(#partsupp.ps_availqty AS Decimal128(26, 2)))]]
          Inner Join: #supplier.s_nationkey = #nation.n_nationkey
            Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
              TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost]
              TableScan: supplier projection=[s_suppkey, s_nationkey]
            Filter: #nation.n_name = Utf8("GERMANY")
              TableScan: nation projection=[n_nationkey, n_name], partial_filters=[#nation.n_name = Utf8("GERMANY")]
        Projection: CAST(#SUM(partsupp.ps_supplycost * partsupp.ps_availqty) AS Decimal128(38, 17)) * Decimal128(Some(10000000000000),38,17) AS __value, alias=__sq_1
          Aggregate: groupBy=[[]], aggr=[[SUM(CAST(#partsupp.ps_supplycost AS Decimal128(26, 2)) * CAST(#partsupp.ps_availqty AS Decimal128(26, 2)))]]
            Inner Join: #supplier.s_nationkey = #nation.n_nationkey
              Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
                TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost]
                TableScan: supplier projection=[s_suppkey, s_nationkey]
              Filter: #nation.n_name = Utf8("GERMANY")
                TableScan: nation projection=[n_nationkey, n_name], partial_filters=[#nation.n_name = Utf8("GERMANY")]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &results);

    Ok(())
}
