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

#[tokio::test]
async fn where_in_multiple() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"select c_custkey from customer
    where c_custkey in ( select o_custkey from orders where o_custkey=c_custkey)
    and c_custkey in ( select o_custkey from orders where o_custkey=c_custkey);"#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    let expected = r#"Projection: #customer.c_custkey
  Filter: #customer.c_custkey IN (<subquery>) AND #customer.c_custkey IN (<subquery>)
    Subquery:
      Projection: #orders.o_custkey
        Filter: #orders.o_custkey = #customer.c_custkey
          TableScan: orders
    Subquery:
      Projection: #orders.o_custkey
        Filter: #orders.o_custkey = #customer.c_custkey
          TableScan: orders
    TableScan: customer"#;
    assert_eq!(format!("{}", plan.display_indent()), expected);

    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"unknown"#.to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn where_in_recursive() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "customer").await?;
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;

    let sql = r#"select c_custkey from customer where c_custkey in (
    select o_custkey from orders where o_orderkey in (
        select l_orderkey from lineitem where l_orderkey = o_orderkey
     ) and o_custkey=c_custkey
);"#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    let expected = r#"Projection: #customer.c_custkey
  Filter: #customer.c_custkey IN (<subquery>)
    Subquery:
      Projection: #orders.o_custkey
        Filter: #orders.o_orderkey IN (<subquery>) AND #orders.o_custkey = #customer.c_custkey
          Subquery:
            Projection: #lineitem.l_orderkey
              Filter: #lineitem.l_orderkey = #orders.o_orderkey
                TableScan: lineitem
          TableScan: orders
    TableScan: customer"#;
    assert_eq!(format!("{}", plan.display_indent()), expected);

    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"unknown"#.to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
    assert_batches_eq!(expected, &results);

    Ok(())
}

#[tokio::test]
async fn correlated_where_in() -> Result<()> {
    let ctx = SessionContext::new();
    register_tpch_csv(&ctx, "orders").await?;
    register_tpch_csv(&ctx, "lineitem").await?;
    register_tpch_csv(&ctx, "partsupp").await?;

    let sql = r#"select * from orders
inner join lineitem on o_orderkey = l_orderkey
where l_partkey in ( select ps_partkey from partsupp where ps_suppkey = l_suppkey );"#;

    // assert plan
    let plan = ctx.create_logical_plan(sql).unwrap();
    let plan = ctx.optimize(&plan).unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected = r#"Projection: #orders.o_orderkey, #orders.o_custkey, #orders.o_orderstatus, #orders.o_totalprice, #orders.o_orderdate, #orders.o_orderpriority, #orders.o_clerk, #orders.o_shippriority, #orders.o_comment, #lineitem.l_orderkey, #lineitem.l_partkey, #lineitem.l_suppkey, #lineitem.l_linenumber, #lineitem.l_quantity, #lineitem.l_extendedprice, #lineitem.l_discount, #lineitem.l_tax, #lineitem.l_returnflag, #lineitem.l_linestatus, #lineitem.l_shipdate, #lineitem.l_commitdate, #lineitem.l_receiptdate, #lineitem.l_shipinstruct, #lineitem.l_shipmode, #lineitem.l_comment
  Semi Join: #lineitem.l_partkey = #partsupp.ps_partkey, #lineitem.l_suppkey = #partsupp.ps_suppkey
    Inner Join: #orders.o_orderkey = #lineitem.l_orderkey
      TableScan: orders projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]
      TableScan: lineitem projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]
    Projection: #partsupp.ps_partkey, #partsupp.ps_suppkey
      TableScan: partsupp projection=[ps_partkey, ps_suppkey]"#
        .to_string();
    assert_eq!(actual, expected);

    // assert data
    let results = execute_to_batches(&ctx, sql).await;
    let expected = vec!["++", "++"];
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
                Filter: #part.p_size = Int64(15) AND #part.p_type LIKE Utf8("%BRASS")
                  TableScan: part projection=[p_partkey, p_mfgr, p_type, p_size], partial_filters=[#part.p_size = Int64(15), #part.p_type LIKE Utf8("%BRASS")]
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
        TableScan: orders projection=[o_orderkey, o_orderpriority]
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
        "| 4-NOT SPECIFIED | 1           |",
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
      Inner Join: #part.p_partkey = #__sq_1.l_partkey
        Inner Join: #lineitem.l_partkey = #part.p_partkey
          TableScan: lineitem projection=[l_partkey, l_quantity, l_extendedprice]
          Filter: #part.p_brand = Utf8("Brand#23") AND #part.p_container = Utf8("MED BOX")
            TableScan: part projection=[p_partkey, p_brand, p_container], partial_filters=[#part.p_brand = Utf8("Brand#23"), #part.p_container = Utf8("MED BOX")]
        Projection: #lineitem.l_partkey, Float64(0.2) * #AVG(lineitem.l_quantity) AS __value, alias=__sq_1
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
    Semi Join: #supplier.s_suppkey = #partsupp.ps_suppkey
      Inner Join: #supplier.s_nationkey = #nation.n_nationkey
        TableScan: supplier projection=[s_suppkey, s_name, s_address, s_nationkey]
        Filter: #nation.n_name = Utf8("CANADA")
          TableScan: nation projection=[n_nationkey, n_name], partial_filters=[#nation.n_name = Utf8("CANADA")]
      Projection: #partsupp.ps_suppkey
        Filter: #partsupp.ps_availqty > #__sq_1.__value
          Inner Join: #partsupp.ps_partkey = #__sq_1.l_partkey, #partsupp.ps_suppkey = #__sq_1.l_suppkey
            Semi Join: #partsupp.ps_partkey = #part.p_partkey
              TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_availqty]
              Projection: #part.p_partkey
                Filter: #part.p_name LIKE Utf8("forest%")
                  TableScan: part projection=[p_partkey, p_name], partial_filters=[#part.p_name LIKE Utf8("forest%")]
            Projection: #lineitem.l_partkey, #lineitem.l_suppkey, Float64(0.5) * #SUM(lineitem.l_quantity) AS __value, alias=__sq_1
              Aggregate: groupBy=[[#lineitem.l_partkey, #lineitem.l_suppkey]], aggr=[[SUM(#lineitem.l_quantity)]]
                Filter: #lineitem.l_shipdate >= CAST(Utf8("1994-01-01") AS Date32)
                  TableScan: lineitem projection=[l_partkey, l_suppkey, l_quantity, l_shipdate], partial_filters=[#lineitem.l_shipdate >= CAST(Utf8("1994-01-01") AS Date32)]"#
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
          Filter: #customer.c_acctbal > #__sq_1.__value
            CrossJoin:
              Anti Join: #customer.c_custkey = #orders.o_custkey
                Filter: substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
                  TableScan: customer projection=[c_custkey, c_phone, c_acctbal], partial_filters=[substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])]
                TableScan: orders projection=[o_custkey]
              Projection: #AVG(customer.c_acctbal) AS __value, alias=__sq_1
                Aggregate: groupBy=[[]], aggr=[[AVG(#customer.c_acctbal)]]
                  Filter: #customer.c_acctbal > Float64(0) AND substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])
                    TableScan: customer projection=[c_phone, c_acctbal], partial_filters=[#customer.c_acctbal > Float64(0), substr(#customer.c_phone, Int64(1), Int64(2)) IN ([Utf8("13"), Utf8("31"), Utf8("23"), Utf8("29"), Utf8("30"), Utf8("18"), Utf8("17")])]"#
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
          Inner Join: #supplier.s_nationkey = #nation.n_nationkey
            Inner Join: #partsupp.ps_suppkey = #supplier.s_suppkey
              TableScan: partsupp projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost]
              TableScan: supplier projection=[s_suppkey, s_nationkey]
            Filter: #nation.n_name = Utf8("GERMANY")
              TableScan: nation projection=[n_nationkey, n_name], partial_filters=[#nation.n_name = Utf8("GERMANY")]
        Projection: #SUM(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001) AS __value, alias=__sq_1
          Aggregate: groupBy=[[]], aggr=[[SUM(#partsupp.ps_supplycost * #partsupp.ps_availqty)]]
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
