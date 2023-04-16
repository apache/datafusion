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
    let dataframe = ctx.sql(sql).await.unwrap();
    debug!("input:\n{}", dataframe.logical_plan().display_indent());

    let plan = dataframe.into_optimized_plan().unwrap();
    let actual = format!("{}", plan.display_indent());
    let expected =  "Sort: customer.c_custkey ASC NULLS LAST\
    \n  Projection: customer.c_custkey\
    \n    Inner Join: customer.c_custkey = __scalar_sq_1.o_custkey Filter: CAST(customer.c_acctbal AS Decimal128(25, 2)) < __scalar_sq_1.__value\
    \n      TableScan: customer projection=[c_custkey, c_acctbal]\
    \n      SubqueryAlias: __scalar_sq_1\
    \n        Projection: orders.o_custkey, SUM(orders.o_totalprice) AS __value\
    \n          Aggregate: groupBy=[[orders.o_custkey]], aggr=[[SUM(orders.o_totalprice)]]\
    \n            Projection: orders.o_custkey, orders.o_totalprice\
    \n              Inner Join: orders.o_orderkey = __scalar_sq_2.l_orderkey Filter: CAST(orders.o_totalprice AS Decimal128(25, 2)) < __scalar_sq_2.__value\
    \n                TableScan: orders projection=[o_orderkey, o_custkey, o_totalprice]\
    \n                SubqueryAlias: __scalar_sq_2\
    \n                  Projection: lineitem.l_orderkey, SUM(lineitem.l_extendedprice) AS price AS __value\
    \n                    Aggregate: groupBy=[[lineitem.l_orderkey]], aggr=[[SUM(lineitem.l_extendedprice)]]\
    \n                      TableScan: lineitem projection=[l_orderkey, l_extendedprice]";
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
    let dataframe = ctx.sql(sql).await.unwrap();
    let plan = dataframe.into_optimized_plan().unwrap();
    let actual = format!("{}", plan.display_indent());

    let expected = "Projection: orders.o_orderkey\
    \n  LeftSemi Join: orders.o_orderstatus = __correlated_sq_1.l_linestatus, orders.o_orderkey = __correlated_sq_1.l_orderkey\
    \n    TableScan: orders projection=[o_orderkey, o_orderstatus]\
    \n    SubqueryAlias: __correlated_sq_1\
    \n      Projection: lineitem.l_linestatus AS l_linestatus, lineitem.l_orderkey\
    \n        TableScan: lineitem projection=[l_orderkey, l_linestatus]";
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
async fn exists_subquery_with_same_table() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    // Subquery and outer query refer to the same table.
    // It will not be rewritten to join because it is not a correlated subquery.
    let sql = "SELECT t1_id, t1_name, t1_int FROM t1 WHERE EXISTS(SELECT t1_int FROM t1 WHERE t1.t1_id > t1.t1_int)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "    Subquery: [t1_int:UInt32;N]",
        "      Projection: t1.t1_int [t1_int:UInt32;N]",
        "        Filter: t1.t1_id > t1.t1_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "          TableScan: t1 [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "    TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn in_subquery_with_same_table() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    // Subquery and outer query refer to the same table.
    // It will be rewritten to join because in-subquery has extra predicate(`t1.t1_id = __correlated_sq_1.t1_int`).
    let sql = "SELECT t1_id, t1_name, t1_int FROM t1 WHERE t1_id IN(SELECT t1_int FROM t1 WHERE t1.t1_id > t1.t1_int)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(&("explain ".to_owned() + sql)).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Explain [plan_type:Utf8, plan:Utf8]",
        "  LeftSemi Join: t1.t1_id = __correlated_sq_1.t1_int [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "    TableScan: t1 projection=[t1_id, t1_name, t1_int] [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "    SubqueryAlias: __correlated_sq_1 [t1_int:UInt32;N]",
        "      Projection: t1.t1_int AS t1_int [t1_int:UInt32;N]",
        "        Filter: t1.t1_id > t1.t1_int [t1_id:UInt32;N, t1_int:UInt32;N]",
        "          TableScan: t1 projection=[t1_id, t1_int] [t1_id:UInt32;N, t1_int:UInt32;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn invalid_scalar_subquery() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name, t1_int, (select t2_id, t2_name FROM t2 WHERE t2.t2_id = t1.t1_int) FROM t1";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let err = dataframe.into_optimized_plan().err().unwrap();
    assert_eq!(
        r#"Context("check_analyzed_plan", Plan("Scalar subquery should only return one column"))"#,
        &format!("{err:?}")
    );

    Ok(())
}

#[tokio::test]
async fn subquery_not_allowed() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    // In/Exist Subquery is not allowed in ORDER BY clause.
    let sql = "SELECT t1_id, t1_name, t1_int FROM t1 order by t1_int in (SELECT t2_int FROM t2 WHERE t1.t1_id > t1.t1_int)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let err = dataframe.into_optimized_plan().err().unwrap();

    assert_eq!(
        r#"Context("check_analyzed_plan", Plan("In/Exist subquery can not be used in Sort plan nodes"))"#,
        &format!("{err:?}")
    );

    Ok(())
}

#[tokio::test]
async fn support_agg_correlated_columns() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT sum(t1.t1_int + t2.t2_id) FROM t2 WHERE t1.t1_name = t2.t2_name)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [SUM(outer_ref(t1.t1_int) + t2.t2_id):UInt64;N]",
        "    Projection: SUM(outer_ref(t1.t1_int) + t2.t2_id) [SUM(outer_ref(t1.t1_int) + t2.t2_id):UInt64;N]",
        "      Aggregate: groupBy=[[]], aggr=[[SUM(outer_ref(t1.t1_int) + t2.t2_id)]] [SUM(outer_ref(t1.t1_int) + t2.t2_id):UInt64;N]",
        "        Filter: outer_ref(t1.t1_name) = t2.t2_name [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_agg_correlated_columns2() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT count(*) FROM t2 WHERE t1.t1_name = t2.t2_name having sum(t1_int + t2_id) >0)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [COUNT(UInt8(1)):Int64;N]",
        "    Projection: COUNT(UInt8(1)) [COUNT(UInt8(1)):Int64;N]",
        "      Filter: CAST(SUM(outer_ref(t1.t1_int) + t2.t2_id) AS Int64) > Int64(0) [COUNT(UInt8(1)):Int64;N, SUM(outer_ref(t1.t1_int) + t2.t2_id):UInt64;N]",
        "        Aggregate: groupBy=[[]], aggr=[[COUNT(UInt8(1)), SUM(outer_ref(t1.t1_int) + t2.t2_id)]] [COUNT(UInt8(1)):Int64;N, SUM(outer_ref(t1.t1_int) + t2.t2_id):UInt64;N]",
        "          Filter: outer_ref(t1.t1_name) = t2.t2_name [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "            TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_join_correlated_columns() -> Result<()> {
    let ctx = create_sub_query_join_context("t0_id", "t1_id", "t2_id", true)?;
    let sql = "SELECT t0_id, t0_name FROM t0 WHERE EXISTS (SELECT 1 FROM t1 INNER JOIN t2 ON(t1.t1_id = t2.t2_id and t1.t1_name = t0.t0_name))";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t0_id:UInt32;N, t0_name:Utf8;N]",
        "  Subquery: [Int64(1):Int64]",
        "    Projection: Int64(1) [Int64(1):Int64]",
        "      Inner Join:  Filter: t1.t1_id = t2.t2_id AND t1.t1_name = outer_ref(t0.t0_name) [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t1 [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t0 projection=[t0_id, t0_name] [t0_id:UInt32;N, t0_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_join_correlated_columns2() -> Result<()> {
    let ctx = create_sub_query_join_context("t0_id", "t1_id", "t2_id", true)?;
    let sql = "SELECT t0_id, t0_name FROM t0 WHERE EXISTS (SELECT 1 FROM t1 INNER JOIN (select * from t2 where t2.t2_name = t0.t0_name) as t2 ON(t1.t1_id = t2.t2_id ))";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t0_id:UInt32;N, t0_name:Utf8;N]",
        "  Subquery: [Int64(1):Int64]",
        "    Projection: Int64(1) [Int64(1):Int64]",
        "      Inner Join:  Filter: t1.t1_id = t2.t2_id [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N, t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        TableScan: t1 [t1_id:UInt32;N, t1_name:Utf8;N, t1_int:UInt32;N]",
        "        SubqueryAlias: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          Projection: t2.t2_id, t2.t2_name, t2.t2_int [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "            Filter: t2.t2_name = outer_ref(t0.t0_name) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "              TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t0 projection=[t0_id, t0_name] [t0_id:UInt32;N, t0_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_order_by_correlated_columns() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2_id >= t1_id order by t1_id)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Sort: outer_ref(t1.t1_id) ASC NULLS LAST [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Projection: t2.t2_id, t2.t2_name, t2.t2_int [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        Filter: t2.t2_id >= outer_ref(t1.t1_id) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_limit_subquery() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t2_id = t1_id limit 1)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Limit: skip=0, fetch=1 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Projection: t2.t2_id, t2.t2_name, t2.t2_int [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        Filter: t2.t2_id = outer_ref(t1.t1_id) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE t1_id in (SELECT t2_id FROM t2 where t1_name = t2_name limit 10)";
    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: t1.t1_id IN (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [t2_id:UInt32;N]",
        "    Limit: skip=0, fetch=10 [t2_id:UInt32;N]",
        "      Projection: t2.t2_id [t2_id:UInt32;N]",
        "        Filter: outer_ref(t1.t1_name) = t2.t2_name [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}

#[tokio::test]
async fn support_union_subquery() -> Result<()> {
    let ctx = create_join_context("t1_id", "t2_id", true)?;

    let sql = "SELECT t1_id, t1_name FROM t1 WHERE EXISTS \
                (SELECT * FROM t2 WHERE t2_id = t1_id UNION ALL \
                SELECT * FROM t2 WHERE upper(t2_name) = upper(t1.t1_name))";

    let msg = format!("Creating logical plan for '{sql}'");
    let dataframe = ctx.sql(sql).await.expect(&msg);
    let plan = dataframe.into_optimized_plan()?;

    let expected = vec![
        "Filter: EXISTS (<subquery>) [t1_id:UInt32;N, t1_name:Utf8;N]",
        "  Subquery: [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "    Union [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Projection: t2.t2_id, t2.t2_name, t2.t2_int [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        Filter: t2.t2_id = outer_ref(t1.t1_id) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "      Projection: t2.t2_id, t2.t2_name, t2.t2_int [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "        Filter: upper(t2.t2_name) = upper(outer_ref(t1.t1_name)) [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "          TableScan: t2 [t2_id:UInt32;N, t2_name:Utf8;N, t2_int:UInt32;N]",
        "  TableScan: t1 projection=[t1_id, t1_name] [t1_id:UInt32;N, t1_name:Utf8;N]",
    ];
    let formatted = plan.display_indent_schema().to_string();
    let actual: Vec<&str> = formatted.trim().lines().collect();
    assert_eq!(
        expected, actual,
        "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
    );

    Ok(())
}
