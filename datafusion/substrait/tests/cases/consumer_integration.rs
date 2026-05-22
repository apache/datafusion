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

//! TPCH `substrait_consumer` tests
//!
//! This module tests that substrait plans as json encoded protobuf can be
//! correctly read as DataFusion plans.
//!
//! The input data comes from  <https://github.com/substrait-io/consumer-testing/tree/main/substrait_consumer/tests/integration/queries/tpch_substrait_plans>

#[cfg(test)]
mod tests {
    use crate::utils::test::add_plan_schemas_to_ctx;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    async fn execute_plan(name: &str) -> Result<Vec<RecordBatch>> {
        let path = format!("tests/testdata/test_plans/{name}");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");
        let ctx = SessionContext::new();
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.execute_logical_plan(plan).await?.collect().await
    }

    /// Pretty-print batches as a table with header on top and data rows sorted.
    fn pretty_sorted(batches: &[RecordBatch]) -> String {
        let pretty = pretty_format_batches(batches).unwrap().to_string();
        let all_lines: Vec<&str> = pretty.trim().lines().collect();
        let header = &all_lines[..3];
        let mut data: Vec<&str> = all_lines[3..all_lines.len() - 1].to_vec();
        data.sort();
        let footer = &all_lines[all_lines.len() - 1..];
        header
            .iter()
            .copied()
            .chain(data)
            .chain(footer.iter().copied())
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn tpch_plan_to_string(query_id: i32) -> Result<String> {
        let path =
            format!("tests/testdata/tpch_substrait_plans/query_{query_id:02}_plan.json");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.state().create_physical_plan(&plan).await?;
        Ok(format!("{plan}"))
    }

    #[tokio::test]
    async fn tpch_test_01() -> Result<()> {
        let plan_str = tpch_plan_to_string(1).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: lineitem.L_RETURNFLAG, lineitem.L_LINESTATUS, sum(lineitem.L_QUANTITY) AS SUM_QTY, sum(lineitem.L_EXTENDEDPRICE) AS SUM_BASE_PRICE, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS SUM_DISC_PRICE, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT * Int32(1) + lineitem.L_TAX) AS SUM_CHARGE, avg(lineitem.L_QUANTITY) AS AVG_QTY, avg(lineitem.L_EXTENDEDPRICE) AS AVG_PRICE, avg(lineitem.L_DISCOUNT) AS AVG_DISC, count(Int64(1)) AS COUNT_ORDER
          Sort: lineitem.L_RETURNFLAG ASC NULLS LAST, lineitem.L_LINESTATUS ASC NULLS LAST
            Aggregate: groupBy=[[lineitem.L_RETURNFLAG, lineitem.L_LINESTATUS]], aggr=[[sum(lineitem.L_QUANTITY), sum(lineitem.L_EXTENDEDPRICE), sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT), sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT * Int32(1) + lineitem.L_TAX), avg(lineitem.L_QUANTITY), avg(lineitem.L_EXTENDEDPRICE), avg(lineitem.L_DISCOUNT), count(Int64(1))]]
              Projection: lineitem.L_RETURNFLAG, lineitem.L_LINESTATUS, lineitem.L_QUANTITY, lineitem.L_EXTENDEDPRICE, lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT), lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT) * (CAST(Int32(1) AS Decimal128(15, 2)) + lineitem.L_TAX), lineitem.L_DISCOUNT
                Filter: lineitem.L_SHIPDATE <= Date32("1998-12-01") - IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 10368000 }")
                  TableScan: lineitem
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_02() -> Result<()> {
        let plan_str = tpch_plan_to_string(2).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Limit: skip=0, fetch=100
          Sort: supplier.S_ACCTBAL DESC NULLS FIRST, nation.N_NAME ASC NULLS LAST, supplier.S_NAME ASC NULLS LAST, part.P_PARTKEY ASC NULLS LAST
            Projection: supplier.S_ACCTBAL, supplier.S_NAME, nation.N_NAME, part.P_PARTKEY, part.P_MFGR, supplier.S_ADDRESS, supplier.S_PHONE, supplier.S_COMMENT
              Filter: part.P_PARTKEY = partsupp.PS_PARTKEY AND supplier.S_SUPPKEY = partsupp.PS_SUPPKEY AND part.P_SIZE = Int32(15) AND part.P_TYPE LIKE CAST(Utf8("%BRASS") AS Utf8) AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_REGIONKEY = region.R_REGIONKEY AND region.R_NAME = Utf8("EUROPE") AND partsupp.PS_SUPPLYCOST = (<subquery>)
                Subquery:
                  Aggregate: groupBy=[[]], aggr=[[min(partsupp.PS_SUPPLYCOST)]]
                    Projection: partsupp.PS_SUPPLYCOST
                      Filter: outer_ref(part.P_PARTKEY) = partsupp.PS_PARTKEY AND supplier.S_SUPPKEY = partsupp.PS_SUPPKEY AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_REGIONKEY = region.R_REGIONKEY AND region.R_NAME = Utf8("EUROPE")
                        Cross Join:
                          Cross Join:
                            Cross Join:
                              TableScan: partsupp
                              TableScan: supplier
                            TableScan: nation
                          TableScan: region
                Cross Join:
                  Cross Join:
                    Cross Join:
                      Cross Join:
                        TableScan: part
                        TableScan: supplier
                      TableScan: partsupp
                    TableScan: nation
                  TableScan: region
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_03() -> Result<()> {
        let plan_str = tpch_plan_to_string(3).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: lineitem.L_ORDERKEY, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS REVENUE, orders.O_ORDERDATE, orders.O_SHIPPRIORITY
          Limit: skip=0, fetch=10
            Sort: sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) DESC NULLS FIRST, orders.O_ORDERDATE ASC NULLS LAST
              Projection: lineitem.L_ORDERKEY, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT), orders.O_ORDERDATE, orders.O_SHIPPRIORITY
                Aggregate: groupBy=[[lineitem.L_ORDERKEY, orders.O_ORDERDATE, orders.O_SHIPPRIORITY]], aggr=[[sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT)]]
                  Projection: lineitem.L_ORDERKEY, orders.O_ORDERDATE, orders.O_SHIPPRIORITY, lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT)
                    Filter: customer.C_MKTSEGMENT = Utf8("BUILDING") AND customer.C_CUSTKEY = orders.O_CUSTKEY AND lineitem.L_ORDERKEY = orders.O_ORDERKEY AND orders.O_ORDERDATE < CAST(Utf8("1995-03-15") AS Date32) AND lineitem.L_SHIPDATE > CAST(Utf8("1995-03-15") AS Date32)
                      Cross Join:
                        Cross Join:
                          TableScan: lineitem
                          TableScan: customer
                        TableScan: orders
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_04() -> Result<()> {
        let plan_str = tpch_plan_to_string(4).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: orders.O_ORDERPRIORITY, count(Int64(1)) AS ORDER_COUNT
          Sort: orders.O_ORDERPRIORITY ASC NULLS LAST
            Aggregate: groupBy=[[orders.O_ORDERPRIORITY]], aggr=[[count(Int64(1))]]
              Projection: orders.O_ORDERPRIORITY
                Filter: orders.O_ORDERDATE >= CAST(Utf8("1993-07-01") AS Date32) AND orders.O_ORDERDATE < CAST(Utf8("1993-10-01") AS Date32) AND EXISTS (<subquery>)
                  Subquery:
                    Filter: lineitem.L_ORDERKEY = outer_ref(orders.O_ORDERKEY) AND lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE
                      TableScan: lineitem
                  TableScan: orders
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_05() -> Result<()> {
        let plan_str = tpch_plan_to_string(5).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: nation.N_NAME, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS REVENUE
          Sort: sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) DESC NULLS FIRST
            Aggregate: groupBy=[[nation.N_NAME]], aggr=[[sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT)]]
              Projection: nation.N_NAME, lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT)
                Filter: customer.C_CUSTKEY = orders.O_CUSTKEY AND lineitem.L_ORDERKEY = orders.O_ORDERKEY AND lineitem.L_SUPPKEY = supplier.S_SUPPKEY AND customer.C_NATIONKEY = supplier.S_NATIONKEY AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_REGIONKEY = region.R_REGIONKEY AND region.R_NAME = Utf8("ASIA") AND orders.O_ORDERDATE >= CAST(Utf8("1994-01-01") AS Date32) AND orders.O_ORDERDATE < CAST(Utf8("1995-01-01") AS Date32)
                  Cross Join:
                    Cross Join:
                      Cross Join:
                        Cross Join:
                          Cross Join:
                            TableScan: customer
                            TableScan: orders
                          TableScan: lineitem
                        TableScan: supplier
                      TableScan: nation
                    TableScan: region
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_06() -> Result<()> {
        let plan_str = tpch_plan_to_string(6).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Aggregate: groupBy=[[]], aggr=[[sum(lineitem.L_EXTENDEDPRICE * lineitem.L_DISCOUNT) AS REVENUE]]
          Projection: lineitem.L_EXTENDEDPRICE * lineitem.L_DISCOUNT
            Filter: lineitem.L_SHIPDATE >= CAST(Utf8("1994-01-01") AS Date32) AND lineitem.L_SHIPDATE < CAST(Utf8("1995-01-01") AS Date32) AND lineitem.L_DISCOUNT >= Decimal128(Some(5),3,2) AND lineitem.L_DISCOUNT <= Decimal128(Some(7),3,2) AND lineitem.L_QUANTITY < CAST(Int32(24) AS Decimal128(15, 2))
              TableScan: lineitem
        "#
                );
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_07() -> Result<()> {
        let plan_str = tpch_plan_to_string(7).await?;
        assert_snapshot!(plan_str, "Missing support for enum function arguments");
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_08() -> Result<()> {
        let plan_str = tpch_plan_to_string(8).await?;
        assert_snapshot!(plan_str, "Missing support for enum function arguments");
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_09() -> Result<()> {
        let plan_str = tpch_plan_to_string(9).await?;
        assert_snapshot!(plan_str, "Missing support for enum function arguments");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_10() -> Result<()> {
        let plan_str = tpch_plan_to_string(10).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: customer.C_CUSTKEY, customer.C_NAME, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS REVENUE, customer.C_ACCTBAL, nation.N_NAME, customer.C_ADDRESS, customer.C_PHONE, customer.C_COMMENT
          Limit: skip=0, fetch=20
            Sort: sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) DESC NULLS FIRST
              Projection: customer.C_CUSTKEY, customer.C_NAME, sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT), customer.C_ACCTBAL, nation.N_NAME, customer.C_ADDRESS, customer.C_PHONE, customer.C_COMMENT
                Aggregate: groupBy=[[customer.C_CUSTKEY, customer.C_NAME, customer.C_ACCTBAL, customer.C_PHONE, nation.N_NAME, customer.C_ADDRESS, customer.C_COMMENT]], aggr=[[sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT)]]
                  Projection: customer.C_CUSTKEY, customer.C_NAME, customer.C_ACCTBAL, customer.C_PHONE, nation.N_NAME, customer.C_ADDRESS, customer.C_COMMENT, lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT)
                    Filter: customer.C_CUSTKEY = orders.O_CUSTKEY AND lineitem.L_ORDERKEY = orders.O_ORDERKEY AND orders.O_ORDERDATE >= CAST(Utf8("1993-10-01") AS Date32) AND orders.O_ORDERDATE < CAST(Utf8("1994-01-01") AS Date32) AND lineitem.L_RETURNFLAG = Utf8("R") AND customer.C_NATIONKEY = nation.N_NATIONKEY
                      Cross Join:
                        Cross Join:
                          Cross Join:
                            TableScan: customer
                            TableScan: orders
                          TableScan: lineitem
                        TableScan: nation
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_11() -> Result<()> {
        let plan_str = tpch_plan_to_string(11).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: partsupp.PS_PARTKEY, sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY) AS value
          Sort: sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY) DESC NULLS FIRST
            Filter: sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY) > (<subquery>)
              Subquery:
                Projection: sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY) * Decimal128(Some(1000000),11,10)
                  Aggregate: groupBy=[[]], aggr=[[sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY)]]
                    Projection: partsupp.PS_SUPPLYCOST * CAST(partsupp.PS_AVAILQTY AS Decimal128(19, 0))
                      Filter: partsupp.PS_SUPPKEY = supplier.S_SUPPKEY AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_NAME = Utf8("JAPAN")
                        Cross Join:
                          Cross Join:
                            TableScan: partsupp
                            TableScan: supplier
                          TableScan: nation
              Aggregate: groupBy=[[partsupp.PS_PARTKEY]], aggr=[[sum(partsupp.PS_SUPPLYCOST * partsupp.PS_AVAILQTY)]]
                Projection: partsupp.PS_PARTKEY, partsupp.PS_SUPPLYCOST * CAST(partsupp.PS_AVAILQTY AS Decimal128(19, 0))
                  Filter: partsupp.PS_SUPPKEY = supplier.S_SUPPKEY AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_NAME = Utf8("JAPAN")
                    Cross Join:
                      Cross Join:
                        TableScan: partsupp
                        TableScan: supplier
                      TableScan: nation
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_12() -> Result<()> {
        let plan_str = tpch_plan_to_string(12).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: lineitem.L_SHIPMODE, sum(CASE WHEN orders.O_ORDERPRIORITY = Utf8("1-URGENT") OR orders.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END) AS HIGH_LINE_COUNT, sum(CASE WHEN orders.O_ORDERPRIORITY != Utf8("1-URGENT") AND orders.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END) AS LOW_LINE_COUNT
          Sort: lineitem.L_SHIPMODE ASC NULLS LAST
            Aggregate: groupBy=[[lineitem.L_SHIPMODE]], aggr=[[sum(CASE WHEN orders.O_ORDERPRIORITY = Utf8("1-URGENT") OR orders.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END), sum(CASE WHEN orders.O_ORDERPRIORITY != Utf8("1-URGENT") AND orders.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END)]]
              Projection: lineitem.L_SHIPMODE, CASE WHEN orders.O_ORDERPRIORITY = Utf8("1-URGENT") OR orders.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END, CASE WHEN orders.O_ORDERPRIORITY != Utf8("1-URGENT") AND orders.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END
                Filter: orders.O_ORDERKEY = lineitem.L_ORDERKEY AND (lineitem.L_SHIPMODE = CAST(Utf8("MAIL") AS Utf8) OR lineitem.L_SHIPMODE = CAST(Utf8("SHIP") AS Utf8)) AND lineitem.L_COMMITDATE < lineitem.L_RECEIPTDATE AND lineitem.L_SHIPDATE < lineitem.L_COMMITDATE AND lineitem.L_RECEIPTDATE >= CAST(Utf8("1994-01-01") AS Date32) AND lineitem.L_RECEIPTDATE < CAST(Utf8("1995-01-01") AS Date32)
                  Cross Join:
                    TableScan: orders
                    TableScan: lineitem
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_13() -> Result<()> {
        let plan_str = tpch_plan_to_string(13).await?;
        assert_snapshot!(
            plan_str,
            @r#"
        Projection: count(orders.O_ORDERKEY) AS C_COUNT, count(Int64(1)) AS CUSTDIST
          Sort: count(Int64(1)) DESC NULLS FIRST, count(orders.O_ORDERKEY) DESC NULLS FIRST
            Projection: count(orders.O_ORDERKEY), count(Int64(1))
              Aggregate: groupBy=[[count(orders.O_ORDERKEY)]], aggr=[[count(Int64(1))]]
                Projection: count(orders.O_ORDERKEY)
                  Aggregate: groupBy=[[customer.C_CUSTKEY]], aggr=[[count(orders.O_ORDERKEY)]]
                    Projection: customer.C_CUSTKEY, orders.O_ORDERKEY
                      Left Join: customer.C_CUSTKEY = orders.O_CUSTKEY Filter: NOT orders.O_COMMENT LIKE CAST(Utf8("%special%requests%") AS Utf8)
                        TableScan: customer
                        TableScan: orders
        "#        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_14() -> Result<()> {
        let plan_str = tpch_plan_to_string(14).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: Decimal128(Some(10000),5,2) * sum(CASE WHEN part.P_TYPE LIKE Utf8("PROMO%") THEN lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT ELSE Decimal128(Some(0),19,4) END) / sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS PROMO_REVENUE
          Aggregate: groupBy=[[]], aggr=[[sum(CASE WHEN part.P_TYPE LIKE Utf8("PROMO%") THEN lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT ELSE Decimal128(Some(0),19,4) END), sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT)]]
            Projection: CASE WHEN part.P_TYPE LIKE CAST(Utf8("PROMO%") AS Utf8) THEN lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT) ELSE Decimal128(Some(0),19,4) END, lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT)
              Filter: lineitem.L_PARTKEY = part.P_PARTKEY AND lineitem.L_SHIPDATE >= Date32("1995-09-01") AND lineitem.L_SHIPDATE < CAST(Utf8("1995-10-01") AS Date32)
                Cross Join:
                  TableScan: lineitem
                  TableScan: part
        "#
                );
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_15() -> Result<()> {
        let plan_str = tpch_plan_to_string(15).await?;
        assert_snapshot!(plan_str, "Test file is empty");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_16() -> Result<()> {
        let plan_str = tpch_plan_to_string(16).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: part.P_BRAND, part.P_TYPE, part.P_SIZE, count(DISTINCT partsupp.PS_SUPPKEY) AS SUPPLIER_CNT
          Sort: count(DISTINCT partsupp.PS_SUPPKEY) DESC NULLS FIRST, part.P_BRAND ASC NULLS LAST, part.P_TYPE ASC NULLS LAST, part.P_SIZE ASC NULLS LAST
            Aggregate: groupBy=[[part.P_BRAND, part.P_TYPE, part.P_SIZE]], aggr=[[count(DISTINCT partsupp.PS_SUPPKEY)]]
              Projection: part.P_BRAND, part.P_TYPE, part.P_SIZE, partsupp.PS_SUPPKEY
                Filter: part.P_PARTKEY = partsupp.PS_PARTKEY AND part.P_BRAND != Utf8("Brand#45") AND NOT part.P_TYPE LIKE CAST(Utf8("MEDIUM POLISHED%") AS Utf8) AND (part.P_SIZE = Int32(49) OR part.P_SIZE = Int32(14) OR part.P_SIZE = Int32(23) OR part.P_SIZE = Int32(45) OR part.P_SIZE = Int32(19) OR part.P_SIZE = Int32(3) OR part.P_SIZE = Int32(36) OR part.P_SIZE = Int32(9)) AND NOT partsupp.PS_SUPPKEY IN (<subquery>)
                  Subquery:
                    Projection: supplier.S_SUPPKEY
                      Filter: supplier.S_COMMENT LIKE CAST(Utf8("%Customer%Complaints%") AS Utf8)
                        TableScan: supplier
                  Cross Join:
                    TableScan: partsupp
                    TableScan: part
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_17() -> Result<()> {
        let plan_str = tpch_plan_to_string(17).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: sum(lineitem.L_EXTENDEDPRICE) / Decimal128(Some(70),2,1) AS AVG_YEARLY
          Aggregate: groupBy=[[]], aggr=[[sum(lineitem.L_EXTENDEDPRICE)]]
            Projection: lineitem.L_EXTENDEDPRICE
              Filter: part.P_PARTKEY = lineitem.L_PARTKEY AND part.P_BRAND = Utf8("Brand#23") AND part.P_CONTAINER = Utf8("MED BOX") AND lineitem.L_QUANTITY < (<subquery>)
                Subquery:
                  Projection: Decimal128(Some(2),2,1) * avg(lineitem.L_QUANTITY)
                    Aggregate: groupBy=[[]], aggr=[[avg(lineitem.L_QUANTITY)]]
                      Projection: lineitem.L_QUANTITY
                        Filter: lineitem.L_PARTKEY = outer_ref(part.P_PARTKEY)
                          TableScan: lineitem
                Cross Join:
                  TableScan: lineitem
                  TableScan: part
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_18() -> Result<()> {
        let plan_str = tpch_plan_to_string(18).await?;
        assert_snapshot!(
        plan_str,
        @"
        Projection: customer.C_NAME, customer.C_CUSTKEY, orders.O_ORDERKEY, orders.O_ORDERDATE, orders.O_TOTALPRICE, sum(lineitem.L_QUANTITY) AS EXPR$5
          Limit: skip=0, fetch=100
            Sort: orders.O_TOTALPRICE DESC NULLS FIRST, orders.O_ORDERDATE ASC NULLS LAST
              Aggregate: groupBy=[[customer.C_NAME, customer.C_CUSTKEY, orders.O_ORDERKEY, orders.O_ORDERDATE, orders.O_TOTALPRICE]], aggr=[[sum(lineitem.L_QUANTITY)]]
                Projection: customer.C_NAME, customer.C_CUSTKEY, orders.O_ORDERKEY, orders.O_ORDERDATE, orders.O_TOTALPRICE, lineitem.L_QUANTITY
                  Filter: orders.O_ORDERKEY IN (<subquery>) AND customer.C_CUSTKEY = orders.O_CUSTKEY AND orders.O_ORDERKEY = lineitem.L_ORDERKEY
                    Subquery:
                      Projection: lineitem.L_ORDERKEY
                        Filter: sum(lineitem.L_QUANTITY) > CAST(Int32(300) AS Decimal128(15, 2))
                          Aggregate: groupBy=[[lineitem.L_ORDERKEY]], aggr=[[sum(lineitem.L_QUANTITY)]]
                            Projection: lineitem.L_ORDERKEY, lineitem.L_QUANTITY
                              TableScan: lineitem
                    Cross Join:
                      Cross Join:
                        TableScan: customer
                        TableScan: orders
                      TableScan: lineitem
        "
                );
        Ok(())
    }
    #[tokio::test]
    async fn tpch_test_19() -> Result<()> {
        let plan_str = tpch_plan_to_string(19).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Aggregate: groupBy=[[]], aggr=[[sum(lineitem.L_EXTENDEDPRICE * Int32(1) - lineitem.L_DISCOUNT) AS REVENUE]]
          Projection: lineitem.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - lineitem.L_DISCOUNT)
            Filter: part.P_PARTKEY = lineitem.L_PARTKEY AND part.P_BRAND = Utf8("Brand#12") AND (part.P_CONTAINER = CAST(Utf8("SM CASE") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("SM BOX") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("SM PACK") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("SM PKG") AS Utf8)) AND lineitem.L_QUANTITY >= CAST(Int32(1) AS Decimal128(15, 2)) AND lineitem.L_QUANTITY <= CAST(Int32(1) + Int32(10) AS Decimal128(15, 2)) AND part.P_SIZE >= Int32(1) AND part.P_SIZE <= Int32(5) AND (lineitem.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR lineitem.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND lineitem.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON") OR part.P_PARTKEY = lineitem.L_PARTKEY AND part.P_BRAND = Utf8("Brand#23") AND (part.P_CONTAINER = CAST(Utf8("MED BAG") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("MED BOX") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("MED PKG") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("MED PACK") AS Utf8)) AND lineitem.L_QUANTITY >= CAST(Int32(10) AS Decimal128(15, 2)) AND lineitem.L_QUANTITY <= CAST(Int32(10) + Int32(10) AS Decimal128(15, 2)) AND part.P_SIZE >= Int32(1) AND part.P_SIZE <= Int32(10) AND (lineitem.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR lineitem.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND lineitem.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON") OR part.P_PARTKEY = lineitem.L_PARTKEY AND part.P_BRAND = Utf8("Brand#34") AND (part.P_CONTAINER = CAST(Utf8("LG CASE") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("LG BOX") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("LG PACK") AS Utf8) OR part.P_CONTAINER = CAST(Utf8("LG PKG") AS Utf8)) AND lineitem.L_QUANTITY >= CAST(Int32(20) AS Decimal128(15, 2)) AND lineitem.L_QUANTITY <= CAST(Int32(20) + Int32(10) AS Decimal128(15, 2)) AND part.P_SIZE >= Int32(1) AND part.P_SIZE <= Int32(15) AND (lineitem.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR lineitem.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND lineitem.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON")
              Cross Join:
                TableScan: lineitem
                TableScan: part
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_20() -> Result<()> {
        let plan_str = tpch_plan_to_string(20).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Sort: supplier.S_NAME ASC NULLS LAST
          Projection: supplier.S_NAME, supplier.S_ADDRESS
            Filter: supplier.S_SUPPKEY IN (<subquery>) AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_NAME = Utf8("CANADA")
              Subquery:
                Projection: partsupp.PS_SUPPKEY
                  Filter: partsupp.PS_PARTKEY IN (<subquery>) AND CAST(partsupp.PS_AVAILQTY AS Decimal128(19, 0)) > (<subquery>)
                    Subquery:
                      Projection: part.P_PARTKEY
                        Filter: part.P_NAME LIKE CAST(Utf8("forest%") AS Utf8)
                          TableScan: part
                    Subquery:
                      Projection: Decimal128(Some(5),2,1) * sum(lineitem.L_QUANTITY)
                        Aggregate: groupBy=[[]], aggr=[[sum(lineitem.L_QUANTITY)]]
                          Projection: lineitem.L_QUANTITY
                            Filter: lineitem.L_PARTKEY = outer_ref(partsupp.PS_PARTKEY) AND lineitem.L_SUPPKEY = outer_ref(partsupp.PS_SUPPKEY) AND lineitem.L_SHIPDATE >= CAST(Utf8("1994-01-01") AS Date32) AND lineitem.L_SHIPDATE < CAST(Utf8("1995-01-01") AS Date32)
                              TableScan: lineitem
                    TableScan: partsupp
              Cross Join:
                TableScan: supplier
                TableScan: nation
        "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_21() -> Result<()> {
        let plan_str = tpch_plan_to_string(21).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: supplier.S_NAME, count(Int64(1)) AS NUMWAIT
          Limit: skip=0, fetch=100
            Sort: count(Int64(1)) DESC NULLS FIRST, supplier.S_NAME ASC NULLS LAST
              Aggregate: groupBy=[[supplier.S_NAME]], aggr=[[count(Int64(1))]]
                Projection: supplier.S_NAME
                  Filter: supplier.S_SUPPKEY = lineitem.L_SUPPKEY AND orders.O_ORDERKEY = lineitem.L_ORDERKEY AND orders.O_ORDERSTATUS = Utf8("F") AND lineitem.L_RECEIPTDATE > lineitem.L_COMMITDATE AND EXISTS (<subquery>) AND NOT EXISTS (<subquery>) AND supplier.S_NATIONKEY = nation.N_NATIONKEY AND nation.N_NAME = Utf8("SAUDI ARABIA")
                    Subquery:
                      Filter: lineitem.L_ORDERKEY = outer_ref(lineitem.L_ORDERKEY) AND lineitem.L_SUPPKEY != outer_ref(lineitem.L_SUPPKEY)
                        TableScan: lineitem
                    Subquery:
                      Filter: lineitem.L_ORDERKEY = outer_ref(lineitem.L_ORDERKEY) AND lineitem.L_SUPPKEY != outer_ref(lineitem.L_SUPPKEY) AND lineitem.L_RECEIPTDATE > lineitem.L_COMMITDATE
                        TableScan: lineitem
                    Cross Join:
                      Cross Join:
                        Cross Join:
                          TableScan: supplier
                          TableScan: lineitem
                        TableScan: orders
                      TableScan: nation
        "#
                        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_22() -> Result<()> {
        let plan_str = tpch_plan_to_string(22).await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: substr(customer.C_PHONE,Int32(1),Int32(2)) AS CNTRYCODE, count(Int64(1)) AS NUMCUST, sum(customer.C_ACCTBAL) AS TOTACCTBAL
          Sort: substr(customer.C_PHONE,Int32(1),Int32(2)) ASC NULLS LAST
            Aggregate: groupBy=[[substr(customer.C_PHONE,Int32(1),Int32(2))]], aggr=[[count(Int64(1)), sum(customer.C_ACCTBAL)]]
              Projection: substr(customer.C_PHONE, Int32(1), Int32(2)), customer.C_ACCTBAL
                Filter: (substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("13") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("31") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("23") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("29") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("30") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("18") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("17") AS Utf8)) AND customer.C_ACCTBAL > (<subquery>) AND NOT EXISTS (<subquery>)
                  Subquery:
                    Aggregate: groupBy=[[]], aggr=[[avg(customer.C_ACCTBAL)]]
                      Projection: customer.C_ACCTBAL
                        Filter: customer.C_ACCTBAL > Decimal128(Some(0),3,2) AND (substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("13") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("31") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("23") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("29") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("30") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("18") AS Utf8) OR substr(customer.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("17") AS Utf8))
                          TableScan: customer
                  Subquery:
                    Filter: orders.O_CUSTKEY = outer_ref(customer.C_CUSTKEY)
                      TableScan: orders
                  TableScan: customer
        "#
                        );
        Ok(())
    }

    /// Tests nested correlated subqueries where the innermost subquery
    /// references the outermost query (steps_out=2).
    ///
    /// This tests the outer schema stack with depth > 1.
    /// The plan represents:
    /// ```sql
    /// SELECT * FROM A
    /// WHERE EXISTS (
    ///     SELECT * FROM B
    ///     WHERE B.b1 = A.a1              -- steps_out=1 (references immediate parent)
    ///       AND EXISTS (
    ///         SELECT * FROM C
    ///         WHERE C.c1 = A.a1          -- steps_out=2 (references grandparent)
    ///           AND C.c2 = B.b2          -- steps_out=1 (references immediate parent)
    ///     )
    /// )
    /// ```
    ///
    #[tokio::test]
    async fn test_nested_correlated_subquery() -> Result<()> {
        let path = "tests/testdata/test_plans/nested_correlated_subquery.substrait.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        let plan_str = format!("{plan}");

        assert_snapshot!(
            plan_str,
            @"
        Filter: EXISTS (<subquery>)
          Subquery:
            Filter: b.b1 = outer_ref(a.a1) AND EXISTS (<subquery>)
              Subquery:
                Filter: c.c1 = outer_ref(a.a1) AND c.c2 = outer_ref(b.b2)
                  TableScan: c
              TableScan: b
          TableScan: a
        "
        );
        Ok(())
    }

    async fn test_plan_to_string(name: &str) -> Result<String> {
        let path = format!("tests/testdata/test_plans/{name}");
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &proto)?;
        let plan = from_substrait_plan(&ctx.state(), &proto).await?;
        ctx.state().create_physical_plan(&plan).await?;
        Ok(format!("{plan}"))
    }

    #[tokio::test]
    async fn test_select_count_from_select_1() -> Result<()> {
        let plan_str =
            test_plan_to_string("select_count_from_select_1.substrait.json").await?;

        assert_snapshot!(
        plan_str,
        @"
        Aggregate: groupBy=[[]], aggr=[[count(Int64(1)) AS count(*)]]
          Values: (Int64(0))
        "
                );
        Ok(())
    }

    #[tokio::test]
    async fn test_expressions_in_virtual_table() -> Result<()> {
        let plan_str =
            test_plan_to_string("virtual_table_with_expressions.substrait.json").await?;

        assert_snapshot!(
        plan_str,
        @r#"
        Projection: dummy1 AS result1, dummy2 AS result2
          Values: (Int64(0), Utf8("temp")), (Int64(1), Utf8("test"))
        "#
                );
        Ok(())
    }

    #[tokio::test]
    //There are some Substrait functions that can be represented with nested built-in expressions
    //xor:bool_bool is implemented in the consumer with binary expressions
    //This tests that the consumer correctly builds the nested expressions for this function
    async fn test_built_in_binary_exprs_for_xor() -> Result<()> {
        let plan_str =
            test_plan_to_string("scalar_fn_to_built_in_binary_expr_xor.substrait.json")
                .await?;

        //Test correct plan structure
        assert_snapshot!(plan_str,
          @"
        Projection: a, b, (a OR b) AND NOT a AND b AS result
          Values: (Boolean(true), Boolean(true)), (Boolean(true), Boolean(false)), (Boolean(false), Boolean(true)), (Boolean(false), Boolean(false))
        "
        );

        Ok(())
    }

    #[tokio::test]
    //There are some Substrait functions that can be represented with nested built-in expressions
    //and_not:bool_bool is implemented in the consumer as binary expressions
    //This tests that the consumer correctly builds the nested expressions for this function
    async fn test_built_in_binary_exprs_for_and_not() -> Result<()> {
        let plan_str = test_plan_to_string(
            "scalar_fn_to_built_in_binary_expr_and_not.substrait.json",
        )
        .await?;

        //Test correct plan structure
        assert_snapshot!(plan_str,
          @"
        Projection: a, b, a AND NOT b AS result
          Values: (Boolean(true), Boolean(true)), (Boolean(true), Boolean(false)), (Boolean(false), Boolean(true)), (Boolean(false), Boolean(false))
        "
        );

        Ok(())
    }

    //The between:any_any_any function is implemented as Expr::Between in the Substrait consumer
    //This test tests that the consumer correctly builds the Expr::Between expression for this function
    #[tokio::test]
    async fn test_between_expr() -> Result<()> {
        let plan_str =
            test_plan_to_string("scalar_fn_to_between_expr.substrait.json").await?;
        assert_snapshot!(plan_str,
          @"
        Projection: expr BETWEEN low AND high AS result
          Values: (Int8(2), Int8(1), Int8(3)), (Int8(4), Int8(1), Int8(2))
        "
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_logb_expr() -> Result<()> {
        let plan_str = test_plan_to_string("scalar_fn_logb_expr.substrait.json").await?;
        assert_snapshot!(plan_str,
          @"
        Projection: x, base, log(base, x) AS result
          Values: (Float32(1), Float32(10)), (Float32(100), Float32(10))
        "
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_joins() -> Result<()> {
        let plan_str = test_plan_to_string("multiple_joins.json").await?;
        assert_snapshot!(
            plan_str,
            @r#"
        Projection: left.count(Int64(1)) AS count_first, left.category, left.count(Int64(1)):1 AS count_second, right.count(Int64(1)) AS count_third
          Left Join: left.id = right.id
            SubqueryAlias: left
              Projection: left.id, left.count(Int64(1)), left.id:1, left.category, right.id AS id:2, right.count(Int64(1)) AS count(Int64(1)):1
                Left Join: left.id = right.id
                  SubqueryAlias: left
                    Projection: left.id, left.count(Int64(1)), right.id AS id:1, right.category
                      Left Join: left.id = right.id
                        SubqueryAlias: left
                          Aggregate: groupBy=[[id]], aggr=[[count(Int64(1))]]
                            Values: (Int64(1)), (Int64(2))
                        SubqueryAlias: right
                          Aggregate: groupBy=[[id, category]], aggr=[[]]
                            Values: (Int64(1), Utf8("info")), (Int64(2), Utf8("low"))
                  SubqueryAlias: right
                    Aggregate: groupBy=[[id]], aggr=[[count(Int64(1))]]
                      Values: (Int64(1)), (Int64(2))
            SubqueryAlias: right
              Aggregate: groupBy=[[id]], aggr=[[count(Int64(1))]]
                Values: (Int64(1)), (Int64(2))
        "#
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_select_window_count() -> Result<()> {
        let plan_str = test_plan_to_string("select_window_count.substrait.json").await?;

        assert_snapshot!(
        plan_str,
        @"
        Projection: count(Int64(1)) PARTITION BY [data.PART] ORDER BY [data.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING AS LEAD_EXPR
          WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [data.PART] ORDER BY [data.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING]]
            TableScan: data
        "
                        );
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_unions() -> Result<()> {
        let plan_str = test_plan_to_string("multiple_unions.json").await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: Utf8("people") AS product_category, Utf8("people")__temp__0 AS product_type, product_key
          Union
            Projection: Utf8("people"), Utf8("people") AS Utf8("people")__temp__0, sales.product_key
              Left Join: sales.product_key = food.@food_id
                TableScan: sales
                TableScan: food
            Union
              Projection: people.$f3, people.$f5, people.product_key0
                Left Join: people.product_key0 = food.@food_id
                  TableScan: people
                  TableScan: food
              TableScan: more_products
        "#
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_join_with_expression_key() -> Result<()> {
        let plan_str = test_plan_to_string("join_with_expression_key.json").await?;
        assert_snapshot!(
        plan_str,
        @r#"
        Projection: left.index_name AS index, right.upper(host) AS host, left.max(size_bytes) AS idx_size, right.max(total_bytes) AS db_size, CAST(left.max(size_bytes) AS Float64) / CAST(right.max(total_bytes) AS Float64) * Float64(100) AS pct_of_db
          Inner Join: left.upper(host) = right.upper(host)
            SubqueryAlias: left
              Aggregate: groupBy=[[index_name, upper(host)]], aggr=[[max(size_bytes)]]
                Projection: size_bytes, index_name, upper(host)
                  Filter: index_name = Utf8("aaa")
                    Values: (Utf8("aaa"), Utf8("host-a"), Int64(128)), (Utf8("bbb"), Utf8("host-b"), Int64(256))
            SubqueryAlias: right
              Aggregate: groupBy=[[upper(host)]], aggr=[[max(total_bytes)]]
                Projection: total_bytes, upper(host)
                  Inner Join:  Filter: upper(host) = upper(host)
                    Values: (Utf8("host-a"), Int64(107)), (Utf8("host-b"), Int64(214))
                    Projection: upper(host)
                      Aggregate: groupBy=[[index_name, upper(host)]], aggr=[[max(size_bytes)]]
                        Projection: size_bytes, index_name, upper(host)
                          Filter: index_name = Utf8("aaa")
                            Values: (Utf8("aaa"), Utf8("host-a"), Int64(128)), (Utf8("bbb"), Utf8("host-b"), Int64(256))
        "#
        );

        Ok(())
    }

    /// Substrait join with both `equal` and `is_not_distinct_from` must demote
    /// `IS NOT DISTINCT FROM` to the join filter.
    #[tokio::test]
    async fn test_mixed_join_equal_and_indistinct_inner_join() -> Result<()> {
        let plan_str =
            test_plan_to_string("mixed_join_equal_and_indistinct.json").await?;
        // Eq becomes the equijoin key; IS NOT DISTINCT FROM is demoted to filter.
        assert_snapshot!(
            plan_str,
            @r#"
        Projection: left.id, left.val, left.comment, right.id AS id0, right.val AS val0, right.comment AS comment0
          Inner Join: left.id = right.id Filter: left.val IS NOT DISTINCT FROM right.val
            SubqueryAlias: left
              Values: (Utf8("1"), Utf8("a"), Utf8("c1")), (Utf8("2"), Utf8("b"), Utf8("c2")), (Utf8("3"), Utf8(NULL), Utf8("c3")), (Utf8("4"), Utf8(NULL), Utf8("c4")), (Utf8("5"), Utf8("e"), Utf8("c5"))...
            SubqueryAlias: right
              Values: (Utf8("1"), Utf8("a"), Utf8("c1")), (Utf8("2"), Utf8("b"), Utf8("c2")), (Utf8("3"), Utf8(NULL), Utf8("c3")), (Utf8("4"), Utf8(NULL), Utf8("c4")), (Utf8("5"), Utf8("e"), Utf8("c5"))...
        "#
        );

        // Execute and verify actual rows, including NULL=NULL matches (ids 3,4).
        let results = execute_plan("mixed_join_equal_and_indistinct.json").await?;
        assert_snapshot!(pretty_sorted(&results),
            @"
        +----+-----+---------+-----+------+----------+
        | id | val | comment | id0 | val0 | comment0 |
        +----+-----+---------+-----+------+----------+
        | 1  | a   | c1      | 1   | a    | c1       |
        | 2  | b   | c2      | 2   | b    | c2       |
        | 3  |     | c3      | 3   |      | c3       |
        | 4  |     | c4      | 4   |      | c4       |
        | 5  | e   | c5      | 5   | e    | c5       |
        | 6  | f   | c6      | 6   | f    | c6       |
        +----+-----+---------+-----+------+----------+
        "
        );

        Ok(())
    }

    /// Substrait join with both `equal` and `is_not_distinct_from` must demote
    /// `IS NOT DISTINCT FROM` to the join filter.
    #[tokio::test]
    async fn test_mixed_join_equal_and_indistinct_left_join() -> Result<()> {
        let plan_str =
            test_plan_to_string("mixed_join_equal_and_indistinct_left.json").await?;
        assert_snapshot!(
            plan_str,
            @r#"
        Projection: left.id, left.val, left.comment, right.id AS id0, right.val AS val0, right.comment AS comment0
          Left Join: left.id = right.id Filter: left.val IS NOT DISTINCT FROM right.val
            SubqueryAlias: left
              Values: (Utf8("1"), Utf8("a"), Utf8("c1")), (Utf8("2"), Utf8("b"), Utf8("c2")), (Utf8("3"), Utf8(NULL), Utf8("c3")), (Utf8("4"), Utf8(NULL), Utf8("c4")), (Utf8("5"), Utf8("e"), Utf8("c5"))...
            SubqueryAlias: right
              Values: (Utf8("1"), Utf8("a"), Utf8("c1")), (Utf8("2"), Utf8("b"), Utf8("c2")), (Utf8("3"), Utf8(NULL), Utf8("c3")), (Utf8("4"), Utf8(NULL), Utf8("c4")), (Utf8("5"), Utf8("e"), Utf8("c5"))...
        "#
        );

        let results = execute_plan("mixed_join_equal_and_indistinct_left.json").await?;
        assert_snapshot!(pretty_sorted(&results),
            @"
        +----+-----+---------+-----+------+----------+
        | id | val | comment | id0 | val0 | comment0 |
        +----+-----+---------+-----+------+----------+
        | 1  | a   | c1      | 1   | a    | c1       |
        | 2  | b   | c2      | 2   | b    | c2       |
        | 3  |     | c3      | 3   |      | c3       |
        | 4  |     | c4      | 4   |      | c4       |
        | 5  | e   | c5      | 5   | e    | c5       |
        | 6  | f   | c6      | 6   | f    | c6       |
        +----+-----+---------+-----+------+----------+
        "
        );

        Ok(())
    }
}
