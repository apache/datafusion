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
    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

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
        Ok(format!("{}", plan))
    }

    #[tokio::test]
    async fn tpch_test_01() -> Result<()> {
        let plan_str = tpch_plan_to_string(1).await?;
        assert_snapshot!(
        plan_str,
        @r#"
            Projection: LINEITEM.L_RETURNFLAG, LINEITEM.L_LINESTATUS, sum(LINEITEM.L_QUANTITY) AS SUM_QTY, sum(LINEITEM.L_EXTENDEDPRICE) AS SUM_BASE_PRICE, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS SUM_DISC_PRICE, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT * Int32(1) + LINEITEM.L_TAX) AS SUM_CHARGE, avg(LINEITEM.L_QUANTITY) AS AVG_QTY, avg(LINEITEM.L_EXTENDEDPRICE) AS AVG_PRICE, avg(LINEITEM.L_DISCOUNT) AS AVG_DISC, count(Int64(1)) AS COUNT_ORDER
              Sort: LINEITEM.L_RETURNFLAG ASC NULLS LAST, LINEITEM.L_LINESTATUS ASC NULLS LAST
                Aggregate: groupBy=[[LINEITEM.L_RETURNFLAG, LINEITEM.L_LINESTATUS]], aggr=[[sum(LINEITEM.L_QUANTITY), sum(LINEITEM.L_EXTENDEDPRICE), sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT), sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT * Int32(1) + LINEITEM.L_TAX), avg(LINEITEM.L_QUANTITY), avg(LINEITEM.L_EXTENDEDPRICE), avg(LINEITEM.L_DISCOUNT), count(Int64(1))]]
                  Projection: LINEITEM.L_RETURNFLAG, LINEITEM.L_LINESTATUS, LINEITEM.L_QUANTITY, LINEITEM.L_EXTENDEDPRICE, LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT), LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT) * (CAST(Int32(1) AS Decimal128(15, 2)) + LINEITEM.L_TAX), LINEITEM.L_DISCOUNT
                    Filter: LINEITEM.L_SHIPDATE <= Date32("1998-12-01") - IntervalDayTime("IntervalDayTime { days: 0, milliseconds: 10368000 }")
                      TableScan: LINEITEM
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
              Sort: SUPPLIER.S_ACCTBAL DESC NULLS FIRST, NATION.N_NAME ASC NULLS LAST, SUPPLIER.S_NAME ASC NULLS LAST, PART.P_PARTKEY ASC NULLS LAST
                Projection: SUPPLIER.S_ACCTBAL, SUPPLIER.S_NAME, NATION.N_NAME, PART.P_PARTKEY, PART.P_MFGR, SUPPLIER.S_ADDRESS, SUPPLIER.S_PHONE, SUPPLIER.S_COMMENT
                  Filter: PART.P_PARTKEY = PARTSUPP.PS_PARTKEY AND SUPPLIER.S_SUPPKEY = PARTSUPP.PS_SUPPKEY AND PART.P_SIZE = Int32(15) AND PART.P_TYPE LIKE CAST(Utf8("%BRASS") AS Utf8) AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_REGIONKEY = REGION.R_REGIONKEY AND REGION.R_NAME = Utf8("EUROPE") AND PARTSUPP.PS_SUPPLYCOST = (<subquery>)
                    Subquery:
                      Aggregate: groupBy=[[]], aggr=[[min(PARTSUPP.PS_SUPPLYCOST)]]
                        Projection: PARTSUPP.PS_SUPPLYCOST
                          Filter: PARTSUPP.PS_PARTKEY = PARTSUPP.PS_PARTKEY AND SUPPLIER.S_SUPPKEY = PARTSUPP.PS_SUPPKEY AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_REGIONKEY = REGION.R_REGIONKEY AND REGION.R_NAME = Utf8("EUROPE")
                            Cross Join: 
                              Cross Join: 
                                Cross Join: 
                                  TableScan: PARTSUPP
                                  TableScan: SUPPLIER
                                TableScan: NATION
                              TableScan: REGION
                    Cross Join: 
                      Cross Join: 
                        Cross Join: 
                          Cross Join: 
                            TableScan: PART
                            TableScan: SUPPLIER
                          TableScan: PARTSUPP
                        TableScan: NATION
                      TableScan: REGION
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
            Projection: LINEITEM.L_ORDERKEY, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS REVENUE, ORDERS.O_ORDERDATE, ORDERS.O_SHIPPRIORITY
              Limit: skip=0, fetch=10
                Sort: sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) DESC NULLS FIRST, ORDERS.O_ORDERDATE ASC NULLS LAST
                  Projection: LINEITEM.L_ORDERKEY, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT), ORDERS.O_ORDERDATE, ORDERS.O_SHIPPRIORITY
                    Aggregate: groupBy=[[LINEITEM.L_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_SHIPPRIORITY]], aggr=[[sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT)]]
                      Projection: LINEITEM.L_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_SHIPPRIORITY, LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT)
                        Filter: CUSTOMER.C_MKTSEGMENT = Utf8("BUILDING") AND CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY AND LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY AND ORDERS.O_ORDERDATE < CAST(Utf8("1995-03-15") AS Date32) AND LINEITEM.L_SHIPDATE > CAST(Utf8("1995-03-15") AS Date32)
                          Cross Join: 
                            Cross Join: 
                              TableScan: LINEITEM
                              TableScan: CUSTOMER
                            TableScan: ORDERS
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
            Projection: ORDERS.O_ORDERPRIORITY, count(Int64(1)) AS ORDER_COUNT
              Sort: ORDERS.O_ORDERPRIORITY ASC NULLS LAST
                Aggregate: groupBy=[[ORDERS.O_ORDERPRIORITY]], aggr=[[count(Int64(1))]]
                  Projection: ORDERS.O_ORDERPRIORITY
                    Filter: ORDERS.O_ORDERDATE >= CAST(Utf8("1993-07-01") AS Date32) AND ORDERS.O_ORDERDATE < CAST(Utf8("1993-10-01") AS Date32) AND EXISTS (<subquery>)
                      Subquery:
                        Filter: LINEITEM.L_ORDERKEY = LINEITEM.L_ORDERKEY AND LINEITEM.L_COMMITDATE < LINEITEM.L_RECEIPTDATE
                          TableScan: LINEITEM
                      TableScan: ORDERS
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
            Projection: NATION.N_NAME, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS REVENUE
              Sort: sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) DESC NULLS FIRST
                Aggregate: groupBy=[[NATION.N_NAME]], aggr=[[sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT)]]
                  Projection: NATION.N_NAME, LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT)
                    Filter: CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY AND LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY AND LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY AND CUSTOMER.C_NATIONKEY = SUPPLIER.S_NATIONKEY AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_REGIONKEY = REGION.R_REGIONKEY AND REGION.R_NAME = Utf8("ASIA") AND ORDERS.O_ORDERDATE >= CAST(Utf8("1994-01-01") AS Date32) AND ORDERS.O_ORDERDATE < CAST(Utf8("1995-01-01") AS Date32)
                      Cross Join: 
                        Cross Join: 
                          Cross Join: 
                            Cross Join: 
                              Cross Join: 
                                TableScan: CUSTOMER
                                TableScan: ORDERS
                              TableScan: LINEITEM
                            TableScan: SUPPLIER
                          TableScan: NATION
                        TableScan: REGION
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
            Aggregate: groupBy=[[]], aggr=[[sum(LINEITEM.L_EXTENDEDPRICE * LINEITEM.L_DISCOUNT) AS REVENUE]]
              Projection: LINEITEM.L_EXTENDEDPRICE * LINEITEM.L_DISCOUNT
                Filter: LINEITEM.L_SHIPDATE >= CAST(Utf8("1994-01-01") AS Date32) AND LINEITEM.L_SHIPDATE < CAST(Utf8("1995-01-01") AS Date32) AND LINEITEM.L_DISCOUNT >= Decimal128(Some(5),3,2) AND LINEITEM.L_DISCOUNT <= Decimal128(Some(7),3,2) AND LINEITEM.L_QUANTITY < CAST(Int32(24) AS Decimal128(15, 2))
                  TableScan: LINEITEM
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
            Projection: CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS REVENUE, CUSTOMER.C_ACCTBAL, NATION.N_NAME, CUSTOMER.C_ADDRESS, CUSTOMER.C_PHONE, CUSTOMER.C_COMMENT
              Limit: skip=0, fetch=20
                Sort: sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) DESC NULLS FIRST
                  Projection: CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME, sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT), CUSTOMER.C_ACCTBAL, NATION.N_NAME, CUSTOMER.C_ADDRESS, CUSTOMER.C_PHONE, CUSTOMER.C_COMMENT
                    Aggregate: groupBy=[[CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME, CUSTOMER.C_ACCTBAL, CUSTOMER.C_PHONE, NATION.N_NAME, CUSTOMER.C_ADDRESS, CUSTOMER.C_COMMENT]], aggr=[[sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT)]]
                      Projection: CUSTOMER.C_CUSTKEY, CUSTOMER.C_NAME, CUSTOMER.C_ACCTBAL, CUSTOMER.C_PHONE, NATION.N_NAME, CUSTOMER.C_ADDRESS, CUSTOMER.C_COMMENT, LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT)
                        Filter: CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY AND LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY AND ORDERS.O_ORDERDATE >= CAST(Utf8("1993-10-01") AS Date32) AND ORDERS.O_ORDERDATE < CAST(Utf8("1994-01-01") AS Date32) AND LINEITEM.L_RETURNFLAG = Utf8("R") AND CUSTOMER.C_NATIONKEY = NATION.N_NATIONKEY
                          Cross Join: 
                            Cross Join: 
                              Cross Join: 
                                TableScan: CUSTOMER
                                TableScan: ORDERS
                              TableScan: LINEITEM
                            TableScan: NATION
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
            Projection: PARTSUPP.PS_PARTKEY, sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY) AS value
              Sort: sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY) DESC NULLS FIRST
                Filter: sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY) > (<subquery>)
                  Subquery:
                    Projection: sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY) * Decimal128(Some(1000000),11,10)
                      Aggregate: groupBy=[[]], aggr=[[sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY)]]
                        Projection: PARTSUPP.PS_SUPPLYCOST * CAST(PARTSUPP.PS_AVAILQTY AS Decimal128(19, 0))
                          Filter: PARTSUPP.PS_SUPPKEY = SUPPLIER.S_SUPPKEY AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_NAME = Utf8("JAPAN")
                            Cross Join: 
                              Cross Join: 
                                TableScan: PARTSUPP
                                TableScan: SUPPLIER
                              TableScan: NATION
                  Aggregate: groupBy=[[PARTSUPP.PS_PARTKEY]], aggr=[[sum(PARTSUPP.PS_SUPPLYCOST * PARTSUPP.PS_AVAILQTY)]]
                    Projection: PARTSUPP.PS_PARTKEY, PARTSUPP.PS_SUPPLYCOST * CAST(PARTSUPP.PS_AVAILQTY AS Decimal128(19, 0))
                      Filter: PARTSUPP.PS_SUPPKEY = SUPPLIER.S_SUPPKEY AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_NAME = Utf8("JAPAN")
                        Cross Join: 
                          Cross Join: 
                            TableScan: PARTSUPP
                            TableScan: SUPPLIER
                          TableScan: NATION
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
            Projection: LINEITEM.L_SHIPMODE, sum(CASE WHEN ORDERS.O_ORDERPRIORITY = Utf8("1-URGENT") OR ORDERS.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END) AS HIGH_LINE_COUNT, sum(CASE WHEN ORDERS.O_ORDERPRIORITY != Utf8("1-URGENT") AND ORDERS.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END) AS LOW_LINE_COUNT
              Sort: LINEITEM.L_SHIPMODE ASC NULLS LAST
                Aggregate: groupBy=[[LINEITEM.L_SHIPMODE]], aggr=[[sum(CASE WHEN ORDERS.O_ORDERPRIORITY = Utf8("1-URGENT") OR ORDERS.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END), sum(CASE WHEN ORDERS.O_ORDERPRIORITY != Utf8("1-URGENT") AND ORDERS.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END)]]
                  Projection: LINEITEM.L_SHIPMODE, CASE WHEN ORDERS.O_ORDERPRIORITY = Utf8("1-URGENT") OR ORDERS.O_ORDERPRIORITY = Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END, CASE WHEN ORDERS.O_ORDERPRIORITY != Utf8("1-URGENT") AND ORDERS.O_ORDERPRIORITY != Utf8("2-HIGH") THEN Int32(1) ELSE Int32(0) END
                    Filter: ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY AND (LINEITEM.L_SHIPMODE = CAST(Utf8("MAIL") AS Utf8) OR LINEITEM.L_SHIPMODE = CAST(Utf8("SHIP") AS Utf8)) AND LINEITEM.L_COMMITDATE < LINEITEM.L_RECEIPTDATE AND LINEITEM.L_SHIPDATE < LINEITEM.L_COMMITDATE AND LINEITEM.L_RECEIPTDATE >= CAST(Utf8("1994-01-01") AS Date32) AND LINEITEM.L_RECEIPTDATE < CAST(Utf8("1995-01-01") AS Date32)
                      Cross Join: 
                        TableScan: ORDERS
                        TableScan: LINEITEM
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
            Projection: count(ORDERS.O_ORDERKEY) AS C_COUNT, count(Int64(1)) AS CUSTDIST
              Sort: count(Int64(1)) DESC NULLS FIRST, count(ORDERS.O_ORDERKEY) DESC NULLS FIRST
                Projection: count(ORDERS.O_ORDERKEY), count(Int64(1))
                  Aggregate: groupBy=[[count(ORDERS.O_ORDERKEY)]], aggr=[[count(Int64(1))]]
                    Projection: count(ORDERS.O_ORDERKEY)
                      Aggregate: groupBy=[[CUSTOMER.C_CUSTKEY]], aggr=[[count(ORDERS.O_ORDERKEY)]]
                        Projection: CUSTOMER.C_CUSTKEY, ORDERS.O_ORDERKEY
                          Left Join: CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY Filter: NOT ORDERS.O_COMMENT LIKE CAST(Utf8("%special%requests%") AS Utf8)
                            TableScan: CUSTOMER
                            TableScan: ORDERS
            "#        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_14() -> Result<()> {
        let plan_str = tpch_plan_to_string(14).await?;
        assert_snapshot!(
        plan_str,
        @r#"
            Projection: Decimal128(Some(10000),5,2) * sum(CASE WHEN PART.P_TYPE LIKE Utf8("PROMO%") THEN LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT ELSE Decimal128(Some(0),19,4) END) / sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS PROMO_REVENUE
              Aggregate: groupBy=[[]], aggr=[[sum(CASE WHEN PART.P_TYPE LIKE Utf8("PROMO%") THEN LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT ELSE Decimal128(Some(0),19,4) END), sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT)]]
                Projection: CASE WHEN PART.P_TYPE LIKE CAST(Utf8("PROMO%") AS Utf8) THEN LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT) ELSE Decimal128(Some(0),19,4) END, LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT)
                  Filter: LINEITEM.L_PARTKEY = PART.P_PARTKEY AND LINEITEM.L_SHIPDATE >= Date32("1995-09-01") AND LINEITEM.L_SHIPDATE < CAST(Utf8("1995-10-01") AS Date32)
                    Cross Join: 
                      TableScan: LINEITEM
                      TableScan: PART
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
            Projection: PART.P_BRAND, PART.P_TYPE, PART.P_SIZE, count(DISTINCT PARTSUPP.PS_SUPPKEY) AS SUPPLIER_CNT
              Sort: count(DISTINCT PARTSUPP.PS_SUPPKEY) DESC NULLS FIRST, PART.P_BRAND ASC NULLS LAST, PART.P_TYPE ASC NULLS LAST, PART.P_SIZE ASC NULLS LAST
                Aggregate: groupBy=[[PART.P_BRAND, PART.P_TYPE, PART.P_SIZE]], aggr=[[count(DISTINCT PARTSUPP.PS_SUPPKEY)]]
                  Projection: PART.P_BRAND, PART.P_TYPE, PART.P_SIZE, PARTSUPP.PS_SUPPKEY
                    Filter: PART.P_PARTKEY = PARTSUPP.PS_PARTKEY AND PART.P_BRAND != Utf8("Brand#45") AND NOT PART.P_TYPE LIKE CAST(Utf8("MEDIUM POLISHED%") AS Utf8) AND (PART.P_SIZE = Int32(49) OR PART.P_SIZE = Int32(14) OR PART.P_SIZE = Int32(23) OR PART.P_SIZE = Int32(45) OR PART.P_SIZE = Int32(19) OR PART.P_SIZE = Int32(3) OR PART.P_SIZE = Int32(36) OR PART.P_SIZE = Int32(9)) AND NOT PARTSUPP.PS_SUPPKEY IN (<subquery>)
                      Subquery:
                        Projection: SUPPLIER.S_SUPPKEY
                          Filter: SUPPLIER.S_COMMENT LIKE CAST(Utf8("%Customer%Complaints%") AS Utf8)
                            TableScan: SUPPLIER
                      Cross Join: 
                        TableScan: PARTSUPP
                        TableScan: PART
            "#
                );
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn tpch_test_17() -> Result<()> {
        let plan_str = tpch_plan_to_string(17).await?;
        assert_snapshot!(plan_str, "panics due to out of bounds field access");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_18() -> Result<()> {
        let plan_str = tpch_plan_to_string(18).await?;
        assert_snapshot!(
        plan_str,
        @r#"
            Projection: CUSTOMER.C_NAME, CUSTOMER.C_CUSTKEY, ORDERS.O_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_TOTALPRICE, sum(LINEITEM.L_QUANTITY) AS EXPR$5
              Limit: skip=0, fetch=100
                Sort: ORDERS.O_TOTALPRICE DESC NULLS FIRST, ORDERS.O_ORDERDATE ASC NULLS LAST
                  Aggregate: groupBy=[[CUSTOMER.C_NAME, CUSTOMER.C_CUSTKEY, ORDERS.O_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_TOTALPRICE]], aggr=[[sum(LINEITEM.L_QUANTITY)]]
                    Projection: CUSTOMER.C_NAME, CUSTOMER.C_CUSTKEY, ORDERS.O_ORDERKEY, ORDERS.O_ORDERDATE, ORDERS.O_TOTALPRICE, LINEITEM.L_QUANTITY
                      Filter: ORDERS.O_ORDERKEY IN (<subquery>) AND CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY AND ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY
                        Subquery:
                          Projection: LINEITEM.L_ORDERKEY
                            Filter: sum(LINEITEM.L_QUANTITY) > CAST(Int32(300) AS Decimal128(15, 2))
                              Aggregate: groupBy=[[LINEITEM.L_ORDERKEY]], aggr=[[sum(LINEITEM.L_QUANTITY)]]
                                Projection: LINEITEM.L_ORDERKEY, LINEITEM.L_QUANTITY
                                  TableScan: LINEITEM
                        Cross Join: 
                          Cross Join: 
                            TableScan: CUSTOMER
                            TableScan: ORDERS
                          TableScan: LINEITEM
            "#
                );
        Ok(())
    }
    #[tokio::test]
    async fn tpch_test_19() -> Result<()> {
        let plan_str = tpch_plan_to_string(19).await?;
        assert_snapshot!(
        plan_str,
        @r#"
            Aggregate: groupBy=[[]], aggr=[[sum(LINEITEM.L_EXTENDEDPRICE * Int32(1) - LINEITEM.L_DISCOUNT) AS REVENUE]]
              Projection: LINEITEM.L_EXTENDEDPRICE * (CAST(Int32(1) AS Decimal128(15, 2)) - LINEITEM.L_DISCOUNT)
                Filter: PART.P_PARTKEY = LINEITEM.L_PARTKEY AND PART.P_BRAND = Utf8("Brand#12") AND (PART.P_CONTAINER = CAST(Utf8("SM CASE") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("SM BOX") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("SM PACK") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("SM PKG") AS Utf8)) AND LINEITEM.L_QUANTITY >= CAST(Int32(1) AS Decimal128(15, 2)) AND LINEITEM.L_QUANTITY <= CAST(Int32(1) + Int32(10) AS Decimal128(15, 2)) AND PART.P_SIZE >= Int32(1) AND PART.P_SIZE <= Int32(5) AND (LINEITEM.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR LINEITEM.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND LINEITEM.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON") OR PART.P_PARTKEY = LINEITEM.L_PARTKEY AND PART.P_BRAND = Utf8("Brand#23") AND (PART.P_CONTAINER = CAST(Utf8("MED BAG") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("MED BOX") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("MED PKG") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("MED PACK") AS Utf8)) AND LINEITEM.L_QUANTITY >= CAST(Int32(10) AS Decimal128(15, 2)) AND LINEITEM.L_QUANTITY <= CAST(Int32(10) + Int32(10) AS Decimal128(15, 2)) AND PART.P_SIZE >= Int32(1) AND PART.P_SIZE <= Int32(10) AND (LINEITEM.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR LINEITEM.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND LINEITEM.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON") OR PART.P_PARTKEY = LINEITEM.L_PARTKEY AND PART.P_BRAND = Utf8("Brand#34") AND (PART.P_CONTAINER = CAST(Utf8("LG CASE") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("LG BOX") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("LG PACK") AS Utf8) OR PART.P_CONTAINER = CAST(Utf8("LG PKG") AS Utf8)) AND LINEITEM.L_QUANTITY >= CAST(Int32(20) AS Decimal128(15, 2)) AND LINEITEM.L_QUANTITY <= CAST(Int32(20) + Int32(10) AS Decimal128(15, 2)) AND PART.P_SIZE >= Int32(1) AND PART.P_SIZE <= Int32(15) AND (LINEITEM.L_SHIPMODE = CAST(Utf8("AIR") AS Utf8) OR LINEITEM.L_SHIPMODE = CAST(Utf8("AIR REG") AS Utf8)) AND LINEITEM.L_SHIPINSTRUCT = Utf8("DELIVER IN PERSON")
                  Cross Join: 
                    TableScan: LINEITEM
                    TableScan: PART
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
            Sort: SUPPLIER.S_NAME ASC NULLS LAST
              Projection: SUPPLIER.S_NAME, SUPPLIER.S_ADDRESS
                Filter: SUPPLIER.S_SUPPKEY IN (<subquery>) AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_NAME = Utf8("CANADA")
                  Subquery:
                    Projection: PARTSUPP.PS_SUPPKEY
                      Filter: PARTSUPP.PS_PARTKEY IN (<subquery>) AND CAST(PARTSUPP.PS_AVAILQTY AS Decimal128(19, 0)) > (<subquery>)
                        Subquery:
                          Projection: PART.P_PARTKEY
                            Filter: PART.P_NAME LIKE CAST(Utf8("forest%") AS Utf8)
                              TableScan: PART
                        Subquery:
                          Projection: Decimal128(Some(5),2,1) * sum(LINEITEM.L_QUANTITY)
                            Aggregate: groupBy=[[]], aggr=[[sum(LINEITEM.L_QUANTITY)]]
                              Projection: LINEITEM.L_QUANTITY
                                Filter: LINEITEM.L_PARTKEY = LINEITEM.L_ORDERKEY AND LINEITEM.L_SUPPKEY = LINEITEM.L_PARTKEY AND LINEITEM.L_SHIPDATE >= CAST(Utf8("1994-01-01") AS Date32) AND LINEITEM.L_SHIPDATE < CAST(Utf8("1995-01-01") AS Date32)
                                  TableScan: LINEITEM
                        TableScan: PARTSUPP
                  Cross Join: 
                    TableScan: SUPPLIER
                    TableScan: NATION
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
        Projection: SUPPLIER.S_NAME, count(Int64(1)) AS NUMWAIT
          Limit: skip=0, fetch=100
            Sort: count(Int64(1)) DESC NULLS FIRST, SUPPLIER.S_NAME ASC NULLS LAST
              Aggregate: groupBy=[[SUPPLIER.S_NAME]], aggr=[[count(Int64(1))]]
                Projection: SUPPLIER.S_NAME
                  Filter: SUPPLIER.S_SUPPKEY = LINEITEM.L_SUPPKEY AND ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY AND ORDERS.O_ORDERSTATUS = Utf8("F") AND LINEITEM.L_RECEIPTDATE > LINEITEM.L_COMMITDATE AND EXISTS (<subquery>) AND NOT EXISTS (<subquery>) AND SUPPLIER.S_NATIONKEY = NATION.N_NATIONKEY AND NATION.N_NAME = Utf8("SAUDI ARABIA")
                    Subquery:
                      Filter: LINEITEM.L_ORDERKEY = LINEITEM.L_TAX AND LINEITEM.L_SUPPKEY != LINEITEM.L_LINESTATUS
                        TableScan: LINEITEM
                    Subquery:
                      Filter: LINEITEM.L_ORDERKEY = LINEITEM.L_TAX AND LINEITEM.L_SUPPKEY != LINEITEM.L_LINESTATUS AND LINEITEM.L_RECEIPTDATE > LINEITEM.L_COMMITDATE
                        TableScan: LINEITEM
                    Cross Join: 
                      Cross Join: 
                        Cross Join: 
                          TableScan: SUPPLIER
                          TableScan: LINEITEM
                        TableScan: ORDERS
                      TableScan: NATION
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
        Projection: substr(CUSTOMER.C_PHONE,Int32(1),Int32(2)) AS CNTRYCODE, count(Int64(1)) AS NUMCUST, sum(CUSTOMER.C_ACCTBAL) AS TOTACCTBAL
          Sort: substr(CUSTOMER.C_PHONE,Int32(1),Int32(2)) ASC NULLS LAST
            Aggregate: groupBy=[[substr(CUSTOMER.C_PHONE,Int32(1),Int32(2))]], aggr=[[count(Int64(1)), sum(CUSTOMER.C_ACCTBAL)]]
              Projection: substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)), CUSTOMER.C_ACCTBAL
                Filter: (substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("13") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("31") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("23") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("29") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("30") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("18") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("17") AS Utf8)) AND CUSTOMER.C_ACCTBAL > (<subquery>) AND NOT EXISTS (<subquery>)
                  Subquery:
                    Aggregate: groupBy=[[]], aggr=[[avg(CUSTOMER.C_ACCTBAL)]]
                      Projection: CUSTOMER.C_ACCTBAL
                        Filter: CUSTOMER.C_ACCTBAL > Decimal128(Some(0),3,2) AND (substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("13") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("31") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("23") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("29") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("30") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("18") AS Utf8) OR substr(CUSTOMER.C_PHONE, Int32(1), Int32(2)) = CAST(Utf8("17") AS Utf8))
                          TableScan: CUSTOMER
                  Subquery:
                    Filter: ORDERS.O_CUSTKEY = ORDERS.O_ORDERKEY
                      TableScan: ORDERS
                  TableScan: CUSTOMER
        "#
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
        Ok(format!("{}", plan))
    }

    #[tokio::test]
    async fn test_select_count_from_select_1() -> Result<()> {
        let plan_str =
            test_plan_to_string("select_count_from_select_1.substrait.json").await?;

        assert_snapshot!(
        plan_str,
        @r#"
            Aggregate: groupBy=[[]], aggr=[[count(Int64(1)) AS count(*)]]
              Values: (Int64(0))
            "#
                );
        Ok(())
    }

    #[tokio::test]
    async fn test_select_window_count() -> Result<()> {
        let plan_str = test_plan_to_string("select_window_count.substrait.json").await?;

        assert_snapshot!(
        plan_str,
        @r#"
        Projection: count(Int64(1)) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING AS LEAD_EXPR
          WindowAggr: windowExpr=[[count(Int64(1)) PARTITION BY [DATA.PART] ORDER BY [DATA.ORD ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING]]
            TableScan: DATA
        "#
                        );
        Ok(())
    }
}
