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
    use datafusion::common::Result;
    use datafusion::execution::options::CsvReadOptions;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    async fn create_context(files: Vec<(&str, &str)>) -> Result<SessionContext> {
        let ctx = SessionContext::new();
        for (table_name, file_path) in files {
            ctx.register_csv(table_name, file_path, CsvReadOptions::default())
                .await?;
        }
        Ok(ctx)
    }
    #[tokio::test]
    async fn tpch_test_1() -> Result<()> {
        let ctx = create_context(vec![(
            "FILENAME_PLACEHOLDER_0",
            "tests/testdata/tpch/lineitem.csv",
        )])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_1.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        let plan_str = format!("{}", plan);
        assert_eq!(
            plan_str,
            "Projection: FILENAME_PLACEHOLDER_0.l_returnflag AS L_RETURNFLAG, FILENAME_PLACEHOLDER_0.l_linestatus AS L_LINESTATUS, sum(FILENAME_PLACEHOLDER_0.l_quantity) AS SUM_QTY, sum(FILENAME_PLACEHOLDER_0.l_extendedprice) AS SUM_BASE_PRICE, sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount) AS SUM_DISC_PRICE, sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount * Int32(1) + FILENAME_PLACEHOLDER_0.l_tax) AS SUM_CHARGE, avg(FILENAME_PLACEHOLDER_0.l_quantity) AS AVG_QTY, avg(FILENAME_PLACEHOLDER_0.l_extendedprice) AS AVG_PRICE, avg(FILENAME_PLACEHOLDER_0.l_discount) AS AVG_DISC, count(Int64(1)) AS COUNT_ORDER\
             \n  Sort: FILENAME_PLACEHOLDER_0.l_returnflag ASC NULLS LAST, FILENAME_PLACEHOLDER_0.l_linestatus ASC NULLS LAST\
             \n    Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.l_returnflag, FILENAME_PLACEHOLDER_0.l_linestatus]], aggr=[[sum(FILENAME_PLACEHOLDER_0.l_quantity), sum(FILENAME_PLACEHOLDER_0.l_extendedprice), sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount), sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount * Int32(1) + FILENAME_PLACEHOLDER_0.l_tax), avg(FILENAME_PLACEHOLDER_0.l_quantity), avg(FILENAME_PLACEHOLDER_0.l_extendedprice), avg(FILENAME_PLACEHOLDER_0.l_discount), count(Int64(1))]]\
             \n      Projection: FILENAME_PLACEHOLDER_0.l_returnflag, FILENAME_PLACEHOLDER_0.l_linestatus, FILENAME_PLACEHOLDER_0.l_quantity, FILENAME_PLACEHOLDER_0.l_extendedprice, FILENAME_PLACEHOLDER_0.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_0.l_discount), FILENAME_PLACEHOLDER_0.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_0.l_discount) * (CAST(Int32(1) AS Decimal128(19, 0)) + FILENAME_PLACEHOLDER_0.l_tax), FILENAME_PLACEHOLDER_0.l_discount\
             \n        Filter: FILENAME_PLACEHOLDER_0.l_shipdate <= Date32(\"1998-12-01\") - IntervalDayTime(\"IntervalDayTime { days: 120, milliseconds: 0 }\")\
             \n          TableScan: FILENAME_PLACEHOLDER_0 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_2() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/part.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_4", "tests/testdata/tpch/region.csv"),
            ("FILENAME_PLACEHOLDER_5", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_6", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_7", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_8", "tests/testdata/tpch/region.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_2.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(
            plan_str,
            "Projection: FILENAME_PLACEHOLDER_1.s_acctbal AS S_ACCTBAL, FILENAME_PLACEHOLDER_1.s_name AS S_NAME, FILENAME_PLACEHOLDER_3.n_name AS N_NAME, FILENAME_PLACEHOLDER_0.p_partkey AS P_PARTKEY, FILENAME_PLACEHOLDER_0.p_mfgr AS P_MFGR, FILENAME_PLACEHOLDER_1.s_address AS S_ADDRESS, FILENAME_PLACEHOLDER_1.s_phone AS S_PHONE, FILENAME_PLACEHOLDER_1.s_comment AS S_COMMENT\
            \n  Limit: skip=0, fetch=100\
            \n    Sort: FILENAME_PLACEHOLDER_1.s_acctbal DESC NULLS FIRST, FILENAME_PLACEHOLDER_3.n_name ASC NULLS LAST, FILENAME_PLACEHOLDER_1.s_name ASC NULLS LAST, FILENAME_PLACEHOLDER_0.p_partkey ASC NULLS LAST\
            \n      Projection: FILENAME_PLACEHOLDER_1.s_acctbal, FILENAME_PLACEHOLDER_1.s_name, FILENAME_PLACEHOLDER_3.n_name, FILENAME_PLACEHOLDER_0.p_partkey, FILENAME_PLACEHOLDER_0.p_mfgr, FILENAME_PLACEHOLDER_1.s_address, FILENAME_PLACEHOLDER_1.s_phone, FILENAME_PLACEHOLDER_1.s_comment\
            \n        Filter: FILENAME_PLACEHOLDER_0.p_partkey = FILENAME_PLACEHOLDER_2.ps_partkey AND FILENAME_PLACEHOLDER_1.s_suppkey = FILENAME_PLACEHOLDER_2.ps_suppkey AND FILENAME_PLACEHOLDER_0.p_size = Int32(15) AND FILENAME_PLACEHOLDER_0.p_type LIKE CAST(Utf8(\"%BRASS\") AS Utf8) AND FILENAME_PLACEHOLDER_1.s_nationkey = FILENAME_PLACEHOLDER_3.n_nationkey AND FILENAME_PLACEHOLDER_3.n_regionkey = FILENAME_PLACEHOLDER_4.r_regionkey AND FILENAME_PLACEHOLDER_4.r_name = CAST(Utf8(\"EUROPE\") AS Utf8) AND FILENAME_PLACEHOLDER_2.ps_supplycost = (<subquery>)\
            \n          Subquery:\
            \n            Aggregate: groupBy=[[]], aggr=[[min(FILENAME_PLACEHOLDER_5.ps_supplycost)]]\
            \n              Projection: FILENAME_PLACEHOLDER_5.ps_supplycost\
            \n                Filter: FILENAME_PLACEHOLDER_5.ps_partkey = FILENAME_PLACEHOLDER_5.ps_partkey AND FILENAME_PLACEHOLDER_6.s_suppkey = FILENAME_PLACEHOLDER_5.ps_suppkey AND FILENAME_PLACEHOLDER_6.s_nationkey = FILENAME_PLACEHOLDER_7.n_nationkey AND FILENAME_PLACEHOLDER_7.n_regionkey = FILENAME_PLACEHOLDER_8.r_regionkey AND FILENAME_PLACEHOLDER_8.r_name = CAST(Utf8(\"EUROPE\") AS Utf8)\
            \n                  Inner Join:  Filter: Boolean(true)\
            \n                    Inner Join:  Filter: Boolean(true)\
            \n                      Inner Join:  Filter: Boolean(true)\
            \n                        TableScan: FILENAME_PLACEHOLDER_5 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
            \n                        TableScan: FILENAME_PLACEHOLDER_6 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
            \n                      TableScan: FILENAME_PLACEHOLDER_7 projection=[n_nationkey, n_name, n_regionkey, n_comment]\
            \n                    TableScan: FILENAME_PLACEHOLDER_8 projection=[r_regionkey, r_name, r_comment]\
            \n          Inner Join:  Filter: Boolean(true)\
            \n            Inner Join:  Filter: Boolean(true)\
            \n              Inner Join:  Filter: Boolean(true)\
            \n                Inner Join:  Filter: Boolean(true)\
            \n                  TableScan: FILENAME_PLACEHOLDER_0 projection=[p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]\
            \n                  TableScan: FILENAME_PLACEHOLDER_1 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
            \n                TableScan: FILENAME_PLACEHOLDER_2 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
            \n              TableScan: FILENAME_PLACEHOLDER_3 projection=[n_nationkey, n_name, n_regionkey, n_comment]\
            \n            TableScan: FILENAME_PLACEHOLDER_4 projection=[r_regionkey, r_name, r_comment]"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_3() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_3.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_2.l_orderkey AS L_ORDERKEY, sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) AS REVENUE, FILENAME_PLACEHOLDER_1.o_orderdate AS O_ORDERDATE, FILENAME_PLACEHOLDER_1.o_shippriority AS O_SHIPPRIORITY\
        \n  Limit: skip=0, fetch=10\
        \n    Sort: sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) DESC NULLS FIRST, FILENAME_PLACEHOLDER_1.o_orderdate ASC NULLS LAST\
        \n      Projection: FILENAME_PLACEHOLDER_2.l_orderkey, sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount), FILENAME_PLACEHOLDER_1.o_orderdate, FILENAME_PLACEHOLDER_1.o_shippriority\
        \n        Aggregate: groupBy=[[FILENAME_PLACEHOLDER_2.l_orderkey, FILENAME_PLACEHOLDER_1.o_orderdate, FILENAME_PLACEHOLDER_1.o_shippriority]], aggr=[[sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount)]]\
        \n          Projection: FILENAME_PLACEHOLDER_2.l_orderkey, FILENAME_PLACEHOLDER_1.o_orderdate, FILENAME_PLACEHOLDER_1.o_shippriority, FILENAME_PLACEHOLDER_2.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_2.l_discount)\
        \n            Filter: FILENAME_PLACEHOLDER_0.c_mktsegment = CAST(Utf8(\"HOUSEHOLD\") AS Utf8) AND FILENAME_PLACEHOLDER_0.c_custkey = FILENAME_PLACEHOLDER_1.o_custkey AND FILENAME_PLACEHOLDER_2.l_orderkey = FILENAME_PLACEHOLDER_1.o_orderkey AND FILENAME_PLACEHOLDER_1.o_orderdate < Date32(\"1995-03-25\") AND FILENAME_PLACEHOLDER_2.l_shipdate > Date32(\"1995-03-25\")\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                Inner Join:  Filter: Boolean(true)\
        \n                  TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\
        \n                  TableScan: FILENAME_PLACEHOLDER_1 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\n                TableScan: FILENAME_PLACEHOLDER_2 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_4() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_4.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");
        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.o_orderpriority AS O_ORDERPRIORITY, count(Int64(1)) AS ORDER_COUNT\
        \n  Sort: FILENAME_PLACEHOLDER_0.o_orderpriority ASC NULLS LAST\
        \n    Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.o_orderpriority]], aggr=[[count(Int64(1))]]\
        \n      Projection: FILENAME_PLACEHOLDER_0.o_orderpriority\
        \n        Filter: FILENAME_PLACEHOLDER_0.o_orderdate >= CAST(Utf8(\"1993-07-01\") AS Date32) AND FILENAME_PLACEHOLDER_0.o_orderdate < CAST(Utf8(\"1993-10-01\") AS Date32) AND EXISTS (<subquery>)\
        \n          Subquery:\
        \n            Filter: FILENAME_PLACEHOLDER_1.l_orderkey = FILENAME_PLACEHOLDER_1.l_orderkey AND FILENAME_PLACEHOLDER_1.l_commitdate < FILENAME_PLACEHOLDER_1.l_receiptdate\
        \n              TableScan: FILENAME_PLACEHOLDER_1 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n          TableScan: FILENAME_PLACEHOLDER_0 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_5() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/supplier.csv"),
            ("NATION", "tests/testdata/tpch/nation.csv"),
            ("REGION", "tests/testdata/tpch/region.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_5.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: NATION.n_name AS N_NAME, sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) AS REVENUE\
        \n  Sort: sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) DESC NULLS FIRST\
        \n    Aggregate: groupBy=[[NATION.n_name]], aggr=[[sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount)]]\
        \n      Projection: NATION.n_name, FILENAME_PLACEHOLDER_2.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_2.l_discount)\
        \n        Filter: FILENAME_PLACEHOLDER_0.c_custkey = FILENAME_PLACEHOLDER_1.o_custkey AND FILENAME_PLACEHOLDER_2.l_orderkey = FILENAME_PLACEHOLDER_1.o_orderkey AND FILENAME_PLACEHOLDER_2.l_suppkey = FILENAME_PLACEHOLDER_3.s_suppkey AND FILENAME_PLACEHOLDER_0.c_nationkey = FILENAME_PLACEHOLDER_3.s_nationkey AND FILENAME_PLACEHOLDER_3.s_nationkey = NATION.n_nationkey AND NATION.n_regionkey = REGION.r_regionkey AND REGION.r_name = CAST(Utf8(\"ASIA\") AS Utf8) AND FILENAME_PLACEHOLDER_1.o_orderdate >= CAST(Utf8(\"1994-01-01\") AS Date32) AND FILENAME_PLACEHOLDER_1.o_orderdate < CAST(Utf8(\"1995-01-01\") AS Date32)\
        \n          Inner Join:  Filter: Boolean(true)\
        \n            Inner Join:  Filter: Boolean(true)\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                Inner Join:  Filter: Boolean(true)\
        \n                  Inner Join:  Filter: Boolean(true)\
        \n                    TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\
        \n                    TableScan: FILENAME_PLACEHOLDER_1 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\
        \n                  TableScan: FILENAME_PLACEHOLDER_2 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n                TableScan: FILENAME_PLACEHOLDER_3 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n              TableScan: NATION projection=[n_nationkey, n_name, n_regionkey, n_comment]\
        \n            TableScan: REGION projection=[r_regionkey, r_name, r_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_6() -> Result<()> {
        let ctx = create_context(vec![(
            "FILENAME_PLACEHOLDER_0",
            "tests/testdata/tpch/lineitem.csv",
        )])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_6.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Aggregate: groupBy=[[]], aggr=[[sum(FILENAME_PLACEHOLDER_0.l_extendedprice * FILENAME_PLACEHOLDER_0.l_discount) AS REVENUE]]\
        \n  Projection: FILENAME_PLACEHOLDER_0.l_extendedprice * FILENAME_PLACEHOLDER_0.l_discount\
        \n    Filter: FILENAME_PLACEHOLDER_0.l_shipdate >= CAST(Utf8(\"1994-01-01\") AS Date32) AND FILENAME_PLACEHOLDER_0.l_shipdate < CAST(Utf8(\"1995-01-01\") AS Date32) AND FILENAME_PLACEHOLDER_0.l_discount >= Decimal128(Some(5),3,2) AND FILENAME_PLACEHOLDER_0.l_discount <= Decimal128(Some(7),3,2) AND FILENAME_PLACEHOLDER_0.l_quantity < CAST(Int32(24) AS Decimal128(19, 0))\
        \n      TableScan: FILENAME_PLACEHOLDER_0 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]");
        Ok(())
    }

    // TODO: missing plan 7, 8, 9
    #[tokio::test]
    async fn tpch_test_10() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/nation.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_10.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.c_custkey AS C_CUSTKEY, FILENAME_PLACEHOLDER_0.c_name AS C_NAME, sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) AS REVENUE, FILENAME_PLACEHOLDER_0.c_acctbal AS C_ACCTBAL, FILENAME_PLACEHOLDER_3.n_name AS N_NAME, FILENAME_PLACEHOLDER_0.c_address AS C_ADDRESS, FILENAME_PLACEHOLDER_0.c_phone AS C_PHONE, FILENAME_PLACEHOLDER_0.c_comment AS C_COMMENT\
        \n  Limit: skip=0, fetch=20\
        \n    Sort: sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount) DESC NULLS FIRST\
        \n      Projection: FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_0.c_name, sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount), FILENAME_PLACEHOLDER_0.c_acctbal, FILENAME_PLACEHOLDER_3.n_name, FILENAME_PLACEHOLDER_0.c_address, FILENAME_PLACEHOLDER_0.c_phone, FILENAME_PLACEHOLDER_0.c_comment\n        Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_0.c_name, FILENAME_PLACEHOLDER_0.c_acctbal, FILENAME_PLACEHOLDER_0.c_phone, FILENAME_PLACEHOLDER_3.n_name, FILENAME_PLACEHOLDER_0.c_address, FILENAME_PLACEHOLDER_0.c_comment]], aggr=[[sum(FILENAME_PLACEHOLDER_2.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_2.l_discount)]]\
        \n          Projection: FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_0.c_name, FILENAME_PLACEHOLDER_0.c_acctbal, FILENAME_PLACEHOLDER_0.c_phone, FILENAME_PLACEHOLDER_3.n_name, FILENAME_PLACEHOLDER_0.c_address, FILENAME_PLACEHOLDER_0.c_comment, FILENAME_PLACEHOLDER_2.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_2.l_discount)\
        \n            Filter: FILENAME_PLACEHOLDER_0.c_custkey = FILENAME_PLACEHOLDER_1.o_custkey AND FILENAME_PLACEHOLDER_2.l_orderkey = FILENAME_PLACEHOLDER_1.o_orderkey AND FILENAME_PLACEHOLDER_1.o_orderdate >= CAST(Utf8(\"1993-10-01\") AS Date32) AND FILENAME_PLACEHOLDER_1.o_orderdate < CAST(Utf8(\"1994-01-01\") AS Date32) AND FILENAME_PLACEHOLDER_2.l_returnflag = Utf8(\"R\") AND FILENAME_PLACEHOLDER_0.c_nationkey = FILENAME_PLACEHOLDER_3.n_nationkey\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                Inner Join:  Filter: Boolean(true)\
        \n                  Inner Join:  Filter: Boolean(true)\
        \n                    TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\
        \n                    TableScan: FILENAME_PLACEHOLDER_1 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\
        \n                  TableScan: FILENAME_PLACEHOLDER_2 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n                TableScan: FILENAME_PLACEHOLDER_3 projection=[n_nationkey, n_name, n_regionkey, n_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_11() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_4", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_5", "tests/testdata/tpch/nation.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_11.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.ps_partkey AS PS_PARTKEY, sum(FILENAME_PLACEHOLDER_0.ps_supplycost * FILENAME_PLACEHOLDER_0.ps_availqty) AS value\
        \n  Sort: sum(FILENAME_PLACEHOLDER_0.ps_supplycost * FILENAME_PLACEHOLDER_0.ps_availqty) DESC NULLS FIRST\
        \n    Filter: sum(FILENAME_PLACEHOLDER_0.ps_supplycost * FILENAME_PLACEHOLDER_0.ps_availqty) > (<subquery>)\
        \n      Subquery:\
        \n        Projection: sum(FILENAME_PLACEHOLDER_3.ps_supplycost * FILENAME_PLACEHOLDER_3.ps_availqty) * Decimal128(Some(1000000),11,10)\
        \n          Aggregate: groupBy=[[]], aggr=[[sum(FILENAME_PLACEHOLDER_3.ps_supplycost * FILENAME_PLACEHOLDER_3.ps_availqty)]]\
        \n            Projection: FILENAME_PLACEHOLDER_3.ps_supplycost * CAST(FILENAME_PLACEHOLDER_3.ps_availqty AS Decimal128(19, 0))\
        \n              Filter: FILENAME_PLACEHOLDER_3.ps_suppkey = FILENAME_PLACEHOLDER_4.s_suppkey AND FILENAME_PLACEHOLDER_4.s_nationkey = FILENAME_PLACEHOLDER_5.n_nationkey AND FILENAME_PLACEHOLDER_5.n_name = CAST(Utf8(\"JAPAN\") AS Utf8)\
        \n                Inner Join:  Filter: Boolean(true)\
        \n                  Inner Join:  Filter: Boolean(true)\
        \n                    TableScan: FILENAME_PLACEHOLDER_3 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
        \n                    TableScan: FILENAME_PLACEHOLDER_4 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n                  TableScan: FILENAME_PLACEHOLDER_5 projection=[n_nationkey, n_name, n_regionkey, n_comment]\
        \n      Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.ps_partkey]], aggr=[[sum(FILENAME_PLACEHOLDER_0.ps_supplycost * FILENAME_PLACEHOLDER_0.ps_availqty)]]\
        \n        Projection: FILENAME_PLACEHOLDER_0.ps_partkey, FILENAME_PLACEHOLDER_0.ps_supplycost * CAST(FILENAME_PLACEHOLDER_0.ps_availqty AS Decimal128(19, 0))\
        \n          Filter: FILENAME_PLACEHOLDER_0.ps_suppkey = FILENAME_PLACEHOLDER_1.s_suppkey AND FILENAME_PLACEHOLDER_1.s_nationkey = FILENAME_PLACEHOLDER_2.n_nationkey AND FILENAME_PLACEHOLDER_2.n_name = CAST(Utf8(\"JAPAN\") AS Utf8)\
        \n            Inner Join:  Filter: Boolean(true)\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                TableScan: FILENAME_PLACEHOLDER_0 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
        \n                TableScan: FILENAME_PLACEHOLDER_1 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n              TableScan: FILENAME_PLACEHOLDER_2 projection=[n_nationkey, n_name, n_regionkey, n_comment]");
        Ok(())
    }

    // missing query 12
    #[tokio::test]
    async fn tpch_test_13() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/orders.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_13.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: count(FILENAME_PLACEHOLDER_1.o_orderkey) AS C_COUNT, count(Int64(1)) AS CUSTDIST\
        \n  Sort: count(Int64(1)) DESC NULLS FIRST, count(FILENAME_PLACEHOLDER_1.o_orderkey) DESC NULLS FIRST\
        \n    Projection: count(FILENAME_PLACEHOLDER_1.o_orderkey), count(Int64(1))\
        \n      Aggregate: groupBy=[[count(FILENAME_PLACEHOLDER_1.o_orderkey)]], aggr=[[count(Int64(1))]]\
        \n        Projection: count(FILENAME_PLACEHOLDER_1.o_orderkey)\
        \n          Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.c_custkey]], aggr=[[count(FILENAME_PLACEHOLDER_1.o_orderkey)]]\
        \n            Projection: FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_1.o_orderkey\
        \n              Left Join: FILENAME_PLACEHOLDER_0.c_custkey = FILENAME_PLACEHOLDER_1.o_custkey Filter: NOT FILENAME_PLACEHOLDER_1.o_comment LIKE CAST(Utf8(\"%special%requests%\") AS Utf8)\
        \n                TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\
        \n                TableScan: FILENAME_PLACEHOLDER_1 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_14() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/part.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_14.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: Decimal128(Some(10000),5,2) * sum(CASE WHEN FILENAME_PLACEHOLDER_1.p_type LIKE Utf8(\"PROMO%\") THEN FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount ELSE Decimal128(Some(0),19,0) END) / sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount) AS PROMO_REVENUE\
        \n  Aggregate: groupBy=[[]], aggr=[[sum(CASE WHEN FILENAME_PLACEHOLDER_1.p_type LIKE Utf8(\"PROMO%\") THEN FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount ELSE Decimal128(Some(0),19,0) END), sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount)]]\
        \n    Projection: CASE WHEN FILENAME_PLACEHOLDER_1.p_type LIKE CAST(Utf8(\"PROMO%\") AS Utf8) THEN FILENAME_PLACEHOLDER_0.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_0.l_discount) ELSE Decimal128(Some(0),19,0) END, FILENAME_PLACEHOLDER_0.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_0.l_discount)\
        \n      Filter: FILENAME_PLACEHOLDER_0.l_partkey = FILENAME_PLACEHOLDER_1.p_partkey AND FILENAME_PLACEHOLDER_0.l_shipdate >= Date32(\"1995-09-01\") AND FILENAME_PLACEHOLDER_0.l_shipdate < CAST(Utf8(\"1995-10-01\") AS Date32)\
        \n        Inner Join:  Filter: Boolean(true)\
        \n          TableScan: FILENAME_PLACEHOLDER_0 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n          TableScan: FILENAME_PLACEHOLDER_1 projection=[p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]");
        Ok(())
    }
    // query 15 is missing
    #[tokio::test]
    async fn tpch_test_16() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/part.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/supplier.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_16.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_1.p_brand AS P_BRAND, FILENAME_PLACEHOLDER_1.p_type AS P_TYPE, FILENAME_PLACEHOLDER_1.p_size AS P_SIZE, count(DISTINCT FILENAME_PLACEHOLDER_0.ps_suppkey) AS SUPPLIER_CNT\
        \n  Sort: count(DISTINCT FILENAME_PLACEHOLDER_0.ps_suppkey) DESC NULLS FIRST, FILENAME_PLACEHOLDER_1.p_brand ASC NULLS LAST, FILENAME_PLACEHOLDER_1.p_type ASC NULLS LAST, FILENAME_PLACEHOLDER_1.p_size ASC NULLS LAST\
        \n    Aggregate: groupBy=[[FILENAME_PLACEHOLDER_1.p_brand, FILENAME_PLACEHOLDER_1.p_type, FILENAME_PLACEHOLDER_1.p_size]], aggr=[[count(DISTINCT FILENAME_PLACEHOLDER_0.ps_suppkey)]]\
        \n      Projection: FILENAME_PLACEHOLDER_1.p_brand, FILENAME_PLACEHOLDER_1.p_type, FILENAME_PLACEHOLDER_1.p_size, FILENAME_PLACEHOLDER_0.ps_suppkey\
        \n        Filter: FILENAME_PLACEHOLDER_1.p_partkey = FILENAME_PLACEHOLDER_0.ps_partkey AND FILENAME_PLACEHOLDER_1.p_brand != CAST(Utf8(\"Brand#45\") AS Utf8) AND NOT FILENAME_PLACEHOLDER_1.p_type LIKE CAST(Utf8(\"MEDIUM POLISHED%\") AS Utf8) AND (FILENAME_PLACEHOLDER_1.p_size = Int32(49) OR FILENAME_PLACEHOLDER_1.p_size = Int32(14) OR FILENAME_PLACEHOLDER_1.p_size = Int32(23) OR FILENAME_PLACEHOLDER_1.p_size = Int32(45) OR FILENAME_PLACEHOLDER_1.p_size = Int32(19) OR FILENAME_PLACEHOLDER_1.p_size = Int32(3) OR FILENAME_PLACEHOLDER_1.p_size = Int32(36) OR FILENAME_PLACEHOLDER_1.p_size = Int32(9)) AND NOT CAST(FILENAME_PLACEHOLDER_0.ps_suppkey IN (<subquery>) AS Boolean)\
        \n          Subquery:\
        \n            Projection: FILENAME_PLACEHOLDER_2.s_suppkey\
        \n              Filter: FILENAME_PLACEHOLDER_2.s_comment LIKE CAST(Utf8(\"%Customer%Complaints%\") AS Utf8)\
        \n                TableScan: FILENAME_PLACEHOLDER_2 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n          Inner Join:  Filter: Boolean(true)\
        \n            TableScan: FILENAME_PLACEHOLDER_0 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
        \n            TableScan: FILENAME_PLACEHOLDER_1 projection=[p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]");
        Ok(())
    }
    /// this test has some problem in json file internally, gonna fix it
    #[ignore]
    #[tokio::test]
    async fn tpch_test_17() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/part.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_17.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let _plan = from_substrait_plan(&ctx, &proto).await?;
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_18() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_18.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.c_name AS C_NAME, FILENAME_PLACEHOLDER_0.c_custkey AS C_CUSTKEY, FILENAME_PLACEHOLDER_1.o_orderkey AS O_ORDERKEY, FILENAME_PLACEHOLDER_1.o_orderdate AS O_ORDERDATE, FILENAME_PLACEHOLDER_1.o_totalprice AS O_TOTALPRICE, sum(FILENAME_PLACEHOLDER_2.l_quantity) AS EXPR$5\
        \n  Limit: skip=0, fetch=100\
        \n    Sort: FILENAME_PLACEHOLDER_1.o_totalprice DESC NULLS FIRST, FILENAME_PLACEHOLDER_1.o_orderdate ASC NULLS LAST\
        \n      Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.c_name, FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_1.o_orderkey, FILENAME_PLACEHOLDER_1.o_orderdate, FILENAME_PLACEHOLDER_1.o_totalprice]], aggr=[[sum(FILENAME_PLACEHOLDER_2.l_quantity)]]\
        \n        Projection: FILENAME_PLACEHOLDER_0.c_name, FILENAME_PLACEHOLDER_0.c_custkey, FILENAME_PLACEHOLDER_1.o_orderkey, FILENAME_PLACEHOLDER_1.o_orderdate, FILENAME_PLACEHOLDER_1.o_totalprice, FILENAME_PLACEHOLDER_2.l_quantity\
        \n          Filter: CAST(FILENAME_PLACEHOLDER_1.o_orderkey IN (<subquery>) AS Boolean) AND FILENAME_PLACEHOLDER_0.c_custkey = FILENAME_PLACEHOLDER_1.o_custkey AND FILENAME_PLACEHOLDER_1.o_orderkey = FILENAME_PLACEHOLDER_2.l_orderkey\
        \n            Subquery:\
        \n              Projection: FILENAME_PLACEHOLDER_3.l_orderkey\
        \n                Filter: sum(FILENAME_PLACEHOLDER_3.l_quantity) > CAST(Int32(300) AS Decimal128(19, 0))\
        \n                  Aggregate: groupBy=[[FILENAME_PLACEHOLDER_3.l_orderkey]], aggr=[[sum(FILENAME_PLACEHOLDER_3.l_quantity)]]\
        \n                    Projection: FILENAME_PLACEHOLDER_3.l_orderkey, FILENAME_PLACEHOLDER_3.l_quantity\
        \n                      TableScan: FILENAME_PLACEHOLDER_3 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n            Inner Join:  Filter: Boolean(true)\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\
        \n                TableScan: FILENAME_PLACEHOLDER_1 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\
        \n              TableScan: FILENAME_PLACEHOLDER_2 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]");
        Ok(())
    }
    #[tokio::test]
    async fn tpch_test_19() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/part.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_19.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Aggregate: groupBy=[[]], aggr=[[sum(FILENAME_PLACEHOLDER_0.l_extendedprice * Int32(1) - FILENAME_PLACEHOLDER_0.l_discount) AS REVENUE]]\n  Projection: FILENAME_PLACEHOLDER_0.l_extendedprice * (CAST(Int32(1) AS Decimal128(19, 0)) - FILENAME_PLACEHOLDER_0.l_discount)\
        \n    Filter: FILENAME_PLACEHOLDER_1.p_partkey = FILENAME_PLACEHOLDER_0.l_partkey AND FILENAME_PLACEHOLDER_1.p_brand = CAST(Utf8(\"Brand#12\") AS Utf8) AND (FILENAME_PLACEHOLDER_1.p_container = Utf8(\"SM CASE\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"SM BOX\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"SM PACK\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"SM PKG\")) AND FILENAME_PLACEHOLDER_0.l_quantity >= CAST(Int32(1) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_0.l_quantity <= CAST(Int32(1) + Int32(10) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_1.p_size >= Int32(1) AND FILENAME_PLACEHOLDER_1.p_size <= Int32(5) AND (FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR\") OR FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR REG\")) AND FILENAME_PLACEHOLDER_0.l_shipinstruct = CAST(Utf8(\"DELIVER IN PERSON\") AS Utf8) OR FILENAME_PLACEHOLDER_1.p_partkey = FILENAME_PLACEHOLDER_0.l_partkey AND FILENAME_PLACEHOLDER_1.p_brand = CAST(Utf8(\"Brand#23\") AS Utf8) AND (FILENAME_PLACEHOLDER_1.p_container = Utf8(\"MED BAG\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"MED BOX\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"MED PKG\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"MED PACK\")) AND FILENAME_PLACEHOLDER_0.l_quantity >= CAST(Int32(10) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_0.l_quantity <= CAST(Int32(10) + Int32(10) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_1.p_size >= Int32(1) AND FILENAME_PLACEHOLDER_1.p_size <= Int32(10) AND (FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR\") OR FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR REG\")) AND FILENAME_PLACEHOLDER_0.l_shipinstruct = CAST(Utf8(\"DELIVER IN PERSON\") AS Utf8) OR FILENAME_PLACEHOLDER_1.p_partkey = FILENAME_PLACEHOLDER_0.l_partkey AND FILENAME_PLACEHOLDER_1.p_brand = CAST(Utf8(\"Brand#34\") AS Utf8) AND (FILENAME_PLACEHOLDER_1.p_container = Utf8(\"LG CASE\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"LG BOX\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"LG PACK\") OR FILENAME_PLACEHOLDER_1.p_container = Utf8(\"LG PKG\")) AND FILENAME_PLACEHOLDER_0.l_quantity >= CAST(Int32(20) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_0.l_quantity <= CAST(Int32(20) + Int32(10) AS Decimal128(19, 0)) AND FILENAME_PLACEHOLDER_1.p_size >= Int32(1) AND FILENAME_PLACEHOLDER_1.p_size <= Int32(15) AND (FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR\") OR FILENAME_PLACEHOLDER_0.l_shipmode = Utf8(\"AIR REG\")) AND FILENAME_PLACEHOLDER_0.l_shipinstruct = CAST(Utf8(\"DELIVER IN PERSON\") AS Utf8)\
        \n      Inner Join:  Filter: Boolean(true)\
        \n        TableScan: FILENAME_PLACEHOLDER_0 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n        TableScan: FILENAME_PLACEHOLDER_1 projection=[p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_20() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/part.csv"),
            ("FILENAME_PLACEHOLDER_4", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_20.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.s_name AS S_NAME, FILENAME_PLACEHOLDER_0.s_address AS S_ADDRESS\
        \n  Sort: FILENAME_PLACEHOLDER_0.s_name ASC NULLS LAST\
        \n    Projection: FILENAME_PLACEHOLDER_0.s_name, FILENAME_PLACEHOLDER_0.s_address\
        \n      Filter: CAST(FILENAME_PLACEHOLDER_0.s_suppkey IN (<subquery>) AS Boolean) AND FILENAME_PLACEHOLDER_0.s_nationkey = FILENAME_PLACEHOLDER_1.n_nationkey AND FILENAME_PLACEHOLDER_1.n_name = CAST(Utf8(\"CANADA\") AS Utf8)\
        \n        Subquery:\
        \n          Projection: FILENAME_PLACEHOLDER_2.ps_suppkey\
        \n            Filter: CAST(FILENAME_PLACEHOLDER_2.ps_partkey IN (<subquery>) AS Boolean) AND CAST(FILENAME_PLACEHOLDER_2.ps_availqty AS Decimal128(19, 1)) > (<subquery>)\
        \n              Subquery:\
        \n                Projection: FILENAME_PLACEHOLDER_3.p_partkey\
        \n                  Filter: FILENAME_PLACEHOLDER_3.p_name LIKE CAST(Utf8(\"forest%\") AS Utf8)\
        \n                    TableScan: FILENAME_PLACEHOLDER_3 projection=[p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment]\
        \n              Subquery:\
        \n                Projection: Decimal128(Some(5),2,1) * sum(FILENAME_PLACEHOLDER_4.l_quantity)\
        \n                  Aggregate: groupBy=[[]], aggr=[[sum(FILENAME_PLACEHOLDER_4.l_quantity)]]\
        \n                    Projection: FILENAME_PLACEHOLDER_4.l_quantity\
        \n                      Filter: FILENAME_PLACEHOLDER_4.l_partkey = FILENAME_PLACEHOLDER_4.l_orderkey AND FILENAME_PLACEHOLDER_4.l_suppkey = FILENAME_PLACEHOLDER_4.l_partkey AND FILENAME_PLACEHOLDER_4.l_shipdate >= CAST(Utf8(\"1994-01-01\") AS Date32) AND FILENAME_PLACEHOLDER_4.l_shipdate < CAST(Utf8(\"1995-01-01\") AS Date32)\
        \n                        TableScan: FILENAME_PLACEHOLDER_4 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n              TableScan: FILENAME_PLACEHOLDER_2 projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment]\
        \n        Inner Join:  Filter: Boolean(true)\
        \n          TableScan: FILENAME_PLACEHOLDER_0 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n          TableScan: FILENAME_PLACEHOLDER_1 projection=[n_nationkey, n_name, n_regionkey, n_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_21() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/orders.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_4", "tests/testdata/tpch/lineitem.csv"),
            ("FILENAME_PLACEHOLDER_5", "tests/testdata/tpch/lineitem.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_21.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: FILENAME_PLACEHOLDER_0.s_name AS S_NAME, count(Int64(1)) AS NUMWAIT\
        \n  Limit: skip=0, fetch=100\
        \n    Sort: count(Int64(1)) DESC NULLS FIRST, FILENAME_PLACEHOLDER_0.s_name ASC NULLS LAST\
        \n      Aggregate: groupBy=[[FILENAME_PLACEHOLDER_0.s_name]], aggr=[[count(Int64(1))]]\
        \n        Projection: FILENAME_PLACEHOLDER_0.s_name\
        \n          Filter: FILENAME_PLACEHOLDER_0.s_suppkey = FILENAME_PLACEHOLDER_1.l_suppkey AND FILENAME_PLACEHOLDER_2.o_orderkey = FILENAME_PLACEHOLDER_1.l_orderkey AND FILENAME_PLACEHOLDER_2.o_orderstatus = Utf8(\"F\") AND FILENAME_PLACEHOLDER_1.l_receiptdate > FILENAME_PLACEHOLDER_1.l_commitdate AND EXISTS (<subquery>) AND NOT EXISTS (<subquery>) AND FILENAME_PLACEHOLDER_0.s_nationkey = FILENAME_PLACEHOLDER_3.n_nationkey AND FILENAME_PLACEHOLDER_3.n_name = CAST(Utf8(\"SAUDI ARABIA\") AS Utf8)\
        \n            Subquery:\
        \n              Filter: FILENAME_PLACEHOLDER_4.l_orderkey = FILENAME_PLACEHOLDER_4.l_tax AND FILENAME_PLACEHOLDER_4.l_suppkey != FILENAME_PLACEHOLDER_4.l_linestatus\
        \n                TableScan: FILENAME_PLACEHOLDER_4 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n            Subquery:\
        \n              Filter: FILENAME_PLACEHOLDER_5.l_orderkey = FILENAME_PLACEHOLDER_5.l_tax AND FILENAME_PLACEHOLDER_5.l_suppkey != FILENAME_PLACEHOLDER_5.l_linestatus AND FILENAME_PLACEHOLDER_5.l_receiptdate > FILENAME_PLACEHOLDER_5.l_commitdate\
        \n                TableScan: FILENAME_PLACEHOLDER_5 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\
        \n            Inner Join:  Filter: Boolean(true)\
        \n              Inner Join:  Filter: Boolean(true)\
        \n                Inner Join:  Filter: Boolean(true)\
        \n                  TableScan: FILENAME_PLACEHOLDER_0 projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment]\
        \n                  TableScan: FILENAME_PLACEHOLDER_1 projection=[l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment]\n                TableScan: FILENAME_PLACEHOLDER_2 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\
        \n              TableScan: FILENAME_PLACEHOLDER_3 projection=[n_nationkey, n_name, n_regionkey, n_comment]");
        Ok(())
    }

    #[tokio::test]
    async fn tpch_test_22() -> Result<()> {
        let ctx = create_context(vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/customer.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/orders.csv"),
        ])
        .await?;
        let path = "tests/testdata/tpch_substrait_plans/query_22.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: substr(FILENAME_PLACEHOLDER_0.c_phone,Int32(1),Int32(2)) AS CNTRYCODE, count(Int64(1)) AS NUMCUST, sum(FILENAME_PLACEHOLDER_0.c_acctbal) AS TOTACCTBAL\n  Sort: substr(FILENAME_PLACEHOLDER_0.c_phone,Int32(1),Int32(2)) ASC NULLS LAST\
        \n    Aggregate: groupBy=[[substr(FILENAME_PLACEHOLDER_0.c_phone,Int32(1),Int32(2))]], aggr=[[count(Int64(1)), sum(FILENAME_PLACEHOLDER_0.c_acctbal)]]\
        \n      Projection: substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)), FILENAME_PLACEHOLDER_0.c_acctbal\
        \n        Filter: (substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"13\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"31\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"23\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"29\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"30\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"18\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_0.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"17\") AS Utf8)) AND FILENAME_PLACEHOLDER_0.c_acctbal > (<subquery>) AND NOT EXISTS (<subquery>)\
        \n          Subquery:\
        \n            Aggregate: groupBy=[[]], aggr=[[avg(FILENAME_PLACEHOLDER_1.c_acctbal)]]\
        \n              Projection: FILENAME_PLACEHOLDER_1.c_acctbal\
        \n                Filter: FILENAME_PLACEHOLDER_1.c_acctbal > Decimal128(Some(0),3,2) AND (substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"13\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"31\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"23\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"29\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"30\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"18\") AS Utf8) OR substr(FILENAME_PLACEHOLDER_1.c_phone, Int32(1), Int32(2)) = CAST(Utf8(\"17\") AS Utf8))\
        \n                  TableScan: FILENAME_PLACEHOLDER_1 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]\n          Subquery:\
        \n            Filter: FILENAME_PLACEHOLDER_2.o_custkey = FILENAME_PLACEHOLDER_2.o_orderkey\
        \n              TableScan: FILENAME_PLACEHOLDER_2 projection=[o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment]\
        \n          TableScan: FILENAME_PLACEHOLDER_0 projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment]");
        Ok(())
    }
}
