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

    async fn register_csv(
        ctx: &SessionContext,
        table_name: &str,
        file_path: &str,
    ) -> Result<()> {
        ctx.register_csv(table_name, file_path, CsvReadOptions::default())
            .await
    }

    async fn create_context_tpch2() -> Result<SessionContext> {
        let ctx = SessionContext::new();

        let registrations = vec![
            ("FILENAME_PLACEHOLDER_0", "tests/testdata/tpch/part.csv"),
            ("FILENAME_PLACEHOLDER_1", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_2", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_3", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_4", "tests/testdata/tpch/region.csv"),
            ("FILENAME_PLACEHOLDER_5", "tests/testdata/tpch/partsupp.csv"),
            ("FILENAME_PLACEHOLDER_6", "tests/testdata/tpch/supplier.csv"),
            ("FILENAME_PLACEHOLDER_7", "tests/testdata/tpch/nation.csv"),
            ("FILENAME_PLACEHOLDER_8", "tests/testdata/tpch/region.csv"),
        ];

        for (table_name, file_path) in registrations {
            register_csv(&ctx, table_name, file_path).await?;
        }

        Ok(ctx)
    }

    async fn create_context_tpch1() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        register_csv(
            &ctx,
            "FILENAME_PLACEHOLDER_0",
            "tests/testdata/tpch/lineitem.csv",
        )
        .await?;
        Ok(ctx)
    }

    #[tokio::test]
    async fn tpch_test_1() -> Result<()> {
        let ctx = create_context_tpch1().await?;
        let path = "tests/testdata/tpch_substrait_plans/query_1.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        let plan_str = format!("{:?}", plan);
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
        let ctx = create_context_tpch2().await?;
        let path = "tests/testdata/tpch_substrait_plans/query_2.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{:?}", plan);
        assert_eq!(
            plan_str,
            "Projection: FILENAME_PLACEHOLDER_1.s_acctbal AS S_ACCTBAL, FILENAME_PLACEHOLDER_1.s_name AS S_NAME, FILENAME_PLACEHOLDER_3.n_name AS N_NAME, FILENAME_PLACEHOLDER_0.p_partkey AS P_PARTKEY, FILENAME_PLACEHOLDER_0.p_mfgr AS P_MFGR, FILENAME_PLACEHOLDER_1.s_address AS S_ADDRESS, FILENAME_PLACEHOLDER_1.s_phone AS S_PHONE, FILENAME_PLACEHOLDER_1.s_comment AS S_COMMENT\
            \n  Limit: skip=0, fetch=100\
            \n    Sort: FILENAME_PLACEHOLDER_1.s_acctbal DESC NULLS FIRST, FILENAME_PLACEHOLDER_3.n_name ASC NULLS LAST, FILENAME_PLACEHOLDER_1.s_name ASC NULLS LAST, FILENAME_PLACEHOLDER_0.p_partkey ASC NULLS LAST\
            \n      Projection: FILENAME_PLACEHOLDER_1.s_acctbal, FILENAME_PLACEHOLDER_1.s_name, FILENAME_PLACEHOLDER_3.n_name, FILENAME_PLACEHOLDER_0.p_partkey, FILENAME_PLACEHOLDER_0.p_mfgr, FILENAME_PLACEHOLDER_1.s_address, FILENAME_PLACEHOLDER_1.s_phone, FILENAME_PLACEHOLDER_1.s_comment\
            \n        Filter: FILENAME_PLACEHOLDER_0.p_partkey = FILENAME_PLACEHOLDER_2.ps_partkey AND FILENAME_PLACEHOLDER_1.s_suppkey = FILENAME_PLACEHOLDER_2.ps_suppkey AND FILENAME_PLACEHOLDER_0.p_size = Int32(15) AND FILENAME_PLACEHOLDER_0.p_type LIKE CAST(Utf8(\"%BRASS\") AS Utf8) AND FILENAME_PLACEHOLDER_1.s_nationkey = FILENAME_PLACEHOLDER_3.n_nationkey AND FILENAME_PLACEHOLDER_3.n_regionkey = FILENAME_PLACEHOLDER_4.r_regionkey AND FILENAME_PLACEHOLDER_4.r_name = CAST(Utf8(\"EUROPE\") AS Utf8) AND FILENAME_PLACEHOLDER_2.ps_supplycost = (<subquery>)\
            \n          Subquery:\
            \n            Aggregate: groupBy=[[]], aggr=[[MIN(FILENAME_PLACEHOLDER_5.ps_supplycost)]]\
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
}
