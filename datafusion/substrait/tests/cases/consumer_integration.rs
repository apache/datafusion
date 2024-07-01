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

    #[tokio::test]
    async fn tpch_test_1() -> Result<()> {
        let ctx = create_context().await?;
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

    async fn create_context() -> datafusion::common::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv(
            "FILENAME_PLACEHOLDER_0",
            "tests/testdata/tpch/lineitem.csv",
            CsvReadOptions::default(),
        )
        .await?;
        Ok(ctx)
    }
}
