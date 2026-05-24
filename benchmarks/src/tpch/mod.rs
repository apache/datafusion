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

//! Benchmark derived from TPC-H. This is not an official TPC-H benchmark.

use arrow::datatypes::SchemaBuilder;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::plan_err,
    error::Result,
};
use std::fs;
mod run;
pub use run::RunOpt;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

pub const TPCH_QUERY_START_ID: usize = 1;
pub const TPCH_QUERY_END_ID: usize = 22;
const TPCH_Q11_FRACTION_SENTINEL: &str = "0.0001 /* __TPCH_Q11_FRACTION__ */";

/// The `.tbl` file contains a trailing column
pub fn get_tbl_tpch_table_schema(table: &str) -> Schema {
    let mut schema = SchemaBuilder::from(get_tpch_table_schema(table).fields);
    schema.push(Field::new("__placeholder", DataType::Utf8, false));
    schema.finish()
}

/// Get the schema for the benchmarks derived from TPC-H
pub fn get_tpch_table_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<Vec<String>> {
    get_query_sql_for_scale_factor(query, 1.0)
}

/// Get the SQL statements from the specified query file using the provided scale factor for
/// TPC-H substitutions such as Q11 FRACTION.
pub fn get_query_sql_for_scale_factor(
    query: usize,
    scale_factor: f64,
) -> Result<Vec<String>> {
    if !(scale_factor.is_finite() && scale_factor > 0.0) {
        return plan_err!(
            "invalid scale factor. Expected a positive finite value, got {scale_factor}"
        );
    }

    if query > 0 && query < 23 {
        let possibilities = vec![
            format!("queries/q{query}.sql"),
            format!("benchmarks/queries/q{query}.sql"),
        ];
        let mut errors = vec![];
        for filename in possibilities {
            match fs::read_to_string(&filename) {
                Ok(contents) => {
                    let contents = customize_query_sql(query, contents, scale_factor)?;
                    return Ok(contents
                        .split(';')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect());
                }
                Err(e) => errors.push(format!("{filename}: {e}")),
            };
        }
        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 22")
    }
}

fn customize_query_sql(
    query: usize,
    contents: String,
    scale_factor: f64,
) -> Result<String> {
    if query != 11 {
        return Ok(contents);
    }

    if !contents.contains(TPCH_Q11_FRACTION_SENTINEL) {
        return plan_err!(
            "invalid query 11. Missing fraction marker {TPCH_Q11_FRACTION_SENTINEL}"
        );
    }

    Ok(contents.replace(
        TPCH_Q11_FRACTION_SENTINEL,
        &format!("(0.0001 / {scale_factor})"),
    ))
}

pub const QUERY_LIMIT: [Option<usize>; 22] = [
    None,
    Some(100),
    Some(10),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(20),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(100),
    None,
    None,
    Some(100),
    None,
];

#[cfg(test)]
mod tests {
    use super::{get_query_sql, get_query_sql_for_scale_factor};
    use datafusion::error::Result;

    fn get_single_query(query: usize) -> Result<String> {
        let mut queries = get_query_sql(query)?;
        assert_eq!(queries.len(), 1);
        Ok(queries.remove(0))
    }

    fn get_single_query_for_scale_factor(
        query: usize,
        scale_factor: f64,
    ) -> Result<String> {
        let mut queries = get_query_sql_for_scale_factor(query, scale_factor)?;
        assert_eq!(queries.len(), 1);
        Ok(queries.remove(0))
    }

    #[test]
    fn q11_uses_scale_factor_substitution() -> Result<()> {
        let sf1_sql = get_single_query(11)?;
        assert!(sf1_sql.contains("(0.0001 / 1)"));

        let sf01_sql = get_single_query_for_scale_factor(11, 0.1)?;
        assert!(sf01_sql.contains("(0.0001 / 0.1)"));

        let sf10_sql = get_single_query_for_scale_factor(11, 10.0)?;
        assert!(sf10_sql.contains("(0.0001 / 10)"));

        let sf30_sql = get_single_query_for_scale_factor(11, 30.0)?;
        assert!(sf30_sql.contains("(0.0001 / 30)"));
        assert!(!sf10_sql.contains("__TPCH_Q11_FRACTION__"));
        Ok(())
    }

    #[test]
    fn interval_queries_use_interval_arithmetic() -> Result<()> {
        assert!(get_single_query(5)?.contains("date '1994-01-01' + interval '1' year"));
        assert!(get_single_query(6)?.contains("date '1994-01-01' + interval '1' year"));
        assert!(get_single_query(10)?.contains("date '1993-10-01' + interval '3' month"));
        assert!(get_single_query(12)?.contains("date '1994-01-01' + interval '1' year"));
        assert!(get_single_query(14)?.contains("date '1995-09-01' + interval '1' month"));
        Ok(())
    }
}
