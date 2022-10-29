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

use std::fs;
use std::path::Path;
use std::time::Instant;

use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::{DataFusionError, Result},
    prelude::*,
};

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

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

/// Get the expected schema for the results of a query
pub fn get_answer_schema(n: usize) -> Schema {
    match n {
        1 => Schema::new(vec![
            Field::new("l_returnflag", DataType::Utf8, true),
            Field::new("l_linestatus", DataType::Utf8, true),
            Field::new("sum_qty", DataType::Decimal128(15, 2), true),
            Field::new("sum_base_price", DataType::Decimal128(15, 2), true),
            Field::new("sum_disc_price", DataType::Decimal128(15, 2), true),
            Field::new("sum_charge", DataType::Decimal128(15, 2), true),
            Field::new("avg_qty", DataType::Decimal128(15, 2), true),
            Field::new("avg_price", DataType::Decimal128(15, 2), true),
            Field::new("avg_disc", DataType::Decimal128(15, 2), true),
            Field::new("count_order", DataType::Int64, true),
        ]),

        2 => Schema::new(vec![
            Field::new("s_acctbal", DataType::Decimal128(15, 2), true),
            Field::new("s_name", DataType::Utf8, true),
            Field::new("n_name", DataType::Utf8, true),
            Field::new("p_partkey", DataType::Int64, true),
            Field::new("p_mfgr", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
            Field::new("s_phone", DataType::Utf8, true),
            Field::new("s_comment", DataType::Utf8, true),
        ]),

        3 => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
            Field::new("o_orderdate", DataType::Date32, true),
            Field::new("o_shippriority", DataType::Int32, true),
        ]),

        4 => Schema::new(vec![
            Field::new("o_orderpriority", DataType::Utf8, true),
            Field::new("order_count", DataType::Int64, true),
        ]),

        5 => Schema::new(vec![
            Field::new("n_name", DataType::Utf8, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
        ]),

        6 => Schema::new(vec![Field::new(
            "revenue",
            DataType::Decimal128(15, 2),
            true,
        )]),

        7 => Schema::new(vec![
            Field::new("supp_nation", DataType::Utf8, true),
            Field::new("cust_nation", DataType::Utf8, true),
            Field::new("l_year", DataType::Int32, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
        ]),

        8 => Schema::new(vec![
            Field::new("o_year", DataType::Int32, true),
            Field::new("mkt_share", DataType::Decimal128(15, 2), true),
        ]),

        9 => Schema::new(vec![
            Field::new("nation", DataType::Utf8, true),
            Field::new("o_year", DataType::Int32, true),
            Field::new("sum_profit", DataType::Decimal128(15, 2), true),
        ]),

        10 => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, true),
            Field::new("c_name", DataType::Utf8, true),
            Field::new("revenue", DataType::Decimal128(15, 2), true),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), true),
            Field::new("n_name", DataType::Utf8, true),
            Field::new("c_address", DataType::Utf8, true),
            Field::new("c_phone", DataType::Utf8, true),
            Field::new("c_comment", DataType::Utf8, true),
        ]),

        11 => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, true),
            Field::new("value", DataType::Decimal128(15, 2), true),
        ]),

        12 => Schema::new(vec![
            Field::new("l_shipmode", DataType::Utf8, true),
            Field::new("high_line_count", DataType::Int64, true),
            Field::new("low_line_count", DataType::Int64, true),
        ]),

        13 => Schema::new(vec![
            Field::new("c_count", DataType::Int64, true),
            Field::new("custdist", DataType::Int64, true),
        ]),

        14 => Schema::new(vec![Field::new("promo_revenue", DataType::Float64, true)]),

        15 => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, true),
            Field::new("s_name", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
            Field::new("s_phone", DataType::Utf8, true),
            Field::new("total_revenue", DataType::Decimal128(15, 2), true),
        ]),

        16 => Schema::new(vec![
            Field::new("p_brand", DataType::Utf8, true),
            Field::new("p_type", DataType::Utf8, true),
            Field::new("p_size", DataType::Int32, true),
            Field::new("supplier_cnt", DataType::Int64, true),
        ]),

        17 => Schema::new(vec![Field::new("avg_yearly", DataType::Float64, true)]),

        18 => Schema::new(vec![
            Field::new("c_name", DataType::Utf8, true),
            Field::new("c_custkey", DataType::Int64, true),
            Field::new("o_orderkey", DataType::Int64, true),
            Field::new("o_orderdate", DataType::Date32, true),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), true),
            Field::new("sum_l_quantity", DataType::Decimal128(15, 2), true),
        ]),

        19 => Schema::new(vec![Field::new(
            "revenue",
            DataType::Decimal128(15, 2),
            true,
        )]),

        20 => Schema::new(vec![
            Field::new("s_name", DataType::Utf8, true),
            Field::new("s_address", DataType::Utf8, true),
        ]),

        21 => Schema::new(vec![
            Field::new("s_name", DataType::Utf8, true),
            Field::new("numwait", DataType::Int64, true),
        ]),

        22 => Schema::new(vec![
            Field::new("cntrycode", DataType::Utf8, true),
            Field::new("numcust", DataType::Int64, true),
            Field::new("totacctbal", DataType::Decimal128(15, 2), true),
        ]),

        _ => unimplemented!(),
    }
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 23 {
        let possibilities = vec![
            format!("queries/q{}.sql", query),
            format!("benchmarks/queries/q{}.sql", query),
        ];
        let mut errors = vec![];
        for filename in possibilities {
            match fs::read_to_string(&filename) {
                Ok(contents) => {
                    return Ok(contents
                        .split(';')
                        .map(|s| s.trim())
                        .filter(|s| !s.is_empty())
                        .map(|s| s.to_string())
                        .collect());
                }
                Err(e) => errors.push(format!("{}: {}", filename, e)),
            };
        }
        Err(DataFusionError::Plan(format!(
            "invalid query. Could not find query: {:?}",
            errors
        )))
    } else {
        Err(DataFusionError::Plan(
            "invalid query. Expected value between 1 and 22".to_owned(),
        ))
    }
}

/// Conver tbl (csv) file to parquet
pub async fn convert_tbl(
    input_path: &str,
    output_path: &str,
    file_format: &str,
    partitions: usize,
    batch_size: usize,
    compression: Compression,
) -> Result<()> {
    let output_root_path = Path::new(output_path);
    for table in TPCH_TABLES {
        let start = Instant::now();
        let schema = get_tpch_table_schema(table);

        let input_path = format!("{}/{}.tbl", input_path, table);
        let options = CsvReadOptions::new()
            .schema(&schema)
            .has_header(false)
            .delimiter(b'|')
            .file_extension(".tbl");

        let config = SessionConfig::new().with_batch_size(batch_size);
        let ctx = SessionContext::with_config(config);

        // build plan to read the TBL file
        let mut csv = ctx.read_csv(&input_path, options).await?;

        // optionally, repartition the file
        if partitions > 1 {
            csv = csv.repartition(Partitioning::RoundRobinBatch(partitions))?
        }

        // create the physical plan
        let csv = csv.to_logical_plan()?;
        let csv = ctx.create_physical_plan(&csv).await?;

        let output_path = output_root_path.join(table);
        let output_path = output_path.to_str().unwrap().to_owned();

        println!(
            "Converting '{}' to {} files in directory '{}'",
            &input_path, &file_format, &output_path
        );
        match file_format {
            "csv" => ctx.write_csv(csv, output_path).await?,
            "parquet" => {
                let props = WriterProperties::builder()
                    .set_compression(compression)
                    .build();
                ctx.write_parquet(csv, output_path, Some(props)).await?
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Invalid output format: {}",
                    other
                )));
            }
        }
        println!("Conversion completed in {} ms", start.elapsed().as_millis());
    }

    Ok(())
}
