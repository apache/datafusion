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

extern crate arrow;
extern crate criterion;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow_array::ArrayRef;
use arrow_array::Float64Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_schema::ArrowError;
use arrow_schema::Schema;
use criterion::criterion_group;
use criterion::criterion_main;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::ParquetReadOptions;
use parking_lot::Mutex;
use parquet::arrow::ArrowWriter;
use rand::distributions::Uniform;
use rand_distr::Distribution;
use std::sync::Arc;
use tempfile::Builder;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

const BLOOM_FILTER_JOIN_SIZE: usize = 10000000;

/// data used for bloom filter join

fn create_parquet_tempfile(
    data: &[Vec<String>],
    schema: Arc<Schema>,
) -> Result<NamedTempFile, ArrowError> {
    let mut temp_file = Builder::new().suffix(".parquet").tempfile()?;
    {
        let mut writer =
            ArrowWriter::try_new(temp_file.as_file_mut(), schema.clone(), None)?;

        // Convert the data to a RecordBatch
        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| match field.data_type() {
                DataType::Int64 => {
                    let column_data: Vec<i64> =
                        data.iter().map(|row| row[i].parse().unwrap()).collect();
                    Arc::new(Int64Array::from(column_data)) as ArrayRef
                }
                DataType::Float64 => {
                    let column_data: Vec<f64> =
                        data.iter().map(|row| row[i].parse().unwrap()).collect();
                    Arc::new(Float64Array::from(column_data)) as ArrayRef
                }
                DataType::Utf8 => {
                    let column_data: Vec<&str> =
                        data.iter().map(|row| row[i].as_str()).collect();
                    Arc::new(StringArray::from(column_data)) as ArrayRef
                }
                _ => unimplemented!(),
            })
            .collect();

        let batch = RecordBatch::try_new(schema, columns)?;

        // Write the RecordBatch to the Parquet file
        writer.write(&batch)?;
        // Ensure the writer is closed properly
        writer.close()?;
    }
    Ok(temp_file)
}

fn create_tpch_q17_lineitem_parquet() -> Result<NamedTempFile, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false), // 添加 l_discount 字段
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
    ]));

    let shipmodes = ["AIR", "RAIL", "SHIP", "TRUCK"];
    let shipinstructs = [
        "DELIVER IN PERSON",
        "NONE",
        "TAKE BACK RETURN",
        "COLLECT COD",
    ];

    let data: Vec<Vec<String>> = (0..BLOOM_FILTER_JOIN_SIZE)
        .map(|v| {
            vec![
                (v % 10).to_string(),                                // l_partkey
                (v % 100).to_string(),                               // l_suppkey
                (v % 50).to_string(),                                // l_quantity
                (10000.0 + (v % 50) as f64 * 1.1).to_string(),       // l_extendedprice
                (v as f64 % 0.1).to_string(), // l_discount (模拟折扣)
                shipmodes[v as usize % shipmodes.len()].to_string(), // l_shipmode
                shipinstructs[v as usize % shipinstructs.len()].to_string(), // l_shipinstruct
            ]
        })
        .collect();

    create_parquet_tempfile(&data, schema)
}

fn create_tpch_q17_part_parquet() -> Result<NamedTempFile, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_container", DataType::Utf8, false),
    ]));

    let brands = ["Brand#12", "Brand#23", "Brand#34", "Brand#45"];
    let containers = ["SM BOX", "MED BOX", "LG BOX", "SM PKG", "MED PKG", "LG PKG"];

    let mut rng = rand::thread_rng();
    let brand_dist = Uniform::from(0..brands.len());
    let container_dist = Uniform::from(0..containers.len());

    let data: Vec<Vec<String>> = (0..BLOOM_FILTER_JOIN_SIZE)
        .map(|v| {
            vec![
                v.to_string(),
                brands[brand_dist.sample(&mut rng)].to_string(),
                containers[container_dist.sample(&mut rng)].to_string(),
            ]
        })
        .collect();

    create_parquet_tempfile(&data, schema)
}

fn create_partsupp_parquet() -> Result<NamedTempFile, ArrowError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Float64, false),
    ]));

    let data: Vec<Vec<String>> = (0..BLOOM_FILTER_JOIN_SIZE)
        .map(|v| {
            vec![
                (v % 10).to_string(),     // ps_partkey: match partkey
                (v % 100).to_string(),    // ps_suppkey: simulate supplier keys
                (v % 50 + 1).to_string(), // ps_availqty: simulate available quantity
                (100.0 + (v % 20) as f64 * 1.1).to_string(), // ps_supplycost: simulate supply cost
            ]
        })
        .collect();

    create_parquet_tempfile(&data, schema)
}

fn query(ctx: Arc<Mutex<SessionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context_with_parquet_tpch_17(
    part_file: &NamedTempFile,
    lineitem_file: &NamedTempFile,
    partsupp_file: &NamedTempFile,
) -> Arc<Mutex<SessionContext>> {
    let rt = Runtime::new().unwrap();
    let ctx = SessionContext::new();

    let part_schema = Arc::new(Schema::new(vec![
        Field::new("p_partkey", DataType::Int64, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_container", DataType::Utf8, false),
    ]));

    let lineitem_schema = Arc::new(Schema::new(vec![
        Field::new("l_partkey", DataType::Int64, false),
        Field::new("l_suppkey", DataType::Int64, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),
    ]));

    let partsupp_schema = Arc::new(Schema::new(vec![
        Field::new("ps_partkey", DataType::Int64, false),
        Field::new("ps_suppkey", DataType::Int64, false),
        Field::new("ps_availqty", DataType::Int64, false),
        Field::new("ps_supplycost", DataType::Float64, false),
    ]));

    rt.block_on(async {
        ctx.register_parquet(
            "part",
            part_file.path().to_str().unwrap(),
            ParquetReadOptions::default().schema(&part_schema),
        )
        .await
        .unwrap_or_else(|err| {
            eprintln!("Failed to register 'part' parquet file: {}", err);
            std::process::exit(1);
        });

        ctx.register_parquet(
            "lineitem",
            lineitem_file.path().to_str().unwrap(),
            ParquetReadOptions::default().schema(&lineitem_schema),
        )
        .await
        .unwrap_or_else(|err| {
            eprintln!("Failed to register 'lineitem' parquet file: {}", err);
            std::process::exit(1);
        });

        ctx.register_parquet(
            "partsupp",
            partsupp_file.path().to_str().unwrap(),
            ParquetReadOptions::default().schema(&partsupp_schema),
        )
        .await
        .unwrap_or_else(|err| {
            eprintln!("Failed to register 'partsupp' parquet file: {}", err);
            std::process::exit(1);
        });
    });

    Arc::new(Mutex::new(ctx))
}

fn criterion_benchmark(c: &mut Criterion) {
    let part_file = create_tpch_q17_part_parquet().unwrap();
    let lineitem_file = create_tpch_q17_lineitem_parquet().unwrap();
    let partsupp_file = create_partsupp_parquet().unwrap();
    let tpch_17_ctx =
        create_context_with_parquet_tpch_17(&part_file, &lineitem_file, &partsupp_file);
    c.bench_function("TPCH Q17", |b| {
        b.iter(|| {
            query(
                tpch_17_ctx.clone(),
                "SELECT
                    SUM(l_extendedprice) / 7.0 AS avg_yearly
                 FROM
                    part, lineitem
                 WHERE
                    p_partkey = l_partkey
                    AND p_brand = 'Brand#23'
                    AND p_container = 'MED BOX'
                    AND l_quantity < (
                        SELECT 0.2 * AVG(l_quantity)
                        FROM lineitem
                        WHERE l_partkey = p_partkey
                    )",
            )
        })
    });

    c.bench_function("Complex Join with Subqueries and Aggregations", |b| {
        b.iter(|| {
            query(
                tpch_17_ctx.clone(),
                "SELECT 
                    p.p_partkey, 
                    SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
                    SUM(ps.ps_supplycost * ps.ps_availqty) AS total_cost
                FROM 
                    part p
                JOIN 
                    lineitem l ON p.p_partkey = l.l_partkey
                JOIN 
                    partsupp ps ON p.p_partkey = ps.ps_partkey AND l.l_suppkey = ps.ps_suppkey
                WHERE 
                    p.p_brand = 'Brand#45'
                    AND p.p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                    AND l.l_quantity >= 5 AND l.l_quantity <= 15
                    AND l.l_shipmode IN ('AIR', 'AIR REG')
                    AND l.l_shipinstruct = 'DELIVER IN PERSON'
                    AND ps.ps_availqty > (
                        SELECT 
                            0.5 * SUM(l2.l_quantity)
                        FROM 
                            lineitem l2
                        WHERE 
                            l2.l_partkey = p.p_partkey
                            AND l2.l_suppkey = ps.ps_suppkey
                    )
                GROUP BY 
                    p.p_partkey
                HAVING 
                    SUM(l.l_extendedprice * (1 - l.l_discount)) > 1000000
                ORDER BY 
                    revenue DESC",
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
