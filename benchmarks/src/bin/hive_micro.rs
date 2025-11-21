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

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow::datatypes::{DataType, Schema};
use datafusion::{
    arrow::util::pretty::pretty_format_batches,
    dataframe::DataFrameWriteOptions,
    datasource::{
        file_format::parquet::ParquetFormat,
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::Result,
    prelude::*,
};
use datafusion_benchmarks::util::{BenchmarkRun, QueryResult};
use datafusion_common::DataFusionError;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "hive_micro",
    about = "Generate and benchmark Hive-style partitioned aggregates"
)]
enum Command {
    /// Build (or rebuild) the Hive micro dataset from TPCH data
    #[structopt(name = "setup")]
    Setup(SetupOpts),
    /// Run the Hive micro benchmark against an existing dataset
    #[structopt(name = "run")]
    Run(RunOpts),
}

#[derive(Debug, StructOpt)]
struct SetupOpts {
    /// Directory containing TPCH data (expects lineitem/ subdir with parquet files)
    #[structopt(long = "tpch-dir", parse(from_os_str))]
    tpch_dir: PathBuf,

    /// Directory where the Hive micro dataset will be written
    #[structopt(long = "hive-dir", parse(from_os_str))]
    hive_dir: PathBuf,

    /// Number of literal partitions (buckets)
    #[structopt(long = "bucket-count", default_value = "8")]
    bucket_count: usize,

    /// Optional number of rows to keep (useful for quick smoke tests)
    #[structopt(long = "limit")]
    limit: Option<usize>,
}

#[derive(Debug, StructOpt)]
struct RunOpts {
    /// Directory containing the Hive micro dataset (output of the setup command)
    #[structopt(long = "hive-dir", parse(from_os_str))]
    hive_dir: PathBuf,

    /// Number of literal partitions (buckets)
    #[structopt(long = "bucket-count", default_value = "8")]
    bucket_count: usize,

    /// Number of benchmark iterations per variant
    #[structopt(long = "iterations", default_value = "5")]
    iterations: usize,

    /// Target execution partitions to request from the session
    #[structopt(long = "target-partitions", default_value = "8")]
    target_partitions: usize,

    /// Optional JSON output file (same format as other benchmarks)
    #[structopt(long = "output", parse(from_os_str))]
    output: Option<PathBuf>,

    /// Optional directory where EXPLAIN ANALYZE plans will be written
    #[structopt(long = "explain", parse(from_os_str))]
    explain_dir: Option<PathBuf>,
}

struct QuerySpec {
    name: &'static str,
    build_sql: fn(&str) -> String,
}

const QUERY_SPECS: &[QuerySpec] = &[
    QuerySpec {
        name: "tpch_q1_like",
        build_sql: query_tpch_q1_like,
    },
    QuerySpec {
        name: "tpch_q3_like",
        build_sql: query_tpch_q3_like,
    },
    QuerySpec {
        name: "tpch_q5_like",
        build_sql: query_tpch_q5_like,
    },
    QuerySpec {
        name: "tpch_q7_like",
        build_sql: query_tpch_q7_like,
    },
    QuerySpec {
        name: "tpch_q8_like",
        build_sql: query_tpch_q8_like,
    },
    QuerySpec {
        name: "tpch_q10_like",
        build_sql: query_tpch_q10_like,
    },
    QuerySpec {
        name: "tpch_q12_like",
        build_sql: query_tpch_q12_like,
    },
    QuerySpec {
        name: "tpch_q14_like",
        build_sql: query_tpch_q14_like,
    },
    QuerySpec {
        name: "tpch_q18_like",
        build_sql: query_tpch_q18_like,
    },
    QuerySpec {
        name: "tpch_q21_like",
        build_sql: query_tpch_q21_like,
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    match Command::from_args() {
        Command::Setup(opts) => run_setup(opts).await,
        Command::Run(opts) => run_benchmark(opts).await,
    }
}

async fn run_setup(opts: SetupOpts) -> Result<()> {
    if opts.bucket_count == 0 {
        return Err(DataFusionError::Plan(
            "bucket-count must be greater than zero".to_string(),
        ));
    }

    if opts.hive_dir.exists() {
        fs::remove_dir_all(&opts.hive_dir)?;
    }
    fs::create_dir_all(&opts.hive_dir)?;

    let mut config = SessionConfig::new();
    config.options_mut().execution.keep_partition_by_columns = true;
    let ctx = SessionContext::new_with_config(config);
    let lineitem_path = opts.tpch_dir.join("lineitem");
    let lineitem_str = lineitem_path
        .to_str()
        .ok_or_else(|| DataFusionError::Plan("invalid TPCH path".to_string()))?;
    ctx.register_parquet("lineitem", lineitem_str, ParquetReadOptions::default())
        .await?;

    let mut query = format!(
        "SELECT \
            CAST(l_orderkey % {bucket} AS INT) AS bucket, \
            CAST(l_extendedprice AS DOUBLE) AS value, \
            CAST(l_quantity AS DOUBLE) AS quantity, \
            CAST(l_discount AS DOUBLE) AS discount, \
            CAST(l_shipdate AS TIMESTAMP) AS ship_date, \
            l_returnflag AS return_flag, \
            l_linestatus AS line_status \
         FROM lineitem",
        bucket = opts.bucket_count
    );
    if let Some(limit) = opts.limit {
        query.push_str(&format!(" LIMIT {}", limit));
    }

    let df = ctx.sql(&query).await?;
    let write_opts =
        DataFrameWriteOptions::new().with_partition_by(vec!["bucket".to_string()]);
    df.write_parquet(
        opts.hive_dir
            .to_str()
            .ok_or_else(|| DataFusionError::Plan("invalid hive dir".to_string()))?,
        write_opts,
        None,
    )
    .await?;

    println!("Hive micro dataset written to {}", opts.hive_dir.display());
    Ok(())
}

async fn run_benchmark(opts: RunOpts) -> Result<()> {
    if opts.bucket_count == 0 {
        return Err(DataFusionError::Plan(
            "bucket-count must be greater than zero".to_string(),
        ));
    }

    if opts.target_partitions == 0 {
        return Err(DataFusionError::Plan(
            "target-partitions must be greater than zero".to_string(),
        ));
    }

    if let Some(dir) = &opts.explain_dir {
        fs::create_dir_all(dir)?;
    }

    let mut benchmark = BenchmarkRun::new();

    for spec in QUERY_SPECS {
        benchmark.start_new_case(&format!("hive_micro_preserve__{}", spec.name));
        for _ in 0..opts.iterations {
            let result =
                execute_variant(&opts, true, "hive_micro_preserve", spec).await?;
            benchmark.write_iter(result.elapsed, result.row_count);
        }
        benchmark.start_new_case(&format!("hive_micro_plain__{}", spec.name));
        for _ in 0..opts.iterations {
            let result = execute_variant(&opts, false, "hive_micro_plain", spec).await?;
            benchmark.write_iter(result.elapsed, result.row_count);
        }
    }

    benchmark.maybe_write_json(opts.output.as_ref())?;
    benchmark.maybe_print_failures();
    Ok(())
}

async fn execute_variant(
    opts: &RunOpts,
    preserve: bool,
    variant_label: &str,
    spec: &QuerySpec,
) -> Result<QueryResult> {
    let config = SessionConfig::new().with_target_partitions(opts.target_partitions);
    let ctx = SessionContext::new_with_config(config);
    let table_name = if preserve {
        "hive_micro_fact"
    } else {
        "hive_micro_fact_plain"
    };

    register_table(
        &ctx,
        table_name,
        &opts.hive_dir,
        preserve,
        opts.bucket_count,
    )
    .await?;

    let sql = (spec.build_sql)(table_name);
    let df = ctx.sql(&sql).await?;

    if let Some(dir) = &opts.explain_dir {
        let explain_df = df.clone().explain(true, true)?;
        let batches = explain_df.collect().await?;
        let content = pretty_format_batches(&batches)?.to_string();
        let file = dir.join(format!("{}_{}.plan", variant_label, spec.name));
        fs::write(file, content)?;
    }

    let start = Instant::now();
    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    let row_count = batches.iter().map(|b| b.num_rows()).sum();

    Ok(QueryResult { elapsed, row_count })
}

async fn register_table(
    ctx: &SessionContext,
    name: &str,
    hive_dir: &Path,
    preserve_partition_values: bool,
    target_partitions: usize,
) -> Result<()> {
    let format = Arc::new(ParquetFormat::default());
    let mut options = ListingOptions::new(format)
        .with_target_partitions(target_partitions.max(1))
        .with_collect_stat(false);

    if preserve_partition_values {
        options = options
            .with_table_partition_cols(vec![("bucket".to_string(), DataType::Int32)])
            .with_preserve_partition_values(true);
    } else {
        options = options.with_preserve_partition_values(false);
    }

    let url = ListingTableUrl::parse(
        hive_dir
            .to_str()
            .ok_or_else(|| DataFusionError::Plan("invalid hive dir".to_string()))?,
    )?;

    let state = ctx.state();
    let schema = options.infer_schema(&state, &url).await?;
    let schema = if preserve_partition_values {
        let fields: Vec<_> = schema
            .fields()
            .iter()
            .filter(|f| f.name() != "bucket")
            .cloned()
            .collect();
        Arc::new(Schema::new(fields))
    } else {
        schema
    };
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?;
    ctx.register_table(name, Arc::new(table))?;
    Ok(())
}

fn query_tpch_q1_like(table: &str) -> String {
    format!(
        "SELECT \
            return_flag, \
            line_status, \
            SUM(quantity) AS sum_qty, \
            SUM(value) AS sum_base_price, \
            SUM(value * (1 - discount)) AS sum_disc_price, \
            SUM(value * (1 - discount) * (1 + 0.07)) AS sum_charge, \
            AVG(quantity) AS avg_qty, \
            AVG(value) AS avg_price, \
            AVG(discount) AS avg_disc, \
            COUNT(*) AS count_order \
         FROM {table} \
        WHERE ship_date <= to_timestamp('1998-09-02T00:00:00Z') \
         GROUP BY return_flag, line_status \
         ORDER BY return_flag, line_status",
        table = table
    )
}

fn query_tpch_q3_like(table: &str) -> String {
    format!(
        "SELECT \
            bucket, \
            date_trunc('month', ship_date) AS month, \
            SUM(value * (1 - discount)) AS revenue \
         FROM {table} \
         WHERE ship_date BETWEEN to_timestamp('1995-01-01T00:00:00Z') \
                             AND to_timestamp('1996-12-31T23:59:59Z') \
         GROUP BY bucket, month \
         ORDER BY revenue DESC \
         LIMIT 50",
        table = table
    )
}

fn query_tpch_q5_like(table: &str) -> String {
    format!(
        "SELECT \
            return_flag, \
            SUM(value * (1 - discount)) AS revenue \
         FROM {table} \
         WHERE ship_date BETWEEN to_timestamp('1993-01-01T00:00:00Z') \
                             AND to_timestamp('1995-12-31T23:59:59Z') \
         GROUP BY return_flag \
         ORDER BY revenue DESC",
        table = table
    )
}

fn query_tpch_q7_like(table: &str) -> String {
    format!(
        "SELECT \
            bucket, \
            return_flag, \
            line_status, \
            SUM(quantity) AS sum_qty \
         FROM {table} \
         WHERE ship_date BETWEEN to_timestamp('1995-01-01T00:00:00Z') \
                             AND to_timestamp('1995-12-31T23:59:59Z') \
         GROUP BY bucket, return_flag, line_status \
         ORDER BY bucket, return_flag, line_status \
         LIMIT 200",
        table = table
    )
}

fn query_tpch_q8_like(table: &str) -> String {
    format!(
        "WITH monthly AS ( \
            SELECT \
                date_trunc('month', ship_date) AS month, \
                SUM(CASE WHEN bucket % 2 = 0 THEN value * (1 - discount) ELSE 0 END) AS even_bucket_rev, \
                SUM(value * (1 - discount)) AS total_rev \
            FROM {table} \
            WHERE ship_date >= to_timestamp('1995-01-01T00:00:00Z') \
            GROUP BY month \
         ) \
         SELECT \
            month, \
            even_bucket_rev, \
            total_rev, \
            CASE WHEN total_rev = 0 THEN NULL ELSE even_bucket_rev / total_rev END AS even_ratio \
         FROM monthly \
         ORDER BY month",
        table = table
    )
}

fn query_tpch_q10_like(table: &str) -> String {
    format!(
        "SELECT \
            bucket, \
            SUM(value * (1 - discount)) AS customer_revenue, \
            AVG(discount) AS avg_discount \
         FROM {table} \
         WHERE ship_date >= to_timestamp('1993-10-01T00:00:00Z') \
           AND ship_date < to_timestamp('1994-01-01T00:00:00Z') \
         GROUP BY bucket \
         ORDER BY customer_revenue DESC \
         LIMIT 20",
        table = table
    )
}

fn query_tpch_q12_like(table: &str) -> String {
    format!(
        "SELECT \
            return_flag, \
            SUM(CASE WHEN ship_date < to_timestamp('1994-06-01T00:00:00Z') THEN 1 ELSE 0 END) AS high_line_count, \
            SUM(CASE WHEN ship_date >= to_timestamp('1994-06-01T00:00:00Z') THEN 1 ELSE 0 END) AS low_line_count \
         FROM {table} \
         WHERE ship_date BETWEEN to_timestamp('1993-01-01T00:00:00Z') \
                             AND to_timestamp('1994-12-31T23:59:59Z') \
         GROUP BY return_flag \
         ORDER BY return_flag",
        table = table
    )
}

fn query_tpch_q14_like(table: &str) -> String {
    format!(
        "SELECT \
            date_trunc('month', ship_date) AS month, \
            100 * SUM(CASE WHEN bucket % 5 = 0 THEN value * (1 - discount) ELSE 0 END) \
                / NULLIF(SUM(value * (1 - discount)), 0) AS promo_revenue \
         FROM {table} \
         WHERE ship_date BETWEEN to_timestamp('1995-01-01T00:00:00Z') \
                             AND to_timestamp('1995-12-31T23:59:59Z') \
         GROUP BY month \
         ORDER BY month",
        table = table
    )
}

fn query_tpch_q18_like(table: &str) -> String {
    format!(
        "SELECT \
            bucket, \
            SUM(value) AS sum_value, \
            SUM(quantity) AS sum_quantity \
         FROM {table} \
         WHERE quantity > 25 \
         GROUP BY bucket \
         HAVING SUM(quantity) > 200 \
         ORDER BY sum_value DESC \
         LIMIT 30",
        table = table
    )
}

fn query_tpch_q21_like(table: &str) -> String {
    format!(
        "SELECT \
            bucket, \
            SUM(CASE WHEN discount < 0.03 THEN 1 ELSE 0 END) AS low_discount_ops, \
            SUM(CASE WHEN discount BETWEEN 0.03 AND 0.07 THEN 1 ELSE 0 END) AS medium_discount_ops, \
            SUM(CASE WHEN discount > 0.07 THEN 1 ELSE 0 END) AS high_discount_ops, \
            COUNT(*) AS total_ops \
         FROM {table} \
         GROUP BY bucket \
         ORDER BY bucket",
        table = table
    )
}
