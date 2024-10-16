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

//! DataFusion h2o benchmarks

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::MemTable;
use datafusion::prelude::CsvReadOptions;
use datafusion::{arrow::util::pretty, error::Result, prelude::SessionContext};
use datafusion_benchmarks::util::BenchmarkRun;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::time::Instant;

#[derive(Debug, StructOpt)]
#[structopt(name = "datafusion-h2o", about = "DataFusion h2o benchmarks")]
enum Opt {
    GroupBy(GroupBy), //TODO add Join queries
}

#[derive(Debug, StructOpt)]
struct GroupBy {
    /// Query number
    #[structopt(short, long)]
    query: usize,
    /// Path to data file
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,
    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,
    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,
    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    println!("Running benchmarks with the following options: {opt:?}");
    match opt {
        Opt::GroupBy(config) => group_by(&config).await,
    }
}

async fn group_by(opt: &GroupBy) -> Result<()> {
    let mut rundata = BenchmarkRun::new();
    let path = opt.path.to_str().unwrap();
    let mut config = ConfigOptions::from_env()?;
    config.execution.batch_size = 65535;

    let ctx = SessionContext::new_with_config(config.into());

    let schema = Schema::new(vec![
        Field::new("id1", DataType::Utf8, false),
        Field::new("id2", DataType::Utf8, false),
        Field::new("id3", DataType::Utf8, false),
        Field::new("id4", DataType::Int32, false),
        Field::new("id5", DataType::Int32, false),
        Field::new("id6", DataType::Int32, false),
        Field::new("v1", DataType::Int32, false),
        Field::new("v2", DataType::Int32, false),
        Field::new("v3", DataType::Float64, false),
    ]);

    if opt.mem_table {
        let listing_config = ListingTableConfig::new(ListingTableUrl::parse(path)?)
            .with_listing_options(ListingOptions::new(Arc::new(CsvFormat::default())))
            .with_schema(Arc::new(schema));
        let csv = ListingTable::try_new(listing_config)?;
        let partition_size = num_cpus::get();
        let memtable =
            MemTable::load(Arc::new(csv), Some(partition_size), &ctx.state()).await?;
        ctx.register_table("x", Arc::new(memtable))?;
    } else {
        ctx.register_csv("x", path, CsvReadOptions::default().schema(&schema))
            .await?;
    }
    rundata.start_new_case(&opt.query.to_string());
    let sql = match opt.query {
        1 => "select id1, sum(v1) as v1 from x group by id1",
        2 => "select id1, id2, sum(v1) as v1 from x group by id1, id2",
        3 => "select id3, sum(v1) as v1, mean(v3) as v3 from x group by id3",
        4 => "select id4, mean(v1) as v1, mean(v2) as v2, mean(v3) as v3 from x group by id4",
        5 => "select id6, sum(v1) as v1, sum(v2) as v2, sum(v3) as v3 from x group by id6",
        6 => "select id4, id5, median(v3) as median_v3, stddev(v3) as sd_v3 from x group by id4, id5",
        7 => "select id3, max(v1)-min(v2) as range_v1_v2 from x group by id3",
        8 => "select id6, largest2_v3 from (select id6, v3 as largest2_v3, row_number() over (partition by id6 order by v3 desc) as order_v3 from x where v3 is not null) sub_query where order_v3 <= 2",
        9 => "select id2, id4, pow(corr(v1, v2), 2) as r2 from x group by id2, id4",
        10 => "select id1, id2, id3, id4, id5, id6, sum(v3) as v3, count(*) as count from x group by id1, id2, id3, id4, id5, id6",
        _ => unimplemented!(),
    };

    println!("Executing {sql}");
    let start = Instant::now();
    let df = ctx.sql(sql).await?;
    let batches = df.collect().await?;
    let elapsed = start.elapsed();
    let numrows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    if opt.debug {
        pretty::print_batches(&batches)?;
    }
    rundata.write_iter(elapsed, numrows);
    println!(
        "h2o groupby query {} took {} ms",
        opt.query,
        elapsed.as_secs_f64() * 1000.0
    );
    rundata.maybe_write_json(opt.output_path.as_ref())?;
    Ok(())
}
