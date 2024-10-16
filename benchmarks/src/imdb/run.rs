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

use std::path::PathBuf;
use std::sync::Arc;

use super::{get_imdb_table_schema, get_query_sql, IMDB_TABLES};
use crate::util::{BenchmarkRun, CommonOpt};

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::{self, pretty_format_batches};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use datafusion_common::{DEFAULT_CSV_EXTENSION, DEFAULT_PARQUET_EXTENSION};

use log::info;
use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
type BoolDefaultTrue = bool;

/// Run the imdb benchmark (a.k.a. JOB).
///
/// This benchmarks is derived from the [Join Order Benchmark / JOB] proposed in paper [How Good Are Query Optimizers, Really?][1].
/// The data and answers are downloaded from
/// [2] and [3].
///
/// [1]: https://www.vldb.org/pvldb/vol9/p204-leis.pdf
/// [2]: http://homepages.cwi.nl/~boncz/job/imdb.tgz
/// [3]: https://db.in.tum.de/~leis/qo/job.tgz

#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,

    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to data files
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// File format: `csv` or `parquet`
    #[structopt(short = "f", long = "format", default_value = "csv")]
    file_format: String,

    /// Load the data into a MemTable before executing the query
    #[structopt(short = "m", long = "mem-table")]
    mem_table: bool,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Whether to disable collection of statistics (and cost based optimizations) or not.
    #[structopt(short = "S", long = "disable-statistics")]
    disable_statistics: bool,

    /// If true then hash join used, if false then sort merge join
    /// True by default.
    #[structopt(short = "j", long = "prefer_hash_join", default_value = "true")]
    prefer_hash_join: BoolDefaultTrue,
}

const IMDB_QUERY_START_ID: usize = 1;
const IMDB_QUERY_END_ID: usize = 113;

fn map_query_id_to_str(query_id: usize) -> &'static str {
    match query_id {
        // 1
        1 => "1a",
        2 => "1b",
        3 => "1c",
        4 => "1d",

        // 2
        5 => "2a",
        6 => "2b",
        7 => "2c",
        8 => "2d",

        // 3
        9 => "3a",
        10 => "3b",
        11 => "3c",

        // 4
        12 => "4a",
        13 => "4b",
        14 => "4c",

        // 5
        15 => "5a",
        16 => "5b",
        17 => "5c",

        // 6
        18 => "6a",
        19 => "6b",
        20 => "6c",
        21 => "6d",
        22 => "6e",
        23 => "6f",

        // 7
        24 => "7a",
        25 => "7b",
        26 => "7c",

        // 8
        27 => "8a",
        28 => "8b",
        29 => "8c",
        30 => "8d",

        // 9
        31 => "9a",
        32 => "9b",
        33 => "9c",
        34 => "9d",

        // 10
        35 => "10a",
        36 => "10b",
        37 => "10c",

        // 11
        38 => "11a",
        39 => "11b",
        40 => "11c",
        41 => "11d",

        // 12
        42 => "12a",
        43 => "12b",
        44 => "12c",

        // 13
        45 => "13a",
        46 => "13b",
        47 => "13c",
        48 => "13d",

        // 14
        49 => "14a",
        50 => "14b",
        51 => "14c",

        // 15
        52 => "15a",
        53 => "15b",
        54 => "15c",
        55 => "15d",

        // 16
        56 => "16a",
        57 => "16b",
        58 => "16c",
        59 => "16d",

        // 17
        60 => "17a",
        61 => "17b",
        62 => "17c",
        63 => "17d",
        64 => "17e",
        65 => "17f",

        // 18
        66 => "18a",
        67 => "18b",
        68 => "18c",

        // 19
        69 => "19a",
        70 => "19b",
        71 => "19c",
        72 => "19d",

        // 20
        73 => "20a",
        74 => "20b",
        75 => "20c",

        // 21
        76 => "21a",
        77 => "21b",
        78 => "21c",

        // 22
        79 => "22a",
        80 => "22b",
        81 => "22c",
        82 => "22d",

        // 23
        83 => "23a",
        84 => "23b",
        85 => "23c",

        // 24
        86 => "24a",
        87 => "24b",

        // 25
        88 => "25a",
        89 => "25b",
        90 => "25c",

        // 26
        91 => "26a",
        92 => "26b",
        93 => "26c",

        // 27
        94 => "27a",
        95 => "27b",
        96 => "27c",

        // 28
        97 => "28a",
        98 => "28b",
        99 => "28c",

        // 29
        100 => "29a",
        101 => "29b",
        102 => "29c",

        // 30
        103 => "30a",
        104 => "30b",
        105 => "30c",

        // 31
        106 => "31a",
        107 => "31b",
        108 => "31c",

        // 32
        109 => "32a",
        110 => "32b",

        // 33
        111 => "33a",
        112 => "33b",
        113 => "33c",

        // Fallback for unknown query_id
        _ => "unknown",
    }
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        println!("Running benchmarks with the following options: {self:?}");
        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => IMDB_QUERY_START_ID..=IMDB_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("Query {query_id}"));
            let query_run = self.benchmark_query(query_id).await?;
            for iter in query_run {
                benchmark_run.write_iter(iter.elapsed, iter.row_count);
            }
        }
        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        Ok(())
    }

    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let mut config = self
            .common
            .config()
            .with_collect_statistics(!self.disable_statistics);
        config.options_mut().optimizer.prefer_hash_join = self.prefer_hash_join;
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = self.common.force_view_types;
        let ctx = SessionContext::new_with_config(config);

        // register tables
        self.register_tables(&ctx).await?;

        let mut millis = vec![];
        // run benchmark
        let mut query_results = vec![];
        for i in 0..self.iterations() {
            let start = Instant::now();

            let query_id_str = map_query_id_to_str(query_id);
            let sql = &get_query_sql(query_id_str)?;

            let mut result = vec![];

            for query in sql {
                result = self.execute_query(&ctx, query).await?;
            }

            let elapsed = start.elapsed(); //.as_secs_f64() * 1000.0;
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);
            info!("output:\n\n{}\n\n", pretty_format_batches(&result)?);
            let row_count = result.iter().map(|b| b.num_rows()).sum();
            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");

        Ok(query_results)
    }

    async fn register_tables(&self, ctx: &SessionContext) -> Result<()> {
        for table in IMDB_TABLES {
            let table_provider = { self.get_table(ctx, table).await? };

            if self.mem_table {
                println!("Loading table '{table}' into memory");
                let start = Instant::now();
                let memtable =
                    MemTable::load(table_provider, Some(self.partitions()), &ctx.state())
                        .await?;
                println!(
                    "Loaded table '{}' into memory in {} ms",
                    table,
                    start.elapsed().as_millis()
                );
                ctx.register_table(*table, Arc::new(memtable))?;
            } else {
                ctx.register_table(*table, table_provider)?;
            }
        }
        Ok(())
    }

    async fn execute_query(
        &self,
        ctx: &SessionContext,
        sql: &str,
    ) -> Result<Vec<RecordBatch>> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();

        if debug {
            println!("=== Logical plan ===\n{plan}\n");
        }

        let plan = state.optimize(&plan)?;
        if debug {
            println!("=== Optimized logical plan ===\n{plan}\n");
        }
        let physical_plan = state.create_physical_plan(&plan).await?;
        if debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }
        let result = collect(physical_plan.clone(), state.task_ctx()).await?;
        if debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                    .indent(true)
            );
            if !result.is_empty() {
                // do not call print_batches if there are no batches as the result is confusing
                // and makes it look like there is a batch with no columns
                pretty::print_batches(&result)?;
            }
        }
        Ok(result)
    }

    async fn get_table(
        &self,
        ctx: &SessionContext,
        table: &str,
    ) -> Result<Arc<dyn TableProvider>> {
        let path = self.path.to_str().unwrap();
        let table_format = self.file_format.as_str();

        // Obtain a snapshot of the SessionState
        let state = ctx.state();
        let (format, path, extension): (Arc<dyn FileFormat>, String, &'static str) =
            match table_format {
                // dbgen creates .tbl ('|' delimited) files without header
                "tbl" => {
                    let path = format!("{path}/{table}.tbl");

                    let format = CsvFormat::default()
                        .with_delimiter(b'|')
                        .with_has_header(false);

                    (Arc::new(format), path, ".tbl")
                }
                "csv" => {
                    let path = format!("{path}/{table}.csv");
                    let format = CsvFormat::default()
                        .with_delimiter(b',')
                        .with_escape(Some(b'\\'))
                        .with_has_header(false);

                    (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
                }
                "parquet" => {
                    let path = format!("{path}/{table}.parquet");
                    let format = ParquetFormat::default()
                        .with_options(ctx.state().table_options().parquet.clone());
                    (Arc::new(format), path, DEFAULT_PARQUET_EXTENSION)
                }
                other => {
                    unimplemented!("Invalid file format '{}'", other);
                }
            };

        let options = ListingOptions::new(format)
            .with_file_extension(extension)
            .with_collect_stat(state.config().collect_statistics());

        let table_path = ListingTableUrl::parse(path)?;
        let config = ListingTableConfig::new(table_path).with_listing_options(options);
        let config = match table_format {
            "parquet" => config.with_schema(Arc::new(get_imdb_table_schema(table))),
            "csv" => config.with_schema(Arc::new(get_imdb_table_schema(table))),
            _ => unreachable!(),
        };

        Ok(Arc::new(ListingTable::try_new(config)?))
    }

    fn iterations(&self) -> usize {
        self.common.iterations
    }

    fn partitions(&self) -> usize {
        self.common.partitions.unwrap_or(num_cpus::get())
    }
}

struct QueryResult {
    elapsed: std::time::Duration,
    row_count: usize,
}

#[cfg(test)]
// Only run with "ci" mode when we have the data
#[cfg(feature = "ci")]
mod tests {
    use std::path::Path;

    use super::*;

    use crate::util::CommonOpt;
    use datafusion::common::exec_err;
    use datafusion::error::Result;
    use datafusion_proto::bytes::{
        logical_plan_from_bytes, logical_plan_to_bytes, physical_plan_from_bytes,
        physical_plan_to_bytes,
    };

    fn get_imdb_data_path() -> Result<String> {
        let path =
            std::env::var("IMDB_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
        if !Path::new(&path).exists() {
            return exec_err!(
                "Benchmark data not found (set IMDB_DATA env var to override): {}",
                path
            );
        }
        Ok(path)
    }

    async fn round_trip_logical_plan(query: usize) -> Result<()> {
        let ctx = SessionContext::default();
        let path = get_imdb_data_path()?;
        let common = CommonOpt {
            iterations: 1,
            partitions: Some(2),
            batch_size: 8192,
            debug: false,
            force_view_types: false,
        };
        let opt = RunOpt {
            query: Some(query),
            common,
            path: PathBuf::from(path.to_string()),
            file_format: "parquet".to_string(),
            mem_table: false,
            output_path: None,
            disable_statistics: false,
            prefer_hash_join: true,
        };
        opt.register_tables(&ctx).await?;
        let queries = get_query_sql(map_query_id_to_str(query))?;
        for query in queries {
            let plan = ctx.sql(&query).await?;
            let plan = plan.into_optimized_plan()?;
            let bytes = logical_plan_to_bytes(&plan)?;
            let plan2 = logical_plan_from_bytes(&bytes, &ctx)?;
            let plan_formatted = format!("{}", plan.display_indent());
            let plan2_formatted = format!("{}", plan2.display_indent());
            assert_eq!(plan_formatted, plan2_formatted);
        }
        Ok(())
    }

    async fn round_trip_physical_plan(query: usize) -> Result<()> {
        let ctx = SessionContext::default();
        let path = get_imdb_data_path()?;
        let common = CommonOpt {
            iterations: 1,
            partitions: Some(2),
            batch_size: 8192,
            debug: false,
            force_view_types: false,
        };
        let opt = RunOpt {
            query: Some(query),
            common,
            path: PathBuf::from(path.to_string()),
            file_format: "parquet".to_string(),
            mem_table: false,
            output_path: None,
            disable_statistics: false,
            prefer_hash_join: true,
        };
        opt.register_tables(&ctx).await?;
        let queries = get_query_sql(map_query_id_to_str(query))?;
        for query in queries {
            let plan = ctx.sql(&query).await?;
            let plan = plan.create_physical_plan().await?;
            let bytes = physical_plan_to_bytes(plan.clone())?;
            let plan2 = physical_plan_from_bytes(&bytes, &ctx)?;
            let plan_formatted = format!("{}", displayable(plan.as_ref()).indent(false));
            let plan2_formatted =
                format!("{}", displayable(plan2.as_ref()).indent(false));
            assert_eq!(plan_formatted, plan2_formatted);
        }
        Ok(())
    }

    macro_rules! test_round_trip_logical {
        ($tn:ident, $query:expr) => {
            #[tokio::test]
            async fn $tn() -> Result<()> {
                round_trip_logical_plan($query).await
            }
        };
    }

    macro_rules! test_round_trip_physical {
        ($tn:ident, $query:expr) => {
            #[tokio::test]
            async fn $tn() -> Result<()> {
                round_trip_physical_plan($query).await
            }
        };
    }

    // logical plan tests
    test_round_trip_logical!(round_trip_logical_plan_1a, 1);
    test_round_trip_logical!(round_trip_logical_plan_1b, 2);
    test_round_trip_logical!(round_trip_logical_plan_1c, 3);
    test_round_trip_logical!(round_trip_logical_plan_1d, 4);
    test_round_trip_logical!(round_trip_logical_plan_2a, 5);
    test_round_trip_logical!(round_trip_logical_plan_2b, 6);
    test_round_trip_logical!(round_trip_logical_plan_2c, 7);
    test_round_trip_logical!(round_trip_logical_plan_2d, 8);
    test_round_trip_logical!(round_trip_logical_plan_3a, 9);
    test_round_trip_logical!(round_trip_logical_plan_3b, 10);
    test_round_trip_logical!(round_trip_logical_plan_3c, 11);
    test_round_trip_logical!(round_trip_logical_plan_4a, 12);
    test_round_trip_logical!(round_trip_logical_plan_4b, 13);
    test_round_trip_logical!(round_trip_logical_plan_4c, 14);
    test_round_trip_logical!(round_trip_logical_plan_5a, 15);
    test_round_trip_logical!(round_trip_logical_plan_5b, 16);
    test_round_trip_logical!(round_trip_logical_plan_5c, 17);
    test_round_trip_logical!(round_trip_logical_plan_6a, 18);
    test_round_trip_logical!(round_trip_logical_plan_6b, 19);
    test_round_trip_logical!(round_trip_logical_plan_6c, 20);
    test_round_trip_logical!(round_trip_logical_plan_6d, 21);
    test_round_trip_logical!(round_trip_logical_plan_6e, 22);
    test_round_trip_logical!(round_trip_logical_plan_6f, 23);
    test_round_trip_logical!(round_trip_logical_plan_7a, 24);
    test_round_trip_logical!(round_trip_logical_plan_7b, 25);
    test_round_trip_logical!(round_trip_logical_plan_7c, 26);
    test_round_trip_logical!(round_trip_logical_plan_8a, 27);
    test_round_trip_logical!(round_trip_logical_plan_8b, 28);
    test_round_trip_logical!(round_trip_logical_plan_8c, 29);
    test_round_trip_logical!(round_trip_logical_plan_8d, 30);
    test_round_trip_logical!(round_trip_logical_plan_9a, 31);
    test_round_trip_logical!(round_trip_logical_plan_9b, 32);
    test_round_trip_logical!(round_trip_logical_plan_9c, 33);
    test_round_trip_logical!(round_trip_logical_plan_9d, 34);
    test_round_trip_logical!(round_trip_logical_plan_10a, 35);
    test_round_trip_logical!(round_trip_logical_plan_10b, 36);
    test_round_trip_logical!(round_trip_logical_plan_10c, 37);
    test_round_trip_logical!(round_trip_logical_plan_11a, 38);
    test_round_trip_logical!(round_trip_logical_plan_11b, 39);
    test_round_trip_logical!(round_trip_logical_plan_11c, 40);
    test_round_trip_logical!(round_trip_logical_plan_11d, 41);
    test_round_trip_logical!(round_trip_logical_plan_12a, 42);
    test_round_trip_logical!(round_trip_logical_plan_12b, 43);
    test_round_trip_logical!(round_trip_logical_plan_12c, 44);
    test_round_trip_logical!(round_trip_logical_plan_13a, 45);
    test_round_trip_logical!(round_trip_logical_plan_13b, 46);
    test_round_trip_logical!(round_trip_logical_plan_13c, 47);
    test_round_trip_logical!(round_trip_logical_plan_13d, 48);
    test_round_trip_logical!(round_trip_logical_plan_14a, 49);
    test_round_trip_logical!(round_trip_logical_plan_14b, 50);
    test_round_trip_logical!(round_trip_logical_plan_14c, 51);
    test_round_trip_logical!(round_trip_logical_plan_15a, 52);
    test_round_trip_logical!(round_trip_logical_plan_15b, 53);
    test_round_trip_logical!(round_trip_logical_plan_15c, 54);
    test_round_trip_logical!(round_trip_logical_plan_15d, 55);
    test_round_trip_logical!(round_trip_logical_plan_16a, 56);
    test_round_trip_logical!(round_trip_logical_plan_16b, 57);
    test_round_trip_logical!(round_trip_logical_plan_16c, 58);
    test_round_trip_logical!(round_trip_logical_plan_16d, 59);
    test_round_trip_logical!(round_trip_logical_plan_17a, 60);
    test_round_trip_logical!(round_trip_logical_plan_17b, 61);
    test_round_trip_logical!(round_trip_logical_plan_17c, 62);
    test_round_trip_logical!(round_trip_logical_plan_17d, 63);
    test_round_trip_logical!(round_trip_logical_plan_17e, 64);
    test_round_trip_logical!(round_trip_logical_plan_17f, 65);
    test_round_trip_logical!(round_trip_logical_plan_18a, 66);
    test_round_trip_logical!(round_trip_logical_plan_18b, 67);
    test_round_trip_logical!(round_trip_logical_plan_18c, 68);
    test_round_trip_logical!(round_trip_logical_plan_19a, 69);
    test_round_trip_logical!(round_trip_logical_plan_19b, 70);
    test_round_trip_logical!(round_trip_logical_plan_19c, 71);
    test_round_trip_logical!(round_trip_logical_plan_19d, 72);
    test_round_trip_logical!(round_trip_logical_plan_20a, 73);
    test_round_trip_logical!(round_trip_logical_plan_20b, 74);
    test_round_trip_logical!(round_trip_logical_plan_20c, 75);
    test_round_trip_logical!(round_trip_logical_plan_21a, 76);
    test_round_trip_logical!(round_trip_logical_plan_21b, 77);
    test_round_trip_logical!(round_trip_logical_plan_21c, 78);
    test_round_trip_logical!(round_trip_logical_plan_22a, 79);
    test_round_trip_logical!(round_trip_logical_plan_22b, 80);
    test_round_trip_logical!(round_trip_logical_plan_22c, 81);
    test_round_trip_logical!(round_trip_logical_plan_22d, 82);
    test_round_trip_logical!(round_trip_logical_plan_23a, 83);
    test_round_trip_logical!(round_trip_logical_plan_23b, 84);
    test_round_trip_logical!(round_trip_logical_plan_23c, 85);
    test_round_trip_logical!(round_trip_logical_plan_24a, 86);
    test_round_trip_logical!(round_trip_logical_plan_24b, 87);
    test_round_trip_logical!(round_trip_logical_plan_25a, 88);
    test_round_trip_logical!(round_trip_logical_plan_25b, 89);
    test_round_trip_logical!(round_trip_logical_plan_25c, 90);
    test_round_trip_logical!(round_trip_logical_plan_26a, 91);
    test_round_trip_logical!(round_trip_logical_plan_26b, 92);
    test_round_trip_logical!(round_trip_logical_plan_26c, 93);
    test_round_trip_logical!(round_trip_logical_plan_27a, 94);
    test_round_trip_logical!(round_trip_logical_plan_27b, 95);
    test_round_trip_logical!(round_trip_logical_plan_27c, 96);
    test_round_trip_logical!(round_trip_logical_plan_28a, 97);
    test_round_trip_logical!(round_trip_logical_plan_28b, 98);
    test_round_trip_logical!(round_trip_logical_plan_28c, 99);
    test_round_trip_logical!(round_trip_logical_plan_29a, 100);
    test_round_trip_logical!(round_trip_logical_plan_29b, 101);
    test_round_trip_logical!(round_trip_logical_plan_29c, 102);
    test_round_trip_logical!(round_trip_logical_plan_30a, 103);
    test_round_trip_logical!(round_trip_logical_plan_30b, 104);
    test_round_trip_logical!(round_trip_logical_plan_30c, 105);
    test_round_trip_logical!(round_trip_logical_plan_31a, 106);
    test_round_trip_logical!(round_trip_logical_plan_31b, 107);
    test_round_trip_logical!(round_trip_logical_plan_31c, 108);
    test_round_trip_logical!(round_trip_logical_plan_32a, 109);
    test_round_trip_logical!(round_trip_logical_plan_32b, 110);
    test_round_trip_logical!(round_trip_logical_plan_33a, 111);
    test_round_trip_logical!(round_trip_logical_plan_33b, 112);
    test_round_trip_logical!(round_trip_logical_plan_33c, 113);

    // physical plan tests
    test_round_trip_physical!(round_trip_physical_plan_1a, 1);
    test_round_trip_physical!(round_trip_physical_plan_1b, 2);
    test_round_trip_physical!(round_trip_physical_plan_1c, 3);
    test_round_trip_physical!(round_trip_physical_plan_1d, 4);
    test_round_trip_physical!(round_trip_physical_plan_2a, 5);
    test_round_trip_physical!(round_trip_physical_plan_2b, 6);
    test_round_trip_physical!(round_trip_physical_plan_2c, 7);
    test_round_trip_physical!(round_trip_physical_plan_2d, 8);
    test_round_trip_physical!(round_trip_physical_plan_3a, 9);
    test_round_trip_physical!(round_trip_physical_plan_3b, 10);
    test_round_trip_physical!(round_trip_physical_plan_3c, 11);
    test_round_trip_physical!(round_trip_physical_plan_4a, 12);
    test_round_trip_physical!(round_trip_physical_plan_4b, 13);
    test_round_trip_physical!(round_trip_physical_plan_4c, 14);
    test_round_trip_physical!(round_trip_physical_plan_5a, 15);
    test_round_trip_physical!(round_trip_physical_plan_5b, 16);
    test_round_trip_physical!(round_trip_physical_plan_5c, 17);
    test_round_trip_physical!(round_trip_physical_plan_6a, 18);
    test_round_trip_physical!(round_trip_physical_plan_6b, 19);
    test_round_trip_physical!(round_trip_physical_plan_6c, 20);
    test_round_trip_physical!(round_trip_physical_plan_6d, 21);
    test_round_trip_physical!(round_trip_physical_plan_6e, 22);
    test_round_trip_physical!(round_trip_physical_plan_6f, 23);
    test_round_trip_physical!(round_trip_physical_plan_7a, 24);
    test_round_trip_physical!(round_trip_physical_plan_7b, 25);
    test_round_trip_physical!(round_trip_physical_plan_7c, 26);
    test_round_trip_physical!(round_trip_physical_plan_8a, 27);
    test_round_trip_physical!(round_trip_physical_plan_8b, 28);
    test_round_trip_physical!(round_trip_physical_plan_8c, 29);
    test_round_trip_physical!(round_trip_physical_plan_8d, 30);
    test_round_trip_physical!(round_trip_physical_plan_9a, 31);
    test_round_trip_physical!(round_trip_physical_plan_9b, 32);
    test_round_trip_physical!(round_trip_physical_plan_9c, 33);
    test_round_trip_physical!(round_trip_physical_plan_9d, 34);
    test_round_trip_physical!(round_trip_physical_plan_10a, 35);
    test_round_trip_physical!(round_trip_physical_plan_10b, 36);
    test_round_trip_physical!(round_trip_physical_plan_10c, 37);
    test_round_trip_physical!(round_trip_physical_plan_11a, 38);
    test_round_trip_physical!(round_trip_physical_plan_11b, 39);
    test_round_trip_physical!(round_trip_physical_plan_11c, 40);
    test_round_trip_physical!(round_trip_physical_plan_11d, 41);
    test_round_trip_physical!(round_trip_physical_plan_12a, 42);
    test_round_trip_physical!(round_trip_physical_plan_12b, 43);
    test_round_trip_physical!(round_trip_physical_plan_12c, 44);
    test_round_trip_physical!(round_trip_physical_plan_13a, 45);
    test_round_trip_physical!(round_trip_physical_plan_13b, 46);
    test_round_trip_physical!(round_trip_physical_plan_13c, 47);
    test_round_trip_physical!(round_trip_physical_plan_13d, 48);
    test_round_trip_physical!(round_trip_physical_plan_14a, 49);
    test_round_trip_physical!(round_trip_physical_plan_14b, 50);
    test_round_trip_physical!(round_trip_physical_plan_14c, 51);
    test_round_trip_physical!(round_trip_physical_plan_15a, 52);
    test_round_trip_physical!(round_trip_physical_plan_15b, 53);
    test_round_trip_physical!(round_trip_physical_plan_15c, 54);
    test_round_trip_physical!(round_trip_physical_plan_15d, 55);
    test_round_trip_physical!(round_trip_physical_plan_16a, 56);
    test_round_trip_physical!(round_trip_physical_plan_16b, 57);
    test_round_trip_physical!(round_trip_physical_plan_16c, 58);
    test_round_trip_physical!(round_trip_physical_plan_16d, 59);
    test_round_trip_physical!(round_trip_physical_plan_17a, 60);
    test_round_trip_physical!(round_trip_physical_plan_17b, 61);
    test_round_trip_physical!(round_trip_physical_plan_17c, 62);
    test_round_trip_physical!(round_trip_physical_plan_17d, 63);
    test_round_trip_physical!(round_trip_physical_plan_17e, 64);
    test_round_trip_physical!(round_trip_physical_plan_17f, 65);
    test_round_trip_physical!(round_trip_physical_plan_18a, 66);
    test_round_trip_physical!(round_trip_physical_plan_18b, 67);
    test_round_trip_physical!(round_trip_physical_plan_18c, 68);
    test_round_trip_physical!(round_trip_physical_plan_19a, 69);
    test_round_trip_physical!(round_trip_physical_plan_19b, 70);
    test_round_trip_physical!(round_trip_physical_plan_19c, 71);
    test_round_trip_physical!(round_trip_physical_plan_19d, 72);
    test_round_trip_physical!(round_trip_physical_plan_20a, 73);
    test_round_trip_physical!(round_trip_physical_plan_20b, 74);
    test_round_trip_physical!(round_trip_physical_plan_20c, 75);
    test_round_trip_physical!(round_trip_physical_plan_21a, 76);
    test_round_trip_physical!(round_trip_physical_plan_21b, 77);
    test_round_trip_physical!(round_trip_physical_plan_21c, 78);
    test_round_trip_physical!(round_trip_physical_plan_22a, 79);
    test_round_trip_physical!(round_trip_physical_plan_22b, 80);
    test_round_trip_physical!(round_trip_physical_plan_22c, 81);
    test_round_trip_physical!(round_trip_physical_plan_22d, 82);
    test_round_trip_physical!(round_trip_physical_plan_23a, 83);
    test_round_trip_physical!(round_trip_physical_plan_23b, 84);
    test_round_trip_physical!(round_trip_physical_plan_23c, 85);
    test_round_trip_physical!(round_trip_physical_plan_24a, 86);
    test_round_trip_physical!(round_trip_physical_plan_24b, 87);
    test_round_trip_physical!(round_trip_physical_plan_25a, 88);
    test_round_trip_physical!(round_trip_physical_plan_25b, 89);
    test_round_trip_physical!(round_trip_physical_plan_25c, 90);
    test_round_trip_physical!(round_trip_physical_plan_26a, 91);
    test_round_trip_physical!(round_trip_physical_plan_26b, 92);
    test_round_trip_physical!(round_trip_physical_plan_26c, 93);
    test_round_trip_physical!(round_trip_physical_plan_27a, 94);
    test_round_trip_physical!(round_trip_physical_plan_27b, 95);
    test_round_trip_physical!(round_trip_physical_plan_27c, 96);
    test_round_trip_physical!(round_trip_physical_plan_28a, 97);
    test_round_trip_physical!(round_trip_physical_plan_28b, 98);
    test_round_trip_physical!(round_trip_physical_plan_28c, 99);
    test_round_trip_physical!(round_trip_physical_plan_29a, 100);
    test_round_trip_physical!(round_trip_physical_plan_29b, 101);
    test_round_trip_physical!(round_trip_physical_plan_29c, 102);
    test_round_trip_physical!(round_trip_physical_plan_30a, 103);
    test_round_trip_physical!(round_trip_physical_plan_30b, 104);
    test_round_trip_physical!(round_trip_physical_plan_30c, 105);
    test_round_trip_physical!(round_trip_physical_plan_31a, 106);
    test_round_trip_physical!(round_trip_physical_plan_31b, 107);
    test_round_trip_physical!(round_trip_physical_plan_31c, 108);
    test_round_trip_physical!(round_trip_physical_plan_32a, 109);
    test_round_trip_physical!(round_trip_physical_plan_32b, 110);
    test_round_trip_physical!(round_trip_physical_plan_33a, 111);
    test_round_trip_physical!(round_trip_physical_plan_33b, 112);
    test_round_trip_physical!(round_trip_physical_plan_33c, 113);
}
