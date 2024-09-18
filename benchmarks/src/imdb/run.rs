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
use crate::{BenchmarkRun, CommonOpt};

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
/// This benchmarks is derived from the [TPC-H][1] version
/// [2.17.1]. The data and answers are generated using `tpch-gen` from
/// [2].
///
/// [1]: http://www.tpc.org/tpch/
/// [2]: https://github.com/databricks/tpch-dbgen.git,
/// [2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf
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
            let mut query_id_str = "";

            // 1
            if query_id == 1 {
                query_id_str = "1a"
            } else if query_id == 2 {
                query_id_str = "1b"
            } else if query_id == 3 {
                query_id_str = "1c"
            } else if query_id == 4 {
                query_id_str = "1d"
            }

            // 2
            if query_id == 5 {
                query_id_str = "2a"
            } else if query_id == 6 {
                query_id_str = "2b"
            } else if query_id == 7 {
                query_id_str = "2c"
            } else if query_id == 8 {
                query_id_str = "2d"
            }

            // 3
            if query_id == 9 {
                query_id_str = "3a"
            } else if query_id == 10 {
                query_id_str = "3b"
            } else if query_id == 11 {
                query_id_str = "3c"
            }

            // 4
            if query_id == 12 {
                query_id_str = "4a"
            } else if query_id == 13 {
                query_id_str = "4b"
            } else if query_id == 14 {
                query_id_str = "4c"
            }

            // 5
            if query_id == 15 {
                query_id_str = "5a"
            } else if query_id == 16 {
                query_id_str = "5b"
            } else if query_id == 17 {
                query_id_str = "5c"
            }

            // 6
            if query_id == 18 {
                query_id_str = "6a"
            } else if query_id == 19 {
                query_id_str = "6b"
            } else if query_id == 20 {
                query_id_str = "6c"
            } else if query_id == 21 {
                query_id_str = "6d"
            } else if query_id == 22 {
                query_id_str = "6e"
            } else if query_id == 23 {
                query_id_str = "6f"
            }

            // 7
            if query_id == 24 {
                query_id_str = "7a"
            } else if query_id == 25 {
                query_id_str = "7b"
            } else if query_id == 26 {
                query_id_str = "7c"
            }

            // 8
            if query_id == 27 {
                query_id_str = "8a"
            } else if query_id == 28 {
                query_id_str = "8b"
            } else if query_id == 29 {
                query_id_str = "8c"
            } else if query_id == 30 {
                query_id_str = "8d"
            }

            // 9
            if query_id == 31 {
                query_id_str = "9a"
            } else if query_id == 32 {
                query_id_str = "9b"
            } else if query_id == 33 {
                query_id_str = "9c"
            } else if query_id == 34 {
                query_id_str = "9d"
            }

            // 10
            if query_id == 35 {
                query_id_str = "10a"
            } else if query_id == 36 {
                query_id_str = "10b"
            } else if query_id == 37 {
                query_id_str = "10c"
            }

            // 11
            if query_id == 38 {
                query_id_str = "11a"
            } else if query_id == 39 {
                query_id_str = "11b"
            } else if query_id == 40 {
                query_id_str = "11c"
            } else if query_id == 41 {
                query_id_str = "11d"
            }

            // 12
            if query_id == 42 {
                query_id_str = "12a"
            } else if query_id == 43 {
                query_id_str = "12b"
            } else if query_id == 44 {
                query_id_str = "12c"
            }

            // 13
            if query_id == 45 {
                query_id_str = "13a"
            } else if query_id == 46 {
                query_id_str = "13b"
            } else if query_id == 47 {
                query_id_str = "13c"
            } else if query_id == 48 {
                query_id_str = "13d"
            }

            // 14
            if query_id == 49 {
                query_id_str = "14a"
            } else if query_id == 50 {
                query_id_str = "14b"
            } else if query_id == 51 {
                query_id_str = "14c"
            }

            // 15
            if query_id == 52 {
                query_id_str = "15a"
            } else if query_id == 53 {
                query_id_str = "15b"
            } else if query_id == 54 {
                query_id_str = "15c"
            } else if query_id == 55 {
                query_id_str = "15d"
            }

            // 16
            if query_id == 56 {
                query_id_str = "16a"
            } else if query_id == 57 {
                query_id_str = "16b"
            } else if query_id == 58 {
                query_id_str = "16c"
            } else if query_id == 59 {
                query_id_str = "16d"
            }

            // 17
            if query_id == 60 {
                query_id_str = "17a"
            } else if query_id == 61 {
                query_id_str = "17b"
            } else if query_id == 62 {
                query_id_str = "17c"
            } else if query_id == 63 {
                query_id_str = "17d"
            } else if query_id == 64 {
                query_id_str = "17e"
            } else if query_id == 65 {
                query_id_str = "17f"
            }

            // 18
            if query_id == 66 {
                query_id_str = "18a"
            } else if query_id == 67 {
                query_id_str = "18b"
            } else if query_id == 68 {
                query_id_str = "18c"
            }

            // 19
            if query_id == 69 {
                query_id_str = "19a"
            } else if query_id == 70 {
                query_id_str = "19b"
            } else if query_id == 71 {
                query_id_str = "19c"
            } else if query_id == 72 {
                query_id_str = "19d"
            }

            // 20
            if query_id == 73 {
                query_id_str = "20a"
            } else if query_id == 74 {
                query_id_str = "20b"
            } else if query_id == 75 {
                query_id_str = "20c"
            }

            // 21
            if query_id == 76 {
                query_id_str = "21a"
            } else if query_id == 77 {
                query_id_str = "21b"
            } else if query_id == 78 {
                query_id_str = "21c"
            }

            // 22
            if query_id == 79 {
                query_id_str = "22a"
            } else if query_id == 80 {
                query_id_str = "22b"
            } else if query_id == 81 {
                query_id_str = "22c"
            } else if query_id == 82 {
                query_id_str = "22d"
            }

            // 23
            if query_id == 83 {
                query_id_str = "23a"
            } else if query_id == 84 {
                query_id_str = "23b"
            } else if query_id == 85 {
                query_id_str = "23c"
            }

            // 24
            if query_id == 86 {
                query_id_str = "24a"
            } else if query_id == 87 {
                query_id_str = "24b"
            }

            // 25
            if query_id == 88 {
                query_id_str = "25a"
            } else if query_id == 89 {
                query_id_str = "25b"
            } else if query_id == 90 {
                query_id_str = "25c"
            }

            // 26
            if query_id == 91 {
                query_id_str = "26a"
            } else if query_id == 92 {
                query_id_str = "26b"
            } else if query_id == 93 {
                query_id_str = "26c"
            }

            // 27
            if query_id == 94 {
                query_id_str = "27a"
            } else if query_id == 95 {
                query_id_str = "27b"
            } else if query_id == 96 {
                query_id_str = "27c"
            }

            // 28
            if query_id == 97 {
                query_id_str = "28a"
            } else if query_id == 98 {
                query_id_str = "28b"
            } else if query_id == 99 {
                query_id_str = "28c"
            }

            // 29
            if query_id == 100 {
                query_id_str = "29a"
            } else if query_id == 101 {
                query_id_str = "29b"
            } else if query_id == 102 {
                query_id_str = "29c"
            }

            // 30
            if query_id == 103 {
                query_id_str = "30a"
            } else if query_id == 104 {
                query_id_str = "30b"
            } else if query_id == 105 {
                query_id_str = "30c"
            }

            // 31
            if query_id == 106 {
                query_id_str = "31a"
            } else if query_id == 107 {
                query_id_str = "31b"
            } else if query_id == 108 {
                query_id_str = "31c"
            }

            // 32
            if query_id == 109 {
                query_id_str = "32a"
            } else if query_id == 110 {
                query_id_str = "32b"
            }

            // 33
            if query_id == 111 {
                query_id_str = "33a"
            } else if query_id == 112 {
                query_id_str = "33b"
            } else if query_id == 113 {
                query_id_str = "33c"
            }

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
        let target_partitions = self.partitions();

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
                    let path = format!("{path}/{table}");
                    let format = CsvFormat::default()
                        .with_delimiter(b',')
                        .with_has_header(true);

                    (Arc::new(format), path, DEFAULT_CSV_EXTENSION)
                }
                "parquet" => {
                    let path = format!("{path}/{table}");
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
            .with_target_partitions(target_partitions)
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
        let queries = get_query_sql(query)?;
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
        let queries = get_query_sql(query)?;
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
    test_round_trip_logical!(round_trip_logical_plan_q1, 1);

    // physical plan tests
    test_round_trip_physical!(round_trip_physical_plan_q1, 1);
}
