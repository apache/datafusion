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

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::util::{BenchmarkRun, CommonOpt};

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTableUrl};
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::prelude::*;
use datafusion_common::instant::Instant;
use futures::TryStreamExt;
use object_store::ObjectStore;
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::ParquetObjectWriter;
use rand::Rng;
use rand::distr::Alphanumeric;
use rand::rngs::ThreadRng;
use structopt::StructOpt;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

/// Test performance of cancelling queries
///
/// Queries in DataFusion should stop executing "quickly" after they are
/// cancelled (the output stream is dropped).
///
/// The queries are executed on a synthetic dataset generated during
/// the benchmark execution that is an anonymized version of a
/// real-world data set.
///
/// The query is an anonymized version of a real-world query, and the
/// test starts the query then cancels it and reports how long it takes
/// for the runtime to fully exit.
#[derive(Debug, StructOpt, Clone)]
#[structopt(verbatim_doc_comment)]
pub struct RunOpt {
    /// Common options
    #[structopt(flatten)]
    common: CommonOpt,

    /// Path to folder where data will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Path to machine readable output file
    #[structopt(parse(from_os_str), short = "o", long = "output")]
    output_path: Option<PathBuf>,

    /// Number of files to generate
    #[structopt(long = "num-files", default_value = "7")]
    num_files: usize,

    /// Number of rows per file to generate
    #[structopt(long = "num-rows-per-file", default_value = "5000000")]
    num_rows_per_file: usize,

    /// How long to wait, in milliseconds, before attempting to cancel
    #[structopt(long = "wait-time", default_value = "100")]
    wait_time: u64,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let files_on_disk =
            find_or_generate_files(&self.path, self.num_files, self.num_rows_per_file)
                .await?;

        // Using an in-memory object store is important for this benchmark to ensure `datafusion`
        // is yielding often enough regardless of whether the file reading happens to be yielding.
        let store =
            Arc::new(object_store::memory::InMemory::new()) as Arc<dyn ObjectStore>;
        println!("Starting to load data into in-memory object store");
        load_data(Arc::clone(&store), &files_on_disk).await?;
        println!("Done loading data into in-memory object store");

        let mut rundata = BenchmarkRun::new();
        rundata.start_new_case("Cancellation");

        for i in 0..self.common.iterations {
            let elapsed = run_test(self.wait_time, Arc::clone(&store))?;
            let ms = elapsed.as_secs_f64() * 1000.0;
            println!("Iteration {i} cancelled in {ms} ms");
            rundata.write_iter(elapsed, 0);
        }

        rundata.maybe_write_json(self.output_path.as_ref())?;

        Ok(())
    }
}

fn run_test(wait_time: u64, store: Arc<dyn ObjectStore>) -> Result<Duration> {
    std::thread::spawn(move || {
        let token = CancellationToken::new();
        let captured_token = token.clone();

        let rt = Runtime::new()?;
        rt.spawn(async move {
            println!("Starting spawned");
            loop {
                let store = Arc::clone(&store);
                tokio::select! {
                    biased;
                    _ = async move {
                        datafusion(store).await.unwrap();
                    } => {
                        println!("matched case doing work");
                    },
                    _ = captured_token.cancelled() => {
                        println!("Received shutdown request");
                        return;
                    },
                }
            }
        });

        println!("in main, sleeping");
        std::thread::sleep(Duration::from_millis(wait_time));

        let start = Instant::now();

        println!("cancelling thread");
        token.cancel();

        drop(rt);

        let elapsed = start.elapsed();
        println!("done dropping runtime in {elapsed:?}");

        Ok(elapsed)
    })
    .join()
    .unwrap()
}

async fn datafusion(store: Arc<dyn ObjectStore>) -> Result<()> {
    let query = "SELECT distinct \"A\", \"B\", \"C\", \"D\", \"E\" FROM \"test_table\"";

    let config = SessionConfig::new()
        .with_target_partitions(4)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true);
    let ctx = SessionContext::new_with_config(config);
    let object_store_url = ObjectStoreUrl::parse("test:///").unwrap();
    ctx.register_object_store(object_store_url.as_ref(), Arc::clone(&store));

    let file_format = ParquetFormat::default().with_enable_pruning(true);
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(ParquetFormat::default().get_ext());

    let table_path = ListingTableUrl::parse("test:///data/")?;

    ctx.register_listing_table(
        "test_table",
        &table_path,
        listing_options.clone(),
        None,
        None,
    )
    .await?;

    println!("Creating logical plan...");
    let logical_plan = ctx.state().create_logical_plan(query).await?;

    println!("Creating physical plan...");
    let physical_plan = Arc::new(CoalescePartitionsExec::new(
        ctx.state().create_physical_plan(&logical_plan).await?,
    ));

    println!("Executing physical plan...");
    let partition = 0;
    let task_context = Arc::new(TaskContext::from(&ctx));
    let stream = physical_plan.execute(partition, task_context).unwrap();

    println!("Getting results...");
    let results: Vec<_> = stream.try_collect().await?;
    println!("Got {} record batches", results.len());

    Ok(())
}

async fn find_or_generate_files(
    data_dir: impl AsRef<Path>,
    num_files: usize,
    num_rows_per_file: usize,
) -> Result<Vec<PathBuf>> {
    // Ignore errors if the directory already exists
    let _ = std::fs::create_dir_all(data_dir.as_ref());
    let files_on_disk = find_files_on_disk(data_dir.as_ref())?;

    if files_on_disk.is_empty() {
        println!("No data files found, generating (this will take a bit)");
        generate_data(data_dir.as_ref(), num_files, num_rows_per_file).await?;
        println!("Done generating files");
        let files_on_disk = find_files_on_disk(data_dir)?;

        if files_on_disk.is_empty() {
            panic!("Tried to generate data files but there are still no files on disk");
        } else {
            println!("Using {} files now on disk", files_on_disk.len());
            Ok(files_on_disk)
        }
    } else {
        println!("Using {} files found on disk", files_on_disk.len());
        Ok(files_on_disk)
    }
}

fn find_files_on_disk(data_dir: impl AsRef<Path>) -> Result<Vec<PathBuf>> {
    Ok(std::fs::read_dir(&data_dir)?
        .filter_map(|file| {
            let path = file.unwrap().path();
            if path
                .extension()
                .map(|ext| ext == "parquet")
                .unwrap_or(false)
            {
                Some(path)
            } else {
                None
            }
        })
        .collect())
}

async fn load_data(
    store: Arc<dyn ObjectStore>,
    files_on_disk: &[PathBuf],
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    for file in files_on_disk {
        let bytes = std::fs::read(file)?;

        let path = object_store::path::Path::from_iter([
            "data",
            file.file_name().unwrap().to_str().unwrap(),
        ]);
        let payload = object_store::PutPayload::from_bytes(bytes.into());
        store
            .put_opts(&path, payload, object_store::PutOptions::default())
            .await?;
    }

    Ok(())
}

async fn generate_data(
    data_dir: impl AsRef<Path>,
    num_files: usize,
    num_rows_per_file: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let absolute = std::env::current_dir().unwrap().join(data_dir);
    let store = Arc::new(object_store::local::LocalFileSystem::new_with_prefix(
        absolute,
    )?);

    let columns = [
        ("A", DataType::Float64),
        ("B", DataType::Float64),
        ("C", DataType::Float64),
        ("D", DataType::Boolean),
        ("E", DataType::Utf8),
        ("F", DataType::Utf8),
        ("G", DataType::Utf8),
        ("H", DataType::Utf8),
        ("I", DataType::Utf8),
        ("J", DataType::Utf8),
        ("K", DataType::Utf8),
    ];

    for file_num in 1..=num_files {
        println!("Generating file {file_num} of {num_files}");
        let data = columns.iter().map(|(column_name, column_type)| {
            let column = random_data(column_type, num_rows_per_file);
            (column_name, column)
        });
        let to_write = RecordBatch::try_from_iter(data).unwrap();
        let path = object_store::path::Path::from(format!("{file_num}.parquet").as_str());
        let object_store_writer = ParquetObjectWriter::new(Arc::clone(&store) as _, path);

        let mut writer =
            AsyncArrowWriter::try_new(object_store_writer, to_write.schema(), None)?;
        writer.write(&to_write).await?;
        writer.close().await?;
    }

    Ok(())
}

fn random_data(column_type: &DataType, rows: usize) -> Arc<dyn Array> {
    let mut rng = rand::rng();
    let values = (0..rows).map(|_| random_value(&mut rng, column_type));
    ScalarValue::iter_to_array(values).unwrap()
}

fn random_value(rng: &mut ThreadRng, column_type: &DataType) -> ScalarValue {
    match column_type {
        DataType::Float64 => ScalarValue::Float64(Some(rng.random())),
        DataType::Boolean => ScalarValue::Boolean(Some(rng.random())),
        DataType::Utf8 => ScalarValue::Utf8(Some(
            rng.sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect(),
        )),
        other => unimplemented!("No random value generation implemented for {other:?}"),
    }
}
