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

use arrow::array::{
    Int32Builder, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
    UInt16Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::common::{Result, ToDFSchema};
use datafusion::config::{
    ConfigOptions, OPT_PARQUET_ENABLE_PAGE_INDEX, OPT_PARQUET_PUSHDOWN_FILTERS,
    OPT_PARQUET_REORDER_FILTERS,
};
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::{lit, or, Expr};
use datafusion::optimizer::utils::disjunction;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::file_format::{FileScanConfig, ParquetExec};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::{col, SessionConfig, SessionContext};
use object_store::path::Path;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[derive(Debug, StructOpt)]
#[structopt(name = "Benchmarks", about = "Apache Arrow Rust Benchmarks.")]
struct Opt {
    /// Activate debug mode to see query results
    #[structopt(short, long)]
    debug: bool,

    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    iterations: usize,

    /// Number of partitions to process in parallel
    #[structopt(long = "partitions", default_value = "2")]
    partitions: usize,

    /// Path to folder where access log file will be generated
    #[structopt(parse(from_os_str), required = true, short = "p", long = "path")]
    path: PathBuf,

    /// Data page size of the generated parquet file
    #[structopt(long = "page-size")]
    page_size: Option<usize>,

    /// Data page size of the generated parquet file
    #[structopt(long = "row-group-size")]
    row_group_size: Option<usize>,

    /// Total size of generated dataset. The default scale factor of 1.0 will generate a roughly 1GB parquet file
    #[structopt(short = "s", long = "scale-factor", default_value = "1.0")]
    scale_factor: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    println!("Running benchmarks with the following options: {:?}", opt);

    let config = SessionConfig::new().with_target_partitions(opt.partitions);
    let mut ctx = SessionContext::with_config(config);

    let path = opt.path.join("logs.parquet");

    let (object_store_url, object_meta) =
        gen_data(path, opt.scale_factor, opt.page_size, opt.row_group_size)?;

    run_benchmarks(
        &mut ctx,
        object_store_url.clone(),
        object_meta.clone(),
        opt.iterations,
        opt.debug,
    )
    .await?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ParquetScanOptions {
    pushdown_filters: bool,
    reorder_filters: bool,
    enable_page_index: bool,
}

async fn run_benchmarks(
    ctx: &mut SessionContext,
    object_store_url: ObjectStoreUrl,
    object_meta: ObjectMeta,
    iterations: usize,
    debug: bool,
) -> Result<()> {
    let scan_options_matrix = vec![
        ParquetScanOptions {
            pushdown_filters: false,
            reorder_filters: false,
            enable_page_index: false,
        },
        ParquetScanOptions {
            pushdown_filters: true,
            reorder_filters: true,
            enable_page_index: true,
        },
        ParquetScanOptions {
            pushdown_filters: true,
            reorder_filters: true,
            enable_page_index: false,
        },
    ];

    let filter_matrix = vec![
        // Selective-ish filter
        col("request_method").eq(lit("GET")),
        // Non-selective filter
        col("request_method").not_eq(lit("GET")),
        // Basic conjunction
        col("request_method")
            .eq(lit("POST"))
            .and(col("response_status").eq(lit(503_u16))),
        // Nested filters
        col("request_method").eq(lit("POST")).and(or(
            col("response_status").eq(lit(503_u16)),
            col("response_status").eq(lit(403_u16)),
        )),
        // Many filters
        disjunction([
            col("request_method").not_eq(lit("GET")),
            col("response_status").eq(lit(400_u16)),
            // TODO this fails in the FilterExec with Error: Internal("The type of Dictionary(Int32, Utf8) = Utf8 of binary physical should be same")
            // col("service").eq(lit("backend")),
        ])
        .unwrap(),
        // Filter everything
        col("response_status").eq(lit(429_u16)),
        // Filter nothing
        col("response_status").gt(lit(0_u16)),
    ];

    for filter_expr in &filter_matrix {
        println!("Executing with filter '{}'", filter_expr);
        for scan_options in &scan_options_matrix {
            println!("Using scan options {:?}", scan_options);
            for i in 0..iterations {
                let start = Instant::now();
                let rows = exec_scan(
                    ctx,
                    object_store_url.clone(),
                    object_meta.clone(),
                    filter_expr.clone(),
                    scan_options.clone(),
                    debug,
                )
                .await?;
                println!(
                    "Iteration {} returned {} rows in {} ms",
                    i,
                    rows,
                    start.elapsed().as_millis()
                );
            }
        }
        println!("\n");
    }
    Ok(())
}

async fn exec_scan(
    ctx: &SessionContext,
    object_store_url: ObjectStoreUrl,
    object_meta: ObjectMeta,
    filter: Expr,
    scan_options: ParquetScanOptions,
    debug: bool,
) -> Result<usize> {
    let schema = BatchBuilder::schema();

    let ParquetScanOptions {
        pushdown_filters,
        reorder_filters,
        enable_page_index,
    } = scan_options;

    let mut config_options = ConfigOptions::new();
    config_options.set_bool(OPT_PARQUET_PUSHDOWN_FILTERS, pushdown_filters);
    config_options.set_bool(OPT_PARQUET_REORDER_FILTERS, reorder_filters);
    config_options.set_bool(OPT_PARQUET_ENABLE_PAGE_INDEX, enable_page_index);

    let scan_config = FileScanConfig {
        object_store_url,
        file_schema: schema.clone(),
        file_groups: vec![vec![PartitionedFile {
            object_meta,
            partition_values: vec![],
            range: None,
            extensions: None,
        }]],
        statistics: Default::default(),
        projection: None,
        limit: None,
        table_partition_cols: vec![],
        config_options: config_options.into_shareable(),
    };

    let df_schema = schema.clone().to_dfschema()?;

    let physical_filter_expr = create_physical_expr(
        &filter,
        &df_schema,
        schema.as_ref(),
        &ExecutionProps::default(),
    )?;

    let parquet_exec = Arc::new(ParquetExec::new(scan_config, Some(filter), None));

    let exec = Arc::new(FilterExec::try_new(physical_filter_expr, parquet_exec)?);

    let task_ctx = ctx.task_ctx();
    let result = collect(exec, task_ctx).await?;

    if debug {
        pretty::print_batches(&result)?;
    }
    Ok(result.iter().map(|b| b.num_rows()).sum())
}

fn gen_data(
    path: PathBuf,
    scale_factor: f32,
    page_size: Option<usize>,
    row_group_size: Option<usize>,
) -> Result<(ObjectStoreUrl, ObjectMeta)> {
    let generator = Generator::new();

    let file = File::create(&path).unwrap();

    let mut props_builder = WriterProperties::builder();

    if let Some(s) = page_size {
        props_builder = props_builder
            .set_data_pagesize_limit(s)
            .set_write_batch_size(s);
    }

    if let Some(s) = row_group_size {
        props_builder = props_builder.set_max_row_group_size(s);
    }

    let mut writer =
        ArrowWriter::try_new(file, generator.schema.clone(), Some(props_builder.build()))
            .unwrap();

    let mut num_rows = 0;

    let num_batches = 100_f32 * scale_factor;

    for batch in generator.take(num_batches as usize) {
        writer.write(&batch).unwrap();
        writer.flush()?;
        num_rows += batch.num_rows();
    }
    writer.close().unwrap();

    println!("Generated test dataset with {} rows", num_rows);

    let size = std::fs::metadata(&path)?.len() as usize;

    let canonical_path = path.canonicalize()?;

    let object_store_url =
        ListingTableUrl::parse(canonical_path.to_str().unwrap_or_default())?
            .object_store();

    let object_meta = ObjectMeta {
        location: Path::parse(canonical_path.to_str().unwrap_or_default())?,
        last_modified: Default::default(),
        size,
    };

    Ok((object_store_url, object_meta))
}

#[derive(Default)]
struct BatchBuilder {
    service: StringDictionaryBuilder<Int32Type>,
    host: StringDictionaryBuilder<Int32Type>,
    pod: StringDictionaryBuilder<Int32Type>,
    container: StringDictionaryBuilder<Int32Type>,
    image: StringDictionaryBuilder<Int32Type>,
    time: TimestampNanosecondBuilder,
    client_addr: StringBuilder,
    request_duration: Int32Builder,
    request_user_agent: StringBuilder,
    request_method: StringBuilder,
    request_host: StringBuilder,
    request_bytes: Int32Builder,
    response_bytes: Int32Builder,
    response_status: UInt16Builder,
}

impl BatchBuilder {
    fn schema() -> SchemaRef {
        let utf8_dict =
            || DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));

        Arc::new(Schema::new(vec![
            Field::new("service", utf8_dict(), true),
            Field::new("host", utf8_dict(), false),
            Field::new("pod", utf8_dict(), false),
            Field::new("container", utf8_dict(), false),
            Field::new("image", utf8_dict(), false),
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("client_addr", DataType::Utf8, true),
            Field::new("request_duration_ns", DataType::Int32, false),
            Field::new("request_user_agent", DataType::Utf8, true),
            Field::new("request_method", DataType::Utf8, true),
            Field::new("request_host", DataType::Utf8, true),
            Field::new("request_bytes", DataType::Int32, true),
            Field::new("response_bytes", DataType::Int32, true),
            Field::new("response_status", DataType::UInt16, false),
        ]))
    }

    fn append(&mut self, rng: &mut StdRng, host: &str, service: &str) {
        let num_pods = rng.gen_range(1..15);
        let pods = generate_sorted_strings(rng, num_pods, 30..40);
        for pod in pods {
            for container_idx in 0..rng.gen_range(1..3) {
                let container = format!("{}_container_{}", service, container_idx);
                let image = format!(
                    "{}@sha256:30375999bf03beec2187843017b10c9e88d8b1a91615df4eb6350fb39472edd9",
                    container
                );

                let num_entries = rng.gen_range(1024..8192);
                for i in 0..num_entries {
                    let time = i as i64 * 1024;
                    self.append_row(rng, host, &pod, service, &container, &image, time);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn append_row(
        &mut self,
        rng: &mut StdRng,
        host: &str,
        pod: &str,
        service: &str,
        container: &str,
        image: &str,
        time: i64,
    ) {
        let methods = &["GET", "PUT", "POST", "HEAD", "PATCH", "DELETE"];
        let status = &[200, 204, 400, 503, 403];

        self.service.append(service).unwrap();
        self.host.append(host).unwrap();
        self.pod.append(pod).unwrap();
        self.container.append(container).unwrap();
        self.image.append(image).unwrap();
        self.time.append_value(time);

        self.client_addr.append_value(format!(
            "{}.{}.{}.{}",
            rng.gen::<u8>(),
            rng.gen::<u8>(),
            rng.gen::<u8>(),
            rng.gen::<u8>()
        ));
        self.request_duration.append_value(rng.gen());
        self.request_user_agent
            .append_value(random_string(rng, 20..100));
        self.request_method
            .append_value(methods[rng.gen_range(0..methods.len())]);
        self.request_host
            .append_value(format!("https://{}.mydomain.com", service));

        self.request_bytes
            .append_option(rng.gen_bool(0.9).then(|| rng.gen()));
        self.response_bytes
            .append_option(rng.gen_bool(0.9).then(|| rng.gen()));
        self.response_status
            .append_value(status[rng.gen_range(0..status.len())]);
    }

    fn finish(mut self, schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(self.service.finish()),
                Arc::new(self.host.finish()),
                Arc::new(self.pod.finish()),
                Arc::new(self.container.finish()),
                Arc::new(self.image.finish()),
                Arc::new(self.time.finish()),
                Arc::new(self.client_addr.finish()),
                Arc::new(self.request_duration.finish()),
                Arc::new(self.request_user_agent.finish()),
                Arc::new(self.request_method.finish()),
                Arc::new(self.request_host.finish()),
                Arc::new(self.request_bytes.finish()),
                Arc::new(self.response_bytes.finish()),
                Arc::new(self.response_status.finish()),
            ],
        )
        .unwrap()
    }
}

fn random_string(rng: &mut StdRng, len_range: Range<usize>) -> String {
    let len = rng.gen_range(len_range);
    (0..len)
        .map(|_| rng.gen_range(b'a'..=b'z') as char)
        .collect::<String>()
}

fn generate_sorted_strings(
    rng: &mut StdRng,
    count: usize,
    str_len: Range<usize>,
) -> Vec<String> {
    let mut strings: Vec<_> = (0..count)
        .map(|_| random_string(rng, str_len.clone()))
        .collect();

    strings.sort_unstable();
    strings
}

/// Generates sorted RecordBatch with an access log style schema for a single host
#[derive(Debug)]
struct Generator {
    schema: SchemaRef,
    rng: StdRng,
    host_idx: usize,
}

impl Generator {
    fn new() -> Self {
        let seed = [
            1, 0, 0, 0, 23, 0, 3, 0, 200, 1, 0, 0, 210, 30, 8, 0, 1, 0, 21, 0, 6, 0, 0,
            0, 0, 0, 5, 0, 0, 0, 0, 0,
        ];

        Self {
            schema: BatchBuilder::schema(),
            host_idx: 0,
            rng: StdRng::from_seed(seed),
        }
    }
}

impl Iterator for Generator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut builder = BatchBuilder::default();

        let host = format!(
            "i-{:016x}.ec2.internal",
            self.host_idx * 0x7d87f8ed5c5 + 0x1ec3ca3151468928
        );
        self.host_idx += 1;

        for service in &["frontend", "backend", "database", "cache"] {
            if self.rng.gen_bool(0.5) {
                continue;
            }
            builder.append(&mut self.rng, &host, service);
        }
        Some(builder.finish(Arc::clone(&self.schema)))
    }
}
