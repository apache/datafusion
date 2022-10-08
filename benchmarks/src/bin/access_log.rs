use arrow::array::{
    Int32Builder, StringBuilder, StringDictionaryBuilder, TimestampNanosecondBuilder,
    UInt16Builder,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty;
use datafusion::common::Result;
use datafusion::physical_plan::collect;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
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

    /// Batch size when reading Parquet files
    #[structopt(short = "s", long = "batch-size", default_value = "8192")]
    batch_size: usize,

    /// Total size of generated dataset. The default scale factor of 1.0 will generate a roughly 1GB parquet file
    #[structopt(short = "s", long = "scale-factor", default_value = "1.0")]
    scale_factor: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt: Opt = Opt::from_args();
    println!("Running benchmarks with the following options: {:?}", opt);

    let config = SessionConfig::new()
        .with_target_partitions(opt.partitions)
        .with_batch_size(opt.batch_size);
    let mut ctx = SessionContext::with_config(config);

    let path = opt.path.join("logs.parquet");

    gen_data(&path, opt.scale_factor);

    ctx.register_parquet(
        "logs",
        path.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    datafusion_sql_benchmarks(&mut ctx, opt.iterations, opt.debug).await?;

    Ok(())
}

async fn datafusion_sql_benchmarks(
    ctx: &mut SessionContext,
    iterations: usize,
    debug: bool,
) -> Result<()> {
    let mut queries = HashMap::new();
    queries.insert(
        "get_requests",
        "SELECT * FROM logs WHERE request_method = 'GET'",
    );
    queries.insert(
        "get_requests_ignore_body",
        "SELECT service,host,pod,container,image,time,client_addr,request_duration_ns,request_user_agent,request_method,request_host,request_bytes,response_status,response_bytes FROM logs WHERE request_method = 'GET'"
    );
    queries.insert(
        "get_post_503",
        "SELECT * FROM logs WHERE request_method = 'POST' AND response_status = 503",
    );
    queries.insert(
        "get_post_503_ignore_body",
        "SELECT service,host,pod,container,image,time,client_addr,request_duration_ns,request_user_agent,request_method,request_host,request_bytes,response_status,response_bytes FROM logs WHERE request_method = 'POST' AND response_status = 503",
    );
    for (name, sql) in &queries {
        println!("Executing '{}'", name);
        for i in 0..iterations {
            let start = Instant::now();
            let rows = execute_sql(ctx, sql, debug).await?;
            println!(
                "Query '{}' iteration {} returned {} rows in {} ms",
                name,
                i,
                rows,
                start.elapsed().as_millis()
            );
        }
    }
    Ok(())
}

async fn execute_sql(ctx: &SessionContext, sql: &str, debug: bool) -> Result<usize> {
    let plan = ctx.create_logical_plan(sql)?;
    let plan = ctx.optimize(&plan)?;
    if debug {
        println!("Optimized logical plan:\n{:?}", plan);
    }
    let physical_plan = ctx.create_physical_plan(&plan).await?;
    let task_ctx = ctx.task_ctx();
    let result = collect(physical_plan, task_ctx).await?;

    if debug {
        pretty::print_batches(&result)?;
    }
    Ok(result.iter().map(|b| b.num_rows()).sum())
}

fn gen_data(path: &PathBuf, scale_factor: f32) {
    let generator = Generator::new();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, generator.schema.clone(), None).unwrap();

    let mut num_rows = 0;

    let num_batches = 12_f32 * scale_factor;

    for batch in generator.take(num_batches as usize) {
        writer.write(&batch).unwrap();
        num_rows += batch.num_rows();
    }
    writer.close().unwrap();

    println!("Generated test dataset with {} rows", num_rows);
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
    response_body: StringBuilder,
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
            // This column will contain large values relative to the others
            Field::new("response_body", DataType::Utf8, false),
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
        self.response_body
            .append_value(random_string(rng, 200..2000))
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
                Arc::new(self.response_body.finish()),
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
