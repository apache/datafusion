use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::prelude::SessionContext;
use datafusion_catalog_listing::helpers::{
    pruned_partition_list, pruned_partition_list_all,
};
use datafusion_datasource::ListingTableUrl;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::minio;

/// Concurrency limit for uploading files to object store
const CONCURRENT_UPLOADS: usize = 100;

/// Configuration for generating a partition structure
#[derive(Debug, Clone)]
struct PartitionConfig {
    /// Number of partition levels (e.g., 1 for /a=1/, 3 for /a=1/b=100/c=1000/)
    depth: usize,
    /// Number of distinct values at each partition level
    breadth: usize,
    /// Number of files in each leaf partition
    files_per_partition: usize,
}

impl PartitionConfig {
    fn new(depth: usize, breadth: usize, files_per_partition: usize) -> Self {
        Self {
            depth,
            breadth,
            files_per_partition,
        }
    }

    /// Calculate total number of leaf partitions
    fn total_partitions(&self) -> usize {
        if self.depth == 0 {
            1
        } else {
            self.breadth.pow(self.depth as u32)
        }
    }

    /// Calculate total number of files
    fn total_files(&self) -> usize {
        self.total_partitions() * self.files_per_partition
    }
}

/// Generate partition paths and populate the provided store
fn generate_partition_structure(store: &dyn ObjectStore, config: &PartitionConfig) {
    if config.depth == 0 {
        // No partitions, just files at root
        for file_idx in 0..config.files_per_partition {
            let path = Path::from(format!("tablepath/data_{}.parquet", file_idx));
            futures::executor::block_on(store.put(&path, vec![0; 5].into())).unwrap();
        }
        return;
    }

    // Generate all partition combinations recursively
    let mut current_partition = Vec::new();
    generate_partitions_recursive(store, config, 0, &mut current_partition);
}

/// Recursively generate partition paths
fn generate_partitions_recursive(
    store: &dyn ObjectStore,
    config: &PartitionConfig,
    current_depth: usize,
    current_partition: &mut Vec<String>,
) {
    if current_depth == config.depth {
        // We've reached a leaf - create files
        let partition_path = if current_partition.is_empty() {
            "tablepath".to_string()
        } else {
            format!("tablepath/{}", current_partition.join("/"))
        };

        for file_idx in 0..config.files_per_partition {
            let file_path = format!("{}/data_{}.parquet", partition_path, file_idx);
            let path = Path::from(file_path);
            futures::executor::block_on(store.put(&path, vec![0; 5].into())).unwrap();
        }
        return;
    }

    // Generate partition values for current level
    // Use pattern: a=1, a=2, ... for first level
    //              b=100, b=200, ... for second level
    //              c=1000, c=2000, ... for third level, etc.
    let partition_name = (b'a' + current_depth as u8) as char;
    let multiplier = 10_usize.pow(current_depth as u32);

    for value_idx in 0..config.breadth {
        let partition_value = (value_idx + 1) * multiplier;
        let partition_part = format!("{}={}", partition_name, partition_value);

        current_partition.push(partition_part);
        generate_partitions_recursive(
            store,
            config,
            current_depth + 1,
            current_partition,
        );
        current_partition.pop();
    }
}

/// Extract partition column definitions from config
fn get_partition_columns(depth: usize) -> Vec<(String, arrow::datatypes::DataType)> {
    (0..depth)
        .map(|i| {
            let name = format!("{}", (b'a' + i as u8) as char);
            (name, arrow::datatypes::DataType::Int32)
        })
        .collect()
}

/// Pre-setup structure for benchmarks - contains everything needed except the actual method call
struct BenchmarkSetup {
    store: Arc<dyn ObjectStore>,
    table_url: ListingTableUrl,
    partition_cols: Vec<(String, arrow::datatypes::DataType)>,
    expected_files: usize,
    ctx: Arc<SessionContext>,
}

impl BenchmarkSetup {
    fn new(store: Arc<dyn ObjectStore>, config: &PartitionConfig) -> Self {
        generate_partition_structure(store.as_ref(), config);
        let table_url = ListingTableUrl::parse("file:///tablepath/").unwrap();
        let partition_cols = get_partition_columns(config.depth);
        let expected_files = config.total_files();
        let ctx = Arc::new(SessionContext::new());

        Self {
            store,
            table_url,
            partition_cols,
            expected_files,
            ctx,
        }
    }

    fn new_with_path(
        store: Arc<dyn ObjectStore>,
        config: &PartitionConfig,
        path_prefix: &str,
    ) -> Self {
        Self::generate_with_path(store.as_ref(), config, path_prefix);
        let table_url =
            ListingTableUrl::parse(&format!("file:///{}/", path_prefix)).unwrap();
        let partition_cols = get_partition_columns(config.depth);
        let expected_files = config.total_files();
        let ctx = Arc::new(SessionContext::new());

        Self {
            store,
            table_url,
            partition_cols,
            expected_files,
            ctx,
        }
    }

    fn generate_with_path(
        store: &dyn ObjectStore,
        config: &PartitionConfig,
        path_prefix: &str,
    ) {
        // Collect all paths first
        let paths = Self::collect_paths(config, path_prefix);

        // Upload concurrently using async runtime
        futures::executor::block_on(Self::upload_files_concurrent(store, paths))
            .expect("Failed to upload files");
    }

    /// Collect all file paths that need to be created without uploading
    fn collect_paths(config: &PartitionConfig, path_prefix: &str) -> Vec<Path> {
        let mut paths = Vec::new();

        if config.depth == 0 {
            for file_idx in 0..config.files_per_partition {
                paths.push(Path::from(format!(
                    "{}/data_{}.parquet",
                    path_prefix, file_idx
                )));
            }
            return paths;
        }

        let mut current_partition = Vec::new();
        Self::collect_paths_recursive(
            config,
            0,
            &mut current_partition,
            path_prefix,
            &mut paths,
        );
        paths
    }

    /// Recursively collect partition paths
    fn collect_paths_recursive(
        config: &PartitionConfig,
        current_depth: usize,
        current_partition: &mut Vec<String>,
        path_prefix: &str,
        paths: &mut Vec<Path>,
    ) {
        if current_depth == config.depth {
            let partition_path = if current_partition.is_empty() {
                path_prefix.to_string()
            } else {
                format!("{}/{}", path_prefix, current_partition.join("/"))
            };

            for file_idx in 0..config.files_per_partition {
                let file_path = format!("{}/data_{}.parquet", partition_path, file_idx);
                paths.push(Path::from(file_path));
            }
            return;
        }

        let partition_name = (b'a' + current_depth as u8) as char;
        let multiplier = 10_usize.pow(current_depth as u32);

        for value_idx in 0..config.breadth {
            let partition_value = (value_idx + 1) * multiplier;
            let partition_part = format!("{}={}", partition_name, partition_value);

            current_partition.push(partition_part);
            Self::collect_paths_recursive(
                config,
                current_depth + 1,
                current_partition,
                path_prefix,
                paths,
            );
            current_partition.pop();
        }
    }

    /// Upload files concurrently with a concurrency limit
    async fn upload_files_concurrent(
        store: &dyn ObjectStore,
        paths: Vec<Path>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let total = paths.len();
        eprintln!(
            "Uploading {} files with concurrency limit of {}...",
            total, CONCURRENT_UPLOADS
        );

        // Use buffer_unordered to upload with controlled concurrency
        let results: Vec<_> = stream::iter(paths)
            .map(|path| async move { store.put(&path, vec![0; 5].into()).await })
            .buffer_unordered(CONCURRENT_UPLOADS)
            .collect()
            .await;

        // Check for any errors
        for result in results {
            result?;
        }

        eprintln!("Successfully uploaded {} files", total);
        Ok(())
    }
}

/// Benchmark pruned_partition_list with a given configuration (total collection time)
/// This version has setup already done - only times the actual method call
async fn run_pruned_partition_list_benchmark(setup: &BenchmarkSetup) {
    let state = setup.ctx.state();

    let result = pruned_partition_list(
        &state,
        setup.store.as_ref(),
        &setup.table_url,
        &[], // No filters
        ".parquet",
        &setup.partition_cols,
    )
    .await
    .expect("pruned_partition_list failed");

    let files: Vec<_> = result.try_collect().await.expect("failed to collect files");

    // Verify we got the expected number of files
    assert_eq!(
        files.len(),
        setup.expected_files,
        "Expected {} files but got {}",
        setup.expected_files,
        files.len()
    );
}

/// Benchmark pruned_partition_list time-to-first-result
/// This version has setup already done - only times the actual method call + first result
async fn run_pruned_partition_list_ttfr(setup: &BenchmarkSetup) {
    let state = setup.ctx.state();

    let mut result = pruned_partition_list(
        &state,
        setup.store.as_ref(),
        &setup.table_url,
        &[], // No filters
        ".parquet",
        &setup.partition_cols,
    )
    .await
    .expect("pruned_partition_list failed");

    // Only get the first result - measures time to first item
    let first = result.try_next().await.expect("failed to get first result");
    assert!(first.is_some(), "Expected at least one result");
}

/// Benchmark pruned_partition_list_all with a given configuration (total collection time)
/// This version has setup already done - only times the actual method call
async fn run_pruned_partition_list_all_benchmark(setup: &BenchmarkSetup) {
    let state = setup.ctx.state();

    let result = pruned_partition_list_all(
        &state,
        setup.store.as_ref(),
        &setup.table_url,
        &[], // No filters
        ".parquet",
        &setup.partition_cols,
    )
    .await
    .expect("pruned_partition_list_all failed");

    let files: Vec<_> = result.try_collect().await.expect("failed to collect files");

    // Verify we got the expected number of files
    assert_eq!(
        files.len(),
        setup.expected_files,
        "Expected {} files but got {}",
        setup.expected_files,
        files.len()
    );
}

/// Benchmark pruned_partition_list_all time-to-first-result
/// This version has setup already done - only times the actual method call + first result
async fn run_pruned_partition_list_all_ttfr(setup: &BenchmarkSetup) {
    let state = setup.ctx.state();

    let mut result = pruned_partition_list_all(
        &state,
        setup.store.as_ref(),
        &setup.table_url,
        &[], // No filters
        ".parquet",
        &setup.partition_cols,
    )
    .await
    .expect("pruned_partition_list_all failed");

    // Only get the first result - measures time to first item
    let first = result.try_next().await.expect("failed to get first result");
    assert!(first.is_some(), "Expected at least one result");
}

/// Benchmark varying partition depth with constant total files (in-memory)
/// Goal: Isolate the impact of partition depth on performance
fn bench_depth_variation_constant_files_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("depth_variation_constant_files_inmemory");

    const TOTAL_FILES: usize = 100_000;
    const BREADTH: usize = 10;

    for depth in [1, 2, 3, 4, 5] {
        let partitions = BREADTH.pow(depth as u32);
        let files_per_partition = TOTAL_FILES / partitions;
        let config = PartitionConfig::new(depth, BREADTH, files_per_partition);

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let setup = BenchmarkSetup::new(store, &config);
        let description = format!(
            "depth={}_partitions={}_files={}",
            depth, partitions, TOTAL_FILES
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark varying partition breadth with constant total files (in-memory)
/// Goal: Isolate the impact of partition breadth on performance
fn bench_breadth_variation_constant_files_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("breadth_variation_constant_files_inmemory");

    const DEPTH: usize = 3;

    let configs = vec![
        (5, 800, 100_000),
        (10, 100, 100_000),
        (20, 12, 96_000),
        (30, 3, 81_000),
        (32, 3, 98_304),
    ];

    for (breadth, files_per_partition, total_files) in configs {
        let config = PartitionConfig::new(DEPTH, breadth, files_per_partition);
        let partitions = config.total_partitions();

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let setup = BenchmarkSetup::new(store, &config);
        let description = format!(
            "breadth={}_partitions={}_files={}",
            breadth, partitions, total_files
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark varying total file count with constant partition structure (in-memory)
/// Goal: Isolate the impact of total file count on performance
fn bench_scale_constant_partitions_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale_constant_partitions_inmemory");

    const DEPTH: usize = 5;
    const BREADTH: usize = 8;
    const PARTITIONS: usize = 32_768; // 8^5

    let files_per_partition_configs = vec![1, 5, 15];

    for files_per_partition in files_per_partition_configs {
        let config = PartitionConfig::new(DEPTH, BREADTH, files_per_partition);
        let total_files = config.total_files();

        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let setup = BenchmarkSetup::new(store, &config);
        let description = format!(
            "files_per_part={}_partitions={}_total={}",
            files_per_partition, PARTITIONS, total_files
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Create a MinIO container and return an S3 ObjectStore connected to it
async fn create_minio_store() -> Result<
    (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>),
    Box<dyn std::error::Error>,
> {
    use testcontainers::core::{CmdWaitFor, ExecCommand};

    const MINIO_USER: &str = "benchuser";
    const MINIO_PASSWORD: &str = "benchpassword";
    const BUCKET_NAME: &str = "benchmark-data";

    // Start MinIO container
    let container = minio::MinIO::default()
        .with_env_var("MINIO_ROOT_USER", MINIO_USER)
        .with_env_var("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
        .start()
        .await?;

    let port = container.get_host_port_ipv4(9000).await?;

    // Execute commands to set up MinIO, similar to datafusion-cli tests
    let commands = [
        ExecCommand::new(["/usr/bin/mc", "ready", "local"]),
        ExecCommand::new([
            "/usr/bin/mc",
            "alias",
            "set",
            "localminio",
            "http://localhost:9000",
            MINIO_USER,
            MINIO_PASSWORD,
        ]),
        ExecCommand::new(["/usr/bin/mc", "mb", "localminio/benchmark-data"]),
    ];

    for command in commands {
        let command =
            command.with_cmd_ready_condition(CmdWaitFor::Exit { code: Some(0) });
        let cmd_ref = format!("{command:?}");

        if let Err(e) = container.exec(command).await {
            let stdout = container.stdout_to_vec().await.unwrap_or_default();
            let stderr = container.stderr_to_vec().await.unwrap_or_default();

            return Err(format!(
                "Failed to execute command: {}\nError: {}\nStdout: {:?}\nStderr: {:?}",
                cmd_ref,
                e,
                String::from_utf8_lossy(&stdout),
                String::from_utf8_lossy(&stderr)
            )
            .into());
        }
    }

    // Build S3 store pointing to MinIO
    let s3 = AmazonS3Builder::new()
        .with_access_key_id(MINIO_USER)
        .with_secret_access_key(MINIO_PASSWORD)
        .with_endpoint(format!("http://localhost:{port}"))
        .with_bucket_name(BUCKET_NAME)
        .with_allow_http(true)
        .build()?;

    Ok((Arc::new(s3), container))
}

/// Benchmark varying partition depth with constant total files (S3/MinIO backend)
/// Goal: Isolate the impact of partition depth on performance
fn bench_depth_variation_constant_files_s3(c: &mut Criterion) {
    let mut group = c.benchmark_group("depth_variation_constant_files_s3");
    group.sample_size(10);

    const TOTAL_FILES: usize = 100_000;
    const BREADTH: usize = 10;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    // Shared container - initialized once when first benchmark runs
    type ContainerData = (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>);
    let container_cell: Arc<std::sync::OnceLock<ContainerData>> =
        Arc::new(std::sync::OnceLock::new());

    // depth=1: 10 partitions × 10,000 files = 100,000 files
    // depth=2: 100 partitions × 1,000 files = 100,000 files
    // depth=3: 1,000 partitions × 100 files = 100,000 files
    // depth=4: 10,000 partitions × 10 files = 100,000 files
    // depth=5: 100,000 partitions × 1 file = 100,000 files
    for (idx, depth) in [1, 2, 3, 4, 5].iter().enumerate() {
        let partitions = BREADTH.pow(*depth as u32);
        let files_per_partition = TOTAL_FILES / partitions;
        let config = PartitionConfig::new(*depth, BREADTH, files_per_partition);
        let table_path = format!("depth_const_{}", idx);
        let description = format!(
            "depth={}_partitions={}_files={}",
            depth, partitions, TOTAL_FILES
        );

        // Per-benchmark lazy setup - shared across all 4 benchmark variants
        let setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
            Arc::new(std::sync::OnceLock::new());

        // original/total - initializes setup on first run
        let setup_cell_clone = Arc::clone(&setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &(
                setup_cell_clone,
                container_cell_clone,
                config.clone(),
                table_path.clone(),
            ),
            |b, (setup_cell, container_cell, config, table_path)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(Arc::clone(store), config, table_path)
                });
                b.to_async(runtime_ref)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        // list_all/total - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        // original/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        // list_all/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark varying partition breadth with constant total files (S3/MinIO backend)
/// Goal: Isolate the impact of partition breadth on performance
fn bench_breadth_variation_constant_files_s3(c: &mut Criterion) {
    let mut group = c.benchmark_group("breadth_variation_constant_files_s3");
    group.sample_size(10);

    const DEPTH: usize = 3;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    // Shared container - initialized once when first benchmark runs
    type ContainerData = (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>);
    let container_cell: Arc<std::sync::OnceLock<ContainerData>> =
        Arc::new(std::sync::OnceLock::new());

    // breadth=5: 125 partitions × 800 files = 100,000 files
    // breadth=10: 1,000 partitions × 100 files = 100,000 files
    // breadth=20: 8,000 partitions × 12 files = 96,000 files
    // breadth=30: 27,000 partitions × 3 files = 81,000 files
    // breadth=32: 32,768 partitions × 3 files = 98,304 files
    let configs = vec![
        (5, 800, 100_000),
        (10, 100, 100_000),
        (20, 12, 96_000),
        (30, 3, 81_000),
        (32, 3, 98_304),
    ];

    for (idx, (breadth, files_per_partition, total_files)) in configs.iter().enumerate() {
        let config = PartitionConfig::new(DEPTH, *breadth, *files_per_partition);
        let partitions = config.total_partitions();
        let table_path = format!("breadth_const_{}", idx);
        let description = format!(
            "breadth={}_partitions={}_files={}",
            breadth, partitions, total_files
        );

        // Per-benchmark lazy setup - shared across all 4 benchmark variants
        let setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
            Arc::new(std::sync::OnceLock::new());

        // original/total - initializes setup on first run
        let setup_cell_clone = Arc::clone(&setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &(
                setup_cell_clone,
                container_cell_clone,
                config.clone(),
                table_path.clone(),
            ),
            |b, (setup_cell, container_cell, config, table_path)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(Arc::clone(store), config, table_path)
                });
                b.to_async(runtime_ref)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        // list_all/total - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        // original/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        // list_all/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark combined realistic scenarios with S3/MinIO backend
fn bench_combined_scenarios_s3(c: &mut Criterion) {
    let mut group = c.benchmark_group("combined_scenarios_s3");
    group.sample_size(10);

    let scenarios = vec![
        ("tiny", PartitionConfig::new(5, 1, 1)),
        ("small", PartitionConfig::new(1, 100, 1)),
        ("medium", PartitionConfig::new(2, 50, 10)),
        ("large", PartitionConfig::new(3, 20, 50)),
        ("deep", PartitionConfig::new(5, 5, 10)),
    ];

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    // Shared container - initialized once when first benchmark runs
    type ContainerData = (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>);
    let container_cell: Arc<std::sync::OnceLock<ContainerData>> =
        Arc::new(std::sync::OnceLock::new());

    for (idx, (name, config)) in scenarios.iter().enumerate() {
        let table_path = format!("scenario_bench_{}", idx);
        let description = format!(
            "{name}_d{}_b{}_f{}_partitions={}_files={}",
            config.depth,
            config.breadth,
            config.files_per_partition,
            config.total_partitions(),
            config.total_files()
        );

        // Per-benchmark lazy setup - shared across all 4 benchmark variants
        let setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
            Arc::new(std::sync::OnceLock::new());

        // original/total - initializes setup on first run
        let setup_cell_clone = Arc::clone(&setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{description}")),
            &(
                setup_cell_clone,
                container_cell_clone,
                config.clone(),
                table_path.clone(),
            ),
            |b, (setup_cell, container_cell, config, table_path)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(Arc::clone(store), config, table_path)
                });
                b.to_async(runtime_ref)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        // list_all/total - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{description}")),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        // original/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{description}")),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        // list_all/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{description}")),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark varying total file count with constant partition structure (S3/MinIO backend)
/// Goal: Isolate the impact of total file count on performance
fn bench_scale_constant_partitions_s3(c: &mut Criterion) {
    let mut group = c.benchmark_group("scale_constant_partitions_s3");
    group.sample_size(10);

    const DEPTH: usize = 5;
    const BREADTH: usize = 8;
    const PARTITIONS: usize = 32_768; // 8^5

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();
    // Shared container - initialized once when first benchmark runs
    type ContainerData = (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>);
    let container_cell: Arc<std::sync::OnceLock<ContainerData>> =
        Arc::new(std::sync::OnceLock::new());

    // 1 file: 32,768 files
    // 5 files: 163,840 files
    // 15 files: 491,520 files (~500k)
    let files_per_partition_configs = vec![1, 5, 15];

    for (idx, files_per_partition) in files_per_partition_configs.iter().enumerate() {
        let config = PartitionConfig::new(DEPTH, BREADTH, *files_per_partition);
        let total_files = config.total_files();
        let table_path = format!("scale_{}", idx);
        let description = format!(
            "files_per_part={}_partitions={}_total={}",
            files_per_partition, PARTITIONS, total_files
        );

        // Per-benchmark lazy setup - shared across all 4 benchmark variants
        let setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
            Arc::new(std::sync::OnceLock::new());

        // original/total - initializes setup on first run
        let setup_cell_clone = Arc::clone(&setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{}", description)),
            &(
                setup_cell_clone,
                container_cell_clone,
                config.clone(),
                table_path.clone(),
            ),
            |b, (setup_cell, container_cell, config, table_path)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(Arc::clone(store), config, table_path)
                });
                b.to_async(runtime_ref)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        // list_all/total - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        // original/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        // list_all/ttfr - reuses setup
        let setup_cell_clone = Arc::clone(&setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{}", description)),
            &setup_cell_clone,
            |b, setup_cell| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark combined realistic scenarios (in-memory)
fn bench_combined_scenarios_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("combined_scenarios_inmemory");

    let scenarios = vec![
        ("tiny", PartitionConfig::new(5, 1, 1)),
        ("small", PartitionConfig::new(1, 100, 1)),
        ("medium", PartitionConfig::new(2, 50, 10)),
        ("large", PartitionConfig::new(3, 20, 50)),
        ("deep", PartitionConfig::new(5, 5, 10)),
    ];

    for (name, config) in scenarios {
        let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
        let setup = BenchmarkSetup::new(store, &config);
        let description = format!(
            "{name}_d{}_b{}_f{}_partitions={}_files={}",
            config.depth,
            config.breadth,
            config.files_per_partition,
            config.total_partitions(),
            config.total_files()
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/total/{description}")),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/total/{description}")),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_benchmark(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("original/ttfr/{description}")),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_ttfr(setup));
            },
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("list_all/ttfr/{description}")),
            &setup,
            |b, setup| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime)
                    .iter(|| run_pruned_partition_list_all_ttfr(setup));
            },
        );
    }

    group.finish();
}

/// Benchmark partition filtering with different filter types (in-memory)
/// Goal: Test partition pruning effectiveness
fn bench_partition_filtering_inmemory(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_filtering_inmemory");

    // Small case: depth=2, breadth=10, 100 partitions, 10,000 files
    let small_config = PartitionConfig::new(2, 10, 100);
    let small_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let small_setup = BenchmarkSetup::new(small_store, &small_config);

    // Define filter scenarios
    let no_filter = vec![];
    let restrictive_filter = vec![datafusion_expr::col("a")
        .eq(datafusion_expr::lit(10))
        .and(datafusion_expr::col("b").eq(datafusion_expr::lit(100)))];
    let broad_filter = vec![datafusion_expr::col("a").lt_eq(datafusion_expr::lit(50))];

    // Small case benchmarks
    for (filter_name, filters, desc) in [
        ("no_filter", &no_filter, "partitions=100_files=10000"),
        (
            "restrictive_filter",
            &restrictive_filter,
            "partitions=1_files=100_pruned=99%",
        ),
        (
            "broad_filter",
            &broad_filter,
            "partitions=50_files=5000_pruned=50%",
        ),
    ] {
        // Original implementation - total time
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/total/small/{}/{}",
                filter_name, desc
            )),
            &(&small_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // list_all implementation - total time
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/total/small/{}/{}",
                filter_name, desc
            )),
            &(&small_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // Original implementation - time to first result
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/ttfr/small/{}/{}",
                filter_name, desc
            )),
            &(&small_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_next().await.unwrap()
                });
            },
        );

        // list_all implementation - time to first result
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/ttfr/small/{}/{}",
                filter_name, desc
            )),
            &(&small_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_next().await.unwrap()
                });
            },
        );
    }

    // Large case: depth=5, breadth=8, 32,768 partitions, 32,768 files
    let large_config = PartitionConfig::new(5, 8, 1);
    let large_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let large_setup = BenchmarkSetup::new(large_store, &large_config);

    let no_filter = vec![];
    let restrictive_filter = vec![datafusion_expr::col("a")
        .eq(datafusion_expr::lit(1))
        .and(datafusion_expr::col("b").eq(datafusion_expr::lit(8)))
        .and(datafusion_expr::col("c").eq(datafusion_expr::lit(64)))
        .and(datafusion_expr::col("d").eq(datafusion_expr::lit(512)))
        .and(datafusion_expr::col("e").eq(datafusion_expr::lit(4096)))];
    let broad_filter = vec![datafusion_expr::col("a").lt_eq(datafusion_expr::lit(4))];

    // Large case benchmarks
    for (filter_name, filters, desc) in [
        ("no_filter", &no_filter, "partitions=32768_files=32768"),
        (
            "restrictive_filter",
            &restrictive_filter,
            "partitions=1_files=1_pruned=99.99%",
        ),
        (
            "broad_filter",
            &broad_filter,
            "partitions=16384_files=16384_pruned=50%",
        ),
    ] {
        // Original implementation - total time
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/total/large/{}/{}",
                filter_name, desc
            )),
            &(&large_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // list_all implementation - total time
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/total/large/{}/{}",
                filter_name, desc
            )),
            &(&large_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // Original implementation - time to first result
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/ttfr/large/{}/{}",
                filter_name, desc
            )),
            &(&large_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_next().await.unwrap()
                });
            },
        );

        // list_all implementation - time to first result
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/ttfr/large/{}/{}",
                filter_name, desc
            )),
            &(&large_setup, filters),
            |b, (setup, filters)| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_next().await.unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Benchmark partition filtering with different filter types (S3/MinIO backend)
/// Goal: Test partition pruning effectiveness with S3
fn bench_partition_filtering_s3(c: &mut Criterion) {
    let mut group = c.benchmark_group("partition_filtering_s3");
    group.sample_size(10);

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let _guard = runtime.enter();

    // Shared container - initialized once when first benchmark runs
    type ContainerData = (Arc<dyn ObjectStore>, ContainerAsync<minio::MinIO>);
    let container_cell: Arc<std::sync::OnceLock<ContainerData>> =
        Arc::new(std::sync::OnceLock::new());

    // Small case setup cell - lazy initialized
    let small_setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
        Arc::new(std::sync::OnceLock::new());
    let small_config = PartitionConfig::new(2, 10, 100);

    // Define filter scenarios
    let no_filter = vec![];
    let restrictive_filter = vec![datafusion_expr::col("a")
        .eq(datafusion_expr::lit(10))
        .and(datafusion_expr::col("b").eq(datafusion_expr::lit(100)))];
    let broad_filter = vec![datafusion_expr::col("a").lt_eq(datafusion_expr::lit(50))];

    // Small case benchmarks
    let part_desc = "partitions=100_files=10000";
    for (filter_name, filters, desc) in [
        ("no_filter", &no_filter, format!("{part_desc}_pruned=0%")),
        (
            "restrictive_filter",
            &restrictive_filter,
            format!("{part_desc}_pruned=99%"),
        ),
        (
            "broad_filter",
            &broad_filter,
            format!("{part_desc}_pruned=50%"),
        ),
    ] {
        // Original implementation - total time
        let setup_cell_clone = Arc::clone(&small_setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/total/small/{}/{}",
                filter_name, desc
            )),
            &(
                setup_cell_clone,
                container_cell_clone,
                small_config.clone(),
                filters,
            ),
            |b, (setup_cell, container_cell, config, filters)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(
                        Arc::clone(store),
                        config,
                        "filter_small",
                    )
                });
                b.to_async(runtime_ref).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // list_all implementation - total time
        let setup_cell_clone = Arc::clone(&small_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/total/small/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // Original implementation - time to first result
        let setup_cell_clone = Arc::clone(&small_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/ttfr/small/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_next().await.unwrap()
                });
            },
        );

        // list_all implementation - time to first result
        let setup_cell_clone = Arc::clone(&small_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/ttfr/small/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_next().await.unwrap()
                });
            },
        );
    }

    // Large case setup cell - lazy initialized
    let large_setup_cell: Arc<std::sync::OnceLock<BenchmarkSetup>> =
        Arc::new(std::sync::OnceLock::new());
    let large_config = PartitionConfig::new(5, 8, 1);

    let no_filter = vec![];
    let restrictive_filter = vec![datafusion_expr::col("a")
        .eq(datafusion_expr::lit(1))
        .and(datafusion_expr::col("b").eq(datafusion_expr::lit(8)))
        .and(datafusion_expr::col("c").eq(datafusion_expr::lit(64)))
        .and(datafusion_expr::col("d").eq(datafusion_expr::lit(512)))
        .and(datafusion_expr::col("e").eq(datafusion_expr::lit(4096)))];
    let broad_filter = vec![datafusion_expr::col("a").lt_eq(datafusion_expr::lit(4))];

    // Large case benchmarks
    let part_desc = "partitions=32768_files=32768";
    for (filter_name, filters, desc) in [
        ("no_filter", &no_filter, format!("{part_desc}_pruned=0%")),
        (
            "restrictive_filter",
            &restrictive_filter,
            format!("{part_desc}_pruned=99.99%"),
        ),
        (
            "broad_filter",
            &broad_filter,
            format!("{part_desc}_pruned=50%"),
        ),
    ] {
        // Original implementation - total time
        let setup_cell_clone = Arc::clone(&large_setup_cell);
        let container_cell_clone = Arc::clone(&container_cell);
        let runtime_ref = &runtime;
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/total/large/{}/{}",
                filter_name, desc
            )),
            &(
                setup_cell_clone,
                container_cell_clone,
                large_config.clone(),
                filters,
            ),
            |b, (setup_cell, container_cell, config, filters)| {
                let setup = setup_cell.get_or_init(|| {
                    let (store, _) = container_cell.get_or_init(|| {
                        runtime_ref
                            .block_on(create_minio_store())
                            .expect("Failed to create MinIO container")
                    });
                    BenchmarkSetup::new_with_path(
                        Arc::clone(store),
                        config,
                        "filter_large",
                    )
                });
                b.to_async(runtime_ref).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // list_all implementation - total time
        let setup_cell_clone = Arc::clone(&large_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/total/large/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_collect::<Vec<_>>().await.unwrap()
                });
            },
        );

        // Original implementation - time to first result
        let setup_cell_clone = Arc::clone(&large_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "original/ttfr/large/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list failed");
                    result.try_next().await.unwrap()
                });
            },
        );

        // list_all implementation - time to first result
        let setup_cell_clone = Arc::clone(&large_setup_cell);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!(
                "list_all/ttfr/large/{}/{}",
                filter_name, desc
            )),
            &(setup_cell_clone, filters),
            |b, (setup_cell, filters)| {
                let setup = setup_cell.get().expect("Setup should be initialized");
                b.to_async(&runtime).iter(|| async {
                    let state = setup.ctx.state();
                    let mut result = pruned_partition_list_all(
                        &state,
                        setup.store.as_ref(),
                        &setup.table_url,
                        filters,
                        ".parquet",
                        &setup.partition_cols,
                    )
                    .await
                    .expect("pruned_partition_list_all failed");
                    result.try_next().await.unwrap()
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_depth_variation_constant_files_inmemory,
    bench_breadth_variation_constant_files_inmemory,
    bench_scale_constant_partitions_inmemory,
    bench_combined_scenarios_inmemory,
    bench_partition_filtering_inmemory,
    bench_depth_variation_constant_files_s3,
    bench_breadth_variation_constant_files_s3,
    bench_scale_constant_partitions_s3,
    bench_combined_scenarios_s3,
    bench_partition_filtering_s3,
);
criterion_main!(benches);
