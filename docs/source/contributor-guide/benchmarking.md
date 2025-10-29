# Benchmarking

This page describes the comprehensive benchmarking infrastructure available in Apache DataFusion to help contributors understand, leverage, and extend performance testing capabilities.

## Overview

DataFusion includes an extensive suite of benchmarks designed to measure performance across different workloads and use cases. These benchmarks help contributors:

- Validate performance improvements in pull requests
- Identify performance regressions
- Compare DataFusion's performance against other engines
- Find appropriate places to add new benchmark code

## Benchmark Categories

### Performance Benchmarks

#### TPCH (TPC-H Benchmark)

Industry-standard decision support benchmark derived from TPC-H version 2.17.1.

**Purpose**: Tests complex analytical queries with joins, aggregations, and sorting
**Data**: Synthetic business data (customers, orders, parts, suppliers)
**Usage**:

```bash
# Generate data
./bench.sh data tpch

# Run benchmark
./bench.sh run tpch

# Run specific query (e.g., Q21)
./bench.sh run tpch10 21
```

#### ClickBench

Widely cited benchmark focusing on grouping, aggregation, and filtering operations.

**Purpose**: Tests analytical query performance on real-world-like data
**Data**: Web analytics dataset
**Usage**:

```bash
./bench.sh data clickbench
./bench.sh run clickbench
```

#### IMDB (Join Order Benchmark)

Real-world movie database benchmark testing query optimization with skewed data.

**Purpose**: Tests join ordering and cardinality estimation with realistic data distribution
**Data**: Internet Movie Database with correlated columns and data skew
**Usage**:

```bash
./bench.sh data imdb
./bench.sh run imdb
```

#### H2O.ai Benchmarks

Performance tests for groupby, join, and window operations with configurable data sizes.

**Purpose**: Tests scalability across different data volumes
**Data Sizes**: Small (1e7), Medium (1e8), Big (1e9 rows)
**Usage**:

```bash
# Groupby benchmarks
./bench.sh data h2o_small
./bench.sh run h2o_small

# Join benchmarks
./bench.sh data h2o_small_join
./bench.sh run h2o_small_join

# Window function benchmarks
./bench.sh data h2o_small_window
./bench.sh run h2o_small_window
```

### Specialized Benchmarks

#### Sort Benchmarks

Tests sorting performance on large datasets.

**Sort TPCH**: End-to-end sorting on TPCH lineitem table

```bash
./bench.sh run sort_tpch
./bench.sh run topk_tpch  # TopK variant
```

**Sort**: General sorting performance on synthetic web server logs

```bash
./bench.sh run sort
```

#### External Aggregation

Tests aggregation performance with memory limits and spilling to disk.

**Purpose**: Validates out-of-core aggregation performance
**Usage**:

```bash
./bench.sh data external_aggr
./bench.sh run external_aggr
```

#### Parquet Filter

Tests Parquet filter pushdown performance.

**Purpose**: Measures filter pushdown optimization effectiveness
**Data**: Synthetic web server access logs
**Usage**:

```bash
./bench.sh run parquet_filter
```

### Micro-benchmarks

#### Hash Join

Focuses specifically on hash join performance with minimal overhead.

**Purpose**: Isolated hash join performance testing
**Data**: Uses `range()` table function
**Usage**:

```bash
./bench.sh run hj
```

#### Nested Loop Join

Tests nested loop join performance across various workloads.

**Purpose**: Isolated nested loop join performance testing
**Usage**:

```bash
./bench.sh run nlj
```

#### Cancellation

Tests query cancellation performance and cleanup time.

**Purpose**: Ensures queries stop executing quickly when cancelled
**Usage**:

```bash
./bench.sh run cancellation
```

## Running Benchmarks

### Using bench.sh Script (Recommended)

The `bench.sh` script provides the easiest way to run benchmarks:

```bash
# Navigate to benchmarks directory
cd benchmarks/

# Show usage
./bench.sh

# Generate all datasets
./bench.sh data

# Generate specific dataset
./bench.sh data tpch

# Run all benchmarks
./bench.sh run

# Run specific benchmark
./bench.sh run tpch

# Compare results between branches
git checkout main
./bench.sh run tpch
git checkout my-branch
./bench.sh run tpch
./bench.sh compare main my-branch
```

### Using dfbench Binary Directly

For more control, use the `dfbench` binary:

```bash
# Build in release mode (required for accurate benchmarks)
cargo build --release --bin dfbench

# Run TPCH benchmark
cargo run --release --bin dfbench -- tpch \
  --iterations 3 \
  --path ./data \
  --format parquet \
  --query 1

# Get help for specific benchmark
cargo run --release --bin dfbench -- tpch --help
```

### Memory Profiling

Use `mem_profile` to measure memory usage:

```bash
cargo run --profile release-nonlto --bin mem_profile -- \
  --bench-profile release-nonlto \
  tpch --path benchmarks/data/tpch_sf1 --partitions 4 --format parquet
```

## Criterion Benchmarks

DataFusion uses Criterion for micro-benchmarks of individual components:

```bash
# Run all criterion benchmarks
cargo bench

# Run specific benchmark group
cargo bench --bench aggregate_query_sql
cargo bench --bench sort

# Run with additional features
cargo bench --features jit
```

## Comparing Performance

### Between Branches

```bash
# Baseline on main branch
git checkout main
./benchmarks/bench.sh data
./benchmarks/bench.sh run tpch

# Test branch performance
git checkout my-feature-branch
./benchmarks/bench.sh run tpch

# Compare results
./benchmarks/bench.sh compare main my-feature-branch
```

### Using JSON Output

```bash
# Generate JSON results
cargo run --release --bin dfbench -- tpch \
  --path ./data --format parquet \
  --output /tmp/results.json

# Compare JSON files
./benchmarks/compare.py /tmp/baseline.json /tmp/feature.json
```

## Configuration Options

### Environment Variables

Configure DataFusion behavior during benchmarks:

```bash
# Disable hash joins
PREFER_HASH_JOIN=false ./bench.sh run tpch

# Disable join repartitioning
DATAFUSION_OPTIMIZER_REPARTITION_JOINS=false ./bench.sh run tpch

# Enable debug logging
RUST_LOG=info ./bench.sh run tpch
```

### Memory Allocators

Enable alternative allocators for performance testing:

```bash
# Using mimalloc
cargo run --release --features "mimalloc" --bin dfbench -- tpch \
  --path ./data --format parquet

# Using snmalloc
cargo run --release --features "snmalloc" --bin dfbench -- tpch \
  --path ./data --format parquet
```

## Adding New Benchmarks

### Step 1: Shell Script Integration

Add to `benchmarks/bench.sh`:

```bash
# Add data generation function
data_my_benchmark() {
    echo "Generating data for my_benchmark..."
    # Data generation logic
}

# Add run function
run_my_benchmark() {
    echo "Running my_benchmark..."
    cargo run --release --bin dfbench -- my-benchmark \
        --path "${DATA_DIR}" \
        --output "${RESULTS_FILE}"
}
```

### Step 2: dfbench Integration

In `benchmarks/src/bin/dfbench.rs`:

```rust
// Add to Options enum
enum Options {
    // ... existing variants
    MyBenchmark(my_benchmark::RunOpt),
}

// Add to main function
match opt {
    // ... existing matches
    Options::MyBenchmark(opt) => opt.run().await?,
}
```

### Step 3: Implementation

Create `benchmarks/src/my_benchmark.rs`:

```rust
use crate::util::BenchmarkRun;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Run my custom benchmark")]
pub struct RunOpt {
    #[structopt(long, help = "Path to data directory")]
    path: String,

    #[structopt(long, help = "Output file for results")]
    output: Option<String>,
}

impl RunOpt {
    pub async fn run(self) -> Result<()> {
        let mut benchmark_run = BenchmarkRun::new();

        // Benchmark implementation
        benchmark_run.start_new_case("My Test Case");
        // ... run and time your benchmark
        benchmark_run.write_iter(elapsed_time);

        benchmark_run.maybe_write_json(self.output.as_ref());
        Ok(())
    }
}
```

## Best Practices

### For Contributors

1. **Always use release builds** for performance testing
2. **Run multiple iterations** to account for variance
3. **Compare against baseline** before submitting PRs
4. **Document performance changes** in PR descriptions
5. **Use appropriate scale factors** for your testing environment

### For Benchmark Development

1. **Minimize non-benchmark overhead** in micro-benchmarks
2. **Use realistic data distributions** when possible
3. **Include both memory and disk-based scenarios**
4. **Test with different scale factors**
5. **Provide clear documentation** for new benchmarks

## Troubleshooting

### Common Issues

**Out of memory errors**:

```bash
# Reduce scale factor or increase memory limit
cargo run --release --bin dfbench -- tpch --path ./data --format parquet --memory-limit 4G
```

**Slow benchmark execution**:

```bash
# Ensure release build
cargo build --release

# Check system resources
htop
```

**Missing data**:

```bash
# Regenerate benchmark data
./bench.sh data tpch
```
