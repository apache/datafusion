<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Benchmarks

This crate contains benchmarks based on popular public data sets and
open source benchmark suites, to help with performance and scalability
testing of DataFusion.


## Other engines

The benchmarks measure changes to DataFusion itself, rather than
its performance against other engines. For competitive benchmarking,
DataFusion is included in the benchmark setups for several popular
benchmarks that compare performance with other engines. For example:

* [ClickBench] scripts are in the [ClickBench repo](https://github.com/ClickHouse/ClickBench/tree/main/datafusion)
* [H2o.ai `db-benchmark`] scripts are in [db-benchmark](https://github.com/apache/datafusion/tree/main/benchmarks/src/h2o.rs)

[ClickBench]: https://github.com/ClickHouse/ClickBench/tree/main
[H2o.ai `db-benchmark`]: https://github.com/h2oai/db-benchmark

# Running the benchmarks

## `bench.sh`

The easiest way to run benchmarks is the [bench.sh](bench.sh)
script. Usage instructions can be found with:

```shell
# show usage
cd ./benchmarks/
./bench.sh
```

## Generating data

You can create / download the data for these benchmarks using the [bench.sh](bench.sh) script:

Create / download all datasets

```shell
./bench.sh data
```

Create / download a specific dataset (TPCH)

```shell
./bench.sh data tpch
```
Data is placed in the `data` subdirectory.

## Running benchmarks

Run benchmark for TPC-H dataset
```shell
./bench.sh run tpch
```
or for TPC-H dataset scale 10
```shell
./bench.sh run tpch10
```

To run for specific query, for example Q21
```shell
./bench.sh run tpch10 21
```

## Select join algorithm
The benchmark runs with `prefer_hash_join == true` by default, which enforces HASH join algorithm.
To run TPCH benchmarks with join other than HASH:
```shell
PREFER_HASH_JOIN=false ./bench.sh run tpch
```

## Comparing performance of main and a branch

```shell
git checkout main

# Create the data
./benchmarks/bench.sh data

# Gather baseline data for tpch benchmark
./benchmarks/bench.sh run tpch

# Switch to the branch named mybranch and gather data
git checkout mybranch
./benchmarks/bench.sh run tpch

# Compare results in the two branches:
./bench.sh compare main mybranch
```

This produces results like:

```shell
Comparing main and mybranch
--------------------
Benchmark tpch.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2520.52ms │    2795.09ms │  1.11x slower │
│ QQuery 2     │     222.37ms │     216.01ms │     no change │
│ QQuery 3     │     248.41ms │     239.07ms │     no change │
│ QQuery 4     │     144.01ms │     129.28ms │ +1.11x faster │
│ QQuery 5     │     339.54ms │     327.53ms │     no change │
│ QQuery 6     │     147.59ms │     138.73ms │ +1.06x faster │
│ QQuery 7     │     605.72ms │     631.23ms │     no change │
│ QQuery 8     │     326.35ms │     372.12ms │  1.14x slower │
│ QQuery 9     │     579.02ms │     634.73ms │  1.10x slower │
│ QQuery 10    │     403.38ms │     420.39ms │     no change │
│ QQuery 11    │     201.94ms │     212.12ms │  1.05x slower │
│ QQuery 12    │     235.94ms │     254.58ms │  1.08x slower │
│ QQuery 13    │     738.40ms │     789.67ms │  1.07x slower │
│ QQuery 14    │     198.73ms │     206.96ms │     no change │
│ QQuery 15    │     183.32ms │     179.53ms │     no change │
│ QQuery 16    │     168.57ms │     186.43ms │  1.11x slower │
│ QQuery 17    │    2032.57ms │    2108.12ms │     no change │
│ QQuery 18    │    1912.80ms │    2134.82ms │  1.12x slower │
│ QQuery 19    │     391.64ms │     368.53ms │ +1.06x faster │
│ QQuery 20    │     648.22ms │     691.41ms │  1.07x slower │
│ QQuery 21    │     866.25ms │    1020.37ms │  1.18x slower │
│ QQuery 22    │     115.94ms │     117.27ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
--------------------
Benchmark tpch_mem.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2182.44ms │    2390.39ms │  1.10x slower │
│ QQuery 2     │     181.16ms │     153.94ms │ +1.18x faster │
│ QQuery 3     │      98.89ms │      95.51ms │     no change │
│ QQuery 4     │      61.43ms │      66.15ms │  1.08x slower │
│ QQuery 5     │     260.20ms │     283.65ms │  1.09x slower │
│ QQuery 6     │      24.24ms │      23.39ms │     no change │
│ QQuery 7     │     545.87ms │     653.34ms │  1.20x slower │
│ QQuery 8     │     147.48ms │     136.00ms │ +1.08x faster │
│ QQuery 9     │     371.53ms │     363.61ms │     no change │
│ QQuery 10    │     197.91ms │     190.37ms │     no change │
│ QQuery 11    │     197.91ms │     183.70ms │ +1.08x faster │
│ QQuery 12    │     100.32ms │     103.08ms │     no change │
│ QQuery 13    │     428.02ms │     440.26ms │     no change │
│ QQuery 14    │      38.50ms │      27.11ms │ +1.42x faster │
│ QQuery 15    │     101.15ms │      63.25ms │ +1.60x faster │
│ QQuery 16    │     171.15ms │     142.44ms │ +1.20x faster │
│ QQuery 17    │    1885.05ms │    1953.58ms │     no change │
│ QQuery 18    │    1549.92ms │    1914.06ms │  1.23x slower │
│ QQuery 19    │     106.53ms │     104.28ms │     no change │
│ QQuery 20    │     532.11ms │     610.62ms │  1.15x slower │
│ QQuery 21    │     723.39ms │     823.34ms │  1.14x slower │
│ QQuery 22    │      91.84ms │      89.89ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

### Running Benchmarks Manually

Assuming data is in the `data` directory, the `tpch` benchmark can be run with a command like this:

```bash
cargo run --release --bin dfbench -- tpch --iterations 3 --path ./data --format tbl --query 1 --batch-size 4096
```

See the help for more details.

### Different features

You can enable `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as `--features`. For example:

```shell
cargo run --release --features "mimalloc" --bin tpch -- benchmark datafusion --iterations 3 --path ./data --format tbl --query 1 --batch-size 4096
```

The benchmark program also supports CSV and Parquet input file formats and a utility is provided to convert from `tbl`
(generated by the `dbgen` utility) to CSV and Parquet.

```bash
cargo run --release --bin tpch -- convert --input ./data --output /mnt/tpch-parquet --format parquet
```

Or if you want to verify and run all the queries in the benchmark, you can just run `cargo test`.

### Comparing results between runs

Any `dfbench` execution with `-o <dir>` argument will produce a
summary JSON in the specified directory. This file contains a
serialized form of all the runs that happened and runtime
metadata (number of cores, DataFusion version, etc.).

```shell
$ git checkout main
# generate an output script in /tmp/output_main
$ mkdir -p /tmp/output_main
$ cargo run --release --bin tpch -- benchmark datafusion --iterations 5 --path ./data --format parquet -o /tmp/output_main/tpch.json
# generate an output script in /tmp/output_branch
$ mkdir -p /tmp/output_branch
$ git checkout my_branch
$ cargo run --release --bin tpch -- benchmark datafusion --iterations 5 --path ./data --format parquet -o /tmp/output_branch/tpch.json
# compare the results:
./compare.py /tmp/output_main/tpch.json  /tmp/output_branch/tpch.json
```

This will produce output like:

```
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃ /home/alamb… ┃ /home/alamb… ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ Q1           │   16252.56ms │   16031.82ms │     no change │
│ Q2           │    3994.56ms │    4353.75ms │  1.09x slower │
│ Q3           │    5572.06ms │    5620.27ms │     no change │
│ Q4           │    2144.14ms │    2194.67ms │     no change │
│ Q5           │    7796.93ms │    7646.74ms │     no change │
│ Q6           │    4382.32ms │    4327.16ms │     no change │
│ Q7           │   18702.50ms │   19922.74ms │  1.07x slower │
│ Q8           │    7383.74ms │    7616.21ms │     no change │
│ Q9           │   13855.17ms │   14408.42ms │     no change │
│ Q10          │    7446.05ms │    8030.00ms │  1.08x slower │
│ Q11          │    3414.81ms │    3850.34ms │  1.13x slower │
│ Q12          │    3027.16ms │    3085.89ms │     no change │
│ Q13          │   18859.06ms │   18627.02ms │     no change │
│ Q14          │    4157.91ms │    4140.22ms │     no change │
│ Q15          │    5293.05ms │    5369.17ms │     no change │
│ Q16          │    6512.42ms │    3011.58ms │ +2.16x faster │
│ Q17          │   86253.33ms │   76036.06ms │ +1.13x faster │
│ Q18          │   45101.99ms │   49717.76ms │  1.10x slower │
│ Q19          │    7323.15ms │    7409.85ms │     no change │
│ Q20          │   19902.39ms │   20965.94ms │  1.05x slower │
│ Q21          │   22040.06ms │   23184.84ms │  1.05x slower │
│ Q22          │    2011.87ms │    2143.62ms │  1.07x slower │
└──────────────┴──────────────┴──────────────┴───────────────┘
```

# Benchmark Runner

The `dfbench` program contains subcommands to run the various
benchmarks. When benchmarking, it should always be built in release
mode using `--release`.

Full help for each benchmark can be found in the relevant
subcommand. For example, to get help for tpch, run:

```shell
cargo run --release --bin dfbench -- tpch --help
...
dfbench-tpch 45.0.0
Run the tpch benchmark.

This benchmarks is derived from the [TPC-H][1] version
[2.17.1]. The data and answers are generated using `tpch-gen` from
[2].

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen.git,
[2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf

USAGE:
    dfbench tpch [FLAGS] [OPTIONS] --path <path>

FLAGS:
    -d, --debug
            Activate debug mode to see more details

    -S, --disable-statistics
            Whether to disable collection of statistics (and cost based optimizations) or not

    -h, --help
            Prints help information
...
```

# Writing a new benchmark

## Creating or downloading data outside of the benchmark

If you want to create or download the data with Rust as part of running the benchmark, see the next
section on adding a benchmark subcommand and add code to create or download data as part of its
`run` function.

If you want to create or download the data with shell commands, in `benchmarks/bench.sh`, define a
new function named `data_[your benchmark name]` and call that function in the `data` command case
as a subcommand case named for your benchmark. Also call the new function in the `data all` case.

## Adding the benchmark subcommand

In `benchmarks/bench.sh`, define a new function named `run_[your benchmark name]` following the
example of existing `run_*` functions. Call that function in the `run` command case as a subcommand
case named for your benchmark. subcommand for your benchmark. Also call the new function in the
`run all` case. Add documentation for your benchmark to the text in the `usage` function.

In `benchmarks/src/bin/dfbench.rs`, add a `dfbench` subcommand for your benchmark by:

- Adding a new variant to the `Options` enum
- Adding corresponding code to handle the new variant in the `main` function, similar to the other
  variants
- Adding a module to the `use datafusion_benchmarks::{}` statement

In `benchmarks/src/lib.rs`, declare the new module you imported in `dfbench.rs` and create the
corresponding file(s) for the module's code.

In the module, following the pattern of other existing benchmarks, define a `RunOpt` struct with:

- A doc comment that will become the `--help` output for the subcommand
- A `run` method that the `dfbench` `main` function will call.
- A `--path` structopt field that the `bench.sh` script should use with `${DATA_DIR}` to define
  where the input data should be stored.
- An `--output` structopt field that the `bench.sh` script should use with `"${RESULTS_FILE}"` to
  define where the benchmark's results should be stored.

### Creating or downloading data as part of the benchmark

Use the `--path` structopt field defined on the `RunOpt` struct to know where to store or look for
the data. Generate the data using whatever Rust code you'd like, before the code that will be
measuring an operation.

### Collecting data

Your benchmark should create and use an instance of `BenchmarkRun` defined in `benchmarks/src/util/run.rs` as follows:

- Call its `start_new_case` method with a string that will appear in the "Query" column of the
  compare output.
- Use `write_iter` to record elapsed times for the behavior you're benchmarking.
- When all cases are done, call the `BenchmarkRun`'s `maybe_write_json` method, giving it the value
  of the `--output` structopt field on `RunOpt`.

# Benchmarks

The output of `dfbench` help includes a description of each benchmark, which is reproduced here for convenience.

## Cancellation

Test performance of cancelling queries.

Queries in DataFusion should stop executing "quickly" after they are
cancelled (the output stream is dropped).

The queries are executed on a synthetic dataset generated during
the benchmark execution that is an anonymized version of a
real-world data set.

The query is an anonymized version of a real-world query, and the
test starts the query then cancels it and reports how long it takes
for the runtime to fully exit.

Example output:

```
Using 7 files found on disk
Starting to load data into in-memory object store
Done loading data into in-memory object store
in main, sleeping
Starting spawned
Creating logical plan...
Creating physical plan...
Executing physical plan...
Getting results...
cancelling thread
done dropping runtime in 83.531417ms
```

## ClickBench

The ClickBench[1] benchmarks are widely cited in the industry and
focus on grouping / aggregation / filtering. This runner uses the
scripts and queries from [2].

[1]: https://github.com/ClickHouse/ClickBench
[2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion

## Parquet Filter

Test performance of parquet filter pushdown

The queries are executed on a synthetic dataset generated during
the benchmark execution and designed to simulate web server access
logs.

Example

dfbench parquet-filter  --path ./data --scale-factor 1.0

generates the synthetic dataset at `./data/logs.parquet`. The size
of the dataset can be controlled through the `size_factor`
(with the default value of `1.0` generating a ~1GB parquet file).

For each filter we will run the query using different
`ParquetScanOption` settings.

Example output:

```
Running benchmarks with the following options: Opt { debug: false, iterations: 3, partitions: 2, path: "./data",
batch_size: 8192, scale_factor: 1.0 }
Generated test dataset with 10699521 rows
Executing with filter 'request_method = Utf8("GET")'
Using scan options ParquetScanOptions { pushdown_filters: false, reorder_predicates: false, enable_page_index: false }
Iteration 0 returned 10699521 rows in 1303 ms
Iteration 1 returned 10699521 rows in 1288 ms
Iteration 2 returned 10699521 rows in 1266 ms
Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: true, enable_page_index: true }
Iteration 0 returned 1781686 rows in 1970 ms
Iteration 1 returned 1781686 rows in 2002 ms
Iteration 2 returned 1781686 rows in 1988 ms
Using scan options ParquetScanOptions { pushdown_filters: true, reorder_predicates: false, enable_page_index: true }
Iteration 0 returned 1781686 rows in 1940 ms
Iteration 1 returned 1781686 rows in 1986 ms
Iteration 2 returned 1781686 rows in 1947 ms
...
```

## Sort
Test performance of sorting large datasets

This test sorts a a synthetic dataset generated during the
benchmark execution, designed to simulate sorting web server
access logs. Such sorting is often done during data transformation
steps.

The tests sort the entire dataset using several different sort
orders.

## Sort TPCH

Test performance of end-to-end sort SQL queries. (While the `Sort` benchmark focuses on a single sort executor, this benchmark tests how sorting is executed across multiple CPU cores by benchmarking sorting the whole relational table.)

Sort integration benchmark runs whole table sort queries on TPCH `lineitem` table, with different characteristics. For example, different number of sort keys, different sort key cardinality, different number of payload columns, etc.

See [`sort_tpch.rs`](src/sort_tpch.rs) for more details.

### Sort TPCH Benchmark Example Runs
1. Run all queries with default setting:
```bash
 cargo run --release --bin dfbench -- sort-tpch -p '....../datafusion/benchmarks/data/tpch_sf1' -o '/tmp/sort_tpch.json'
```

2. Run a specific query:
```bash
 cargo run --release --bin dfbench -- sort-tpch -p '....../datafusion/benchmarks/data/tpch_sf1' -o '/tmp/sort_tpch.json' --query 2
```

3. Run all queries with `bench.sh` script:
```bash
./bench.sh run sort_tpch
```

## IMDB

Run Join Order Benchmark (JOB) on IMDB dataset.

The Internet Movie Database (IMDB) dataset contains real-world movie data. Unlike synthetic datasets like TPCH, which assume uniform data distribution and uncorrelated columns, the IMDB dataset includes skewed data and correlated columns (which are common for real dataset), making it more suitable for testing query optimizers, particularly for cardinality estimation.

This benchmark is derived from [Join Order Benchmark](https://github.com/gregrahn/join-order-benchmark).

See paper [How Good Are Query Optimizers, Really](http://www.vldb.org/pvldb/vol9/p204-leis.pdf) for more details.

## TPCH

Run the tpch benchmark.

This benchmarks is derived from the [TPC-H][1] version
[2.17.1]. The data and answers are generated using `tpch-gen` from
[2].

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen.git,
[2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf

## External Aggregation

Run the benchmark for aggregations with limited memory.

When the memory limit is exceeded, the aggregation intermediate results will be spilled to disk, and finally read back with sort-merge.

External aggregation benchmarks run several aggregation queries with different memory limits, on TPCH `lineitem` table. Queries can be found in [`external_aggr.rs`](src/bin/external_aggr.rs).

This benchmark is inspired by [DuckDB's external aggregation paper](https://hannes.muehleisen.org/publications/icde2024-out-of-core-kuiper-boncz-muehleisen.pdf), specifically Section VI.

### External Aggregation Example Runs
1. Run all queries with predefined memory limits:
```bash
# Under 'benchmarks/' directory
cargo run --release --bin external_aggr -- benchmark -n 4 --iterations 3 -p '....../data/tpch_sf1' -o '/tmp/aggr.json'
```

2. Run a query with specific memory limit:
```bash
cargo run --release --bin external_aggr -- benchmark -n 4 --iterations 3 -p '....../data/tpch_sf1' -o '/tmp/aggr.json' --query 1 --memory-limit 30M
```

3. Run all queries with `bench.sh` script:
```bash
./bench.sh data external_aggr
./bench.sh run external_aggr
```


## h2o benchmarks for groupby

### Generate data for h2o benchmarks
There are three options for generating data for h2o benchmarks: `small`, `medium`, and `big`. The data is generated in the `data` directory.

1. Generate small data (1e7 rows)
```bash
./bench.sh data h2o_small
```


2. Generate medium data (1e8 rows)
```bash
./bench.sh data h2o_medium
```


3. Generate large data (1e9 rows)
```bash
./bench.sh data h2o_big
```

### Run h2o benchmarks
There are three options for running h2o benchmarks: `small`, `medium`, and `big`.
1. Run small data benchmark
```bash
./bench.sh run h2o_small
```

2. Run medium data benchmark
```bash
./bench.sh run h2o_medium
```

3. Run large data benchmark
```bash
./bench.sh run h2o_big
```

4. Run a specific query with a specific data path

For example, to run query 1 with the small data generated above:
```bash
cargo run --release --bin dfbench -- h2o --path ./benchmarks/data/h2o/G1_1e7_1e7_100_0.csv  --query 1
```

## h2o benchmarks for join

### Generate data for h2o benchmarks
There are three options for generating data for h2o benchmarks: `small`, `medium`, and `big`. The data is generated in the `data` directory.

1. Generate small data (4 table files, the largest is 1e7 rows)
```bash
./bench.sh data h2o_small_join
```


2. Generate medium data (4 table files, the largest is 1e8 rows)
```bash
./bench.sh data h2o_medium_join
```

3. Generate large data (4 table files, the largest is 1e9 rows)
```bash
./bench.sh data h2o_big_join
```

### Run h2o benchmarks
There are three options for running h2o benchmarks: `small`, `medium`, and `big`.
1. Run small data benchmark
```bash
./bench.sh run h2o_small_join
```

2. Run medium data benchmark
```bash
./bench.sh run h2o_medium_join
```

3. Run large data benchmark
```bash
./bench.sh run h2o_big_join
```

4. Run a specific query with a specific join data paths, the data paths are including 4 table files.

For example, to run query 1 with the small data generated above:
```bash
cargo run --release --bin dfbench -- h2o --join-paths ./benchmarks/data/h2o/J1_1e7_NA_0.csv,./benchmarks/data/h2o/J1_1e7_1e1_0.csv,./benchmarks/data/h2o/J1_1e7_1e4_0.csv,./benchmarks/data/h2o/J1_1e7_1e7_NA.csv --queries-path ./benchmarks/queries/h2o/join.sql --query 1
```
[1]: http://www.tpc.org/tpch/
[2]: https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
