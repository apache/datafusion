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

# SQL Benchmarks

This directory contains a collection of benchmarks each driven by a simple '.benchmark' text file and sql queries
that exercise the DataFusion execution engine against a variety of benchmark suites. The sql benchmark framework
is intentionally simple so that benchmarks and queries can be added or modified without touching the core
engine or requiring recompilation.

The sql benchmarks are organized in sub‑directories that correspond to the benchmark suites that are commonly used
in the community:

| Benchmark Suite       | Description                                                        |
|-----------------------|--------------------------------------------------------------------|
| `clickbench`          | ClickBench benchmark                                               |
| `clickbench extended` | 12 additional, more complex queries against the Clickbench dataset |
| `clickbench_sorted`   | ClickBench benchmark using a pre-sorted hits file.                 |
| `h2o`                 | The `h2o` benchmark                                                |
| `hj`                  | Hash join benchmark                                                |
| `imdb`                | IMDb benchmark                                                     |
| `nlj`                 | Nested‑loop join benchmark                                         |
| `smj`                 | Sort‑merge join benchmark                                          |
| `sort tpch`           | Sorting benchmarks against the TPC-H lineitem table                |
| `taxi`                | NYC taxi dataset benchmark                                         |
| `tpcds`               | TPC‑DS queries                                                     |
| `tpch`                | TPC‑H queries                                                      |

# Running Benchmarks

The easiest way to run a benchmark is to use the `bench.sh` shell script (up one level from this document)
as it takes care of configuring any required environment variables and can populate any required data files.
However, it is possible to directly run a sql benchmark using the `cargo bench` command. For example:

```shell
BENCH_NAME=tpch cargo bench --bench sql
```

# Benchmark configuration

Sql benchmarks are configured via environment variables. Cargo's bench command and
[criterion](https://github.com/criterion-rs/criterion.rs) (the underlying benchmark framework) have an unfortunate
limitation in that custom command arguments cannot be passed into a benchmark. The alternative is to use environment
variables to pass in arguments which is what is used here.

The SQL benchmarking tool uses the following environment variables:

| Environment Variable  | Description                                                                                                                                                                                       |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BENCH_NAME            | The name of the benchmark suite to run. For example 'imdb'. This should correspond to a directory name in the `sql_benchmarks` directory.                                                         |
| BENCH_SUBGROUP        | The subgroup with the benchmark suite to run. For example 'window' to run the window subgroup of the h2o benchmark.                                                                               |
| BENCH_QUERY           | A query number to run.                                                                                                                                                                            |
| BENCH_PERSIST_RESULTS | true/false to persist benchmark results. Results will be persisted in csv format so be cognizant of the size of the results.                                                                      |
| BENCH_VALIDATE        | true/false to validate benchmark results against persisted results or result_query's. If both `BENCH_PERSIST_RESULTS` and `BENCH_VALIDATE` are true, persist mode runs and validation is skipped. |
| SIMULATE_LATENCY      | Simulate object store latency to mimic remote storage (e.g. S3). Adds random latency in the range 20-200ms to each object store operation.                                                        |
| MEM_POOL_TYPE         | The memory pool type to use, should be one of "fair" or "greedy".                                                                                                                                 |
| MEMORY_LIMIT          | Memory limit (e.g. '100M', '1.5G'). If not specified, run all pre-defined memory limits for given query if there's any, otherwise run with no memory limit.                                       | |

Example – Run the H2O window benchmarks on the 'small' sized CSV data files:

``` bash
BENCH_NAME=h2o BENCH_SUBGROUP=window H2O_BENCH_SIZE=small H20_FILE_TYPE=csv cargo bench --bench sql 
```

Some benchmarks use custom environment variables as outlined below:

| Name                         | Description                                                                                                              | Default value | 
|------------------------------|--------------------------------------------------------------------------------------------------------------------------|---------------|
| BENCH_SIZE                   | Used in the tpch, sort-tpch and tpcds benchmarks. The size corresponds to the scale factor.                              | `1`           |
| TPCH_FILE_TYPE               | Used in the tpch benchmark to specify which file type to query against. The valid options are `csv`, `parquet` and `mem` | `parquet`     |
| H2O_FILE_TYPE                | Used in the h2o benchmark to specify which file type to query against. The valid options are `csv` and `parquet`         | `csv`         |
| CLICKBENCH_TYPE              | The type of partitioning for the clickbench benchmark. Valid options are `single` and `partitioned`                      | `single`      | 
| H2O_BENCH_SIZE               | Used in the h2o benchmark. The valid options are `small`, `medium` and `big`                                             | `small`       |                           
| PREFER_HASH_JOIN             | Control datafusion's config option `datafusion.optimizer.prefer_hash_join`                                               | true          |
| HASH_JOIN_BUFFERING_CAPACITY | Control datafusion's config option `datafusion.execution.hash_join_buffering_capacity`                                   | 0             |
| BENCH_SORTED                 | Used in the sort_tpch benchmark to indicate whether the lineitem table should be sorted.                                 | false         |
| SORTED_BY                    | Used in the clickbench_sorted benchmark to indicate the column to sort by.                                               | `EventTime`   |
| SORTED_ORDER                 | Used in the clickbench_sorted benchmark to indicate the sort order of the column.                                        | `ASC`         |

## How it works

SQL benchmarks are run via cargo's bench command using [criterion](https://docs.rs/criterion/latest/criterion/)
for running and gathering statistics of each sql being benchmarked.

Each individual benchmark is represented by a `<name>.benchmark` file that contains a number of directives instructing
the tool on how to load data, run initializations, run assertions, run the benchmark, optionally persist and
validate results, and finally run any cleanup if required.

Variables are supported in two forms:

* string substitution based on environment variables (with default values if unset): \${ENV_VAR} and
  \${ENV_VAR:-default}.
* if / else based on whether an environment variable is true or not
  (\${ENV_VAR:-default|true value|false value}). In this form only the value `true` (case-insensitive) selects the
  true branch; any other set value selects the false branch. If ENV_VAR is unset, the valud of `default` is used to
* select the branch.

Comments in files are supported with lines starting with # or --.

Many if not most of the benchmarks are set up using templates to reduce duplication across the .benchmark files. For
example here is one of the benchmark files for the h2o benchmark suite:

```
subgroup groupby

template sql_benchmarks/h2o/h2o.benchmark.template
QUERY_NUMBER=1
QUERY_NUMBER_PADDED=01
```

The template directive above defines the subgroup the benchmark is part of, sets two variables (`QUERY_NUMBER` and
`QUERY_NUMBER_PADDED`) and points to a file containing more directives that are shared across the benchmark suite.

```
load sql_benchmarks/h2o/init/load_${BENCH_SUBGROUP:-groupby}_${BENCH_SIZE:-small}_${BENCH_FILE_TYPE:-csv}.sql

name Q${QUERY_NUMBER_PADDED}
group h2o

run sql_benchmarks/h2o/queries/${BENCH_SUBGROUP:-groupby}/q${QUERY_NUMBER_PADDED}.sql

result sql_benchmarks/h2o/results/${BENCH_SUBGROUP:-groupby}/${BENCH_SIZE:-small}/q${QUERY_NUMBER_PADDED}.csv
```

The above showcases the use of defaults for variables: `${NAME:-default}`

# Directives

<table>
<tr><th>Directive</th><th>Description</th></tr>
<tr>
<td>name</td>
<td>

The name of the benchmark. This will be used as part of the display name used by criterion.<br/><br/>Example:<br/>
<blockquote>name Q${QUERY_NUMBER_PADDED}</blockquote>

The `name` directive also makes the value available to benchmark-file replacements as `BENCH_NAME`. This is separate
from the `BENCH_NAME` environment variable used to select which benchmark group to run.

</td>
</tr>
<tr>
<td>group</td>
<td>

The group name of the benchmark used for grouping benchmarks together.<br/><br/>Example:<br/>
<blockquote>group imdb</blockquote>

</td>
</tr>
<tr>
<td>subgroup</td>
<td>

The sub group name of the benchmark used for filtering to a specific sub group.<br/><br/>Example:<br/>
<blockquote>subgroup window</blockquote>

</td>
</tr>
<tr>
<td>load</td>
<td>

The load directive called during initialization of the benchmark. If a path to a file is provided on the same
line as the load directive that path will be parsed and any sql statements in that file will be executed during
initialization. If no path is specified the next line is required to be the sql statement to execute. <br/> <br/> The
load directive (including any following sql statement) must be followed by a blank line. <br/><br/>Example:<br/>
<blockquote>load sql_benchmarks/h2o/init/load_${BENCH_SUBGROUP:-groupby}_${BENCH_SIZE:-small}_${BENCH_FILE_TYPE:-csv}.sql</blockquote>
or
<blockquote>
load<br/>
CREATE TABLE test AS (SELECT value as key FROM range(1000000) ORDER BY value);
</blockquote>

</td>
</tr>
<tr>
<td>init</td>
<td>

The init directive is called after the load directive prior to benchmark execution. If a path to a file is
provided on the same line as the init directive that path will be parsed and any sql statements in that file will be
executed during the benchmark initialization. If no path is specified the next line is required to be the sql statement
to execute.<br/><br/> The init directive (including any following sql statement) must be followed by a blank
line.<br/><br/>Example:<br/>
<blockquote>
init<br/>
set datafusion.execution.parquet.binary_as_string = true;  
</blockquote>

</td>
</tr>
<tr>
<td>run</td>
<td>

The run directive called during execution of the benchmark. If a path to a file is provided on the same line as
the run directive that path will be parsed and any sql statements in that file will be executed during the benchmark
run. If no path is specified the next line is required to be the sql statement to execute. <br/><br/> Multiple
statements are allowed within a single run directive, however a benchmark file may contain only one run directive. When
running with `BENCH_PERSIST_RESULTS` or `BENCH_VALIDATE`, only the last `SELECT` or `WITH` statement from that run
directive will be used for comparison. <br/><br/> The run directive (including any following sql statement) must be
followed by a blank line.<br/><br/>Example:<br/>
<blockquote>run sql_benchmarks/imdb/queries/${QUERY_NUMBER_PADDED}.sql</blockquote>

</td>
</tr>
<tr>
<td>cleanup</td>
<td>

The cleanup directive is called after all other directives and can be used to cleanup after the benchmark -
e.g. to drop tables. If a path to a file is provided on the same line as the cleanup directive that path will be parsed
and any sql statements in that file will be executed during cleanup. If no path is specified the next line is
required to be the sql statement to execute. <br/> <br/> The cleanup directive (including any following sql statement)
must be followed by a blank line. <br/><br/>Example:<br/>
<blockquote>
cleanup<br/>
DROP TABLE test;
</blockquote>

</td>
</tr>
<tr>
<td>expect_plan</td>
<td>

The expect_plan directive will check the physical plan for the string provided on the same line. This
can be used to validate that a particular join was used. <br/> <br/> Example:<br/>
<blockquote>expect_plan NestedLoopJoinExec</blockquote>

</td>
</tr>
<tr>
<td>assert</td>
<td>

The assert directive is run between the init and run directives and can be used to validate system state correctness
prior to running the benchmark sql. The format is
<blockquote>
assert II<br/>
SELECT name, value = 3 FROM information_schema.df_settings WHERE name IN ('datafusion.execution.target_partitions', 'datafusion.execution.planning_concurrency');<br/>
----<br/>
datafusion.execution.planning_concurrency true<br/>
datafusion.execution.target_partitions true<br/>
</blockquote>

The number of I's corresponds to the number of columns in the result. The expected results can be either tab delimited
or pipe delimited.

</td>
</tr>                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
<tr>
<td>result_query</td>
<td>

The result_query directive is run during the verify phase and can be used to verify a different set of results than any
that might come from queries executed from the `run` directive. The format is the same as the `assert` directive
above.<br/><br/>Example:
<blockquote>
result_query III<br/>
SELECT COUNT(DISTINCT id2), SUM(r4), COUNT(*) FROM answer;<br/>
----<br/>
123 345 45
</blockquote>


Note that the results of the run query are not automatically stored into a table in datafusion. If you want to
verify a result from queries executed from the `run` directive those queries will have to be saved to a table directly
using `CREATE TABLE AS (..)` or similar.

</td>
</tr>
<tr>
<td>result</td>
<td>

The result directive declares the expected result file used during verification. A path to a file is required on the
same line as the result directive. The file is parsed only during verification, and must be a pipe-delimited CSV file
with a header row. During verification, these expected rows are compared with the rows produced by the last saved
`SELECT` or `WITH` statement from the `run` directive. <br/><br/>Example:<br/>
<blockquote>
result sql_benchmarks/imdb/results/${QUERY_NUMBER_PADDED}.csv  
</blockquote>

</td>
</tr>
<tr>
<td>template</td>
<td>

The template directive allows for inclusion of another file in a benchmark file. A path to a file is
required on the same line as the template directive which will be parsed as a benchmark file. Parameters can be passed
to the template file using the format `KEY=value`, one per line after the template directive followed by a blank line.
<br/><br/>Example:<br/>
<blockquote>
template sql_benchmarks/smj/smj.benchmark.template<br/>
QUERY_NUMBER=1<br/>
QUERY_NUMBER_PADDED=01
</blockquote> 

</td>
</tr>
<tr>
<td>include</td>
<td>The include directive is similar to the template directive except that it does not support parameters.</td>
</tr>
<tr>
<td>echo</td>
<td>

The echo directive allows for echoing a string to stdout during the execution of the benchmark and may be useful for
debugging.<br/><br/>Example:<br/>
<blockquote>
echo The value for batch size is ${BATCH_SIZE:-8192}
</blockquote>

</td>
</tr>
</table>

# Extending an existing benchmark suite

If you want to add a new query:

* Create a new qXX.sql in the corresponding queries folder of the benchmark.
* Add a new qXX.benchmark that references the appropriate template (clickbench.benchmark.template,
  h2o.benchmark.template,
  etc.).
* (Optional) Add a new entry to the suite’s load script if the data set is different.
* (Optional) Manually create a result csv to be compared against benchmark results during verification.

# Adding a new benchmark suite

* Create a new directory named for the new benchmark suite.
* Within there create a `<name>.benchmark` for each individual benchmark.
* Populate the benchmark with directives as described above. Use the other benchmarks as examples for standardization.
* No rust files need to be updated to run the new benchmark suite.
