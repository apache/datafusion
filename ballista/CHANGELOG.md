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

# Changelog

## [ballista-0.7.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.7.0) (2022-05-12)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.1.0-rc1...ballista-0.7.0)

**Breaking changes:**

- Make `ExecutionPlan::execute` Sync [\#2434](https://github.com/apache/arrow-datafusion/pull/2434) ([tustvold](https://github.com/tustvold))
- Add `Expr::Exists` to represent EXISTS subquery expression [\#2339](https://github.com/apache/arrow-datafusion/pull/2339) ([andygrove](https://github.com/andygrove))
- Remove dependency from `LogicalPlan::TableScan` to `ExecutionPlan` [\#2284](https://github.com/apache/arrow-datafusion/pull/2284) ([andygrove](https://github.com/andygrove))
- Move logical expression type-coercion code from `physical-expr` crate to `expr` crate [\#2257](https://github.com/apache/arrow-datafusion/pull/2257) ([andygrove](https://github.com/andygrove))
- feat: 2061 create external table ddl table partition cols [\#2099](https://github.com/apache/arrow-datafusion/pull/2099) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([jychen7](https://github.com/jychen7))
- Reorganize the project folders [\#2081](https://github.com/apache/arrow-datafusion/pull/2081) ([yahoNanJing](https://github.com/yahoNanJing))
- Support more ScalarFunction in Ballista [\#2008](https://github.com/apache/arrow-datafusion/pull/2008) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Merge dataframe and dataframe imp [\#1998](https://github.com/apache/arrow-datafusion/pull/1998) ([vchag](https://github.com/vchag))
- Rename `ExecutionContext` to `SessionContext`, `ExecutionContextState` to `SessionState`, add `TaskContext` to support multi-tenancy configurations - Part 1 [\#1987](https://github.com/apache/arrow-datafusion/pull/1987) ([mingmwang](https://github.com/mingmwang))
- Add Coalesce function [\#1969](https://github.com/apache/arrow-datafusion/pull/1969) ([msathis](https://github.com/msathis))
- Add Create Schema functionality in SQL [\#1959](https://github.com/apache/arrow-datafusion/pull/1959) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- remove sync constraint of SendableRecordBatchStream [\#1884](https://github.com/apache/arrow-datafusion/pull/1884) ([doki23](https://github.com/doki23))

**Implemented enhancements:**

- Add `CREATE VIEW` [\#2279](https://github.com/apache/arrow-datafusion/pull/2279) ([matthewmturner](https://github.com/matthewmturner))
- \[Ballista\] Support Union in ballista. [\#2098](https://github.com/apache/arrow-datafusion/pull/2098) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add missing aggr\_expr to PhysicalExprNode for Ballista. [\#1989](https://github.com/apache/arrow-datafusion/pull/1989) ([Ted-Jiang](https://github.com/Ted-Jiang))

**Fixed bugs:**

- Ballista integration tests no longer work [\#2440](https://github.com/apache/arrow-datafusion/issues/2440)
- Ballista crates cannot be released from DafaFusion 7.0.0 source release [\#1980](https://github.com/apache/arrow-datafusion/issues/1980)
- protobuf OctetLength should be deserialized as octet\_length, not length [\#1834](https://github.com/apache/arrow-datafusion/pull/1834) ([carols10cents](https://github.com/carols10cents))

**Documentation updates:**

- MINOR: Make crate READMEs consistent [\#2437](https://github.com/apache/arrow-datafusion/pull/2437) ([andygrove](https://github.com/andygrove))
- docs: Update the Ballista dev env instructions [\#2419](https://github.com/apache/arrow-datafusion/pull/2419) ([haoxins](https://github.com/haoxins))
- Revise document of installing ballista pinned to specified version [\#2034](https://github.com/apache/arrow-datafusion/pull/2034) ([WinkerDu](https://github.com/WinkerDu))
- Fix typos \(Datafusion -\> DataFusion\) [\#1993](https://github.com/apache/arrow-datafusion/pull/1993) ([andygrove](https://github.com/andygrove))

**Performance improvements:**

- Introduce StageManager for managing tasks stage by stage [\#1983](https://github.com/apache/arrow-datafusion/pull/1983) ([yahoNanJing](https://github.com/yahoNanJing))

**Closed issues:**

- Make expected result string in unit tests more readable [\#2412](https://github.com/apache/arrow-datafusion/issues/2412)
- remove duplicated `fn aggregate()` in aggregate expression tests [\#2399](https://github.com/apache/arrow-datafusion/issues/2399)
- split `distinct_expression.rs` into `count_distinct.rs` and `array_agg_distinct.rs` [\#2385](https://github.com/apache/arrow-datafusion/issues/2385)
- move sql tests in `context.rs` to corresponding test files in `datafustion/core/tests/sql` [\#2328](https://github.com/apache/arrow-datafusion/issues/2328)
- Date32/Date64 as join keys for merge join [\#2314](https://github.com/apache/arrow-datafusion/issues/2314)
- Error precision and scale for decimal coercion in logic comparison [\#2232](https://github.com/apache/arrow-datafusion/issues/2232)
- Support Multiple row layout [\#2188](https://github.com/apache/arrow-datafusion/issues/2188)
- Discussion: Is Ballista a standalone system or framework [\#1916](https://github.com/apache/arrow-datafusion/issues/1916)

**Merged pull requests:**

- MINOR: Enable multi-statement benchmark queries [\#2507](https://github.com/apache/arrow-datafusion/pull/2507) ([andygrove](https://github.com/andygrove))
- Persist session configs in scheduler [\#2501](https://github.com/apache/arrow-datafusion/pull/2501) ([thinkharderdev](https://github.com/thinkharderdev))
- Update to `sqlparser` `0.17.0` [\#2500](https://github.com/apache/arrow-datafusion/pull/2500) ([alamb](https://github.com/alamb))
- Limit cpu cores used when generating changelog [\#2494](https://github.com/apache/arrow-datafusion/pull/2494) ([andygrove](https://github.com/andygrove))
- MINOR: Parameterize changelog script [\#2484](https://github.com/apache/arrow-datafusion/pull/2484) ([jychen7](https://github.com/jychen7))
- Fix stage key extraction [\#2472](https://github.com/apache/arrow-datafusion/pull/2472) ([thinkharderdev](https://github.com/thinkharderdev))
- Add support for list\_dir\(\) on local fs [\#2467](https://github.com/apache/arrow-datafusion/pull/2467) ([wjones127](https://github.com/wjones127))
- minor: update versions and paths in changelog scripts [\#2429](https://github.com/apache/arrow-datafusion/pull/2429) ([andygrove](https://github.com/andygrove))
- Fix Ballista executing during plan [\#2428](https://github.com/apache/arrow-datafusion/pull/2428) ([tustvold](https://github.com/tustvold))
- Re-organize and rename aggregates physical plan [\#2388](https://github.com/apache/arrow-datafusion/pull/2388) ([yjshen](https://github.com/yjshen))
- Upgrade to arrow 13 [\#2382](https://github.com/apache/arrow-datafusion/pull/2382) ([alamb](https://github.com/alamb))
- Grouped Aggregate in row format [\#2375](https://github.com/apache/arrow-datafusion/pull/2375) ([yjshen](https://github.com/yjshen))
- Stop optimizing queries twice [\#2369](https://github.com/apache/arrow-datafusion/pull/2369) ([andygrove](https://github.com/andygrove))
- Bump follow-redirects from 1.13.2 to 1.14.9 in /ballista/ui/scheduler [\#2325](https://github.com/apache/arrow-datafusion/pull/2325) ([dependabot[bot]](https://github.com/apps/dependabot))
- Move FileType enum from sql module to logical\_plan module [\#2290](https://github.com/apache/arrow-datafusion/pull/2290) ([andygrove](https://github.com/andygrove))
- Add BatchPartitioner \(\#2285\) [\#2287](https://github.com/apache/arrow-datafusion/pull/2287) ([tustvold](https://github.com/tustvold))
- Update uuid requirement from 0.8 to 1.0 [\#2280](https://github.com/apache/arrow-datafusion/pull/2280) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump async from 2.6.3 to 2.6.4 in /ballista/ui/scheduler [\#2277](https://github.com/apache/arrow-datafusion/pull/2277) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump minimist from 1.2.5 to 1.2.6 in /ballista/ui/scheduler [\#2276](https://github.com/apache/arrow-datafusion/pull/2276) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump url-parse from 1.5.1 to 1.5.10 in /ballista/ui/scheduler [\#2275](https://github.com/apache/arrow-datafusion/pull/2275) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump nanoid from 3.1.20 to 3.3.3 in /ballista/ui/scheduler [\#2274](https://github.com/apache/arrow-datafusion/pull/2274) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update to Arrow 12.0.0, update tonic and prost [\#2253](https://github.com/apache/arrow-datafusion/pull/2253) ([alamb](https://github.com/alamb))
- Add ExecutorMetricsCollector interface [\#2234](https://github.com/apache/arrow-datafusion/pull/2234) ([thinkharderdev](https://github.com/thinkharderdev))
- minor: add editor config file [\#2224](https://github.com/apache/arrow-datafusion/pull/2224) ([jackwener](https://github.com/jackwener))
- \[Ballista\] Enable ApproxPercentileWithWeight in Ballista and fill UT  [\#2192](https://github.com/apache/arrow-datafusion/pull/2192) ([Ted-Jiang](https://github.com/Ted-Jiang))
- make nightly clippy happy [\#2186](https://github.com/apache/arrow-datafusion/pull/2186) ([xudong963](https://github.com/xudong963))
- \[Ballista\]Make PhysicalAggregateExprNode has repeated PhysicalExprNode [\#2184](https://github.com/apache/arrow-datafusion/pull/2184) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add LogicalPlan::SubqueryAlias [\#2172](https://github.com/apache/arrow-datafusion/pull/2172) ([andygrove](https://github.com/andygrove))
- Implement fast path of with\_new\_children\(\) in ExecutionPlan [\#2168](https://github.com/apache/arrow-datafusion/pull/2168) ([mingmwang](https://github.com/mingmwang))
- \[MINOR\] ignore suspicious slow test in Ballista [\#2167](https://github.com/apache/arrow-datafusion/pull/2167) ([Ted-Jiang](https://github.com/Ted-Jiang))
- enable explain for ballista [\#2163](https://github.com/apache/arrow-datafusion/pull/2163) ([doki23](https://github.com/doki23))
- Add delimiter for create external table [\#2162](https://github.com/apache/arrow-datafusion/pull/2162) ([matthewmturner](https://github.com/matthewmturner))
- Update sqlparser requirement from 0.15 to 0.16 [\#2152](https://github.com/apache/arrow-datafusion/pull/2152) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add IF NOT EXISTS to `CREATE TABLE` and `CREATE EXTERNAL TABLE` [\#2143](https://github.com/apache/arrow-datafusion/pull/2143) ([matthewmturner](https://github.com/matthewmturner))
- Update quarterly roadmap for Q2 [\#2133](https://github.com/apache/arrow-datafusion/pull/2133) ([matthewmturner](https://github.com/matthewmturner))
- \[Ballista\] Add ballista plugin manager and UDF plugin [\#2131](https://github.com/apache/arrow-datafusion/pull/2131) ([gaojun2048](https://github.com/gaojun2048))
- Serialize scalar UDFs in physical plan [\#2130](https://github.com/apache/arrow-datafusion/pull/2130) ([thinkharderdev](https://github.com/thinkharderdev))
- doc: update release schedule [\#2110](https://github.com/apache/arrow-datafusion/pull/2110) ([jychen7](https://github.com/jychen7))
- Reduce repetition in Decimal binary kernels, upgrade to arrow 11.1 [\#2107](https://github.com/apache/arrow-datafusion/pull/2107) ([alamb](https://github.com/alamb))
- update zlib version to 1.2.12 [\#2106](https://github.com/apache/arrow-datafusion/pull/2106) ([waitingkuo](https://github.com/waitingkuo))
- Add CREATE DATABASE command to SQL [\#2094](https://github.com/apache/arrow-datafusion/pull/2094) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- Refactor SessionContext, BallistaContext to support multi-tenancy configurations - Part 3 [\#2091](https://github.com/apache/arrow-datafusion/pull/2091) ([mingmwang](https://github.com/mingmwang))
- Remove dependency of common for the storage crate [\#2076](https://github.com/apache/arrow-datafusion/pull/2076) ([yahoNanJing](https://github.com/yahoNanJing))
- [MINOR] fix doc in `EXTRACT\(field FROM source\) [\#2074](https://github.com/apache/arrow-datafusion/pull/2074) ([Ted-Jiang](https://github.com/Ted-Jiang))
- \[Bug\]\[Datafusion\] fix TaskContext session\_config bug [\#2070](https://github.com/apache/arrow-datafusion/pull/2070) ([gaojun2048](https://github.com/gaojun2048))
- Short-circuit evaluation for `CaseWhen` [\#2068](https://github.com/apache/arrow-datafusion/pull/2068) ([yjshen](https://github.com/yjshen))
- split datafusion-object-store module [\#2065](https://github.com/apache/arrow-datafusion/pull/2065) ([yahoNanJing](https://github.com/yahoNanJing))
- Change log level for noisy logs [\#2060](https://github.com/apache/arrow-datafusion/pull/2060) ([thinkharderdev](https://github.com/thinkharderdev))
- Update to arrow/parquet 11.0 [\#2048](https://github.com/apache/arrow-datafusion/pull/2048) ([alamb](https://github.com/alamb))
- minor: format comments \(`//` to `// `\) [\#2047](https://github.com/apache/arrow-datafusion/pull/2047) ([jackwener](https://github.com/jackwener))
- use cargo-tomlfmt to check Cargo.toml formatting in CI [\#2033](https://github.com/apache/arrow-datafusion/pull/2033) ([WinkerDu](https://github.com/WinkerDu))
- Refactor SessionContext, SessionState and SessionConfig to support multi-tenancy configurations - Part 2 [\#2029](https://github.com/apache/arrow-datafusion/pull/2029) ([mingmwang](https://github.com/mingmwang))
- Simplify prerequisites for running examples [\#2028](https://github.com/apache/arrow-datafusion/pull/2028) ([doki23](https://github.com/doki23))
- Use SessionContext to parse Expr protobuf [\#2024](https://github.com/apache/arrow-datafusion/pull/2024) ([thinkharderdev](https://github.com/thinkharderdev))
- Fix stuck issue for the load testing of Push-based task scheduling [\#2006](https://github.com/apache/arrow-datafusion/pull/2006) ([yahoNanJing](https://github.com/yahoNanJing))
- Fixing a typo in documentation [\#1997](https://github.com/apache/arrow-datafusion/pull/1997) ([psvri](https://github.com/psvri))
- Fix minor clippy issue [\#1995](https://github.com/apache/arrow-datafusion/pull/1995) ([alamb](https://github.com/alamb))
- Make it possible to only scan part of a parquet file in a partition [\#1990](https://github.com/apache/arrow-datafusion/pull/1990) ([yjshen](https://github.com/yjshen))
- Update Dockerfile to fix integration tests [\#1982](https://github.com/apache/arrow-datafusion/pull/1982) ([andygrove](https://github.com/andygrove))
- Update sqlparser requirement from 0.14 to 0.15 [\#1966](https://github.com/apache/arrow-datafusion/pull/1966) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix logical conflict with protobuf [\#1958](https://github.com/apache/arrow-datafusion/pull/1958) ([alamb](https://github.com/alamb))
- Update to arrow 10.0.0, pyo3 0.16 [\#1957](https://github.com/apache/arrow-datafusion/pull/1957) ([alamb](https://github.com/alamb))
- update jit-related dependencies [\#1953](https://github.com/apache/arrow-datafusion/pull/1953) ([xudong963](https://github.com/xudong963))
- Allow different types of query variables \(`@@var`\) rather than just string [\#1943](https://github.com/apache/arrow-datafusion/pull/1943) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([maxburke](https://github.com/maxburke))
- Pruning serialization [\#1941](https://github.com/apache/arrow-datafusion/pull/1941) ([thinkharderdev](https://github.com/thinkharderdev))
- Fix select from EmptyExec always return 0 row after optimizer passes [\#1938](https://github.com/apache/arrow-datafusion/pull/1938) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Introduce Ballista query stage scheduler [\#1935](https://github.com/apache/arrow-datafusion/pull/1935) ([yahoNanJing](https://github.com/yahoNanJing))
- Add db benchmark script [\#1928](https://github.com/apache/arrow-datafusion/pull/1928) ([matthewmturner](https://github.com/matthewmturner))
- fix a typo [\#1919](https://github.com/apache/arrow-datafusion/pull/1919) ([vchag](https://github.com/vchag))
- \[MINOR\] Update copyright year in Docs [\#1918](https://github.com/apache/arrow-datafusion/pull/1918) ([alamb](https://github.com/alamb))
- add metadata to DFSchema, close \#1806. [\#1914](https://github.com/apache/arrow-datafusion/pull/1914) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([jiacai2050](https://github.com/jiacai2050))
- Refactor scheduler state mod [\#1913](https://github.com/apache/arrow-datafusion/pull/1913) ([yahoNanJing](https://github.com/yahoNanJing))
- Refactor the event channel [\#1912](https://github.com/apache/arrow-datafusion/pull/1912) ([yahoNanJing](https://github.com/yahoNanJing))
- Refactor scheduler server [\#1911](https://github.com/apache/arrow-datafusion/pull/1911) ([yahoNanJing](https://github.com/yahoNanJing))
- Clippy fix on nightly [\#1907](https://github.com/apache/arrow-datafusion/pull/1907) ([yjshen](https://github.com/yjshen))
- Updated Rust version to 1.59 in all the files [\#1903](https://github.com/apache/arrow-datafusion/pull/1903) ([NaincyKumariKnoldus](https://github.com/NaincyKumariKnoldus))
- Remove uneeded Mutex in Ballista Client [\#1898](https://github.com/apache/arrow-datafusion/pull/1898) ([alamb](https://github.com/alamb))
- Create a `datafusion-proto` crate for datafusion protobuf serialization [\#1887](https://github.com/apache/arrow-datafusion/pull/1887) ([carols10cents](https://github.com/carols10cents))
- Fix clippy lints [\#1885](https://github.com/apache/arrow-datafusion/pull/1885) ([HaoYang670](https://github.com/HaoYang670))
- Separate cpu-bound \(query-execution\) and IO-bound\(heartbeat\) to  … [\#1883](https://github.com/apache/arrow-datafusion/pull/1883) ([Ted-Jiang](https://github.com/Ted-Jiang))
- \[Minor\] Clean up DecimalArray API Usage [\#1869](https://github.com/apache/arrow-datafusion/pull/1869) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Changes after went through "Datafusion as a library section" [\#1868](https://github.com/apache/arrow-datafusion/pull/1868) ([nonontb](https://github.com/nonontb))
- Remove allow unused imports from ballista-core, then fix all warnings [\#1853](https://github.com/apache/arrow-datafusion/pull/1853) ([carols10cents](https://github.com/carols10cents))
- Update to arrow 9.1.0 [\#1851](https://github.com/apache/arrow-datafusion/pull/1851) ([alamb](https://github.com/alamb))
- move some tests out of context and into sql [\#1846](https://github.com/apache/arrow-datafusion/pull/1846) ([alamb](https://github.com/alamb))
- Fix compiling ballista  in standalone mode, add build to CI [\#1839](https://github.com/apache/arrow-datafusion/pull/1839) ([alamb](https://github.com/alamb))
- Update documentation example for change in API [\#1812](https://github.com/apache/arrow-datafusion/pull/1812) ([alamb](https://github.com/alamb))
- Refactor scheduler state with different management policy for volatile and stable states [\#1810](https://github.com/apache/arrow-datafusion/pull/1810) ([yahoNanJing](https://github.com/yahoNanJing))
- DataFusion + Conbench Integration [\#1791](https://github.com/apache/arrow-datafusion/pull/1791) ([dianaclarke](https://github.com/dianaclarke))
- Enable periodic cleanup of work\_dir directories in ballista executor [\#1783](https://github.com/apache/arrow-datafusion/pull/1783) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Use`eq_dyn`, `neq_dyn`, `lt_dyn`, `lt_eq_dyn`, `gt_dyn`, `gt_eq_dyn` kernels from arrow [\#1475](https://github.com/apache/arrow-datafusion/pull/1475) ([alamb](https://github.com/alamb))

## [7.1.0-rc1](https://github.com/apache/arrow-datafusion/tree/7.1.0-rc1) (2022-04-10)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.0.0-rc2...7.1.0-rc1)

**Implemented enhancements:**

- Support substring with three arguments: \(str, from, for\) for DataFrame API and Ballista [\#2092](https://github.com/apache/arrow-datafusion/issues/2092)
- UnionAll support for Ballista [\#2032](https://github.com/apache/arrow-datafusion/issues/2032)
- Separate cpu-bound and IO-bound work in ballista-executor by using diff tokio runtime. [\#1770](https://github.com/apache/arrow-datafusion/issues/1770)
- \[Ballista\] Introduce DAGScheduler for better managing the stage-based task scheduling [\#1704](https://github.com/apache/arrow-datafusion/issues/1704)
- \[Ballista\] Support to better manage cluster state, like alive executors, executor available task slots, etc [\#1703](https://github.com/apache/arrow-datafusion/issues/1703)

**Closed issues:**

- Optimize memory usage pattern to avoid "double memory" behavior [\#2149](https://github.com/apache/arrow-datafusion/issues/2149)
- Document approx\_percentile\_cont\_with\_weight in users guide [\#2078](https://github.com/apache/arrow-datafusion/issues/2078)
- \[follow up\]cleaning up statements.remove\(0\) [\#1986](https://github.com/apache/arrow-datafusion/issues/1986)
- Formatting error on documentation for Python [\#1873](https://github.com/apache/arrow-datafusion/issues/1873)
- Remove duplicate tests from `test_const_evaluator_scalar_functions` [\#1727](https://github.com/apache/arrow-datafusion/issues/1727)
- Question: Is the Ballista project providing value to the overall DataFusion project? [\#1273](https://github.com/apache/arrow-datafusion/issues/1273)

## [7.0.0-rc2](https://github.com/apache/arrow-datafusion/tree/7.0.0-rc2) (2022-02-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/7.0.0...7.0.0-rc2)

## [7.0.0](https://github.com/apache/arrow-datafusion/tree/7.0.0) (2022-02-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/6.0.0-rc0...7.0.0)

**Breaking changes:**

- Update `ExecutionPlan` to know about sortedness and repartitioning optimizer pass respect the invariants [\#1776](https://github.com/apache/arrow-datafusion/pull/1776) ([alamb](https://github.com/alamb))
- Update to `arrow 8.0.0` [\#1673](https://github.com/apache/arrow-datafusion/pull/1673) ([alamb](https://github.com/alamb))

**Implemented enhancements:**

- Task assignment between Scheduler and Executors [\#1221](https://github.com/apache/arrow-datafusion/issues/1221)
- Add `approx_median()` aggregate function [\#1729](https://github.com/apache/arrow-datafusion/pull/1729) ([realno](https://github.com/realno))
- \[Ballista\] Add Decimal128, Date64, TimestampSecond, TimestampMillisecond, Interv… [\#1659](https://github.com/apache/arrow-datafusion/pull/1659) ([gaojun2048](https://github.com/gaojun2048))
- Add `corr` aggregate function [\#1561](https://github.com/apache/arrow-datafusion/pull/1561) ([realno](https://github.com/realno))
- Add `covar`, `covar_pop` and `covar_samp` aggregate functions [\#1551](https://github.com/apache/arrow-datafusion/pull/1551) ([realno](https://github.com/realno))
- Add `approx_quantile()` aggregation function [\#1539](https://github.com/apache/arrow-datafusion/pull/1539) ([domodwyer](https://github.com/domodwyer))
- Initial MemoryManager and DiskManager APIs  for query execution + External Sort implementation [\#1526](https://github.com/apache/arrow-datafusion/pull/1526) ([yjshen](https://github.com/yjshen))
- Add `stddev` and `variance` [\#1525](https://github.com/apache/arrow-datafusion/pull/1525) ([realno](https://github.com/realno))
- Add `rem` operation for Expr [\#1467](https://github.com/apache/arrow-datafusion/pull/1467) ([liukun4515](https://github.com/liukun4515))
- Implement `array_agg` aggregate function [\#1300](https://github.com/apache/arrow-datafusion/pull/1300) ([viirya](https://github.com/viirya))

**Fixed bugs:**

- Ballista context::tests::test\_standalone\_mode test fails [\#1020](https://github.com/apache/arrow-datafusion/issues/1020)
- \[Ballista\] Fix scheduler state mod bug [\#1655](https://github.com/apache/arrow-datafusion/pull/1655) ([gaojun2048](https://github.com/gaojun2048))
- Pass local address host so we do not get mismatch between IPv4 and IP… [\#1466](https://github.com/apache/arrow-datafusion/pull/1466) ([thinkharderdev](https://github.com/thinkharderdev))
- Add Timezone to Scalar::Time\* types,   and better timezone awareness to Datafusion's time types [\#1455](https://github.com/apache/arrow-datafusion/pull/1455) ([maxburke](https://github.com/maxburke))

**Documentation updates:**

- Add dependencies to ballista example documentation [\#1346](https://github.com/apache/arrow-datafusion/pull/1346) ([jgoday](https://github.com/jgoday))
- \[MINOR\] Fix some typos. [\#1310](https://github.com/apache/arrow-datafusion/pull/1310) ([Ted-Jiang](https://github.com/Ted-Jiang))
- fix some clippy warnings from nightly channel [\#1277](https://github.com/apache/arrow-datafusion/pull/1277) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Jimexist](https://github.com/Jimexist))

**Performance improvements:**

- Introduce push-based task scheduling for Ballista [\#1560](https://github.com/apache/arrow-datafusion/pull/1560) ([yahoNanJing](https://github.com/yahoNanJing))

**Closed issues:**

- Track memory usage in Non Limited Operators [\#1569](https://github.com/apache/arrow-datafusion/issues/1569)
- \[Question\] Why does ballista store tables in the client instead of in the SchedulerServer [\#1473](https://github.com/apache/arrow-datafusion/issues/1473)
- Why use the expr types before coercion to get the result type? [\#1358](https://github.com/apache/arrow-datafusion/issues/1358)
- A problem about the projection\_push\_down optimizer gathers valid columns  [\#1312](https://github.com/apache/arrow-datafusion/issues/1312)
- apply constant folding to `LogicalPlan::Values` [\#1170](https://github.com/apache/arrow-datafusion/issues/1170)
- reduce usage of `IntoIterator<Item = Expr>` in logical plan builder window fn [\#372](https://github.com/apache/arrow-datafusion/issues/372)

**Merged pull requests:**

- Fix verification scripts for 7.0.0 release [\#1830](https://github.com/apache/arrow-datafusion/pull/1830) ([alamb](https://github.com/alamb))
- update README for ballista [\#1817](https://github.com/apache/arrow-datafusion/pull/1817) ([liukun4515](https://github.com/liukun4515))
- Fix logical conflict [\#1801](https://github.com/apache/arrow-datafusion/pull/1801) ([alamb](https://github.com/alamb))
- Improve the error message and UX of tpch benchmark program [\#1800](https://github.com/apache/arrow-datafusion/pull/1800) ([alamb](https://github.com/alamb))
- Update to sqlparser 0.14 [\#1796](https://github.com/apache/arrow-datafusion/pull/1796) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Update datafusion versions [\#1793](https://github.com/apache/arrow-datafusion/pull/1793) ([matthewmturner](https://github.com/matthewmturner))
- Update datafusion to use arrow 9.0.0 [\#1775](https://github.com/apache/arrow-datafusion/pull/1775) ([alamb](https://github.com/alamb))
- Update parking\_lot requirement from 0.11 to 0.12 [\#1735](https://github.com/apache/arrow-datafusion/pull/1735) ([dependabot[bot]](https://github.com/apps/dependabot))
- substitute `parking_lot::Mutex` for `std::sync::Mutex` [\#1720](https://github.com/apache/arrow-datafusion/pull/1720) ([xudong963](https://github.com/xudong963))
- Create ListingTableConfig which includes file format and schema inference [\#1715](https://github.com/apache/arrow-datafusion/pull/1715) ([matthewmturner](https://github.com/matthewmturner))
- Support `create_physical_expr` and `ExecutionContextState` or `DefaultPhysicalPlanner` for faster speed [\#1700](https://github.com/apache/arrow-datafusion/pull/1700) ([alamb](https://github.com/alamb))
- Use NamedTempFile rather than `String` in DiskManager [\#1680](https://github.com/apache/arrow-datafusion/pull/1680) ([alamb](https://github.com/alamb))
- Abstract over logical and physical plan representations in Ballista [\#1677](https://github.com/apache/arrow-datafusion/pull/1677) ([thinkharderdev](https://github.com/thinkharderdev))
- upgrade clap to version 3 [\#1672](https://github.com/apache/arrow-datafusion/pull/1672) ([Jimexist](https://github.com/Jimexist))
- Improve configuration and resource use of `MemoryManager` and `DiskManager` [\#1668](https://github.com/apache/arrow-datafusion/pull/1668) ([alamb](https://github.com/alamb))
- Make `MemoryManager` and `MemoryStream` public [\#1664](https://github.com/apache/arrow-datafusion/pull/1664) ([yjshen](https://github.com/yjshen))
- Consolidate Schema and RecordBatch projection [\#1638](https://github.com/apache/arrow-datafusion/pull/1638) ([alamb](https://github.com/alamb))
- Update hashbrown requirement from 0.11 to 0.12 [\#1631](https://github.com/apache/arrow-datafusion/pull/1631) ([dependabot[bot]](https://github.com/apps/dependabot))
- Update etcd-client requirement from 0.7 to 0.8 [\#1626](https://github.com/apache/arrow-datafusion/pull/1626) ([dependabot[bot]](https://github.com/apps/dependabot))
- update nightly version [\#1597](https://github.com/apache/arrow-datafusion/pull/1597) ([Jimexist](https://github.com/Jimexist))
- Add support show tables and show columns for ballista [\#1593](https://github.com/apache/arrow-datafusion/pull/1593) ([gaojun2048](https://github.com/gaojun2048))
- minor: improve the benchmark readme [\#1567](https://github.com/apache/arrow-datafusion/pull/1567) ([xudong963](https://github.com/xudong963))
- Consolidate `batch_size` configuration in `ExecutionConfig`, `RuntimeConfig` and `PhysicalPlanConfig` [\#1562](https://github.com/apache/arrow-datafusion/pull/1562) ([yjshen](https://github.com/yjshen))
- Update to rust 1.58 [\#1557](https://github.com/apache/arrow-datafusion/pull/1557) ([xudong963](https://github.com/xudong963))
- support mathematics operation for decimal data type [\#1554](https://github.com/apache/arrow-datafusion/pull/1554) ([liukun4515](https://github.com/liukun4515))
- Make call SchedulerServer::new once in ballista-scheduler process [\#1537](https://github.com/apache/arrow-datafusion/pull/1537) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Add load test command in tpch.rs. [\#1530](https://github.com/apache/arrow-datafusion/pull/1530) ([Ted-Jiang](https://github.com/Ted-Jiang))
- Remove one copy of ballista datatype serialization code [\#1524](https://github.com/apache/arrow-datafusion/pull/1524) ([alamb](https://github.com/alamb))
- Update to arrow-7.0.0 [\#1523](https://github.com/apache/arrow-datafusion/pull/1523) ([alamb](https://github.com/alamb))
- Workaround build failure: Pin quote to 1.0.10 [\#1499](https://github.com/apache/arrow-datafusion/pull/1499) ([alamb](https://github.com/alamb))
- add rfcs for datafusion [\#1490](https://github.com/apache/arrow-datafusion/pull/1490) ([xudong963](https://github.com/xudong963))
- support comparison for decimal data type and refactor the binary coercion rule [\#1483](https://github.com/apache/arrow-datafusion/pull/1483) ([liukun4515](https://github.com/liukun4515))
- Update arrow-rs to 6.4.0 and replace boolean comparison in datafusion with arrow compute kernel [\#1446](https://github.com/apache/arrow-datafusion/pull/1446) ([xudong963](https://github.com/xudong963))
- support cast/try\_cast for decimal: signed numeric to decimal [\#1442](https://github.com/apache/arrow-datafusion/pull/1442) ([liukun4515](https://github.com/liukun4515))
- use 0.13 sql parser [\#1435](https://github.com/apache/arrow-datafusion/pull/1435) ([Jimexist](https://github.com/Jimexist))
- Clarify communication on bi-weekly sync [\#1427](https://github.com/apache/arrow-datafusion/pull/1427) ([alamb](https://github.com/alamb))
- Minimize features [\#1399](https://github.com/apache/arrow-datafusion/pull/1399) ([carols10cents](https://github.com/carols10cents))
- Update rust vesion to 1.57 [\#1395](https://github.com/apache/arrow-datafusion/pull/1395) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Add coercion rules for AggregateFunctions [\#1387](https://github.com/apache/arrow-datafusion/pull/1387) ([liukun4515](https://github.com/liukun4515))
- upgrade the arrow-rs version [\#1385](https://github.com/apache/arrow-datafusion/pull/1385) ([liukun4515](https://github.com/liukun4515))
- Extract logical plan: rename the plan name \(follow up\) [\#1354](https://github.com/apache/arrow-datafusion/pull/1354) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([liukun4515](https://github.com/liukun4515))
- upgrade arrow-rs to 6.2.0 [\#1334](https://github.com/apache/arrow-datafusion/pull/1334) ([liukun4515](https://github.com/liukun4515))
- Update release instructions [\#1331](https://github.com/apache/arrow-datafusion/pull/1331) ([alamb](https://github.com/alamb))
- Extract Aggregate, Sort, and Join to struct from AggregatePlan [\#1326](https://github.com/apache/arrow-datafusion/pull/1326) ([matthewmturner](https://github.com/matthewmturner))
- Extract `EmptyRelation`, `Limit`, `Values` from `LogicalPlan` [\#1325](https://github.com/apache/arrow-datafusion/pull/1325) ([liukun4515](https://github.com/liukun4515))
- Extract CrossJoin, Repartition, Union in LogicalPlan [\#1322](https://github.com/apache/arrow-datafusion/pull/1322) ([liukun4515](https://github.com/liukun4515))
- Extract Explain, Analyze, Extension in LogicalPlan as independent struct [\#1317](https://github.com/apache/arrow-datafusion/pull/1317) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Extract CreateMemoryTable, DropTable, CreateExternalTable in LogicalPlan as independent struct [\#1311](https://github.com/apache/arrow-datafusion/pull/1311) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([liukun4515](https://github.com/liukun4515))
- Extract Projection, Filter, Window in LogicalPlan as independent struct [\#1309](https://github.com/apache/arrow-datafusion/pull/1309) ([ic4y](https://github.com/ic4y))
- Add PSQL comparison tests for except, intersect [\#1292](https://github.com/apache/arrow-datafusion/pull/1292) ([mrob95](https://github.com/mrob95))
- Extract logical plans in LogicalPlan as independent struct: TableScan [\#1290](https://github.com/apache/arrow-datafusion/pull/1290) ([xudong963](https://github.com/xudong963))

## [6.0.0-rc0](https://github.com/apache/arrow-datafusion/tree/6.0.0-rc0) (2021-11-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/6.0.0...6.0.0-rc0)

## [6.0.0](https://github.com/apache/arrow-datafusion/tree/6.0.0) (2021-11-14)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/ballista-0.6.0...6.0.0)


## [ballista-0.6.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.6.0) (2021-11-13)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/ballista-0.5.0...ballista-0.6.0)

**Breaking changes:**

- File partitioning for ListingTable [\#1141](https://github.com/apache/arrow-datafusion/pull/1141) ([rdettai](https://github.com/rdettai))
- Register tables in BallistaContext using TableProviders instead of Dataframe [\#1028](https://github.com/apache/arrow-datafusion/pull/1028) ([rdettai](https://github.com/rdettai))
- Make TableProvider.scan\(\) and PhysicalPlanner::create\_physical\_plan\(\) async [\#1013](https://github.com/apache/arrow-datafusion/pull/1013) ([rdettai](https://github.com/rdettai))
- Reorganize table providers by table format [\#1010](https://github.com/apache/arrow-datafusion/pull/1010) ([rdettai](https://github.com/rdettai))
- Move CBOs and Statistics to physical plan [\#965](https://github.com/apache/arrow-datafusion/pull/965) ([rdettai](https://github.com/rdettai))
- Update to sqlparser v 0.10.0 [\#934](https://github.com/apache/arrow-datafusion/pull/934) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- FilePartition and PartitionedFile for scanning flexibility [\#932](https://github.com/apache/arrow-datafusion/pull/932) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([yjshen](https://github.com/yjshen))
- Improve SQLMetric APIs, port existing metrics [\#908](https://github.com/apache/arrow-datafusion/pull/908) ([alamb](https://github.com/alamb))
- Add support for EXPLAIN ANALYZE [\#858](https://github.com/apache/arrow-datafusion/pull/858) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- Rename concurrency to target\_partitions [\#706](https://github.com/apache/arrow-datafusion/pull/706) ([andygrove](https://github.com/andygrove))

**Implemented enhancements:**

- Update datafusion-cli to support Ballista, or implement new ballista-cli [\#886](https://github.com/apache/arrow-datafusion/issues/886)
- Prepare Ballista crates for publishing [\#509](https://github.com/apache/arrow-datafusion/issues/509)
- Add drop table support [\#1266](https://github.com/apache/arrow-datafusion/pull/1266) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([viirya](https://github.com/viirya))
- use arrow 6.1.0 [\#1255](https://github.com/apache/arrow-datafusion/pull/1255) ([Jimexist](https://github.com/Jimexist))
- Add support for `create table as` via MemTable [\#1243](https://github.com/apache/arrow-datafusion/pull/1243) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Dandandan](https://github.com/Dandandan))
- add values list expression [\#1165](https://github.com/apache/arrow-datafusion/pull/1165) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Jimexist](https://github.com/Jimexist))
- Multiple files per partitions for CSV Avro Json [\#1138](https://github.com/apache/arrow-datafusion/pull/1138) ([rdettai](https://github.com/rdettai))
- Implement INTERSECT & INTERSECT DISTINCT [\#1135](https://github.com/apache/arrow-datafusion/pull/1135) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Simplify file struct abstractions [\#1120](https://github.com/apache/arrow-datafusion/pull/1120) ([rdettai](https://github.com/rdettai))
- Implement `is [not] distinct from` [\#1117](https://github.com/apache/arrow-datafusion/pull/1117) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Dandandan](https://github.com/Dandandan))
- add digest\(utf8, method\) function and refactor all current hash digest functions [\#1090](https://github.com/apache/arrow-datafusion/pull/1090) ([Jimexist](https://github.com/Jimexist))
- \[crypto\] add `blake3` algorithm to `digest` function [\#1086](https://github.com/apache/arrow-datafusion/pull/1086) ([Jimexist](https://github.com/Jimexist))
- \[crypto\] add blake2b and blake2s functions [\#1081](https://github.com/apache/arrow-datafusion/pull/1081) ([Jimexist](https://github.com/Jimexist))
-  Update sqlparser-rs to 0.11 [\#1052](https://github.com/apache/arrow-datafusion/pull/1052) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([alamb](https://github.com/alamb))
- remove hard coded partition count in ballista logicalplan deserialization [\#1044](https://github.com/apache/arrow-datafusion/pull/1044) ([xudong963](https://github.com/xudong963))
- Indexed field access for List [\#1006](https://github.com/apache/arrow-datafusion/pull/1006) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Igosuki](https://github.com/Igosuki))
- Update DataFusion to arrow 6.0 [\#984](https://github.com/apache/arrow-datafusion/pull/984) ([alamb](https://github.com/alamb))
- Implement Display for Expr, improve operator display [\#971](https://github.com/apache/arrow-datafusion/pull/971) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([matthewmturner](https://github.com/matthewmturner))
- ObjectStore API to read from remote storage systems [\#950](https://github.com/apache/arrow-datafusion/pull/950) ([yjshen](https://github.com/yjshen))
- fixes \#933 replace placeholder fmt\_as fr ExecutionPlan impls [\#939](https://github.com/apache/arrow-datafusion/pull/939) ([tiphaineruy](https://github.com/tiphaineruy))
- Support `NotLike` in Ballista [\#916](https://github.com/apache/arrow-datafusion/pull/916) ([Dandandan](https://github.com/Dandandan))
- Avro Table Provider [\#910](https://github.com/apache/arrow-datafusion/pull/910) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([Igosuki](https://github.com/Igosuki))
- Add BaselineMetrics, Timestamp metrics, add for `CoalescePartitionsExec`, rename output\_time -\> elapsed\_compute [\#909](https://github.com/apache/arrow-datafusion/pull/909) ([alamb](https://github.com/alamb))
- \[Ballista\] Add executor last seen info to the ui [\#895](https://github.com/apache/arrow-datafusion/pull/895) ([msathis](https://github.com/msathis))
- add cross join support to ballista [\#891](https://github.com/apache/arrow-datafusion/pull/891) ([houqp](https://github.com/houqp))
- Add Ballista support to DataFusion CLI [\#889](https://github.com/apache/arrow-datafusion/pull/889) ([andygrove](https://github.com/andygrove))
- Add support for PostgreSQL regex match [\#870](https://github.com/apache/arrow-datafusion/pull/870) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([b41sh](https://github.com/b41sh))

**Fixed bugs:**

- Test execution\_plans::shuffle\_writer::tests::test Fail [\#1040](https://github.com/apache/arrow-datafusion/issues/1040)
- Integration test fails to build docker images [\#918](https://github.com/apache/arrow-datafusion/issues/918)
- Ballista: Remove hard-coded concurrency from logical plan serde code [\#708](https://github.com/apache/arrow-datafusion/issues/708)
- How can I make ballista distributed compute work? [\#327](https://github.com/apache/arrow-datafusion/issues/327)
- fix subquery alias [\#1067](https://github.com/apache/arrow-datafusion/pull/1067) [[sql](https://github.com/apache/arrow-datafusion/labels/sql)] ([xudong963](https://github.com/xudong963))
- Fix compilation for ballista in stand-alone mode [\#1008](https://github.com/apache/arrow-datafusion/pull/1008) ([Igosuki](https://github.com/Igosuki))

**Documentation updates:**

- Add Ballista roadmap [\#1166](https://github.com/apache/arrow-datafusion/pull/1166) ([andygrove](https://github.com/andygrove))
- Adds note on compatible rust version [\#1097](https://github.com/apache/arrow-datafusion/pull/1097) ([1nF0rmed](https://github.com/1nF0rmed))
- implement `approx_distinct` function using HyperLogLog [\#1087](https://github.com/apache/arrow-datafusion/pull/1087) ([Jimexist](https://github.com/Jimexist))
- Improve User Guide [\#954](https://github.com/apache/arrow-datafusion/pull/954) ([andygrove](https://github.com/andygrove))
- Update plan\_query\_stages doc [\#951](https://github.com/apache/arrow-datafusion/pull/951) ([rdettai](https://github.com/rdettai))
- \[DataFusion\] -  Add show and show\_limit function for DataFrame [\#923](https://github.com/apache/arrow-datafusion/pull/923) ([francis-du](https://github.com/francis-du))
- update docs related to protoc and optional syntax [\#902](https://github.com/apache/arrow-datafusion/pull/902) ([Jimexist](https://github.com/Jimexist))
- Improve Ballista crate README content [\#878](https://github.com/apache/arrow-datafusion/pull/878) ([andygrove](https://github.com/andygrove))

**Performance improvements:**

- optimize build profile for datafusion python binding, cli and ballista [\#1137](https://github.com/apache/arrow-datafusion/pull/1137) ([houqp](https://github.com/houqp))

**Closed issues:**

- InList expr with NULL literals do not work [\#1190](https://github.com/apache/arrow-datafusion/issues/1190)
- update the homepage README to include values, `approx_distinct`, etc. [\#1171](https://github.com/apache/arrow-datafusion/issues/1171)
- \[Python\]: Inconsistencies with Python package name  [\#1011](https://github.com/apache/arrow-datafusion/issues/1011)
- Wanting to contribute to project where to start? [\#983](https://github.com/apache/arrow-datafusion/issues/983)
- delete redundant code [\#973](https://github.com/apache/arrow-datafusion/issues/973)
- How to build DataFusion python wheel  [\#853](https://github.com/apache/arrow-datafusion/issues/853)
- Produce a design for a metrics framework [\#21](https://github.com/apache/arrow-datafusion/issues/21)

**Merged pull requests:**

- \[nit\] simplify ballista executor `CollectExec` impl codes [\#1140](https://github.com/apache/arrow-datafusion/pull/1140) ([panarch](https://github.com/panarch))


For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

## [ballista-0.5.0](https://github.com/apache/arrow-datafusion/tree/ballista-0.5.0) (2021-08-10)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/4.0.0...ballista-0.5.0)

**Breaking changes:**

- \[ballista\] support date\_part and date\_turnc ser/de, pass tpch 7 [\#840](https://github.com/apache/arrow-datafusion/pull/840) ([houqp](https://github.com/houqp))
- Box ScalarValue:Lists, reduce size by half size [\#788](https://github.com/apache/arrow-datafusion/pull/788) ([alamb](https://github.com/alamb))
- Support DataFrame.collect for Ballista DataFrames [\#785](https://github.com/apache/arrow-datafusion/pull/785) ([andygrove](https://github.com/andygrove))
- JOIN conditions are order dependent [\#778](https://github.com/apache/arrow-datafusion/pull/778) ([seddonm1](https://github.com/seddonm1))
- UnresolvedShuffleExec should represent a single shuffle [\#727](https://github.com/apache/arrow-datafusion/pull/727) ([andygrove](https://github.com/andygrove))
- Ballista: Make shuffle partitions configurable in benchmarks [\#702](https://github.com/apache/arrow-datafusion/pull/702) ([andygrove](https://github.com/andygrove))
- Rename MergeExec to CoalescePartitionsExec [\#635](https://github.com/apache/arrow-datafusion/pull/635) ([andygrove](https://github.com/andygrove))
- Ballista: Rename QueryStageExec to ShuffleWriterExec [\#633](https://github.com/apache/arrow-datafusion/pull/633) ([andygrove](https://github.com/andygrove))
- fix 593, reduce cloning by taking ownership in logical planner's `from` fn [\#610](https://github.com/apache/arrow-datafusion/pull/610) ([Jimexist](https://github.com/Jimexist))
- fix join column handling logic for `On` and `Using` constraints [\#605](https://github.com/apache/arrow-datafusion/pull/605) ([houqp](https://github.com/houqp))
- Move ballista standalone mode to client [\#589](https://github.com/apache/arrow-datafusion/pull/589) ([edrevo](https://github.com/edrevo))
- Ballista: Implement map-side shuffle [\#543](https://github.com/apache/arrow-datafusion/pull/543) ([andygrove](https://github.com/andygrove))
- ShuffleReaderExec now supports multiple locations per partition [\#541](https://github.com/apache/arrow-datafusion/pull/541) ([andygrove](https://github.com/andygrove))
- Make external hostname in executor optional [\#232](https://github.com/apache/arrow-datafusion/pull/232) ([edrevo](https://github.com/edrevo))
- Remove namespace from executors [\#75](https://github.com/apache/arrow-datafusion/pull/75) ([edrevo](https://github.com/edrevo))
- Support qualified columns in queries [\#55](https://github.com/apache/arrow-datafusion/pull/55) ([houqp](https://github.com/houqp))
- Read CSV format text from stdin or memory [\#54](https://github.com/apache/arrow-datafusion/pull/54) ([heymind](https://github.com/heymind))
- Remove Ballista DataFrame [\#48](https://github.com/apache/arrow-datafusion/pull/48) ([andygrove](https://github.com/andygrove))
- Use atomics for SQLMetric implementation, remove unused name field [\#25](https://github.com/apache/arrow-datafusion/pull/25) ([returnString](https://github.com/returnString))

**Implemented enhancements:**

- Add crate documentation for Ballista crates [\#830](https://github.com/apache/arrow-datafusion/issues/830)
- Support DataFrame.collect for Ballista DataFrames [\#787](https://github.com/apache/arrow-datafusion/issues/787)
- Ballista: Prep for supporting shuffle correctly, part one [\#736](https://github.com/apache/arrow-datafusion/issues/736)
- Ballista: Implement physical plan serde for ShuffleWriterExec [\#710](https://github.com/apache/arrow-datafusion/issues/710)
- Ballista: Finish implementing shuffle mechanism [\#707](https://github.com/apache/arrow-datafusion/issues/707)
- Rename QueryStageExec to ShuffleWriterExec [\#542](https://github.com/apache/arrow-datafusion/issues/542)
- Ballista ShuffleReaderExec should be able to read from multiple locations per partition [\#540](https://github.com/apache/arrow-datafusion/issues/540)
- \[Ballista\] Use deployments in k8s user guide [\#473](https://github.com/apache/arrow-datafusion/issues/473)
- Ballista refactor QueryStageExec in preparation for map-side shuffle [\#458](https://github.com/apache/arrow-datafusion/issues/458)
- Ballista: Implement map-side of shuffle [\#456](https://github.com/apache/arrow-datafusion/issues/456)
- Refactor Ballista to separate Flight logic from execution logic [\#449](https://github.com/apache/arrow-datafusion/issues/449)
- Use published versions of arrow rather than github shas [\#393](https://github.com/apache/arrow-datafusion/issues/393)
- BallistaContext::collect\(\) logging is too noisy [\#352](https://github.com/apache/arrow-datafusion/issues/352)
- Update Ballista to use new physical plan formatter utility [\#343](https://github.com/apache/arrow-datafusion/issues/343)
- Add Ballista Getting Started documentation [\#329](https://github.com/apache/arrow-datafusion/issues/329)
- Remove references to ballistacompute Docker Hub repo [\#325](https://github.com/apache/arrow-datafusion/issues/325)
- Implement scalable distributed joins [\#63](https://github.com/apache/arrow-datafusion/issues/63)
- Remove hard-coded Ballista version from scripts [\#32](https://github.com/apache/arrow-datafusion/issues/32)
- Implement streaming versions of Dataframe.collect methods [\#789](https://github.com/apache/arrow-datafusion/pull/789) ([andygrove](https://github.com/andygrove))
- Ballista shuffle is finally working as intended, providing scalable distributed joins [\#750](https://github.com/apache/arrow-datafusion/pull/750) ([andygrove](https://github.com/andygrove))
- Update to use arrow 5.0 [\#721](https://github.com/apache/arrow-datafusion/pull/721) ([alamb](https://github.com/alamb))
- Implement serde for ShuffleWriterExec [\#712](https://github.com/apache/arrow-datafusion/pull/712) ([andygrove](https://github.com/andygrove))
- dedup using join column in wildcard expansion [\#678](https://github.com/apache/arrow-datafusion/pull/678) ([houqp](https://github.com/houqp))
- Implement metrics for shuffle read and write [\#676](https://github.com/apache/arrow-datafusion/pull/676) ([andygrove](https://github.com/andygrove))
- Remove hard-coded PartitionMode from Ballista serde [\#637](https://github.com/apache/arrow-datafusion/pull/637) ([andygrove](https://github.com/andygrove))
- Ballista: Implement scalable distributed joins [\#634](https://github.com/apache/arrow-datafusion/pull/634) ([andygrove](https://github.com/andygrove))
- Add Keda autoscaling for ballista in k8s [\#586](https://github.com/apache/arrow-datafusion/pull/586) ([edrevo](https://github.com/edrevo))
- Add some resiliency to lost executors [\#568](https://github.com/apache/arrow-datafusion/pull/568) ([edrevo](https://github.com/edrevo))
- Add `partition by` constructs in window functions and modify logical planning [\#501](https://github.com/apache/arrow-datafusion/pull/501) ([Jimexist](https://github.com/Jimexist))
- Support anti join [\#482](https://github.com/apache/arrow-datafusion/pull/482) ([Dandandan](https://github.com/Dandandan))
- add `order by` construct in window function and logical plans [\#463](https://github.com/apache/arrow-datafusion/pull/463) ([Jimexist](https://github.com/Jimexist))
- Refactor Ballista executor so that FlightService delegates to an Executor struct [\#450](https://github.com/apache/arrow-datafusion/pull/450) ([andygrove](https://github.com/andygrove))
- implement lead and lag built-in window function [\#429](https://github.com/apache/arrow-datafusion/pull/429) ([Jimexist](https://github.com/Jimexist))
- Implement fmt\_as for ShuffleReaderExec [\#400](https://github.com/apache/arrow-datafusion/pull/400) ([andygrove](https://github.com/andygrove))
- Add window expression part 1 - logical and physical planning, structure, to/from proto, and explain, for empty over clause only [\#334](https://github.com/apache/arrow-datafusion/pull/334) ([Jimexist](https://github.com/Jimexist))
- \[breaking change\] fix 265, log should be log10, and add ln [\#271](https://github.com/apache/arrow-datafusion/pull/271) ([Jimexist](https://github.com/Jimexist))
- Allow table providers to indicate their type for catalog metadata [\#205](https://github.com/apache/arrow-datafusion/pull/205) ([returnString](https://github.com/returnString))
- Add query 19 to TPC-H regression tests [\#59](https://github.com/apache/arrow-datafusion/pull/59) ([Dandandan](https://github.com/Dandandan))
- Use arrow eq kernels in CaseWhen expression evaluation [\#52](https://github.com/apache/arrow-datafusion/pull/52) ([Dandandan](https://github.com/Dandandan))
- Add option param for standalone mode [\#42](https://github.com/apache/arrow-datafusion/pull/42) ([djKooks](https://github.com/djKooks))
- \[DataFusion\] Optimize hash join inner workings, null handling fix [\#24](https://github.com/apache/arrow-datafusion/pull/24) ([Dandandan](https://github.com/Dandandan))
- \[Ballista\] Docker files for ui [\#22](https://github.com/apache/arrow-datafusion/pull/22) ([msathis](https://github.com/msathis))

**Fixed bugs:**

- Ballista: TPC-H q3 @ SF=1000 never completes [\#835](https://github.com/apache/arrow-datafusion/issues/835)
- Ballista does not support MIN/MAX aggregate functions [\#832](https://github.com/apache/arrow-datafusion/issues/832)
- Ballista docker images fail to build [\#828](https://github.com/apache/arrow-datafusion/issues/828)
- Ballista: UnresolvedShuffleExec should only have a single stage\_id [\#726](https://github.com/apache/arrow-datafusion/issues/726)
- Ballista integration tests are failing [\#623](https://github.com/apache/arrow-datafusion/issues/623)
- Integration test build failure due to arrow-rs using unstable feature [\#596](https://github.com/apache/arrow-datafusion/issues/596)
- `cargo build` cannot build the project [\#531](https://github.com/apache/arrow-datafusion/issues/531)
- ShuffleReaderExec does not get formatted correctly in displayable physical plan [\#399](https://github.com/apache/arrow-datafusion/issues/399)
- Implement serde for MIN and MAX [\#833](https://github.com/apache/arrow-datafusion/pull/833) ([andygrove](https://github.com/andygrove))
- Ballista: Prep for fixing shuffle mechansim, part 1 [\#738](https://github.com/apache/arrow-datafusion/pull/738) ([andygrove](https://github.com/andygrove))
- Ballista: Shuffle write bug fix [\#714](https://github.com/apache/arrow-datafusion/pull/714) ([andygrove](https://github.com/andygrove))
- honor table name for csv/parquet scan in ballista plan serde [\#629](https://github.com/apache/arrow-datafusion/pull/629) ([houqp](https://github.com/houqp))
- MINOR: Fix integration tests by adding datafusion-cli module to docker image [\#322](https://github.com/apache/arrow-datafusion/pull/322) ([andygrove](https://github.com/andygrove))

**Documentation updates:**

- Add minimal crate documentation for Ballista crates [\#831](https://github.com/apache/arrow-datafusion/pull/831) ([andygrove](https://github.com/andygrove))
- Add Ballista examples [\#775](https://github.com/apache/arrow-datafusion/pull/775) ([andygrove](https://github.com/andygrove))
- Update ballista.proto link in architecture doc [\#502](https://github.com/apache/arrow-datafusion/pull/502) ([terrycorley](https://github.com/terrycorley))
- Update k8s user guide to use deployments [\#474](https://github.com/apache/arrow-datafusion/pull/474) ([edrevo](https://github.com/edrevo))
- use prettier to format md files [\#367](https://github.com/apache/arrow-datafusion/pull/367) ([Jimexist](https://github.com/Jimexist))
- Make it easier for developers to find Ballista documentation [\#330](https://github.com/apache/arrow-datafusion/pull/330) ([andygrove](https://github.com/andygrove))
- Instructions for cross-compiling Ballista to the Raspberry Pi [\#263](https://github.com/apache/arrow-datafusion/pull/263) ([andygrove](https://github.com/andygrove))
- Add install guide in README [\#236](https://github.com/apache/arrow-datafusion/pull/236) ([djKooks](https://github.com/djKooks))

**Performance improvements:**

- Ballista: Avoid sleeping between polling for tasks [\#698](https://github.com/apache/arrow-datafusion/pull/698) ([Dandandan](https://github.com/Dandandan))
- Make BallistaContext::collect streaming [\#535](https://github.com/apache/arrow-datafusion/pull/535) ([edrevo](https://github.com/edrevo))

**Closed issues:**

- Confirm git tagging strategy for releases [\#770](https://github.com/apache/arrow-datafusion/issues/770)
- arrow::util::pretty::pretty\_format\_batches missing [\#769](https://github.com/apache/arrow-datafusion/issues/769)
- move the `assert_batches_eq!` macros to a non part of datafusion [\#745](https://github.com/apache/arrow-datafusion/issues/745)
- fix an issue where aliases are not respected in generating downstream schemas in window expr [\#592](https://github.com/apache/arrow-datafusion/issues/592)
- make the planner to print more succinct and useful information in window function explain clause [\#526](https://github.com/apache/arrow-datafusion/issues/526)
- move window frame module to be in `logical_plan` [\#517](https://github.com/apache/arrow-datafusion/issues/517)
- use a more rust idiomatic way of handling nth\_value [\#448](https://github.com/apache/arrow-datafusion/issues/448)
- Make Ballista not depend on arrow directly [\#446](https://github.com/apache/arrow-datafusion/issues/446)
- create a test with more than one partition for window functions [\#435](https://github.com/apache/arrow-datafusion/issues/435)
- Implement hash-partitioned hash aggregate [\#27](https://github.com/apache/arrow-datafusion/issues/27)
- Consider using GitHub pages for DataFusion/Ballista documentation [\#18](https://github.com/apache/arrow-datafusion/issues/18)
- Add Ballista to default cargo workspace [\#17](https://github.com/apache/arrow-datafusion/issues/17)
- Update "repository" in Cargo.toml [\#16](https://github.com/apache/arrow-datafusion/issues/16)
- Consolidate TPC-H benchmarks [\#6](https://github.com/apache/arrow-datafusion/issues/6)
- \[Ballista\] Fix integration test script [\#4](https://github.com/apache/arrow-datafusion/issues/4)
- Ballista should not have separate DataFrame implementation [\#2](https://github.com/apache/arrow-datafusion/issues/2)

**Merged pull requests:**

- Change datatype of tpch keys from Int32 to UInt64 to support sf=1000 [\#836](https://github.com/apache/arrow-datafusion/pull/836) ([andygrove](https://github.com/andygrove))
- Add ballista-examples to docker build [\#829](https://github.com/apache/arrow-datafusion/pull/829) ([andygrove](https://github.com/andygrove))
- Update dependencies: prost to 0.8 and tonic to 0.5 [\#818](https://github.com/apache/arrow-datafusion/pull/818) ([alamb](https://github.com/alamb))
- Move `hash_array` into hash\_utils.rs [\#807](https://github.com/apache/arrow-datafusion/pull/807) ([alamb](https://github.com/alamb))
- Fix: Update clippy lints for Rust 1.54 [\#794](https://github.com/apache/arrow-datafusion/pull/794) ([alamb](https://github.com/alamb))
- MINOR: Remove unused Ballista query execution code path [\#732](https://github.com/apache/arrow-datafusion/pull/732) ([andygrove](https://github.com/andygrove))
- \[fix\] benchmark run with compose [\#666](https://github.com/apache/arrow-datafusion/pull/666) ([rdettai](https://github.com/rdettai))
- bring back dev scripts for ballista [\#648](https://github.com/apache/arrow-datafusion/pull/648) ([Jimexist](https://github.com/Jimexist))
- Remove unnecessary mutex [\#639](https://github.com/apache/arrow-datafusion/pull/639) ([edrevo](https://github.com/edrevo))
- round trip TPCH queries in tests [\#630](https://github.com/apache/arrow-datafusion/pull/630) ([houqp](https://github.com/houqp))
- Fix build [\#627](https://github.com/apache/arrow-datafusion/pull/627) ([andygrove](https://github.com/andygrove))
- in ballista also check for UI prettier changes [\#578](https://github.com/apache/arrow-datafusion/pull/578) ([Jimexist](https://github.com/Jimexist))
- turn on clippy rule for needless borrow [\#545](https://github.com/apache/arrow-datafusion/pull/545) ([Jimexist](https://github.com/Jimexist))
- reuse datafusion physical planner in ballista building from protobuf [\#532](https://github.com/apache/arrow-datafusion/pull/532) ([Jimexist](https://github.com/Jimexist))
- update cargo.toml in python crate and fix unit test due to hash joins [\#483](https://github.com/apache/arrow-datafusion/pull/483) ([Jimexist](https://github.com/Jimexist))
- make `VOLUME` declaration in tpch datagen docker absolute [\#466](https://github.com/apache/arrow-datafusion/pull/466) ([crepererum](https://github.com/crepererum))
- Refactor QueryStageExec in preparation for implementing map-side shuffle [\#459](https://github.com/apache/arrow-datafusion/pull/459) ([andygrove](https://github.com/andygrove))
- Simplified usage of `use arrow` in ballista. [\#447](https://github.com/apache/arrow-datafusion/pull/447) ([jorgecarleitao](https://github.com/jorgecarleitao))
- Benchmark subcommand to distinguish between DataFusion and Ballista [\#402](https://github.com/apache/arrow-datafusion/pull/402) ([jgoday](https://github.com/jgoday))
- \#352: BallistaContext::collect\(\) logging is too noisy [\#394](https://github.com/apache/arrow-datafusion/pull/394) ([jgoday](https://github.com/jgoday))
- cleanup function return type fn [\#350](https://github.com/apache/arrow-datafusion/pull/350) ([Jimexist](https://github.com/Jimexist))
- Update Ballista to use new physical plan formatter utility [\#344](https://github.com/apache/arrow-datafusion/pull/344) ([andygrove](https://github.com/andygrove))
- Update arrow dependencies again [\#341](https://github.com/apache/arrow-datafusion/pull/341) ([alamb](https://github.com/alamb))
- Remove references to Ballista Docker images published to ballistacompute Docker Hub repo [\#326](https://github.com/apache/arrow-datafusion/pull/326) ([andygrove](https://github.com/andygrove))
- Update arrow-rs deps [\#317](https://github.com/apache/arrow-datafusion/pull/317) ([alamb](https://github.com/alamb))
- Update arrow deps [\#269](https://github.com/apache/arrow-datafusion/pull/269) ([alamb](https://github.com/alamb))
- Enable redundant\_field\_names clippy lint [\#261](https://github.com/apache/arrow-datafusion/pull/261) ([Dandandan](https://github.com/Dandandan))
- Update arrow-rs deps \(to fix build due to flatbuffers update\) [\#224](https://github.com/apache/arrow-datafusion/pull/224) ([alamb](https://github.com/alamb))
- update arrow-rs deps to latest master [\#216](https://github.com/apache/arrow-datafusion/pull/216) ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
