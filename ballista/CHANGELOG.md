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

For older versions, see [apache/arrow/CHANGELOG.md](https://github.com/apache/arrow/blob/master/CHANGELOG.md)

# Changelog

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
