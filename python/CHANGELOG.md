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

## [python-0.3.0](https://github.com/apache/arrow-datafusion/tree/python-0.3.0) (2021-08-10)

[Full Changelog](https://github.com/apache/arrow-datafusion/compare/4.0.0...python-0.3.0)

**Implemented enhancements:**

- add more math functions and unit tests to `python` crate  [\#748](https://github.com/apache/arrow-datafusion/pull/748) ([Jimexist](https://github.com/Jimexist))
- Expose ExecutionContext.register\_csv to the python bindings [\#524](https://github.com/apache/arrow-datafusion/pull/524) ([kszucs](https://github.com/kszucs))
- Implement missing join types for Python dataframe [\#503](https://github.com/apache/arrow-datafusion/pull/503) ([Dandandan](https://github.com/Dandandan))
- Add missing functions to python [\#388](https://github.com/apache/arrow-datafusion/pull/388) ([jgoday](https://github.com/jgoday))

**Fixed bugs:**

- fix maturin version in pyproject.toml [\#756](https://github.com/apache/arrow-datafusion/pull/756) ([Jimexist](https://github.com/Jimexist))
- fix pyarrow type id mapping in `python` crate [\#742](https://github.com/apache/arrow-datafusion/pull/742) ([Jimexist](https://github.com/Jimexist))

**Closed issues:**

- Confirm git tagging strategy for releases [\#770](https://github.com/apache/arrow-datafusion/issues/770)
- arrow::util::pretty::pretty\_format\_batches missing [\#769](https://github.com/apache/arrow-datafusion/issues/769)
- move the `assert_batches_eq!` macros to a non part of datafusion [\#745](https://github.com/apache/arrow-datafusion/issues/745)
- fix an issue where aliases are not respected in generating downstream schemas in window expr [\#592](https://github.com/apache/arrow-datafusion/issues/592)
- make the planner to print more succinct and useful information in window function explain clause [\#526](https://github.com/apache/arrow-datafusion/issues/526)
- move window frame module to be in `logical_plan` [\#517](https://github.com/apache/arrow-datafusion/issues/517)
- use a more rust idiomatic way of handling nth\_value [\#448](https://github.com/apache/arrow-datafusion/issues/448)
- create a test with more than one partition for window functions [\#435](https://github.com/apache/arrow-datafusion/issues/435)
- Implement hash-partitioned hash aggregate [\#27](https://github.com/apache/arrow-datafusion/issues/27)
- Consider using GitHub pages for DataFusion/Ballista documentation [\#18](https://github.com/apache/arrow-datafusion/issues/18)
- Update "repository" in Cargo.toml [\#16](https://github.com/apache/arrow-datafusion/issues/16)

**Merged pull requests:**

- fix python binding for `concat`, `concat_ws`, and `random` [\#768](https://github.com/apache/arrow-datafusion/pull/768) ([Jimexist](https://github.com/Jimexist))
- fix 226, make `concat`, `concat_ws`, and `random` work with `Python` crate [\#761](https://github.com/apache/arrow-datafusion/pull/761) ([Jimexist](https://github.com/Jimexist))
- fix python crate with the changes to logical plan builder [\#650](https://github.com/apache/arrow-datafusion/pull/650) ([Jimexist](https://github.com/Jimexist))
- use nightly nightly-2021-05-10 [\#536](https://github.com/apache/arrow-datafusion/pull/536) ([Jimexist](https://github.com/Jimexist))
- Define the unittests using pytest [\#493](https://github.com/apache/arrow-datafusion/pull/493) ([kszucs](https://github.com/kszucs))
- use requirements.txt to formalize python deps [\#484](https://github.com/apache/arrow-datafusion/pull/484) ([Jimexist](https://github.com/Jimexist))
- update cargo.toml in python crate and fix unit test due to hash joins [\#483](https://github.com/apache/arrow-datafusion/pull/483) ([Jimexist](https://github.com/Jimexist))
- simplify python function definitions [\#477](https://github.com/apache/arrow-datafusion/pull/477) ([Jimexist](https://github.com/Jimexist))
- Expose DataFrame::sort in the python bindings [\#469](https://github.com/apache/arrow-datafusion/pull/469) ([kszucs](https://github.com/kszucs))
- Revert "Revert "Add datafusion-python  \(\#69\)" \(\#257\)" [\#270](https://github.com/apache/arrow-datafusion/pull/270) ([andygrove](https://github.com/andygrove))
- Revert "Add datafusion-python  \(\#69\)" [\#257](https://github.com/apache/arrow-datafusion/pull/257) ([andygrove](https://github.com/andygrove))
- update arrow-rs deps to latest master [\#216](https://github.com/apache/arrow-datafusion/pull/216) ([alamb](https://github.com/alamb))
- Add datafusion-python  [\#69](https://github.com/apache/arrow-datafusion/pull/69) ([jorgecarleitao](https://github.com/jorgecarleitao))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
