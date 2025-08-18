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

# Apache DataFusion

[![Crates.io][crates-badge]][crates-url]
[![Apache licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]
![Commit Activity][commit-activity-badge]
[![Open Issues][open-issues-badge]][open-issues-url]
[![Discord chat][discord-badge]][discord-url]
[![Linkedin][linkedin-badge]][linkedin-url]
![Crates.io MSRV][msrv-badge]

[crates-badge]: https://img.shields.io/crates/v/datafusion.svg
[crates-url]: https://crates.io/crates/datafusion
[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/apache/datafusion/blob/main/LICENSE.txt
[actions-badge]: https://github.com/apache/datafusion/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/apache/datafusion/actions?query=branch%3Amain
[discord-badge]: https://img.shields.io/badge/Chat-Discord-purple
[discord-url]: https://discord.com/invite/Qw5gKqHxUM
[commit-activity-badge]: https://img.shields.io/github/commit-activity/m/apache/datafusion
[open-issues-badge]: https://img.shields.io/github/issues-raw/apache/datafusion
[open-issues-url]: https://github.com/apache/datafusion/issues
[linkedin-badge]: https://img.shields.io/badge/Follow-Linkedin-blue
[linkedin-url]: https://www.linkedin.com/company/apache-datafusion/
[msrv-badge]: https://img.shields.io/crates/msrv/datafusion?label=Min%20Rust%20Version

[Website](https://datafusion.apache.org/) |
[API Docs](https://docs.rs/datafusion/latest/datafusion/) |
[Chat](https://discord.com/channels/885562378132000778/885562378132000781)

<a href="https://datafusion.apache.org/">
  <img src="https://github.com/apache/datafusion/raw/HEAD/docs/source/_static/images/2x_bgwhite_original.png" width="512" alt="logo"/>
</a>

DataFusion is an extensible query engine written in [Rust] that
uses [Apache Arrow] as its in-memory format.

This crate provides libraries and binaries for developers building fast and
feature rich database and analytic systems, customized to particular workloads.
See [use cases] for examples. The following related subprojects target end users:

- [DataFusion Python](https://github.com/apache/datafusion-python/) offers a Python interface for SQL and DataFrame
  queries.
- [DataFusion Comet](https://github.com/apache/datafusion-comet/) is an accelerator for Apache Spark based on
  DataFusion.

"Out of the box,"
DataFusion offers [SQL] and [`Dataframe`] APIs, excellent [performance],
built-in support for CSV, Parquet, JSON, and Avro, extensive customization, and
a great community.

DataFusion features a full query planner, a columnar, streaming, multi-threaded,
vectorized execution engine, and partitioned data sources. You can
customize DataFusion at almost all points including additional data sources,
query languages, functions, custom operators and more.
See the [Architecture] section for more details.

[rust]: http://rustlang.org
[apache arrow]: https://arrow.apache.org
[use cases]: https://datafusion.apache.org/user-guide/introduction.html#use-cases
[python bindings]: https://github.com/apache/datafusion-python
[performance]: https://benchmark.clickhouse.com/
[architecture]: https://datafusion.apache.org/contributor-guide/architecture.html

Here are links to some important information

- [Project Site](https://datafusion.apache.org/)
- [Installation](https://datafusion.apache.org/user-guide/cli/installation.html)
- [Rust Getting Started](https://datafusion.apache.org/user-guide/example-usage.html)
- [Rust DataFrame API](https://datafusion.apache.org/user-guide/dataframe.html)
- [Rust API docs](https://docs.rs/datafusion/latest/datafusion)
- [Rust Examples](https://github.com/apache/datafusion/tree/main/datafusion-examples)
- [Python DataFrame API](https://arrow.apache.org/datafusion-python/)
- [Architecture](https://docs.rs/datafusion/latest/datafusion/index.html#architecture)

## What can you do with this crate?

DataFusion is great for building projects such as domain specific query engines, new database platforms and data pipelines, query languages and more.
It lets you start quickly from a fully working engine, and then customize those features specific to your use. [Click Here](https://datafusion.apache.org/user-guide/introduction.html#known-users) to see a list known users.

## Contributing to DataFusion

Please see the [contributor guide] and [communication] pages for more information.

[contributor guide]: https://datafusion.apache.org/contributor-guide
[communication]: https://datafusion.apache.org/contributor-guide/communication.html

## Crate features

This crate has several [features] which can be specified in your `Cargo.toml`.

[features]: https://doc.rust-lang.org/cargo/reference/features.html

Default features:

- `nested_expressions`: functions for working with nested type function such as `array_to_string`
- `compression`: reading files compressed with `xz2`, `bzip2`, `flate2`, and `zstd`
- `crypto_expressions`: cryptographic functions such as `md5` and `sha256`
- `datetime_expressions`: date and time functions such as `to_timestamp`
- `encoding_expressions`: `encode` and `decode` functions
- `parquet`: support for reading the [Apache Parquet] format
- `parquet_encryption`: support for using [Parquet Modular Encryption]
- `regex_expressions`: regular expression functions, such as `regexp_match`
- `unicode_expressions`: Include unicode aware functions such as `character_length`
- `unparser`: enables support to reverse LogicalPlans back into SQL
- `recursive_protection`: uses [recursive](https://docs.rs/recursive/latest/recursive/) for stack overflow protection.

Optional features:

- `avro`: support for reading the [Apache Avro] format
- `backtrace`: include backtrace information in error messages
- `pyarrow`: conversions between PyArrow and DataFusion types
- `serde`: enable arrow-schema's `serde` feature

[apache avro]: https://avro.apache.org/
[apache parquet]: https://parquet.apache.org/
[parquet modular encryption]: https://parquet.apache.org/docs/file-format/data-pages/encryption/

## DataFusion API Evolution and Deprecation Guidelines

Public methods in Apache DataFusion evolve over time: while we try to maintain a
stable API, we also improve the API over time. As a result, we typically
deprecate methods before removing them, according to the [deprecation guidelines].

[deprecation guidelines]: https://datafusion.apache.org/library-user-guide/api-health.html

## Dependencies and `Cargo.lock`

Following the [guidance] on committing `Cargo.lock` files, this project commits
its `Cargo.lock` file.

CI uses the committed `Cargo.lock` file, and dependencies are updated regularly
using [Dependabot] PRs.

[guidance]: https://blog.rust-lang.org/2023/08/29/committing-lockfiles.html
[dependabot]: https://docs.github.com/en/code-security/dependabot/working-with-dependabot
