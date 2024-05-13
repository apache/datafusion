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
[![Discord chat][discord-badge]][discord-url]

[crates-badge]: https://img.shields.io/crates/v/datafusion.svg
[crates-url]: https://crates.io/crates/datafusion
[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/apache/datafusion/blob/main/LICENSE.txt
[actions-badge]: https://github.com/apache/datafusion/actions/workflows/rust.yml/badge.svg
[actions-url]: https://github.com/apache/datafusion/actions?query=branch%3Amain
[discord-badge]: https://img.shields.io/discord/885562378132000778.svg?logo=discord&style=flat-square
[discord-url]: https://discord.com/invite/Qw5gKqHxUM

[Website](https://github.com/apache/datafusion) |
[Guides](https://github.com/apache/datafusion/tree/main/docs) |
[API Docs](https://docs.rs/datafusion/latest/datafusion/) |
[Chat](https://discord.com/channels/885562378132000778/885562378132000781)

<img src="./docs/source/_static/images/2x_bgwhite_original.png" width="512" alt="logo"/>

Apache DataFusion is a very fast, extensible query engine for building high-quality data-centric systems in
[Rust](http://rustlang.org), using the [Apache Arrow](https://arrow.apache.org)
in-memory format. [Python Bindings](https://github.com/apache/datafusion-python) are also available. DataFusion offers SQL and Dataframe APIs, excellent [performance](https://benchmark.clickhouse.com/), built-in support for CSV, Parquet, JSON, and Avro, extensive customization, and a great community.

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

- `array_expressions`: functions for working with arrays such as `array_to_string`
- `compression`: reading files compressed with `xz2`, `bzip2`, `flate2`, and `zstd`
- `crypto_expressions`: cryptographic functions such as `md5` and `sha256`
- `datetime_expressions`: date and time functions such as `to_timestamp`
- `encoding_expressions`: `encode` and `decode` functions
- `parquet`: support for reading the [Apache Parquet] format
- `regex_expressions`: regular expression functions, such as `regexp_match`
- `unicode_expressions`: Include unicode aware functions such as `character_length`
- `unparser` : enables support to reverse LogicalPlans back into SQL

Optional features:

- `avro`: support for reading the [Apache Avro] format
- `backtrace`: include backtrace information in error messages
- `pyarrow`: conversions between PyArrow and DataFusion types
- `serde`: enable arrow-schema's `serde` feature

[apache avro]: https://avro.apache.org/
[apache parquet]: https://parquet.apache.org/

## Rust Version Compatibility Policy

DataFusion's Minimum Required Stable Rust Version (MSRV) policy is to support
each stable Rust version for 6 months after it is
[released](https://github.com/rust-lang/rust/blob/master/RELEASES.md). This
generally translates to support for the most recent 3 to 4 stable Rust versions.

We enforce this policy using a [MSRV CI Check](https://github.com/search?q=repo%3Aapache%2Fdatafusion+rust-version+language%3ATOML+path%3A%2F%5ECargo.toml%2F&type=code)
