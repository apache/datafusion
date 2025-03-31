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

# Introduction

DataFusion is a very fast, extensible query engine for building
high-quality data-centric systems in [Rust](http://rustlang.org),
using the [Apache Arrow](https://arrow.apache.org) in-memory format.
DataFusion originated as part of the [Apache Arrow](https://arrow.apache.org/)
project.

DataFusion offers SQL and Dataframe APIs, excellent [performance](https://benchmark.clickhouse.com/), built-in support for CSV, Parquet, JSON, and Avro, [python bindings], extensive customization, a great community, and more.

[python bindings]: https://github.com/apache/datafusion-python

## Project Goals

DataFusion aims to be the query engine of choice for new, fast
data centric systems such as databases, dataframe libraries, machine
learning and streaming applications by leveraging the unique features
of [Rust](https://www.rust-lang.org/) and [Apache
Arrow](https://arrow.apache.org/).

## Features

- Feature-rich [SQL support](https://datafusion.apache.org/user-guide/sql/index.html) and [DataFrame API](https://datafusion.apache.org/user-guide/dataframe.html)
- Blazingly fast, vectorized, multi-threaded, streaming execution engine.
- Native support for Parquet, CSV, JSON, and Avro file formats. Support
  for custom file formats and non file datasources via the `TableProvider` trait.
- Many extension points: user defined scalar/aggregate/window functions, DataSources, SQL,
  other query languages, custom plan and execution nodes, optimizer passes, and more.
- Streaming, asynchronous IO directly from popular object stores, including AWS S3,
  Azure Blob Storage, and Google Cloud Storage (Other storage systems are supported via the
  `ObjectStore` trait).
- [Excellent Documentation](https://docs.rs/datafusion/latest) and a
  [welcoming community](https://datafusion.apache.org/contributor-guide/communication.html).
- A state of the art query optimizer with expression coercion and
  simplification, projection and filter pushdown, sort and distribution
  aware optimizations, automatic join reordering, and more.
- Permissive Apache 2.0 License, predictable and well understood
  [Apache Software Foundation](https://www.apache.org/) governance.
- Implementation in [Rust](https://www.rust-lang.org/), a modern
  system language with development productivity similar to Java or
  Golang, the performance of C++, and [loved by programmers
  everywhere](https://insights.stackoverflow.com/survey/2021#technology-most-loved-dreaded-and-wanted).
- Support for [Substrait](https://substrait.io/) query plans, to
  easily pass plans across language and system boundaries.

## Use Cases

DataFusion can be used without modification as an embedded SQL
engine or can be customized and used as a foundation for
building new systems.

While most current usecases are "analytic" or (throughput) some
components of DataFusion such as the plan representations, are
suitable for "streaming" and "transaction" style systems (low
latency).

Here are some example systems built using DataFusion:

- Specialized Analytical Database systems such as [HoraeDB] and more general Apache Spark like system such a [Ballista].
- New query language engines such as [prql-query] and accelerators such as [VegaFusion]
- Research platform for new Database Systems, such as [Flock]
- SQL support to another library, such as [dask sql]
- Streaming data platforms such as [Synnada]
- Tools for reading / sorting / transcoding Parquet, CSV, AVRO, and JSON files such as [qv]
- Native Spark runtime replacement such as [Blaze]

By using DataFusion, projects are freed to focus on their specific
features, and avoid reimplementing general (but still necessary)
features such as an expression representation, standard optimizations,
parellelized streaming execution plans, file format support, etc.

## Known Users

Here are some active projects using DataFusion:

 <!-- "Active" means github repositories that had at least one commit in the last 6 months -->

- [Arroyo](https://github.com/ArroyoSystems/arroyo) Distributed stream processing engine in Rust
- [Ballista](https://github.com/apache/datafusion-ballista) Distributed SQL Query Engine
- [Blaze](https://github.com/kwai/blaze) The Blaze accelerator for Apache Spark leverages native vectorized execution to accelerate query processing
- [CnosDB](https://github.com/cnosdb/cnosdb) Open Source Distributed Time Series Database
- [Comet](https://github.com/apache/datafusion-comet) Apache Spark native query execution plugin
- [Cube Store](https://github.com/cube-js/cube.js/tree/master/rust)
- [Dask SQL](https://github.com/dask-contrib/dask-sql) Distributed SQL query engine in Python
- [datafusion-dft](https://github.com/datafusion-contrib/datafusion-dft) Batteries included CLI, TUI, and server implementations for DataFusion.
- [delta-rs](https://github.com/delta-io/delta-rs) Native Rust implementation of Delta Lake
- [Exon](https://github.com/wheretrue/exon) Analysis toolkit for life-science applications
- [Funnel](https://funnel.io/) Data Platform powering Marketing Intelligence applications.
- [GlareDB](https://github.com/GlareDB/glaredb) Fast SQL database for querying and analyzing distributed data.
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) Open Source & Cloud Native Distributed Time Series Database
- [HoraeDB](https://github.com/apache/incubator-horaedb) Distributed Time-Series Database
- [InfluxDB](https://github.com/influxdata/influxdb) Time Series Database
- [Kamu](https://github.com/kamu-data/kamu-cli/) Planet-scale streaming data pipeline
- [LakeSoul](https://github.com/lakesoul-io/LakeSoul) Open source LakeHouse framework with native IO in Rust.
- [Lance](https://github.com/lancedb/lance) Modern columnar data format for ML
- [OpenObserve](https://github.com/openobserve/openobserve) Distributed cloud native observability platform
- [ParadeDB](https://github.com/paradedb/paradedb) PostgreSQL for Search & Analytics
- [Parseable](https://github.com/parseablehq/parseable) Log storage and observability platform
- [Polygon.io](https://polygon.io/) Stock Market API
- [qv](https://github.com/timvw/qv) Quickly view your data
- [Restate](https://github.com/restatedev) Easily build resilient applications using distributed durable async/await
- [ROAPI](https://github.com/roapi/roapi)
- [Sail](https://github.com/lakehq/sail) Unifying stream, batch, and AI workloads with Apache Spark compatibility
- [Seafowl](https://github.com/splitgraph/seafowl) CDN-friendly analytical database
- [Sleeper](https://github.com/gchq/sleeper) Serverless, cloud-native, log-structured merge tree based, scalable key-value store
- [Spice.ai](https://github.com/spiceai/spiceai) Unified SQL query interface & materialization engine
- [Synnada](https://synnada.ai/) Streaming-first framework for data products
- [VegaFusion](https://vegafusion.io/) Server-side acceleration for the [Vega](https://vega.github.io/) visualization grammar
- [Telemetry](https://telemetry.sh/) Structured logging made easy

Here are some less active projects that used DataFusion:

- [bdt](https://github.com/datafusion-contrib/bdt) Boring Data Tool
- [Cloudfuse Buzz](https://github.com/cloudfuse-io/buzz-rust)
- [Flock](https://github.com/flock-lab/flock)
- [Tensorbase](https://github.com/tensorbase/tensorbase)

[ballista]: https://github.com/apache/datafusion-ballista
[blaze]: https://github.com/blaze-init/blaze
[cloudfuse buzz]: https://github.com/cloudfuse-io/buzz-rust
[cnosdb]: https://github.com/cnosdb/cnosdb
[cube store]: https://github.com/cube-js/cube.js/tree/master/rust
[dask sql]: https://github.com/dask-contrib/dask-sql
[datafusion-tui]: https://github.com/datafusion-contrib/datafusion-tui
[delta-rs]: https://github.com/delta-io/delta-rs
[flock]: https://github.com/flock-lab/flock
[kamu]: https://github.com/kamu-data/kamu-cli
[greptime db]: https://github.com/GreptimeTeam/greptimedb
[horaedb]: https://github.com/apache/incubator-horaedb
[influxdb]: https://github.com/influxdata/influxdb
[openobserve]: https://github.com/openobserve/openobserve
[parseable]: https://github.com/parseablehq/parseable
[prql-query]: https://github.com/prql/prql-query
[qv]: https://github.com/timvw/qv
[roapi]: https://github.com/roapi/roapi
[seafowl]: https://github.com/splitgraph/seafowl
[spice.ai]: https://github.com/spiceai/spiceai
[synnada]: https://synnada.ai/
[tensorbase]: https://github.com/tensorbase/tensorbase
[vegafusion]: https://vegafusion.io/ "if you know of another project, please submit a PR to add a link!"

## Integrations and Extensions

There are a number of community projects that extend DataFusion or
provide integrations with other systems, some of which are described below:

### Language Bindings

- [datafusion-c](https://github.com/datafusion-contrib/datafusion-c)
- [datafusion-python](https://github.com/apache/datafusion-python)
- [datafusion-ruby](https://github.com/datafusion-contrib/datafusion-ruby)
- [datafusion-java](https://github.com/datafusion-contrib/datafusion-java)

### Integrations

- [datafusion-bigtable](https://github.com/datafusion-contrib/datafusion-bigtable)
- [datafusion-catalogprovider-glue](https://github.com/datafusion-contrib/datafusion-catalogprovider-glue)
- [datafusion-federation](https://github.com/datafusion-contrib/datafusion-federation)

## Why DataFusion?

- _High Performance_: Leveraging Rust and Arrow's memory model, DataFusion is very fast.
- _Easy to Connect_: Being part of the Apache Arrow ecosystem (Arrow, Parquet and Flight), DataFusion works well with the rest of the big data ecosystem
- _Easy to Embed_: Allowing extension at almost any point in its design, and published regularly as a crate on [crates.io](http://crates.io), DataFusion can be integrated and tailored for your specific usecase.
- _High Quality_: Extensively tested, both by itself and with the rest of the Arrow ecosystem, DataFusion can and is used as the foundation for production systems.
