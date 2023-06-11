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
DataFusion is part of the [Apache Arrow](https://arrow.apache.org/)
project.

DataFusion offers SQL and Dataframe APIs, excellent [performance](https://benchmark.clickhouse.com/), built-in support for CSV, Parquet, JSON, and Avro, extensive customization, and a great community.

## Project Goals

DataFusion's goal is to be the query engine of choice for new, fast
data centric systems such as databases, dataframe libraries, machine
learning and streaming applications by leveraging the unique features
of [Rust](https://www.rust-lang.org/) and [Apache
Arrow](https://arrow.apache.org/).

## Features

- Feature-rich [SQL support](https://arrow.apache.org/datafusion/user-guide/sql/index.html) and [DataFrame API](https://arrow.apache.org/datafusion/user-guide/dataframe.html)
- Blazingly fast, vectorized, multi-threaded, streaming execution engine.
- Native support for Parquet, CSV, JSON, and Avro file formats. Support
  for custom file formats and non file datasources via the `TableProvider` trait.
- Many extension points: user defined scalar/aggregate/window functions, DataSources, SQL,
  other query languages, custom plan and execution nodes, optimizer passes, and more.
- Streaming, asynchronous IO directly from popular object stores, including AWS S3,
  Azure Blob Storage, and Google Cloud Storage. Other storage systems are supported via the
  `ObjectStore` trait.
- [Excellent Documentation](https://docs.rs/datafusion/latest) and a
  [welcoming community](https://arrow.apache.org/datafusion/contributor-guide/communication.html).
- A state of the art query optimizer with projection and filter pushdown, sort aware optimizations,
  automatic join reordering, expression coercion, and more.
- Permissive Apache 2.0 License, predictable and well understood [Apache Software Foundation](https://www.apache.org/) governance.
- Implementation in [Rust](https://www.rust-lang.org/), a modern
  system language with development productivity similar to Java or
  Golang, the performance of C++, and [loved by programmers
  everywhere](https://insights.stackoverflow.com/survey/2021#technology-most-loved-dreaded-and-wanted).
- Support for [Substrait](https://substrait.io/) formatted query plans,
  making it easier to integrate DataFusion with other
  projects, and to pass plans across language boundaries.

## Use Cases

DataFusion can be used without modification as an embedded SQL
engine or can be customized and used as a foundation for
building new systems.

While most current usecases are "analytic" or (focusing on data
throughput) some components of DataFusion such as the plan
representations, are suitable for "streaming" and "transaction" style
systems (focused on latency).

Here are some examples of systems built using DataFusion:

- Specialized Analytical Database systems such as [CeresDB] and more general Apache Spark like system such a [Ballista].
- New query language engines such as [prql-query] and accelerators such as [VegaFusion]
- Research platform for new Database Systems, such as [Flock]
- SQL support to another library, such as [dask sql]
- Streaming data platforms such as [Synnada]
- Tools for reading / sorting / transcoding Parquet, CSV, AVRO, and JSON files such as [qv]
- A faster Spark runtime replacement [Blaze]

By using DataFusion, the projects are freed to focus on their specific
features, and avoid reimplementing general (but still necessary)
features such as an expression representation, standard optimizations,
execution plans, file format support, etc.

## Known Users

Here are some of the projects known to use DataFusion:

- [Ballista](https://github.com/apache/arrow-ballista) Distributed SQL Query Engine
- [Blaze](https://github.com/blaze-init/blaze) Spark accelerator with DataFusion at its core
- [CeresDB](https://github.com/CeresDB/ceresdb) Distributed Time-Series Database
- [Cloudfuse Buzz](https://github.com/cloudfuse-io/buzz-rust)
- [CnosDB](https://github.com/cnosdb/cnosdb) Open Source Distributed Time Series Database
- [Cube Store](https://github.com/cube-js/cube.js/tree/master/rust)
- [Dask SQL](https://github.com/dask-contrib/dask-sql) Distributed SQL query engine in Python
- [datafusion-tui](https://github.com/datafusion-contrib/datafusion-tui) Text UI for DataFusion
- [delta-rs](https://github.com/delta-io/delta-rs) Native Rust implementation of Delta Lake
- [Flock](https://github.com/flock-lab/flock)
- [GreptimeDB](https://github.com/GreptimeTeam/greptimedb) Open Source & Cloud Native Distributed Time Series Database
- [InfluxDB IOx](https://github.com/influxdata/influxdb_iox) Time Series Database
- [Kamu](https://github.com/kamu-data/kamu-cli/) Planet-scale streaming data pipeline
- [Parseable](https://github.com/parseablehq/parseable) Log storage and observability platform
- [qv](https://github.com/timvw/qv) Quickly view your data
- [bdt](https://github.com/andygrove/bdt) Boring Data Tool
- [ROAPI](https://github.com/roapi/roapi)
- [Seafowl](https://github.com/splitgraph/seafowl) CDN-friendly analytical database
- [Synnada](https://synnada.ai/) Streaming-first framework for data products
- [Tensorbase](https://github.com/tensorbase/tensorbase)
- [VegaFusion](https://vegafusion.io/) Server-side acceleration for the [Vega](https://vega.github.io/) visualization grammar
- [ZincObserve](https://github.com/zinclabs/zincobserve) Distributed cloud native observability platform

[ballista]: https://github.com/apache/arrow-ballista
[blaze]: https://github.com/blaze-init/blaze
[ceresdb]: https://github.com/CeresDB/ceresdb
[cloudfuse buzz]: https://github.com/cloudfuse-io/buzz-rust
[cnosdb]: https://github.com/cnosdb/cnosdb
[cube store]: https://github.com/cube-js/cube.js/tree/master/rust
[dask sql]: https://github.com/dask-contrib/dask-sql
[datafusion-tui]: https://github.com/datafusion-contrib/datafusion-tui
[delta-rs]: https://github.com/delta-io/delta-rs
[flock]: https://github.com/flock-lab/flock
[kamu]: https://github.com/kamu-data/kamu-cli
[greptime db]: https://github.com/GreptimeTeam/greptimedb
[influxdb iox]: https://github.com/influxdata/influxdb_iox
[parseable]: https://github.com/parseablehq/parseable
[prql-query]: https://github.com/prql/prql-query
[qv]: https://github.com/timvw/qv
[roapi]: https://github.com/roapi/roapi
[seafowl]: https://github.com/splitgraph/seafowl
[synnada]: https://synnada.ai/
[tensorbase]: https://github.com/tensorbase/tensorbase
[vegafusion]: https://vegafusion.io/
[zincobserve]: https://github.com/zinclabs/zincobserve "if you know of another project, please submit a PR to add a link!"

## Integrations and Extensions

There are a number of community projects that extend DataFusion or
provide integrations with other systems.

### Language Bindings

- [datafusion-c](https://github.com/datafusion-contrib/datafusion-c)
- [datafusion-python](https://github.com/apache/arrow-datafusion-python)
- [datafusion-ruby](https://github.com/datafusion-contrib/datafusion-ruby)
- [datafusion-java](https://github.com/datafusion-contrib/datafusion-java)

### Integrations

- [datafusion-bigtable](https://github.com/datafusion-contrib/datafusion-bigtable)
- [datafusion-catalogprovider-glue](https://github.com/datafusion-contrib/datafusion-catalogprovider-glue)

## Why DataFusion?

- _High Performance_: Leveraging Rust and Arrow's memory model, DataFusion is very fast.
- _Easy to Connect_: Being part of the Apache Arrow ecosystem (Arrow, Parquet and Flight), DataFusion works well with the rest of the big data ecosystem
- _Easy to Embed_: Allowing extension at almost any point in its design, DataFusion can be tailored for your specific usecase
- _High Quality_: Extensively tested, both by itself and with the rest of the Arrow ecosystem, DataFusion can be used as the foundation for production systems.
