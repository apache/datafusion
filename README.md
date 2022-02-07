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

# DataFusion

<img src="docs/source/_static/images/DataFusion-Logo-Background-White.svg" width="256"/>

DataFusion is an extensible query execution framework, written in
Rust, that uses [Apache Arrow](https://arrow.apache.org) as its
in-memory format.

DataFusion supports both an SQL and a DataFrame API for building
logical query plans as well as a query optimizer and execution engine
capable of parallel execution against partitioned data sources (CSV
and Parquet) using threads.

DataFusion also supports distributed query execution via the
[Ballista](ballista/README.md) crate.

## Use Cases

DataFusion is used to create modern, fast and efficient data
pipelines, ETL processes, and database systems, which need the
performance of Rust and Apache Arrow and want to provide their users
the convenience of an SQL interface or a DataFrame API.

## Why DataFusion?

- _High Performance_: Leveraging Rust and Arrow's memory model, DataFusion achieves very high performance
- _Easy to Connect_: Being part of the Apache Arrow ecosystem (Arrow, Parquet and Flight), DataFusion works well with the rest of the big data ecosystem
- _Easy to Embed_: Allowing extension at almost any point in its design, DataFusion can be tailored for your specific usecase
- _High Quality_: Extensively tested, both by itself and with the rest of the Arrow ecosystem, DataFusion can be used as the foundation for production systems.

## Known Uses

Projects that adapt to or serve as plugins to DataFusion:

- [datafusion-python](https://github.com/datafusion-contrib/datafusion-python)
- [datafusion-java](https://github.com/datafusion-contrib/datafusion-java)
- [datafusion-ruby](https://github.com/j-a-m-l/datafusion-ruby)
- [datafusion-objectstore-s3](https://github.com/datafusion-contrib/datafusion-objectstore-s3)
- [datafusion-hdfs-native](https://github.com/datafusion-contrib/datafusion-hdfs-native)

Here are some of the projects known to use DataFusion:

- [Ballista](ballista) Distributed Compute Platform
- [Cloudfuse Buzz](https://github.com/cloudfuse-io/buzz-rust)
- [Cube Store](https://github.com/cube-js/cube.js/tree/master/rust)
- [delta-rs](https://github.com/delta-io/delta-rs)
- [InfluxDB IOx](https://github.com/influxdata/influxdb_iox) Time Series Database
- [ROAPI](https://github.com/roapi/roapi)
- [Tensorbase](https://github.com/tensorbase/tensorbase)
- [Squirtle](https://github.com/DSLAM-UMD/Squirtle)
- [VegaFusion](https://vegafusion.io/) Server-side acceleration for the [Vega](https://vega.github.io/) visualization grammar

(if you know of another project, please submit a PR to add a link!)

## Example Usage

Please see [example usage](https://arrow.apache.org/datafusion/user-guide/example-usage.html) to find how to use DataFusion.

## Roadmap

Please see [Roadmap](docs/source/specification/roadmap.md) for information of where the project is headed.

## Architecture Overview

There is no formal document describing DataFusion's architecture yet, but the following presentations offer a good overview of its different components and how they interact together.

- (March 2021): The DataFusion architecture is described in _Query Engine Design and the Rust-Based DataFusion in Apache Arrow_: [recording](https://www.youtube.com/watch?v=K6eCAVEk4kU) (DataFusion content starts [~ 15 minutes in](https://www.youtube.com/watch?v=K6eCAVEk4kU&t=875s)) and [slides](https://www.slideshare.net/influxdata/influxdb-iox-tech-talks-query-engine-design-and-the-rustbased-datafusion-in-apache-arrow-244161934)
- (February 2021): How DataFusion is used within the Ballista Project is described in \*Ballista: Distributed Compute with Rust and Apache Arrow: [recording](https://www.youtube.com/watch?v=ZZHQaOap9pQ)

## User's guide

Please see [User Guide](https://arrow.apache.org/datafusion/) for more information about DataFusion.

## Developer's guide

Please see [Developers Guide](DEVELOPERS.md) for information about developing DataFusion.
