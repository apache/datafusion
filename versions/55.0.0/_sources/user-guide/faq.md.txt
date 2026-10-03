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

# Frequently Asked Questions

## What is the relationship between Apache Arrow, DataFusion, and Ballista?

Apache Arrow is a library which provides a standardized memory representation for columnar data. It also provides
"kernels" for performing common operations on this data.

DataFusion is a library for executing queries in-process using the Apache Arrow memory
model and computational kernels. It is designed to run within a single process, using threads
for parallel query execution.

[Ballista](https://github.com/apache/datafusion-ballista) is a distributed compute platform built on DataFusion.

# How does DataFusion Compare with `XYZ`?

When compared to similar systems, DataFusion typically is:

1. Targeted at developers, rather than end users / data scientists.
2. Designed to be embedded, rather than a complete file based SQL system.
3. Governed by the [Apache Software Foundation](https://www.apache.org/) process, rather than a single company or individual.
4. Implemented in `Rust`, rather than `C/C++`

Here is a comparison with similar projects that may help understand
when DataFusion might be suitable or unsuitable for your needs:

- [DuckDB](https://www.duckdb.org) is an open source, in process analytic database.
  Like DataFusion, it supports very fast execution, both from its custom file format
  and directly from parquet files. Unlike DataFusion, it is written in C/C++ and it
  is primarily used directly by users as a serverless database and query system rather
  than as a library for building such database systems.

- [Polars](http://pola.rs): Polars is one of the fastest DataFrame
  libraries at the time of writing. Like DataFusion, it is also
  written in Rust and uses the Apache Arrow memory model, but unlike
  DataFusion it is not designed with as many extension points.

- [Facebook Velox](https://github.com/facebookincubator/velox)
  is an execution engine. Like DataFusion, Velox aims to
  provide a reusable foundation for building database-like systems. Unlike DataFusion,
  it is written in C/C++ and does not include a SQL frontend or planning / optimization
  framework.

- [Databend](https://github.com/datafuselabs/databend) is a complete
  database system. Like DataFusion it is also written in Rust and
  utilizes the Apache Arrow memory model, but unlike DataFusion it
  targets end-users rather than developers of other database systems.
