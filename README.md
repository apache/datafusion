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

[![Coverage Status](https://codecov.io/gh/apache/arrow-datafusion/rust/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/arrow-datafusion?branch=master)

<img src="https://arrow.apache.org/datafusion/_images/DataFusion-Logo-Background-White.png" width="256" alt="logo"/>

DataFusion is a very fast, extensible query engine for building high-quality data-centric systems in [Rust](http://rustlang.org), using the [Apache Arrow](https://arrow.apache.org) in-memory format. [Python Bindings](https://github.com/apache/arrow-datafusion-python) are also available.

DataFusion offers SQL and Dataframe APIs, excellent [performance](https://benchmark.clickhouse.com/), built-in support for CSV, Parquet, JSON, and Avro, extensive customization, and a great community.

Here's are links to the documentation:

* [Rust API](https://arrow.apache.org/datafusion/user-guide/dataframe.html)
* [Python API](https://arrow.apache.org/datafusion-python/)

## Using DataFusion

The [example usage] section in the user guide and the [datafusion-examples] code in the crate contain information on using DataFusion.

## Notable projects that are build with DataFusion

DataFusion is great for building other data projects like SQL interfaces on query engines and time series platforms.  Here are some of the large projects that are built with DataFusion:

* [roapi](https://github.com/roapi/roapi): Easily create APIs for slow moving datasets.
* [dask-sql](https://github.com/dask-contrib/dask-sql): SQL interface for Dask query engine.
* [influxdb_iox](https://github.com/influxdata/influxdb_iox): real-time analytics datastore.

## Contributing to DataFusion

The [developer’s guide] contains information on how to contribute.

[example usage]: https://arrow.apache.org/datafusion/user-guide/example-usage.html
[datafusion-examples]: https://github.com/apache/arrow-datafusion/tree/master/datafusion-examples
[developer’s guide]: https://arrow.apache.org/datafusion/contributor-guide/index.html#developer-s-guide
