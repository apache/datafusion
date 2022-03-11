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

# Roadmap

A quarterly roadmap will be published to give the DataFusion community visibility into the priorities of the projects contributors. This roadmap is not binding.

## 2022 Q1

### DataFusion Core

- Publish official Arrow2 branch
- Implementation of memory manager (i.e. to enable spilling to disk as needed)

### Benchmarking

- Inclusion in Db-Benchmark with all quries covered
- All TPCH queries covered

### Performance Improvements

- Predicate evaluation
- Improve multi-column comparisons (that can't be vectorized at the moment)
- Null constant support

### New Features

- Read JSON as table
- Simplify DDL with DataFusion-Cli
- Add Decimal128 data type and the attendant features such as Arrow Kernel and UDF support
- Add new experimental e-graph based optimizer

### Ballista

- Begin work on design documents and plan / priorities for development

### Extensions ([datafusion-contrib](https://github.com/datafusion-contrib]))

- Stable S3 support
- Begin design discussions and prototyping of a stream provider

## Beyond 2022 Q1

There is no clear timeline for the below, but community members have expressed interest in working on these topics.

### DataFusion Core

- Custom SQL support
- Split DataFusion into multiple crates
- Push based query execution and code generation

### Ballista

- Evolve architecture so that it can be deployed in a multi-tenant cloud native environment
- Ensure Ballista is scalable, elastic, and stable for production usage
- Develop distributed ML capabilities
