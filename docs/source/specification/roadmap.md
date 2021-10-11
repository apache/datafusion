\<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Roadmap

This document describes high level goals of the DataFusion and
Ballista development community. It is not meant to restrict, but
rather help newcomers understand the broader context of where other
parts of the community is headed, and inspire additional
contributions.

DataFusion and Ballista, are part of the [Apache
Arrow](https://arrow.apache.org/) project and governed by the Apache
Software Foundation governance model. Thus it is an entirely driven by
volunteer contributions, and we welcome contributions for items not on
this roadmap. However, before submitting a large PR, we strongly
suggest you read the [before
starting](https://arrow.apache.org/docs/developers/contributing.html#before-starting)
recommendations to minimize code review surprises.

# DataFusion

## Vision

DataFusion's goal is to become _the de facto query engine_ of choice
for new analytic applications, by leveraging the unique features of
Rust and [Apache Arrow](https://arrow.apache.org/) to provide:

1. Best-in-class query performance for a single node
2. A feature-complete declarative query interface via (most of) PostgreSQL
3. A feature-rich procedural interface for creating and running execution plans
4. High performance, erogonomic extensibility points at at every layer

## SQL Language

- Complete support list on [status](https://github.com/apache/arrow-datafusion/blob/master/README.md#status)
- Timestamp Arithmetic [#194](https://github.com/apache/arrow-datafusion/issues/194)
- SQL Parser extension point
- Support for nested structures (fields, lists, structs)

## Query Optimizer

- Additional constant folding / partial evaluation [#1070](https://github.com/apache/arrow-datafusion/issues/1070)
- More sophisticated cost based optimizer for join ordering

## Runtime / Infrastructure

- Better support for reading data from remote filesystems (e.g. S3) without caching it locally
- [arrow2](https://github.com/apache/arrow-datafusion/milestone/3)

## Resource Management

- Finer grain control and limit of runtime memory and CPU usage

## Python Interface

TBD

## DataFusion CLI (`datafusion-cli`)

TODO: add relevant parts of https://github.com/apache/arrow-datafusion/issues/1096

## Ballista

# Vision

TBD
