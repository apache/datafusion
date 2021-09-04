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

# Ballista: Distributed Compute with Apache Arrow and DataFusion

Ballista is a distributed compute platform primarily implemented in Rust, and powered by Apache Arrow and
DataFusion. It is built on an architecture that allows other programming languages (such as Python, C++, and
Java) to be supported as first-class citizens without paying a penalty for serialization costs.

The foundational technologies in Ballista are:

- [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
- [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient
  data transfer between processes.
- [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans.
- [Docker](https://www.docker.com/) for packaging up executors along with user-defined code.

Ballista can be deployed as a standalone cluster and also supports [Kubernetes](https://kubernetes.io/). In either
case, the scheduler can be configured to use [etcd](https://etcd.io/) as a backing store to (eventually) provide
redundancy in the case of a scheduler failing.

# Getting Started

Refer to the core [Ballista crate README](rust/client/README.md) for the Getting Started guide.

## Distributed Scheduler Overview

Ballista uses the DataFusion query execution framework to create a physical plan and then transforms it into a
distributed physical plan by breaking the query down into stages whenever the partitioning scheme changes.

Specifically, any `RepartitionExec` operator is replaced with an `UnresolvedShuffleExec` and the child operator
of the repartition operator is wrapped in a `ShuffleWriterExec` operator and scheduled for execution.

Each executor polls the scheduler for the next task to run. Tasks are currently always `ShuffleWriterExec` operators
and each task represents one _input_ partition that will be executed. The resulting batches are repartitioned
according to the shuffle partitioning scheme and each _output_ partition is streamed to disk in Arrow IPC format.

The scheduler will replace `UnresolvedShuffleExec` operators with `ShuffleReaderExec` operators once all shuffle
tasks have completed. The `ShuffleReaderExec` operator connects to other executors as required using the Flight
interface, and streams the shuffle IPC files.

# How does this compare to Apache Spark?

Ballista implements a similar design to Apache Spark, but there are some key differences.

- The choice of Rust as the main execution language means that memory usage is deterministic and avoids the overhead of
  GC pauses.
- Ballista is designed from the ground up to use columnar data, enabling a number of efficiencies such as vectorized
  processing (SIMD and GPU) and efficient compression. Although Spark does have some columnar support, it is still
  largely row-based today.
- The combination of Rust and Arrow provides excellent memory efficiency and memory usage can be 5x - 10x lower than
  Apache Spark in some cases, which means that more processing can fit on a single node, reducing the overhead of
  distributed compute.
- The use of Apache Arrow as the memory model and network protocol means that data can be exchanged between executors
  in any programming language with minimal serialization overhead.
