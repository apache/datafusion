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

The library user guide explains how to use the DataFusion library as a
dependency in your Rust project and customize its behavior using its extension APIs.

Please check out the [user guide] for getting started using
DataFusion's SQL and DataFrame APIs, or the [contributor guide]
for details on how to contribute to DataFusion.

If you haven't reviewed the [architecture section in the docs][docs], it's a
useful place to get the lay of the land before starting down a specific path.

DataFusion is designed to be extensible at all points, including

- [x] User Defined Functions (UDFs)
- [x] User Defined Aggregate Functions (UDAFs)
- [x] User Defined Table Source (`TableProvider`) for tables
- [x] User Defined `Optimizer` passes (plan rewrites)
- [x] User Defined `LogicalPlan` nodes
- [x] User Defined `ExecutionPlan` nodes

[user guide]: ../user-guide/example-usage.md
[contributor guide]: ../contributor-guide/index.md
[docs]: https://docs.rs/datafusion/latest/datafusion/#architecture
