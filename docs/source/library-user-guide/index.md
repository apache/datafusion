<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Introduction

The library user guide explains how to use the DataFusion library as a
dependency in your Rust project and customize its behavior using its extension APIs.

Please check out the [user guide] for getting started using
DataFusion's SQL and DataFrame APIs, or the [contributor guide]
for details on how to contribute to DataFusion.

If you haven't reviewed the [architecture section in the docs][docs], it's a
useful place to get the lay of the land before starting down a specific path.

For guidance on running DataFusion with SQL from untrusted users, see
[Securing DataFusion](securing-datafusion.md).

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
