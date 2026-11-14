<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Protobuf Serialization / Deserialization

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate contains code to convert DataFusion plans to and from bytes using [Protocol Buffers],
which can be useful for sending plans over the network, for example when building a distributed
query engine.

See [API Docs] for details and examples.

## Migrating generate-series serialization helpers

Generate-series encoding now uses `LazyBatchGenerator::try_to_proto`, with the
`proto` feature enabled. Existing `PhysicalPlanNodeExt` helpers remain available
as deprecated compatibility methods:

- Replace `try_into_generate_series_physical_plan` with
  `datafusion_functions_table::generate_series::proto::try_from_proto`.
- Replace `try_from_lazy_memory_exec` with `ExecutionPlan::try_to_proto` when
  handling `Ok(None)` yourself, or `AsExecutionPlan::try_from_physical_plan` for
  serialization with an extension codec.
- Replace `generate_series_name_to_str` and `str_to_generate_series_name` with
  explicit matches on `GenerateSeriesName::{GsGenerateSeries, GsRange}`.

A custom generator returning `Some(node)` is reconstructed as a built-in series
generator. Its rows, batch boundaries, and schema after reset must match that
built-in generator. Encode the initial state, not a partially consumed cursor.
`Ok(None)` permits extension fallback; `Err` stops serialization. The existing
single-generator protobuf wire format is unchanged.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[protocol buffers]: https://protobuf.dev/
[api docs]: http://docs.rs/datafusion-proto/latest
