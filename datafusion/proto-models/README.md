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

# Apache DataFusion Protobuf Models

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate contains the [prost]-generated Rust types for DataFusion's logical
and physical plan protobuf schemas. It is intentionally kept narrow: it has no
DataFusion dependencies beyond [`datafusion-proto-common`] and exposes only the
generated structs (and optional [pbjson]/[serde] support).

This crate is consumed by [`datafusion-proto`] and may also be depended on
directly by other DataFusion crates that need to refer to the proto schema
types without pulling in the full [`datafusion-proto`] surface.

Most projects should use the [`datafusion-proto`] crate directly, which
re-exports this module. If you are already using the [`datafusion-proto`]
crate, there is no reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[prost]: https://docs.rs/prost/latest/prost/
[pbjson]: https://docs.rs/pbjson/latest/pbjson/
[serde]: https://serde.rs/
[`datafusion-proto`]: https://crates.io/crates/datafusion-proto
[`datafusion-proto-common`]: https://crates.io/crates/datafusion-proto-common
