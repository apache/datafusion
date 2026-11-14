<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Protobuf Models

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate contains the [prost]-generated Rust types for DataFusion's logical
and physical plan protobuf schemas, plus the `From` / `TryFrom` conversions
between those types and the [`datafusion-common`] types they mirror. The
conversions live here because their DataFusion side sits _below_ this crate in
the dependency graph and so cannot host the impls itself — the same arrangement
[`datafusion-proto-common`] uses for [`ScalarValue`] and [`Statistics`].

It is otherwise intentionally kept narrow: its only DataFusion dependencies are
[`datafusion-common`] and [`datafusion-proto-common`], and apart from those
conversions it exposes only the generated structs (and optional
[pbjson]/[serde] support). In particular it does not depend on
`datafusion-expr` or on any of the execution crates.

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
[`datafusion-common`]: https://crates.io/crates/datafusion-common
[`datafusion-proto`]: https://crates.io/crates/datafusion-proto
[`datafusion-proto-common`]: https://crates.io/crates/datafusion-proto-common
[`scalarvalue`]: https://docs.rs/datafusion-common/latest/datafusion_common/scalar/enum.ScalarValue.html
[`statistics`]: https://docs.rs/datafusion-common/latest/datafusion_common/stats/struct.Statistics.html
