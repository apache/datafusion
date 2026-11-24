<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Protobuf Common Serialization / Deserialization

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate contains code to convert DataFusion primitive types to and from
bytes using [Protocol Buffers], which can be useful for sending data over the network.

See [API Docs] for details and examples.

Most projects should use the [`datafusion-proto`] crate directly, which re-exports
this module. If you are already using the [`datafusion-proto`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[protocol buffers]: https://protobuf.dev/
[`datafusion-proto`]: https://crates.io/crates/datafusion-proto
[api docs]: http://docs.rs/datafusion-proto/latest
