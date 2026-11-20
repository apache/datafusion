<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Core Physical Expressions

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that provides shared APIs for implementing
physical expressions such as [`PhysicalExpr`] and [`PhysicalSortExpr`].

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion
[`physicalexpr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/trait.PhysicalExpr.html
[`physicalsortexpr`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/struct.PhysicalSortExpr.html
