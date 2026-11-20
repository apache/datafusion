<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Physical Expression Adapter

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate provides utilities for adapting physical expressions to different schemas in DataFusion.

It handles schema differences in file scans by rewriting expressions to match the physical schema,
including type casting, missing columns, and partition values.

For detailed documentation, see the [`PhysicalExprAdapter`] trait documentation.

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[`datafusion`]: https://crates.io/crates/datafusion
[`physicalexpradapter`]: https://docs.rs/datafusion/latest/datafusion/physical_expr_adapter/trait.PhysicalExprAdapter.html
