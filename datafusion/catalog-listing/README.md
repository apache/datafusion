<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
-->

# Apache DataFusion Catalog Listing

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion with [ListingTable], an implementation
of [TableProvider] based on files in a directory (either locally or on remote
object storage such as S3).

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[listingtable]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[tableprovider]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
[`datafusion`]: https://crates.io/crates/datafusion
