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

# Apache DataFusion Variant Function Library

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate contains functions for working with the [Parquet Variant] logical
type, such as `parse_json`, `variant_get`, `variant_object_keys`, and friends.
Variant values are stored as Arrow `StructArray`s carrying the `VariantType`
extension type and are produced/consumed by the [`parquet-variant`] family of
crates.

Most projects should use the [`datafusion`] crate directly, which re-exports
this module. If you are already using the [`datafusion`] crate, there is no
reason to use this crate directly in your project as well.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[parquet variant]: https://github.com/apache/parquet-format/blob/master/VariantEncoding.md
[`parquet-variant`]: https://crates.io/crates/parquet-variant
[`datafusion`]: https://crates.io/crates/datafusion
