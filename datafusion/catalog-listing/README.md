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

# DataFusion catalog-listing

[DataFusion][df] is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

This crate is a submodule of DataFusion with [ListingTable], an implementation
of [TableProvider] based on files in a directory (either locally or on remote
object storage such as S3).

[df]: https://crates.io/crates/datafusion
[listingtable]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingTable.html
[tableprovider]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html
