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

# Apache DataFusion Substrait

[Apache DataFusion] is an extensible query execution framework, written in Rust, that uses [Apache Arrow] as its in-memory format.

This crate is a submodule of DataFusion that provides a [Substrait] producer and consumer for DataFusion
plans. See [API Docs] for details and examples.

## Table function scans

When a logical `TableScan` originates from built-in table functions such as `generate_series` or `range`,
the Substrait producer populates `ReadRel.advanced_extension` with an [`Any`](https://protobuf.dev/reference/protobuf/google.protobuf/#any) payload
whose `type_url` is `type.googleapis.com/datafusion.substrait.TableFunctionReadRel`. The embedded
`TableFunctionReadRel` message stores the function name and the evaluated argument literals (after defaults
and type coercions). Integer table functions, for example, always emit a three-element argument list
representing `start`, `end`, and `step`, even when the user omitted optional parameters. If a consumer does
not recognize the extension, it can still fall back to the accompanying `NamedTable` reference.

[apache arrow]: https://arrow.apache.org/
[apache datafusion]: https://datafusion.apache.org/
[substrait]: https://substrait.io
[api docs]: https://docs.rs/datafusion-substrait/latest
