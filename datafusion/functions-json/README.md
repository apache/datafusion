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

# datafusion-functions-json

JSON scalar functions for [DataFusion](https://datafusion.apache.org/).

This crate provides JSON manipulation functions operating on JSON-encoded strings.
Based on the [datafusion-functions-json](https://github.com/datafusion-contrib/datafusion-functions-json)
community crate.

## Functions

| Function                        | Description                                                 |
| ------------------------------- | ----------------------------------------------------------- |
| `json_get_str(json, key1, ...)` | Extract a string value from a JSON string at the given path |

## Usage

This crate is **not** auto-wired into the `datafusion` core. Following the same
pattern as [`datafusion-functions-spark`](../spark), downstream users register
the functions explicitly to keep the core small and compile times fast.

### With a [`FunctionRegistry`]

```rust
use datafusion_functions_json;
// registry is any FunctionRegistry, e.g. SessionState
datafusion_functions_json::register_all(&mut registry)?;
```

### With a [`SessionStateBuilder`] (requires `core` feature)

Enable the optional `core` feature in `Cargo.toml`:

```toml
datafusion-functions-json = { version = "...", features = ["core"] }
```

Then use the [`SessionStateBuilderJson`] extension trait:

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion_functions_json::SessionStateBuilderJson;

// `with_json_features` should be called after `with_default_features`
// so it overwrites any colliding names.
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_json_features()
    .build();
```

[`functionregistry`]: https://docs.rs/datafusion-execution/latest/datafusion_execution/registry/trait.FunctionRegistry.html
[`sessionstatebuilder`]: https://docs.rs/datafusion/latest/datafusion/execution/struct.SessionStateBuilder.html
[`sessionstatebuilderjson`]: https://docs.rs/datafusion-functions-json/latest/datafusion_functions_json/trait.SessionStateBuilderJson.html
