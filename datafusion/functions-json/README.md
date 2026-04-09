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

These functions are registered automatically when the `json_expressions` feature
is enabled on the `datafusion` crate. They can also be registered manually:

```rust
use datafusion_functions_json;
// registry is a FunctionRegistry, e.g. SessionState
datafusion_functions_json::register_all(&mut registry)?;
```
