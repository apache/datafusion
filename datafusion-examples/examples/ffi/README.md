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

# Example FFI Usage

The purpose of these crates is to provide an example of how one can use the
DataFusion Foreign Function Interface (FFI). See [API Docs] for detailed
usage.

This example is broken into three crates.

- `ffi_module_interface` is a common library to be shared by both the module
  to be loaded and the program that will load it. It defines how the module
  is to be structured.
- `ffi_example_table_provider` creates a library to exposes the module.
- `ffi_module_loader` is an example program that loads the module, gets data
  from it, and displays this data to the user.

## Building and running

In order for the program to run successfully, the module to be loaded must be
built first. This example expects both the module and the program to be
built using the same build mode (debug or release).

```shell
cd ffi_example_table_provider
cargo build
cd ../ffi_module_loader
cargo run
```

[api docs]: http://docs.rs/datafusion-ffi/latest
