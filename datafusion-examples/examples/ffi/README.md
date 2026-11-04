<!--
SPDX-License-Identifier: Apache-2.0
SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements
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
