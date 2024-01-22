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

<!-- Note this file is included in the crates.io page as well https://crates.io/crates/datafusion-cli -->

# DataFusion Command-line Interface

[DataFusion](https://arrow.apache.org/datafusion/) is an extensible query execution framework, written in Rust, that uses Apache Arrow as its in-memory format.

The DataFusion CLI is a command line utility that runs SQL queries using the DataFusion engine.

# Frequently Asked Questions

## Where can I find more information?

Answer: See the [`datafusion-cli` documentation](https://arrow.apache.org/datafusion/user-guide/cli.html) for further information.

## How do I make my IDE work with `datafusion-cli`?

Answer: "open" the `datafusion/datafusion-cli` project as its own top level
project in my IDE (rather than opening `datafusion`)

The reason `datafusion-cli` is not listed as part of the workspace in the main
[`datafusion Cargo.toml`] file is that `datafusion-cli` is a binary and has a
checked in `Cargo.lock` file to ensure reproducible builds.

However, the `datafusion` and sub crates are intended for use as libraries and
thus do not have a `Cargo.lock` file checked in.

[`datafusion cargo.toml`]: https://github.com/apache/arrow-datafusion/blob/main/Cargo.toml
