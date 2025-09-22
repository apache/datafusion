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

# Apache DataFusion Foreign Function Interface

This crate contains code to allow interoperability of [Apache DataFusion] with
functions from other libraries and/or DataFusion versions using a stable
interface.

One of the limitations of the Rust programming language is that there is no
stable [Rust ABI] (Application Binary Interface). If a library is compiled with
one version of the Rust compiler and you attempt to use that library with a
program compiled by a different Rust compiler, there is no guarantee that you
can access the data structures. In order to share code between libraries loaded
at runtime, you need to use Rust's [FFI] (Foreign Function Interface (FFI)).

The purpose of this crate is to define interfaces between DataFusion libraries
that will remain stable across different versions of DataFusion. This allows
users to write libraries that can interface between each other at runtime rather
than require compiling all of the code into a single executable.

In general, it is recommended to run the same version of DataFusion by both the
producer and consumer of the data and functions shared across the [FFI], but
this is not strictly required.

See [API Docs] for details and examples.

## Use Cases

Two use cases have been identified for this crate, but they are not intended to
be all inclusive.

1. [`datafusion-python`] which will use the FFI to provide external services such
   as a `TableProvider` without needing to re-export the entire `datafusion-python`
   code base. With `datafusion-ffi` these packages do not need `datafusion-python`
   as a dependency at all.
2. Users may want to create a modular interface that allows runtime loading of
   libraries. For example, you may wish to design a program that only uses the
   built in table sources, but also allows for extension from the community led
   [datafusion-contrib] repositories. You could enable module loading so that
   users could at runtime load a library to access additional data sources.
   Alternatively, you could use this approach so that customers could interface
   with their own proprietary data sources.

## Limitations

One limitation of the approach in this crate is that it is designed specifically
to work across Rust libraries. In general, you can use Rust's [FFI] to
operate across different programming languages, but that is not the design
intent of this crate. Instead, we are using external crates that provide
stable interfaces that closely mirror the Rust native approach. To learn more
about this approach see the [abi_stable] and [async-ffi] crates.

If you have a library in another language that you wish to interface to
DataFusion the recommendation is to create a Rust wrapper crate to interface
with your library and then to connect it to DataFusion using this crate.
Alternatively, you could use [bindgen] to interface directly to the [FFI] provided
by this crate, but that is currently not supported.

## FFI Boundary

We expect this crate to be used by both sides of the FFI Boundary. This should
provide ergonamic ways to both produce and consume structs and functions across
this layer.

For example, if you have a library that provides a custom `TableProvider`, you
can expose it by using `FFI_TableProvider::new()`. When you need to consume a
`FFI_TableProvider`, you can access it by converting using
`ForeignTableProvider::from()` which will create a struct that implements
`TableProvider`.

There is a complete end to end demonstration in the
[examples](https://github.com/apache/datafusion/tree/main/datafusion-examples/examples/ffi).

## Asynchronous Calls

Some of the functions with this crate require asynchronous operation. These
will perform similar to their pure rust counterparts by using the [async-ffi]
crate. In general, any call to an asynchronous function in this interface will
not block the rest of the program's execution.

## Struct Layout

In this crate we have a variety of structs which closely mimic the behavior of
their internal counterparts. To see detailed notes about how to use them, see
the example in `FFI_TableProvider`.

[apache datafusion]: https://datafusion.apache.org/
[api docs]: http://docs.rs/datafusion-ffi/latest
[rust abi]: https://doc.rust-lang.org/reference/abi.html
[ffi]: https://doc.rust-lang.org/nomicon/ffi.html
[abi_stable]: https://crates.io/crates/abi_stable
[async-ffi]: https://crates.io/crates/async-ffi
[bindgen]: https://crates.io/crates/bindgen
[`datafusion-python`]: https://datafusion.apache.org/python/
[datafusion-contrib]: https://github.com/datafusion-contrib
