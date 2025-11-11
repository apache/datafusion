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

## Library Marker ID

When reviewing the code, many of the structs in this crate contain a call to
a `library_maker_id`. The purpose of this call is to determine if a library is
accessing *local* code through the FFI structs. Consider this example: you have
a `primary` program that exposes functions to create a schema provider. You
have a `secondary` library that exposes a function to create a catalog provider
and the `secondary` library uses the schema provider of the `primary` program.
From the point of view of the `secondary` library, the schema provider is
foreign code.

Now when we register the `secondary` library with the `primary` program as a
catalog provider and we make calls to get a schema, the `secondary` library
will return a FFI wrapped schema provider back to the `primary` program. In
this case that schema provider is actually local code to the `primary` program
except that it is wrapped in the FFI code!

We work around this by the `library_marker_id` calls. What this does is it
creates a global variable within each library and returns a `u64` address
of that library. This is guaranteed to be unique for every library that contains
FFI code. By comparing these `u64` addresses we can determine if a FFI struct
is local or foreign.

In our example of the schema provider, if you were to make a call in your
primary program to get the schema provider, it would reach out to the foreign
catalog provider and send back a `FFI_SchemaProvider` object. By then
comparing the `library_marker_id` of this object to the `primary` program, we
determine it is local code. This means it is safe to access the underlying
private data.

## Testing Coverage

Since this library contains a large amount of `unsafe` code, it is important
to ensure proper test coverage. To generate a coverage report, you can use
[tarpaulin] as follows. It is necessary to use the `integration-tests` feature
in order to properly generate coverage.

```shell
cargo tarpaulin --package datafusion-ffi --tests --features integration-tests --out Html
```

While it is not normally required to check Rust code for memory leaks, this
crate does manual memory management due to the FFI boundary. You can test for
leaks using the generated unit tests. How you run these checks differs depending
on your OS.

### Linux

On Linux, you can install `cargo-valgrind`

```shell
cargo valgrind test --features integration-tests -p datafusion-ffi
```

### MacOS

You can find the generated binaries for your unit tests by running `cargo test`.

```shell
cargo test --features integration-tests -p datafusion-ffi --no-run
```

This should generate output that shows the path to the test binaries. Then
you can run commands such as the following. The specific paths of the tests
will vary.

```shell
leaks --atExit -- target/debug/deps/datafusion_ffi-e77a2604a85a8afe
leaks --atExit -- target/debug/deps/ffi_integration-e91b7127a59b71a7
# ...
```

[apache datafusion]: https://datafusion.apache.org/
[api docs]: http://docs.rs/datafusion-ffi/latest
[rust abi]: https://doc.rust-lang.org/reference/abi.html
[ffi]: https://doc.rust-lang.org/nomicon/ffi.html
[abi_stable]: https://crates.io/crates/abi_stable
[async-ffi]: https://crates.io/crates/async-ffi
[bindgen]: https://crates.io/crates/bindgen
[`datafusion-python`]: https://datafusion.apache.org/python/
[datafusion-contrib]: https://github.com/datafusion-contrib
[tarpaulin]: https://crates.io/crates/cargo-tarpaulin
