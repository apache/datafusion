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

# `datafusion-ffi`: Apache DataFusion Foreign Function Interface

This crate contains code to allow interoperability of Apache [DataFusion]
with functions from other languages using a stable interface.

See [API Docs] for details and examples.

We expect this crate may be used by both sides of the FFI. This allows users
to create modules that can interoperate with the necessity of using the same
version of DataFusion. The driving use case has been the `datafusion-python`
repository, but many other use cases may exist. We envision at least two
use cases.

1. `datafusion-python` which will use the FFI to provide external services such
as a `TableProvider` without needing to re-export the entire `datafusion-python`
code base. With `datafusion-ffi` these packages do not need `datafusion-python`
as a dependency at all.
2. Users may want to create a modular interface that allows runtime loading of
libraries.

## Struct Layout

In this crate we have a variety of structs which closely mimic the behavior of
their internal counterparts. In the following example, we will refer to the
`TableProvider`, but the same pattern exists for other structs.

Each of the exposted structs in this crate is provided with two variants that
are to be used based on which side of the interface you are viewing it from.
The structs that begin with `Exported` are designed to be used by the side that
provides the functions. The receiver of these structs begins with `Foreign`.

For example, we have a struct `FFI_TableProvider` to give access to the
`TableProvider` functions like `table_type()` and `scan()`. If we write a
library that wishes to expose it's `TableProvider`, then we can access the
private data that contains the Arc reference to the `TableProvider` via
`ExportedTableProvider`. This data is local to the library.

If we have a program that accesses a `TableProvider` provided via FFI, then it
will use `ForeignTableProvider`. When using `ForeignTableProvider` we **must**
not attempt to access the `private_data` field in `FFI_TableProvider`. If a
user is testing locally, you may be able to successfully access this field, but
it will only work if you are building against the exact same version of
`DataFusion` for both libraries **and** the same compiler. It is not guaranteed
to work in general.

It is worth noting that which library is the `local` and which is `foreign`
depends on which interface we are considering. For example, suppose we have a
Python library called `my_provider` that exposes a `TableProvider` called
`MyProvider` via `FFI_TableProvider`. Within the library `my_provider` we can
access the `private_data` via `ExportedTableProvider`. We connect this to
`datafusion-python`, where we access it as a `ForeignTableProvider`. Now when
we call `scan()` on this interface, we have to pass it a `FFI_SessionConfig`.
The `SessionConfig` is local to `datafusion-python` and **not** `my_provider`.
It is important to be careful when expanding these functions to be certain which
side of the interface each object refers to.

[datafusion]: https://datafusion.apache.org
[api docs]: http://docs.rs/datafusion-ffi/latest
