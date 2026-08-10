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

<!--
  Note the main crates.io landing page https://crates.io/crates/datafusion
  uses the workspace README.md file, not this file
-->

# Apache DataFusion Dylib

This crate is an optional dependency of the main `datafusion` crate that bundles `datafusion-core` into a shared library for reuse across binaries and faster link times during development.

## Rationale
Some projects that use `datafusion` may have many tests, examples, and other binaries. With static linking every such binary will link its own copy of `datafusion`. Static linking is often desirable for distribution simplicity and performance, but (especially in `dev` builds) may result in disk use bloat and slow build times due to repeated linking.

This crate allows users to link `datafusion` as a shared library by enabling `datafusion/dylib` crate feature.

## Implementation
The implementation mirrors [dynamic linking in Bevy engine](https://bevy.org/learn/quick-start/getting-started/setup/#dynamic-linking) which has provided this option for years for similar reasons. It is also described in the [blog post](https://robert.kra.hn/posts/2022-09-09-speeding-up-incremental-rust-compilation-with-dylibs/) by Robert Krahn.

The mechanism is **opt-in**, and imposes **zero cost** on consumers who don't enable it.

**How it Works:**
- The `datafusion-core` crate is the main guts of implementation
- The `datafusion-dylib` crate with `crate-type = ["dylib"]` contains a single `use datafusion_core` statement that forces the shared library to contain the `datafusion-core` symbol table.
- The `datafusion` facade crate re-exports everything from `datafusion-core` and forwards every existing feature 1:1.
- When `dylib` feature is enabled, the facade's pulls in `datafusion-dylib` via `#[cfg(feature = "dylib")] use datafusion_dylib as _;`
- Linker sees duplicated symbols and automatically prefers those from a shared library.

Dependency graph:
```
               ┌────────────────┐
               │   datafusion   │ ← owns the `dylib` feature
               │    (facade)    │
               └───────┬────────┘
                       │
          pub use datafusion_core::*
                       │
            ┌──────────┴──────────┐
            │                     │
            │  (always)           │  (only when `dylib` feature is on)
            │                     │
            ▼                     ▼
  ┌─────────────────┐   ┌──────────────────┐
  │ datafusion-core │   │ datafusion-dylib │
  │                 │   │                  │
  │  all real code  │   │  use datafusion  │
  │                 │◄──┤       as _;      │
  │                 │   │                  │
  └─────────────────┘   └──────────────────┘
```

When `dylib` feature is off (default), `datafusion-dylib` is not compiled and adds zero overhead.

## Downsides & Limitations
- All feature flags need to be propagated 1:1 from the facade into the core crate adding slight maintenance burden

## Alternatives
- Always build DF as `dylib` - impacts how people distribute their client apps, can impact performance
- Specify `crate-type = ["rlib", "dylib"]` with `-C prefer-dynamic` compile option - currently this will always build both types of libraries, adding overhead for users that don't need dynamic lib
- Let users create dynamic wrappers in their workspaces - this was painful to do and breaks down with transitive dependencies on DF
