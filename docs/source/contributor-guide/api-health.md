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

# API health policy

DataFusion is used extensively as a library in other applications and has a
large public API. We try to keep the API well maintained and minimize breaking
changes to avoid issues for downstream users.

## Breaking API Changes

### What is the public Rust API and what is a breaking API change?

An item is part of the public Rust API if it appears on the [docs.rs page].

Breaking changes _require_ users to modify their code for it to compile and
run, and are listed as "Major Changes" in the [SemVer Compatibility Section of
the Cargo Book]. Common examples include:

- Adding new required parameters to a function (`foo(a: i32, b: i32)` -> `foo(a: i32, b: i32, c: i32)`)
- Removing a `pub` function
- Changing the return type of a function
- Adding a new function to a `trait` without a default implementation

Examples of non-breaking changes include:

- Marking a function as deprecated (`#[deprecated]`)
- Adding a new function to a `trait` with a default implementation

### What is the public SQL API and what is a breaking SQL change?

DataFusion is also used as a SQL engine, so changes to SQL semantics (the
results returned for a given query) are a form of breaking change. Even with
no Rust API change, altering the behavior of an existing SQL construct can
silently break downstream applications, dashboards, and tests.

We apply the same caution to SQL semantics changes as to Rust API changes:
the benefit must be weighed against the cost of breaking downstream users.

### When to make breaking API changes?

When possible, we prefer to avoid making breaking API changes. One common way to
avoid such changes is to deprecate the old API, as described in the [Deprecation
Guidelines](#deprecation-guidelines) section below.

If you do want to propose a breaking API change, we must weigh the benefits of the
change with the cost (impact on downstream users). It is often frustrating for
downstream users to change their applications, and it is even more so if they
do not gain improved capabilities.

Examples of good reasons for a breaking API or SQL change:

- It enables new use cases that were not possible before
- It significantly improves performance
- The previous behavior is clearly wrong (e.g. produces incorrect results)

Examples of potentially weak reasons:

- An internal refactor to make DataFusion more consistent
- Removing an API that is not widely used but has not been marked as deprecated
- Slightly improving compatibility with another database (for example,
  PostgreSQL or DuckDB)

### What to do when making breaking API changes?

When making breaking Rust API changes, please:

1. Add the `api-change` label so the change is highlighted in the release notes.
2. Document non-trivial changes in the version-specific [Upgrade Guide].

For breaking SQL changes, also describe the previous and new behavior in the PR
description, ideally including example queries and results where appropriate.
This makes review easier and helps downstream users discover the affected
semantics.

[docs.rs page]: https://docs.rs/datafusion/latest/datafusion/index.html
[semver compatibility section of the cargo book]: https://doc.rust-lang.org/cargo/reference/semver.html#change-categories

## Upgrade Guides

When a change requires DataFusion users to modify their code as part of an
upgrade, please consider documenting it in the version-specific [Upgrade Guide].

[upgrade guide]: ../library-user-guide/upgrading/index.rst

## Deprecation Guidelines

When deprecating a method:

- Mark the API as deprecated using `#[deprecated]` and specify the exact DataFusion version in which it was deprecated
- Concisely describe the preferred API to help the user transition

The deprecated version is the next version that introduces the deprecation. For
example, if the current version listed in [`Cargo.toml`] is `43.0.0`, then the next
version will be `44.0.0`.

[`cargo.toml`]: https://github.com/apache/datafusion/blob/main/Cargo.toml

To mark the API as deprecated, use the `#[deprecated(since = "...", note = "...")]` attribute.

For example:

```rust
#[deprecated(since = "41.0.0", note = "Use new API instead")]
pub fn api_to_deprecated(a: usize, b: usize) {}
```

Deprecated methods will remain in the codebase for a period of 6 major versions or 6 months, whichever is longer, to provide users ample time to transition away from them.

Please refer to [DataFusion releases](https://crates.io/crates/datafusion/versions) to plan API migration ahead of time.
