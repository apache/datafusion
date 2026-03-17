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

### What is the public API and what is a breaking API change?

In general, an item is part of the public API if it appears on the [docs.rs page].

Breaking public API changes are those that _require_ users to change their code
for it to compile and execute, and are listed as "Major Changes" in the [SemVer
Compatibility Section of the Cargo Book]. Common examples of breaking changes include:

- Adding new required parameters to a function (`foo(a: i32, b: i32)` -> `foo(a: i32, b: i32, c: i32)`)
- Removing a `pub` function
- Changing the return type of a function
- Adding a new function to a `trait` without a default implementation

Examples of non-breaking changes include:

- Marking a function as deprecated (`#[deprecated]`)
- Adding a new function to a `trait` with a default implementation

### When to make breaking API changes?

When possible, we prefer to avoid making breaking API changes. One common way to
avoid such changes is to deprecate the old API, as described in the [Deprecation
Guidelines](#deprecation-guidelines) section below.

If you do want to propose a breaking API change, we must weigh the benefits of the
change with the cost (impact on downstream users). It is often frustrating for
downstream users to change their applications, and it is even more so if they
do not gain improved capabilities.

Examples of good reasons for making a breaking API change include:

- The change allows new use cases that were not possible before
- The change significantly enables improved performance

Examples of potentially weak reasons for making breaking API changes include:

- The change is an internal refactor to make DataFusion more consistent
- The change is to remove an API that is not widely used but has not been marked as deprecated

### What to do when making breaking API changes?

When making breaking public API changes, please:

1. Add the `api-change` label to the PR so we can highlight the changes in the release notes.
2. Consider adding documentation to the version-specific [Upgrade Guide] if the required changes are non-trivial.

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
