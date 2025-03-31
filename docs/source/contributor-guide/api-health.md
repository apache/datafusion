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

DataFusion is used extensively as a library and has a large public API, thus it
is important that the API is well maintained. In general, we try to minimize
breaking API changes, but they are sometimes necessary.

When possible, rather than making breaking API changes, we prefer to deprecate
APIs to give users time to adjust to the changes.

## Upgrade Guides

When making changes that require DataFusion users to make changes to their code
as part of an upgrade please consider adding documentation to the version
specific [Upgrade Guide]

[upgrade guide]: ../library-user-guide/upgrading.md

## Breaking Changes

In general, a function is part of the public API if it appears on the [docs.rs page]

Breaking public API changes are those that _require_ users to change their code
for it to compile and execute, and are listed as "Major Changes" in the [SemVer
Compatibility Section of the cargo book]. Common examples of breaking changes:

- Adding new required parameters to a function (`foo(a: i32, b: i32)` -> `foo(a: i32, b: i32, c: i32)`)
- Removing a `pub` function
- Changing the return type of a function

When making breaking public API changes, please add the `api-change` label to
the PR so we can highlight the changes in the release notes.

[docs.rs page]: https://docs.rs/datafusion/latest/datafusion/index.html
[semver compatibility section of the cargo book]: https://doc.rust-lang.org/cargo/reference/semver.html#change-categories

## Deprecation Guidelines

When deprecating a method:

- Mark the API as deprecated using `#[deprecated]` and specify the exact DataFusion version in which it was deprecated
- Concisely describe the preferred API to help the user transition

The deprecated version is the next version which contains the deprecation. For
example, if the current version listed in [`Cargo.toml`] is `43.0.0` then the next
version will be `44.0.0`.

[`cargo.toml`]: https://github.com/apache/datafusion/blob/main/Cargo.toml

To mark the API as deprecated, use the `#[deprecated(since = "...", note = "...")]` attribute.

For example:

```rust
#[deprecated(since = "41.0.0", note = "Use new API instead")]
pub fn api_to_deprecated(a: usize, b: usize) {}
```

Deprecated methods will remain in the codebase for a period of 6 major versions or 6 months, whichever is longer, to provide users ample time to transition away from them.

Please refer to [DataFusion releases](https://crates.io/crates/datafusion/versions) to plan ahead API migration
