// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! [`UnnestOptions`] for unnesting structured types

/// Options for unnesting a column that contains a list type,
/// replicating values in the other, non nested rows.
///
/// Conceptually this operation is like joining each row with all the
/// values in the list column.
///
/// If `preserve_nulls` is false, nulls and empty lists
/// from the input column are not carried through to the output. This
/// is the default behavior for other systems such as ClickHouse and
/// DuckDB
///
/// If `preserve_nulls` is true (the default), nulls from the input
/// column are carried through to the output.
///
/// # Examples
///
/// ## `Unnest(c1)`, preserve_nulls: false
/// ```text
///      ┌─────────┐ ┌─────┐                ┌─────────┐ ┌─────┐
///      │ {1, 2}  │ │  A  │   Unnest       │    1    │ │  A  │
///      ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
///      │  null   │ │  B  │                │    2    │ │  A  │
///      ├─────────┤ ├─────┤ ────────────▶  ├─────────┤ ├─────┤
///      │   {}    │ │  D  │                │    3    │ │  E  │
///      ├─────────┤ ├─────┤                └─────────┘ └─────┘
///      │   {3}   │ │  E  │                    c1        c2
///      └─────────┘ └─────┘
///        c1         c2
/// ```
///
/// ## `Unnest(c1)`, preserve_nulls: true
/// ```text
///      ┌─────────┐ ┌─────┐                ┌─────────┐ ┌─────┐
///      │ {1, 2}  │ │  A  │   Unnest       │    1    │ │  A  │
///      ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
///      │  null   │ │  B  │                │    2    │ │  A  │
///      ├─────────┤ ├─────┤ ────────────▶  ├─────────┤ ├─────┤
///      │   {}    │ │  D  │                │  null   │ │  B  │
///      ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
///      │   {3}   │ │  E  │                │    3    │ │  E  │
///      └─────────┘ └─────┘                └─────────┘ └─────┘
///        c1         c2                        c1        c2
/// ```
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct UnnestOptions {
    /// Should nulls in the input be preserved? Defaults to true
    pub preserve_nulls: bool,
}

impl Default for UnnestOptions {
    fn default() -> Self {
        Self {
            // default to true to maintain backwards compatible behavior
            preserve_nulls: true,
        }
    }
}

impl UnnestOptions {
    /// Create a new [`UnnestOptions`] with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the behavior with nulls in the input as described on
    /// [`Self`]
    pub fn with_preserve_nulls(mut self, preserve_nulls: bool) -> Self {
        self.preserve_nulls = preserve_nulls;
        self
    }
}
