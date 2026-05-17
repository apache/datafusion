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

use crate::Column;

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
///
/// `recursions` instruct how a column should be unnested (e.g unnesting a column multiple
/// time, with depth = 1 and depth = 2). Any unnested column not being mentioned inside this
/// options is inferred to be unnested with depth = 1
///
/// If `position` is set, an additional column is appended to the output containing the
/// position of each element within its source list. The index base is selected by the
/// SQL spelling used: `WITH ORDINALITY` (Postgres, SQL standard) is 1-indexed,
/// `WITH OFFSET` (BigQuery) is 0-indexed.
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct UnnestOptions {
    /// Should nulls in the input be preserved? Defaults to true
    pub preserve_nulls: bool,
    /// If specific columns need to be unnested multiple times (e.g at different depth),
    /// declare them here. Any unnested columns not being mentioned inside this option
    /// will be unnested with depth = 1
    pub recursions: Vec<RecursionUnnestOption>,
    /// If set, append a position column to the output (per-list element index).
    /// Defaults to `None` (no position column emitted).
    pub position: Option<PositionColumn>,
}

/// Instruction on how to unnest a column (mostly with a list type)
/// such as how to name the output, and how many level it should be unnested
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct RecursionUnnestOption {
    pub input_column: Column,
    pub output_column: Column,
    pub depth: usize,
}

/// The 0/1 index base for the position column emitted by `UNNEST`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexBase {
    /// 0-indexed (BigQuery `WITH OFFSET`, Snowflake `FLATTEN.INDEX`, Spark `posexplode`).
    Zero,
    /// 1-indexed (Postgres / SQL standard `WITH ORDINALITY`, Trino/Presto).
    One,
}

/// Specification for the extra position column produced by `UNNEST WITH ORDINALITY`
/// (1-indexed) or `UNNEST WITH OFFSET` (0-indexed).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct PositionColumn {
    /// Output column name (e.g. `"ordinality"`, `"offset"`, or a user alias).
    pub name: String,
    /// Whether the column is 0- or 1-indexed.
    pub base: IndexBase,
}

impl PositionColumn {
    pub fn new(name: impl Into<String>, base: IndexBase) -> Self {
        Self {
            name: name.into(),
            base,
        }
    }
}

impl Default for UnnestOptions {
    fn default() -> Self {
        Self {
            // default to true to maintain backwards compatible behavior
            preserve_nulls: true,
            recursions: vec![],
            position: None,
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

    /// Set the recursions for the unnest operation
    pub fn with_recursions(mut self, recursion: RecursionUnnestOption) -> Self {
        self.recursions.push(recursion);
        self
    }

    /// Request a position column on the output (see [`PositionColumn`]).
    pub fn with_position(mut self, position: PositionColumn) -> Self {
        self.position = Some(position);
        self
    }
}
