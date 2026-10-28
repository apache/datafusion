// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`UnnestOptions`] for unnesting structured types

use crate::Column;

/// How [`UnnestOptions`] handles `NULL` and empty list values in the input column.
///
/// The variants enumerate the three observable behaviors so that callers do
/// not have to compose multiple boolean flags to express what they want.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Hash)]
pub enum NullHandling {
    /// Drop rows where the input list is `NULL` or empty. Matches the
    /// default behavior of systems such as DuckDB and ClickHouse.
    Drop,
    /// Preserve `NULL` input rows as a single output row containing `NULL`.
    /// Empty lists still produce zero output rows. This is the default and
    /// matches DataFusion's historical `preserve_nulls = true` behavior.
    #[default]
    Preserve,
    /// Like [`Self::Preserve`], and additionally treat an empty list
    /// identically to a `NULL` list, producing a single output row
    /// containing `NULL`.
    PreserveAndExpandEmpty,
}

/// Options for unnesting a column that contains a list type,
/// replicating values in the other, non nested rows.
///
/// Conceptually this operation is like joining each row with all the
/// values in the list column.
///
/// The behavior with `NULL` and empty input lists is controlled by
/// [`NullHandling`]. See its variants for full details.
///
/// # Examples
///
/// ## `Unnest(c1)`, null_handling: NullHandling::Drop
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
/// ## `Unnest(c1)`, null_handling: NullHandling::Preserve
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
/// ## `Unnest(c1)`, null_handling: NullHandling::PreserveAndExpandEmpty
/// ```text
///      ┌─────────┐ ┌─────┐                ┌─────────┐ ┌─────┐
///      │ {1, 2}  │ │  A  │   Unnest       │    1    │ │  A  │
///      ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
///      │  null   │ │  B  │                │    2    │ │  A  │
///      ├─────────┤ ├─────┤ ────────────▶  ├─────────┤ ├─────┤
///      │   {}    │ │  D  │                │  null   │ │  B  │
///      ├─────────┤ ├─────┤                ├─────────┤ ├─────┤
///      │   {3}   │ │  E  │                │  null   │ │  D  │
///      └─────────┘ └─────┘                ├─────────┤ ├─────┤
///        c1         c2                    │    3    │ │  E  │
///                                         └─────────┘ └─────┘
///                                             c1        c2
/// ```
///
/// `recursions` instruct how a column should be unnested (e.g unnesting a column multiple
/// time, with depth = 1 and depth = 2). Any unnested column not being mentioned inside this
/// options is inferred to be unnested with depth = 1
#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq)]
pub struct UnnestOptions {
    /// How to handle `NULL` and empty list values in the input column.
    /// Defaults to [`NullHandling::Preserve`].
    pub null_handling: NullHandling,
    /// If specific columns need to be unnested multiple times (e.g at different depth),
    /// declare them here. Any unnested columns not being mentioned inside this option
    /// will be unnested with depth = 1
    pub recursions: Vec<RecursionUnnestOption>,
}

/// Instruction on how to unnest a column (mostly with a list type)
/// such as how to name the output, and how many level it should be unnested
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct RecursionUnnestOption {
    pub input_column: Column,
    pub output_column: Column,
    pub depth: usize,
}

impl Default for UnnestOptions {
    fn default() -> Self {
        Self {
            null_handling: NullHandling::Preserve,
            recursions: vec![],
        }
    }
}

impl UnnestOptions {
    /// Create a new [`UnnestOptions`] with default values
    pub fn new() -> Self {
        Default::default()
    }

    /// Set the [`NullHandling`] mode used when unnesting `NULL` or empty
    /// input lists.
    pub fn with_null_handling(mut self, null_handling: NullHandling) -> Self {
        self.null_handling = null_handling;
        self
    }

    /// Backward-compatible setter that maps the previous boolean
    /// `preserve_nulls` flag onto [`NullHandling`].
    ///
    /// `true` maps to [`NullHandling::Preserve`]; `false` maps to
    /// [`NullHandling::Drop`]. To opt into the new empty-list-preserving
    /// mode, call [`Self::with_null_handling`] directly with
    /// [`NullHandling::PreserveAndExpandEmpty`].
    pub fn with_preserve_nulls(self, preserve_nulls: bool) -> Self {
        let null_handling = if preserve_nulls {
            NullHandling::Preserve
        } else {
            NullHandling::Drop
        };
        self.with_null_handling(null_handling)
    }

    /// Returns true if `NULL` input rows produce a single output row
    /// containing `NULL`.
    pub fn preserve_nulls(&self) -> bool {
        !matches!(self.null_handling, NullHandling::Drop)
    }

    /// Returns true if empty input lists should produce a single
    /// output row containing `NULL`.
    pub fn expand_empty_as_null(&self) -> bool {
        matches!(self.null_handling, NullHandling::PreserveAndExpandEmpty)
    }

    /// Set the recursions for the unnest operation
    pub fn with_recursions(mut self, recursion: RecursionUnnestOption) -> Self {
        self.recursions.push(recursion);
        self
    }
}
