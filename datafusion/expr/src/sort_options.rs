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

use arrow::compute::SortOptions as ArrowSortOptions;

/// Options for sorting.
///
/// This struct implements a builder pattern for creating `SortOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Hash, Default)]
pub struct SortOptions {
    pub descending: bool,
    pub nulls_first: bool,
}

impl SortOptions {
    /// Create a new `SortOptions` struct with default values.
    ///
    /// The default values are:
    /// - `descending`: false (Ascending)
    /// - `nulls_first`: false (Nulls Last)
    ///
    /// # Example
    /// ```
    /// use datafusion_expr::SortOptions;
    /// let options = SortOptions::new();
    /// assert_eq!(options.descending, false);
    /// assert_eq!(options.nulls_first, false);
    /// ```
    #[inline]
    pub const fn new() -> Self {
        Self {
            descending: false,
            nulls_first: false,
        }
    }

    /// Set sort order to descending.
    #[inline]
    pub const fn desc(mut self) -> Self {
        self.descending = true;
        self
    }

    /// Set sort order to ascending.
    #[inline]
    pub const fn asc(mut self) -> Self {
        self.descending = false;
        self
    }

    /// Set nulls to come first.
    #[inline]
    pub const fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    /// Set nulls to come last.
    #[inline]
    pub const fn nulls_last(mut self) -> Self {
        self.nulls_first = false;
        self
    }
}

impl From<SortOptions> for ArrowSortOptions {
    fn from(options: SortOptions) -> Self {
        ArrowSortOptions {
            descending: options.descending,
            nulls_first: options.nulls_first,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sort_options_default() {
        let options = SortOptions::new();
        assert_eq!(options.descending, false);
        assert_eq!(options.nulls_first, false);
    }

    #[test]
    fn test_sort_options_desc() {
        let options = SortOptions::new().desc();
        assert_eq!(options.descending, true);
        assert_eq!(options.nulls_first, false);
    }

    #[test]
    fn test_sort_options_asc() {
        let options = SortOptions::new().desc().asc();
        assert_eq!(options.descending, false);
        assert_eq!(options.nulls_first, false);
    }

    #[test]
    fn test_sort_options_nulls_first() {
        let options = SortOptions::new().nulls_first();
        assert_eq!(options.descending, false);
        assert_eq!(options.nulls_first, true);
    }

    #[test]
    fn test_sort_options_nulls_last() {
        let options = SortOptions::new().nulls_first().nulls_last();
        assert_eq!(options.descending, false);
        assert_eq!(options.nulls_first, false);
    }
}
