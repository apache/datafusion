use arrow::compute::SortOptions as ArrowSortOptions;

/// Options for sorting.
/// 
/// This struct implements a builder pattern for creating `SortOptions`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SortOptions {
    pub descending: bool,
    pub nulls_first: bool,
}

impl SortOptions {
    /// Create a new `SortOptions` with default values (Ascending, Nulls Last).
    pub fn new() -> Self {
        Self {
            descending: false,
            nulls_first: false,
        }
    }

    /// Set sort order to descending.
    pub fn desc(mut self) -> Self {
        self.descending = true;
        self
    }

    /// Set sort order to ascending.
    pub fn asc(mut self) -> Self {
        self.descending = false;
        self
    }

    /// Set nulls to come first.
    pub fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    /// Set nulls to come last.
    pub fn nulls_last(mut self) -> Self {
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
