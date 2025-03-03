use crate::error::_internal_err;
use crate::types::SortOrdering;
use crate::Result;
use arrow::array::{ArrayRef, DynComparator, UInt32Array};
use arrow::compute::SortOptions;
use arrow::error::ArrowError;

/// TODO
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq)]
pub struct AdvSortOptions {
    /// Specifies the ordering that is used for sorting. This enables implementing user-defined
    /// sorting.
    pub ordering: SortOrdering,
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl AdvSortOptions {
    /// Creates a new [AdvSortOptions].
    pub fn new(ordering: SortOrdering, descending: bool, nulls_first: bool) -> Self {
        Self {
            ordering,
            descending,
            nulls_first,
        }
    }

    /// Creates a new [AdvSortOptions] with a default ordering from the arrow [SortOption].
    pub fn with_default_ordering(options: SortOptions) -> Self {
        Self::new(
            SortOrdering::Default,
            options.descending,
            options.nulls_first,
        )
    }

    /// Tries to create an [SortOptions] with the same `descending` and `nulls_first`.
    ///
    /// # Errors
    ///
    /// This method will return an error if a custom [SortOrdering] is used.
    pub fn to_arrow(&self) -> Result<SortOptions> {
        match self.ordering {
            SortOrdering::Default => Ok(SortOptions {
                descending: self.descending,
                nulls_first: self.nulls_first,
            }),
            SortOrdering::Custom(_) => {
                _internal_err!("Cannot create arrow SortOptions with custom ordering")
            }
        }
    }

    /// Returns a [AdvSortOptions] with a flipped descending.
    ///
    /// This does not change the order of nulls.
    pub fn with_reversed_order(mut self) -> Self {
        self.descending = !self.descending;
        self
    }

    /// Returns a [AdvSortOptions] with the given `value` for `descending`.
    pub fn with_descending(mut self, value: bool) -> Self {
        self.descending = value;
        self
    }

    /// Returns a [AdvSortOptions] with the given `value` for `nulls_first`.
    pub fn with_nulls_first(mut self, value: bool) -> Self {
        self.nulls_first = value;
        self
    }
}

/// TODO
#[derive(Clone, Debug)]
pub struct AdvSortColumn {
    pub values: ArrayRef,
    pub options: Option<AdvSortOptions>,
}

impl AdvSortColumn {
    pub fn dyn_compartor(&self) -> DynComparator {
        todo!()
    }

    pub fn to_arrow(&self) -> Result<arrow::compute::SortColumn> {
        let has_custom_sort = self
            .options
            .as_ref()
            .map(|opt| opt.ordering != SortOrdering::Default)
            .unwrap_or(false);
        match has_custom_sort {
            true => _internal_err!("Cannot create arrow SortColumn with custom sort"),
            false => Ok(arrow::compute::SortColumn {
                values: self.values.clone(),
                options: self.options.as_ref().map(|o| o.to_arrow().unwrap()),
            }),
        }
    }
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
pub fn lexsort_to_indices(
    columns: &[AdvSortColumn],
    limit: Option<usize>,
) -> std::result::Result<UInt32Array, ArrowError> {
    if columns.is_empty() {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    }

    let all_columns_default_ordering = columns
        .iter()
        .map(|c| c.to_arrow())
        .collect::<Result<Vec<_>>>();
    if let Ok(columns) = all_columns_default_ordering {
        return arrow::compute::lexsort_to_indices(&columns, limit);
    }

    todo!("Custom sorting not yet implemented.")
    //
    // if columns.len() == 1 && can_sort_to_indices(columns[0].values.data_type()) {
    //     // fallback to non-lexical sort
    //     let column = &columns[0];
    //     return sort_to_indices(&column.values, column.options, limit);
    // }
    //
    // let row_count = columns[0].values.len();
    // if columns.iter().any(|item| item.values.len() != row_count) {
    //     return Err(ArrowError::ComputeError(
    //         "lexical sort columns have different row counts".to_string(),
    //     ));
    // };
    //
    // let mut value_indices = (0..row_count).collect::<Vec<usize>>();
    // let mut len = value_indices.len();
    //
    // if let Some(limit) = limit {
    //     len = limit.min(len);
    // }
    //
    // let lexicographical_comparator = LexicographicalComparator::try_new(columns)?;
    // // uint32 can be sorted unstably
    // sort_unstable_by(&mut value_indices, len, |a, b| {
    //     lexicographical_comparator.compare(*a, *b)
    // });
    //
    // Ok(UInt32Array::from_iter_values(
    //     value_indices.iter().take(len).map(|i| *i as u32),
    // ))
}
