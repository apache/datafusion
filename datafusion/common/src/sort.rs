use crate::error::{_exec_err, _internal_err};
use crate::types::SortOrdering;
use crate::Result;
use arrow::array::{ArrayRef, DynComparator, UInt32Array};
use arrow::compute::{partial_sort, SortColumn, SortOptions};
use std::cmp::Ordering;
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, SortField};

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
    pub fn dyn_compartor(&self) -> Result<DynComparator> {
        let ordering = self
            .options
            .as_ref()
            .map(|opt| opt.ordering.clone())
            .unwrap_or_default();
        let options = self
            .options
            .as_ref()
            .and_then(|opt| opt.to_arrow().ok())
            .unwrap_or_default();
        ordering.dyn_comparator(self.values.clone(), options)
    }

    pub fn to_arrow(&self) -> Result<SortColumn> {
        let has_custom_sort = self
            .options
            .as_ref()
            .map(|opt| opt.ordering != SortOrdering::Default)
            .unwrap_or(false);
        match has_custom_sort {
            true => _internal_err!("Cannot create arrow SortColumn with custom sort"),
            false => Ok(SortColumn {
                values: self.values.clone(),
                options: self.options.as_ref().map(|o| o.to_arrow().unwrap()),
            }),
        }
    }
}

/// A lexicographical comparator that wraps given array data (columns) and can lexicographically compare data
/// at given two indices. The lifetime is the same at the data wrapped.
pub struct LexicographicalComparator {
    compare_items: Vec<DynComparator>,
}

impl LexicographicalComparator {
    /// lexicographically compare values at the wrapped columns with given indices.
    pub fn compare(&self, a_idx: usize, b_idx: usize) -> Ordering {
        for comparator in &self.compare_items {
            match comparator(a_idx, b_idx) {
                Ordering::Equal => continue,
                r => return r,
            }
        }
        Ordering::Equal
    }

    /// Create a new lex comparator that will wrap the given sort columns and give comparison
    /// results with two indices.
    pub fn new(compare_items: Vec<DynComparator>) -> LexicographicalComparator {
        LexicographicalComparator { compare_items }
    }
}

/// Sort elements lexicographically from a list of `ArrayRef` into an unsigned integer
/// (`UInt32Array`) of indices.
pub fn lexsort_to_indices(
    columns: &[AdvSortColumn],
    fetch: Option<usize>,
) -> Result<UInt32Array> {
    if columns.is_empty() {
        return _exec_err!("Sort requires at least one column");
    }

    let all_columns_default_ordering = columns
        .iter()
        .map(|c| c.to_arrow())
        .collect::<Result<Vec<_>>>();
    if let Ok(sort_columns) = all_columns_default_ordering {
        if is_multi_column_with_lists(&sort_columns) {
            // lex_sort_to_indices doesn't support List with more than one column
            // https://github.com/apache/arrow-rs/issues/5454
            lexsort_to_indices_multi_columns(sort_columns, fetch)?
        } else {
            arrow::compute::lexsort_to_indices(&sort_columns, fetch)?
        };
    }

    if columns.len() == 1 {
        // fallback to non-lexical sort
        let column = &columns[0];
        let options = column
            .options
            .as_ref()
            .expect("Otherwise fallback to arrow earlier");
        return options.ordering.sort_to_indices(
            &column.values,
            SortOptions::new(options.descending, options.nulls_first),
            fetch,
        );
    }

    let row_count = columns[0].values.len();
    if columns.iter().any(|item| item.values.len() != row_count) {
        return _exec_err!("lexical sort columns have different row counts");
    };

    let mut value_indices = (0..row_count).collect::<Vec<usize>>();
    let mut len = value_indices.len();

    if let Some(limit) = fetch {
        len = limit.min(len);
    }

    let compare_items = columns.iter()
        .map(|c| c.dyn_compartor())
        .collect::<Result<Vec<_>>>()?;

    let lexicographical_comparator = LexicographicalComparator::new(compare_items);
    // uint32 can be sorted unstably
    sort_unstable_by(&mut value_indices, len, |a, b| {
        lexicographical_comparator.compare(*a, *b)
    });

    Ok(UInt32Array::from_iter_values(
        value_indices.iter().take(len).map(|i| *i as u32),
    ))
}

#[inline]
fn is_multi_column_with_lists(sort_columns: &[SortColumn]) -> bool {
    sort_columns.iter().any(|c| {
        matches!(
            c.values.data_type(),
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        )
    })
}

pub(crate) fn lexsort_to_indices_multi_columns(
    sort_columns: Vec<SortColumn>,
    limit: Option<usize>,
) -> Result<UInt32Array> {
    let (fields, columns) = sort_columns.into_iter().fold(
        (vec![], vec![]),
        |(mut fields, mut columns), sort_column| {
            fields.push(SortField::new_with_options(
                sort_column.values.data_type().clone(),
                sort_column.options.unwrap_or_default(),
            ));
            columns.push(sort_column.values);
            (fields, columns)
        },
    );

    // TODO reuse converter and rows, refer to TopK.
    let converter = RowConverter::new(fields)?;
    let rows = converter.convert_columns(&columns)?;
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));

    let mut len = rows.num_rows();
    if let Some(limit) = limit {
        len = limit.min(len);
    }
    let indices =
        UInt32Array::from_iter_values(sort.iter().take(len).map(|(i, _)| *i as u32));

    Ok(indices)
}


/// we can only do this if the T is primitive
#[inline]
fn sort_unstable_by<T, F>(array: &mut [T], limit: usize, cmp: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    if array.len() == limit {
        array.sort_unstable_by(cmp);
    } else {
        partial_sort(array, limit, cmp);
    }
}
