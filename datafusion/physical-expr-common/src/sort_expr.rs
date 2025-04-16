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

//! Sort expressions

use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::vec::IntoIter;

use crate::physical_expr::{fmt_sql, PhysicalExpr};

use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr_common::columnar_value::ColumnarValue;

/// Represents Sort operation for a column in a RecordBatch
///
/// Example:
/// ```
/// # use std::any::Any;
/// # use std::fmt::{Display, Formatter};
/// # use std::hash::Hasher;
/// # use std::sync::Arc;
/// # use arrow::array::RecordBatch;
/// # use datafusion_common::Result;
/// # use arrow::compute::SortOptions;
/// # use arrow::datatypes::{DataType, Schema};
/// # use datafusion_expr_common::columnar_value::ColumnarValue;
/// # use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
/// # use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;
/// # // this crate doesn't have a physical expression implementation
/// # // so make a really simple one
/// # #[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// # struct MyPhysicalExpr;
/// # impl PhysicalExpr for MyPhysicalExpr {
/// #  fn as_any(&self) -> &dyn Any {todo!() }
/// #  fn data_type(&self, input_schema: &Schema) -> Result<DataType> {todo!()}
/// #  fn nullable(&self, input_schema: &Schema) -> Result<bool> {todo!() }
/// #  fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {todo!() }
/// #  fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {todo!()}
/// #  fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> Result<Arc<dyn PhysicalExpr>> {todo!()}
/// # fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result { todo!() }
/// # }
/// # impl Display for MyPhysicalExpr {
/// #    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { write!(f, "a") }
/// # }
/// # fn col(name: &str) -> Arc<dyn PhysicalExpr> { Arc::new(MyPhysicalExpr) }
/// // Sort by a ASC
/// let options = SortOptions::default();
/// let sort_expr = PhysicalSortExpr::new(col("a"), options);
/// assert_eq!(sort_expr.to_string(), "a ASC");
///
/// // Sort by a DESC NULLS LAST
/// let sort_expr = PhysicalSortExpr::new_default(col("a"))
///   .desc()
///   .nulls_last();
/// assert_eq!(sort_expr.to_string(), "a DESC NULLS LAST");
/// ```
#[derive(Clone, Debug, Eq)]
pub struct PhysicalSortExpr {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted
    pub options: SortOptions,
}

impl PhysicalSortExpr {
    /// Create a new PhysicalSortExpr
    pub fn new(expr: Arc<dyn PhysicalExpr>, options: SortOptions) -> Self {
        Self { expr, options }
    }

    /// Create a new PhysicalSortExpr with default [`SortOptions`]
    pub fn new_default(expr: Arc<dyn PhysicalExpr>) -> Self {
        Self::new(expr, SortOptions::default())
    }

    /// Reverses the sort expression. For instance, `[a ASC NULLS LAST]` turns
    /// into `[a DESC NULLS FIRST]`. Such reversals are useful in planning, e.g.
    /// when constructing equivalent window expressions.
    pub fn reverse(&self) -> Self {
        let mut result = self.clone();
        result.options = !result.options;
        result
    }

    /// Set the sort sort options to ASC
    pub fn asc(mut self) -> Self {
        self.options.descending = false;
        self
    }

    /// Set the sort sort options to DESC
    pub fn desc(mut self) -> Self {
        self.options.descending = true;
        self
    }

    /// Set the sort sort options to NULLS FIRST
    pub fn nulls_first(mut self) -> Self {
        self.options.nulls_first = true;
        self
    }

    /// Set the sort sort options to NULLS LAST
    pub fn nulls_last(mut self) -> Self {
        self.options.nulls_first = false;
        self
    }

    /// Like [`PhysicalExpr::fmt_sql`] prints a [`PhysicalSortExpr`] in a SQL-like format.
    pub fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "{} {}",
            fmt_sql(self.expr.as_ref()),
            to_str(&self.options)
        )
    }

    /// Evaluates the sort expression into a `SortColumn` that can be passed
    /// into the arrow sort kernel.
    pub fn evaluate_to_sort_column(&self, batch: &RecordBatch) -> Result<SortColumn> {
        let array_to_sort = match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(batch.num_rows())?,
        };
        Ok(SortColumn {
            values: array_to_sort,
            options: Some(self.options),
        })
    }

    /// Checks whether this sort expression satisfies the given `requirement`.
    /// If sort options are unspecified in `requirement`, only expressions are
    /// compared for inequality.
    pub fn satisfy(
        &self,
        requirement: &PhysicalSortRequirement,
        schema: &Schema,
    ) -> bool {
        let opts = &requirement.options;
        self.expr.eq(&requirement.expr)
            && if self.expr.nullable(schema).unwrap_or(true) {
                opts.is_none_or(|opts| self.options == opts)
            } else {
                // If the column is not nullable, NULLS FIRST/LAST is not important.
                opts.is_none_or(|opts| self.options.descending == opts.descending)
            }
    }

    /// Checks whether this sort expression satisfies the given `sort_expr`.
    pub fn satisfy_expr(&self, sort_expr: &Self, schema: &Schema) -> bool {
        self.expr.eq(&sort_expr.expr)
            && if self.expr.nullable(schema).unwrap_or(true) {
                self.options == sort_expr.options
            } else {
                // If the column is not nullable, NULLS FIRST/LAST is not important.
                self.options.descending == sort_expr.options.descending
            }
    }
}

impl PartialEq for PhysicalSortExpr {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options && self.expr.eq(&other.expr)
    }
}

impl Hash for PhysicalSortExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.options.hash(state);
    }
}

impl Display for PhysicalSortExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{} {}", self.expr, to_str(&self.options))
    }
}

/// Represents sort requirement associated with a plan
///
/// If the requirement includes [`SortOptions`] then both the
/// expression *and* the sort options must match.
///
/// If the requirement does not include [`SortOptions`]) then only the
/// expressions must match.
///
/// # Examples
///
/// With sort options (`A`, `DESC NULLS FIRST`):
/// * `ORDER BY A DESC NULLS FIRST` matches
/// * `ORDER BY A ASC  NULLS FIRST` does not match (`ASC` vs `DESC`)
/// * `ORDER BY B DESC NULLS FIRST` does not match (different expr)
///
/// Without sort options (`A`, None):
/// * `ORDER BY A DESC NULLS FIRST` matches
/// * `ORDER BY A ASC  NULLS FIRST` matches (`ASC` and `NULL` options ignored)
/// * `ORDER BY B DESC NULLS FIRST` does not match  (different expr)
#[derive(Clone, Debug)]
pub struct PhysicalSortRequirement {
    /// Physical expression representing the column to sort
    pub expr: Arc<dyn PhysicalExpr>,
    /// Option to specify how the given column should be sorted.
    /// If unspecified, there are no constraints on sort options.
    pub options: Option<SortOptions>,
}

impl PartialEq for PhysicalSortRequirement {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options && self.expr.eq(&other.expr)
    }
}

impl Display for PhysicalSortRequirement {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let opts_string = self.options.as_ref().map_or("NA", to_str);
        write!(f, "{} {}", self.expr, opts_string)
    }
}

/// Writes a list of [`PhysicalSortRequirement`]s to a `std::fmt::Formatter`.
///
/// Example output: `[a + 1, b]`
pub fn format_physical_sort_requirement_list(
    exprs: &[PhysicalSortRequirement],
) -> impl Display + '_ {
    struct DisplayWrapper<'a>(&'a [PhysicalSortRequirement]);
    impl Display for DisplayWrapper<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            let mut iter = self.0.iter();
            write!(f, "[")?;
            if let Some(expr) = iter.next() {
                write!(f, "{}", expr)?;
            }
            for expr in iter {
                write!(f, ", {}", expr)?;
            }
            write!(f, "]")?;
            Ok(())
        }
    }
    DisplayWrapper(exprs)
}

impl PhysicalSortRequirement {
    /// Creates a new requirement.
    ///
    /// If `options` is `Some(..)`, creates an `exact` requirement,
    /// which must match both `options` and `expr`.
    ///
    /// If `options` is `None`, Creates a new `expr_only` requirement,
    /// which must match only `expr`.
    ///
    /// See [`PhysicalSortRequirement`] for examples.
    pub fn new(expr: Arc<dyn PhysicalExpr>, options: Option<SortOptions>) -> Self {
        Self { expr, options }
    }

    /// Returns whether this requirement is equal or more specific than `other`.
    pub fn compatible(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr)
            && other
                .options
                .is_none_or(|other_opts| self.options == Some(other_opts))
    }
}

/// Returns the SQL string representation of the given [SortOptions] object.
#[inline]
fn to_str(options: &SortOptions) -> &str {
    match (options.descending, options.nulls_first) {
        (true, true) => "DESC",
        (true, false) => "DESC NULLS LAST",
        (false, true) => "ASC",
        (false, false) => "ASC NULLS LAST",
    }
}

// Cross-conversion utilities between `PhysicalSortExpr` and `PhysicalSortRequirement`
impl From<PhysicalSortExpr> for PhysicalSortRequirement {
    fn from(value: PhysicalSortExpr) -> Self {
        Self::new(value.expr, Some(value.options))
    }
}

impl From<PhysicalSortRequirement> for PhysicalSortExpr {
    /// The default sort options `ASC, NULLS LAST` when the requirement does
    /// not specify sort options. This default is consistent with PostgreSQL.
    ///
    /// Reference: <https://www.postgresql.org/docs/current/queries-order.html>
    fn from(value: PhysicalSortRequirement) -> Self {
        let options = value.options.unwrap_or(SortOptions {
            descending: false,
            nulls_first: false,
        });
        Self::new(value.expr, options)
    }
}

///`LexOrdering` contains a `Vec<PhysicalSortExpr>`, which represents
/// a lexicographical ordering.
///
/// For example, `vec![a ASC, b DESC]` represents a lexicographical ordering
/// that first sorts by column `a` in ascending order, then by column `b` in
/// descending order.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LexOrdering {
    inner: Vec<PhysicalSortExpr>,
}

impl LexOrdering {
    /// Creates a new [`LexOrdering`] from the given vector of sort expressions.
    /// If the vector is empty, returns `None`.
    pub fn new(inner: impl IntoIterator<Item = PhysicalSortExpr>) -> Option<Self> {
        let inner = inner.into_iter().collect::<Vec<_>>();
        (!inner.is_empty()).then(|| Self { inner })
    }

    /// Appends an element to the back of the `LexOrdering`.
    pub fn push(&mut self, physical_sort_expr: PhysicalSortExpr) {
        self.inner.push(physical_sort_expr)
    }

    /// Add all elements from `iter` to the `LexOrdering`.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = PhysicalSortExpr>) {
        self.inner.extend(iter)
    }

    /// Returns the number of elements that can be stored in the `LexOrdering`
    /// without reallocating.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Takes ownership of the underlying vector of sort expressions and
    /// returns it.
    pub fn take(self) -> Vec<PhysicalSortExpr> {
        self.inner
    }

    /// Constructs a duplicate-free `LexOrdering` by filtering out duplicate
    /// entries that have same physical expression inside.
    ///
    /// For example, `vec![a Some(ASC), a Some(DESC)]` collapses to `vec![a
    /// Some(ASC)]`.
    pub fn collapse(mut self) -> Self {
        let mut orderings = Vec::<PhysicalSortExpr>::new();
        for element in self.inner {
            if !orderings.iter().any(|item| item.expr.eq(&element.expr)) {
                orderings.push(element);
            }
        }
        self.inner = orderings;
        self
    }

    /// Truncates the `LexOrdering`, keeping only the first `len` elements.
    /// Returns `true` if truncation made a change, `false` otherwise. Negative
    /// cases happen in two scenarios: (1) When `len` is greater than or equal
    /// to the number of expressions inside this `LexOrdering`, making truncation
    /// a no-op, or (2) when `len` is `0`, making truncation impossible.
    pub fn truncate(&mut self, len: usize) -> bool {
        if len == 0 || len >= self.inner.len() {
            return false;
        }
        self.inner.truncate(len);
        true
    }
}

impl PartialOrd for LexOrdering {
    /// There is a partial ordering among `LexOrdering` objects. For example, the
    /// ordering `[a ASC]` is coarser (less) than ordering `[a ASC, b ASC]`.
    /// If two orderings do not share a prefix, they are incomparable.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.iter()
            .zip(other.iter())
            .all(|(lhs, rhs)| lhs == rhs)
            .then(|| self.len().cmp(&other.len()))
    }
}

impl<const N: usize> From<[PhysicalSortExpr; N]> for LexOrdering {
    fn from(value: [PhysicalSortExpr; N]) -> Self {
        // TODO: Replace this with a condition on the generic parameter when
        //       Rust supports it.
        assert!(N > 0);
        Self {
            inner: value.to_vec(),
        }
    }
}

impl Deref for LexOrdering {
    type Target = [PhysicalSortExpr];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl DerefMut for LexOrdering {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut_slice()
    }
}

impl Display for LexOrdering {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut first = true;
        for sort_expr in &self.inner {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{}", sort_expr)?;
        }
        Ok(())
    }
}

impl IntoIterator for LexOrdering {
    type Item = PhysicalSortExpr;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a LexOrdering {
    type Item = &'a PhysicalSortExpr;
    type IntoIter = std::slice::Iter<'a, PhysicalSortExpr>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

///`LexRequirement` is an struct containing a `Vec<PhysicalSortRequirement>`, which
/// represents a lexicographical ordering requirement.
#[derive(Debug, Clone, PartialEq)]
pub struct LexRequirement {
    inner: Vec<PhysicalSortRequirement>,
}

impl LexRequirement {
    /// Creates a new [`LexRequirement`] from the given vector of sort expressions.
    /// If the vector is empty, returns `None`.
    pub fn new(inner: impl IntoIterator<Item = PhysicalSortRequirement>) -> Option<Self> {
        let inner = inner.into_iter().collect::<Vec<_>>();
        (!inner.is_empty()).then(|| Self { inner })
    }

    /// Appends an element to the back of the `LexRequirement`.
    pub fn push(&mut self, requirement: PhysicalSortRequirement) {
        self.inner.push(requirement)
    }

    /// Add all elements from `iter` to the `LexRequirement`.
    pub fn extend(&mut self, iter: impl IntoIterator<Item = PhysicalSortRequirement>) {
        self.inner.extend(iter)
    }

    /// Returns the number of elements that can be stored in the `LexRequirement`
    /// without reallocating.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Takes ownership of the underlying vector of sort requirements and
    /// returns it.
    pub fn take(self) -> Vec<PhysicalSortRequirement> {
        self.inner
    }

    /// Constructs a duplicate-free `LexRequirement` by filtering out duplicate
    /// entries that have same physical expression inside.
    ///
    /// For example, `vec![a Some(ASC), a Some(DESC)]` collapses to `vec![a
    /// Some(ASC)]`.
    pub fn collapse(mut self) -> Self {
        let mut reqs = Vec::<PhysicalSortRequirement>::new();
        for element in self.inner {
            if !reqs.iter().any(|item| item.expr.eq(&element.expr)) {
                reqs.push(element);
            }
        }
        self.inner = reqs;
        self
    }
}

impl<const N: usize> From<[PhysicalSortRequirement; N]> for LexRequirement {
    fn from(value: [PhysicalSortRequirement; N]) -> Self {
        // TODO: Replace this with a condition on the generic parameter when
        //       Rust supports it.
        assert!(N > 0);
        Self {
            inner: value.to_vec(),
        }
    }
}

impl Deref for LexRequirement {
    type Target = [PhysicalSortRequirement];

    fn deref(&self) -> &Self::Target {
        self.inner.as_slice()
    }
}

impl DerefMut for LexRequirement {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut_slice()
    }
}

impl IntoIterator for LexRequirement {
    type Item = PhysicalSortRequirement;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.into_iter()
    }
}

impl<'a> IntoIterator for &'a LexRequirement {
    type Item = &'a PhysicalSortRequirement;
    type IntoIter = std::slice::Iter<'a, PhysicalSortRequirement>;

    fn into_iter(self) -> Self::IntoIter {
        self.inner.iter()
    }
}

// Cross-conversion utilities between `LexOrdering` and `LexRequirement`
impl From<LexOrdering> for LexRequirement {
    fn from(value: LexOrdering) -> Self {
        // Can construct directly as `value` is non-degenerate:
        Self {
            inner: value.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<LexRequirement> for LexOrdering {
    fn from(value: LexRequirement) -> Self {
        // Can construct directly as `value` is non-degenerate:
        Self {
            inner: value.into_iter().map(Into::into).collect(),
        }
    }
}

/// Represents a plan's input ordering requirements. Vector elements represent
/// alternative ordering requirements in the order of preference.
#[derive(Debug, Clone, PartialEq)]
pub enum OrderingRequirements {
    /// The operator is not able to work without one of these requirements.
    Hard(Vec<LexRequirement>),
    /// The operator can benefit from these input orderings when available,
    /// but can still work in the absence of any input ordering.
    Soft(Vec<LexRequirement>),
}

impl OrderingRequirements {
    /// Creates a new instance from the given alternatives. If an empty list of
    /// alternatives are given, returns `None`.
    pub fn new(alternatives: Vec<LexRequirement>, soft: bool) -> Option<Self> {
        (!alternatives.is_empty()).then(|| {
            if soft {
                Self::Soft(alternatives)
            } else {
                Self::Hard(alternatives)
            }
        })
    }

    /// Creates a new instance with a single hard requirement.
    pub fn new_single(requirement: LexRequirement) -> Self {
        Self::Hard(vec![requirement])
    }

    /// Creates a new instance with a single soft requirement.
    pub fn new_single_soft(requirement: LexRequirement) -> Self {
        Self::Soft(vec![requirement])
    }

    /// Adds an alternative requirement to the list of alternatives.
    pub fn add_alternative(&mut self, requirement: LexRequirement) {
        match self {
            Self::Hard(alts) | Self::Soft(alts) => alts.push(requirement),
        }
    }

    /// Returns the first (i.e. most preferred) `LexRequirement` among
    /// alternative requirements.
    pub fn into_single(self) -> LexRequirement {
        match self {
            Self::Hard(mut alts) | Self::Soft(mut alts) => alts.swap_remove(0),
        }
    }

    /// Returns a reference to the first (i.e. most preferred) `LexRequirement`
    /// among alternative requirements.
    pub fn first(&self) -> &LexRequirement {
        match self {
            Self::Hard(alts) | Self::Soft(alts) => &alts[0],
        }
    }
}

impl From<LexRequirement> for OrderingRequirements {
    fn from(requirement: LexRequirement) -> Self {
        Self::new_single(requirement)
    }
}

impl From<LexOrdering> for OrderingRequirements {
    fn from(ordering: LexOrdering) -> Self {
        Self::new_single(ordering.into())
    }
}

impl Deref for OrderingRequirements {
    type Target = [LexRequirement];

    fn deref(&self) -> &Self::Target {
        match &self {
            Self::Hard(alts) | Self::Soft(alts) => alts.as_slice(),
        }
    }
}

impl DerefMut for OrderingRequirements {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Hard(alts) | Self::Soft(alts) => alts.as_mut_slice(),
        }
    }
}
