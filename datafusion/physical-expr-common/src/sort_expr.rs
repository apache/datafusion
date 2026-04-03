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

use crate::physical_expr::{PhysicalExpr, fmt_sql};

use arrow::compute::kernels::sort::{SortColumn, SortOptions};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use datafusion_common::{HashSet, Result};
use datafusion_expr_common::columnar_value::ColumnarValue;
use indexmap::IndexSet;
/// Represents Sort operation for a column in a RecordBatch
///
/// Example:
/// ```
/// # use std::any::Any;
/// # use std::collections::HashMap;
/// # use std::fmt::{Display, Formatter};
/// # use std::hash::Hasher;
/// # use std::sync::Arc;
/// # use arrow::array::RecordBatch;
/// # use datafusion_common::Result;
/// # use arrow::compute::SortOptions;
/// # use arrow::datatypes::{DataType, Field, FieldRef, Schema};
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
/// #  fn return_field(&self, input_schema: &Schema) -> Result<FieldRef> { unimplemented!() }
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
    /// compared for inequality. See [`options_compatible`] for details on
    /// how sort options compare with one another.
    pub fn satisfy(
        &self,
        requirement: &PhysicalSortRequirement,
        schema: &Schema,
    ) -> bool {
        self.expr.eq(&requirement.expr)
            && requirement.options.is_none_or(|opts| {
                options_compatible(
                    &self.options,
                    &opts,
                    self.expr.nullable(schema).unwrap_or(true),
                )
            })
    }

    /// Checks whether this sort expression satisfies the given `sort_expr`.
    /// See [`options_compatible`] for details on how sort options compare with
    /// one another.
    pub fn satisfy_expr(&self, sort_expr: &Self, schema: &Schema) -> bool {
        self.expr.eq(&sort_expr.expr)
            && options_compatible(
                &self.options,
                &sort_expr.options,
                self.expr.nullable(schema).unwrap_or(true),
            )
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

/// Returns whether the given two [`SortOptions`] are compatible. Here,
/// compatibility means that they are either exactly equal, or they differ only
/// in whether NULL values come in first/last, which is immaterial because the
/// column in question is not nullable (specified by the `nullable` parameter).
pub fn options_compatible(
    options_lhs: &SortOptions,
    options_rhs: &SortOptions,
    nullable: bool,
) -> bool {
    if nullable {
        options_lhs == options_rhs
    } else {
        // If the column is not nullable, NULLS FIRST/LAST is not important.
        options_lhs.descending == options_rhs.descending
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
                write!(f, "{expr}")?;
            }
            for expr in iter {
                write!(f, ", {expr}")?;
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

/// Returns the SQL string representation of the given [`SortOptions`] object.
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
        let options = value
            .options
            .unwrap_or_else(|| SortOptions::new(false, false));
        Self::new(value.expr, options)
    }
}

/// This object represents a lexicographical ordering and contains a vector
/// of `PhysicalSortExpr` objects.
///
/// For example, a `vec![a ASC, b DESC]` represents a lexicographical ordering
/// that first sorts by column `a` in ascending order, then by column `b` in
/// descending order.
///
/// # Invariants
///
/// The following always hold true for a `LexOrdering`:
///
/// 1. It is non-degenerate, meaning it contains at least one element.
/// 2. It is duplicate-free, meaning it does not contain multiple entries for
///    the same column.
#[derive(Clone, Debug)]
pub struct LexOrdering {
    /// Vector of sort expressions representing the lexicographical ordering.
    exprs: Vec<PhysicalSortExpr>,
    /// Set of expressions in the lexicographical ordering, used to ensure
    /// that the ordering is duplicate-free. Note that the elements in this
    /// set are the same underlying physical expressions as in `exprs`.
    set: IndexSet<Arc<dyn PhysicalExpr>>,
}

impl LexOrdering {
    /// Creates a new [`LexOrdering`] from the given vector of sort expressions.
    /// If the vector is empty, returns `None`.
    pub fn new(exprs: impl IntoIterator<Item = PhysicalSortExpr>) -> Option<Self> {
        let exprs = exprs.into_iter();
        let mut candidate = Self {
            // not valid yet; valid publicly-returned instance must be non-empty
            exprs: Vec::new(),
            set: IndexSet::new(),
        };
        for expr in exprs {
            candidate.push(expr);
        }
        if candidate.exprs.is_empty() {
            None
        } else {
            Some(candidate)
        }
    }

    /// Appends an element to the back of the `LexOrdering`.
    pub fn push(&mut self, sort_expr: PhysicalSortExpr) {
        if self.set.insert(Arc::clone(&sort_expr.expr)) {
            self.exprs.push(sort_expr);
        }
    }

    /// Add all elements from `iter` to the `LexOrdering`.
    pub fn extend(&mut self, sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>) {
        for sort_expr in sort_exprs {
            self.push(sort_expr);
        }
    }

    /// Returns the leading `PhysicalSortExpr` of the `LexOrdering`. Note that
    /// this function does not return an `Option`, as a `LexOrdering` is always
    /// non-degenerate (i.e. it contains at least one element).
    pub fn first(&self) -> &PhysicalSortExpr {
        // Can safely `unwrap` because `LexOrdering` is non-degenerate:
        self.exprs.first().unwrap()
    }

    /// Returns the number of elements that can be stored in the `LexOrdering`
    /// without reallocating.
    pub fn capacity(&self) -> usize {
        self.exprs.capacity()
    }

    /// Truncates the `LexOrdering`, keeping only the first `len` elements.
    /// Returns `true` if truncation made a change, `false` otherwise. Negative
    /// cases happen in two scenarios: (1) When `len` is greater than or equal
    /// to the number of expressions inside this `LexOrdering`, making truncation
    /// a no-op, or (2) when `len` is `0`, making truncation impossible.
    pub fn truncate(&mut self, len: usize) -> bool {
        if len == 0 || len >= self.exprs.len() {
            return false;
        }
        for PhysicalSortExpr { expr, .. } in self.exprs[len..].iter() {
            self.set.swap_remove(expr);
        }
        self.exprs.truncate(len);
        true
    }

    /// Check if reversing this ordering would satisfy another ordering requirement.
    ///
    /// This supports **prefix matching**: if this ordering is `[A DESC, B ASC]`
    /// and `other` is `[A ASC]`, reversing this gives `[A ASC, B DESC]`, which
    /// satisfies `other` since `[A ASC]` is a prefix.
    ///
    /// # Arguments
    /// * `other` - The ordering requirement to check against
    ///
    /// # Returns
    /// `true` if reversing this ordering would satisfy `other`
    ///
    /// # Example
    /// ```text
    /// self:  [number DESC, letter ASC]
    /// other: [number ASC]
    /// After reversing self: [number ASC, letter DESC]  ✓ Prefix match!
    /// ```
    pub fn is_reverse(&self, other: &LexOrdering) -> bool {
        let self_exprs = self.as_ref();
        let other_exprs = other.as_ref();

        if other_exprs.len() > self_exprs.len() {
            return false;
        }

        other_exprs.iter().zip(self_exprs.iter()).all(|(req, cur)| {
            req.expr.eq(&cur.expr) && is_reversed_sort_options(&req.options, &cur.options)
        })
    }

    /// Returns the sort options for the given expression if one is defined in this `LexOrdering`.
    pub fn get_sort_options(&self, expr: &dyn PhysicalExpr) -> Option<SortOptions> {
        for e in self {
            if e.expr.as_ref().dyn_eq(expr) {
                return Some(e.options);
            }
        }

        None
    }
}

/// Check if two SortOptions represent reversed orderings.
///
/// Returns `true` if both `descending` and `nulls_first` are opposite.
///
/// # Example
/// ```
/// use arrow::compute::SortOptions;
/// # use datafusion_physical_expr_common::sort_expr::is_reversed_sort_options;
///
/// let asc_nulls_last = SortOptions {
///     descending: false,
///     nulls_first: false,
/// };
/// let desc_nulls_first = SortOptions {
///     descending: true,
///     nulls_first: true,
/// };
///
/// assert!(is_reversed_sort_options(&asc_nulls_last, &desc_nulls_first));
/// assert!(is_reversed_sort_options(&desc_nulls_first, &asc_nulls_last));
/// ```
pub fn is_reversed_sort_options(lhs: &SortOptions, rhs: &SortOptions) -> bool {
    lhs.descending != rhs.descending && lhs.nulls_first != rhs.nulls_first
}

impl PartialEq for LexOrdering {
    fn eq(&self, other: &Self) -> bool {
        let Self {
            exprs,
            set: _, // derived from `exprs`
        } = self;
        // PartialEq must be consistent with PartialOrd
        exprs == &other.exprs
    }
}
impl Eq for LexOrdering {}
impl PartialOrd for LexOrdering {
    /// There is a partial ordering among `LexOrdering` objects. For example, the
    /// ordering `[a ASC]` is coarser (less) than ordering `[a ASC, b ASC]`.
    /// If two orderings do not share a prefix, they are incomparable.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // PartialEq must be consistent with PartialOrd
        self.exprs
            .iter()
            .zip(other.exprs.iter())
            .all(|(lhs, rhs)| lhs == rhs)
            .then(|| self.len().cmp(&other.len()))
    }
}

impl<const N: usize> From<[PhysicalSortExpr; N]> for LexOrdering {
    fn from(value: [PhysicalSortExpr; N]) -> Self {
        // TODO: Replace this assertion with a condition on the generic parameter
        //       when Rust supports it.
        assert!(N > 0);
        Self::new(value)
            .expect("A LexOrdering from non-empty array must be non-degenerate")
    }
}

impl Deref for LexOrdering {
    type Target = [PhysicalSortExpr];

    fn deref(&self) -> &Self::Target {
        self.exprs.as_slice()
    }
}

impl Display for LexOrdering {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut first = true;
        for sort_expr in &self.exprs {
            if first {
                first = false;
            } else {
                write!(f, ", ")?;
            }
            write!(f, "{sort_expr}")?;
        }
        Ok(())
    }
}

impl IntoIterator for LexOrdering {
    type Item = PhysicalSortExpr;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.into_iter()
    }
}

impl<'a> IntoIterator for &'a LexOrdering {
    type Item = &'a PhysicalSortExpr;
    type IntoIter = std::slice::Iter<'a, PhysicalSortExpr>;

    fn into_iter(self) -> Self::IntoIter {
        self.exprs.iter()
    }
}

impl From<LexOrdering> for Vec<PhysicalSortExpr> {
    fn from(ordering: LexOrdering) -> Self {
        ordering.exprs
    }
}

/// This object represents a lexicographical ordering requirement and contains
/// a vector of `PhysicalSortRequirement` objects.
///
/// For example, a `vec![a Some(ASC), b None]` represents a lexicographical
/// requirement that firsts imposes an ordering by column `a` in ascending
/// order, then by column `b` in *any* (ascending or descending) order. The
/// ordering is non-degenerate, meaning it contains at least one element, and
/// it is duplicate-free, meaning it does not contain multiple entries for the
/// same column.
///
/// Note that a `LexRequirement` need not enforce the uniqueness of its sort
/// expressions after construction like a `LexOrdering` does, because it provides
/// no mutation methods. If such methods become necessary, we will need to
/// enforce uniqueness like the latter object.
#[derive(Debug, Clone, PartialEq)]
pub struct LexRequirement {
    reqs: Vec<PhysicalSortRequirement>,
}

impl LexRequirement {
    /// Creates a new [`LexRequirement`] from the given vector of sort expressions.
    /// If the vector is empty, returns `None`.
    pub fn new(reqs: impl IntoIterator<Item = PhysicalSortRequirement>) -> Option<Self> {
        let (non_empty, requirements) = Self::construct(reqs);
        non_empty.then_some(requirements)
    }

    /// Returns the leading `PhysicalSortRequirement` of the `LexRequirement`.
    /// Note that this function does not return an `Option`, as a `LexRequirement`
    /// is always non-degenerate (i.e. it contains at least one element).
    pub fn first(&self) -> &PhysicalSortRequirement {
        // Can safely `unwrap` because `LexRequirement` is non-degenerate:
        self.reqs.first().unwrap()
    }

    /// Constructs a new `LexRequirement` from the given sort requirements w/o
    /// enforcing non-degeneracy. This function is used internally and is not
    /// meant (or safe) for external use.
    fn construct(
        reqs: impl IntoIterator<Item = PhysicalSortRequirement>,
    ) -> (bool, Self) {
        let mut set = HashSet::new();
        let reqs = reqs
            .into_iter()
            .filter_map(|r| set.insert(Arc::clone(&r.expr)).then_some(r))
            .collect();
        (!set.is_empty(), Self { reqs })
    }
}

impl<const N: usize> From<[PhysicalSortRequirement; N]> for LexRequirement {
    fn from(value: [PhysicalSortRequirement; N]) -> Self {
        // TODO: Replace this assertion with a condition on the generic parameter
        //       when Rust supports it.
        assert!(N > 0);
        let (non_empty, requirement) = Self::construct(value);
        debug_assert!(non_empty);
        requirement
    }
}

impl Deref for LexRequirement {
    type Target = [PhysicalSortRequirement];

    fn deref(&self) -> &Self::Target {
        self.reqs.as_slice()
    }
}

impl IntoIterator for LexRequirement {
    type Item = PhysicalSortRequirement;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.reqs.into_iter()
    }
}

impl<'a> IntoIterator for &'a LexRequirement {
    type Item = &'a PhysicalSortRequirement;
    type IntoIter = std::slice::Iter<'a, PhysicalSortRequirement>;

    fn into_iter(self) -> Self::IntoIter {
        self.reqs.iter()
    }
}

impl From<LexRequirement> for Vec<PhysicalSortRequirement> {
    fn from(requirement: LexRequirement) -> Self {
        requirement.reqs
    }
}

// Cross-conversion utilities between `LexOrdering` and `LexRequirement`
impl From<LexOrdering> for LexRequirement {
    fn from(value: LexOrdering) -> Self {
        // Can construct directly as `value` is non-degenerate:
        let (non_empty, requirements) =
            Self::construct(value.into_iter().map(Into::into));
        debug_assert!(non_empty);
        requirements
    }
}

impl From<LexRequirement> for LexOrdering {
    fn from(value: LexRequirement) -> Self {
        // Can construct directly as `value` is non-degenerate
        Self::new(value.into_iter().map(Into::into))
            .expect("A LexOrdering from LexRequirement must be non-degenerate")
    }
}

/// Represents a plan's input ordering requirements. Vector elements represent
/// alternative ordering requirements in the order of preference. The list of
/// alternatives can be either hard or soft, depending on whether the operator
/// can work without an input ordering.
///
/// # Invariants
///
/// The following always hold true for a `OrderingRequirements`:
///
/// 1. It is non-degenerate, meaning it contains at least one ordering. The
///    absence of an input ordering requirement is represented by a `None` value
///    in `ExecutionPlan` APIs, which return an `Option<OrderingRequirements>`.
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
    pub fn new_alternatives(
        alternatives: impl IntoIterator<Item = LexRequirement>,
        soft: bool,
    ) -> Option<Self> {
        let alternatives = alternatives.into_iter().collect::<Vec<_>>();
        (!alternatives.is_empty()).then(|| {
            if soft {
                Self::Soft(alternatives)
            } else {
                Self::Hard(alternatives)
            }
        })
    }

    /// Creates a new instance with a single hard requirement.
    pub fn new(requirement: LexRequirement) -> Self {
        Self::Hard(vec![requirement])
    }

    /// Creates a new instance with a single soft requirement.
    pub fn new_soft(requirement: LexRequirement) -> Self {
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

    /// Returns all alternatives as a vector of `LexRequirement` objects and a
    /// boolean value indicating softness/hardness of the requirements.
    pub fn into_alternatives(self) -> (Vec<LexRequirement>, bool) {
        match self {
            Self::Hard(alts) => (alts, false),
            Self::Soft(alts) => (alts, true),
        }
    }
}

impl From<LexRequirement> for OrderingRequirements {
    fn from(requirement: LexRequirement) -> Self {
        Self::new(requirement)
    }
}

impl From<LexOrdering> for OrderingRequirements {
    fn from(ordering: LexOrdering) -> Self {
        Self::new(ordering.into())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_reversed_sort_options() {
        // Test basic reversal: ASC NULLS LAST ↔ DESC NULLS FIRST
        let asc_nulls_last = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let desc_nulls_first = SortOptions {
            descending: true,
            nulls_first: true,
        };
        assert!(is_reversed_sort_options(&asc_nulls_last, &desc_nulls_first));
        assert!(is_reversed_sort_options(&desc_nulls_first, &asc_nulls_last));

        // Test another reversal: ASC NULLS FIRST ↔ DESC NULLS LAST
        let asc_nulls_first = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let desc_nulls_last = SortOptions {
            descending: true,
            nulls_first: false,
        };
        assert!(is_reversed_sort_options(&asc_nulls_first, &desc_nulls_last));
        assert!(is_reversed_sort_options(&desc_nulls_last, &asc_nulls_first));

        // Test non-reversal: same options
        assert!(!is_reversed_sort_options(&asc_nulls_last, &asc_nulls_last));
        assert!(!is_reversed_sort_options(
            &desc_nulls_first,
            &desc_nulls_first
        ));

        // Test non-reversal: only descending differs
        assert!(!is_reversed_sort_options(&asc_nulls_last, &desc_nulls_last));
        assert!(!is_reversed_sort_options(&desc_nulls_last, &asc_nulls_last));

        // Test non-reversal: only nulls_first differs
        assert!(!is_reversed_sort_options(&asc_nulls_last, &asc_nulls_first));
        assert!(!is_reversed_sort_options(&asc_nulls_first, &asc_nulls_last));
    }
}
