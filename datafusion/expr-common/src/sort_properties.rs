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

use std::ops::Neg;

use crate::interval_arithmetic::Interval;

use arrow::compute::SortOptions;
use arrow::datatypes::DataType;

/// To propagate [`SortOptions`] across the `PhysicalExpr`, it is insufficient
/// to simply use `Option<SortOptions>`: There must be a differentiation between
/// unordered columns and literal values, since literals may not break the ordering
/// when they are used as a child of some binary expression when the other child has
/// some ordering. On the other hand, unordered columns cannot maintain ordering when
/// they take part in such operations.
///
/// Example: ((a_ordered + b_unordered) + c_ordered) expression cannot end up with
/// sorted data; however the ((a_ordered + 999) + c_ordered) expression can. Therefore,
/// we need two different variants for literals and unordered columns as literals are
/// often more ordering-friendly under most mathematical operations.
#[derive(PartialEq, Debug, Clone, Copy, Default)]
pub enum SortProperties {
    /// Use the ordinary [`SortOptions`] struct to represent ordered data:
    Ordered(SortOptions),
    // This alternative represents unordered data:
    #[default]
    Unordered,
    // Singleton is used for single-valued literal numbers:
    Singleton,
}

impl SortProperties {
    pub fn add(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, _) => *rhs,
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn sub(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            _ => Self::Unordered,
        }
    }

    pub fn gt_or_gteq(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Ordered(rhs)) => Self::Ordered(SortOptions {
                descending: !rhs.descending,
                nulls_first: rhs.nulls_first,
            }),
            (_, Self::Singleton) => *self,
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending != rhs.descending =>
            {
                *self
            }
            _ => Self::Unordered,
        }
    }

    pub fn and_or(&self, rhs: &Self) -> Self {
        match (self, rhs) {
            (Self::Ordered(lhs), Self::Ordered(rhs))
                if lhs.descending == rhs.descending =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first || rhs.nulls_first,
                })
            }
            (Self::Ordered(opt), Self::Singleton)
            | (Self::Singleton, Self::Ordered(opt)) => Self::Ordered(SortOptions {
                descending: opt.descending,
                nulls_first: opt.nulls_first,
            }),
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            _ => Self::Unordered,
        }
    }
}

impl Neg for SortProperties {
    type Output = Self;

    fn neg(mut self) -> Self::Output {
        if let SortProperties::Ordered(SortOptions { descending, .. }) = &mut self {
            *descending = !*descending;
        }
        self
    }
}

/// Represents the properties of a `PhysicalExpr`, including its sorting,
/// range, and whether it preserves lexicographical ordering.
#[derive(Debug, Clone)]
pub struct ExprProperties {
    /// Properties that describe the sorting behavior of the expression,
    /// such as whether it is ordered, unordered, or a singleton value.
    pub sort_properties: SortProperties,
    /// A closed interval representing the range of possible values for
    /// the expression. Used to compute reliable bounds.
    pub range: Interval,
    /// Indicates whether the expression preserves lexicographical ordering
    /// of its inputs. For example, string concatenation preserves ordering,
    /// while addition does not.
    ///
    /// This is a *non-strict* (monotone) property: inputs advancing in
    /// lexicographical order never make the output decrease, but distinct
    /// inputs may map to equal outputs (ties). See
    /// [`Self::strictly_order_preserving`] for the strict variant and an
    /// explanation of the difference.
    pub preserves_lex_ordering: bool,
    /// Indicates whether the expression is strictly order-preserving with
    /// respect to its inputs that are `Ordered`: the output is ordered in the
    /// same direction, equal outputs can only result from equal values of
    /// those inputs (i.e. the mapping is one-to-one), and nulls map to nulls.
    ///
    /// i.e. setting this to true means that `a.cmp(b) == f(a).cmp(f(b))`
    ///
    /// # Difference from [`Self::preserves_lex_ordering`]
    ///
    /// The two properties differ in both their premise and their strictness:
    ///
    /// - `preserves_lex_ordering` assumes the inputs advance in
    ///   *lexicographical* order (a later input may decrease whenever an
    ///   earlier one increases), and only promises a non-decreasing output,
    ///   allowing distinct inputs to collapse into equal outputs; `floor`,
    ///   `date_trunc` and narrowing casts do exactly that.
    /// - `strictly_order_preserving` assumes every `Ordered` input advances
    ///   *simultaneously* (component-wise, which is what actually holds when
    ///   all of them are sorted in the data), and promises a strict output:
    ///   equal outputs only from equal inputs.
    ///
    /// For an expression with a single ordered input the premises coincide,
    /// and this field is simply the stronger claim: it implies
    /// `preserves_lex_ordering`. With multiple ordered inputs, neither
    /// implies the other: `concat(a, b)` preserves lexicographical ordering
    /// but is not strict (distinct inputs can produce equal outputs), while
    /// `a + b` over two ordered, overflow-free inputs is strict but not
    /// lexicographical (under the lexicographical premise `b` may decrease
    /// while `a` increases, making the sum decrease).
    ///
    /// The distinction matters for suffix sort keys. Optimizers use this
    /// field to substitute a sort key with an expression computed from it:
    /// if data is sorted by `[x, y]`, it is also sorted by `[expr(x), y]`.
    /// That claim requires `y` to be sorted within each run of equal
    /// `expr(x)` values, which only holds if equal outputs imply equal `x`
    /// values. With a merely monotone expression such as `floor`, one output
    /// run can span several `x` groups, and `y` restarts at each group:
    ///
    /// ```text
    /// sorted by [x, y]:  (1.2, 5), (1.8, 1), (2.5, 3)
    /// [floor(x), y]:     (1, 5),   (1, 1),   (2, 3)   <-- y not sorted within
    ///                                                     the "1" run
    /// ```
    ///
    /// Hence a monotone expression only justifies the length-1 ordering
    /// `[expr(x)]`, while a strictly order-preserving one keeps the entire
    /// suffix valid. When in doubt, set to `false`.
    pub strictly_order_preserving: bool,
}

impl ExprProperties {
    /// Creates a new `ExprProperties` instance with unknown sort properties,
    /// unknown range, and unknown lexicographical ordering preservation.
    pub fn new_unknown() -> Self {
        Self {
            sort_properties: SortProperties::default(),
            range: Interval::make_unbounded(&DataType::Null).unwrap(),
            preserves_lex_ordering: false,
            strictly_order_preserving: false,
        }
    }

    /// Sets the sorting properties of the expression and returns the modified instance.
    pub fn with_order(mut self, order: SortProperties) -> Self {
        self.sort_properties = order;
        self
    }

    /// Sets the range of the expression and returns the modified instance.
    pub fn with_range(mut self, range: Interval) -> Self {
        self.range = range;
        self
    }

    /// Sets whether the expression maintains lexicographical ordering and returns the modified instance.
    pub fn with_preserves_lex_ordering(mut self, preserves_lex_ordering: bool) -> Self {
        self.preserves_lex_ordering = preserves_lex_ordering;
        self
    }

    /// Sets whether the expression is strictly order-preserving and returns
    /// the modified instance.
    pub fn with_strictly_order_preserving(
        mut self,
        strictly_order_preserving: bool,
    ) -> Self {
        self.strictly_order_preserving = strictly_order_preserving;
        self
    }
}
