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
                if lhs.descending == rhs.descending
                    && lhs.nulls_first == rhs.nulls_first =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first,
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
                if lhs.descending != rhs.descending
                    && lhs.nulls_first == rhs.nulls_first =>
            {
                Self::Ordered(SortOptions {
                    descending: lhs.descending,
                    nulls_first: lhs.nulls_first,
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
                if lhs.descending != rhs.descending
                    && lhs.nulls_first == rhs.nulls_first =>
            {
                *self
            }
            _ => Self::Unordered,
        }
    }

    /// `AND` keeps an ordering only when nulls sort after `true`
    /// (`ASC NULLS LAST`, `DESC NULLS FIRST`).
    pub fn and(&self, rhs: &Self) -> Self {
        self.kleene(rhs, |opt| opt.descending == opt.nulls_first)
    }

    /// `OR` keeps an ordering only when nulls sort before `false`
    /// (`ASC NULLS FIRST`, `DESC NULLS LAST`).
    pub fn or(&self, rhs: &Self) -> Self {
        self.kleene(rhs, |opt| opt.descending != opt.nulls_first)
    }

    /// `preserves` reports whether the operator keeps data in that ordering.
    /// A `Singleton` may be a literal NULL, so it carries the same guard.
    fn kleene(&self, rhs: &Self, preserves: fn(&SortOptions) -> bool) -> Self {
        match (self, rhs) {
            (Self::Singleton, Self::Singleton) => Self::Singleton,
            (Self::Ordered(opt), Self::Singleton)
            | (Self::Singleton, Self::Ordered(opt))
                if preserves(opt) =>
            {
                Self::Ordered(*opt)
            }
            (Self::Ordered(lhs), Self::Ordered(rhs)) if lhs == rhs && preserves(lhs) => {
                Self::Ordered(*lhs)
            }
            _ => Self::Unordered,
        }
    }
}

#[cfg(test)]
mod sort_properties_test {
    use super::{SortOptions, SortProperties};
    use rstest::rstest;

    const fn ordered(descending: bool, nulls_first: bool) -> SortProperties {
        SortProperties::Ordered(SortOptions {
            descending,
            nulls_first,
        })
    }

    const ASC_NF: SortProperties = ordered(false, true);
    const ASC_NL: SortProperties = ordered(false, false);
    const DESC_NF: SortProperties = ordered(true, true);
    const DESC_NL: SortProperties = ordered(true, false);
    const UNORDERED: SortProperties = SortProperties::Unordered;
    const SINGLETON: SortProperties = SortProperties::Singleton;

    type BinOp = fn(&SortProperties, &SortProperties) -> SortProperties;

    /// Each method's direction rule and its `Singleton` arms.
    ///
    /// Operands that *disagree* on null placement are deliberately absent:
    /// that half is covered exhaustively by
    /// [`conflicting_null_placement_is_never_ordered`].
    #[test]
    fn ordering_propagation() {
        let cases: &[(&str, BinOp, SortProperties, SortProperties, SortProperties)] = &[
            // `add` preserves ordering when both operands run in the same
            // direction. It is commutative, so one argument order suffices.
            (
                "add: same direction is preserved",
                SortProperties::add,
                ASC_NF,
                ASC_NF,
                ASC_NF,
            ),
            (
                "add: nulls_last placement is preserved",
                SortProperties::add,
                ASC_NL,
                ASC_NL,
                ASC_NL,
            ),
            (
                "add: opposing directions are unordered",
                SortProperties::add,
                ASC_NF,
                DESC_NF,
                UNORDERED,
            ),
            (
                "add: literal with ordered",
                SortProperties::add,
                SINGLETON,
                ASC_NF,
                ASC_NF,
            ),
            (
                "add: two literals stay a literal",
                SortProperties::add,
                SINGLETON,
                SINGLETON,
                SINGLETON,
            ),
            // `and` keeps ASC NULLS LAST / DESC NULLS FIRST, `or` keeps ASC
            // NULLS FIRST / DESC NULLS LAST. Both are commutative.
            (
                "and: ASC NULLS LAST is preserved",
                SortProperties::and,
                ASC_NL,
                ASC_NL,
                ASC_NL,
            ),
            (
                "and: DESC NULLS FIRST is preserved",
                SortProperties::and,
                DESC_NF,
                DESC_NF,
                DESC_NF,
            ),
            (
                "and: ASC NULLS FIRST is unordered",
                SortProperties::and,
                ASC_NF,
                ASC_NF,
                UNORDERED,
            ),
            (
                "and: DESC NULLS LAST is unordered",
                SortProperties::and,
                DESC_NL,
                DESC_NL,
                UNORDERED,
            ),
            (
                "and: mixed directions are unordered",
                SortProperties::and,
                ASC_NL,
                DESC_NF,
                UNORDERED,
            ),
            (
                "and: literal with ASC NULLS LAST",
                SortProperties::and,
                SINGLETON,
                ASC_NL,
                ASC_NL,
            ),
            (
                "and: literal with ASC NULLS FIRST is unordered",
                SortProperties::and,
                SINGLETON,
                ASC_NF,
                UNORDERED,
            ),
            (
                "and: two literals stay a literal",
                SortProperties::and,
                SINGLETON,
                SINGLETON,
                SINGLETON,
            ),
            (
                "or: ASC NULLS FIRST is preserved",
                SortProperties::or,
                ASC_NF,
                ASC_NF,
                ASC_NF,
            ),
            (
                "or: DESC NULLS LAST is preserved",
                SortProperties::or,
                DESC_NL,
                DESC_NL,
                DESC_NL,
            ),
            (
                "or: ASC NULLS LAST is unordered",
                SortProperties::or,
                ASC_NL,
                ASC_NL,
                UNORDERED,
            ),
            (
                "or: DESC NULLS FIRST is unordered",
                SortProperties::or,
                DESC_NF,
                DESC_NF,
                UNORDERED,
            ),
            (
                "or: mixed directions are unordered",
                SortProperties::or,
                ASC_NF,
                DESC_NL,
                UNORDERED,
            ),
            (
                "or: literal with ASC NULLS FIRST",
                SortProperties::or,
                SINGLETON,
                ASC_NF,
                ASC_NF,
            ),
            (
                "or: literal with ASC NULLS LAST is unordered",
                SortProperties::or,
                SINGLETON,
                ASC_NL,
                UNORDERED,
            ),
            (
                "or: two literals stay a literal",
                SortProperties::or,
                SINGLETON,
                SINGLETON,
                SINGLETON,
            ),
            // `sub` needs the *opposite* rule: an ascending column minus a
            // descending one still ascends. It is not commutative
            (
                "sub: opposing directions are preserved",
                SortProperties::sub,
                ASC_NF,
                DESC_NF,
                ASC_NF,
            ),
            (
                "sub: result follows the left operand",
                SortProperties::sub,
                DESC_NF,
                ASC_NF,
                DESC_NF,
            ),
            (
                "sub: nulls_last placement is preserved",
                SortProperties::sub,
                ASC_NL,
                DESC_NL,
                ASC_NL,
            ),
            (
                "sub: same direction is unordered",
                SortProperties::sub,
                ASC_NF,
                ASC_NF,
                UNORDERED,
            ),
            (
                "sub: literal minus ordered flips the direction",
                SortProperties::sub,
                SINGLETON,
                ASC_NF,
                DESC_NF,
            ),
            (
                "sub: ordered minus literal keeps the direction",
                SortProperties::sub,
                ASC_NF,
                SINGLETON,
                ASC_NF,
            ),
            (
                "sub: two literals stay a literal",
                SortProperties::sub,
                SINGLETON,
                SINGLETON,
                SINGLETON,
            ),
            // `gt_or_gteq` compares into a boolean column, which is ordered by
            // `false < true`. Same direction rule as `sub`, also asymmetric.
            (
                "gt_or_gteq: opposing directions are preserved",
                SortProperties::gt_or_gteq,
                ASC_NF,
                DESC_NF,
                ASC_NF,
            ),
            (
                "gt_or_gteq: result follows the left operand",
                SortProperties::gt_or_gteq,
                DESC_NF,
                ASC_NF,
                DESC_NF,
            ),
            (
                "gt_or_gteq: nulls_last placement is preserved",
                SortProperties::gt_or_gteq,
                DESC_NL,
                ASC_NL,
                DESC_NL,
            ),
            (
                "gt_or_gteq: same direction is unordered",
                SortProperties::gt_or_gteq,
                ASC_NF,
                ASC_NF,
                UNORDERED,
            ),
            (
                "gt_or_gteq: literal on the left flips the direction",
                SortProperties::gt_or_gteq,
                SINGLETON,
                ASC_NF,
                DESC_NF,
            ),
            (
                "gt_or_gteq: literal on the right keeps the direction",
                SortProperties::gt_or_gteq,
                ASC_NF,
                SINGLETON,
                ASC_NF,
            ),
            (
                "gt_or_gteq: two literals stay a literal",
                SortProperties::gt_or_gteq,
                SINGLETON,
                SINGLETON,
                SINGLETON,
            ),
        ];

        for &(name, op, lhs, rhs, expected) in cases {
            assert_eq!(op(&lhs, &rhs), expected, "case: {name}");
        }

        // `add`, `and` and `or` are commutative, so the table above covers
        // only one argument order.
        for (lhs, rhs) in [
            (ASC_NF, DESC_NL),
            (ASC_NF, SINGLETON),
            (ASC_NL, SINGLETON),
            (ASC_NL, DESC_NF),
        ] {
            assert_eq!(lhs.add(&rhs), rhs.add(&lhs), "add is commutative");
            assert_eq!(lhs.and(&rhs), rhs.and(&lhs), "and is commutative");
            assert_eq!(lhs.or(&rhs), rhs.or(&lhs), "or is commutative");
        }
    }

    /// If two ordered operands disagree on null placement, the result is
    /// always Unordered, no matter which operator or direction is used.
    /// Checked below for every combination.
    ///
    /// Nulls propagate through the arithmetic and comparison operators: the
    /// result is null wherever either operand is null. `nulls_first` treats
    /// those rows as a prefix, `nulls_last` as a suffix. A set that's both
    /// can't be described by any `SortOptions`. `and`/`or` keep an ordering
    /// only when both operands already share it.
    ///
    /// The assertion only checks "not Ordered", not which ordering
    /// results. That keeps the test from just repeating the logic it's
    /// checking.
    #[rstest]
    #[case::add("add", SortProperties::add)]
    #[case::sub("sub", SortProperties::sub)]
    #[case::gt_or_gteq("gt_or_gteq", SortProperties::gt_or_gteq)]
    #[case::and("and", SortProperties::and)]
    #[case::or("or", SortProperties::or)]
    fn conflicting_null_placement_is_never_ordered(
        #[values(false, true)] l_descending: bool,
        #[values(false, true)] r_descending: bool,
        #[values(false, true)] l_nulls_first: bool,
        #[case] op_name: &str,
        #[case] op: BinOp,
    ) {
        // Negating `l_nulls_first` makes the operands disagree by construction.
        let lhs = ordered(l_descending, l_nulls_first);
        let rhs = ordered(r_descending, !l_nulls_first);
        assert_eq!(op(&lhs, &rhs), UNORDERED, "{op_name}: {lhs:?} and {rhs:?}");
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
    /// of its inputs.
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
    /// implies the other: a lexicographical-ordering-preserving expression
    /// need not be strict (distinct inputs may still produce equal outputs),
    /// while `a + b` over two ordered, overflow-free inputs is strict but not
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
