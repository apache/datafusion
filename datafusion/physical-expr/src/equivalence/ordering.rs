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

use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;
use std::vec::IntoIter;

use crate::expressions::with_new_schema;
use crate::{add_offset_to_physical_sort_exprs, LexOrdering, PhysicalExpr};

use arrow::compute::SortOptions;
use arrow::datatypes::SchemaRef;
use datafusion_common::{HashSet, Result};
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

/// An `OrderingEquivalenceClass` keeps track of distinct alternative orderings
/// than can describe a table. For example, consider the following table:
///
/// ```text
/// ┌───┬───┬───┬───┐
/// │ a │ b │ c │ d │
/// ├───┼───┼───┼───┤
/// │ 1 │ 4 │ 3 │ 1 │
/// │ 2 │ 3 │ 3 │ 2 │
/// │ 3 │ 1 │ 2 │ 2 │
/// │ 3 │ 2 │ 1 │ 3 │
/// └───┴───┴───┴───┘
/// ```
///
/// Here, both `[a ASC, b ASC]` and `[c DESC, d ASC]` describe the table
/// ordering. In this case, we say that these orderings are equivalent.
///
/// An `OrderingEquivalenceClass` is a set of such equivalent orderings, which
/// is represented by a vector of `LexOrdering`s. The set does not store any
/// redundant information by enforcing the invariant that no suffix of an
/// ordering in the equivalence class is a prefix of another ordering in the
/// equivalence class. The set can be empty, which means that there are no
/// orderings that describe the table.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OrderingEquivalenceClass {
    orderings: Vec<LexOrdering>,
}

impl OrderingEquivalenceClass {
    /// Clears (empties) this ordering equivalence class.
    pub fn clear(&mut self) {
        self.orderings.clear();
    }

    /// Creates a new ordering equivalence class from the given orderings
    /// and removes any redundant entries (if given).
    pub fn new(
        orderings: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) -> Self {
        let mut result = Self {
            orderings: orderings.into_iter().filter_map(LexOrdering::new).collect(),
        };
        result.remove_redundant_entries();
        result
    }

    /// Extend this ordering equivalence class with the given orderings.
    pub fn extend(&mut self, orderings: impl IntoIterator<Item = LexOrdering>) {
        self.orderings.extend(orderings);
        // Make sure that there are no redundant orderings:
        self.remove_redundant_entries();
    }

    /// Adds new orderings into this ordering equivalence class.
    pub fn add_orderings(
        &mut self,
        sort_exprs: impl IntoIterator<Item = impl IntoIterator<Item = PhysicalSortExpr>>,
    ) {
        self.orderings
            .extend(sort_exprs.into_iter().filter_map(LexOrdering::new));
        // Make sure that there are no redundant orderings:
        self.remove_redundant_entries();
    }

    /// Removes redundant orderings from this ordering equivalence class.
    ///
    /// For instance, if we already have the ordering `[a ASC, b ASC, c DESC]`,
    /// then there is no need to keep ordering `[a ASC, b ASC]` in the state.
    fn remove_redundant_entries(&mut self) {
        let mut work = true;
        while work {
            work = false;
            let mut idx = 0;
            'outer: while idx < self.orderings.len() {
                let mut ordering_idx = idx + 1;
                while ordering_idx < self.orderings.len() {
                    if let Some(remove) = self.resolve_overlap(idx, ordering_idx) {
                        work = true;
                        if remove {
                            self.orderings.swap_remove(idx);
                            continue 'outer;
                        }
                    }
                    if let Some(remove) = self.resolve_overlap(ordering_idx, idx) {
                        work = true;
                        if remove {
                            self.orderings.swap_remove(ordering_idx);
                            continue;
                        }
                    }
                    ordering_idx += 1;
                }
                idx += 1;
            }
        }
    }

    /// Trims `orderings[idx]` if some suffix of it overlaps with a prefix of
    /// `orderings[pre_idx]`. If there is any overlap, returns a `Some(true)`
    /// if any trimming took place, and `Some(false)` otherwise. If there is
    /// no overlap, returns `None`.
    ///
    /// For example, if `orderings[idx]` is `[a ASC, b ASC, c DESC]` and
    /// `orderings[pre_idx]` is `[b ASC, c DESC]`, then the function will trim
    /// `orderings[idx]` to `[a ASC]`.
    fn resolve_overlap(&mut self, idx: usize, pre_idx: usize) -> Option<bool> {
        let length = self.orderings[idx].len();
        let other_length = self.orderings[pre_idx].len();
        for overlap in 1..=length.min(other_length) {
            if self.orderings[idx][length - overlap..]
                == self.orderings[pre_idx][..overlap]
            {
                return Some(!self.orderings[idx].truncate(length - overlap));
            }
        }
        None
    }

    /// Returns the concatenation of all the orderings. This enables merge
    /// operations to preserve all equivalent orderings simultaneously.
    pub fn output_ordering(&self) -> Option<LexOrdering> {
        self.orderings.iter().cloned().reduce(|mut cat, o| {
            cat.extend(o);
            cat
        })
    }

    // Append orderings in `other` to all existing orderings in this ordering
    // equivalence class.
    pub fn join_suffix(mut self, other: &Self) -> Self {
        let n_ordering = self.orderings.len();
        // Replicate entries before cross product:
        let n_cross = std::cmp::max(n_ordering, other.len() * n_ordering);
        self.orderings = self.orderings.into_iter().cycle().take(n_cross).collect();
        // Append sort expressions of `other` to the current orderings:
        for (outer_idx, ordering) in other.iter().enumerate() {
            let base = outer_idx * n_ordering;
            // Use the cross product index:
            for idx in base..(base + n_ordering) {
                self.orderings[idx].extend(ordering.iter().cloned());
            }
        }
        self
    }

    /// Adds `offset` value to the index of each expression inside this
    /// ordering equivalence class.
    pub fn add_offset(&mut self, offset: isize) -> Result<()> {
        let orderings = std::mem::take(&mut self.orderings);
        for ordering_result in orderings
            .into_iter()
            .map(|o| add_offset_to_physical_sort_exprs(o, offset))
        {
            self.orderings.extend(LexOrdering::new(ordering_result?));
        }
        Ok(())
    }

    /// Transforms this `OrderingEquivalenceClass` by mapping columns in the
    /// original schema to columns in the new schema by index. The new schema
    /// and the original schema needs to be aligned; i.e. they should have the
    /// same number of columns, and fields at the same index have the same type
    /// in both schemas.
    pub fn with_new_schema(mut self, schema: &SchemaRef) -> Result<Self> {
        self.orderings = self
            .orderings
            .into_iter()
            .map(|ordering| {
                ordering
                    .into_iter()
                    .map(|mut sort_expr| {
                        sort_expr.expr = with_new_schema(sort_expr.expr, schema)?;
                        Ok(sort_expr)
                    })
                    .collect::<Result<Vec<_>>>()
                    // The following `unwrap` is safe because the vector will always
                    // be non-empty.
                    .map(|v| LexOrdering::new(v).unwrap())
            })
            .collect::<Result<_>>()?;
        Ok(self)
    }

    /// Gets sort options associated with this expression if it is a leading
    /// ordering expression. Otherwise, returns `None`.
    pub fn get_options(&self, expr: &Arc<dyn PhysicalExpr>) -> Option<SortOptions> {
        for ordering in self.iter() {
            let leading_ordering = &ordering[0];
            if leading_ordering.expr.eq(expr) {
                return Some(leading_ordering.options);
            }
        }
        None
    }

    /// Checks whether the given expression is partially constant according to
    /// this ordering equivalence class.
    ///
    /// This function determines whether `expr` appears in at least one combination
    /// of `descending` and `nulls_first` options that indicate partial constantness
    /// in a lexicographical ordering. Specifically, an expression is considered
    /// a partial constant in this context if its `SortOptions` satisfies either
    /// of the following conditions:
    /// - It is `descending` with `nulls_first` and _also_ `ascending` with
    ///   `nulls_last`, OR
    /// - It is `descending` with `nulls_last` and _also_ `ascending` with
    ///   `nulls_first`.
    ///
    /// The equivalence mechanism primarily uses `ConstExpr`s to represent globally
    /// constant expressions. However, some expressions may only be partially
    /// constant within a lexicographical ordering. This function helps identify
    /// such cases. If an expression is constant within a prefix ordering, it is
    /// added as a constant during `ordering_satisfy_requirement()` iterations
    /// after the corresponding prefix requirement is satisfied.
    ///
    /// ### Future Improvements
    ///
    /// This function may become unnecessary if any of the following improvements
    /// are implemented:
    /// 1. `SortOptions` supports encoding constantness information.
    /// 2. `EquivalenceProperties` gains `FunctionalDependency` awareness, eliminating
    ///    the need for `Constant` and `Constraints`.
    pub fn is_expr_partial_const(&self, expr: &Arc<dyn PhysicalExpr>) -> bool {
        let mut constantness_defining_pairs = [
            HashSet::from([(false, false), (true, true)]),
            HashSet::from([(false, true), (true, false)]),
        ];

        for ordering in self.iter() {
            let leading_ordering = ordering.first();
            if leading_ordering.expr.eq(expr) {
                let opt = (
                    leading_ordering.options.descending,
                    leading_ordering.options.nulls_first,
                );
                constantness_defining_pairs[0].remove(&opt);
                constantness_defining_pairs[1].remove(&opt);
            }
        }

        constantness_defining_pairs
            .iter()
            .any(|pair| pair.is_empty())
    }
}

impl Deref for OrderingEquivalenceClass {
    type Target = [LexOrdering];

    fn deref(&self) -> &Self::Target {
        self.orderings.as_slice()
    }
}

impl From<Vec<LexOrdering>> for OrderingEquivalenceClass {
    fn from(orderings: Vec<LexOrdering>) -> Self {
        let mut result = Self { orderings };
        result.remove_redundant_entries();
        result
    }
}

/// Convert the `OrderingEquivalenceClass` into an iterator of `LexOrdering`s.
impl IntoIterator for OrderingEquivalenceClass {
    type Item = LexOrdering;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.orderings.into_iter()
    }
}

impl Display for OrderingEquivalenceClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[")?;
        let mut iter = self.orderings.iter();
        if let Some(ordering) = iter.next() {
            write!(f, "[{ordering}]")?;
        }
        for ordering in iter {
            write!(f, ", [{ordering}]")?;
        }
        write!(f, "]")
    }
}

impl From<OrderingEquivalenceClass> for Vec<LexOrdering> {
    fn from(oeq_class: OrderingEquivalenceClass) -> Self {
        oeq_class.orderings
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::equivalence::tests::create_test_schema;
    use crate::equivalence::{
        convert_to_orderings, convert_to_sort_exprs, EquivalenceClass, EquivalenceGroup,
        EquivalenceProperties, OrderingEquivalenceClass,
    };
    use crate::expressions::{col, BinaryExpr, Column};
    use crate::utils::tests::TestScalarUDF;
    use crate::{
        AcrossPartitions, ConstExpr, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr,
        ScalarFunctionExpr,
    };

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::Result;
    use datafusion_expr::{Operator, ScalarUDF};

    #[test]
    fn test_ordering_satisfy() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]));
        let crude = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("a", 0)),
            options: SortOptions::default(),
        }];
        let finer = vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: SortOptions::default(),
            },
        ];
        // finer ordering satisfies, crude ordering should return true
        let eq_properties_finer = EquivalenceProperties::new_with_orderings(
            Arc::clone(&input_schema),
            [finer.clone()],
        );
        assert!(eq_properties_finer.ordering_satisfy(crude.clone())?);

        // Crude ordering doesn't satisfy finer ordering. should return false
        let eq_properties_crude =
            EquivalenceProperties::new_with_orderings(Arc::clone(&input_schema), [crude]);
        assert!(!eq_properties_crude.ordering_satisfy(finer)?);
        Ok(())
    }

    #[test]
    fn test_ordering_satisfy_with_equivalence2() -> Result<()> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let test_fun = Arc::new(ScalarUDF::new_from_impl(TestScalarUDF::new()));

        let floor_a = Arc::new(ScalarFunctionExpr::try_new(
            Arc::clone(&test_fun),
            vec![Arc::clone(col_a)],
            &test_schema,
            Arc::new(ConfigOptions::default()),
        )?) as PhysicalExprRef;
        let floor_f = Arc::new(ScalarFunctionExpr::try_new(
            Arc::clone(&test_fun),
            vec![Arc::clone(col_f)],
            &test_schema,
            Arc::new(ConfigOptions::default()),
        )?) as PhysicalExprRef;
        let exp_a = Arc::new(ScalarFunctionExpr::try_new(
            Arc::clone(&test_fun),
            vec![Arc::clone(col_a)],
            &test_schema,
            Arc::new(ConfigOptions::default()),
        )?) as PhysicalExprRef;

        let a_plus_b = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_b),
        )) as Arc<dyn PhysicalExpr>;
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let test_cases = vec![
            // ------------ TEST CASE 1 ------------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![(col_a, options), (col_d, options), (col_b, options)],
                    // [c ASC]
                    vec![(col_c, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, b ASC], requirement is not satisfied.
                vec![(col_a, options), (col_b, options)],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 2 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(a) ASC],
                vec![(&floor_a, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 2.1 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(f) ASC], (Please note that a=f)
                vec![(&floor_f, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 3 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, c ASC, a+b ASC],
                vec![(col_a, options), (col_c, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 4 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [floor(a) ASC, a+b ASC],
                vec![(&floor_a, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                false,
            ),
            // ------------ TEST CASE 5 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [exp(a) ASC, a+b ASC],
                vec![(&exp_a, options), (&a_plus_b, options)],
                // expected: requirement is not satisfied.
                // TODO: If we know that exp function is 1-to-1 function.
                //  we could have deduced that above requirement is satisfied.
                false,
            ),
            // ------------ TEST CASE 6 ------------
            (
                // orderings
                vec![
                    // [a ASC, d ASC, b ASC]
                    vec![(col_a, options), (col_d, options), (col_b, options)],
                    // [c ASC]
                    vec![(col_c, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, d ASC, floor(a) ASC],
                vec![(col_a, options), (col_d, options), (&floor_a, options)],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 7 ------------
            (
                // orderings
                vec![
                    // [a ASC, c ASC, b ASC]
                    vec![(col_a, options), (col_c, options), (col_b, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, floor(a) ASC, a + b ASC],
                vec![(col_a, options), (&floor_a, options), (&a_plus_b, options)],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 8 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![(col_a, options), (col_b, options), (col_c, options)],
                    // [d ASC]
                    vec![(col_d, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, c ASC, floor(a) ASC, a + b ASC],
                vec![
                    (col_a, options),
                    (col_c, options),
                    (&floor_a, options),
                    (&a_plus_b, options),
                ],
                // expected: requirement is not satisfied.
                false,
            ),
            // ------------ TEST CASE 9 ------------
            (
                // orderings
                vec![
                    // [a ASC, b ASC, c ASC, d ASC]
                    vec![
                        (col_a, options),
                        (col_b, options),
                        (col_c, options),
                        (col_d, options),
                    ],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [a ASC, b ASC, c ASC, floor(a) ASC],
                vec![
                    (col_a, options),
                    (col_b, options),
                    (col_c, options),
                    (&floor_a, options),
                ],
                // expected: requirement is satisfied.
                true,
            ),
            // ------------ TEST CASE 10 ------------
            (
                // orderings
                vec![
                    // [d ASC, b ASC]
                    vec![(col_d, options), (col_b, options)],
                    // [c ASC, a ASC]
                    vec![(col_c, options), (col_a, options)],
                ],
                // equivalence classes
                vec![vec![col_a, col_f]],
                // constants
                vec![col_e],
                // requirement [c ASC, d ASC, a + b ASC],
                vec![(col_c, options), (col_d, options), (&a_plus_b, options)],
                // expected: requirement is satisfied.
                true,
            ),
        ];

        for (orderings, eq_group, constants, reqs, expected) in test_cases {
            let err_msg =
                format!("error in test orderings: {orderings:?}, eq_group: {eq_group:?}, constants: {constants:?}, reqs: {reqs:?}, expected: {expected:?}");
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
            let orderings = convert_to_orderings(&orderings);
            eq_properties.add_orderings(orderings);
            let classes = eq_group
                .into_iter()
                .map(|eq_class| EquivalenceClass::new(eq_class.into_iter().cloned()));
            let eq_group = EquivalenceGroup::new(classes);
            eq_properties.add_equivalence_group(eq_group)?;

            let constants = constants.into_iter().map(|expr| {
                ConstExpr::new(Arc::clone(expr), AcrossPartitions::Uniform(None))
            });
            eq_properties.add_constants(constants)?;

            let reqs = convert_to_sort_exprs(&reqs);
            assert_eq!(eq_properties.ordering_satisfy(reqs)?, expected, "{err_msg}");
        }

        Ok(())
    }

    #[test]
    fn test_ordering_satisfy_different_lengths() -> Result<()> {
        let test_schema = create_test_schema()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let options = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // a=c (e.g they are aliases).
        let mut eq_properties = EquivalenceProperties::new(test_schema);
        eq_properties.add_equal_conditions(Arc::clone(col_a), Arc::clone(col_c))?;

        let orderings = vec![
            vec![(col_a, options)],
            vec![(col_e, options)],
            vec![(col_d, options), (col_f, options)],
        ];
        let orderings = convert_to_orderings(&orderings);

        // Column [a ASC], [e ASC], [d ASC, f ASC] are all valid orderings for the schema.
        eq_properties.add_orderings(orderings);

        // First entry in the tuple is required ordering, second entry is the expected flag
        // that indicates whether this required ordering is satisfied.
        // ([a ASC], true) indicate a ASC requirement is already satisfied by existing orderings.
        let test_cases = vec![
            // [c ASC, a ASC, e ASC], expected represents this requirement is satisfied
            (
                vec![(col_c, options), (col_a, options), (col_e, options)],
                true,
            ),
            (vec![(col_c, options), (col_b, options)], false),
            (vec![(col_c, options), (col_d, options)], true),
            (
                vec![(col_d, options), (col_f, options), (col_b, options)],
                false,
            ),
            (vec![(col_d, options), (col_f, options)], true),
        ];

        for (reqs, expected) in test_cases {
            let err_msg =
                format!("error in test reqs: {reqs:?}, expected: {expected:?}",);
            let reqs = convert_to_sort_exprs(&reqs);
            assert_eq!(eq_properties.ordering_satisfy(reqs)?, expected, "{err_msg}");
        }

        Ok(())
    }

    #[test]
    fn test_remove_redundant_entries_oeq_class() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let col_e = &col("e", &schema)?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // First entry in the tuple is the given orderings for the table
        // Second entry is the simplest version of the given orderings that is functionally equivalent.
        let test_cases = vec![
            // ------- TEST CASE 1 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                ],
            ),
            // ------- TEST CASE 2 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 3 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b DESC]
                    vec![(col_a, option_asc), (col_b, option_desc)],
                    // [a ASC]
                    vec![(col_a, option_asc)],
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b DESC]
                    vec![(col_a, option_asc), (col_b, option_desc)],
                    // [a ASC, c ASC]
                    vec![(col_a, option_asc), (col_c, option_asc)],
                ],
            ),
            // ------- TEST CASE 4 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [a ASC]
                    vec![(col_a, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
            ),
            // ------- TEST CASE 5 ---------
            // Empty ordering
            (
                vec![],
                // No ordering in the state (empty ordering is ignored).
                vec![],
            ),
            // ------- TEST CASE 6 ---------
            (
                // ORDERINGS GIVEN
                vec![
                    // [a ASC, b ASC]
                    vec![(col_a, option_asc), (col_b, option_asc)],
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [a ASC]
                    vec![(col_a, option_asc)],
                    // [b ASC]
                    vec![(col_b, option_asc)],
                ],
            ),
            // ------- TEST CASE 7 ---------
            // b, a
            // c, a
            // d, b, c
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC, a ASC]
                    vec![(col_b, option_asc), (col_a, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC, b ASC, c ASC]
                    vec![
                        (col_d, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC, a ASC]
                    vec![(col_b, option_asc), (col_a, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
            // ------- TEST CASE 8 ---------
            // b, e
            // c, a
            // d, b, e, c, a
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC, e ASC]
                    vec![(col_b, option_asc), (col_e, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC, b ASC, e ASC, c ASC, a ASC]
                    vec![
                        (col_d, option_asc),
                        (col_b, option_asc),
                        (col_e, option_asc),
                        (col_c, option_asc),
                        (col_a, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC, e ASC]
                    vec![(col_b, option_asc), (col_e, option_asc)],
                    // [c ASC, a ASC]
                    vec![(col_c, option_asc), (col_a, option_asc)],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
            // ------- TEST CASE 9 ---------
            // b
            // a, b, c
            // d, a, b
            (
                // ORDERINGS GIVEN
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [d ASC, a ASC, b ASC]
                    vec![
                        (col_d, option_asc),
                        (col_a, option_asc),
                        (col_b, option_asc),
                    ],
                ],
                // EXPECTED orderings that is succinct.
                vec![
                    // [b ASC]
                    vec![(col_b, option_asc)],
                    // [a ASC, b ASC, c ASC]
                    vec![
                        (col_a, option_asc),
                        (col_b, option_asc),
                        (col_c, option_asc),
                    ],
                    // [d ASC]
                    vec![(col_d, option_asc)],
                ],
            ),
        ];
        for (orderings, expected) in test_cases {
            let orderings = convert_to_orderings(&orderings);
            let expected = convert_to_orderings(&expected);
            let actual = OrderingEquivalenceClass::from(orderings.clone());
            let err_msg = format!(
                "orderings: {orderings:?}, expected: {expected:?}, actual :{actual:?}"
            );
            assert_eq!(actual.len(), expected.len(), "{err_msg}");
            for elem in actual {
                assert!(expected.contains(&elem), "{}", err_msg);
            }
        }

        Ok(())
    }
}
