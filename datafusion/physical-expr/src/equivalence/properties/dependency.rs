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

use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::expr_refers;
use crate::{LexOrdering, PhysicalSortExpr};

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;

// A list of sort expressions that can be calculated from a known set of
/// dependencies.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Dependencies {
    sort_exprs: IndexSet<PhysicalSortExpr>,
}

impl Display for Dependencies {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        let mut iter = self.sort_exprs.iter();
        if let Some(dep) = iter.next() {
            write!(f, "{dep}")?;
        }
        for dep in iter {
            write!(f, ", {dep}")?;
        }
        write!(f, "]")
    }
}

impl Dependencies {
    // Creates a new `Dependencies` instance from the given sort expressions.
    pub fn new(sort_exprs: impl IntoIterator<Item = PhysicalSortExpr>) -> Self {
        Self {
            sort_exprs: sort_exprs.into_iter().collect(),
        }
    }
}

impl Deref for Dependencies {
    type Target = IndexSet<PhysicalSortExpr>;

    fn deref(&self) -> &Self::Target {
        &self.sort_exprs
    }
}

impl DerefMut for Dependencies {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sort_exprs
    }
}

impl IntoIterator for Dependencies {
    type Item = PhysicalSortExpr;
    type IntoIter = <IndexSet<PhysicalSortExpr> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.sort_exprs.into_iter()
    }
}

/// Contains a mapping of all dependencies we have processed for each sort expr
pub struct DependencyEnumerator<'a> {
    /// Maps `expr` --> `[exprs]` that have previously been processed
    seen: IndexMap<&'a PhysicalSortExpr, IndexSet<&'a PhysicalSortExpr>>,
}

impl<'a> DependencyEnumerator<'a> {
    pub fn new() -> Self {
        Self {
            seen: IndexMap::new(),
        }
    }

    /// Insert a new dependency,
    ///
    /// returns false if the dependency was already in the map
    /// returns true if the dependency was newly inserted
    fn insert(
        &mut self,
        target: &'a PhysicalSortExpr,
        dep: &'a PhysicalSortExpr,
    ) -> bool {
        self.seen.entry(target).or_default().insert(dep)
    }

    /// This function recursively analyzes the dependencies of the given sort
    /// expression within the given dependency map to construct lexicographical
    /// orderings that include the sort expression and its dependencies.
    ///
    /// # Parameters
    ///
    /// - `referred_sort_expr`: A reference to the sort expression (`PhysicalSortExpr`)
    ///   for which lexicographical orderings satisfying its dependencies are to be
    ///   constructed.
    /// - `dependency_map`: A reference to the `DependencyMap` that contains
    ///   dependencies for different `PhysicalSortExpr`s.
    ///
    /// # Returns
    ///
    /// A vector of lexicographical orderings (`Vec<LexOrdering>`) based on the given
    /// sort expression and its dependencies.
    pub fn construct_orderings(
        &mut self,
        referred_sort_expr: &'a PhysicalSortExpr,
        dependency_map: &'a DependencyMap,
    ) -> Vec<LexOrdering> {
        let node = dependency_map
            .get(referred_sort_expr)
            .expect("`referred_sort_expr` should be inside `dependency_map`");
        // Since we work on intermediate nodes, we are sure `node.target` exists.
        let target = node.target.as_ref().unwrap();
        // An empty dependency means the referred_sort_expr represents a global ordering.
        // Return its projected version, which is the target_expression.
        if node.dependencies.is_empty() {
            return vec![[target.clone()].into()];
        };

        node.dependencies
            .iter()
            .flat_map(|dep| {
                let mut orderings = if self.insert(target, dep) {
                    self.construct_orderings(dep, dependency_map)
                } else {
                    vec![]
                };

                for ordering in orderings.iter_mut() {
                    ordering.push(target.clone());
                }
                orderings
            })
            .collect()
    }
}

/// Maps an expression --> DependencyNode
///
/// # Debugging / deplaying `DependencyMap`
///
/// This structure implements `Display` to assist debugging. For example:
///
/// ```text
/// DependencyMap: {
///   a@0 ASC --> (target: a@0 ASC, dependencies: [[]])
///   b@1 ASC --> (target: b@1 ASC, dependencies: [[a@0 ASC, c@2 ASC]])
///   c@2 ASC --> (target: c@2 ASC, dependencies: [[b@1 ASC, a@0 ASC]])
///   d@3 ASC --> (target: d@3 ASC, dependencies: [[c@2 ASC, b@1 ASC]])
/// }
/// ```
///
/// # Note on IndexMap Rationale
///
/// Using `IndexMap` (which preserves insert order) to ensure consistent results
/// across different executions for the same query. We could have used `HashSet`
/// and `HashMap` instead without any loss of functionality.
///
/// As an example, if existing orderings are
/// 1. `[a ASC, b ASC]`
/// 2. `[c ASC]`
///
/// Then both the following output orderings are valid
/// 1. `[a ASC, b ASC, c ASC]`
/// 2. `[c ASC, a ASC, b ASC]`
///
/// These are both valid as they are concatenated versions of the alternative
/// orderings. Had we used `HashSet`/`HashMap`, we couldn't guarantee to generate
/// the same result among the possible two results in the example above.
#[derive(Debug, Default)]
pub struct DependencyMap {
    map: IndexMap<PhysicalSortExpr, DependencyNode>,
}

impl DependencyMap {
    /// Insert a new dependency of `sort_expr` (i.e. `dependency`) into the map
    /// along with its target sort expression.
    pub fn insert(
        &mut self,
        sort_expr: PhysicalSortExpr,
        target_sort_expr: Option<PhysicalSortExpr>,
        dependency: Option<PhysicalSortExpr>,
    ) {
        let entry = self.map.entry(sort_expr);
        let node = entry.or_insert_with(|| DependencyNode {
            target: target_sort_expr,
            dependencies: Dependencies::default(),
        });
        node.dependencies.extend(dependency);
    }
}

impl Deref for DependencyMap {
    type Target = IndexMap<PhysicalSortExpr, DependencyNode>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl Display for DependencyMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "DependencyMap: {{")?;
        for (sort_expr, node) in self.map.iter() {
            writeln!(f, "  {sort_expr} --> {node}")?;
        }
        writeln!(f, "}}")
    }
}

/// Represents a node in the dependency map used to construct projected orderings.
///
/// A `DependencyNode` contains information about a particular sort expression,
/// including its target sort expression and a set of dependencies on other sort
/// expressions.
///
/// # Fields
///
/// - `target`: An optional `PhysicalSortExpr` representing the target sort
///   expression associated with the node. It is `None` if the sort expression
///   cannot be projected.
/// - `dependencies`: A [`Dependencies`] containing dependencies on other sort
///   expressions that are referred to by the target sort expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DependencyNode {
    pub(crate) target: Option<PhysicalSortExpr>,
    pub(crate) dependencies: Dependencies,
}

impl Display for DependencyNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(target) = &self.target {
            write!(f, "(target: {target}, ")?;
        } else {
            write!(f, "(")?;
        }
        write!(f, "dependencies: [{}])", self.dependencies)
    }
}

/// This function analyzes the dependency map to collect referred dependencies for
/// a given source expression.
///
/// # Parameters
///
/// - `dependency_map`: A reference to the `DependencyMap` where each
///   `PhysicalSortExpr` is associated with a `DependencyNode`.
/// - `source`: A reference to the source expression (`Arc<dyn PhysicalExpr>`)
///   for which relevant dependencies need to be identified.
///
/// # Returns
///
/// A `Vec<Dependencies>` containing the dependencies for the given source
/// expression. These dependencies are expressions that are referred to by
/// the source expression based on the provided dependency map.
pub fn referred_dependencies(
    dependency_map: &DependencyMap,
    source: &Arc<dyn PhysicalExpr>,
) -> Vec<Dependencies> {
    // Associate `PhysicalExpr`s with `PhysicalSortExpr`s that contain them:
    let mut expr_to_sort_exprs = IndexMap::<_, Dependencies>::new();
    for sort_expr in dependency_map
        .keys()
        .filter(|sort_expr| expr_refers(source, &sort_expr.expr))
    {
        let key = Arc::clone(&sort_expr.expr);
        expr_to_sort_exprs
            .entry(key)
            .or_default()
            .insert(sort_expr.clone());
    }

    // Generate all valid dependencies for the source. For example, if the source
    // is `a + b` and the map is `[a -> (a ASC, a DESC), b -> (b ASC)]`, we get
    // `vec![HashSet(a ASC, b ASC), HashSet(a DESC, b ASC)]`.
    expr_to_sort_exprs
        .into_values()
        .multi_cartesian_product()
        .map(Dependencies::new)
        .collect()
}

/// This function retrieves the dependencies of the given relevant sort expression
/// from the given dependency map. It then constructs prefix orderings by recursively
/// analyzing the dependencies and include them in the orderings.
///
/// # Parameters
///
/// - `relevant_sort_expr`: A reference to the relevant sort expression
///   (`PhysicalSortExpr`) for which prefix orderings are to be constructed.
/// - `dependency_map`: A reference to the `DependencyMap` containing dependencies.
///
/// # Returns
///
/// A vector of prefix orderings (`Vec<LexOrdering>`) based on the given relevant
/// sort expression and its dependencies.
pub fn construct_prefix_orderings(
    relevant_sort_expr: &PhysicalSortExpr,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    let mut dep_enumerator = DependencyEnumerator::new();
    dependency_map
        .get(relevant_sort_expr)
        .expect("no relevant sort expr found")
        .dependencies
        .iter()
        .flat_map(|dep| dep_enumerator.construct_orderings(dep, dependency_map))
        .collect()
}

/// Generates all possible orderings where dependencies are satisfied for the
/// current projection expression.
///
/// # Example
///  If `dependencies` is `a + b ASC` and the dependency map holds dependencies
///  * `a ASC` --> `[c ASC]`
///  * `b ASC` --> `[d DESC]`,
///
/// This function generates these two sort orders
/// * `[c ASC, d DESC, a + b ASC]`
/// * `[d DESC, c ASC, a + b ASC]`
///
/// # Parameters
///
/// * `dependencies` - Set of relevant expressions.
/// * `dependency_map` - Map of dependencies for expressions that may appear in
///   `dependencies`.
///
/// # Returns
///
/// A vector of lexical orderings (`Vec<LexOrdering>`) representing all valid
/// orderings based on the given dependencies.
pub fn generate_dependency_orderings(
    dependencies: &Dependencies,
    dependency_map: &DependencyMap,
) -> Vec<LexOrdering> {
    // Construct all the valid prefix orderings for each expression appearing
    // in the projection. Note that if relevant prefixes are empty, there is no
    // dependency, meaning that dependent is a leading ordering.
    dependencies
        .iter()
        .filter_map(|dep| {
            let prefixes = construct_prefix_orderings(dep, dependency_map);
            (!prefixes.is_empty()).then_some(prefixes)
        })
        // Generate all possible valid orderings:
        .multi_cartesian_product()
        .flat_map(|prefix_orderings| {
            let length = prefix_orderings.len();
            prefix_orderings
                .into_iter()
                .permutations(length)
                .filter_map(|prefixes| {
                    prefixes.into_iter().reduce(|mut acc, ordering| {
                        acc.extend(ordering);
                        acc
                    })
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::equivalence::tests::{
        convert_to_sort_reqs, create_test_params, create_test_schema, output_schema,
        parse_sort_expr,
    };
    use crate::equivalence::{convert_to_sort_exprs, ProjectionMapping};
    use crate::expressions::{col, BinaryExpr, CastExpr, Column};
    use crate::{ConstExpr, EquivalenceProperties, ScalarFunctionExpr};

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion_common::config::ConfigOptions;
    use datafusion_common::{Constraint, Constraints, Result};
    use datafusion_expr::sort_properties::SortProperties;
    use datafusion_expr::Operator;
    use datafusion_functions::string::concat;
    use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
    use datafusion_physical_expr_common::sort_expr::{
        LexRequirement, PhysicalSortRequirement,
    };

    #[test]
    fn project_equivalence_properties_test() -> Result<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let input_properties = EquivalenceProperties::new(Arc::clone(&input_schema));
        let col_a = col("a", &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (Arc::clone(&col_a), "a1".to_string()),
            (Arc::clone(&col_a), "a2".to_string()),
            (Arc::clone(&col_a), "a3".to_string()),
            (Arc::clone(&col_a), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &input_schema)?;

        let out_schema = output_schema(&projection_mapping, &input_schema)?;
        // a as a1, a as a2, a as a3, a as a3
        let proj_exprs = vec![
            (Arc::clone(&col_a), "a1".to_string()),
            (Arc::clone(&col_a), "a2".to_string()),
            (Arc::clone(&col_a), "a3".to_string()),
            (Arc::clone(&col_a), "a4".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &input_schema)?;

        // a as a1, a as a2, a as a3, a as a3
        let col_a1 = &col("a1", &out_schema)?;
        let col_a2 = &col("a2", &out_schema)?;
        let col_a3 = &col("a3", &out_schema)?;
        let col_a4 = &col("a4", &out_schema)?;
        let out_properties = input_properties.project(&projection_mapping, out_schema);

        // At the output a1=a2=a3=a4
        assert_eq!(out_properties.eq_group().len(), 1);
        let eq_class = out_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_class.len(), 4);
        assert!(eq_class.contains(col_a1));
        assert!(eq_class.contains(col_a2));
        assert!(eq_class.contains(col_a3));
        assert!(eq_class.contains(col_a4));

        Ok(())
    }

    #[test]
    fn project_equivalence_properties_test_multi() -> Result<()> {
        // test multiple input orderings with equivalence properties
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int64, true),
        ]));

        let mut input_properties = EquivalenceProperties::new(Arc::clone(&input_schema));
        // add equivalent ordering [a, b, c, d]
        input_properties.add_ordering([
            parse_sort_expr("a", &input_schema),
            parse_sort_expr("b", &input_schema),
            parse_sort_expr("c", &input_schema),
            parse_sort_expr("d", &input_schema),
        ]);

        // add equivalent ordering [a, c, b, d]
        input_properties.add_ordering([
            parse_sort_expr("a", &input_schema),
            parse_sort_expr("c", &input_schema),
            parse_sort_expr("b", &input_schema), // NB b and c are swapped
            parse_sort_expr("d", &input_schema),
        ]);

        // simply project all the columns in order
        let proj_exprs = vec![
            (col("a", &input_schema)?, "a".to_string()),
            (col("b", &input_schema)?, "b".to_string()),
            (col("c", &input_schema)?, "c".to_string()),
            (col("d", &input_schema)?, "d".to_string()),
        ];
        let projection_mapping = ProjectionMapping::try_new(proj_exprs, &input_schema)?;
        let out_properties = input_properties.project(&projection_mapping, input_schema);

        assert_eq!(
            out_properties.to_string(),
            "order: [[a@0 ASC, c@2 ASC, b@1 ASC, d@3 ASC], [a@0 ASC, b@1 ASC, c@2 ASC, d@3 ASC]]"
        );

        Ok(())
    }

    #[test]
    fn test_normalize_ordering_equivalence_classes() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a_expr = col("a", &schema)?;
        let col_b_expr = col("b", &schema)?;
        let col_c_expr = col("c", &schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));

        eq_properties.add_equal_conditions(col_a_expr, Arc::clone(&col_c_expr))?;
        eq_properties.add_orderings([
            vec![PhysicalSortExpr::new_default(Arc::clone(&col_b_expr))],
            vec![PhysicalSortExpr::new_default(Arc::clone(&col_c_expr))],
        ]);

        let mut expected_eqs = EquivalenceProperties::new(Arc::new(schema));
        expected_eqs.add_orderings([
            vec![PhysicalSortExpr::new_default(col_b_expr)],
            vec![PhysicalSortExpr::new_default(col_c_expr)],
        ]);

        assert!(eq_properties.oeq_class().eq(expected_eqs.oeq_class()));
        Ok(())
    }

    #[test]
    fn test_get_indices_of_matching_sort_exprs_with_order_eq() -> Result<()> {
        let sort_options = SortOptions::default();
        let sort_options_not = SortOptions::default().not();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let required_columns = [Arc::clone(&col_b), Arc::clone(&col_a)];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::new(Column::new("b", 1)), sort_options_not),
            PhysicalSortExpr::new(Arc::new(Column::new("a", 0)), sort_options),
        ]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns)?;
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr::new(col_b, sort_options_not),
                PhysicalSortExpr::new(col_a, sort_options),
            ]
        );

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let required_columns = [Arc::clone(&col_b), Arc::clone(&col_a)];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_orderings([
            vec![PhysicalSortExpr::new(
                Arc::new(Column::new("c", 2)),
                sort_options,
            )],
            vec![
                PhysicalSortExpr::new(Arc::new(Column::new("b", 1)), sort_options_not),
                PhysicalSortExpr::new(Arc::new(Column::new("a", 0)), sort_options),
            ],
        ]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns)?;
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr::new(col_b, sort_options_not),
                PhysicalSortExpr::new(col_a, sort_options),
            ]
        );

        let required_columns = [
            Arc::new(Column::new("b", 1)) as _,
            Arc::new(Column::new("a", 0)) as _,
        ];
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));

        // not satisfied orders
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::new(Column::new("b", 1)), sort_options_not),
            PhysicalSortExpr::new(Arc::new(Column::new("c", 2)), sort_options),
            PhysicalSortExpr::new(Arc::new(Column::new("a", 0)), sort_options),
        ]);
        let (_, idxs) = eq_properties.find_longest_permutation(&required_columns)?;
        assert_eq!(idxs, vec![0]);

        Ok(())
    }

    #[test]
    fn test_update_properties() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]);

        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;
        let col_d = col("d", &schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // b=a (e.g they are aliases)
        eq_properties.add_equal_conditions(Arc::clone(&col_b), Arc::clone(&col_a))?;
        // [b ASC], [d ASC]
        eq_properties.add_orderings([
            vec![PhysicalSortExpr::new(Arc::clone(&col_b), option_asc)],
            vec![PhysicalSortExpr::new(Arc::clone(&col_d), option_asc)],
        ]);

        let test_cases = vec![
            // d + b
            (
                Arc::new(BinaryExpr::new(col_d, Operator::Plus, Arc::clone(&col_b))) as _,
                SortProperties::Ordered(option_asc),
            ),
            // b
            (col_b, SortProperties::Ordered(option_asc)),
            // a
            (Arc::clone(&col_a), SortProperties::Ordered(option_asc)),
            // a + c
            (
                Arc::new(BinaryExpr::new(col_a, Operator::Plus, col_c)),
                SortProperties::Unordered,
            ),
        ];
        for (expr, expected) in test_cases {
            let leading_orderings = eq_properties
                .oeq_class()
                .iter()
                .map(|ordering| ordering.first().clone())
                .collect::<Vec<_>>();
            let expr_props = eq_properties.get_expr_properties(Arc::clone(&expr));
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}, leading_orderings: {leading_orderings:?}",
                expr, expected, expr_props.sort_properties
            );
            assert_eq!(expr_props.sort_properties, expected, "{err_msg}");
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation() -> Result<()> {
        // Schema satisfies following orderings:
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        // and
        // Column [a=c] (e.g they are aliases).
        // At below we add [d ASC, h DESC] also, for test purposes
        let (test_schema, mut eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_h = &col("h", &test_schema)?;
        // a + d
        let a_plus_d = Arc::new(BinaryExpr::new(
            Arc::clone(col_a),
            Operator::Plus,
            Arc::clone(col_d),
        )) as _;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // [d ASC, h DESC] also satisfies schema.
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::clone(col_d), option_asc),
            PhysicalSortExpr::new(Arc::clone(col_h), option_desc),
        ]);
        let test_cases = vec![
            // TEST CASE 1
            (vec![col_a], vec![(col_a, option_asc)]),
            // TEST CASE 2
            (vec![col_c], vec![(col_c, option_asc)]),
            // TEST CASE 3
            (
                vec![col_d, col_e, col_b],
                vec![
                    (col_d, option_asc),
                    (col_e, option_desc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 4
            (vec![col_b], vec![]),
            // TEST CASE 5
            (vec![col_d], vec![(col_d, option_asc)]),
            // TEST CASE 5
            (vec![&a_plus_d], vec![(&a_plus_d, option_asc)]),
            // TEST CASE 6
            (
                vec![col_b, col_d],
                vec![(col_d, option_asc), (col_b, option_asc)],
            ),
            // TEST CASE 6
            (
                vec![col_c, col_e],
                vec![(col_c, option_asc), (col_e, option_desc)],
            ),
            // TEST CASE 7
            (
                vec![col_d, col_h, col_e, col_f, col_b],
                vec![
                    (col_d, option_asc),
                    (col_e, option_desc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 8
            (
                vec![col_e, col_d, col_h, col_f, col_b],
                vec![
                    (col_e, option_desc),
                    (col_d, option_asc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                    (col_b, option_asc),
                ],
            ),
            // TEST CASE 9
            (
                vec![col_e, col_d, col_b, col_h, col_f],
                vec![
                    (col_e, option_desc),
                    (col_d, option_asc),
                    (col_b, option_asc),
                    (col_h, option_desc),
                    (col_f, option_asc),
                ],
            ),
        ];
        for (exprs, expected) in test_cases {
            let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = convert_to_sort_exprs(&expected);
            let (actual, _) = eq_properties.find_longest_permutation(&exprs)?;
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation2() -> Result<()> {
        // Schema satisfies following orderings:
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        // and
        // Column [a=c] (e.g they are aliases).
        // At below we add [d ASC, h DESC] also, for test purposes
        let (test_schema, mut eq_properties) = create_test_params()?;
        let col_h = &col("h", &test_schema)?;

        // Add column h as constant
        eq_properties.add_constants(vec![ConstExpr::from(Arc::clone(col_h))])?;

        let test_cases = vec![
            // TEST CASE 1
            // ordering of the constants are treated as default ordering.
            // This is the convention currently used.
            (vec![col_h], vec![(col_h, SortOptions::default())]),
        ];
        for (exprs, expected) in test_cases {
            let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = convert_to_sort_exprs(&expected);
            let (actual, _) = eq_properties.find_longest_permutation(&exprs)?;
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_normalize_sort_reqs() -> Result<()> {
        // Schema satisfies following properties
        // a=c
        // and following orderings are valid
        // [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First element in the tuple stores vector of requirement, second element is the expected return value for ordering_satisfy function
        let requirements = vec![
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (
                vec![(col_a, Some(option_desc))],
                vec![(col_a, Some(option_desc))],
            ),
            (vec![(col_a, None)], vec![(col_a, None)]),
            // Test whether equivalence works as expected
            (
                vec![(col_c, Some(option_asc))],
                vec![(col_a, Some(option_asc))],
            ),
            (vec![(col_c, None)], vec![(col_a, None)]),
            // Test whether ordering equivalence works as expected
            (
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_d, Some(option_asc)), (col_b, Some(option_asc))],
            ),
            (
                vec![(col_d, None), (col_b, None)],
                vec![(col_d, None), (col_b, None)],
            ),
            (
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
                vec![(col_e, Some(option_desc)), (col_f, Some(option_asc))],
            ),
            // We should be able to normalize in compatible requirements also (not exactly equal)
            (
                vec![(col_e, Some(option_desc)), (col_f, None)],
                vec![(col_e, Some(option_desc)), (col_f, None)],
            ),
            (
                vec![(col_e, None), (col_f, None)],
                vec![(col_e, None), (col_f, None)],
            ),
        ];

        for (reqs, expected_normalized) in requirements.into_iter() {
            let req = convert_to_sort_reqs(&reqs);
            let expected_normalized = convert_to_sort_reqs(&expected_normalized);

            assert_eq!(
                eq_properties.normalize_sort_requirements(req).unwrap(),
                expected_normalized
            );
        }

        Ok(())
    }

    #[test]
    fn test_schema_normalize_sort_requirement_with_equivalence() -> Result<()> {
        let option1 = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Assume that column a and c are aliases.
        let (test_schema, eq_properties) = create_test_params()?;
        let col_a = &col("a", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;

        // Test cases for equivalence normalization
        // First entry in the tuple is PhysicalSortRequirement, second entry in the tuple is
        // expected PhysicalSortRequirement after normalization.
        let test_cases = vec![
            (vec![(col_a, Some(option1))], vec![(col_a, Some(option1))]),
            // In the normalized version column c should be replace with column a
            (vec![(col_c, Some(option1))], vec![(col_a, Some(option1))]),
            (vec![(col_c, None)], vec![(col_a, None)]),
            (vec![(col_d, Some(option1))], vec![(col_d, Some(option1))]),
        ];
        for (reqs, expected) in test_cases.into_iter() {
            let reqs = convert_to_sort_reqs(&reqs);
            let expected = convert_to_sort_reqs(&expected);
            let normalized = eq_properties
                .normalize_sort_requirements(reqs.clone())
                .unwrap();
            assert!(
                expected.eq(&normalized),
                "error in test: reqs: {reqs:?}, expected: {expected:?}, normalized: {normalized:?}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_eliminate_redundant_monotonic_sorts() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Date32, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ]));
        let mut base_properties = EquivalenceProperties::new(Arc::clone(&schema));
        base_properties.reorder(
            ["a", "b", "c"]
                .into_iter()
                .map(|c| PhysicalSortExpr::new_default(col(c, schema.as_ref()).unwrap())),
        )?;

        struct TestCase {
            name: &'static str,
            constants: Vec<Arc<dyn PhysicalExpr>>,
            equal_conditions: Vec<[Arc<dyn PhysicalExpr>; 2]>,
            sort_columns: &'static [&'static str],
            should_satisfy_ordering: bool,
        }

        let col_a = col("a", schema.as_ref())?;
        let col_b = col("b", schema.as_ref())?;
        let col_c = col("c", schema.as_ref())?;
        let cast_c = Arc::new(CastExpr::new(col_c, DataType::Date32, None)) as _;

        let cases = vec![
            TestCase {
                name: "(a, b, c) -> (c)",
                // b is constant, so it should be removed from the sort order
                constants: vec![Arc::clone(&col_b)],
                equal_conditions: vec![[Arc::clone(&cast_c), Arc::clone(&col_a)]],
                sort_columns: &["c"],
                should_satisfy_ordering: true,
            },
            // Same test with above test, where equality order is swapped.
            // Algorithm shouldn't depend on this order.
            TestCase {
                name: "(a, b, c) -> (c)",
                // b is constant, so it should be removed from the sort order
                constants: vec![col_b],
                equal_conditions: vec![[Arc::clone(&col_a), Arc::clone(&cast_c)]],
                sort_columns: &["c"],
                should_satisfy_ordering: true,
            },
            TestCase {
                name: "not ordered because (b) is not constant",
                // b is not constant anymore
                constants: vec![],
                // a and c are still compatible, but this is irrelevant since the original ordering is (a, b, c)
                equal_conditions: vec![[Arc::clone(&cast_c), Arc::clone(&col_a)]],
                sort_columns: &["c"],
                should_satisfy_ordering: false,
            },
        ];

        for case in cases {
            // Construct the equivalence properties in different orders
            // to exercise different code paths
            // (The resulting properties _should_ be the same)
            for properties in [
                // Equal conditions before constants
                {
                    let mut properties = base_properties.clone();
                    for [left, right] in case.equal_conditions.clone() {
                        properties.add_equal_conditions(left, right)?
                    }
                    properties.add_constants(
                        case.constants.iter().cloned().map(ConstExpr::from),
                    )?;
                    properties
                },
                // Constants before equal conditions
                {
                    let mut properties = base_properties.clone();
                    properties.add_constants(
                        case.constants.iter().cloned().map(ConstExpr::from),
                    )?;
                    for [left, right] in case.equal_conditions {
                        properties.add_equal_conditions(left, right)?
                    }
                    properties
                },
            ] {
                let sort = case
                    .sort_columns
                    .iter()
                    .map(|&name| col(name, &schema).map(PhysicalSortExpr::new_default))
                    .collect::<Result<Vec<_>>>()?;

                assert_eq!(
                    properties.ordering_satisfy(sort)?,
                    case.should_satisfy_ordering,
                    "failed test '{}'",
                    case.name
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_ordering_equivalence_with_lex_monotonic_concat() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        let a_concat_b: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            "concat",
            concat(),
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            Field::new("f", DataType::Utf8, true).into(),
            Arc::new(ConfigOptions::default()),
        ));

        // Assume existing ordering is [c ASC, a ASC, b ASC]
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        eq_properties.add_ordering([
            PhysicalSortExpr::new_default(Arc::clone(&col_c)).asc(),
            PhysicalSortExpr::new_default(Arc::clone(&col_a)).asc(),
            PhysicalSortExpr::new_default(Arc::clone(&col_b)).asc(),
        ]);

        // Add equality condition c = concat(a, b)
        eq_properties.add_equal_conditions(Arc::clone(&col_c), a_concat_b)?;

        let orderings = eq_properties.oeq_class();

        let expected_ordering1 = [PhysicalSortExpr::new_default(col_c).asc()].into();
        let expected_ordering2 = [
            PhysicalSortExpr::new_default(col_a).asc(),
            PhysicalSortExpr::new_default(col_b).asc(),
        ]
        .into();

        // The ordering should be [c ASC] and [a ASC, b ASC]
        assert_eq!(orderings.len(), 2);
        assert!(orderings.contains(&expected_ordering1));
        assert!(orderings.contains(&expected_ordering2));

        Ok(())
    }

    #[test]
    fn test_ordering_equivalence_with_non_lex_monotonic_multiply() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        let a_times_b = Arc::new(BinaryExpr::new(
            Arc::clone(&col_a),
            Operator::Multiply,
            Arc::clone(&col_b),
        )) as _;

        // Assume existing ordering is [c ASC, a ASC, b ASC]
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let initial_ordering: LexOrdering = [
            PhysicalSortExpr::new_default(Arc::clone(&col_c)).asc(),
            PhysicalSortExpr::new_default(col_a).asc(),
            PhysicalSortExpr::new_default(col_b).asc(),
        ]
        .into();

        eq_properties.add_ordering(initial_ordering.clone());

        // Add equality condition c = a * b
        eq_properties.add_equal_conditions(col_c, a_times_b)?;

        let orderings = eq_properties.oeq_class();

        // The ordering should remain unchanged since multiplication is not lex-monotonic
        assert_eq!(orderings.len(), 1);
        assert!(orderings.contains(&initial_ordering));

        Ok(())
    }

    #[test]
    fn test_ordering_equivalence_with_concat_equality() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Utf8, false),
        ]));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        let a_concat_b = Arc::new(ScalarFunctionExpr::new(
            "concat",
            concat(),
            vec![Arc::clone(&col_a), Arc::clone(&col_b)],
            Field::new("f", DataType::Utf8, true).into(),
            Arc::new(ConfigOptions::default()),
        )) as _;

        // Assume existing ordering is [concat(a, b) ASC, a ASC, b ASC]
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        eq_properties.add_ordering([
            PhysicalSortExpr::new_default(Arc::clone(&a_concat_b)).asc(),
            PhysicalSortExpr::new_default(Arc::clone(&col_a)).asc(),
            PhysicalSortExpr::new_default(Arc::clone(&col_b)).asc(),
        ]);

        // Add equality condition c = concat(a, b)
        eq_properties.add_equal_conditions(col_c, Arc::clone(&a_concat_b))?;

        let orderings = eq_properties.oeq_class();

        let expected_ordering1 = [PhysicalSortExpr::new_default(a_concat_b).asc()].into();
        let expected_ordering2 = [
            PhysicalSortExpr::new_default(col_a).asc(),
            PhysicalSortExpr::new_default(col_b).asc(),
        ]
        .into();

        // The ordering should be [c ASC] and [a ASC, b ASC]
        assert_eq!(orderings.len(), 2);
        assert!(orderings.contains(&expected_ordering1));
        assert!(orderings.contains(&expected_ordering2));

        Ok(())
    }

    #[test]
    fn test_requirements_compatible() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        let eq_properties = EquivalenceProperties::new(schema);
        let lex_a: LexRequirement =
            [PhysicalSortRequirement::new(Arc::clone(&col_a), None)].into();
        let lex_a_b: LexRequirement = [
            PhysicalSortRequirement::new(col_a, None),
            PhysicalSortRequirement::new(col_b, None),
        ]
        .into();
        let lex_c = [PhysicalSortRequirement::new(col_c, None)].into();

        assert!(eq_properties.requirements_compatible(lex_a.clone(), lex_a.clone()));
        assert!(!eq_properties.requirements_compatible(lex_a.clone(), lex_a_b.clone()));
        assert!(eq_properties.requirements_compatible(lex_a_b, lex_a.clone()));
        assert!(!eq_properties.requirements_compatible(lex_c, lex_a));

        Ok(())
    }

    #[test]
    fn test_with_reorder_constant_filtering() -> Result<()> {
        let schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        // Setup constant columns
        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        eq_properties.add_constants([ConstExpr::from(Arc::clone(&col_a))])?;

        let sort_exprs = vec![
            PhysicalSortExpr::new_default(Arc::clone(&col_a)),
            PhysicalSortExpr::new_default(Arc::clone(&col_b)),
        ];

        let change = eq_properties.reorder(sort_exprs)?;
        assert!(change);

        assert_eq!(eq_properties.oeq_class().len(), 1);
        let ordering = eq_properties.oeq_class().iter().next().unwrap();
        assert_eq!(ordering.len(), 2);
        assert!(ordering[0].expr.eq(&col_a));
        assert!(ordering[1].expr.eq(&col_b));

        Ok(())
    }

    #[test]
    fn test_with_reorder_preserve_suffix() -> Result<()> {
        let schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        let asc = SortOptions::default();
        let desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // Initial ordering: [a ASC, b DESC, c ASC]
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::clone(&col_a), asc),
            PhysicalSortExpr::new(Arc::clone(&col_b), desc),
            PhysicalSortExpr::new(Arc::clone(&col_c), asc),
        ]);

        // New ordering: [a ASC]
        let new_order = vec![PhysicalSortExpr::new(Arc::clone(&col_a), asc)];

        let change = eq_properties.reorder(new_order)?;
        assert!(!change);

        // Should only contain [a ASC, b DESC, c ASC]
        assert_eq!(eq_properties.oeq_class().len(), 1);
        let ordering = eq_properties.oeq_class().iter().next().unwrap();
        assert_eq!(ordering.len(), 3);
        assert!(ordering[0].expr.eq(&col_a));
        assert!(ordering[0].options.eq(&asc));
        assert!(ordering[1].expr.eq(&col_b));
        assert!(ordering[1].options.eq(&desc));
        assert!(ordering[2].expr.eq(&col_c));
        assert!(ordering[2].options.eq(&asc));

        Ok(())
    }

    #[test]
    fn test_with_reorder_equivalent_expressions() -> Result<()> {
        let schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;

        // Make a and b equivalent
        eq_properties.add_equal_conditions(Arc::clone(&col_a), Arc::clone(&col_b))?;

        // Initial ordering: [a ASC, c ASC]
        eq_properties.add_ordering([
            PhysicalSortExpr::new_default(Arc::clone(&col_a)),
            PhysicalSortExpr::new_default(Arc::clone(&col_c)),
        ]);

        // New ordering: [b ASC]
        let new_order = vec![PhysicalSortExpr::new_default(Arc::clone(&col_b))];

        let change = eq_properties.reorder(new_order)?;

        assert!(!change);
        // Should only contain [a/b ASC, c ASC]
        assert_eq!(eq_properties.oeq_class().len(), 1);

        // Verify orderings
        let asc = SortOptions::default();
        let ordering = eq_properties.oeq_class().iter().next().unwrap();
        assert_eq!(ordering.len(), 2);
        assert!(ordering[0].expr.eq(&col_a) || ordering[0].expr.eq(&col_b));
        assert!(ordering[0].options.eq(&asc));
        assert!(ordering[1].expr.eq(&col_c));
        assert!(ordering[1].options.eq(&asc));

        Ok(())
    }

    #[test]
    fn test_with_reorder_incompatible_prefix() -> Result<()> {
        let schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;

        let asc = SortOptions::default();
        let desc = SortOptions {
            descending: true,
            nulls_first: true,
        };

        // Initial ordering: [a ASC, b DESC]
        eq_properties.add_ordering([
            PhysicalSortExpr::new(Arc::clone(&col_a), asc),
            PhysicalSortExpr::new(Arc::clone(&col_b), desc),
        ]);

        // New ordering: [a DESC]
        let new_order = vec![PhysicalSortExpr::new(Arc::clone(&col_a), desc)];

        let change = eq_properties.reorder(new_order.clone())?;

        assert!(change);
        // Should only contain the new ordering since options don't match
        assert_eq!(eq_properties.oeq_class().len(), 1);
        let ordering = eq_properties.oeq_class().iter().next().unwrap();
        assert_eq!(ordering.to_vec(), new_order);

        Ok(())
    }

    #[test]
    fn test_with_reorder_comprehensive() -> Result<()> {
        let schema = create_test_schema()?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&schema));

        let col_a = col("a", &schema)?;
        let col_b = col("b", &schema)?;
        let col_c = col("c", &schema)?;
        let col_d = col("d", &schema)?;
        let col_e = col("e", &schema)?;

        // Constants: c is constant
        eq_properties.add_constants([ConstExpr::from(Arc::clone(&col_c))])?;

        // Equality: b = d
        eq_properties.add_equal_conditions(Arc::clone(&col_b), Arc::clone(&col_d))?;

        // Orderings: [d ASC, a ASC], [e ASC]
        eq_properties.add_orderings([
            vec![
                PhysicalSortExpr::new_default(Arc::clone(&col_d)),
                PhysicalSortExpr::new_default(Arc::clone(&col_a)),
            ],
            vec![PhysicalSortExpr::new_default(Arc::clone(&col_e))],
        ]);

        // New ordering: [b ASC, c ASC]
        let new_order = vec![
            PhysicalSortExpr::new_default(Arc::clone(&col_b)),
            PhysicalSortExpr::new_default(Arc::clone(&col_c)),
        ];

        let old_orderings = eq_properties.oeq_class().clone();
        let change = eq_properties.reorder(new_order)?;
        // Original orderings should be preserved:
        assert!(!change);
        assert_eq!(eq_properties.oeq_class, old_orderings);

        Ok(())
    }

    #[test]
    fn test_ordering_satisfaction_with_key_constraints() -> Result<()> {
        let pk_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]));

        let unique_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]));

        // Test cases to run
        let test_cases = vec![
            // (name, schema, constraint, base_ordering, satisfied_orderings, unsatisfied_orderings)
            (
                "single column primary key",
                &pk_schema,
                vec![Constraint::PrimaryKey(vec![0])],
                vec!["a"], // base ordering
                vec![vec!["a", "b"], vec!["a", "c", "d"]],
                vec![vec!["b", "a"], vec!["c", "a"]],
            ),
            (
                "single column unique",
                &unique_schema,
                vec![Constraint::Unique(vec![0])],
                vec!["a"], // base ordering
                vec![vec!["a", "b"], vec!["a", "c", "d"]],
                vec![vec!["b", "a"], vec!["c", "a"]],
            ),
            (
                "multi-column primary key",
                &pk_schema,
                vec![Constraint::PrimaryKey(vec![0, 1])],
                vec!["a", "b"], // base ordering
                vec![vec!["a", "b", "c"], vec!["a", "b", "d"]],
                vec![vec!["b", "a"], vec!["a", "c", "b"]],
            ),
            (
                "multi-column unique",
                &unique_schema,
                vec![Constraint::Unique(vec![0, 1])],
                vec!["a", "b"], // base ordering
                vec![vec!["a", "b", "c"], vec!["a", "b", "d"]],
                vec![vec!["b", "a"], vec!["c", "a", "b"]],
            ),
            (
                "nullable unique",
                &unique_schema,
                vec![Constraint::Unique(vec![2, 3])],
                vec!["c", "d"], // base ordering
                vec![],
                vec![vec!["c", "d", "a"]],
            ),
            (
                "ordering with arbitrary column unique",
                &unique_schema,
                vec![Constraint::Unique(vec![0, 1])],
                vec!["a", "c", "b"], // base ordering
                vec![vec!["a", "c", "b", "d"]],
                vec![vec!["a", "b", "d"]],
            ),
            (
                "ordering with arbitrary column pk",
                &pk_schema,
                vec![Constraint::PrimaryKey(vec![0, 1])],
                vec!["a", "c", "b"], // base ordering
                vec![vec!["a", "c", "b", "d"]],
                vec![vec!["a", "b", "d"]],
            ),
            (
                "ordering with arbitrary column pk complex",
                &pk_schema,
                vec![Constraint::PrimaryKey(vec![3, 1])],
                vec!["b", "a", "d"], // base ordering
                vec![vec!["b", "a", "d", "c"]],
                vec![vec!["b", "c", "d", "a"], vec!["b", "a", "c", "d"]],
            ),
        ];

        for (
            name,
            schema,
            constraints,
            base_order,
            satisfied_orders,
            unsatisfied_orders,
        ) in test_cases
        {
            let mut eq_properties = EquivalenceProperties::new(Arc::clone(schema));

            // Convert string column names to orderings
            let satisfied_orderings: Vec<_> = satisfied_orders
                .iter()
                .map(|cols| {
                    cols.iter()
                        .map(|col_name| {
                            PhysicalSortExpr::new_default(col(col_name, schema).unwrap())
                        })
                        .collect::<Vec<_>>()
                })
                .collect();

            let unsatisfied_orderings: Vec<_> = unsatisfied_orders
                .iter()
                .map(|cols| {
                    cols.iter()
                        .map(|col_name| {
                            PhysicalSortExpr::new_default(col(col_name, schema).unwrap())
                        })
                        .collect::<Vec<_>>()
                })
                .collect();

            // Test that orderings are not satisfied before adding constraints
            for ordering in satisfied_orderings.clone() {
                let err_msg = format!(
                    "{name}: ordering {ordering:?} should not be satisfied before adding constraints",
                );
                assert!(!eq_properties.ordering_satisfy(ordering)?, "{err_msg}");
            }

            // Add base ordering
            let base_ordering = base_order.iter().map(|col_name| PhysicalSortExpr {
                expr: col(col_name, schema).unwrap(),
                options: SortOptions::default(),
            });
            eq_properties.add_ordering(base_ordering);

            // Add constraints
            eq_properties =
                eq_properties.with_constraints(Constraints::new_unverified(constraints));

            // Test that expected orderings are now satisfied
            for ordering in satisfied_orderings {
                let err_msg = format!(
                    "{name}: ordering {ordering:?} should be satisfied after adding constraints",
                );
                assert!(eq_properties.ordering_satisfy(ordering)?, "{err_msg}");
            }

            // Test that unsatisfied orderings remain unsatisfied
            for ordering in unsatisfied_orderings {
                let err_msg = format!(
                    "{name}: ordering {ordering:?} should not be satisfied after adding constraints",
                );
                assert!(!eq_properties.ordering_satisfy(ordering)?, "{err_msg}");
            }
        }

        Ok(())
    }
}
