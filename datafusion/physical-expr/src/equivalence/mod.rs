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

use std::borrow::Borrow;
use std::sync::Arc;

use crate::PhysicalExpr;

use arrow::compute::SortOptions;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};

mod class;
mod grouping;
mod ordering;
mod properties;

pub use class::{AcrossPartitions, ConstExpr, EquivalenceClass, EquivalenceGroup};
pub use grouping::GroupingEquivalenceClass;
pub use ordering::OrderingEquivalenceClass;
// Re-export for backwards compatibility, we recommend importing from
// datafusion_physical_expr::projection instead
pub use crate::projection::{ProjectionMapping, project_ordering, project_orderings};
pub use properties::{
    EquivalenceProperties, calculate_union, join_equivalence_properties,
};

// Convert each tuple to a `PhysicalSortExpr` and construct a vector.
pub fn convert_to_sort_exprs<T: Borrow<Arc<dyn PhysicalExpr>>>(
    args: &[(T, SortOptions)],
) -> Vec<PhysicalSortExpr> {
    args.iter()
        .map(|(expr, options)| PhysicalSortExpr::new(Arc::clone(expr.borrow()), *options))
        .collect()
}

// Convert each vector of tuples to a `LexOrdering`.
pub fn convert_to_orderings<T: Borrow<Arc<dyn PhysicalExpr>>>(
    args: &[Vec<(T, SortOptions)>],
) -> Vec<LexOrdering> {
    args.iter()
        .filter_map(|sort_exprs| LexOrdering::new(convert_to_sort_exprs(sort_exprs)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::{BinaryExpr, Column, cast, col, lit};
    use crate::{LexRequirement, PhysicalSortExpr};

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::Result;
    use datafusion_expr::Operator;
    use datafusion_physical_expr_common::sort_expr::PhysicalSortRequirement;

    /// Converts a string to a physical sort expression
    ///
    /// # Example
    /// * `"a"` -> (`"a"`, `SortOptions::default()`)
    /// * `"a ASC"` -> (`"a"`, `SortOptions { descending: false, nulls_first: false }`)
    pub fn parse_sort_expr(name: &str, schema: &SchemaRef) -> PhysicalSortExpr {
        let mut parts = name.split_whitespace();
        let name = parts.next().expect("empty sort expression");
        let mut sort_expr = PhysicalSortExpr::new(
            col(name, schema).expect("invalid column name"),
            SortOptions::default(),
        );

        if let Some(options) = parts.next() {
            sort_expr = match options {
                "ASC" => sort_expr.asc(),
                "DESC" => sort_expr.desc(),
                _ => panic!(
                    "unknown sort options. Expected 'ASC' or 'DESC', got {options}"
                ),
            }
        }

        assert!(
            parts.next().is_none(),
            "unexpected tokens in column name. Expected 'name' / 'name ASC' / 'name DESC' but got  '{name}'"
        );

        sort_expr
    }

    // Generate a schema which consists of 8 columns (a, b, c, d, e, f, g, h)
    pub fn create_test_schema() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Int32, true);
        let b = Field::new("b", DataType::Int32, true);
        let c = Field::new("c", DataType::Int32, true);
        let d = Field::new("d", DataType::Int32, true);
        let e = Field::new("e", DataType::Int32, true);
        let f = Field::new("f", DataType::Int32, true);
        let g = Field::new("g", DataType::Int32, true);
        let h = Field::new("h", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f, g, h]));

        Ok(schema)
    }

    /// Construct a schema with following properties
    /// Schema satisfies following orderings:
    /// [a ASC], [d ASC, b ASC], [e DESC, f ASC, g ASC]
    /// and
    /// Column [a=c] (e.g they are aliases).
    pub fn create_test_params() -> Result<(SchemaRef, EquivalenceProperties)> {
        let test_schema = create_test_schema()?;
        let col_a = col("a", &test_schema)?;
        let col_b = col("b", &test_schema)?;
        let col_c = col("c", &test_schema)?;
        let col_d = col("d", &test_schema)?;
        let col_e = col("e", &test_schema)?;
        let col_f = col("f", &test_schema)?;
        let col_g = col("g", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
        eq_properties.add_equal_conditions(Arc::clone(&col_a), Arc::clone(&col_c))?;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let orderings = vec![
            // [a ASC]
            vec![(col_a, option_asc)],
            // [d ASC, b ASC]
            vec![(col_d, option_asc), (col_b, option_asc)],
            // [e DESC, f ASC, g ASC]
            vec![
                (col_e, option_desc),
                (col_f, option_asc),
                (col_g, option_asc),
            ],
        ];
        let orderings = convert_to_orderings(&orderings);
        eq_properties.add_orderings(orderings);
        Ok((test_schema, eq_properties))
    }

    // Convert each tuple to a `PhysicalSortRequirement` and construct a
    // a `LexRequirement` from them.
    pub fn convert_to_sort_reqs(
        args: &[(&Arc<dyn PhysicalExpr>, Option<SortOptions>)],
    ) -> LexRequirement {
        let exprs = args.iter().map(|(expr, options)| {
            PhysicalSortRequirement::new(Arc::clone(*expr), *options)
        });
        LexRequirement::new(exprs).unwrap()
    }

    #[test]
    fn add_equal_conditions_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Int64, true),
        ]));

        let mut eq_properties = EquivalenceProperties::new(schema);
        let col_a = Arc::new(Column::new("a", 0)) as _;
        let col_b = Arc::new(Column::new("b", 1)) as _;
        let col_c = Arc::new(Column::new("c", 2)) as _;
        let col_x = Arc::new(Column::new("x", 3)) as _;
        let col_y = Arc::new(Column::new("y", 4)) as _;

        // a and b are aliases
        eq_properties.add_equal_conditions(Arc::clone(&col_a), Arc::clone(&col_b))?;
        assert_eq!(eq_properties.eq_group().len(), 1);

        // This new entry is redundant, size shouldn't increase
        eq_properties.add_equal_conditions(Arc::clone(&col_b), Arc::clone(&col_a))?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 2);
        assert!(eq_groups.contains(&col_a));
        assert!(eq_groups.contains(&col_b));

        // b and c are aliases. Existing equivalence class should expand,
        // however there shouldn't be any new equivalence class
        eq_properties.add_equal_conditions(Arc::clone(&col_b), Arc::clone(&col_c))?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 3);
        assert!(eq_groups.contains(&col_a));
        assert!(eq_groups.contains(&col_b));
        assert!(eq_groups.contains(&col_c));

        // This is a new set of equality. Hence equivalent class count should be 2.
        eq_properties.add_equal_conditions(Arc::clone(&col_x), Arc::clone(&col_y))?;
        assert_eq!(eq_properties.eq_group().len(), 2);

        // This equality bridges distinct equality sets.
        // Hence equivalent class count should decrease from 2 to 1.
        eq_properties.add_equal_conditions(Arc::clone(&col_x), Arc::clone(&col_a))?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 5);
        assert!(eq_groups.contains(&col_a));
        assert!(eq_groups.contains(&col_b));
        assert!(eq_groups.contains(&col_c));
        assert!(eq_groups.contains(&col_x));
        assert!(eq_groups.contains(&col_y));

        Ok(())
    }

    #[test]
    fn grouping_satisfy_requires_the_complete_tuple() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let c = col("c", &schema)?;
        let mut properties = EquivalenceProperties::new(schema);
        properties.add_grouping([Arc::clone(&a), Arc::clone(&b)]);

        assert!(properties.grouping_satisfy([Arc::clone(&a), Arc::clone(&b)])?);
        assert!(properties.grouping_satisfy([Arc::clone(&b), Arc::clone(&a)])?);
        assert!(!properties.grouping_satisfy([Arc::clone(&a)])?);
        assert!(!properties.grouping_satisfy([Arc::clone(&b)])?);
        assert!(!properties.grouping_satisfy([a, b, c])?);
        Ok(())
    }

    #[test]
    fn ordering_implies_grouped_prefixes() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let c = col("c", &schema)?;
        let properties = EquivalenceProperties::new_with_orderings(
            schema,
            [[
                PhysicalSortExpr::new_default(Arc::clone(&a)),
                PhysicalSortExpr::new_default(Arc::clone(&b)),
                PhysicalSortExpr::new_default(Arc::clone(&c)),
            ]],
        );

        assert!(properties.grouping_satisfy([Arc::clone(&a)])?);
        assert!(properties.grouping_satisfy([Arc::clone(&a), Arc::clone(&b)])?);
        assert!(properties.grouping_satisfy([Arc::clone(&b), Arc::clone(&a)])?);
        assert!(!properties.grouping_satisfy([Arc::clone(&b)])?);
        assert!(!properties.grouping_satisfy([a, c])?);
        Ok(())
    }

    #[test]
    fn equivalent_orderings_can_jointly_satisfy_grouping() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let properties = EquivalenceProperties::new_with_orderings(
            schema,
            [
                [PhysicalSortExpr::new_default(Arc::clone(&a))],
                [PhysicalSortExpr::new_default(Arc::clone(&b))],
            ],
        );

        assert!(properties.grouping_satisfy([b, a])?);
        Ok(())
    }

    #[test]
    fn reorder_clears_explicit_grouping() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let ordering = [PhysicalSortExpr::new_default(Arc::clone(&b))];
        let mut properties =
            EquivalenceProperties::new_with_orderings(schema, [ordering.clone()]);
        properties.add_grouping([Arc::clone(&a)]);

        // The ordering is already satisfied, but a physical sort may still
        // rearrange equal `b` values and separate an `a` group.
        assert!(!properties.reorder(ordering)?);
        assert!(!properties.grouping_satisfy([a])?);
        assert!(properties.grouping_satisfy([b])?);
        Ok(())
    }

    #[test]
    fn grouping_normalizes_equivalences_and_constants() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let c = col("c", &schema)?;
        let mut properties = EquivalenceProperties::new(schema);
        properties.add_equal_conditions(Arc::clone(&a), Arc::clone(&c))?;
        properties.add_constants([ConstExpr::from(Arc::clone(&a))])?;
        properties.add_grouping([Arc::clone(&a), Arc::clone(&b)]);

        assert!(properties.grouping_satisfy([Arc::clone(&b)])?);
        assert!(properties.grouping_satisfy([Arc::clone(&b), Arc::clone(&c)])?);

        properties.clear_per_partition_constants();
        assert!(!properties.grouping_satisfy([Arc::clone(&b)])?);
        assert!(properties.grouping_satisfy([b, c])?);
        Ok(())
    }

    #[test]
    fn project_grouping_is_all_or_nothing() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_grouping([Arc::clone(&a), Arc::clone(&b)]);

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]));
        let mapping = ProjectionMapping::try_new(
            [
                (Arc::clone(&b), "x".to_string()),
                (Arc::clone(&a), "y".to_string()),
            ],
            &schema,
        )?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));
        let x = col("x", &output_schema)?;
        let y = col("y", &output_schema)?;
        assert!(projected.grouping_satisfy([x, y])?);

        let output_schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let mapping =
            ProjectionMapping::try_new([(Arc::clone(&a), "x".to_string())], &schema)?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));
        assert!(projected.geq_class().is_empty());
        assert!(!projected.grouping_satisfy([col("x", &output_schema)?])?);
        Ok(())
    }

    #[test]
    fn project_grouping_omits_constant_members() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let b = col("b", &schema)?;
        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_constants([ConstExpr::from(Arc::clone(&a))])?;
        properties.add_grouping([a, Arc::clone(&b)]);

        let output_schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let mapping = ProjectionMapping::try_new([(b, "x".to_string())], &schema)?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));

        assert!(projected.grouping_satisfy([col("x", &output_schema)?])?);
        Ok(())
    }

    #[test]
    fn project_grouped_expression_requires_an_injective_mapping() -> Result<()> {
        let schema = create_test_schema()?;
        let a = col("a", &schema)?;
        let mut properties = EquivalenceProperties::new(Arc::clone(&schema));
        properties.add_grouping([Arc::clone(&a)]);

        let widened = cast(Arc::clone(&a), &schema, DataType::Int64)?;
        let output_schema =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let mapping = ProjectionMapping::try_new([(widened, "x".to_string())], &schema)?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));
        assert!(projected.grouping_satisfy([col("x", &output_schema)?])?);

        let greater_than_one = Arc::new(BinaryExpr::new(a, Operator::Gt, lit(1_i32)))
            as Arc<dyn PhysicalExpr>;
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "is_greater",
            DataType::Boolean,
            true,
        )]));
        let mapping = ProjectionMapping::try_new(
            [(greater_than_one, "is_greater".to_string())],
            &schema,
        )?;
        let projected = properties.project(&mapping, Arc::clone(&output_schema));
        assert!(projected.geq_class().is_empty());
        Ok(())
    }
}
