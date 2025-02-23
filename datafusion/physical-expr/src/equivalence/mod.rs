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

use std::sync::Arc;

use crate::expressions::Column;
use crate::{LexRequirement, PhysicalExpr};

use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};

mod class;
mod ordering;
mod projection;
mod properties;

pub use class::{AcrossPartitions, ConstExpr, EquivalenceClass, EquivalenceGroup};
pub use ordering::OrderingEquivalenceClass;
pub use projection::ProjectionMapping;
pub use properties::{
    calculate_union, join_equivalence_properties, EquivalenceProperties,
};

/// This function constructs a duplicate-free `LexOrderingReq` by filtering out
/// duplicate entries that have same physical expression inside. For example,
/// `vec![a Some(ASC), a Some(DESC)]` collapses to `vec![a Some(ASC)]`.
///
/// It will also filter out entries that are ordered if the next entry is;
/// for instance, `vec![floor(a) Some(ASC), a Some(ASC)]` will be collapsed to
/// `vec![a Some(ASC)]`.
#[deprecated(since = "45.0.0", note = "Use LexRequirement::collapse")]
pub fn collapse_lex_req(input: LexRequirement) -> LexRequirement {
    input.collapse()
}

/// Adds the `offset` value to `Column` indices inside `expr`. This function is
/// generally used during the update of the right table schema in join operations.
pub fn add_offset_to_expr(
    expr: Arc<dyn PhysicalExpr>,
    offset: usize,
) -> Arc<dyn PhysicalExpr> {
    expr.transform_down(|e| match e.as_any().downcast_ref::<Column>() {
        Some(col) => Ok(Transformed::yes(Arc::new(Column::new(
            col.name(),
            offset + col.index(),
        )))),
        None => Ok(Transformed::no(e)),
    })
    .data()
    .unwrap()
    // Note that we can safely unwrap here since our transform always returns
    // an `Ok` value.
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::expressions::col;
    use crate::PhysicalSortExpr;

    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::{plan_datafusion_err, Result};
    use datafusion_physical_expr_common::sort_expr::{
        LexOrdering, PhysicalSortRequirement,
    };

    pub fn output_schema(
        mapping: &ProjectionMapping,
        input_schema: &Arc<Schema>,
    ) -> Result<SchemaRef> {
        // Calculate output schema
        let fields: Result<Vec<Field>> = mapping
            .iter()
            .map(|(source, target)| {
                let name = target
                    .as_any()
                    .downcast_ref::<Column>()
                    .ok_or_else(|| plan_datafusion_err!("Expects to have column"))?
                    .name();
                let field = Field::new(
                    name,
                    source.data_type(input_schema)?,
                    source.nullable(input_schema)?,
                );

                Ok(field)
            })
            .collect();

        let output_schema = Arc::new(Schema::new_with_metadata(
            fields?,
            input_schema.metadata().clone(),
        ));

        Ok(output_schema)
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
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_g = &col("g", &test_schema)?;
        let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
        eq_properties.add_equal_conditions(col_a, col_c)?;

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
        eq_properties.add_new_orderings(orderings);
        Ok((test_schema, eq_properties))
    }

    // Convert each tuple to PhysicalSortRequirement
    pub fn convert_to_sort_reqs(
        in_data: &[(&Arc<dyn PhysicalExpr>, Option<SortOptions>)],
    ) -> LexRequirement {
        in_data
            .iter()
            .map(|(expr, options)| {
                PhysicalSortRequirement::new(Arc::clone(*expr), *options)
            })
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    pub fn convert_to_sort_exprs(
        in_data: &[(&Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> LexOrdering {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: Arc::clone(*expr),
                options: *options,
            })
            .collect()
    }

    // Convert each inner tuple to PhysicalSortExpr
    pub fn convert_to_orderings(
        orderings: &[Vec<(&Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<LexOrdering> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs(sort_exprs))
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    pub fn convert_to_sort_exprs_owned(
        in_data: &[(Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> LexOrdering {
        LexOrdering::new(
            in_data
                .iter()
                .map(|(expr, options)| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: *options,
                })
                .collect(),
        )
    }

    // Convert each inner tuple to PhysicalSortExpr
    pub fn convert_to_orderings_owned(
        orderings: &[Vec<(Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<LexOrdering> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs_owned(sort_exprs))
            .collect()
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
        let col_a_expr = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;
        let col_c_expr = Arc::new(Column::new("c", 2)) as Arc<dyn PhysicalExpr>;
        let col_x_expr = Arc::new(Column::new("x", 3)) as Arc<dyn PhysicalExpr>;
        let col_y_expr = Arc::new(Column::new("y", 4)) as Arc<dyn PhysicalExpr>;

        // a and b are aliases
        eq_properties.add_equal_conditions(&col_a_expr, &col_b_expr)?;
        assert_eq!(eq_properties.eq_group().len(), 1);

        // This new entry is redundant, size shouldn't increase
        eq_properties.add_equal_conditions(&col_b_expr, &col_a_expr)?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 2);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));

        // b and c are aliases. Existing equivalence class should expand,
        // however there shouldn't be any new equivalence class
        eq_properties.add_equal_conditions(&col_b_expr, &col_c_expr)?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 3);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));

        // This is a new set of equality. Hence equivalent class count should be 2.
        eq_properties.add_equal_conditions(&col_x_expr, &col_y_expr)?;
        assert_eq!(eq_properties.eq_group().len(), 2);

        // This equality bridges distinct equality sets.
        // Hence equivalent class count should decrease from 2 to 1.
        eq_properties.add_equal_conditions(&col_x_expr, &col_a_expr)?;
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = eq_properties.eq_group().iter().next().unwrap();
        assert_eq!(eq_groups.len(), 5);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));
        assert!(eq_groups.contains(&col_x_expr));
        assert!(eq_groups.contains(&col_y_expr));

        Ok(())
    }
}
