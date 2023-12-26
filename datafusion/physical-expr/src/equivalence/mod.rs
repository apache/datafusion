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

mod class;
mod ordering;
mod projection;
mod properties;
use crate::expressions::Column;
use crate::{LexRequirement, PhysicalExpr, PhysicalSortRequirement};
pub use class::{EquivalenceClass, EquivalenceGroup};
use datafusion_common::tree_node::{Transformed, TreeNode};
pub use ordering::OrderingEquivalenceClass;
pub use projection::ProjectionMapping;
pub use properties::EquivalenceProperties;
use std::sync::Arc;

/// This function constructs a duplicate-free `LexOrderingReq` by filtering out
/// duplicate entries that have same physical expression inside. For example,
/// `vec![a Some(ASC), a Some(DESC)]` collapses to `vec![a Some(ASC)]`.
pub fn collapse_lex_req(input: LexRequirement) -> LexRequirement {
    let mut output = Vec::<PhysicalSortRequirement>::new();
    for item in input {
        if !output.iter().any(|req| req.expr.eq(&item.expr)) {
            output.push(item);
        }
    }
    output
}

/// Adds the `offset` value to `Column` indices inside `expr`. This function is
/// generally used during the update of the right table schema in join operations.
pub fn add_offset_to_expr(
    expr: Arc<dyn PhysicalExpr>,
    offset: usize,
) -> Arc<dyn PhysicalExpr> {
    expr.transform_down(&|e| match e.as_any().downcast_ref::<Column>() {
        Some(col) => Ok(Transformed::Yes(Arc::new(Column::new(
            col.name(),
            offset + col.index(),
        )))),
        None => Ok(Transformed::No(e)),
    })
    .unwrap()
    // Note that we can safely unwrap here since our transform always returns
    // an `Ok` value.
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::execution_props::ExecutionProps;
    use crate::expressions::{col, lit, BinaryExpr, Column, Literal};
    use crate::functions::create_physical_expr;
    use crate::PhysicalSortExpr;

    use arrow::compute::{lexsort_to_indices, SortColumn};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{ArrayRef, Float64Array, RecordBatch, UInt32Array};
    use arrow_schema::{Fields, SchemaRef, SortOptions, TimeUnit};
    use datafusion_common::{plan_datafusion_err, DataFusionError, Result, ScalarValue};
    use datafusion_expr::{BuiltinScalarFunction, Operator};

    use itertools::{izip, Itertools};
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

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
        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        eq_properties.add_equal_conditions(col_a, col_c);

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

    // Generate a schema which consists of 6 columns (a, b, c, d, e, f)
    fn create_test_schema_2() -> Result<SchemaRef> {
        let a = Field::new("a", DataType::Float64, true);
        let b = Field::new("b", DataType::Float64, true);
        let c = Field::new("c", DataType::Float64, true);
        let d = Field::new("d", DataType::Float64, true);
        let e = Field::new("e", DataType::Float64, true);
        let f = Field::new("f", DataType::Float64, true);
        let schema = Arc::new(Schema::new(vec![a, b, c, d, e, f]));

        Ok(schema)
    }

    /// Construct a schema with random ordering
    /// among column a, b, c, d
    /// where
    /// Column [a=f] (e.g they are aliases).
    /// Column e is constant.
    pub fn create_random_schema(seed: u64) -> Result<(SchemaRef, EquivalenceProperties)> {
        let test_schema = create_test_schema_2()?;
        let col_a = &col("a", &test_schema)?;
        let col_b = &col("b", &test_schema)?;
        let col_c = &col("c", &test_schema)?;
        let col_d = &col("d", &test_schema)?;
        let col_e = &col("e", &test_schema)?;
        let col_f = &col("f", &test_schema)?;
        let col_exprs = [col_a, col_b, col_c, col_d, col_e, col_f];

        let mut eq_properties = EquivalenceProperties::new(test_schema.clone());
        // Define a and f are aliases
        eq_properties.add_equal_conditions(col_a, col_f);
        // Column e has constant value.
        eq_properties = eq_properties.add_constants([col_e.clone()]);

        // Randomly order columns for sorting
        let mut rng = StdRng::seed_from_u64(seed);
        let mut remaining_exprs = col_exprs[0..4].to_vec(); // only a, b, c, d are sorted

        let options_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };

        while !remaining_exprs.is_empty() {
            let n_sort_expr = rng.gen_range(0..remaining_exprs.len() + 1);
            remaining_exprs.shuffle(&mut rng);

            let ordering = remaining_exprs
                .drain(0..n_sort_expr)
                .map(|expr| PhysicalSortExpr {
                    expr: expr.clone(),
                    options: options_asc,
                })
                .collect();

            eq_properties.add_new_orderings([ordering]);
        }

        Ok((test_schema, eq_properties))
    }

    // Convert each tuple to PhysicalSortRequirement
    fn convert_to_sort_reqs(
        in_data: &[(&Arc<dyn PhysicalExpr>, Option<SortOptions>)],
    ) -> Vec<PhysicalSortRequirement> {
        in_data
            .iter()
            .map(|(expr, options)| {
                PhysicalSortRequirement::new((*expr).clone(), *options)
            })
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    pub fn convert_to_sort_exprs(
        in_data: &[(&Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> Vec<PhysicalSortExpr> {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: (*expr).clone(),
                options: *options,
            })
            .collect()
    }

    // Convert each inner tuple to PhysicalSortExpr
    pub fn convert_to_orderings(
        orderings: &[Vec<(&Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<Vec<PhysicalSortExpr>> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs(sort_exprs))
            .collect()
    }

    // Convert each tuple to PhysicalSortExpr
    fn convert_to_sort_exprs_owned(
        in_data: &[(Arc<dyn PhysicalExpr>, SortOptions)],
    ) -> Vec<PhysicalSortExpr> {
        in_data
            .iter()
            .map(|(expr, options)| PhysicalSortExpr {
                expr: (*expr).clone(),
                options: *options,
            })
            .collect()
    }

    // Convert each inner tuple to PhysicalSortExpr
    pub fn convert_to_orderings_owned(
        orderings: &[Vec<(Arc<dyn PhysicalExpr>, SortOptions)>],
    ) -> Vec<Vec<PhysicalSortExpr>> {
        orderings
            .iter()
            .map(|sort_exprs| convert_to_sort_exprs_owned(sort_exprs))
            .collect()
    }

    // Apply projection to the input_data, return projected equivalence properties and record batch
    pub fn apply_projection(
        proj_exprs: Vec<(Arc<dyn PhysicalExpr>, String)>,
        input_data: &RecordBatch,
        input_eq_properties: &EquivalenceProperties,
    ) -> Result<(RecordBatch, EquivalenceProperties)> {
        let input_schema = input_data.schema();
        let projection_mapping = ProjectionMapping::try_new(&proj_exprs, &input_schema)?;

        let output_schema = output_schema(&projection_mapping, &input_schema)?;
        let num_rows = input_data.num_rows();
        // Apply projection to the input record batch.
        let projected_values = projection_mapping
            .iter()
            .map(|(source, _target)| source.evaluate(input_data)?.into_array(num_rows))
            .collect::<Result<Vec<_>>>()?;
        let projected_batch = if projected_values.is_empty() {
            RecordBatch::new_empty(output_schema.clone())
        } else {
            RecordBatch::try_new(output_schema.clone(), projected_values)?
        };

        let projected_eq =
            input_eq_properties.project(&projection_mapping, output_schema);
        Ok((projected_batch, projected_eq))
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
        eq_properties.add_equal_conditions(&col_a_expr, &col_b_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);

        // This new entry is redundant, size shouldn't increase
        eq_properties.add_equal_conditions(&col_b_expr, &col_a_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 2);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));

        // b and c are aliases. Exising equivalence class should expand,
        // however there shouldn't be any new equivalence class
        eq_properties.add_equal_conditions(&col_b_expr, &col_c_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 3);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));

        // This is a new set of equality. Hence equivalent class count should be 2.
        eq_properties.add_equal_conditions(&col_x_expr, &col_y_expr);
        assert_eq!(eq_properties.eq_group().len(), 2);

        // This equality bridges distinct equality sets.
        // Hence equivalent class count should decrease from 2 to 1.
        eq_properties.add_equal_conditions(&col_x_expr, &col_a_expr);
        assert_eq!(eq_properties.eq_group().len(), 1);
        let eq_groups = &eq_properties.eq_group().classes[0];
        assert_eq!(eq_groups.len(), 5);
        assert!(eq_groups.contains(&col_a_expr));
        assert!(eq_groups.contains(&col_b_expr));
        assert!(eq_groups.contains(&col_c_expr));
        assert!(eq_groups.contains(&col_x_expr));
        assert!(eq_groups.contains(&col_y_expr));

        Ok(())
    }

    /// Checks if the table (RecordBatch) remains unchanged when sorted according to the provided `required_ordering`.
    ///
    /// The function works by adding a unique column of ascending integers to the original table. This column ensures
    /// that rows that are otherwise indistinguishable (e.g., if they have the same values in all other columns) can
    /// still be differentiated. When sorting the extended table, the unique column acts as a tie-breaker to produce
    /// deterministic sorting results.
    ///
    /// If the table remains the same after sorting with the added unique column, it indicates that the table was
    /// already sorted according to `required_ordering` to begin with.
    pub fn is_table_same_after_sort(
        mut required_ordering: Vec<PhysicalSortExpr>,
        batch: RecordBatch,
    ) -> Result<bool> {
        // Clone the original schema and columns
        let original_schema = batch.schema();
        let mut columns = batch.columns().to_vec();

        // Create a new unique column
        let n_row = batch.num_rows();
        let vals: Vec<usize> = (0..n_row).collect::<Vec<_>>();
        let vals: Vec<f64> = vals.into_iter().map(|val| val as f64).collect();
        let unique_col = Arc::new(Float64Array::from_iter_values(vals)) as ArrayRef;
        columns.push(unique_col.clone());

        // Create a new schema with the added unique column
        let unique_col_name = "unique";
        let unique_field =
            Arc::new(Field::new(unique_col_name, DataType::Float64, false));
        let fields: Vec<_> = original_schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(unique_field))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Create a new batch with the added column
        let new_batch = RecordBatch::try_new(schema.clone(), columns)?;

        // Add the unique column to the required ordering to ensure deterministic results
        required_ordering.push(PhysicalSortExpr {
            expr: Arc::new(Column::new(unique_col_name, original_schema.fields().len())),
            options: Default::default(),
        });

        // Convert the required ordering to a list of SortColumn
        let sort_columns = required_ordering
            .iter()
            .map(|order_expr| {
                let expr_result = order_expr.expr.evaluate(&new_batch)?;
                let values = expr_result.into_array(new_batch.num_rows())?;
                Ok(SortColumn {
                    values,
                    options: Some(order_expr.options),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Check if the indices after sorting match the initial ordering
        let sorted_indices = lexsort_to_indices(&sort_columns, None)?;
        let original_indices = UInt32Array::from_iter_values(0..n_row as u32);

        Ok(sorted_indices == original_indices)
    }

    // If we already generated a random result for one of the
    // expressions in the equivalence classes. For other expressions in the same
    // equivalence class use same result. This util gets already calculated result, when available.
    fn get_representative_arr(
        eq_group: &EquivalenceClass,
        existing_vec: &[Option<ArrayRef>],
        schema: SchemaRef,
    ) -> Option<ArrayRef> {
        for expr in eq_group.iter() {
            let col = expr.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            if let Some(res) = &existing_vec[idx] {
                return Some(res.clone());
            }
        }
        None
    }

    // Generate a table that satisfies the given equivalence properties; i.e.
    // equivalences, ordering equivalences, and constants.
    pub fn generate_table_for_eq_properties(
        eq_properties: &EquivalenceProperties,
        n_elem: usize,
        n_distinct: usize,
    ) -> Result<RecordBatch> {
        let mut rng = StdRng::seed_from_u64(23);

        let schema = eq_properties.schema();
        let mut schema_vec = vec![None; schema.fields.len()];

        // Utility closure to generate random array
        let mut generate_random_array = |num_elems: usize, max_val: usize| -> ArrayRef {
            let values: Vec<f64> = (0..num_elems)
                .map(|_| rng.gen_range(0..max_val) as f64 / 2.0)
                .collect();
            Arc::new(Float64Array::from_iter_values(values))
        };

        // Fill constant columns
        for constant in &eq_properties.constants {
            let col = constant.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            let arr = Arc::new(Float64Array::from_iter_values(vec![0 as f64; n_elem]))
                as ArrayRef;
            schema_vec[idx] = Some(arr);
        }

        // Fill columns based on ordering equivalences
        for ordering in eq_properties.oeq_class.iter() {
            let (sort_columns, indices): (Vec<_>, Vec<_>) = ordering
                .iter()
                .map(|PhysicalSortExpr { expr, options }| {
                    let col = expr.as_any().downcast_ref::<Column>().unwrap();
                    let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                    let arr = generate_random_array(n_elem, n_distinct);
                    (
                        SortColumn {
                            values: arr,
                            options: Some(*options),
                        },
                        idx,
                    )
                })
                .unzip();

            let sort_arrs = arrow::compute::lexsort(&sort_columns, None)?;
            for (idx, arr) in izip!(indices, sort_arrs) {
                schema_vec[idx] = Some(arr);
            }
        }

        // Fill columns based on equivalence groups
        for eq_group in eq_properties.eq_group.iter() {
            let representative_array =
                get_representative_arr(eq_group, &schema_vec, schema.clone())
                    .unwrap_or_else(|| generate_random_array(n_elem, n_distinct));

            for expr in eq_group.iter() {
                let col = expr.as_any().downcast_ref::<Column>().unwrap();
                let (idx, _field) = schema.column_with_name(col.name()).unwrap();
                schema_vec[idx] = Some(representative_array.clone());
            }
        }

        let res: Vec<_> = schema_vec
            .into_iter()
            .zip(schema.fields.iter())
            .map(|(elem, field)| {
                (
                    field.name(),
                    // Generate random values for columns that do not occur in any of the groups (equivalence, ordering equivalence, constants)
                    elem.unwrap_or_else(|| generate_random_array(n_elem, n_distinct)),
                )
            })
            .collect();

        Ok(RecordBatch::try_from_iter(res)?)
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

            let normalized = eq_properties.normalize_sort_requirements(&reqs);
            assert!(
                expected.eq(&normalized),
                "error in test: reqs: {reqs:?}, expected: {expected:?}, normalized: {normalized:?}"
            );
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
                eq_properties.normalize_sort_requirements(&req),
                expected_normalized
            );
        }

        Ok(())
    }

    #[test]
    fn test_get_finer() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // First entry, and second entry are the physical sort requirement that are argument for get_finer_requirement.
        // Third entry is the expected result.
        let tests_cases = vec![
            // Get finer requirement between [a Some(ASC)] and [a None, b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC)]
            (
                vec![(col_a, Some(option_asc))],
                vec![(col_a, None), (col_b, Some(option_asc))],
                Some(vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC), c Some(ASC)] and [a Some(ASC), b Some(ASC)]
            // result should be [a Some(ASC), b Some(ASC), c Some(ASC)]
            (
                vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                Some(vec![
                    (col_a, Some(option_asc)),
                    (col_b, Some(option_asc)),
                    (col_c, Some(option_asc)),
                ]),
            ),
            // Get finer requirement between [a Some(ASC), b Some(ASC)] and [a Some(ASC), b Some(DESC)]
            // result should be None
            (
                vec![(col_a, Some(option_asc)), (col_b, Some(option_asc))],
                vec![(col_a, Some(option_asc)), (col_b, Some(option_desc))],
                None,
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_reqs(&lhs);
            let rhs = convert_to_sort_reqs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_reqs(&expected));
            let finer = eq_properties.get_finer_requirement(&lhs, &rhs);
            assert_eq!(finer, expected)
        }

        Ok(())
    }

    #[test]
    fn test_get_meet_ordering() -> Result<()> {
        let schema = create_test_schema()?;
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let eq_properties = EquivalenceProperties::new(schema);
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        let tests_cases = vec![
            // Get meet ordering between [a ASC] and [a ASC, b ASC]
            // result should be [a ASC]
            (
                vec![(col_a, option_asc)],
                vec![(col_a, option_asc), (col_b, option_asc)],
                Some(vec![(col_a, option_asc)]),
            ),
            // Get meet ordering between [a ASC] and [a DESC]
            // result should be None.
            (vec![(col_a, option_asc)], vec![(col_a, option_desc)], None),
            // Get meet ordering between [a ASC, b ASC] and [a ASC, b DESC]
            // result should be [a ASC].
            (
                vec![(col_a, option_asc), (col_b, option_asc)],
                vec![(col_a, option_asc), (col_b, option_desc)],
                Some(vec![(col_a, option_asc)]),
            ),
        ];
        for (lhs, rhs, expected) in tests_cases {
            let lhs = convert_to_sort_exprs(&lhs);
            let rhs = convert_to_sort_exprs(&rhs);
            let expected = expected.map(|expected| convert_to_sort_exprs(&expected));
            let finer = eq_properties.get_meet_ordering(&lhs, &rhs);
            assert_eq!(finer, expected)
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
        let col_h = &col("h", &test_schema)?;
        // a + d
        let a_plus_d = Arc::new(BinaryExpr::new(
            col_a.clone(),
            Operator::Plus,
            col_d.clone(),
        )) as Arc<dyn PhysicalExpr>;

        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        let option_desc = SortOptions {
            descending: true,
            nulls_first: true,
        };
        // [d ASC, h ASC] also satisfies schema.
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            },
            PhysicalSortExpr {
                expr: col_h.clone(),
                options: option_desc,
            },
        ]]);
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
        ];
        for (exprs, expected) in test_cases {
            let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
            let expected = convert_to_sort_exprs(&expected);
            let (actual, _) = eq_properties.find_longest_permutation(&exprs);
            assert_eq!(actual, expected);
        }

        Ok(())
    }

    #[test]
    fn test_find_longest_permutation_random() -> Result<()> {
        const N_RANDOM_SCHEMA: usize = 100;
        const N_ELEMENTS: usize = 125;
        const N_DISTINCT: usize = 5;

        for seed in 0..N_RANDOM_SCHEMA {
            // Create a random schema with random properties
            let (test_schema, eq_properties) = create_random_schema(seed as u64)?;
            // Generate a data that satisfies properties given
            let table_data_with_properties =
                generate_table_for_eq_properties(&eq_properties, N_ELEMENTS, N_DISTINCT)?;

            let floor_a = create_physical_expr(
                &BuiltinScalarFunction::Floor,
                &[col("a", &test_schema)?],
                &test_schema,
                &ExecutionProps::default(),
            )?;
            let a_plus_b = Arc::new(BinaryExpr::new(
                col("a", &test_schema)?,
                Operator::Plus,
                col("b", &test_schema)?,
            )) as Arc<dyn PhysicalExpr>;
            let exprs = vec![
                col("a", &test_schema)?,
                col("b", &test_schema)?,
                col("c", &test_schema)?,
                col("d", &test_schema)?,
                col("e", &test_schema)?,
                col("f", &test_schema)?,
                floor_a,
                a_plus_b,
            ];

            for n_req in 0..=exprs.len() {
                for exprs in exprs.iter().combinations(n_req) {
                    let exprs = exprs.into_iter().cloned().collect::<Vec<_>>();
                    let (ordering, indices) =
                        eq_properties.find_longest_permutation(&exprs);
                    // Make sure that find_longest_permutation return values are consistent
                    let ordering2 = indices
                        .iter()
                        .zip(ordering.iter())
                        .map(|(&idx, sort_expr)| PhysicalSortExpr {
                            expr: exprs[idx].clone(),
                            options: sort_expr.options,
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        ordering, ordering2,
                        "indices and lexicographical ordering do not match"
                    );

                    let err_msg = format!(
                        "Error in test case ordering:{:?}, eq_properties.oeq_class: {:?}, eq_properties.eq_group: {:?}, eq_properties.constants: {:?}",
                        ordering, eq_properties.oeq_class, eq_properties.eq_group, eq_properties.constants
                    );
                    assert_eq!(ordering.len(), indices.len(), "{}", err_msg);
                    // Since ordered section satisfies schema, we expect
                    // that result will be same after sort (e.g sort was unnecessary).
                    assert!(
                        is_table_same_after_sort(
                            ordering.clone(),
                            table_data_with_properties.clone(),
                        )?,
                        "{}",
                        err_msg
                    );
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_update_ordering() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, true),
        ]);

        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema.clone()));
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let col_c = &col("c", &schema)?;
        let col_d = &col("d", &schema)?;
        let option_asc = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // b=a (e.g they are aliases)
        eq_properties.add_equal_conditions(col_b, col_a);
        // [b ASC], [d ASC]
        eq_properties.add_new_orderings(vec![
            vec![PhysicalSortExpr {
                expr: col_b.clone(),
                options: option_asc,
            }],
            vec![PhysicalSortExpr {
                expr: col_d.clone(),
                options: option_asc,
            }],
        ]);

        let test_cases = vec![
            // d + b
            (
                Arc::new(BinaryExpr::new(
                    col_d.clone(),
                    Operator::Plus,
                    col_b.clone(),
                )) as Arc<dyn PhysicalExpr>,
                SortProperties::Ordered(option_asc),
            ),
            // b
            (col_b.clone(), SortProperties::Ordered(option_asc)),
            // a
            (col_a.clone(), SortProperties::Ordered(option_asc)),
            // a + c
            (
                Arc::new(BinaryExpr::new(
                    col_a.clone(),
                    Operator::Plus,
                    col_c.clone(),
                )),
                SortProperties::Unordered,
            ),
        ];
        for (expr, expected) in test_cases {
            let leading_orderings = eq_properties
                .oeq_class()
                .iter()
                .flat_map(|ordering| ordering.first().cloned())
                .collect::<Vec<_>>();
            let expr_ordering = eq_properties.get_expr_ordering(expr.clone());
            let err_msg = format!(
                "expr:{:?}, expected: {:?}, actual: {:?}, leading_orderings: {leading_orderings:?}",
                expr, expected, expr_ordering.state
            );
            assert_eq!(expr_ordering.state, expected, "{}", err_msg);
        }

        Ok(())
    }

    #[test]
    fn test_contains_any() {
        let lit_true = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))
            as Arc<dyn PhysicalExpr>;
        let lit_false = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
            as Arc<dyn PhysicalExpr>;
        let lit2 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(2)))) as Arc<dyn PhysicalExpr>;
        let lit1 =
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
        let col_b_expr = Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>;

        let cls1 = EquivalenceClass::new(vec![lit_true.clone(), lit_false.clone()]);
        let cls2 = EquivalenceClass::new(vec![lit_true.clone(), col_b_expr.clone()]);
        let cls3 = EquivalenceClass::new(vec![lit2.clone(), lit1.clone()]);

        // lit_true is common
        assert!(cls1.contains_any(&cls2));
        // there is no common entry
        assert!(!cls1.contains_any(&cls3));
        assert!(!cls2.contains_any(&cls3));
    }

    #[test]
    fn test_get_indices_of_matching_sort_exprs_with_order_eq() -> Result<()> {
        let sort_options = SortOptions::default();
        let sort_options_not = SortOptions::default().not();

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
            ]
        );

        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]);
        let col_a = &col("a", &schema)?;
        let col_b = &col("b", &schema)?;
        let required_columns = [col_b.clone(), col_a.clone()];
        let mut eq_properties = EquivalenceProperties::new(Arc::new(schema));
        eq_properties.add_new_orderings([
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            }],
            vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("b", 1)),
                    options: sort_options_not,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options: sort_options,
                },
            ],
        ]);
        let (result, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0, 1]);
        assert_eq!(
            result,
            vec![
                PhysicalSortExpr {
                    expr: col_b.clone(),
                    options: sort_options_not
                },
                PhysicalSortExpr {
                    expr: col_a.clone(),
                    options: sort_options
                }
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
        eq_properties.add_new_orderings([vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("b", 1)),
                options: sort_options_not,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options: sort_options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options: sort_options,
            },
        ]]);
        let (_, idxs) = eq_properties.find_longest_permutation(&required_columns);
        assert_eq!(idxs, vec![0]);

        Ok(())
    }
}
