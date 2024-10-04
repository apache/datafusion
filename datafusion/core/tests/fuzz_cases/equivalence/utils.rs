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
//
// use datafusion_physical_expr::expressions::{col, Column};
use datafusion_physical_expr::{ConstExpr, EquivalenceProperties, PhysicalSortExpr};
use std::any::Any;
use std::sync::Arc;

use arrow::compute::{lexsort_to_indices, SortColumn};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, Float32Array, Float64Array, RecordBatch, UInt32Array};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{exec_err, plan_datafusion_err, DataFusionError, Result};

use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_physical_expr::equivalence::{EquivalenceClass, ProjectionMapping};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use itertools::izip;
use rand::prelude::*;

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

    let mut eq_properties = EquivalenceProperties::new(Arc::clone(&test_schema));
    // Define a and f are aliases
    eq_properties.add_equal_conditions(col_a, col_f)?;
    // Column e has constant value.
    eq_properties = eq_properties.with_constants([ConstExpr::from(col_e)]);

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
                expr: Arc::clone(expr),
                options: options_asc,
            })
            .collect();

        eq_properties.add_new_orderings([ordering]);
    }

    Ok((test_schema, eq_properties))
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
        RecordBatch::new_empty(Arc::clone(&output_schema))
    } else {
        RecordBatch::try_new(Arc::clone(&output_schema), projected_values)?
    };

    let projected_eq = input_eq_properties.project(&projection_mapping, output_schema);
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
    eq_properties.add_equal_conditions(&col_a_expr, &col_b_expr)?;
    assert_eq!(eq_properties.eq_group().len(), 1);

    // This new entry is redundant, size shouldn't increase
    eq_properties.add_equal_conditions(&col_b_expr, &col_a_expr)?;
    assert_eq!(eq_properties.eq_group().len(), 1);
    let eq_groups = &eq_properties.eq_group().classes[0];
    assert_eq!(eq_groups.len(), 2);
    assert!(eq_groups.contains(&col_a_expr));
    assert!(eq_groups.contains(&col_b_expr));

    // b and c are aliases. Exising equivalence class should expand,
    // however there shouldn't be any new equivalence class
    eq_properties.add_equal_conditions(&col_b_expr, &col_c_expr)?;
    assert_eq!(eq_properties.eq_group().len(), 1);
    let eq_groups = &eq_properties.eq_group().classes[0];
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
    columns.push(Arc::clone(&unique_col));

    // Create a new schema with the added unique column
    let unique_col_name = "unique";
    let unique_field = Arc::new(Field::new(unique_col_name, DataType::Float64, false));
    let fields: Vec<_> = original_schema
        .fields()
        .iter()
        .cloned()
        .chain(std::iter::once(unique_field))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Create a new batch with the added column
    let new_batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;

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
            return Some(Arc::clone(res));
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
        let col = constant.expr().as_any().downcast_ref::<Column>().unwrap();
        let (idx, _field) = schema.column_with_name(col.name()).unwrap();
        let arr =
            Arc::new(Float64Array::from_iter_values(vec![0 as f64; n_elem])) as ArrayRef;
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
            get_representative_arr(eq_group, &schema_vec, Arc::clone(schema))
                .unwrap_or_else(|| generate_random_array(n_elem, n_distinct));

        for expr in eq_group.iter() {
            let col = expr.as_any().downcast_ref::<Column>().unwrap();
            let (idx, _field) = schema.column_with_name(col.name()).unwrap();
            schema_vec[idx] = Some(Arc::clone(&representative_array));
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

#[derive(Debug, Clone)]
pub struct TestScalarUDF {
    pub(crate) signature: Signature,
}

impl TestScalarUDF {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(
                1,
                vec![Float64, Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TestScalarUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "test-scalar-udf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = &arg_types[0];

        match arg_type {
            DataType::Float32 => Ok(DataType::Float32),
            _ => Ok(DataType::Float64),
        }
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new({
                let arg = &args[0].as_any().downcast_ref::<Float64Array>().ok_or_else(
                    || {
                        DataFusionError::Internal(format!(
                            "could not cast {} to {}",
                            self.name(),
                            std::any::type_name::<Float64Array>()
                        ))
                    },
                )?;

                arg.iter()
                    .map(|a| a.map(f64::floor))
                    .collect::<Float64Array>()
            }),
            DataType::Float32 => Arc::new({
                let arg = &args[0].as_any().downcast_ref::<Float32Array>().ok_or_else(
                    || {
                        DataFusionError::Internal(format!(
                            "could not cast {} to {}",
                            self.name(),
                            std::any::type_name::<Float32Array>()
                        ))
                    },
                )?;

                arg.iter()
                    .map(|a| a.map(f32::floor))
                    .collect::<Float32Array>()
            }),
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }
}
