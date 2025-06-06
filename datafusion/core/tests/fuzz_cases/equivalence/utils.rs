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

use std::any::Any;
use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array, RecordBatch, UInt32Array};
use arrow::compute::{lexsort_to_indices, take_record_batch, SortColumn, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::{exec_err, plan_err, DataFusionError, Result};
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_physical_expr::equivalence::{
    convert_to_orderings, EquivalenceClass, ProjectionMapping,
};
use datafusion_physical_expr::{ConstExpr, EquivalenceProperties};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::expressions::{col, Column};

use itertools::izip;
use rand::prelude::*;

/// Projects the input schema based on the given projection mapping.
pub fn output_schema(
    mapping: &ProjectionMapping,
    input_schema: &Arc<Schema>,
) -> Result<SchemaRef> {
    // Calculate output schema:
    let mut fields = vec![];
    for (source, targets) in mapping.iter() {
        let data_type = source.data_type(input_schema)?;
        let nullable = source.nullable(input_schema)?;
        for (target, _) in targets.iter() {
            let Some(column) = target.as_any().downcast_ref::<Column>() else {
                return plan_err!("Expects to have column");
            };
            fields.push(Field::new(column.name(), data_type.clone(), nullable));
        }
    }

    let output_schema = Arc::new(Schema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ));

    Ok(output_schema)
}

// Generate a schema which consists of 6 columns (a, b, c, d, e, f)
pub fn create_test_schema_2() -> Result<SchemaRef> {
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
    eq_properties.add_equal_conditions(Arc::clone(col_a), Arc::clone(col_f))?;
    // Column e has constant value.
    eq_properties.add_constants([ConstExpr::from(Arc::clone(col_e))])?;

    // Randomly order columns for sorting
    let mut rng = StdRng::seed_from_u64(seed);
    let mut remaining_exprs = col_exprs[0..4].to_vec(); // only a, b, c, d are sorted

    let options_asc = SortOptions {
        descending: false,
        nulls_first: false,
    };

    while !remaining_exprs.is_empty() {
        let n_sort_expr = rng.random_range(1..remaining_exprs.len() + 1);
        remaining_exprs.shuffle(&mut rng);

        let ordering =
            remaining_exprs
                .drain(0..n_sort_expr)
                .map(|expr| PhysicalSortExpr {
                    expr: Arc::clone(expr),
                    options: options_asc,
                });

        eq_properties.add_ordering(ordering);
    }

    Ok((test_schema, eq_properties))
}

// Apply projection to the input_data, return projected equivalence properties and record batch
pub fn apply_projection(
    proj_exprs: impl IntoIterator<Item = (Arc<dyn PhysicalExpr>, String)>,
    input_data: &RecordBatch,
    input_eq_properties: &EquivalenceProperties,
) -> Result<(RecordBatch, EquivalenceProperties)> {
    let input_schema = input_data.schema();
    let projection_mapping = ProjectionMapping::try_new(proj_exprs, &input_schema)?;

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
    mut required_ordering: LexOrdering,
    batch: &RecordBatch,
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
    eq_properties.add_equal_conditions(Arc::clone(col_a), Arc::clone(col_c))?;

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
            .map(|_| rng.random_range(0..max_val) as f64 / 2.0)
            .collect();
        Arc::new(Float64Array::from_iter_values(values))
    };

    // Fill constant columns
    for constant in eq_properties.constants() {
        let col = constant.expr.as_any().downcast_ref::<Column>().unwrap();
        let (idx, _field) = schema.column_with_name(col.name()).unwrap();
        let arr =
            Arc::new(Float64Array::from_iter_values(vec![0 as f64; n_elem])) as ArrayRef;
        schema_vec[idx] = Some(arr);
    }

    // Fill columns based on ordering equivalences
    for ordering in eq_properties.oeq_class().iter() {
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
    for eq_group in eq_properties.eq_group().iter() {
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

// Generate a table that satisfies the given orderings;
pub fn generate_table_for_orderings(
    mut orderings: Vec<LexOrdering>,
    schema: SchemaRef,
    n_elem: usize,
    n_distinct: usize,
) -> Result<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(23);

    assert!(!orderings.is_empty());
    // Sort the inner vectors by their lengths (longest first)
    orderings.sort_by_key(|v| std::cmp::Reverse(v.len()));

    let arrays = schema
        .fields
        .iter()
        .map(|field| {
            (
                field.name(),
                generate_random_f64_array(n_elem, n_distinct, &mut rng),
            )
        })
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(arrays)?;

    // Sort batch according to first ordering expression
    let sort_columns = get_sort_columns(&batch, &orderings[0])?;
    let sort_indices = lexsort_to_indices(&sort_columns, None)?;
    let mut batch = take_record_batch(&batch, &sort_indices)?;

    // prune out rows that is invalid according to remaining orderings.
    for ordering in orderings.iter().skip(1) {
        let sort_columns = get_sort_columns(&batch, ordering)?;

        // Collect sort options and values into separate vectors.
        let (sort_options, sort_col_values): (Vec<_>, Vec<_>) = sort_columns
            .into_iter()
            .map(|sort_col| (sort_col.options.unwrap(), sort_col.values))
            .unzip();

        let mut cur_idx = 0;
        let mut keep_indices = vec![cur_idx as u32];
        for next_idx in 1..batch.num_rows() {
            let cur_row = get_row_at_idx(&sort_col_values, cur_idx)?;
            let next_row = get_row_at_idx(&sort_col_values, next_idx)?;

            if compare_rows(&cur_row, &next_row, &sort_options)? != Ordering::Greater {
                // next row satisfies ordering relation given, compared to the current row.
                keep_indices.push(next_idx as u32);
                cur_idx = next_idx;
            }
        }
        // Only keep valid rows, that satisfies given ordering relation.
        batch = take_record_batch(&batch, &UInt32Array::from_iter_values(keep_indices))?;
    }

    Ok(batch)
}

// Utility function to generate random f64 array
fn generate_random_f64_array(
    n_elems: usize,
    n_distinct: usize,
    rng: &mut StdRng,
) -> ArrayRef {
    let values: Vec<f64> = (0..n_elems)
        .map(|_| rng.random_range(0..n_distinct) as f64 / 2.0)
        .collect();
    Arc::new(Float64Array::from_iter_values(values))
}

// Helper function to get sort columns from a batch
fn get_sort_columns(
    batch: &RecordBatch,
    ordering: &LexOrdering,
) -> Result<Vec<SortColumn>> {
    ordering
        .iter()
        .map(|expr| expr.evaluate_to_sort_column(batch))
        .collect::<Result<Vec<_>>>()
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;

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
