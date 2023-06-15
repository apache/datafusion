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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Array, ListArray};
use arrow_schema::{Fields, SortOptions};
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use itertools::izip;
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression, where ordering requirement is given
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    name: String,
    input_data_type: DataType,
    ob_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        ob_data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            input_data_type,
            ob_data_types,
            ordering_req,
        }
    }
}

impl AggregateExpr for OrderSensitiveArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            Field::new("item", self.input_data_type.clone(), true),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(OrderSensitiveArrayAggAccumulator::try_new(
            &self.input_data_type,
            &self.ob_data_types,
            self.ordering_req.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.input_data_type.clone(), true),
            false,
        )];
        let orderings = ordering_fields(&self.ordering_req, &self.ob_data_types);
        fields.push(Field::new_list(
            format_state_name(&self.name, "array_agg_orderings"),
            Field::new(
                "item",
                DataType::Struct(Fields::from(orderings.clone())),
                true,
            ),
            false,
        ));
        fields.extend(orderings);
        Ok(fields)
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn order_bys(&self) -> Option<&[PhysicalSortExpr]> {
        if self.ordering_req.is_empty() {
            None
        } else {
            Some(&self.ordering_req)
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for OrderSensitiveArrayAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.ob_data_types == x.ob_data_types
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct OrderSensitiveArrayAggAccumulator {
    values: Vec<ScalarValue>,
    ordering_values: Vec<Vec<ScalarValue>>,
    datatypes: Vec<DataType>,
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> Result<Self> {
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            values: vec![],
            ordering_values: vec![],
            datatypes,
            ordering_req,
        })
    }
}

impl Accumulator for OrderSensitiveArrayAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let n_row = values[0].len();
        for index in 0..n_row {
            let row = get_row_at_idx(values, index)?;
            self.values.push(row[0].clone());
            self.ordering_values.push(row[1..].to_vec());
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is Array_agg result
        let array_agg_values = &states[0];
        // 2nd entry is the history of lexicographical ordered values
        // that array agg result is inserted. It is similar to ARRAY_AGG result.
        // However, values inside are ordering expression results
        let agg_orderings = &states[1];
        if agg_orderings.as_any().is::<ListArray>() {
            let mut partition_values = vec![];
            let mut partition_ordering_values = vec![];
            for index in 0..agg_orderings.len() {
                let ordering = ScalarValue::try_from_array(agg_orderings, index)?;
                let other_ordering_values =
                    self.convert_array_agg_to_orderings(ordering)?;
                // First entry in the state is Array_agg result
                let array_agg_res = ScalarValue::try_from_array(array_agg_values, index)?;
                if let ScalarValue::List(Some(other_values), _) = array_agg_res {
                    partition_values.push(other_values);
                    partition_ordering_values.push(other_ordering_values);
                } else {
                    return Err(DataFusionError::Internal(
                        "array_agg state must be list!".into(),
                    ));
                }
            }
            let sort_options = self
                .ordering_req
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>();
            let (merged_values, merged_ordering_values) = merge_ordered_arrays(
                &partition_values,
                &partition_ordering_values,
                &sort_options,
            )?;
            self.values = merged_values;
            self.ordering_values = merged_ordering_values;
        } else {
            return Err(DataFusionError::Execution(
                "Expects to receive list array".to_string(),
            ));
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut res = vec![self.evaluate()?];
        res.push(self.evaluate_orderings()?);
        let last_ordering = if let Some(ordering) = self.ordering_values.last() {
            ordering.clone()
        } else {
            // In case of ordering is empty, construct ordering as NULL
            self.datatypes
                .iter()
                .skip(1)
                .map(ScalarValue::try_from)
                .collect::<Result<Vec<_>>>()?
        };
        res.extend(last_ordering);
        Ok(res)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::new_list(
            Some(self.values.clone()),
            self.datatypes[0].clone(),
        ))
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + ScalarValue::size_of_vec(&self.values)
            - std::mem::size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total +=
            std::mem::size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total = total + ScalarValue::size_of_vec(row) - std::mem::size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += std::mem::size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total = total + dtype.size() - std::mem::size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += std::mem::size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}

impl OrderSensitiveArrayAggAccumulator {
    fn convert_array_agg_to_orderings(
        &self,
        in_data: ScalarValue,
    ) -> Result<Vec<Vec<ScalarValue>>> {
        if let ScalarValue::List(Some(list_vals), _field_ref) = in_data {
            list_vals.into_iter().map(|struct_vals| {
                if let ScalarValue::Struct(Some(orderings), _fields) = struct_vals {
                    Ok(orderings)
                } else {
                    Err(DataFusionError::Execution(format!(
                        "Expects to receive ScalarValue::Struct(Some(..), _) but got:{:?}",
                        struct_vals.get_datatype()
                    )))
                }
            }).collect::<Result<Vec<_>>>()
        } else {
            Err(DataFusionError::Execution(format!(
                "Expects to receive ScalarValue::List(Some(..), _) but got:{:?}",
                in_data.get_datatype()
            )))
        }
    }

    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let mut orderings = vec![];
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());
        for ordering in &self.ordering_values {
            let res = ScalarValue::Struct(Some(ordering.clone()), struct_field.clone());
            orderings.push(res);
        }
        let struct_type = DataType::Struct(Fields::from(fields));
        Ok(ScalarValue::new_list(Some(orderings), struct_type))
    }
}

fn merge_ordered_arrays(
    values: &[Vec<ScalarValue>],
    ordering_values: &[Vec<Vec<ScalarValue>>],
    sort_options: &[SortOptions],
) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    if values.len() != ordering_values.len()
        || values
            .iter()
            .zip(ordering_values.iter())
            .any(|(vals, ordering_vals)| vals.len() != ordering_vals.len())
    {
        return Err(DataFusionError::Execution(
            "Expects lhs arguments and/or rhs arguments to have same size".to_string(),
        ));
    }
    let n_branch = values.len();
    let mut indices = vec![0_usize; n_branch];
    let end_indices = (0..n_branch)
        .map(|idx| values[idx].len())
        .collect::<Vec<_>>();
    let mut merged_values = vec![];
    let mut merged_ordering_values = vec![];
    // Create comparator to decide insertion order of right and left arrays
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| -> Result<bool> {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(cmp.is_lt())
    };

    loop {
        let mut branch_idx = None;
        let mut min_ordering = None;
        for (idx, end_idx, ordering, branch_idxx) in izip!(
            indices.iter(),
            end_indices.iter(),
            ordering_values.iter(),
            0..n_branch
        ) {
            if idx == end_idx {
                continue;
            }
            let ordering_row = &ordering[*idx];
            let mut reset = false;
            if let Some(min_ordering) = min_ordering {
                if compare_fn(ordering_row, min_ordering)? {
                    reset = true;
                }
            } else {
                reset = true;
            }
            if reset {
                min_ordering = Some(ordering_row);
                branch_idx = Some(branch_idxx);
            }
        }

        if let Some(branch_idx) = branch_idx {
            let row_idx = (&indices)[branch_idx];
            merged_values.push(values[branch_idx][row_idx].clone());
            merged_ordering_values.push(ordering_values[branch_idx][row_idx].clone());
            indices[branch_idx] += 1;
        } else {
            // All branches consumed exit from the loop
            break;
        }
    }

    Ok((merged_values, merged_ordering_values))
}

#[cfg(test)]
mod tests {
    use crate::aggregate::array_agg_ordered::merge_ordered_arrays;
    use arrow_array::{Array, ArrayRef, Int64Array};
    use arrow_schema::SortOptions;
    use datafusion_common::from_slice::FromSlice;
    use datafusion_common::utils::get_row_at_idx;
    use datafusion_common::{Result, ScalarValue};
    use std::sync::Arc;

    #[test]
    fn test_merge_asc() -> Result<()> {
        let lhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_slice([0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])),
        ];
        let n_row = lhs_arrays[0].len();
        let lhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&lhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_slice([0, 0, 1, 1, 2])),
            Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;
        let sort_options = vec![
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ];

        let lhs_vals_arr = Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])) as ArrayRef;
        let lhs_vals = (0..lhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&lhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;
        let expected =
            Arc::new(Int64Array::from_slice([0, 0, 1, 1, 2, 2, 3, 3, 4, 4])) as ArrayRef;
        let (merged_vals, _) = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;
        assert_eq!(&merged_vals, &expected);
        Ok(())
    }

    #[test]
    fn test_merge_desc() -> Result<()> {
        let lhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_slice([2, 1, 1, 0, 0])),
            Arc::new(Int64Array::from_slice([4, 3, 2, 1, 0])),
        ];
        let n_row = lhs_arrays[0].len();
        let lhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&lhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from_slice([2, 1, 1, 0, 0])),
            Arc::new(Int64Array::from_slice([4, 3, 2, 1, 0])),
        ];
        let n_row = rhs_arrays[0].len();
        let rhs_orderings = (0..n_row)
            .map(|idx| get_row_at_idx(&rhs_arrays, idx))
            .collect::<Result<Vec<_>>>()?;
        let sort_options = vec![
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            SortOptions {
                descending: true,
                nulls_first: false,
            },
        ];

        let lhs_vals_arr = Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])) as ArrayRef;
        let lhs_vals = (0..lhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&lhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;

        let rhs_vals_arr = Arc::new(Int64Array::from_slice([0, 1, 2, 3, 4])) as ArrayRef;
        let rhs_vals = (0..rhs_vals_arr.len())
            .map(|idx| ScalarValue::try_from_array(&rhs_vals_arr, idx))
            .collect::<Result<Vec<_>>>()?;
        let expected =
            Arc::new(Int64Array::from_slice([0, 0, 1, 1, 2, 2, 3, 3, 4, 4])) as ArrayRef;
        let (merged_vals, _) = merge_ordered_arrays(
            &[lhs_vals, rhs_vals],
            &[lhs_orderings, rhs_orderings],
            &sort_options,
        )?;
        let merged_vals = ScalarValue::iter_to_array(merged_vals.into_iter())?;
        assert_eq!(&merged_vals, &expected);
        Ok(())
    }
}
