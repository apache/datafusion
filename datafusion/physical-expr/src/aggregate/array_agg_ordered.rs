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
use crate::{AggregateExpr, LexOrdering, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::{Array, ListArray};
use arrow_schema::{Fields, SortOptions};
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use datafusion_common::ScalarValue;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression, where ordering requirement is given
#[derive(Debug)]
pub struct OrderSensitiveArrayAgg {
    name: String,
    data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl OrderSensitiveArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_types,
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
            Field::new("item", self.data_types[0].clone(), true),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(OrderSensitiveArrayAggAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
            self.ordering_req.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.data_types[0].clone(), true),
            false,
        )];
        let orderings = ordering_fields(&self.ordering_req, &self.data_types[1..]);
        fields.push(Field::new_list(
            format_state_name(&self.name, "array_agg"),
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
        let mut res = vec![self.expr.clone()];
        let ordering_req_exprs = self
            .ordering_req
            .iter()
            .map(|e| e.expr.clone())
            .collect::<Vec<_>>();
        res.extend(ordering_req_exprs.clone());
        res
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
                    && self.data_types == x.data_types
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
            for index in 0..agg_orderings.len() {
                let ordering = ScalarValue::try_from_array(agg_orderings, index)?;
                let other_ordering_values =
                    self.convert_array_agg_to_orderings(ordering)?;
                // First entry in the state is Array_agg result
                let array_agg_res = ScalarValue::try_from_array(array_agg_values, index)?;
                if let ScalarValue::List(Some(other_values), _) = array_agg_res {
                    let sort_options = self
                        .ordering_req
                        .iter()
                        .map(|sort_expr| sort_expr.options)
                        .collect::<Vec<_>>();
                    let (new_values, new_ordering_values) = merge_ordered_arrays(
                        &self.values,
                        &other_values,
                        &self.ordering_values,
                        &other_ordering_values,
                        &sort_options,
                    )?;
                    self.values = new_values;
                    self.ordering_values = new_ordering_values;
                } else {
                    return Err(DataFusionError::Internal(
                        "array_agg state must be list!".into(),
                    ));
                }
            }
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
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.values)
            - std::mem::size_of_val(&self.values)
            + self.datatypes[0].size()
            - std::mem::size_of_val(&self.datatypes[0])
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
    lhs_values: &[ScalarValue],
    rhs_values: &[ScalarValue],
    lhs_ordering: &[Vec<ScalarValue>],
    rhs_ordering: &[Vec<ScalarValue>],
    sort_options: &[SortOptions],
) -> Result<(Vec<ScalarValue>, Vec<Vec<ScalarValue>>)> {
    if (lhs_values.len() != lhs_ordering.len()) | (rhs_values.len() != rhs_ordering.len())
    {
        return Err(DataFusionError::Execution(
            "Expects lhs arguments and/or rhs arguments to have same size".to_string(),
        ));
    }
    let mut lidx = 0;
    let mut ridx = 0;
    let lend = lhs_values.len();
    let rend = rhs_values.len();
    let mut new_values = vec![];
    let mut new_ordering_values = vec![];
    // Create comparator to decide insertion order of right and left arrays
    let compare_fn = |current: &[ScalarValue], target: &[ScalarValue]| -> Result<bool> {
        let cmp = compare_rows(current, target, sort_options)?;
        Ok(cmp.is_lt())
    };
    while lidx < lend || ridx < rend {
        if lidx == lend {
            new_values.extend(rhs_values[ridx..].to_vec());
            new_ordering_values.extend(rhs_ordering[ridx..].to_vec());
            ridx = rend;
        } else if ridx == rend {
            new_values.extend(lhs_values[lidx..].to_vec());
            new_ordering_values.extend(lhs_ordering[lidx..].to_vec());
            lidx = lend;
        } else if compare_fn(&lhs_ordering[lidx], &rhs_ordering[ridx])? {
            new_values.push(lhs_values[lidx].clone());
            new_ordering_values.push(lhs_ordering[lidx].clone());
            lidx += 1;
        } else {
            new_values.push(rhs_values[ridx].clone());
            new_ordering_values.push(rhs_ordering[ridx].clone());
            ridx += 1;
        }
    }

    Ok((new_values, new_ordering_values))
}
