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
use std::sync::Arc;

use crate::aggregate::array_agg_ordered::merge_ordered_arrays;
use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{
    reverse_order_bys, AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr,
};
use arrow_array::cast::AsArray;
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Fields};
use datafusion_common::utils::get_row_at_idx;
use datafusion_common::DataFusionError;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::Accumulator;

/// Expression for a ARRAY_AGG(ORDER BY) aggregation.
/// When aggregation works in multiple partitions
/// aggregations are split into multiple partitions,
/// then their results are merged. This aggregator
/// is a version of ARRAY_AGG that can support producing
/// intermediate aggregation (with necessary side information)
/// and that can merge aggregations from multiple partitions.
#[derive(Debug)]
pub struct NthValueAgg {
    /// Column name
    name: String,
    /// The DataType for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
    n: i64,
    /// If the input expression can have NULLs
    nullable: bool,
    /// Ordering data types
    order_by_data_types: Vec<DataType>,
    /// Ordering requirement
    ordering_req: LexOrdering,
}

impl NthValueAgg {
    /// Create a new `OrderSensitiveArrayAgg` aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        n: i64,
        name: impl Into<String>,
        input_data_type: DataType,
        nullable: bool,
        order_by_data_types: Vec<DataType>,
        ordering_req: LexOrdering,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            expr,
            n,
            nullable,
            order_by_data_types,
            ordering_req,
        }
    }
}

impl AggregateExpr for NthValueAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(NthValueAccumulator::try_new(
            self.n,
            &self.input_data_type,
            &self.order_by_data_types,
            self.ordering_req.clone(),
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new_list(
            format_state_name(&self.name, "nth_value"),
            Field::new("item", self.input_data_type.clone(), true),
            self.nullable, // This should be the same as field()
        )];
        if !self.ordering_req.is_empty() {
            let orderings =
                ordering_fields(&self.ordering_req, &self.order_by_data_types);
            fields.push(Field::new_list(
                format_state_name(&self.name, "nth_value_orderings"),
                Field::new("item", DataType::Struct(Fields::from(orderings)), true),
                self.nullable,
            ));
        }
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

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(Self {
            name: self.name.to_string(),
            input_data_type: self.input_data_type.clone(),
            expr: self.expr.clone(),
            // index should be from the opposite side
            n: -self.n,
            nullable: self.nullable,
            order_by_data_types: self.order_by_data_types.clone(),
            // reverse requirement
            ordering_req: reverse_order_bys(&self.ordering_req),
        }) as _)
    }
}

impl PartialEq<dyn Any> for NthValueAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.order_by_data_types == x.order_by_data_types
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
pub(crate) struct NthValueAccumulator {
    n: i64,
    // `values` stores entries in the ARRAY_AGG result.
    values: Vec<ScalarValue>,
    // `ordering_values` stores values of ordering requirement expression
    // corresponding to each value in the ARRAY_AGG.
    // For each `ScalarValue` inside `values`, there will be a corresponding
    // `Vec<ScalarValue>` inside `ordering_values` which stores it ordering.
    // This information is used during merging results of the different partitions.
    // For detailed information how merging is done see [`merge_ordered_arrays`]
    ordering_values: Vec<Vec<ScalarValue>>,
    // `datatypes` stores, datatype of expression inside ARRAY_AGG and ordering requirement expressions.
    datatypes: Vec<DataType>,
    // Stores ordering requirement of the Accumulator
    ordering_req: LexOrdering,
}

impl NthValueAccumulator {
    /// Create a new order-sensitive ARRAY_AGG accumulator based on the given
    /// item data type.
    pub fn try_new(
        n: i64,
        datatype: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
    ) -> Result<Self> {
        if n == 0 {
            // n cannot be 0
            return internal_err!("Nth value indices are 1 based. 0 is invalid index");
        }
        let mut datatypes = vec![datatype.clone()];
        datatypes.extend(ordering_dtypes.iter().cloned());
        Ok(Self {
            n,
            values: vec![],
            ordering_values: vec![],
            datatypes,
            ordering_req,
        })
    }
}

impl Accumulator for NthValueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        if from_start {
            // direction is from start
            let n_remaining = n_required.saturating_sub(self.values.len());
            self.append_new_data(values, Some(n_remaining))?;
        } else {
            // direction is from end
            self.append_new_data(values, None)?;
            let start_offset = self.values.len().saturating_sub(n_required);
            if start_offset > 0 {
                self.values = self.values[start_offset..].to_vec();
                self.ordering_values = self.ordering_values[start_offset..].to_vec();
            }
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        // First entry in the state is the aggregation result.
        let array_agg_values = &states[0];
        let n_required = self.n.unsigned_abs() as usize;
        if self.ordering_req.is_empty() {
            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;
            // Stores ARRAY_AGG results coming from each partition
            let mut partition_values = vec![];
            // Existing values should be merged also.
            partition_values.extend(self.values.clone());
            for v in array_agg_res.into_iter() {
                partition_values.extend(v);
                if partition_values.len() > n_required {
                    // There is enough data collected can stop merging
                    break;
                }
            }
            self.values = partition_values;
        } else if let Some(agg_orderings) = states[1].as_list_opt::<i32>() {
            // 2nd entry stores values received for ordering requirement columns, for each aggregation value inside ARRAY_AGG list.
            // For each `StructArray` inside ARRAY_AGG list, we will receive an `Array` that stores
            // values received from its ordering requirement expression. (This information is necessary for during merging).

            // Stores ARRAY_AGG results coming from each partition
            let mut partition_values = vec![];
            // Stores ordering requirement expression results coming from each partition
            let mut partition_ordering_values = vec![];

            // Existing values should be merged also.
            partition_values.push(self.values.clone());
            partition_ordering_values.push(self.ordering_values.clone());

            let array_agg_res =
                ScalarValue::convert_array_to_scalar_vec(array_agg_values)?;

            for v in array_agg_res.into_iter() {
                partition_values.push(v);
            }

            let orderings = ScalarValue::convert_array_to_scalar_vec(agg_orderings)?;

            for partition_ordering_rows in orderings.into_iter() {
                // Extract value from struct to ordering_rows for each group/partition
                let ordering_value = partition_ordering_rows.into_iter().map(|ordering_row| {
                    if let ScalarValue::Struct(Some(ordering_columns_per_row), _) = ordering_row {
                        Ok(ordering_columns_per_row)
                    } else {
                        exec_err!(
                                "Expects to receive ScalarValue::Struct(Some(..), _) but got:{:?}",
                                ordering_row.data_type()
                            )
                    }
                }).collect::<Result<Vec<_>>>()?;

                partition_ordering_values.push(ordering_value);
            }

            let sort_options = self
                .ordering_req
                .iter()
                .map(|sort_expr| sort_expr.options)
                .collect::<Vec<_>>();
            let (new_values, new_orderings) = merge_ordered_arrays(
                &partition_values,
                &partition_ordering_values,
                &sort_options,
            )?;
            self.values = new_values;
            self.ordering_values = new_orderings;
        } else {
            return exec_err!("Expects to receive a list array");
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.evaluate_values()?];
        if !self.ordering_req.is_empty() {
            result.push(self.evaluate_orderings()?);
        }
        Ok(result)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let n_required = self.n.unsigned_abs() as usize;
        let from_start = self.n > 0;
        let nth_value_idx = if from_start {
            // index is from start
            let forward_idx = n_required - 1;
            (forward_idx < self.values.len()).then_some(forward_idx)
        } else {
            // index is from end
            self.values.len().checked_sub(n_required)
        };
        if let Some(idx) = nth_value_idx {
            let nth_value = self.values[idx].clone();
            Ok(nth_value.clone())
        } else {
            ScalarValue::try_from(self.datatypes[0].clone())
        }
    }

    fn size(&self) -> usize {
        let mut total = std::mem::size_of_val(self)
            + ScalarValue::size_of_vec(&self.values)
            - std::mem::size_of_val(&self.values);

        // Add size of the `self.ordering_values`
        total +=
            std::mem::size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - std::mem::size_of_val(row);
        }

        // Add size of the `self.datatypes`
        total += std::mem::size_of::<DataType>() * self.datatypes.capacity();
        for dtype in &self.datatypes {
            total += dtype.size() - std::mem::size_of_val(dtype);
        }

        // Add size of the `self.ordering_req`
        total += std::mem::size_of::<PhysicalSortExpr>() * self.ordering_req.capacity();
        // TODO: Calculate size of each `PhysicalSortExpr` more accurately.
        total
    }
}

impl NthValueAccumulator {
    fn evaluate_orderings(&self) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.datatypes[1..]);
        let struct_field = Fields::from(fields.clone());

        let orderings: Vec<ScalarValue> = self
            .ordering_values
            .iter()
            .map(|ordering| {
                ScalarValue::Struct(Some(ordering.clone()), struct_field.clone())
            })
            .collect();
        let struct_type = DataType::Struct(Fields::from(fields));

        // Wrap in List, so we have the same data structure ListArray(StructArray..) for group by cases
        let arr = ScalarValue::new_list(&orderings, &struct_type);
        Ok(ScalarValue::List(arr))
    }

    fn evaluate_values(&self) -> Result<ScalarValue> {
        let arr = ScalarValue::new_list(&self.values, &self.datatypes[0]);
        Ok(ScalarValue::List(arr))
    }

    fn append_new_data(
        &mut self,
        values: &[ArrayRef],
        fetch: Option<usize>,
    ) -> Result<()> {
        let n_row = values[0].len();
        let n_to_add = if let Some(fetch) = fetch {
            std::cmp::min(fetch, n_row)
        } else {
            n_row
        };
        for index in 0..n_to_add {
            let row = get_row_at_idx(values, index)?;
            self.values.push(row[0].clone());
            self.ordering_values.push(row[1..].to_vec());
        }
        Ok(())
    }
}
