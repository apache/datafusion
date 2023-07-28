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

//! Defines the FIRST_VALUE/LAST_VALUE aggregations.

use crate::aggregate::utils::{down_cast_any_ref, ordering_fields};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, LexOrdering, PhysicalExpr, PhysicalSortExpr};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;

use arrow::compute;
use arrow::compute::{lexsort_to_indices, SortColumn};
use arrow_array::cast::AsArray;
use arrow_schema::SortOptions;
use datafusion_common::utils::{compare_rows, get_row_at_idx};
use std::any::Any;
use std::sync::Arc;

/// FIRST_VALUE aggregate expression
#[derive(Debug)]
pub struct FirstValue {
    name: String,
    input_data_type: DataType,
    order_by_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl FirstValue {
    /// Creates a new FIRST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        ordering_req: LexOrdering,
        order_by_data_types: Vec<DataType>,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            order_by_data_types,
            expr,
            ordering_req,
        }
    }
}

impl AggregateExpr for FirstValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "first_value"),
            self.input_data_type.clone(),
            true,
        )];
        fields.extend(ordering_fields(
            &self.ordering_req,
            &self.order_by_data_types,
        ));
        fields.push(Field::new(
            format_state_name(&self.name, "is_set"),
            DataType::Boolean,
            true,
        ));
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
        let name = if self.name.starts_with("FIRST") {
            format!("LAST{}", &self.name[5..])
        } else {
            format!("LAST_VALUE({})", self.expr)
        };
        Some(Arc::new(LastValue::new(
            self.expr.clone(),
            name,
            self.input_data_type.clone(),
            self.ordering_req.clone(),
            self.order_by_data_types.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
        )?))
    }
}

impl PartialEq<dyn Any> for FirstValue {
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
struct FirstValueAccumulator {
    first: ScalarValue,
    // At the beginning, `is_set` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_set` flag and do not update `first` anymore.
    is_set: bool,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator. These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ordering_dtypes: &[DataType]) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        ScalarValue::try_from(data_type).map(|value| Self {
            first: value,
            is_set: false,
            orderings,
        })
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.first.clone()];
        result.extend(self.orderings.iter().cloned());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // If we have seen first value, we shouldn't update it
        if !values[0].is_empty() && !self.is_set {
            let row = get_row_at_idx(values, 0)?;
            // Update with last value in the array.
            self.first = row[0].clone();
            self.orderings = row[1..].to_vec();
            self.is_set = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        let is_set_idx = states.len() - 1;
        let is_set_flags = &states[is_set_idx];
        let flags = is_set_flags.as_boolean();
        let mut filtered_first_vals = vec![];
        for state in states.iter() {
            filtered_first_vals.push(compute::filter(state, flags)?)
        }
        let filtered_first_vals = compute::filter(&states[0], flags)?;
        // This range corresponds to ordering values
        let filtered_orderings = states[1..is_set_idx]
            .iter()
            .map(|state| compute::filter(state, flags).unwrap())
            .collect::<Vec<_>>();
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let sort_cols = filtered_orderings
            .iter()
            .map(|item| SortColumn {
                values: item.clone(),
                options: Some(sort_options.clone()),
            })
            .collect::<Vec<_>>();
        let indices = lexsort_to_indices(&sort_cols, None)?;

        let ordered_first_vals = compute::take(&filtered_first_vals, &indices, None)?;
        let orderings = filtered_orderings
            .iter()
            .map(|item| compute::take(&filtered_first_vals, &indices, None).unwrap())
            .collect::<Vec<_>>();

        let mut args = vec![];
        args.push(ordered_first_vals);
        args.extend(orderings);
        if !args[0].is_empty() {
            let first_row = get_row_at_idx(&args, 0)?;
            let first_val = first_row[0].clone();
            let first_ordering = first_row[1..].to_vec();
            if !self.is_set
                || compare_rows(&first_ordering, &self.orderings, &[sort_options])?
                    .is_lt()
            {
                self.first = first_val;
                self.orderings = first_ordering;
                self.is_set = true;
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - std::mem::size_of_val(&self.orderings)
    }
}

/// LAST_VALUE aggregate expression
#[derive(Debug)]
pub struct LastValue {
    name: String,
    input_data_type: DataType,
    order_by_data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl LastValue {
    /// Creates a new LAST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        ordering_req: LexOrdering,
        order_by_data_types: Vec<DataType>,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            order_by_data_types,
            expr,
            ordering_req,
        }
    }
}

impl AggregateExpr for LastValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "last_value"),
            self.input_data_type.clone(),
            true,
        )];
        fields.extend(ordering_fields(
            &self.ordering_req,
            &self.order_by_data_types,
        ));
        fields.push(Field::new(
            format_state_name(&self.name, "is_set"),
            DataType::Boolean,
            true,
        ));
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
        let name = if self.name.starts_with("LAST") {
            format!("FIRST{}", &self.name[4..])
        } else {
            format!("FIRST_VALUE({})", self.expr)
        };
        Some(Arc::new(FirstValue::new(
            self.expr.clone(),
            name,
            self.input_data_type.clone(),
            self.ordering_req.clone(),
            self.order_by_data_types.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastValueAccumulator::try_new(
            &self.input_data_type,
            &self.order_by_data_types,
        )?))
    }
}

impl PartialEq<dyn Any> for LastValue {
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
struct LastValueAccumulator {
    last: ScalarValue,
    // The `is_set` flag keeps track of whether the last value is finalized.
    // This information is used to discriminate genuine NULLs and NULLS that
    // occur due to empty partitions.
    is_set: bool,
    orderings: Vec<ScalarValue>,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ordering_dtypes: &[DataType]) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            last: ScalarValue::try_from(data_type)?,
            is_set: false,
            orderings,
        })
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.last.clone()];
        result.extend(self.orderings.clone());
        result.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !values[0].is_empty() {
            let row = get_row_at_idx(values, values[0].len() - 1)?;
            // Update with last value in the array.
            self.last = row[0].clone();
            self.orderings = row[1..].to_vec();
            self.is_set = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        let is_set_idx = states.len() - 1;
        let is_set_flags = &states[is_set_idx];
        let flags = is_set_flags.as_boolean();
        let mut filtered_first_vals = vec![];
        for state in states.iter() {
            filtered_first_vals.push(compute::filter(state, flags)?)
        }
        let filtered_first_vals = compute::filter(&states[0], flags)?;
        // Corresponds to ordering section
        let filtered_orderings = states[1..is_set_idx]
            .iter()
            .map(|state| compute::filter(state, flags).unwrap())
            .collect::<Vec<_>>();
        let sort_options = SortOptions {
            descending: false,
            nulls_first: false,
        };

        let sort_cols = filtered_orderings
            .iter()
            .map(|item| SortColumn {
                values: item.clone(),
                options: Some(sort_options.clone()),
            })
            .collect::<Vec<_>>();
        let indices = lexsort_to_indices(&sort_cols, None)?;

        let ordered_first_vals = compute::take(&filtered_first_vals, &indices, None)?;
        let orderings = filtered_orderings
            .iter()
            .map(|item| compute::take(&filtered_first_vals, &indices, None).unwrap())
            .collect::<Vec<_>>();

        let mut args = vec![];
        args.push(ordered_first_vals);
        args.extend(orderings);
        if !args[0].is_empty() {
            let last_idx = args[0].len() - 1;
            let last_row = get_row_at_idx(&args, last_idx)?;
            let last_val = last_row[0].clone();
            let last_ordering = last_row[1..].to_vec();
            if !self.is_set
                || compare_rows(&last_ordering, &self.orderings, &[sort_options])?.is_gt()
            {
                self.last = last_val;
                self.orderings = last_ordering;
                self.is_set = true;
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.last)
            + self.last.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - std::mem::size_of_val(&self.orderings)
    }
}

#[cfg(test)]
mod tests {
    use crate::aggregate::first_last::{FirstValueAccumulator, LastValueAccumulator};
    use arrow_array::{ArrayRef, Int64Array};
    use arrow_schema::DataType;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::Accumulator;
    use std::sync::Arc;

    #[test]
    fn test_first_last_value_value() -> Result<()> {
        let mut first_accumulator =
            FirstValueAccumulator::try_new(&DataType::Int64, &[])?;
        let mut last_accumulator = LastValueAccumulator::try_new(&DataType::Int64, &[])?;
        // first value in the tuple is start of the range (inclusive),
        // second value in the tuple is end of the range (exclusive)
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())) as ArrayRef
            })
            .collect::<Vec<_>>();
        for arr in arrs {
            // Once first_value is set, accumulator should remember it.
            // It shouldn't update first_value for each new batch
            first_accumulator.update_batch(&[arr.clone()])?;
            // last_value should be updated for each new batch.
            last_accumulator.update_batch(&[arr])?;
        }
        // First Value comes from the first value of the first batch which is 0
        assert_eq!(first_accumulator.evaluate()?, ScalarValue::Int64(Some(0)));
        // Last value comes from the last value of the last batch which is 12
        assert_eq!(last_accumulator.evaluate()?, ScalarValue::Int64(Some(12)));
        Ok(())
    }
}
