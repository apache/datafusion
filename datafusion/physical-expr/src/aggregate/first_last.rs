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
use arrow_array::cast::AsArray;
use datafusion_common::utils::get_row_at_idx;
use std::any::Any;
use std::sync::Arc;

/// FIRST_VALUE aggregate expression
#[derive(Debug)]
pub struct FirstValue {
    name: String,
    pub data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl FirstValue {
    /// Creates a new FIRST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        ordering_req: LexOrdering,
        data_types: Vec<DataType>,
    ) -> Self {
        Self {
            name: name.into(),
            data_types,
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
        Ok(Field::new(&self.name, self.data_types[0].clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "first_value"),
            self.data_types[0].clone(),
            true,
        )];
        fields.extend(ordering_fields(&self.ordering_req, &self.data_types[1..]));
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
            self.ordering_req.clone(),
            self.data_types.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstValueAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
        )?))
    }
}

impl PartialEq<dyn Any> for FirstValue {
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
struct FirstValueAccumulator {
    first: ScalarValue,
    // At the beginning, `is_set` is `false`, this means `first` is not seen yet.
    // Once we see (`is_set=true`) first value, we do not update `first`.
    is_set: bool,
    orderings: Vec<ScalarValue>,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ordering_dtypes: &[DataType]) -> Result<Self> {
        let mut orderings = vec![];
        for dtype in ordering_dtypes.iter() {
            orderings.push(ScalarValue::try_from(dtype)?);
        }
        ScalarValue::try_from(data_type).map(|value| Self {
            first: value,
            is_set: false,
            orderings,
        })
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut res = vec![self.first.clone()];
        res.extend(self.orderings.clone());
        res.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(res)
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
        let last_idx = states.len() - 1;
        let is_set_flags = &states[last_idx];
        let mut filtered_first_vals = vec![];
        for state in states.iter().take(last_idx - 1) {
            filtered_first_vals.push(compute::filter(state, is_set_flags.as_boolean())?)
        }
        self.update_batch(&filtered_first_vals)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
    }
}

/// LAST_VALUE aggregate expression
#[derive(Debug)]
pub struct LastValue {
    name: String,
    pub data_types: Vec<DataType>,
    expr: Arc<dyn PhysicalExpr>,
    ordering_req: LexOrdering,
}

impl LastValue {
    /// Creates a new LAST_VALUE aggregation function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        ordering_req: LexOrdering,
        data_types: Vec<DataType>,
    ) -> Self {
        Self {
            name: name.into(),
            data_types,
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
        Ok(Field::new(&self.name, self.data_types[0].clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastValueAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        let mut fields = vec![Field::new(
            format_state_name(&self.name, "last_value"),
            self.data_types[0].clone(),
            true,
        )];
        fields.extend(ordering_fields(&self.ordering_req, &self.data_types[1..]));
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
            self.ordering_req.clone(),
            self.data_types.clone(),
        )))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastValueAccumulator::try_new(
            &self.data_types[0],
            &self.data_types[1..],
        )?))
    }
}

impl PartialEq<dyn Any> for LastValue {
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
struct LastValueAccumulator {
    last: ScalarValue,
    // `is_set` keeps track of whether last value is setted.
    // This information is used to discriminate genuine Nulls and Nulls that occur because of
    // empty partition.
    is_set: bool,
    orderings: Vec<ScalarValue>,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ordering_dtypes: &[DataType]) -> Result<Self> {
        let mut orderings = vec![];
        for dtype in ordering_dtypes {
            orderings.push(ScalarValue::try_from(dtype)?);
        }
        Ok(Self {
            last: ScalarValue::try_from(data_type)?,
            is_set: false,
            orderings,
        })
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut res = vec![self.last.clone()];
        res.extend(self.orderings.clone());
        res.push(ScalarValue::Boolean(Some(self.is_set)));
        Ok(res)
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
        let last_idx = states.len() - 1;
        let is_set_flags = &states[last_idx];
        let mut filtered_first_vals = vec![];
        for state in states.iter().take(last_idx - 1) {
            filtered_first_vals.push(compute::filter(state, is_set_flags.as_boolean())?)
        }
        self.update_batch(&filtered_first_vals)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.last) + self.last.size()
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
