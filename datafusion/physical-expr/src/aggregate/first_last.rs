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

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// FIRST aggregate expression
#[derive(Debug)]
pub struct FirstAgg {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl FirstAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            expr,
        }
    }
}

impl AggregateExpr for FirstAgg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "first"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    // TODO: Add support for reverse expr

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(FirstAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for FirstAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct FirstAccumulator {
    first: ScalarValue,
    count: u64,
}

impl FirstAccumulator {
    /// new First accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            first: ScalarValue::try_from(data_type)?,
            count: 0,
        })
    }
}

impl Accumulator for FirstAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.first.clone(), ScalarValue::from(self.count)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // If we have seen first value, we shouldn't update it
        let values = &values[0];
        if self.count == 0 && values.len() > 0 {
            self.first = ScalarValue::try_from_array(values, 0)?;
        }
        self.count += (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count -= (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST(first1, first2, first3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if self.count == 0 {
            ScalarValue::try_from(&self.first.get_datatype())
        } else {
            Ok(self.first.clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.first)
            + self.first.size()
    }
}

/// LAST aggregate expression
#[derive(Debug)]
pub struct LastAgg {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl LastAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            data_type,
            expr,
        }
    }
}

impl AggregateExpr for LastAgg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "last"),
            self.data_type.clone(),
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    // TODO: Add support for reverse expr

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(LastAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for LastAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct LastAccumulator {
    last: ScalarValue,
    count: u64,
}

impl LastAccumulator {
    /// new First accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            last: ScalarValue::try_from(data_type)?,
            count: 0,
        })
    }
}

impl Accumulator for LastAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.last.clone(), ScalarValue::from(self.count)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // If we have seen first value, we shouldn't update it
        let values = &values[0];
        if values.len() > 0 {
            // Update with last value in the array.
            self.last = ScalarValue::try_from_array(values, values.len() - 1)?;
        }
        self.count += (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count -= (values.len() - values.null_count()) as u64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST(first1, first2, first3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if self.count == 0 {
            ScalarValue::try_from(&self.last.get_datatype())
        } else {
            Ok(self.last.clone())
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.last) + self.last.size()
    }
}
