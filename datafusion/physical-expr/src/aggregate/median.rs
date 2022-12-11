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

//! # Median

use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::sort_to_indices;
use arrow::datatypes::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, AggregateState};
use std::any::Any;
use std::sync::Arc;

/// MEDIAN aggregate expression. This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_MEDIAN provides a much more efficient solution.
#[derive(Debug)]
pub struct Median {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl Median {
    /// Create a new MEDIAN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for Median {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MedianAccumulator {
            data_type: self.data_type.clone(),
            all_values: vec![],
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        //Intermediate state is a list of the elements we have collected so far
        let field = Field::new("item", self.data_type.clone(), true);
        let data_type = DataType::List(Box::new(field));

        Ok(vec![Field::new(
            &format_state_name(&self.name, "median"),
            data_type,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
/// The median accumulator accumulates the raw input values
/// as `ScalarValue`s
///
/// The intermediate state is represented as a List of those scalars
struct MedianAccumulator {
    data_type: DataType,
    all_values: Vec<ScalarValue>,
}

impl Accumulator for MedianAccumulator {
    fn state(&self) -> Result<Vec<AggregateState>> {
        let state =
            ScalarValue::new_list(Some(self.all_values.clone()), self.data_type.clone());
        Ok(vec![AggregateState::Scalar(state)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1);
        let array = &values[0];

        assert_eq!(array.data_type(), &self.data_type);
        self.all_values.reserve(self.all_values.len() + array.len());
        for index in 0..array.len() {
            self.all_values
                .push(ScalarValue::try_from_array(array, index)?);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 1);

        let array = &states[0];
        assert!(matches!(array.data_type(), DataType::List(_)));
        for index in 0..array.len() {
            match ScalarValue::try_from_array(array, index)? {
                ScalarValue::List(Some(mut values), _) => {
                    self.all_values.append(&mut values);
                }
                ScalarValue::List(None, _) => {} // skip empty state
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected state in median. Expected DataType::List, got {:?}",
                        v
                    )))
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // Create an array of all the non null values and find the
        // sorted indexes
        let array = ScalarValue::iter_to_array(
            self.all_values
                .iter()
                // ignore null values
                .filter(|v| !v.is_null())
                .cloned(),
        )?;

        // find the mid point
        let len = array.len();
        let mid = len / 2;

        // only sort up to the top size/2 elements
        let limit = Some(mid + 1);
        let options = None;
        let indices = sort_to_indices(&array, options, limit)?;

        // pick the relevant indices in the original arrays
        let result = if len >= 2 && len % 2 == 0 {
            // even number of values, average the two mid points
            let s1 = scalar_at_index(&array, &indices, mid - 1)?;
            let s2 = scalar_at_index(&array, &indices, mid)?;
            match s1.add(s2)? {
                ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(v / 2)),
                ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(v / 2)),
                ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(v / 2)),
                ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(v / 2)),
                ScalarValue::UInt8(Some(v)) => ScalarValue::UInt8(Some(v / 2)),
                ScalarValue::UInt16(Some(v)) => ScalarValue::UInt16(Some(v / 2)),
                ScalarValue::UInt32(Some(v)) => ScalarValue::UInt32(Some(v / 2)),
                ScalarValue::UInt64(Some(v)) => ScalarValue::UInt64(Some(v / 2)),
                ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(v / 2.0)),
                ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(v / 2.0)),
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported type in MedianAccumulator: {:?}",
                        v
                    )))
                }
            }
        } else {
            // odd number of values, pick that one
            scalar_at_index(&array, &indices, mid)?
        };

        Ok(result)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.all_values)
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

/// Given a returns `array[indicies[indicie_index]]` as a `ScalarValue`
fn scalar_at_index(
    array: &dyn Array,
    indices: &UInt32Array,
    indicies_index: usize,
) -> Result<ScalarValue> {
    let array_index = indices
        .value(indicies_index)
        .try_into()
        .expect("Convert uint32 to usize");
    ScalarValue::try_from_array(array, array_index)
}
