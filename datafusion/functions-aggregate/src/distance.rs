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

//! [`Distance`]: Euclidean distance aggregations.

use std::any::Any;
use std::fmt::Debug;

use arrow::compute::kernels::cast;
use arrow::compute::{and, filter, is_not_null};
use arrow::{
    array::{ArrayRef, Float64Array},
    datatypes::{DataType, Field},
};

use datafusion_common::{
    downcast_value, plan_err, unwrap_or_internal_err, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::{
    function::{AccumulatorArgs, StateFieldsArgs},
    type_coercion::aggregates::NUMERICS,
    utils::format_state_name,
    Accumulator, AggregateUDFImpl, Signature, Volatility,
};

make_udaf_expr_and_func!(
    Distance,
    dis,
    y x,
    "Distance between two numeric values.",
    dis_udaf
);

#[derive(Debug)]
pub struct Distance {
    signature: Signature,
}

impl Default for Distance {
    fn default() -> Self {
        Self::new()
    }
}

impl Distance {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, NUMERICS.to_vec(), Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for Distance {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "distance"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if !arg_types[0].is_numeric() {
            return plan_err!("Distance requires numeric input types");
        }

        Ok(DataType::Float64)
    }
    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistanceAccumulator::try_new()?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        let name = args.name;
        Ok(vec![Field::new(
            format_state_name(name, "sum_of_squares"),
            DataType::Float64,
            true,
        )])
    }
}

/// An accumulator to compute distance of two numeric columns
#[derive(Debug)]
pub struct DistanceAccumulator {
    sum_of_squares: f64,
}

impl DistanceAccumulator {
    /// Creates a new `DistanceAccumulator`
    pub fn try_new() -> Result<Self> {
        Ok(Self {
            sum_of_squares: 0_f64,
        })
    }
}

impl Accumulator for DistanceAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.sum_of_squares)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;

        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };
            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);

            self.sum_of_squares += (value1 - value2).powi(2);
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = if values[0].null_count() != 0 || values[1].null_count() != 0 {
            let mask = and(&is_not_null(&values[0])?, &is_not_null(&values[1])?)?;
            let values1 = filter(&values[0], &mask)?;
            let values2 = filter(&values[1], &mask)?;

            vec![values1, values2]
        } else {
            values.to_vec()
        };

        let values1 = &cast(&values[0], &DataType::Float64)?;
        let values2 = &cast(&values[1], &DataType::Float64)?;
        let mut arr1 = downcast_value!(values1, Float64Array).iter().flatten();
        let mut arr2 = downcast_value!(values2, Float64Array).iter().flatten();

        for i in 0..values1.len() {
            let value1 = if values1.is_valid(i) {
                arr1.next()
            } else {
                None
            };
            let value2 = if values2.is_valid(i) {
                arr2.next()
            } else {
                None
            };

            if value1.is_none() || value2.is_none() {
                continue;
            }

            let value1 = unwrap_or_internal_err!(value1);
            let value2 = unwrap_or_internal_err!(value2);

            let diff = value1 - value2;
            self.sum_of_squares -= diff.powi(2);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let list_sum_of_squares = downcast_value!(states[0], Float64Array);
        for i in 0..list_sum_of_squares.len() {
            self.sum_of_squares += list_sum_of_squares.value(i);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Float64(Some(self.sum_of_squares.sqrt())))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}
