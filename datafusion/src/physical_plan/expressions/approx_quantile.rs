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

use std::{any::Any, iter, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Field},
};

use crate::{
    error::DataFusionError,
    physical_plan::{tdigest::TDigest, Accumulator, AggregateExpr, PhysicalExpr},
    scalar::ScalarValue,
};

use crate::error::Result;

use super::{format_state_name, Literal};

/// Return `true` if `arg_type` is of a [`DataType`] that the [`ApproxQuantile`]
/// aggregation can operate on.
pub fn is_approx_quantile_supported_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
    )
}

/// APPROX_QUANTILE aggregate expression
#[derive(Debug)]
pub struct ApproxQuantile {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    quantile: f64,
}

impl ApproxQuantile {
    /// Create a new ApproxQuantile aggregate function.
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredQuantileLiteral]
        debug_assert_eq!(expr.len(), 2);

        // Extract the desired quantile literal
        let lit = expr[1]
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or(DataFusionError::Internal(
                "desired quantile argument must be float literal".to_string(),
            ))?
            .value();
        let quantile = match lit {
            ScalarValue::Float32(Some(q)) => *q as f64,
            ScalarValue::Float64(Some(q)) => *q as f64,
            got => return Err(DataFusionError::NotImplemented(format!(
                "Quantile value for 'APPROX_QUANTILE' must be Float32 or Float64 literal (got data type {})",
                got
            )))
        };

        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr: expr[0].clone(),
            quantile,
        })
    }
}

impl AggregateExpr for ApproxQuantile {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), false))
    }

    /// See [`TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                &format_state_name(&self.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new(
                &format_state_name(&self.name, "centroids"),
                DataType::List(Box::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator: Box<dyn Accumulator> = match &self.input_data_type {
            t @ (DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64) => {
                Box::new(ApproxQuantileAccumulator::new(self.quantile, t.clone()))
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Support for 'APPROX_QUANTILE' for data type {} is not implemented",
                    other
                )))
            }
        };
        Ok(accumulator)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub struct ApproxQuantileAccumulator {
    digest: TDigest,
    quantile: f64,
    return_type: DataType,
}

impl ApproxQuantileAccumulator {
    pub fn new(quantile: f64, return_type: DataType) -> Self {
        Self {
            digest: TDigest::new(100),
            quantile,
            return_type,
        }
    }
}

impl Accumulator for ApproxQuantileAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(self.digest.to_scalar_state())
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        debug_assert_eq!(
            values.len(),
            1,
            "invalid number of values in batch quantile update"
        );
        let values = &values[0];

        self.digest = match values.data_type() {
            DataType::Float64 => {
                let array = values.as_any().downcast_ref::<Float64Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::Float32 => {
                let array = values.as_any().downcast_ref::<Float32Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::Int64 => {
                let array = values.as_any().downcast_ref::<Int64Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::Int32 => {
                let array = values.as_any().downcast_ref::<Int32Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::Int16 => {
                let array = values.as_any().downcast_ref::<Int16Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::Int8 => {
                let array = values.as_any().downcast_ref::<Int8Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::UInt64 => {
                let array = values.as_any().downcast_ref::<UInt64Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::UInt32 => {
                let array = values.as_any().downcast_ref::<UInt32Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::UInt16 => {
                let array = values.as_any().downcast_ref::<UInt16Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            DataType::UInt8 => {
                let array = values.as_any().downcast_ref::<UInt8Array>().unwrap();
                self.digest.merge_unsorted(array.values().iter().cloned())?
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "APPROX_QUANTILE is not expected to receive the type {:?}",
                    e
                )));
            }
        };

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let q = self.digest.estimate_quantile(self.quantile);

        // These acceptable return types MUST match the validation in
        // ApproxQuantile::create_accumulator.
        Ok(match &self.return_type {
            DataType::Int8 => ScalarValue::Int8(Some(q as i8)),
            DataType::Int16 => ScalarValue::Int16(Some(q as i16)),
            DataType::Int32 => ScalarValue::Int32(Some(q as i32)),
            DataType::Int64 => ScalarValue::Int64(Some(q as i64)),
            DataType::UInt8 => ScalarValue::UInt8(Some(q as u8)),
            DataType::UInt16 => ScalarValue::UInt16(Some(q as u16)),
            DataType::UInt32 => ScalarValue::UInt32(Some(q as u32)),
            DataType::UInt64 => ScalarValue::UInt64(Some(q as u64)),
            DataType::Float32 => ScalarValue::Float32(Some(q as f32)),
            DataType::Float64 => ScalarValue::Float64(Some(q as f64)),
            v => unreachable!("unexpected return type {:?}", v),
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        let states = (0..states[0].len())
            .map(|index| {
                states
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, index))
                    .collect::<Result<Vec<_>>>()
                    .map(|state| TDigest::from_scalar_state(&state))
            })
            .chain(iter::once(Ok(self.digest.clone())))
            .collect::<Result<Vec<_>>>()?;

        self.digest = TDigest::merge_digests(&states);

        Ok(())
    }
}
