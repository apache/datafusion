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

use std::any::Any;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::fmt::Debug;
use std::ops::BitOrAssign;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::{DataType, Field};
use arrow::ipc::LargeBinary;
use roaring::RoaringBitmap;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;

use super::format_state_name;

/// APPROX_DISTINCT aggregate expression
#[derive(Debug)]
pub struct BitMapDistinct {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl BitMapDistinct {
    /// Create a new ApproxDistinct aggregate function.
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            input_data_type,
            expr,
        }
    }
}

impl AggregateExpr for BitMapDistinct {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// the field of the final result of this aggregation.
    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, DataType::UInt64, false))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator: Box<dyn Accumulator> = match &self.input_data_type {
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32 => Box::new(BitmapDistinctCountAccumulator::try_new()),
            //DataType::UInt64 => Box::new(),
            //DataType::Int64 => Box::new(),
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Support for 'bitmap_distinct' for data type {} is not implemented",
                    other
                )))
            }
        };
        Ok(accumulator)
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "bitmap_registers"),
            DataType::LargeBinary,
            false,
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
struct BitmapDistinctCountAccumulator {
    bitmap: roaring::bitmap::RoaringBitmap,
}

impl BitmapDistinctCountAccumulator {
    fn try_new() -> Self {
        Self {
            bitmap: RoaringBitmap::new(),
        }
    }
}

impl Accumulator for BitmapDistinctCountAccumulator {
    //state() can be used by physical nodes to aggregate states together and send them over the network/threads, to combine values.
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let mut bytes = vec![];
        self.bitmap.serialize_into(&mut bytes).unwrap();
        Ok(vec![ScalarValue::LargeBinary(Option::from(bytes))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        //implement this in arrow-rs with simd
        let values = &values[0];
        let array = values
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("can not cast in BitmapDistinctCountAccumulator");
        for i in 0..array.len() {
            self.bitmap.insert(array.value(i));
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let bytes = states[0].as_any().downcast_ref::<LargeBinary>().unwrap();
        let bytes_vec: Vec<u8> = bytes.try_into().unwrap();
        let bitmap = RoaringBitmap::deserialize_from(&bytes_vec).unwrap();
        self.bitmap.bitor_assign(bitmap);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::from(self.bitmap.len()))
    }
}

pub(crate) fn is_bitmap_count_distinct_supported_arg_type(arg_type: &DataType) -> bool {
    matches!(
        arg_type,
        DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
    )
}
