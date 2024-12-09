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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{as_primitive_array, ArrayAccessor, ArrayIter, Float32Array, Float64Array},
    datatypes::{ArrowNativeType, Float32Type, Float64Type},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr_common::physical_expr::down_cast_any_ref;
use datafusion_physical_expr::PhysicalExpr;

#[derive(Debug, Hash)]
pub struct NormalizeNaNAndZero {
    pub data_type: DataType,
    pub child: Arc<dyn PhysicalExpr>,
}

impl NormalizeNaNAndZero {
    pub fn new(data_type: DataType, child: Arc<dyn PhysicalExpr>) -> Self {
        Self { data_type, child }
    }
}

impl PhysicalExpr for NormalizeNaNAndZero {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> datafusion_common::Result<bool> {
        self.child.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let cv = self.child.evaluate(batch)?;
        let array = cv.into_array(batch.num_rows())?;

        match &self.data_type {
            DataType::Float32 => {
                let v = eval_typed(as_primitive_array::<Float32Type>(&array));
                let new_array = Float32Array::from(v);
                Ok(ColumnarValue::Array(Arc::new(new_array)))
            }
            DataType::Float64 => {
                let v = eval_typed(as_primitive_array::<Float64Type>(&array));
                let new_array = Float64Array::from(v);
                Ok(ColumnarValue::Array(Arc::new(new_array)))
            }
            dt => panic!("Unexpected data type {:?}", dt),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.child.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(NormalizeNaNAndZero::new(
            self.data_type.clone(),
            Arc::clone(&children[0]),
        )))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.child.hash(&mut s);
        self.data_type.hash(&mut s);
        self.hash(&mut s);
    }
}

fn eval_typed<V: FloatDouble, T: ArrayAccessor<Item = V>>(input: T) -> Vec<Option<V>> {
    let iter = ArrayIter::new(input);
    iter.map(|o| {
        o.map(|v| {
            if v.is_nan() {
                v.nan()
            } else if v.is_neg_zero() {
                v.zero()
            } else {
                v
            }
        })
    })
    .collect()
}

impl Display for NormalizeNaNAndZero {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FloatNormalize [child: {}]", self.child)
    }
}

impl PartialEq<dyn Any> for NormalizeNaNAndZero {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.child.eq(&x.child) && self.data_type.eq(&x.data_type))
            .unwrap_or(false)
    }
}

trait FloatDouble: ArrowNativeType {
    fn is_nan(&self) -> bool;
    fn nan(&self) -> Self;
    fn is_neg_zero(&self) -> bool;
    fn zero(&self) -> Self;
}

impl FloatDouble for f32 {
    fn is_nan(&self) -> bool {
        f32::is_nan(*self)
    }
    fn nan(&self) -> Self {
        f32::NAN
    }
    fn is_neg_zero(&self) -> bool {
        *self == -0.0
    }
    fn zero(&self) -> Self {
        0.0
    }
}
impl FloatDouble for f64 {
    fn is_nan(&self) -> bool {
        f64::is_nan(*self)
    }
    fn nan(&self) -> Self {
        f64::NAN
    }
    fn is_neg_zero(&self) -> bool {
        *self == -0.0
    }
    fn zero(&self) -> Self {
        0.0
    }
}
