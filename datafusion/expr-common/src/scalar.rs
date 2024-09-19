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

use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};

#[derive(Clone, Debug)]
pub struct Scalar {
    value: ScalarValue,
    data_type: DataType,
}

impl From<ScalarValue> for Scalar {
    fn from(value: ScalarValue) -> Self {
        Self {
            data_type: value.data_type(),
            value,
        }
    }
}

impl TryFrom<DataType> for Scalar {
    type Error = DataFusionError;
    fn try_from(value: DataType) -> Result<Self> {
        Ok(Self {
            value: ScalarValue::try_from(&value)?,
            data_type: value,
        })
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl Scalar {
    pub fn new(value: ScalarValue, data_type: DataType) -> Self {
        Self { value, data_type }
    }

    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        let value = ScalarValue::try_from_array(array, index)?;
        Ok(Self::new(value, array.data_type().clone()))
    }

    #[inline]
    pub fn value(&self) -> &ScalarValue {
        &self.value
    }

    #[inline]
    pub fn into_value(self) -> ScalarValue {
        self.value
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    #[inline]
    pub fn to_array_of_size(&self, size: usize) -> Result<ArrayRef> {
        self.value.to_array_of_size_and_type(size, &self.data_type)
    }

    #[inline]
    pub fn to_array(&self) -> Result<ArrayRef> {
        self.to_array_of_size(1)
    }

    pub fn to_scalar(&self) -> Result<arrow::array::Scalar<ArrayRef>> {
        Ok(arrow::array::Scalar::new(self.to_array()?))
    }

    pub fn iter_to_array(scalars: impl IntoIterator<Item = Scalar>) -> Result<ArrayRef> {
        let mut scalars = scalars.into_iter().peekable();

        // figure out the type based on the first element
        let data_type = match scalars.peek() {
            None => return exec_err!("Empty iterator passed to Scalar::iter_to_array"),
            Some(sv) => sv.data_type().clone(),
        };

        ScalarValue::iter_to_array_of_type(scalars.map(|scalar| scalar.value), &data_type)
    }
}
