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

use core::fmt;
use std::hash::Hash;

use arrow::{
    array::{Array, ArrayRef},
    datatypes::DataType,
};
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};

/// Represents a physical scalar value obtained after evaluating an expression.
///
/// Currently [`Scalar`] represents a no-op wrapper around scalar value. This
/// is due to the fact that the current variants of [`ScalarValue`] correspond
/// 1:1 with the variants of [`DataType`]. The goal of this type is aid the
/// process of condensing some of the variants (e.g. [`ScalarValue::LargeUtf8`],
/// [`ScalarValue::Utf8View`], and [`ScalarValue::Utf8`]) into a single variant
/// (e.g. [`ScalarValue::Utf8`]). This type achieves this goal by storing the
/// physical type as [`DataType`] alongside the value represented as
/// [`ScalarValue`].
///
/// [`Scalar`] cannot be constructed directly as not all [`DataType`] variants
/// can correctly be represented by a [`ScalarValue`] without casting. For more
/// information see:
/// - [`Scalar::from`]
/// - [`Scalar::try_from_array`]
/// - [`Scalar::new_null_of`]
///
/// [`Scalar`] is meant to be stored as a variant of [`crate::columnar_value::ColumnarValue`].
#[derive(Clone, Eq)]
pub struct Scalar {
    pub value: ScalarValue,
    data_type: DataType,
}

impl From<ScalarValue> for Scalar {
    /// Converts a [`ScalarValue`] to a Scalar, resolving the [`DataType`] from
    /// the value itself.
    fn from(value: ScalarValue) -> Self {
        Self {
            data_type: value.data_type(),
            value,
        }
    }
}

impl<T: Array> TryFrom<arrow::array::Scalar<T>> for Scalar {
    type Error = DataFusionError;

    /// Converts a [`arrow::array::Scalar`] to a Scalar, resolving the
    /// [`DataType`] from the inner scalar array.
    ///
    /// See [`Scalar::try_from_array`].
    fn try_from(value: arrow::array::Scalar<T>) -> Result<Self> {
        let array = value.into_inner();
        Self::try_from_array(&array, 0)
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value)
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Hash for Scalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl fmt::Debug for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl Scalar {
    /// Converts a value in `array` at `index` into a Scalar, resolving the
    /// [`DataType`] from the `array`'s data type.
    ///
    /// Once [`ScalarValue`] variants will be condensed, this method will prove
    /// very useful in order to keep track of the wanted data type (sourced from
    /// `array`) as the type information might be lost when simply calling
    /// [`ScalarValue::try_from_array`].
    pub fn try_from_array(array: &dyn Array, index: usize) -> Result<Self> {
        let data_type = array.data_type().clone();
        let value = ScalarValue::try_from_array(array, index)?;
        Ok(Self { value, data_type })
    }

    /// Creates a null Scalar of the specified `data_type`.
    ///
    /// Note that the null `value` stored inside the Scalar might be of a
    /// logically equivalent type of `data_type`, but the type information
    /// provided will not be lost as it will be stored alongside the value.
    pub fn new_null_of(data_type: DataType) -> Result<Self> {
        Ok(Self {
            value: ScalarValue::try_from(&data_type)?,
            data_type,
        })
    }

    /// Converts an iterator of references [`Scalar`] into an [`ArrayRef`]
    /// corresponding to those values.
    pub fn iter_to_array(scalars: impl IntoIterator<Item = Self>) -> Result<ArrayRef> {
        let mut scalars = scalars.into_iter().peekable();

        // figure out the type based on the first element
        let data_type = match scalars.peek() {
            None => return exec_err!("Empty iterator passed to Scalar::iter_to_array"),
            Some(sv) => sv.data_type().clone(),
        };

        ScalarValue::iter_to_array_of_type(scalars.map(|scalar| scalar.value), &data_type)
    }

    pub fn cast_to(&self, data_type: &DataType) -> Result<Self> {
        Ok(Self::from(self.value.cast_to(data_type)?))
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
}
