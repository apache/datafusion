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

use std::convert::TryInto;
use std::{any::Any, sync::Arc};

use arrow::array::TimestampMillisecondArray;
use arrow::array::*;
use arrow::compute::kernels::arithmetic::{
    add, add_scalar, divide, divide_scalar, modulus, modulus_scalar, multiply,
    multiply_scalar, subtract, subtract_scalar,
};
use arrow::compute::kernels::boolean::{and_kleene, not, or_kleene};
use arrow::compute::kernels::comparison::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::kernels::comparison::{
    eq_bool, eq_bool_scalar, gt_bool, gt_bool_scalar, gt_eq_bool, gt_eq_bool_scalar,
    lt_bool, lt_bool_scalar, lt_eq_bool, lt_eq_bool_scalar, neq_bool, neq_bool_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_dyn_bool_scalar, gt_dyn_bool_scalar, gt_eq_dyn_bool_scalar, lt_dyn_bool_scalar,
    lt_eq_dyn_bool_scalar, neq_dyn_bool_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_dyn_scalar, gt_dyn_scalar, gt_eq_dyn_scalar, lt_dyn_scalar, lt_eq_dyn_scalar,
    neq_dyn_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_dyn_utf8_scalar, gt_dyn_utf8_scalar, gt_eq_dyn_utf8_scalar, lt_dyn_utf8_scalar,
    lt_eq_dyn_utf8_scalar, neq_dyn_utf8_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_scalar, gt_eq_scalar, gt_scalar, lt_eq_scalar, lt_scalar, neq_scalar,
};
use arrow::compute::kernels::comparison::{
    eq_utf8, gt_eq_utf8, gt_utf8, like_utf8, lt_eq_utf8, lt_utf8, neq_utf8, nlike_utf8,
    regexp_is_match_utf8,
};
use arrow::compute::kernels::comparison::{
    eq_utf8_scalar, gt_eq_utf8_scalar, gt_utf8_scalar, like_utf8_scalar,
    lt_eq_utf8_scalar, lt_utf8_scalar, neq_utf8_scalar, nlike_utf8_scalar,
    regexp_is_match_utf8_scalar,
};
use arrow::datatypes::{ArrowNumericType, DataType, Schema, TimeUnit};
use arrow::error::ArrowError::DivideByZero;
use arrow::record_batch::RecordBatch;

use crate::error::{DataFusionError, Result};
use crate::logical_plan::Operator;
use crate::physical_plan::coercion_rule::binary_rule::coerce_types;
use crate::physical_plan::expressions::try_cast;
use crate::physical_plan::{ColumnarValue, PhysicalExpr};
use crate::scalar::ScalarValue;

// Simple (low performance) kernels until optimized kernels are added to arrow
// See https://github.com/apache/arrow-rs/issues/960

fn is_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    // Different from `neq_bool` because `null is distinct from null` is false and not null
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left != right))
        .collect())
}

fn is_not_distinct_from_bool(
    left: &BooleanArray,
    right: &BooleanArray,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| Some(left == right))
        .collect())
}

// TODO add iter for decimal array
// TODO move this to arrow-rs
// https://github.com/apache/arrow-rs/issues/1083
pub(super) fn eq_decimal_scalar(
    left: &DecimalArray,
    right: i128,
) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) == right)?;
        }
    }
    Ok(bool_builder.finish())
}

pub(super) fn eq_decimal(
    left: &DecimalArray,
    right: &DecimalArray,
) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) == right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn neq_decimal_scalar(left: &DecimalArray, right: i128) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) != right)?;
        }
    }
    Ok(bool_builder.finish())
}

fn neq_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) != right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn lt_decimal_scalar(left: &DecimalArray, right: i128) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) < right)?;
        }
    }
    Ok(bool_builder.finish())
}

fn lt_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) < right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn lt_eq_decimal_scalar(left: &DecimalArray, right: i128) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) <= right)?;
        }
    }
    Ok(bool_builder.finish())
}

fn lt_eq_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) <= right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn gt_decimal_scalar(left: &DecimalArray, right: i128) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) > right)?;
        }
    }
    Ok(bool_builder.finish())
}

fn gt_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) > right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn gt_eq_decimal_scalar(left: &DecimalArray, right: i128) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) >= right)?;
        }
    }
    Ok(bool_builder.finish())
}

fn gt_eq_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            bool_builder.append_null()?;
        } else {
            bool_builder.append_value(left.value(i) >= right.value(i))?;
        }
    }
    Ok(bool_builder.finish())
}

fn is_distinct_from_decimal(
    left: &DecimalArray,
    right: &DecimalArray,
) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        match (left.is_null(i), right.is_null(i)) {
            (true, true) => bool_builder.append_value(false)?,
            (true, false) | (false, true) => bool_builder.append_value(true)?,
            (_, _) => bool_builder.append_value(left.value(i) != right.value(i))?,
        }
    }
    Ok(bool_builder.finish())
}

fn is_not_distinct_from_decimal(
    left: &DecimalArray,
    right: &DecimalArray,
) -> Result<BooleanArray> {
    let mut bool_builder = BooleanBuilder::new(left.len());
    for i in 0..left.len() {
        match (left.is_null(i), right.is_null(i)) {
            (true, true) => bool_builder.append_value(true)?,
            (true, false) | (false, true) => bool_builder.append_value(false)?,
            (_, _) => bool_builder.append_value(left.value(i) == right.value(i))?,
        }
    }
    Ok(bool_builder.finish())
}

fn add_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<DecimalArray> {
    let mut decimal_builder =
        DecimalBuilder::new(left.len(), left.precision(), left.scale());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            decimal_builder.append_null()?;
        } else {
            decimal_builder.append_value(left.value(i) + right.value(i))?;
        }
    }
    Ok(decimal_builder.finish())
}

fn subtract_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<DecimalArray> {
    let mut decimal_builder =
        DecimalBuilder::new(left.len(), left.precision(), left.scale());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            decimal_builder.append_null()?;
        } else {
            decimal_builder.append_value(left.value(i) - right.value(i))?;
        }
    }
    Ok(decimal_builder.finish())
}

fn multiply_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<DecimalArray> {
    let mut decimal_builder =
        DecimalBuilder::new(left.len(), left.precision(), left.scale());
    let divide = 10_i128.pow(left.scale() as u32);
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            decimal_builder.append_null()?;
        } else {
            decimal_builder.append_value(left.value(i) * right.value(i) / divide)?;
        }
    }
    Ok(decimal_builder.finish())
}

fn divide_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<DecimalArray> {
    let mut decimal_builder =
        DecimalBuilder::new(left.len(), left.precision(), left.scale());
    let mul = 10_f64.powi(left.scale() as i32);
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            decimal_builder.append_null()?;
        } else if right.value(i) == 0 {
            return Err(DataFusionError::ArrowError(DivideByZero));
        } else {
            let l_value = left.value(i) as f64;
            let r_value = right.value(i) as f64;
            let result = ((l_value / r_value) * mul) as i128;
            decimal_builder.append_value(result)?;
        }
    }
    Ok(decimal_builder.finish())
}

fn modulus_decimal(left: &DecimalArray, right: &DecimalArray) -> Result<DecimalArray> {
    let mut decimal_builder =
        DecimalBuilder::new(left.len(), left.precision(), left.scale());
    for i in 0..left.len() {
        if left.is_null(i) || right.is_null(i) {
            decimal_builder.append_null()?;
        } else if right.value(i) == 0 {
            return Err(DataFusionError::ArrowError(DivideByZero));
        } else {
            decimal_builder.append_value(left.value(i) % right.value(i))?;
        }
    }
    Ok(decimal_builder.finish())
}

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation.
macro_rules! binary_bitwise_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let left = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let right = $RIGHT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let result = (0..len)
            .into_iter()
            .map(|i| {
                if left.is_null(i) || right.is_null(i) {
                    None
                } else {
                    Some(left.value(i) $OP right.value(i))
                }
            })
            .collect::<$ARRAY_TYPE>();
        Ok(Arc::new(result))
    }};
}

/// The binary_bitwise_array_op macro only evaluates for integer types
/// like int64, int32.
/// It is used to do bitwise operation on an array with a scalar.
macro_rules! binary_bitwise_array_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:tt, $ARRAY_TYPE:ident, $TYPE:ty) => {{
        let len = $LEFT.len();
        let array = $LEFT.as_any().downcast_ref::<$ARRAY_TYPE>().unwrap();
        let scalar = $RIGHT;
        if scalar.is_null() {
            Ok(new_null_array(array.data_type(), len))
        } else {
            let right: $TYPE = scalar.try_into().unwrap();
            let result = (0..len)
                .into_iter()
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some(array.value(i) $OP right)
                    }
                })
                .collect::<$ARRAY_TYPE>();
            Ok(Arc::new(result) as ArrayRef)
        }
    }};
}

fn bitwise_and(left: ArrayRef, right: ArrayRef) -> Result<ArrayRef> {
    match &left.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_op!(left, right, &, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_op!(left, right, &, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_op!(left, right, &, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_op!(left, right, &, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    }
}

fn bitwise_and_scalar(
    array: &dyn Array,
    scalar: ScalarValue,
) -> Option<Result<ArrayRef>> {
    let result = match array.data_type() {
        DataType::Int8 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int8Array, i8)
        }
        DataType::Int16 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int16Array, i16)
        }
        DataType::Int32 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int32Array, i32)
        }
        DataType::Int64 => {
            binary_bitwise_array_scalar!(array, scalar, &, Int64Array, i64)
        }
        other => Err(DataFusionError::Internal(format!(
            "Data type {:?} not supported for binary operation '{}' on dyn arrays",
            other,
            Operator::BitwiseAnd
        ))),
    };
    Some(result)
}

/// Binary expression
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
}

impl BinaryExpr {
    /// Create new binary expression
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self { left, op, right }
    }

    /// Get the left side of the binary expression
    pub fn left(&self) -> &Arc<dyn PhysicalExpr> {
        &self.left
    }

    /// Get the right side of the binary expression
    pub fn right(&self) -> &Arc<dyn PhysicalExpr> {
        &self.right
    }

    /// Get the operator for this binary expression
    pub fn op(&self) -> &Operator {
        &self.op
    }
}

impl std::fmt::Display for BinaryExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

macro_rules! compute_decimal_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT.as_any().downcast_ref::<$DT>().unwrap();
        Ok(Arc::new(paste::expr! {[<$OP _decimal_scalar>]}(
            ll,
            $RIGHT.try_into()?,
        )?))
    }};
}

macro_rules! compute_decimal_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT.as_any().downcast_ref::<$DT>().unwrap();
        let rr = $RIGHT.as_any().downcast_ref::<$DT>().unwrap();
        Ok(Arc::new(paste::expr! {[<$OP _decimal>]}(ll, rr)?))
    }};
}

/// Invoke a compute kernel on a pair of binary data arrays
macro_rules! compute_utf8_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new(paste::expr! {[<$OP _utf8>]}(&ll, &rr)?))
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_utf8_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        if let ScalarValue::Utf8(Some(string_value)) = $RIGHT {
            Ok(Arc::new(paste::expr! {[<$OP _utf8_scalar>]}(
                &ll,
                &string_value,
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_op_scalar for '{}' failed to cast literal value {}",
                stringify!($OP),
                $RIGHT
            )))
        }
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
macro_rules! compute_utf8_op_dyn_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        if let Some(string_value) = $RIGHT {
            Ok(Arc::new(paste::expr! {[<$OP _dyn_utf8_scalar>]}(
                $LEFT,
                &string_value,
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_op_scalar for '{}' failed with literal 'none' value",
                stringify!($OP),
            )))
        }
    }};
}

/// Invoke a compute kernel on a boolean data array and a scalar value
macro_rules! compute_bool_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        use std::convert::TryInto;
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        // generate the scalar function name, such as lt_scalar, from the $OP parameter
        // (which could have a value of lt) and the suffix _scalar
        Ok(Arc::new(paste::expr! {[<$OP _bool_scalar>]}(
            &ll,
            $RIGHT.try_into()?,
        )?))
    }};
}

/// Invoke a compute kernel on a boolean data array and a scalar value
macro_rules! compute_bool_op_dyn_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        // generate the scalar function name, such as lt_dyn_bool_scalar, from the $OP parameter
        // (which could have a value of lt) and the suffix _scalar
        if let Some(b) = $RIGHT {
            Ok(Arc::new(paste::expr! {[<$OP _dyn_bool_scalar>]}(
                $LEFT,
                b,
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_op_scalar for '{}' failed with literal 'none' value",
                stringify!($OP),
            )))
        }
    }};
}

/// Invoke a bool compute kernel on array(s)
macro_rules! compute_bool_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast left side array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast right side array");
        Ok(Arc::new(paste::expr! {[<$OP _bool>]}(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast operant array");
        Ok(Arc::new(paste::expr! {[<$OP _bool>]}(&operand)?))
    }};
}

/// Invoke a compute kernel on a data array and a scalar value
/// LEFT is array, RIGHT is scalar value
macro_rules! compute_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        // generate the scalar function name, such as lt_scalar, from the $OP parameter
        // (which could have a value of lt) and the suffix _scalar
        Ok(Arc::new(paste::expr! {[<$OP _scalar>]}(
            &ll,
            $RIGHT.try_into()?,
        )?))
    }};
}

/// Invoke a dyn compute kernel on a data array and a scalar value
/// LEFT is Primitive or Dictionart array of numeric values, RIGHT is scalar value
macro_rules! compute_op_dyn_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        // generate the scalar function name, such as lt_dyn_scalar, from the $OP parameter
        // (which could have a value of lt_dyn) and the suffix _scalar
        if let Some(value) = $RIGHT {
            Ok(Arc::new(paste::expr! {[<$OP _dyn_scalar>]}(
                $LEFT,
                value,
            )?))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_op_scalar for '{}' failed with literal 'none' value",
                stringify!($OP),
            )))
        }
    }};
}

/// Invoke a compute kernel on array(s)
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

macro_rules! binary_string_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result: Result<Arc<dyn Array>> = match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for scalar operation '{}' on string array",
                other, stringify!($OP)
            ))),
        };
        Some(result)
    }};
}

macro_rules! binary_string_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for binary operation '{}' on string arrays",
                other, stringify!($OP)
            ))),
        }
    }};
}

/// Invoke a compute kernel on a pair of arrays
/// The binary_primitive_array_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! binary_primitive_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            // TODO support decimal type
            // which is not the primitive type
            DataType::Decimal(_,_) => compute_decimal_op!($LEFT, $RIGHT, $OP, DecimalArray),
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for binary operation '{}' on primitive arrays",
                other, stringify!($OP)
            ))),
        }
    }};
}

/// Invoke a compute kernel on an array and a scalar
/// The binary_primitive_array_op_scalar macro only evaluates for primitive
/// types like integers and floats.
macro_rules! binary_primitive_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result: Result<Arc<dyn Array>> = match $LEFT.data_type() {
            DataType::Int8 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float64Array),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for scalar operation '{}' on primitive array",
                other, stringify!($OP)
            ))),
        };
        Some(result)
    }};
}

/// The binary_array_op_scalar macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
#[macro_export]
macro_rules! binary_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result: Result<Arc<dyn Array>> = match $LEFT.data_type() {
            DataType::Decimal(_,_) => compute_decimal_op_scalar!($LEFT, $RIGHT, $OP, DecimalArray),
            DataType::Int8 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op_scalar!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op_scalar!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampNanosecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampSecondArray)
            }
            DataType::Date32 => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, Date32Array)
            }
            DataType::Date64 => {
                compute_op_scalar!($LEFT, $RIGHT, $OP, Date64Array)
            }
            DataType::Boolean => compute_bool_op_scalar!($LEFT, $RIGHT, $OP, BooleanArray),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for scalar operation '{}' on dyn array",
                other, stringify!($OP)
            ))),
        };
        Some(result)
    }};
}

/// The binary_array_op macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
#[macro_export]
macro_rules! binary_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Decimal(_,_) => compute_decimal_op!($LEFT, $RIGHT, $OP, DecimalArray),
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            DataType::Utf8 => compute_utf8_op!($LEFT, $RIGHT, $OP, StringArray),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampNanosecondArray)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampMicrosecondArray)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampMillisecondArray)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                compute_op!($LEFT, $RIGHT, $OP, TimestampSecondArray)
            }
            DataType::Date32 => {
                compute_op!($LEFT, $RIGHT, $OP, Date32Array)
            }
            DataType::Date64 => {
                compute_op!($LEFT, $RIGHT, $OP, Date64Array)
            }
            DataType::Boolean => compute_bool_op!($LEFT, $RIGHT, $OP, BooleanArray),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for binary operation '{}' on dyn arrays",
                other, stringify!($OP)
            ))),
        }
    }};
}

/// Invoke a boolean kernel on a pair of arrays
macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

macro_rules! binary_string_array_flag_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $NOT:expr, $FLAG:expr) => {{
        match $LEFT.data_type() {
            DataType::Utf8 => {
                compute_utf8_flag_op!($LEFT, $RIGHT, $OP, StringArray, $NOT, $FLAG)
            }
            DataType::LargeUtf8 => {
                compute_utf8_flag_op!($LEFT, $RIGHT, $OP, LargeStringArray, $NOT, $FLAG)
            }
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for binary_string_array_flag_op operation '{}' on string array",
                other, stringify!($OP)
            ))),
        }
    }};
}

/// Invoke a compute kernel on a pair of binary data arrays with flags
macro_rules! compute_utf8_flag_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $ARRAYTYPE:ident, $NOT:expr, $FLAG:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("compute_utf8_flag_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("compute_utf8_flag_op failed to downcast array");

        let flag = if $FLAG {
            Some($ARRAYTYPE::from(vec!["i"; ll.len()]))
        } else {
            None
        };
        let mut array = paste::expr! {[<$OP _utf8>]}(&ll, &rr, flag.as_ref())?;
        if $NOT {
            array = not(&array).unwrap();
        }
        Ok(Arc::new(array))
    }};
}

macro_rules! binary_string_array_flag_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $NOT:expr, $FLAG:expr) => {{
        let result: Result<Arc<dyn Array>> = match $LEFT.data_type() {
            DataType::Utf8 => {
                compute_utf8_flag_op_scalar!($LEFT, $RIGHT, $OP, StringArray, $NOT, $FLAG)
            }
            DataType::LargeUtf8 => {
                compute_utf8_flag_op_scalar!($LEFT, $RIGHT, $OP, LargeStringArray, $NOT, $FLAG)
            }
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for binary_string_array_flag_op_scalar operation '{}' on string array",
                other, stringify!($OP)
            ))),
        };
        Some(result)
    }};
}

/// Invoke a compute kernel on a data array and a scalar value with flag
macro_rules! compute_utf8_flag_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $ARRAYTYPE:ident, $NOT:expr, $FLAG:expr) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$ARRAYTYPE>()
            .expect("compute_utf8_flag_op_scalar failed to downcast array");

        if let ScalarValue::Utf8(Some(string_value)) = $RIGHT {
            let flag = if $FLAG { Some("i") } else { None };
            let mut array =
                paste::expr! {[<$OP _utf8_scalar>]}(&ll, &string_value, flag)?;
            if $NOT {
                array = not(&array).unwrap();
            }
            Ok(Arc::new(array))
        } else {
            Err(DataFusionError::Internal(format!(
                "compute_utf8_flag_op_scalar failed to cast literal value {} for operation '{}'",
                $RIGHT, stringify!($OP)
            )))
        }
    }};
}

/// Returns the return type of a binary operator or an error when the binary operator cannot
/// perform the computation between the argument's types, even after type coercion.
///
/// This function makes some assumptions about the underlying available computations.
pub fn binary_operator_data_type(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // validate that it is possible to perform the operation on incoming types.
    // (or the return datatype cannot be inferred)
    let result_type = coerce_types(lhs_type, op, rhs_type)?;

    match op {
        // operators that return a boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::And
        | Operator::Or
        | Operator::Like
        | Operator::NotLike
        | Operator::Lt
        | Operator::Gt
        | Operator::GtEq
        | Operator::LtEq
        | Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch
        | Operator::IsDistinctFrom
        | Operator::IsNotDistinctFrom => Ok(DataType::Boolean),
        // bitwise operations return the common coerced type
        Operator::BitwiseAnd => Ok(result_type),
        // math operations return the same value as the common coerced type
        Operator::Plus
        | Operator::Minus
        | Operator::Divide
        | Operator::Multiply
        | Operator::Modulo => Ok(result_type),
    }
}

impl PhysicalExpr for BinaryExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        binary_operator_data_type(
            &self.left.data_type(input_schema)?,
            &self.op,
            &self.right.data_type(input_schema)?,
        )
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input_schema)? || self.right.nullable(input_schema)?)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let left_value = self.left.evaluate(batch)?;
        let right_value = self.right.evaluate(batch)?;
        let left_data_type = left_value.data_type();
        let right_data_type = right_value.data_type();

        if left_data_type != right_data_type {
            return Err(DataFusionError::Internal(format!(
                "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                self.op, left_data_type, right_data_type
            )));
        }

        // Attempt to use special kernels if one input is scalar and the other is an array
        let scalar_result = match (&left_value, &right_value) {
            (ColumnarValue::Array(array), ColumnarValue::Scalar(scalar)) => {
                // if left is array and right is literal - use scalar operations
                self.evaluate_array_scalar(array, scalar)?
            }
            (ColumnarValue::Scalar(scalar), ColumnarValue::Array(array)) => {
                // if right is literal and left is array - reverse operator and parameters
                self.evaluate_scalar_array(scalar, array)?
            }
            (_, _) => None, // default to array implementation
        };

        if let Some(result) = scalar_result {
            return result.map(|a| ColumnarValue::Array(a));
        }

        // if both arrays or both literals - extract arrays and continue execution
        let (left, right) = (
            left_value.into_array(batch.num_rows()),
            right_value.into_array(batch.num_rows()),
        );
        self.evaluate_with_resolved_args(left, &left_data_type, right, &right_data_type)
            .map(|a| ColumnarValue::Array(a))
    }
}

/// The binary_array_op_dyn_scalar macro includes types that extend beyond the primitive,
/// such as Utf8 strings.
#[macro_export]
macro_rules! binary_array_op_dyn_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        let result: Result<Arc<dyn Array>> = match $RIGHT {
            ScalarValue::Boolean(b) => compute_bool_op_dyn_scalar!($LEFT, b, $OP),
            ScalarValue::Decimal128(..) => compute_decimal_op_scalar!($LEFT, $RIGHT, $OP, DecimalArray),
            ScalarValue::Utf8(v) => compute_utf8_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::LargeUtf8(v) => compute_utf8_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::Int8(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::Int16(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::Int32(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::Int64(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::UInt8(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::UInt16(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::UInt32(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::UInt64(v) => compute_op_dyn_scalar!($LEFT, v, $OP),
            ScalarValue::Float32(_) => compute_op_scalar!($LEFT, $RIGHT, $OP, Float32Array),
            ScalarValue::Float64(_) => compute_op_scalar!($LEFT, $RIGHT, $OP, Float64Array),
            ScalarValue::Date32(_) => compute_op_scalar!($LEFT, $RIGHT, $OP, Date32Array),
            ScalarValue::Date64(_) => compute_op_scalar!($LEFT, $RIGHT, $OP, Date64Array),
            ScalarValue::TimestampSecond(..) => compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampSecondArray),
            ScalarValue::TimestampMillisecond(..) => compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampMillisecondArray),
            ScalarValue::TimestampMicrosecond(..) => compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampMicrosecondArray),
            ScalarValue::TimestampNanosecond(..) => compute_op_scalar!($LEFT, $RIGHT, $OP, TimestampNanosecondArray),
            other => Err(DataFusionError::Internal(format!("Data type {:?} not supported for scalar operation '{}' on dyn array", other, stringify!($OP))))
        };
        Some(result)
    }}
}

impl BinaryExpr {
    /// Evaluate the expression of the left input is an array and
    /// right is literal - use scalar operations
    fn evaluate_array_scalar(
        &self,
        array: &dyn Array,
        scalar: &ScalarValue,
    ) -> Result<Option<Result<ArrayRef>>> {
        let scalar_result = match &self.op {
            Operator::Lt => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), lt)
            }
            Operator::LtEq => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), lt_eq)
            }
            Operator::Gt => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), gt)
            }
            Operator::GtEq => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), gt_eq)
            }
            Operator::Eq => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), eq)
            }
            Operator::NotEq => {
                binary_array_op_dyn_scalar!(array, scalar.clone(), neq)
            }
            Operator::Like => {
                binary_string_array_op_scalar!(array, scalar.clone(), like)
            }
            Operator::NotLike => {
                binary_string_array_op_scalar!(array, scalar.clone(), nlike)
            }
            Operator::Plus => {
                binary_primitive_array_op_scalar!(array, scalar.clone(), add)
            }
            Operator::Minus => {
                binary_primitive_array_op_scalar!(array, scalar.clone(), subtract)
            }
            Operator::Multiply => {
                binary_primitive_array_op_scalar!(array, scalar.clone(), multiply)
            }
            Operator::Divide => {
                binary_primitive_array_op_scalar!(array, scalar.clone(), divide)
            }
            Operator::Modulo => {
                binary_primitive_array_op_scalar!(array, scalar.clone(), modulus)
            }
            Operator::RegexMatch => binary_string_array_flag_op_scalar!(
                array,
                scalar.clone(),
                regexp_is_match,
                false,
                false
            ),
            Operator::RegexIMatch => binary_string_array_flag_op_scalar!(
                array,
                scalar.clone(),
                regexp_is_match,
                false,
                true
            ),
            Operator::RegexNotMatch => binary_string_array_flag_op_scalar!(
                array,
                scalar.clone(),
                regexp_is_match,
                true,
                false
            ),
            Operator::RegexNotIMatch => binary_string_array_flag_op_scalar!(
                array,
                scalar.clone(),
                regexp_is_match,
                true,
                true
            ),
            Operator::BitwiseAnd => bitwise_and_scalar(array, scalar.clone()),
            // if scalar operation is not supported - fallback to array implementation
            _ => None,
        };

        Ok(scalar_result)
    }

    /// Evaluate the expression if the left input is a literal and the
    /// right is an array - reverse operator and parameters
    fn evaluate_scalar_array(
        &self,
        scalar: &ScalarValue,
        array: &ArrayRef,
    ) -> Result<Option<Result<ArrayRef>>> {
        let scalar_result = match &self.op {
            Operator::Lt => binary_array_op_scalar!(array, scalar.clone(), gt),
            Operator::LtEq => binary_array_op_scalar!(array, scalar.clone(), gt_eq),
            Operator::Gt => binary_array_op_scalar!(array, scalar.clone(), lt),
            Operator::GtEq => binary_array_op_scalar!(array, scalar.clone(), lt_eq),
            Operator::Eq => binary_array_op_scalar!(array, scalar.clone(), eq),
            Operator::NotEq => {
                binary_array_op_scalar!(array, scalar.clone(), neq)
            }
            // if scalar operation is not supported - fallback to array implementation
            _ => None,
        };
        Ok(scalar_result)
    }

    fn evaluate_with_resolved_args(
        &self,
        left: Arc<dyn Array>,
        left_data_type: &DataType,
        right: Arc<dyn Array>,
        right_data_type: &DataType,
    ) -> Result<ArrayRef> {
        match &self.op {
            Operator::Like => binary_string_array_op!(left, right, like),
            Operator::NotLike => binary_string_array_op!(left, right, nlike),
            Operator::Lt => binary_array_op!(left, right, lt),
            Operator::LtEq => binary_array_op!(left, right, lt_eq),
            Operator::Gt => binary_array_op!(left, right, gt),
            Operator::GtEq => binary_array_op!(left, right, gt_eq),
            Operator::Eq => binary_array_op!(left, right, eq),
            Operator::NotEq => binary_array_op!(left, right, neq),
            Operator::IsDistinctFrom => binary_array_op!(left, right, is_distinct_from),
            Operator::IsNotDistinctFrom => {
                binary_array_op!(left, right, is_not_distinct_from)
            }
            Operator::Plus => binary_primitive_array_op!(left, right, add),
            Operator::Minus => binary_primitive_array_op!(left, right, subtract),
            Operator::Multiply => binary_primitive_array_op!(left, right, multiply),
            Operator::Divide => binary_primitive_array_op!(left, right, divide),
            Operator::Modulo => binary_primitive_array_op!(left, right, modulus),
            Operator::And => {
                if left_data_type == &DataType::Boolean {
                    boolean_op!(left, right, and_kleene)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op,
                        left.data_type(),
                        right.data_type()
                    )));
                }
            }
            Operator::Or => {
                if left_data_type == &DataType::Boolean {
                    boolean_op!(left, right, or_kleene)
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "Cannot evaluate binary expression {:?} with types {:?} and {:?}",
                        self.op, left_data_type, right_data_type
                    )));
                }
            }
            Operator::RegexMatch => {
                binary_string_array_flag_op!(left, right, regexp_is_match, false, false)
            }
            Operator::RegexIMatch => {
                binary_string_array_flag_op!(left, right, regexp_is_match, false, true)
            }
            Operator::RegexNotMatch => {
                binary_string_array_flag_op!(left, right, regexp_is_match, true, false)
            }
            Operator::RegexNotIMatch => {
                binary_string_array_flag_op!(left, right, regexp_is_match, true, true)
            }
            Operator::BitwiseAnd => bitwise_and(left, right),
        }
    }
}

fn is_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

fn is_distinct_from_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x != y))
        .collect())
}

fn is_not_distinct_from<T>(
    left: &PrimitiveArray<T>,
    right: &PrimitiveArray<T>,
) -> Result<BooleanArray>
where
    T: ArrowNumericType,
{
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

fn is_not_distinct_from_utf8<OffsetSize: StringOffsetSizeTrait>(
    left: &GenericStringArray<OffsetSize>,
    right: &GenericStringArray<OffsetSize>,
) -> Result<BooleanArray> {
    Ok(left
        .iter()
        .zip(right.iter())
        .map(|(x, y)| Some(x == y))
        .collect())
}

/// return two physical expressions that are optionally coerced to a
/// common type that the binary operator supports.
fn binary_cast(
    lhs: Arc<dyn PhysicalExpr>,
    op: &Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
    let lhs_type = &lhs.data_type(input_schema)?;
    let rhs_type = &rhs.data_type(input_schema)?;

    let result_type = coerce_types(lhs_type, op, rhs_type)?;

    Ok((
        try_cast(lhs, input_schema, result_type.clone())?,
        try_cast(rhs, input_schema, result_type)?,
    ))
}

/// Create a binary expression whose arguments are correctly coerced.
/// This function errors if it is not possible to coerce the arguments
/// to computational types supported by the operator.
pub fn binary(
    lhs: Arc<dyn PhysicalExpr>,
    op: Operator,
    rhs: Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (l, r) = binary_cast(lhs, &op, rhs, input_schema)?;
    Ok(Arc::new(BinaryExpr::new(l, op, r)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::from_slice::FromSlice;
    use crate::physical_plan::expressions::{col, lit};
    use arrow::datatypes::{ArrowNumericType, Field, Int32Type, SchemaRef};
    use arrow::util::display::array_value_to_string;

    // Create a binary expression without coercion. Used here when we do not want to coerce the expressions
    // to valid types. Usage can result in an execution (after plan) error.
    fn binary_simple(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
        input_schema: &Schema,
    ) -> Arc<dyn PhysicalExpr> {
        binary(l, op, r, input_schema).unwrap()
    }

    #[test]
    fn binary_comparison() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(&[1, 2, 4, 8, 16]);

        // expression: "a < b"
        let lt = binary_simple(
            col("a", &schema)?,
            Operator::Lt,
            col("b", &schema)?,
            &schema,
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        let result = lt.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![false, false, true, true, true];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    #[test]
    fn binary_nested() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[2, 4, 6, 8, 10]);
        let b = Int32Array::from_slice(&[2, 5, 4, 8, 8]);

        // expression: "a < b OR a == b"
        let expr = binary_simple(
            binary_simple(
                col("a", &schema)?,
                Operator::Lt,
                col("b", &schema)?,
                &schema,
            ),
            Operator::Or,
            binary_simple(
                col("a", &schema)?,
                Operator::Eq,
                col("b", &schema)?,
                &schema,
            ),
            &schema,
        );
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])?;

        assert_eq!("a@0 < b@1 OR a@0 = b@1", format!("{}", expr));

        let result = expr.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.len(), 5);

        let expected = vec![true, true, false, true, false];
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("failed to downcast to BooleanArray");
        for (i, &expected_item) in expected.iter().enumerate().take(5) {
            assert_eq!(result.value(i), expected_item);
        }

        Ok(())
    }

    // runs an end-to-end test of physical type coercion:
    // 1. construct a record batch with two columns of type A and B
    //  (*_ARRAY is the Rust Arrow array type, and *_TYPE is the DataType of the elements)
    // 2. construct a physical expression of A OP B
    // 3. evaluate the expression
    // 4. verify that the resulting expression is of type C
    // 5. verify that the results of evaluation are $VEC
    macro_rules! test_coercion {
        ($A_ARRAY:ident, $A_TYPE:expr, $A_VEC:expr, $B_ARRAY:ident, $B_TYPE:expr, $B_VEC:expr, $OP:expr, $C_ARRAY:ident, $C_TYPE:expr, $VEC:expr) => {{
            let schema = Schema::new(vec![
                Field::new("a", $A_TYPE, false),
                Field::new("b", $B_TYPE, false),
            ]);
            let a = $A_ARRAY::from($A_VEC);
            let b = $B_ARRAY::from($B_VEC);

            // verify that we can construct the expression
            let expression =
                binary(col("a", &schema)?, $OP, col("b", &schema)?, &schema)?;
            let batch = RecordBatch::try_new(
                Arc::new(schema.clone()),
                vec![Arc::new(a), Arc::new(b)],
            )?;

            // verify that the expression's type is correct
            assert_eq!(expression.data_type(&schema)?, $C_TYPE);

            // compute
            let result = expression.evaluate(&batch)?.into_array(batch.num_rows());

            // verify that the array's data_type is correct
            assert_eq!(*result.data_type(), $C_TYPE);

            // verify that the data itself is downcastable
            let result = result
                .as_any()
                .downcast_ref::<$C_ARRAY>()
                .expect("failed to downcast");
            // verify that the result itself is correct
            for (i, x) in $VEC.iter().enumerate() {
                assert_eq!(result.value(i), *x);
            }
        }};
    }

    #[test]
    fn test_type_coersion() -> Result<()> {
        test_coercion!(
            Int32Array,
            DataType::Int32,
            vec![1i32, 2i32],
            UInt32Array,
            DataType::UInt32,
            vec![1u32, 2u32],
            Operator::Plus,
            Int32Array,
            DataType::Int32,
            vec![2i32, 4i32]
        );
        test_coercion!(
            Int32Array,
            DataType::Int32,
            vec![1i32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Plus,
            Int32Array,
            DataType::Int32,
            vec![2i32]
        );
        test_coercion!(
            Float32Array,
            DataType::Float32,
            vec![1f32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Plus,
            Float32Array,
            DataType::Float32,
            vec![2f32]
        );
        test_coercion!(
            Float32Array,
            DataType::Float32,
            vec![2f32],
            UInt16Array,
            DataType::UInt16,
            vec![1u16],
            Operator::Multiply,
            Float32Array,
            DataType::Float32,
            vec![2f32]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["hello world", "world"],
            StringArray,
            DataType::Utf8,
            vec!["%hello%", "%hello%"],
            Operator::Like,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13", "1995-01-26"],
            Date32Array,
            DataType::Date32,
            vec![9112, 9156],
            Operator::Eq,
            BooleanArray,
            DataType::Boolean,
            vec![true, true]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13", "1995-01-26"],
            Date32Array,
            DataType::Date32,
            vec![9113, 9154],
            Operator::Lt,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13T12:34:56", "1995-01-26T01:23:45"],
            Date64Array,
            DataType::Date64,
            vec![787322096000, 791083425000],
            Operator::Eq,
            BooleanArray,
            DataType::Boolean,
            vec![true, true]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["1994-12-13T12:34:56", "1995-01-26T01:23:45"],
            Date64Array,
            DataType::Date64,
            vec![787322096001, 791083424999],
            Operator::Lt,
            BooleanArray,
            DataType::Boolean,
            vec![true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["abc"; 5],
            StringArray,
            DataType::Utf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexMatch,
            BooleanArray,
            DataType::Boolean,
            vec![true, false, true, false, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["abc"; 5],
            StringArray,
            DataType::Utf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexIMatch,
            BooleanArray,
            DataType::Boolean,
            vec![true, true, true, true, false]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["abc"; 5],
            StringArray,
            DataType::Utf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexNotMatch,
            BooleanArray,
            DataType::Boolean,
            vec![false, true, false, true, true]
        );
        test_coercion!(
            StringArray,
            DataType::Utf8,
            vec!["abc"; 5],
            StringArray,
            DataType::Utf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexNotIMatch,
            BooleanArray,
            DataType::Boolean,
            vec![false, false, false, false, true]
        );
        test_coercion!(
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["abc"; 5],
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexMatch,
            BooleanArray,
            DataType::Boolean,
            vec![true, false, true, false, false]
        );
        test_coercion!(
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["abc"; 5],
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexIMatch,
            BooleanArray,
            DataType::Boolean,
            vec![true, true, true, true, false]
        );
        test_coercion!(
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["abc"; 5],
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexNotMatch,
            BooleanArray,
            DataType::Boolean,
            vec![false, true, false, true, true]
        );
        test_coercion!(
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["abc"; 5],
            LargeStringArray,
            DataType::LargeUtf8,
            vec!["^a", "^A", "(b|d)", "(B|D)", "^(b|c)"],
            Operator::RegexNotIMatch,
            BooleanArray,
            DataType::Boolean,
            vec![false, false, false, false, true]
        );
        test_coercion!(
            Int16Array,
            DataType::Int16,
            vec![1i16, 2i16, 3i16],
            Int64Array,
            DataType::Int64,
            vec![10i64, 4i64, 5i64],
            Operator::BitwiseAnd,
            Int64Array,
            DataType::Int64,
            vec![0i64, 0i64, 1i64]
        );
        Ok(())
    }

    // Note it would be nice to use the same test_coercion macro as
    // above, but sadly the type of the values of the dictionary are
    // not encoded in the rust type of the DictionaryArray. Thus there
    // is no way at the time of this writing to create a dictionary
    // array using the `From` trait
    #[test]
    fn test_dictionary_type_to_array_coersion() -> Result<()> {
        // Test string  a string dictionary
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let string_type = DataType::Utf8;

        // build dictionary
        let keys_builder = PrimitiveBuilder::<Int32Type>::new(10);
        let values_builder = arrow::array::StringBuilder::new(10);
        let mut dict_builder = StringDictionaryBuilder::new(keys_builder, values_builder);

        dict_builder.append("one")?;
        dict_builder.append_null()?;
        dict_builder.append("three")?;
        dict_builder.append("four")?;
        let dict_array = dict_builder.finish();

        let str_array =
            StringArray::from(vec![Some("not one"), Some("two"), None, Some("four")]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("dict", dict_type, true),
            Field::new("str", string_type, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(dict_array), Arc::new(str_array)],
        )?;

        let expected = "false\n\n\ntrue";

        // Test 1: dict = str

        // verify that we can construct the expression
        let expression = binary(
            col("dict", &schema)?,
            Operator::Eq,
            col("str", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, array_to_string(&result)?);

        // Test 2: now test the other direction
        // str = dict

        // verify that we can construct the expression
        let expression = binary(
            col("str", &schema)?,
            Operator::Eq,
            col("dict", &schema)?,
            &schema,
        )?;
        assert_eq!(expression.data_type(&schema)?, DataType::Boolean);

        // evaluate and verify the result type matched
        let result = expression.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.data_type(), &DataType::Boolean);

        // verify that the result itself is correct
        assert_eq!(expected, array_to_string(&result)?);

        Ok(())
    }

    // Convert the array to a newline delimited string of pretty printed values
    fn array_to_string(array: &ArrayRef) -> Result<String> {
        let s = (0..array.len())
            .map(|i| array_value_to_string(array, i))
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>()?
            .join("\n");
        Ok(s)
    }

    #[test]
    fn plus_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        let a = Int32Array::from_slice(&[1, 2, 3, 4, 5]);
        let b = Int32Array::from_slice(&[1, 2, 4, 8, 16]);

        apply_arithmetic::<Int32Type>(
            Arc::new(schema),
            vec![Arc::new(a), Arc::new(b)],
            Operator::Plus,
            Int32Array::from_slice(&[2, 4, 7, 12, 21]),
        )?;

        Ok(())
    }

    #[test]
    fn minus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[1, 2, 4, 8, 16]));
        let b = Arc::new(Int32Array::from_slice(&[1, 2, 3, 4, 5]));

        apply_arithmetic::<Int32Type>(
            schema.clone(),
            vec![a.clone(), b.clone()],
            Operator::Minus,
            Int32Array::from_slice(&[0, 0, 1, 4, 11]),
        )?;

        // should handle have negative values in result (for signed)
        apply_arithmetic::<Int32Type>(
            schema,
            vec![b, a],
            Operator::Minus,
            Int32Array::from_slice(&[0, 0, -1, -4, -11]),
        )?;

        Ok(())
    }

    #[test]
    fn multiply_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[4, 8, 16, 32, 64]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 8, 16, 32]));

        apply_arithmetic::<Int32Type>(
            schema,
            vec![a, b],
            Operator::Multiply,
            Int32Array::from_slice(&[8, 32, 128, 512, 2048]),
        )?;

        Ok(())
    }

    #[test]
    fn divide_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 8, 16, 32]));

        apply_arithmetic::<Int32Type>(
            schema,
            vec![a, b],
            Operator::Divide,
            Int32Array::from_slice(&[4, 8, 16, 32, 64]),
        )?;

        Ok(())
    }

    #[test]
    fn modulus_op() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let a = Arc::new(Int32Array::from_slice(&[8, 32, 128, 512, 2048]));
        let b = Arc::new(Int32Array::from_slice(&[2, 4, 7, 14, 32]));

        apply_arithmetic::<Int32Type>(
            schema,
            vec![a, b],
            Operator::Modulo,
            Int32Array::from_slice(&[0, 0, 2, 8, 0]),
        )?;

        Ok(())
    }

    fn apply_arithmetic<T: ArrowNumericType>(
        schema: SchemaRef,
        data: Vec<ArrayRef>,
        op: Operator,
        expected: PrimitiveArray<T>,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", &schema)?, op, col("b", &schema)?, &schema);
        let batch = RecordBatch::try_new(schema, data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    fn apply_logic_op(
        schema: &SchemaRef,
        left: &ArrayRef,
        right: &ArrayRef,
        op: Operator,
        expected: BooleanArray,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", schema)?, op, col("b", schema)?, schema);
        let data: Vec<ArrayRef> = vec![left.clone(), right.clone()];
        let batch = RecordBatch::try_new(schema.clone(), data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    // Test `scalar <op> arr` produces expected
    fn apply_logic_op_scalar_arr(
        schema: &SchemaRef,
        scalar: &ScalarValue,
        arr: &ArrayRef,
        op: Operator,
        expected: &BooleanArray,
    ) -> Result<()> {
        let scalar = lit(scalar.clone());

        let arithmetic_op = binary_simple(scalar, op, col("a", schema)?, schema);
        let batch = RecordBatch::try_new(Arc::clone(schema), vec![Arc::clone(arr)])?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.as_ref(), expected);

        Ok(())
    }

    // Test `arr <op> scalar` produces expected
    fn apply_logic_op_arr_scalar(
        schema: &SchemaRef,
        arr: &ArrayRef,
        scalar: &ScalarValue,
        op: Operator,
        expected: &BooleanArray,
    ) -> Result<()> {
        let scalar = lit(scalar.clone());

        let arithmetic_op = binary_simple(col("a", schema)?, op, scalar, schema);
        let batch = RecordBatch::try_new(Arc::clone(schema), vec![Arc::clone(arr)])?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());
        assert_eq!(result.as_ref(), expected);

        Ok(())
    }

    #[test]
    fn and_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ])) as ArrayRef;
        let b = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ])) as ArrayRef;

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(false),
            Some(false),
            None,
            Some(false),
            None,
        ]);
        apply_logic_op(&Arc::new(schema), &a, &b, Operator::And, expected)?;

        Ok(())
    }

    #[test]
    fn or_with_nulls_op() -> Result<()> {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Boolean, true),
        ]);
        let a = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(false),
            None,
        ])) as ArrayRef;
        let b = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
            None,
        ])) as ArrayRef;

        let expected = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            None,
        ]);
        apply_logic_op(&Arc::new(schema), &a, &b, Operator::Or, expected)?;

        Ok(())
    }

    /// Returns (schema, a: BooleanArray, b: BooleanArray) with all possible inputs
    ///
    /// a: [true, true, true,  NULL, NULL, NULL,  false, false, false]
    /// b: [true, NULL, false, true, NULL, false, true,  NULL,  false]
    fn bool_test_arrays() -> (SchemaRef, ArrayRef, ArrayRef) {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Boolean, false),
        ]);
        let a: BooleanArray = [
            Some(true),
            Some(true),
            Some(true),
            None,
            None,
            None,
            Some(false),
            Some(false),
            Some(false),
        ]
        .iter()
        .collect();
        let b: BooleanArray = [
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
        ]
        .iter()
        .collect();
        (Arc::new(schema), Arc::new(a), Arc::new(b))
    }

    /// Returns (schema, BooleanArray) with [true, NULL, false]
    fn scalar_bool_test_array() -> (SchemaRef, ArrayRef) {
        let schema = Schema::new(vec![Field::new("a", DataType::Boolean, false)]);
        let a: BooleanArray = vec![Some(true), None, Some(false)].iter().collect();
        (Arc::new(schema), Arc::new(a))
    }

    #[test]
    fn eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = vec![
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(false),
            None,
            Some(true),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::Eq, expected).unwrap();
    }

    #[test]
    fn eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Eq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Eq,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Eq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Eq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn neq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(false),
            None,
            Some(true),
            None,
            None,
            None,
            Some(true),
            None,
            Some(false),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::NotEq, expected).unwrap();
    }

    #[test]
    fn neq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::NotEq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::NotEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::NotEq,
            &expected,
        )
        .unwrap();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::NotEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn lt_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(false),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
            None,
            Some(false),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::Lt, expected).unwrap();
    }

    #[test]
    fn lt_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Lt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Lt,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn lt_eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(true),
            None,
            Some(false),
            None,
            None,
            None,
            Some(true),
            None,
            Some(true),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::LtEq, expected).unwrap();
    }

    #[test]
    fn lt_eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::LtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::LtEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn gt_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(false),
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            Some(false),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::Gt, expected).unwrap();
    }

    #[test]
    fn gt_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(false)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::Gt,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::Gt,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn gt_eq_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(true),
            None,
            Some(true),
            None,
            None,
            None,
            Some(false),
            None,
            Some(true),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::GtEq, expected).unwrap();
    }

    #[test]
    fn gt_eq_op_bool_scalar() {
        let (schema, a) = scalar_bool_test_array();
        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(true),
            &a,
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(false)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(true),
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(false), None, Some(true)].iter().collect();
        apply_logic_op_scalar_arr(
            &schema,
            &ScalarValue::from(false),
            &a,
            Operator::GtEq,
            &expected,
        )
        .unwrap();

        let expected = [Some(true), None, Some(true)].iter().collect();
        apply_logic_op_arr_scalar(
            &schema,
            &a,
            &ScalarValue::from(false),
            Operator::GtEq,
            &expected,
        )
        .unwrap();
    }

    #[test]
    fn is_distinct_from_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
            Some(false),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::IsDistinctFrom, expected).unwrap();
    }

    #[test]
    fn is_not_distinct_from_op_bool() {
        let (schema, a, b) = bool_test_arrays();
        let expected = [
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ]
        .iter()
        .collect();
        apply_logic_op(&schema, &a, &b, Operator::IsNotDistinctFrom, expected).unwrap();
    }

    #[test]
    fn relatively_deeply_nested() {
        // Reproducer for https://github.com/apache/arrow-datafusion/issues/419

        // where even relatively shallow binary expressions overflowed
        // the stack in debug builds

        let input: Vec<_> = vec![1, 2, 3, 4, 5].into_iter().map(Some).collect();
        let a: Int32Array = input.iter().collect();

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(a) as _)]).unwrap();
        let schema = batch.schema();

        // build a left deep tree ((((a + a) + a) + a ....
        let tree_depth: i32 = 100;
        let expr = (0..tree_depth)
            .into_iter()
            .map(|_| col("a", schema.as_ref()).unwrap())
            .reduce(|l, r| binary_simple(l, Operator::Plus, r, &schema))
            .unwrap();

        let result = expr
            .evaluate(&batch)
            .expect("evaluation")
            .into_array(batch.num_rows());

        let expected: Int32Array = input
            .into_iter()
            .map(|i| i.map(|i| i * tree_depth))
            .collect();
        assert_eq!(result.as_ref(), &expected);
    }

    fn create_decimal_array(
        array: &[Option<i128>],
        precision: usize,
        scale: usize,
    ) -> Result<DecimalArray> {
        let mut decimal_builder = DecimalBuilder::new(array.len(), precision, scale);
        for value in array {
            match value {
                None => {
                    decimal_builder.append_null()?;
                }
                Some(v) => {
                    decimal_builder.append_value(*v)?;
                }
            }
        }
        Ok(decimal_builder.finish())
    }

    #[test]
    fn comparison_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        // eq: array = i128
        let result = eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // neq: array != i128
        let result = neq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // lt: array < i128
        let result = lt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: array <= i128
        let result = lt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // gt: array > i128
        let result = gt_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // gt_eq: array >= i128
        let result = gt_eq_decimal_scalar(&decimal_array, value_i128)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
            result
        );

        let left_decimal_array = decimal_array;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128 - 1),
                Some(value_i128),
                Some(value_i128 + 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        // eq: left == right
        let result = eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(false), Some(true)]),
            result
        );
        // neq: left != right
        let result = neq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]),
            result
        );
        // lt: left < right
        let result = lt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
            result
        );
        // lt_eq: left <= right
        let result = lt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), None, Some(true), Some(true)]),
            result
        );
        // gt: left > right
        let result = gt_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(false)]),
            result
        );
        // gt_eq: left >= right
        let result = gt_eq_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
            result
        );
        // is_distinct: left distinct right
        let result = is_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(true), Some(true), Some(true), Some(false)]),
            result
        );
        // is_distinct: left distinct right
        let result =
            is_not_distinct_from_decimal(&left_decimal_array, &right_decimal_array)?;
        assert_eq!(
            BooleanArray::from(vec![Some(false), Some(false), Some(false), Some(true)]),
            result
        );
        Ok(())
    }

    #[test]
    fn comparison_decimal_expr_test() -> Result<()> {
        let decimal_scalar = ScalarValue::Decimal128(Some(123_456), 10, 3);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        // scalar == array
        apply_logic_op_scalar_arr(
            &schema,
            &decimal_scalar,
            &(Arc::new(Int64Array::from(vec![Some(124), None])) as ArrayRef),
            Operator::Eq,
            &BooleanArray::from(vec![Some(false), None]),
        )
        .unwrap();

        // array != scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from(vec![Some(123), None, Some(1)])) as ArrayRef),
            &decimal_scalar,
            Operator::NotEq,
            &BooleanArray::from(vec![Some(true), None, Some(true)]),
        )
        .unwrap();

        // array < scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from(vec![Some(123), None, Some(124)])) as ArrayRef),
            &decimal_scalar,
            Operator::Lt,
            &BooleanArray::from(vec![Some(true), None, Some(false)]),
        )
        .unwrap();

        // array > scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Int64Array::from(vec![Some(123), None, Some(124)])) as ArrayRef),
            &decimal_scalar,
            Operator::Gt,
            &BooleanArray::from(vec![Some(false), None, Some(true)]),
        )
        .unwrap();

        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        // array == scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from(vec![Some(123.456), None, Some(123.457)]))
                as ArrayRef),
            &decimal_scalar,
            Operator::Eq,
            &BooleanArray::from(vec![Some(true), None, Some(false)]),
        )
        .unwrap();

        // array <= scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from(vec![
                Some(123.456),
                None,
                Some(123.457),
                Some(123.45),
            ])) as ArrayRef),
            &decimal_scalar,
            Operator::LtEq,
            &BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
        )
        .unwrap();
        // array >= scalar
        apply_logic_op_arr_scalar(
            &schema,
            &(Arc::new(Float64Array::from(vec![
                Some(123.456),
                None,
                Some(123.457),
                Some(123.45),
            ])) as ArrayRef),
            &decimal_scalar,
            Operator::GtEq,
            &BooleanArray::from(vec![Some(true), None, Some(true), Some(false)]),
        )
        .unwrap();

        // compare decimal array with other array type
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Decimal(10, 0), true),
        ]));

        let value: i64 = 123;

        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128),
                None,
                Some((value - 1) as i128),
                Some((value + 1) as i128),
            ],
            10,
            0,
        )?) as ArrayRef;

        let int64_array = Arc::new(Int64Array::from(vec![
            Some(value),
            Some(value - 1),
            Some(value),
            Some(value + 1),
        ])) as ArrayRef;

        // eq: int64array == decimal array
        apply_logic_op(
            &schema,
            &int64_array,
            &decimal_array,
            Operator::Eq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
        )
        .unwrap();
        // neq: int64array != decimal array
        apply_logic_op(
            &schema,
            &int64_array,
            &decimal_array,
            Operator::NotEq,
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));

        let value: i128 = 123;
        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128), // 1.23
                None,
                Some((value - 1) as i128), // 1.22
                Some((value + 1) as i128), // 1.24
            ],
            10,
            2,
        )?) as ArrayRef;
        let float64_array = Arc::new(Float64Array::from(vec![
            Some(1.23),
            Some(1.22),
            Some(1.23),
            Some(1.24),
        ])) as ArrayRef;
        // lt: float64array < decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::Lt,
            BooleanArray::from(vec![Some(false), None, Some(false), Some(false)]),
        )
        .unwrap();
        // lt_eq: float64array <= decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::LtEq,
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]),
        )
        .unwrap();
        // gt: float64array > decimal array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::Gt,
            BooleanArray::from(vec![Some(false), None, Some(true), Some(false)]),
        )
        .unwrap();
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::GtEq,
            BooleanArray::from(vec![Some(true), None, Some(true), Some(true)]),
        )
        .unwrap();
        // is distinct: float64array is distinct decimal array
        // TODO: now we do not refactor the `is distinct or is not distinct` rule of coercion.
        // traced by https://github.com/apache/arrow-datafusion/issues/1590
        // the decimal array will be casted to float64array
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::IsDistinctFrom,
            BooleanArray::from(vec![Some(false), Some(true), Some(true), Some(false)]),
        )
        .unwrap();
        // is not distinct
        apply_logic_op(
            &schema,
            &float64_array,
            &decimal_array,
            Operator::IsNotDistinctFrom,
            BooleanArray::from(vec![Some(true), Some(false), Some(false), Some(true)]),
        )
        .unwrap();

        Ok(())
    }

    #[test]
    fn arithmetic_decimal_op_test() -> Result<()> {
        let value_i128: i128 = 123;
        let left_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                None,
                Some(value_i128 - 1),
                Some(value_i128 + 1),
            ],
            25,
            3,
        )?;
        let right_decimal_array = create_decimal_array(
            &[
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
                Some(value_i128),
            ],
            25,
            3,
        )?;
        // add
        let result = add_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect =
            create_decimal_array(&[Some(246), None, Some(245), Some(247)], 25, 3)?;
        assert_eq!(expect, result);
        // subtract
        let result = subtract_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(0), None, Some(-1), Some(1)], 25, 3)?;
        assert_eq!(expect, result);
        // multiply
        let result = multiply_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(15), None, Some(15), Some(15)], 25, 3)?;
        assert_eq!(expect, result);
        // divide
        let left_decimal_array = create_decimal_array(
            &[Some(1234567), None, Some(1234567), Some(1234567)],
            25,
            3,
        )?;
        let right_decimal_array =
            create_decimal_array(&[Some(10), Some(100), Some(55), Some(-123)], 25, 3)?;
        let result = divide_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(
            &[Some(123456700), None, Some(22446672), Some(-10037130)],
            25,
            3,
        )?;
        assert_eq!(expect, result);
        // modulus
        let result = modulus_decimal(&left_decimal_array, &right_decimal_array)?;
        let expect = create_decimal_array(&[Some(7), None, Some(37), Some(16)], 25, 3)?;
        assert_eq!(expect, result);

        Ok(())
    }

    fn apply_arithmetic_op(
        schema: &SchemaRef,
        left: &ArrayRef,
        right: &ArrayRef,
        op: Operator,
        expected: ArrayRef,
    ) -> Result<()> {
        let arithmetic_op =
            binary_simple(col("a", schema)?, op, col("b", schema)?, schema);
        let data: Vec<ArrayRef> = vec![left.clone(), right.clone()];
        let batch = RecordBatch::try_new(schema.clone(), data)?;
        let result = arithmetic_op.evaluate(&batch)?.into_array(batch.num_rows());

        assert_eq!(result.as_ref(), expected.as_ref());
        Ok(())
    }

    #[test]
    fn arithmetic_decimal_expr_test() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let value: i128 = 123;
        let decimal_array = Arc::new(create_decimal_array(
            &[
                Some(value as i128), // 1.23
                None,
                Some((value - 1) as i128), // 1.22
                Some((value + 1) as i128), // 1.24
            ],
            10,
            2,
        )?) as ArrayRef;
        let int32_array = Arc::new(Int32Array::from(vec![
            Some(123),
            Some(122),
            Some(123),
            Some(124),
        ])) as ArrayRef;

        // add: Int32array add decimal array
        let expect = Arc::new(create_decimal_array(
            &[Some(12423), None, Some(12422), Some(12524)],
            13,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Plus,
            expect,
        )
        .unwrap();

        // subtract: decimal array subtract int32 array
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[Some(-12177), None, Some(-12178), Some(-12276)],
            13,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Minus,
            expect,
        )
        .unwrap();

        // multiply: decimal array multiply int32 array
        let expect = Arc::new(create_decimal_array(
            &[Some(15129), None, Some(15006), Some(15376)],
            21,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Multiply,
            expect,
        )
        .unwrap();
        // divide: int32 array divide decimal array
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[
                Some(10000000000000),
                None,
                Some(10081967213114),
                Some(10000000000000),
            ],
            23,
            11,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Divide,
            expect,
        )
        .unwrap();
        // modulus: int32 array modulus decimal array
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Decimal(10, 2), true),
        ]));
        let expect = Arc::new(create_decimal_array(
            &[Some(000), None, Some(100), Some(000)],
            10,
            2,
        )?) as ArrayRef;
        apply_arithmetic_op(
            &schema,
            &int32_array,
            &decimal_array,
            Operator::Modulo,
            expect,
        )
        .unwrap();

        Ok(())
    }

    #[test]
    fn bitwise_array_test() -> Result<()> {
        let left = Arc::new(Int32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right =
            Arc::new(Int32Array::from(vec![Some(1), Some(3), Some(7)])) as ArrayRef;
        let result = bitwise_and(left, right)?;
        let expected = Int32Array::from(vec![Some(0), None, Some(3)]);
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn bitwise_scalar_test() -> Result<()> {
        let left = Arc::new(Int32Array::from(vec![Some(12), None, Some(11)])) as ArrayRef;
        let right = ScalarValue::from(3i32);
        let result = bitwise_and_scalar(&left, right).unwrap()?;
        let expected = Int32Array::from(vec![Some(0), None, Some(3)]);
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }
}
