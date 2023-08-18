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

//! Defines BitAnd, BitOr, and BitXor Aggregate accumulators

use ahash::RandomState;
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::datatypes::{
    DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow::{
    array::{
        ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::Field,
};
use datafusion_common::internal_err;
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::collections::HashSet;

use crate::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::compute::{bit_and, bit_or, bit_xor};

/// Creates a [`PrimitiveGroupsAccumulator`] with the specified
/// [`ArrowPrimitiveType`] that initailizes each accumulator to $START
/// and applies `$FN` to each element
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_accumulator {
    ($SELF:expr, $START:expr, $PRIMTYPE:ident, $FN:expr) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$SELF.data_type, $FN)
                .with_starting_value($START),
        ))
    }};
}

// returns the new value after bit_and/bit_or/bit_xor with the new values, taking nullability into account
macro_rules! typed_bit_and_or_xor_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let delta = $OP(array);
        Ok(ScalarValue::$SCALAR(delta))
    }};
}

// bit_and/bit_or/bit_xor the array and returns a ScalarValue of its corresponding type.
macro_rules! bit_and_or_xor_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Int64 => {
                typed_bit_and_or_xor_batch!($VALUES, Int64Array, Int64, $OP)
            }
            DataType::Int32 => {
                typed_bit_and_or_xor_batch!($VALUES, Int32Array, Int32, $OP)
            }
            DataType::Int16 => {
                typed_bit_and_or_xor_batch!($VALUES, Int16Array, Int16, $OP)
            }
            DataType::Int8 => {
                typed_bit_and_or_xor_batch!($VALUES, Int8Array, Int8, $OP)
            }
            DataType::UInt64 => {
                typed_bit_and_or_xor_batch!($VALUES, UInt64Array, UInt64, $OP)
            }
            DataType::UInt32 => {
                typed_bit_and_or_xor_batch!($VALUES, UInt32Array, UInt32, $OP)
            }
            DataType::UInt16 => {
                typed_bit_and_or_xor_batch!($VALUES, UInt16Array, UInt16, $OP)
            }
            DataType::UInt8 => {
                typed_bit_and_or_xor_batch!($VALUES, UInt8Array, UInt8, $OP)
            }
            e => {
                return internal_err!(
                    "Bit and/Bit or/Bit xor is not expected to receive the type {e:?}"
                );
            }
        }
    }};
}

/// dynamically-typed bit_and(array) -> ScalarValue
fn bit_and_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bit_and_or_xor_batch!(values, bit_and)
}

/// dynamically-typed bit_or(array) -> ScalarValue
fn bit_or_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bit_and_or_xor_batch!(values, bit_or)
}

/// dynamically-typed bit_xor(array) -> ScalarValue
fn bit_xor_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bit_and_or_xor_batch!(values, bit_xor)
}

/// BIT_AND aggregate expression
#[derive(Debug, Clone)]
pub struct BitAnd {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl BitAnd {
    /// Create a new BIT_AND aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for BitAnd {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitAndAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "bit_and"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use std::ops::BitAndAssign;
        // Note the default value for BitAnd should be all set
        // (e.g. `0b11...111`) use MAX / -1 here to get appropriate
        // bit pattern for each type
        match self.data_type {
            DataType::Int8 => {
                instantiate_accumulator!(self, -1, Int8Type, |x, y| x.bitand_assign(y))
            }
            DataType::Int16 => {
                instantiate_accumulator!(self, -1, Int16Type, |x, y| x.bitand_assign(y))
            }
            DataType::Int32 => {
                instantiate_accumulator!(self, -1, Int32Type, |x, y| x.bitand_assign(y))
            }
            DataType::Int64 => {
                instantiate_accumulator!(self, -1, Int64Type, |x, y| x.bitand_assign(y))
            }
            DataType::UInt8 => {
                instantiate_accumulator!(self, u8::MAX, UInt8Type, |x, y| x
                    .bitand_assign(y))
            }
            DataType::UInt16 => {
                instantiate_accumulator!(self, u16::MAX, UInt16Type, |x, y| x
                    .bitand_assign(y))
            }
            DataType::UInt32 => {
                instantiate_accumulator!(self, u32::MAX, UInt32Type, |x, y| x
                    .bitand_assign(y))
            }
            DataType::UInt64 => {
                instantiate_accumulator!(self, u64::MAX, UInt64Type, |x, y| x
                    .bitand_assign(y))
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }
}

impl PartialEq<dyn Any> for BitAnd {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct BitAndAccumulator {
    bit_and: ScalarValue,
}

impl BitAndAccumulator {
    /// new bit_and accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            bit_and: ScalarValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for BitAndAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &bit_and_batch(values)?;
        self.bit_and = self.bit_and.bitand(delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bit_and.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.bit_and.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.bit_and)
            + self.bit_and.size()
    }
}

/// BIT_OR aggregate expression
#[derive(Debug, Clone)]
pub struct BitOr {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl BitOr {
    /// Create a new BIT_OR aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for BitOr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitOrAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "bit_or"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use std::ops::BitOrAssign;
        match self.data_type {
            DataType::Int8 => {
                instantiate_accumulator!(self, 0, Int8Type, |x, y| x.bitor_assign(y))
            }
            DataType::Int16 => {
                instantiate_accumulator!(self, 0, Int16Type, |x, y| x.bitor_assign(y))
            }
            DataType::Int32 => {
                instantiate_accumulator!(self, 0, Int32Type, |x, y| x.bitor_assign(y))
            }
            DataType::Int64 => {
                instantiate_accumulator!(self, 0, Int64Type, |x, y| x.bitor_assign(y))
            }
            DataType::UInt8 => {
                instantiate_accumulator!(self, 0, UInt8Type, |x, y| x.bitor_assign(y))
            }
            DataType::UInt16 => {
                instantiate_accumulator!(self, 0, UInt16Type, |x, y| x.bitor_assign(y))
            }
            DataType::UInt32 => {
                instantiate_accumulator!(self, 0, UInt32Type, |x, y| x.bitor_assign(y))
            }
            DataType::UInt64 => {
                instantiate_accumulator!(self, 0, UInt64Type, |x, y| x.bitor_assign(y))
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }
}

impl PartialEq<dyn Any> for BitOr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct BitOrAccumulator {
    bit_or: ScalarValue,
}

impl BitOrAccumulator {
    /// new bit_or accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            bit_or: ScalarValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for BitOrAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bit_or.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &bit_or_batch(values)?;
        self.bit_or = self.bit_or.bitor(delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.bit_or.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.bit_or)
            + self.bit_or.size()
    }
}

/// BIT_XOR aggregate expression
#[derive(Debug, Clone)]
pub struct BitXor {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl BitXor {
    /// Create a new BIT_XOR aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for BitXor {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BitXorAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "bit_xor"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use std::ops::BitXorAssign;
        match self.data_type {
            DataType::Int8 => {
                instantiate_accumulator!(self, 0, Int8Type, |x, y| x.bitxor_assign(y))
            }
            DataType::Int16 => {
                instantiate_accumulator!(self, 0, Int16Type, |x, y| x.bitxor_assign(y))
            }
            DataType::Int32 => {
                instantiate_accumulator!(self, 0, Int32Type, |x, y| x.bitxor_assign(y))
            }
            DataType::Int64 => {
                instantiate_accumulator!(self, 0, Int64Type, |x, y| x.bitxor_assign(y))
            }
            DataType::UInt8 => {
                instantiate_accumulator!(self, 0, UInt8Type, |x, y| x.bitxor_assign(y))
            }
            DataType::UInt16 => {
                instantiate_accumulator!(self, 0, UInt16Type, |x, y| x.bitxor_assign(y))
            }
            DataType::UInt32 => {
                instantiate_accumulator!(self, 0, UInt32Type, |x, y| x.bitxor_assign(y))
            }
            DataType::UInt64 => {
                instantiate_accumulator!(self, 0, UInt64Type, |x, y| x.bitxor_assign(y))
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }
}

impl PartialEq<dyn Any> for BitXor {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct BitXorAccumulator {
    bit_xor: ScalarValue,
}

impl BitXorAccumulator {
    /// new bit_xor accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            bit_xor: ScalarValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for BitXorAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bit_xor.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &bit_xor_batch(values)?;
        self.bit_xor = self.bit_xor.bitxor(delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.bit_xor.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.bit_xor)
            + self.bit_xor.size()
    }
}

/// Expression for a BIT_XOR(DISTINCT) aggregation.
#[derive(Debug, Clone)]
pub struct DistinctBitXor {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl DistinctBitXor {
    /// Create a new DistinctBitXor aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

impl AggregateExpr for DistinctBitXor {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctBitXorAccumulator::try_new(
            &self.data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        // State field is a List which stores items to rebuild hash set.
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "bit_xor distinct"),
            Field::new("item", self.data_type.clone(), true),
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

impl PartialEq<dyn Any> for DistinctBitXor {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct DistinctBitXorAccumulator {
    hash_values: HashSet<ScalarValue, RandomState>,
    data_type: DataType,
}

impl DistinctBitXorAccumulator {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            hash_values: HashSet::default(),
            data_type: data_type.clone(),
        })
    }
}

impl Accumulator for DistinctBitXorAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let mut distinct_values = Vec::new();
            self.hash_values
                .iter()
                .for_each(|distinct_value| distinct_values.push(distinct_value.clone()));
            vec![ScalarValue::new_list(
                Some(distinct_values),
                self.data_type.clone(),
            )]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        (0..values[0].len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let v = ScalarValue::try_from_array(arr, index)?;
                self.hash_values.insert(v);
            }
            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = &states[0];
        (0..arr.len()).try_for_each(|index| {
            let scalar = ScalarValue::try_from_array(arr, index)?;

            if let ScalarValue::List(Some(scalar), _) = scalar {
                scalar.iter().for_each(|scalar| {
                    if !ScalarValue::is_null(scalar) {
                        self.hash_values.insert(scalar.clone());
                    }
                });
            } else {
                return internal_err!("Unexpected accumulator state");
            }
            Ok(())
        })
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let mut bit_xor_value = ScalarValue::try_from(&self.data_type)?;
        for distinct_value in self.hash_values.iter() {
            bit_xor_value = bit_xor_value.bitxor(distinct_value)?;
        }
        Ok(bit_xor_value)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_hashset(&self.hash_values)
            - std::mem::size_of_val(&self.hash_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;

    #[test]
    fn bit_and_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![4, 7, 15]));
        generic_test_op!(a, DataType::Int32, BitAnd, ScalarValue::from(4i32))
    }

    #[test]
    fn bit_and_i32_with_nulls() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(5)]));
        generic_test_op!(a, DataType::Int32, BitAnd, ScalarValue::from(1i32))
    }

    #[test]
    fn bit_and_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, BitAnd, ScalarValue::Int32(None))
    }

    #[test]
    fn bit_and_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![4_u32, 7_u32, 15_u32]));
        generic_test_op!(a, DataType::UInt32, BitAnd, ScalarValue::from(4u32))
    }

    #[test]
    fn bit_or_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![4, 7, 15]));
        generic_test_op!(a, DataType::Int32, BitOr, ScalarValue::from(15i32))
    }

    #[test]
    fn bit_or_i32_with_nulls() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(5)]));
        generic_test_op!(a, DataType::Int32, BitOr, ScalarValue::from(7i32))
    }

    #[test]
    fn bit_or_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, BitOr, ScalarValue::Int32(None))
    }

    #[test]
    fn bit_or_u32() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![4_u32, 7_u32, 15_u32]));
        generic_test_op!(a, DataType::UInt32, BitOr, ScalarValue::from(15u32))
    }

    #[test]
    fn bit_xor_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![4, 7, 4, 7, 15]));
        generic_test_op!(a, DataType::Int32, BitXor, ScalarValue::from(15i32))
    }

    #[test]
    fn bit_xor_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(3),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, BitXor, ScalarValue::from(6i32))
    }

    #[test]
    fn bit_xor_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, BitXor, ScalarValue::Int32(None))
    }

    #[test]
    fn bit_xor_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![4_u32, 7_u32, 4_u32, 7_u32, 15_u32]));
        generic_test_op!(a, DataType::UInt32, BitXor, ScalarValue::from(15u32))
    }

    #[test]
    fn bit_xor_distinct_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![4, 7, 4, 7, 15]));
        generic_test_op!(a, DataType::Int32, DistinctBitXor, ScalarValue::from(12i32))
    }

    #[test]
    fn bit_xor_distinct_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(3),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, DistinctBitXor, ScalarValue::from(7i32))
    }

    #[test]
    fn bit_xor_distinct_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, DistinctBitXor, ScalarValue::Int32(None))
    }

    #[test]
    fn bit_xor_distinct_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![4_u32, 7_u32, 4_u32, 7_u32, 15_u32]));
        generic_test_op!(
            a,
            DataType::UInt32,
            DistinctBitXor,
            ScalarValue::from(12u32)
        )
    }
}
