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
use datafusion_common::cast::as_list_array;
use std::any::Any;
use std::sync::Arc;

use crate::{AggregateExpr, PhysicalExpr};
use arrow::datatypes::DataType;
use arrow::{array::ArrayRef, datatypes::Field};
use datafusion_common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, GroupsAccumulator};
use std::collections::HashSet;

use crate::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::compute::{bit_and, bit_or, bit_xor};
use arrow_array::cast::AsArray;
use arrow_array::{downcast_integer, ArrowNumericType};
use arrow_buffer::ArrowNativeType;

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
        macro_rules! helper {
            ($t:ty) => {
                Ok(Box::<BitAndAccumulator<$t>>::default())
            };
        }
        downcast_integer! {
            &self.data_type => (helper),
            _ => Err(DataFusionError::NotImplemented(format!(
                "BitAndAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
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

        // Note the default value for BitAnd should be all set, i.e. `!0`
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(
                    PrimitiveGroupsAccumulator::<$t, _>::new($dt, |x, y| {
                        x.bitand_assign(y)
                    })
                    .with_starting_value(!0),
                ))
            };
        }

        let data_type = &self.data_type;
        downcast_integer! {
            data_type => (helper, data_type),
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ),
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

struct BitAndAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitAndAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitAndAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitAndAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitAndAccumulator<T>
where
    T::Native: std::ops::BitAnd<Output = T::Native>,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = bit_and(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(x);
            *v = *v & x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
        macro_rules! helper {
            ($t:ty) => {
                Ok(Box::<BitOrAccumulator<$t>>::default())
            };
        }
        downcast_integer! {
            &self.data_type => (helper),
            _ => Err(DataFusionError::NotImplemented(format!(
                "BitOrAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
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
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PrimitiveGroupsAccumulator::<$t, _>::new(
                    $dt,
                    |x, y| x.bitor_assign(y),
                )))
            };
        }

        let data_type = &self.data_type;
        downcast_integer! {
            data_type => (helper, data_type),
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ),
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

struct BitOrAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitOrAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitOrAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitOrAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitOrAccumulator<T>
where
    T::Native: std::ops::BitOr<Output = T::Native>,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = bit_or(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v | x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
        macro_rules! helper {
            ($t:ty) => {
                Ok(Box::<BitXorAccumulator<$t>>::default())
            };
        }
        downcast_integer! {
            &self.data_type => (helper),
            _ => Err(DataFusionError::NotImplemented(format!(
                "BitXor not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
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
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(PrimitiveGroupsAccumulator::<$t, _>::new(
                    $dt,
                    |x, y| x.bitxor_assign(y),
                )))
            };
        }

        let data_type = &self.data_type;
        downcast_integer! {
            data_type => (helper, data_type),
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ),
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

struct BitXorAccumulator<T: ArrowNumericType> {
    value: Option<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for BitXorAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for BitXorAccumulator<T> {
    fn default() -> Self {
        Self { value: None }
    }
}

impl<T: ArrowNumericType> Accumulator for BitXorAccumulator<T>
where
    T::Native: std::ops::BitXor<Output = T::Native>,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(x) = bit_xor(values[0].as_primitive::<T>()) {
            let v = self.value.get_or_insert(T::Native::usize_as(0));
            *v = *v ^ x;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        ScalarValue::new_primitive::<T>(self.value, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
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
        macro_rules! helper {
            ($t:ty) => {
                Ok(Box::<DistinctBitXorAccumulator<$t>>::default())
            };
        }
        downcast_integer! {
            &self.data_type => (helper),
            _ => Err(DataFusionError::NotImplemented(format!(
                "DistinctBitXorAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
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

struct DistinctBitXorAccumulator<T: ArrowNumericType> {
    values: HashSet<T::Native, RandomState>,
}

impl<T: ArrowNumericType> std::fmt::Debug for DistinctBitXorAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctBitXorAccumulator({})", T::DATA_TYPE)
    }
}

impl<T: ArrowNumericType> Default for DistinctBitXorAccumulator<T> {
    fn default() -> Self {
        Self {
            values: HashSet::default(),
        }
    }
}

impl<T: ArrowNumericType> Accumulator for DistinctBitXorAccumulator<T>
where
    T::Native: std::ops::BitXor<Output = T::Native> + std::hash::Hash + Eq,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let values = self
                .values
                .iter()
                .map(|x| ScalarValue::new_primitive::<T>(Some(*x), &T::DATA_TYPE))
                .collect::<Result<Vec<_>>>()?;

            let arr = ScalarValue::new_list(&values, &T::DATA_TYPE);
            vec![ScalarValue::List(arr)]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(array.value(idx));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(*x);
            }),
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if let Some(state) = states.first() {
            let list_arr = as_list_array(state)?;
            for arr in list_arr.iter().flatten() {
                self.update_batch(&[arr])?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc ^ *distinct_value;
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &T::DATA_TYPE)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<T::Native>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::array::*;
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
