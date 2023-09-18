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
use std::sync::Arc;

use crate::{AggregateExpr, PhysicalExpr};
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, BooleanArray},
    datatypes::Field,
};
use datafusion_common::{downcast_value, DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;

use crate::aggregate::row_accumulator::{
    is_row_accumulator_support_dtype, RowAccumulator,
};
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use arrow::array::Array;
use arrow::compute::{bool_and, bool_or};
use datafusion_row::accessor::RowAccessor;

// returns the new value after bool_and/bool_or with the new values, taking nullability into account
macro_rules! typed_bool_and_or_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let delta = $OP(array);
        Ok(ScalarValue::$SCALAR(delta))
    }};
}

// bool_and/bool_or the array and returns a ScalarValue of its corresponding type.
macro_rules! bool_and_or_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            DataType::Boolean => {
                typed_bool_and_or_batch!($VALUES, BooleanArray, Boolean, $OP)
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "Bool and/Bool or is not expected to receive the type {e:?}"
                )));
            }
        }
    }};
}

/// dynamically-typed bool_and(array) -> ScalarValue
fn bool_and_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bool_and_or_batch!(values, bool_and)
}

/// dynamically-typed bool_or(array) -> ScalarValue
fn bool_or_batch(values: &ArrayRef) -> Result<ScalarValue> {
    bool_and_or_batch!(values, bool_or)
}

// bool_and/bool_or of two scalar values.
macro_rules! typed_bool_and_or_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $TYPE:ident, $OP:ident) => {{
        paste::item! {
            match $SCALAR {
                None => {}
                Some(v) => $ACC.[<$OP _ $TYPE>]($INDEX, *v as $TYPE)
            }
        }
    }};
}

macro_rules! bool_and_or_v2 {
    ($INDEX:ident, $ACC:ident, $SCALAR:expr, $OP:ident) => {{
        Ok(match $SCALAR {
            ScalarValue::Boolean(rhs) => {
                typed_bool_and_or_v2!($INDEX, $ACC, rhs, bool, $OP)
            }
            ScalarValue::Null => {
                // do nothing
            }
            e => {
                return Err(DataFusionError::Internal(format!(
                    "BOOL AND/BOOL OR is not expected to receive scalars of incompatible types {:?}",
                    e
                )))
            }
        })
    }};
}

pub fn bool_and_row(
    index: usize,
    accessor: &mut RowAccessor,
    s: &ScalarValue,
) -> Result<()> {
    bool_and_or_v2!(index, accessor, s, bitand)
}

pub fn bool_or_row(
    index: usize,
    accessor: &mut RowAccessor,
    s: &ScalarValue,
) -> Result<()> {
    bool_and_or_v2!(index, accessor, s, bitor)
}

/// BOOL_AND aggregate expression
#[derive(Debug, Clone)]
pub struct BoolAnd {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl BoolAnd {
    /// Create a new BOOL_AND aggregate function
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

impl AggregateExpr for BoolAnd {
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
        Ok(Box::new(BoolAndAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "bool_and"),
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

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.data_type)
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(BoolAndRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BoolAndAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for BoolAnd {
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
struct BoolAndAccumulator {
    bool_and: ScalarValue,
}

impl BoolAndAccumulator {
    /// new bool_and accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            bool_and: ScalarValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for BoolAndAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = &bool_and_batch(values)?;
        self.bool_and = self.bool_and.and(delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bool_and.clone()])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.bool_and.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.bool_and)
            + self.bool_and.size()
    }
}

#[derive(Debug)]
struct BoolAndRowAccumulator {
    index: usize,
    datatype: DataType,
}

impl BoolAndRowAccumulator {
    pub fn new(index: usize, datatype: DataType) -> Self {
        Self { index, datatype }
    }
}

impl RowAccumulator for BoolAndRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &bool_and_batch(values)?;
        bool_and_row(self.index, accessor, delta)
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let value = &values[0];
        bool_and_row(self.index, accessor, value)
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        bool_and_row(self.index, accessor, value)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.datatype, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
    }
}

/// BOOL_OR aggregate expression
#[derive(Debug, Clone)]
pub struct BoolOr {
    name: String,
    pub data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl BoolOr {
    /// Create a new BOOL_OR aggregate function
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

impl AggregateExpr for BoolOr {
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
        Ok(Box::new(BoolOrAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "bool_or"),
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

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.data_type)
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(BoolOrRowAccumulator::new(
            start_index,
            self.data_type.clone(),
        )))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BoolOrAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for BoolOr {
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
struct BoolOrAccumulator {
    bool_or: ScalarValue,
}

impl BoolOrAccumulator {
    /// new bool_or accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            bool_or: ScalarValue::try_from(data_type)?,
        })
    }
}

impl Accumulator for BoolOrAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bool_or.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = bool_or_batch(values)?;
        self.bool_or = self.bool_or.or(&delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(self.bool_or.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.bool_or)
            + self.bool_or.size()
    }
}

#[derive(Debug)]
struct BoolOrRowAccumulator {
    index: usize,
    datatype: DataType,
}

impl BoolOrRowAccumulator {
    pub fn new(index: usize, datatype: DataType) -> Self {
        Self { index, datatype }
    }
}

impl RowAccumulator for BoolOrRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        let delta = &bool_or_batch(values)?;
        bool_or_row(self.index, accessor, delta)?;
        Ok(())
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let value = &values[0];
        bool_or_row(self.index, accessor, value)
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        bool_or_row(self.index, accessor, value)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        self.update_batch(states, accessor)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        Ok(accessor.get_as_scalar(&self.datatype, self.index))
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.index
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
    use arrow_array::BooleanArray;
    use datafusion_common::Result;

    #[test]
    fn test_bool_and() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));
        generic_test_op!(a, DataType::Boolean, BoolAnd, ScalarValue::from(false))
    }

    #[test]
    fn bool_and_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(true),
            Some(true),
        ]));
        generic_test_op!(a, DataType::Boolean, BoolAnd, ScalarValue::from(true))
    }

    #[test]
    fn bool_and_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![None, None]));
        generic_test_op!(a, DataType::Boolean, BoolAnd, ScalarValue::Boolean(None))
    }

    #[test]
    fn test_bool_or() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![true, true, false]));
        generic_test_op!(a, DataType::Boolean, BoolOr, ScalarValue::from(true))
    }

    #[test]
    fn bool_or_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            None,
            Some(false),
            Some(false),
        ]));
        generic_test_op!(a, DataType::Boolean, BoolOr, ScalarValue::from(false))
    }

    #[test]
    fn bool_or_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(BooleanArray::from(vec![None, None]));
        generic_test_op!(a, DataType::Boolean, BoolOr, ScalarValue::Boolean(None))
    }
}
