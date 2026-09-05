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

//! [`Max`] and [`MaxAccumulator`] accumulator for the `max` function
//! [`Min`] and [`MinAccumulator`] accumulator for the `min` function

mod min_max_bytes;
mod min_max_struct;
mod blocked_min_max_bytes;

use arrow::array::ArrayRef;
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Float16Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, exec_err, internal_err};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::blocked_prim_op::BlockedPrimitiveGroupsAccumulator;
use datafusion_physical_expr::expressions;
use std::cmp::Ordering;
use std::fmt::Debug;

use arrow::datatypes::i256;
use arrow::datatypes::{
    Date32Type, Date64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};

use crate::min_max::min_max_bytes::MinMaxBytesAccumulator;
use crate::min_max::min_max_struct::MinMaxStructAccumulator;
use datafusion_common::ScalarValue;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, SetMonotonicity, Signature, Volatility,
    function::AccumulatorArgs,
};
use datafusion_expr::{GroupsAccumulator, StatisticsArgs};
use datafusion_macros::user_doc;
use half::f16;
use std::collections::VecDeque;
use std::mem::{size_of, size_of_val};
use std::ops::Deref;
use datafusion_expr::groups_accumulator::BlockedGroupsAccumulator;
use datafusion_functions_aggregate_common::accumulator::BlockedAccumulatorArgs;

fn get_min_max_result_type(input_types: &[DataType]) -> Result<Vec<DataType>> {
    // make sure that the input types only has one element.
    if input_types.len() != 1 {
        return exec_err!(
            "min/max was called with {} arguments. It requires only 1.",
            input_types.len()
        );
    }
    // min and max support the dictionary data type
    // unpack the dictionary to get the value
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        // TODO add checker for datatype which min and max supported
        // For example, the `Struct` and `Map` type are not supported in the MIN and MAX function
        _ => Ok(input_types.to_vec()),
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the maximum value in the specified column.",
    syntax_example = "max(expression)",
    sql_example = r#"```sql
> SELECT max(column_name) FROM table_name;
+----------------------+
| max(column_name)     |
+----------------------+
| 150                  |
+----------------------+
```"#,
    standard_argument(name = "expression",)
)]
// MAX aggregate UDF
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Max {
    signature: Signature,
}

impl Max {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}
/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MAX`
/// the specified [`ArrowPrimitiveType`].
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! primitive_max_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new($DATA_TYPE, |cur, new| {
                match (new).partial_cmp(cur) {
                    Some(Ordering::Greater) | None => {
                        // new is Greater or None
                        *cur = new
                    }
                    _ => {}
                }
            })
            // Initialize each accumulator to $NATIVE::MIN
            .with_starting_value($NATIVE::MIN),
        ))
    }};
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, total, $BITS:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new($DATA_TYPE, |cur, new| {
                if new.total_cmp(cur) == Ordering::Greater {
                    *cur = new
                }
            })
            // Use the total-order minimum so negative NaNs replace the sentinel.
            .with_starting_value($NATIVE::from_bits($BITS::MAX)),
        ))
    }};
}
/// Creates a [`BlockedPrimitiveGroupsAccumulator`] for computing `MAX`
/// the specified [`ArrowPrimitiveType`].
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! primitive_max_blocked_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, $BLOCK_SIZE:expr) => {{
        Ok(Box::new(
            BlockedPrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new($DATA_TYPE, |cur, new| {
                match (new).partial_cmp(cur) {
                    Some(Ordering::Greater) | None => {
                        // new is Greater or None
                        *cur = new
                    }
                    _ => {}
                }
            }, $BLOCK_SIZE)
            // Initialize each accumulator to $NATIVE::MIN
            .with_starting_value($NATIVE::MIN),
        ))
    }};
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, total, $BITS:ident, $BLOCK_SIZE:expr) => {{
        Ok(Box::new(
            BlockedPrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new($DATA_TYPE, |cur, new| {
                if new.total_cmp(cur) == Ordering::Greater {
                    *cur = new
                }
            }, $BLOCK_SIZE)
            // Use the total-order minimum so negative NaNs replace the sentinel.
            .with_starting_value($NATIVE::from_bits($BITS::MAX)),
        ))
    }};
}

/// Creates a [`PrimitiveGroupsAccumulator`] for computing `MIN`
/// the specified [`ArrowPrimitiveType`].
///
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! primitive_min_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$DATA_TYPE, |cur, new| {
                match (new).partial_cmp(cur) {
                    Some(Ordering::Less) | None => {
                        // new is Less or NaN
                        *cur = new
                    }
                    _ => {}
                }
            })
            // Initialize each accumulator to $NATIVE::MAX
            .with_starting_value($NATIVE::MAX),
        ))
    }};
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, total, $BITS:ident) => {{
        Ok(Box::new(
            PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$DATA_TYPE, |cur, new| {
                if new.total_cmp(cur) == Ordering::Less {
                    *cur = new
                }
            })
            // Use the total-order maximum so positive NaNs replace the sentinel.
            .with_starting_value($NATIVE::from_bits($BITS::MAX >> 1)),
        ))
    }};
}

/// Creates a [`BlockedPrimitiveGroupsAccumulator`] for computing `MIN`
/// the specified [`ArrowPrimitiveType`].
///
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! primitive_min_blocked_accumulator {
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, $BLOCK_SIZE:expr) => {{
        Ok(Box::new(
            BlockedPrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$DATA_TYPE, |cur, new| {
                match (new).partial_cmp(cur) {
                    Some(Ordering::Less) | None => {
                        // new is Less or NaN
                        *cur = new
                    }
                    _ => {}
                }
            }, $BLOCK_SIZE)
            // Initialize each accumulator to $NATIVE::MAX
            .with_starting_value($NATIVE::MAX),
        ))
    }};
    ($DATA_TYPE:ident, $NATIVE:ident, $PRIMTYPE:ident, total, $BITS:ident, $BLOCK_SIZE:expr) => {{
        Ok(Box::new(
            BlockedPrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(&$DATA_TYPE, |cur, new| {
                if new.total_cmp(cur) == Ordering::Less {
                    *cur = new
                }
            }, $BLOCK_SIZE)
            // Use the total-order maximum so positive NaNs replace the sentinel.
            .with_starting_value($NATIVE::from_bits($BITS::MAX >> 1)),
        ))
    }};
}

trait FromColumnStatistics {
    fn value_from_column_statistics(
        &self,
        stats: &ColumnStatistics,
    ) -> Option<ScalarValue>;

    fn value_from_statistics(
        &self,
        statistics_args: &StatisticsArgs,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(num_rows) = &statistics_args.statistics.num_rows {
            match *num_rows {
                0 => return ScalarValue::try_from(statistics_args.return_type).ok(),
                value if value > 0 => {
                    let col_stats = &statistics_args.statistics.column_statistics;
                    if statistics_args.exprs.len() == 1 {
                        // TODO optimize with exprs other than Column
                        if let Some(col_expr) =
                            statistics_args.exprs[0].downcast_ref::<expressions::Column>()
                        {
                            return self.value_from_column_statistics(
                                &col_stats[col_expr.index()],
                            );
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }
}

impl FromColumnStatistics for Max {
    fn value_from_column_statistics(
        &self,
        col_stats: &ColumnStatistics,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(ref val) = col_stats.max_value
            && !val.is_null()
        {
            return Some(val.clone());
        }
        None
    }
}

impl AggregateUDFImpl for Max {
    fn name(&self) -> &str {
        "max"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MaxAccumulator::try_new(
            acc_args.return_field.data_type(),
        )?))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_field.data_type(),
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
                | Utf8
                | LargeUtf8
                | Utf8View
                | Binary
                | LargeBinary
                | BinaryView
                | Duration(_)
                | Struct(_)
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_field.data_type();
        match data_type {
            Int8 => primitive_max_accumulator!(data_type, i8, Int8Type),
            Int16 => primitive_max_accumulator!(data_type, i16, Int16Type),
            Int32 => primitive_max_accumulator!(data_type, i32, Int32Type),
            Int64 => primitive_max_accumulator!(data_type, i64, Int64Type),
            UInt8 => primitive_max_accumulator!(data_type, u8, UInt8Type),
            UInt16 => primitive_max_accumulator!(data_type, u16, UInt16Type),
            UInt32 => primitive_max_accumulator!(data_type, u32, UInt32Type),
            UInt64 => primitive_max_accumulator!(data_type, u64, UInt64Type),
            Float16 => {
                primitive_max_accumulator!(data_type, f16, Float16Type, total, u16)
            }
            Float32 => {
                primitive_max_accumulator!(data_type, f32, Float32Type, total, u32)
            }
            Float64 => {
                primitive_max_accumulator!(data_type, f64, Float64Type, total, u64)
            }
            Date32 => primitive_max_accumulator!(data_type, i32, Date32Type),
            Date64 => primitive_max_accumulator!(data_type, i64, Date64Type),
            Time32(Second) => {
                primitive_max_accumulator!(data_type, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                primitive_max_accumulator!(data_type, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                primitive_max_accumulator!(data_type, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                primitive_max_accumulator!(data_type, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                primitive_max_accumulator!(data_type, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                primitive_max_accumulator!(data_type, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                primitive_max_accumulator!(data_type, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                primitive_max_accumulator!(data_type, i64, TimestampNanosecondType)
            }
            Duration(Second) => {
                primitive_max_accumulator!(data_type, i64, DurationSecondType)
            }
            Duration(Millisecond) => {
                primitive_max_accumulator!(data_type, i64, DurationMillisecondType)
            }
            Duration(Microsecond) => {
                primitive_max_accumulator!(data_type, i64, DurationMicrosecondType)
            }
            Duration(Nanosecond) => {
                primitive_max_accumulator!(data_type, i64, DurationNanosecondType)
            }
            Decimal32(_, _) => {
                primitive_max_accumulator!(data_type, i32, Decimal32Type)
            }
            Decimal64(_, _) => {
                primitive_max_accumulator!(data_type, i64, Decimal64Type)
            }
            Decimal128(_, _) => {
                primitive_max_accumulator!(data_type, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                primitive_max_accumulator!(data_type, i256, Decimal256Type)
            }
            Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView => {
                Ok(Box::new(MinMaxBytesAccumulator::new_max(data_type.clone())))
            }
            Struct(_) => Ok(Box::new(MinMaxStructAccumulator::new_max(
                data_type.clone(),
            ))),
            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!("GroupsAccumulator not supported for max({})", data_type),
        }
    }

    fn blocked_groups_accumulator_supported(&self, args: BlockedAccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_field.data_type(),
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_blocked_groups_accumulator(
        &self,
        args: BlockedAccumulatorArgs,
    ) -> Result<Box<dyn BlockedGroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_field.data_type();
        let batch_size = args.batch_size;
        match data_type {
            Int8 => primitive_max_blocked_accumulator!(data_type, i8, Int8Type, batch_size),
            Int16 => primitive_max_blocked_accumulator!(data_type, i16, Int16Type, batch_size),
            Int32 => primitive_max_blocked_accumulator!(data_type, i32, Int32Type, batch_size),
            Int64 => primitive_max_blocked_accumulator!(data_type, i64, Int64Type, batch_size),
            UInt8 => primitive_max_blocked_accumulator!(data_type, u8, UInt8Type, batch_size),
            UInt16 => primitive_max_blocked_accumulator!(data_type, u16, UInt16Type, batch_size),
            UInt32 => primitive_max_blocked_accumulator!(data_type, u32, UInt32Type, batch_size),
            UInt64 => primitive_max_blocked_accumulator!(data_type, u64, UInt64Type, batch_size),
            Float16 => {
                primitive_max_blocked_accumulator!(data_type, f16, Float16Type, total, u16, batch_size)
            }
            Float32 => {
                primitive_max_blocked_accumulator!(data_type, f32, Float32Type, total, u32, batch_size)
            }
            Float64 => {
                primitive_max_blocked_accumulator!(data_type, f64, Float64Type, total, u64, batch_size)
            }
            Date32 => primitive_max_blocked_accumulator!(data_type, i32, Date32Type, batch_size),
            Date64 => primitive_max_blocked_accumulator!(data_type, i64, Date64Type, batch_size),
            Time32(Second) => {
                primitive_max_blocked_accumulator!(data_type, i32, Time32SecondType, batch_size)
            }
            Time32(Millisecond) => {
                primitive_max_blocked_accumulator!(data_type, i32, Time32MillisecondType, batch_size)
            }
            Time64(Microsecond) => {
                primitive_max_blocked_accumulator!(data_type, i64, Time64MicrosecondType, batch_size)
            }
            Time64(Nanosecond) => {
                primitive_max_blocked_accumulator!(data_type, i64, Time64NanosecondType, batch_size)
            }
            Timestamp(Second, _) => {
                primitive_max_blocked_accumulator!(data_type, i64, TimestampSecondType, batch_size)
            }
            Timestamp(Millisecond, _) => {
                primitive_max_blocked_accumulator!(data_type, i64, TimestampMillisecondType, batch_size)
            }
            Timestamp(Microsecond, _) => {
                primitive_max_blocked_accumulator!(data_type, i64, TimestampMicrosecondType, batch_size)
            }
            Timestamp(Nanosecond, _) => {
                primitive_max_blocked_accumulator!(data_type, i64, TimestampNanosecondType, batch_size)
            }
            Duration(Second) => {
                primitive_max_blocked_accumulator!(data_type, i64, DurationSecondType, batch_size)
            }
            Duration(Millisecond) => {
                primitive_max_blocked_accumulator!(data_type, i64, DurationMillisecondType, batch_size)
            }
            Duration(Microsecond) => {
                primitive_max_blocked_accumulator!(data_type, i64, DurationMicrosecondType, batch_size)
            }
            Duration(Nanosecond) => {
                primitive_max_blocked_accumulator!(data_type, i64, DurationNanosecondType, batch_size)
            }
            Decimal32(_, _) => {
                primitive_max_blocked_accumulator!(data_type, i32, Decimal32Type, batch_size)
            }
            Decimal64(_, _) => {
                primitive_max_blocked_accumulator!(data_type, i64, Decimal64Type, batch_size)
            }
            Decimal128(_, _) => {
                primitive_max_blocked_accumulator!(data_type, i128, Decimal128Type, batch_size)
            }
            Decimal256(_, _) => {
                primitive_max_blocked_accumulator!(data_type, i256, Decimal256Type, batch_size)
            }
            // This is only reached if blocked_groups_accumulator_supported is out of sync
            _ => internal_err!("BlockedGroupsAccumulator not supported for max({})", data_type),
        }
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMaxAccumulator::try_new(
            args.return_field.data_type(),
        )?))
    }

    fn is_descending(&self) -> Option<bool> {
        Some(true)
    }

    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::Insensitive
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_result_type(arg_types)
    }
    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Identical
    }
    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.value_from_statistics(statistics_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        // `MAX` is monotonically increasing as it always increases or stays
        // the same as new values are seen.
        SetMonotonicity::Increasing
    }
}

#[derive(Debug)]
pub struct SlidingMaxAccumulator {
    /// Typed NULL returned when the window contains no non-null values
    empty_value: ScalarValue,
    moving_max: MovingMax<ScalarValue>,
}

impl SlidingMaxAccumulator {
    /// new max accumulator
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            empty_value: ScalarValue::try_from(datatype)?,
            moving_max: MovingMax::<ScalarValue>::new(),
        })
    }

    fn current_max(&self) -> ScalarValue {
        match self.moving_max.max() {
            Some(res) => res.clone(),
            None => self.empty_value.clone(),
        }
    }
}

impl Accumulator for SlidingMaxAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                self.moving_max.push(val);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // We assume that values are retracted in the order they were added, so
        // the retracted values must be the oldest elements of `moving_max`.
        // NULLs are never pushed, so be sure to only pop once per non-NULL
        // value.
        let valid_count = values[0].len() - values[0].logical_null_count();
        for _ in 0..valid_count {
            self.moving_max.pop();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.current_max()])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.current_max())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.empty_value)
            + self.empty_value.size()
            + self.moving_max.heap_size(|sv| sv.size() - size_of_val(sv))
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the minimum value in the specified column.",
    syntax_example = "min(expression)",
    sql_example = r#"```sql
> SELECT min(column_name) FROM table_name;
+----------------------+
| min(column_name)     |
+----------------------+
| 12                   |
+----------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Min {
    signature: Signature,
}

impl Min {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl FromColumnStatistics for Min {
    fn value_from_column_statistics(
        &self,
        col_stats: &ColumnStatistics,
    ) -> Option<ScalarValue> {
        if let Precision::Exact(ref val) = col_stats.min_value
            && !val.is_null()
        {
            return Some(val.clone());
        }
        None
    }
}

impl AggregateUDFImpl for Min {
    fn name(&self) -> &str {
        "min"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(MinAccumulator::try_new(
            acc_args.return_field.data_type(),
        )?))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_field.data_type(),
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
                | Utf8
                | LargeUtf8
                | Utf8View
                | Binary
                | LargeBinary
                | BinaryView
                | Duration(_)
                | Struct(_)
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_field.data_type();
        match data_type {
            Int8 => primitive_min_accumulator!(data_type, i8, Int8Type),
            Int16 => primitive_min_accumulator!(data_type, i16, Int16Type),
            Int32 => primitive_min_accumulator!(data_type, i32, Int32Type),
            Int64 => primitive_min_accumulator!(data_type, i64, Int64Type),
            UInt8 => primitive_min_accumulator!(data_type, u8, UInt8Type),
            UInt16 => primitive_min_accumulator!(data_type, u16, UInt16Type),
            UInt32 => primitive_min_accumulator!(data_type, u32, UInt32Type),
            UInt64 => primitive_min_accumulator!(data_type, u64, UInt64Type),
            Float16 => {
                primitive_min_accumulator!(data_type, f16, Float16Type, total, u16)
            }
            Float32 => {
                primitive_min_accumulator!(data_type, f32, Float32Type, total, u32)
            }
            Float64 => {
                primitive_min_accumulator!(data_type, f64, Float64Type, total, u64)
            }
            Date32 => primitive_min_accumulator!(data_type, i32, Date32Type),
            Date64 => primitive_min_accumulator!(data_type, i64, Date64Type),
            Time32(Second) => {
                primitive_min_accumulator!(data_type, i32, Time32SecondType)
            }
            Time32(Millisecond) => {
                primitive_min_accumulator!(data_type, i32, Time32MillisecondType)
            }
            Time64(Microsecond) => {
                primitive_min_accumulator!(data_type, i64, Time64MicrosecondType)
            }
            Time64(Nanosecond) => {
                primitive_min_accumulator!(data_type, i64, Time64NanosecondType)
            }
            Timestamp(Second, _) => {
                primitive_min_accumulator!(data_type, i64, TimestampSecondType)
            }
            Timestamp(Millisecond, _) => {
                primitive_min_accumulator!(data_type, i64, TimestampMillisecondType)
            }
            Timestamp(Microsecond, _) => {
                primitive_min_accumulator!(data_type, i64, TimestampMicrosecondType)
            }
            Timestamp(Nanosecond, _) => {
                primitive_min_accumulator!(data_type, i64, TimestampNanosecondType)
            }
            Duration(Second) => {
                primitive_min_accumulator!(data_type, i64, DurationSecondType)
            }
            Duration(Millisecond) => {
                primitive_min_accumulator!(data_type, i64, DurationMillisecondType)
            }
            Duration(Microsecond) => {
                primitive_min_accumulator!(data_type, i64, DurationMicrosecondType)
            }
            Duration(Nanosecond) => {
                primitive_min_accumulator!(data_type, i64, DurationNanosecondType)
            }
            Decimal32(_, _) => {
                primitive_min_accumulator!(data_type, i32, Decimal32Type)
            }
            Decimal64(_, _) => {
                primitive_min_accumulator!(data_type, i64, Decimal64Type)
            }
            Decimal128(_, _) => {
                primitive_min_accumulator!(data_type, i128, Decimal128Type)
            }
            Decimal256(_, _) => {
                primitive_min_accumulator!(data_type, i256, Decimal256Type)
            }
            Utf8 | LargeUtf8 | Utf8View | Binary | LargeBinary | BinaryView => {
                Ok(Box::new(MinMaxBytesAccumulator::new_min(data_type.clone())))
            }
            Struct(_) => Ok(Box::new(MinMaxStructAccumulator::new_min(
                data_type.clone(),
            ))),
            // This is only reached if groups_accumulator_supported is out of sync
            _ => internal_err!("GroupsAccumulator not supported for min({})", data_type),
        }
    }

    fn blocked_groups_accumulator_supported(&self, args: BlockedAccumulatorArgs) -> bool {
        use DataType::*;
        matches!(
            args.return_field.data_type(),
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
        )
    }

    fn create_blocked_groups_accumulator(
        &self,
        args: BlockedAccumulatorArgs,
    ) -> Result<Box<dyn BlockedGroupsAccumulator>> {
        use DataType::*;
        use TimeUnit::*;
        let data_type = args.return_field.data_type();
        let batch_size = args.batch_size;
        match data_type {
            Int8 => primitive_min_blocked_accumulator!(data_type, i8, Int8Type, batch_size),
            Int16 => primitive_min_blocked_accumulator!(data_type, i16, Int16Type, batch_size),
            Int32 => primitive_min_blocked_accumulator!(data_type, i32, Int32Type, batch_size),
            Int64 => primitive_min_blocked_accumulator!(data_type, i64, Int64Type, batch_size),
            UInt8 => primitive_min_blocked_accumulator!(data_type, u8, UInt8Type, batch_size),
            UInt16 => primitive_min_blocked_accumulator!(data_type, u16, UInt16Type, batch_size),
            UInt32 => primitive_min_blocked_accumulator!(data_type, u32, UInt32Type, batch_size),
            UInt64 => primitive_min_blocked_accumulator!(data_type, u64, UInt64Type, batch_size),
            Float16 => {
                primitive_min_blocked_accumulator!(data_type, f16, Float16Type, total, u16, batch_size)
            }
            Float32 => {
                primitive_min_blocked_accumulator!(data_type, f32, Float32Type, total, u32, batch_size)
            }
            Float64 => {
                primitive_min_blocked_accumulator!(data_type, f64, Float64Type, total, u64, batch_size)
            }
            Date32 => primitive_min_blocked_accumulator!(data_type, i32, Date32Type, batch_size),
            Date64 => primitive_min_blocked_accumulator!(data_type, i64, Date64Type, batch_size),
            Time32(Second) => {
                primitive_min_blocked_accumulator!(data_type, i32, Time32SecondType, batch_size)
            }
            Time32(Millisecond) => {
                primitive_min_blocked_accumulator!(data_type, i32, Time32MillisecondType, batch_size)
            }
            Time64(Microsecond) => {
                primitive_min_blocked_accumulator!(data_type, i64, Time64MicrosecondType, batch_size)
            }
            Time64(Nanosecond) => {
                primitive_min_blocked_accumulator!(data_type, i64, Time64NanosecondType, batch_size)
            }
            Timestamp(Second, _) => {
                primitive_min_blocked_accumulator!(data_type, i64, TimestampSecondType, batch_size)
            }
            Timestamp(Millisecond, _) => {
                primitive_min_blocked_accumulator!(data_type, i64, TimestampMillisecondType, batch_size)
            }
            Timestamp(Microsecond, _) => {
                primitive_min_blocked_accumulator!(data_type, i64, TimestampMicrosecondType, batch_size)
            }
            Timestamp(Nanosecond, _) => {
                primitive_min_blocked_accumulator!(data_type, i64, TimestampNanosecondType, batch_size)
            }
            Duration(Second) => {
                primitive_min_blocked_accumulator!(data_type, i64, DurationSecondType, batch_size)
            }
            Duration(Millisecond) => {
                primitive_min_blocked_accumulator!(data_type, i64, DurationMillisecondType, batch_size)
            }
            Duration(Microsecond) => {
                primitive_min_blocked_accumulator!(data_type, i64, DurationMicrosecondType, batch_size)
            }
            Duration(Nanosecond) => {
                primitive_min_blocked_accumulator!(data_type, i64, DurationNanosecondType, batch_size)
            }
            Decimal32(_, _) => {
                primitive_min_blocked_accumulator!(data_type, i32, Decimal32Type, batch_size)
            }
            Decimal64(_, _) => {
                primitive_min_blocked_accumulator!(data_type, i64, Decimal64Type, batch_size)
            }
            Decimal128(_, _) => {
                primitive_min_blocked_accumulator!(data_type, i128, Decimal128Type, batch_size)
            }
            Decimal256(_, _) => {
                primitive_min_blocked_accumulator!(data_type, i256, Decimal256Type, batch_size)
            }
            // This is only reached if blocked_groups_accumulator_supported is out of sync
            _ => internal_err!("BlockedGroupsAccumulator not supported for min({})", data_type),
        }
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingMinAccumulator::try_new(
            args.return_field.data_type(),
        )?))
    }

    fn is_descending(&self) -> Option<bool> {
        Some(false)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.value_from_statistics(statistics_args)
    }
    fn order_sensitivity(&self) -> datafusion_expr::utils::AggregateOrderSensitivity {
        datafusion_expr::utils::AggregateOrderSensitivity::Insensitive
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        get_min_max_result_type(arg_types)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        datafusion_expr::ReversedUDAF::Identical
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn set_monotonicity(&self, _data_type: &DataType) -> SetMonotonicity {
        // `MIN` is monotonically decreasing as it always decreases or stays
        // the same as new values are seen.
        SetMonotonicity::Decreasing
    }
}

#[derive(Debug)]
pub struct SlidingMinAccumulator {
    /// Typed NULL returned when the window contains no non-null values
    empty_value: ScalarValue,
    moving_min: MovingMin<ScalarValue>,
}

impl SlidingMinAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            empty_value: ScalarValue::try_from(datatype)?,
            moving_min: MovingMin::<ScalarValue>::new(),
        })
    }

    fn current_min(&self) -> ScalarValue {
        match self.moving_min.min() {
            Some(res) => res.clone(),
            None => self.empty_value.clone(),
        }
    }
}

impl Accumulator for SlidingMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.current_min()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        for idx in 0..values[0].len() {
            let val = ScalarValue::try_from_array(&values[0], idx)?;
            if !val.is_null() {
                self.moving_min.push(val);
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // We assume that values are retracted in the order they were added, so
        // the retracted values must be the oldest elements of `moving_min`.
        // NULLs are never pushed, so be sure to only pop once per non-NULL
        // value.
        let valid_count = values[0].len() - values[0].logical_null_count();
        for _ in 0..valid_count {
            self.moving_min.pop();
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.current_min())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.empty_value)
            + self.empty_value.size()
            + self.moving_min.heap_size(|sv| sv.size() - size_of_val(sv))
    }
}

/// Keep track of the minimum value in a sliding window.
///
/// `MovingMin` keeps track of the minimum value in a sliding window using a
/// monotonic deque. Each element is stored with its sequence number, and the
/// deque maintains candidate elements in ascending value order.
///
/// Complexity:
/// - O(1) for getting the minimum
/// - amortized O(1) for push
/// - O(1) for pop
#[derive(Debug)]
pub(crate) struct MovingMin<T> {
    deque: VecDeque<(u64, T)>,
    push_seq: u64,
    pop_seq: u64,
}

impl<T: PartialOrd> Default for MovingMin<T> {
    fn default() -> Self {
        Self {
            deque: VecDeque::new(),
            push_seq: 0,
            pop_seq: 0,
        }
    }
}

impl<T: PartialOrd> MovingMin<T> {
    /// Creates a new `MovingMin` to keep track of the minimum in a sliding window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMin` to keep track of the minimum in a sliding window with
    /// `capacity` allocated slots.
    #[cfg(test)]
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            deque: VecDeque::with_capacity(capacity),
            push_seq: 0,
            pop_seq: 0,
        }
    }

    /// Returns the minimum of the sliding window or `None` if the window is
    /// empty.
    #[inline]
    pub fn min(&self) -> Option<&T> {
        self.deque.front().map(|(_, val)| val)
    }

    #[inline]
    fn check_invariants(&self) {
        debug_assert!(self.pop_seq <= self.push_seq);
        debug_assert!(
            self.deque
                .front()
                .is_none_or(|&(front_seq, _)| front_seq >= self.pop_seq)
        );
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        let seq = self.push_seq;
        self.push_seq += 1;
        while self.deque.back().is_some_and(|back_val| back_val.1 >= val) {
            self.deque.pop_back();
        }
        self.deque.push_back((seq, val));

        self.check_invariants();
    }

    /// Removes the oldest value from the sliding window.
    ///
    /// If the window is empty, this is a no-op.
    #[inline]
    pub fn pop(&mut self) {
        if self.is_empty() {
            return;
        }
        let seq = self.pop_seq;
        self.pop_seq += 1;
        if self
            .deque
            .front()
            .is_some_and(|front_val| front_val.0 == seq)
        {
            self.deque.pop_front();
        }

        self.check_invariants();
    }

    /// Returns the number of elements stored in the sliding window.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        (self.push_seq - self.pop_seq) as usize
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.push_seq == self.pop_seq
    }

    /// Heap bytes owned by the deque plus each stored `T`'s
    /// heap payload as reported by `elem_heap`. Excludes `size_of::<Self>()`.
    #[inline]
    fn heap_size(&self, elem_heap: impl Fn(&T) -> usize) -> usize {
        moving_deque_heap_size(&self.deque, elem_heap)
    }
}

/// Shared implementation for [`MovingMin::heap_size`] and
/// [`MovingMax::heap_size`]. Both share the same deque layout.
#[inline]
fn moving_deque_heap_size<T>(
    deque: &VecDeque<(u64, T)>,
    elem_heap: impl Fn(&T) -> usize,
) -> usize {
    let buffers = deque.capacity() * size_of::<(u64, T)>();
    let elems: usize = deque.iter().map(|(_, val)| elem_heap(val)).sum();
    buffers + elems
}

/// Keep track of the maximum value in a sliding window.
///
/// `MovingMax` keeps track of the maximum value in a sliding window using a
/// monotonic deque. Each element is stored with its sequence number, and the
/// deque maintains candidate elements in descending value order.
///
/// Complexity:
/// - O(1) for getting the maximum
/// - amortized O(1) for push
/// - O(1) for pop
#[derive(Debug)]
pub(crate) struct MovingMax<T> {
    deque: VecDeque<(u64, T)>,
    push_seq: u64,
    pop_seq: u64,
}

impl<T: PartialOrd> Default for MovingMax<T> {
    fn default() -> Self {
        Self {
            deque: VecDeque::new(),
            push_seq: 0,
            pop_seq: 0,
        }
    }
}

impl<T: PartialOrd> MovingMax<T> {
    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new `MovingMax` to keep track of the maximum in a sliding window with
    /// `capacity` allocated slots.
    #[cfg(test)]
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            deque: VecDeque::with_capacity(capacity),
            push_seq: 0,
            pop_seq: 0,
        }
    }

    /// Returns the maximum of the sliding window or `None` if the window is empty.
    #[inline]
    pub fn max(&self) -> Option<&T> {
        self.deque.front().map(|(_, val)| val)
    }

    #[inline]
    fn check_invariants(&self) {
        debug_assert!(self.pop_seq <= self.push_seq);
        debug_assert!(
            self.deque
                .front()
                .is_none_or(|&(front_seq, _)| front_seq >= self.pop_seq)
        );
    }

    /// Pushes a new element into the sliding window.
    #[inline]
    pub fn push(&mut self, val: T) {
        let seq = self.push_seq;
        self.push_seq += 1;
        while self.deque.back().is_some_and(|back_val| back_val.1 <= val) {
            self.deque.pop_back();
        }
        self.deque.push_back((seq, val));

        self.check_invariants();
    }

    /// Removes the oldest value from the sliding window.
    ///
    /// If the window is empty, this is a no-op.
    #[inline]
    pub fn pop(&mut self) {
        if self.is_empty() {
            return;
        }
        let seq = self.pop_seq;
        self.pop_seq += 1;
        if self
            .deque
            .front()
            .is_some_and(|front_val| front_val.0 == seq)
        {
            self.deque.pop_front();
        }

        self.check_invariants();
    }

    /// Returns the number of elements stored in the sliding window.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        (self.push_seq - self.pop_seq) as usize
    }

    /// Returns `true` if the moving window contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.push_seq == self.pop_seq
    }

    /// Heap bytes owned by the deque plus each stored `T`'s
    /// heap payload as reported by `elem_heap`. Excludes `size_of::<Self>()`.
    #[inline]
    fn heap_size(&self, elem_heap: impl Fn(&T) -> usize) -> usize {
        moving_deque_heap_size(&self.deque, elem_heap)
    }
}

make_udaf_expr_and_func!(
    Max,
    max,
    expression,
    "Returns the maximum of a group of values.",
    max_udaf
);

make_udaf_expr_and_func!(
    Min,
    min,
    expression,
    "Returns the minimum of a group of values.",
    min_udaf
);

// Re-export accumulators from the common module for backwards compatibility
pub use datafusion_functions_aggregate_common::min_max::{
    MaxAccumulator, MinAccumulator,
};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            Array, AsArray, DictionaryArray, Float32Array, Int8Array, Int32Array,
            IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray,
            PrimitiveArray, StringArray,
        },
        datatypes::{
            ArrowDictionaryKeyType, IntervalDayTimeType, IntervalMonthDayNanoType,
            IntervalUnit, IntervalYearMonthType,
        },
    };
    use datafusion_expr::EmitTo;
    use std::sync::Arc;

    #[test]
    fn grouped_float_min_max_total_order() -> Result<()> {
        fn grouped_float32_min() -> Result<Box<dyn GroupsAccumulator>> {
            let data_type = &DataType::Float32;
            primitive_min_accumulator!(data_type, f32, Float32Type, total, u32)
        }

        fn grouped_float32_max() -> Result<Box<dyn GroupsAccumulator>> {
            let data_type = &DataType::Float32;
            primitive_max_accumulator!(data_type, f32, Float32Type, total, u32)
        }

        fn evaluate_grouped_float32(
            mut accumulator: Box<dyn GroupsAccumulator>,
            values: Vec<f32>,
        ) -> f32 {
            let group_indices = vec![0; values.len()];
            let values = Arc::new(Float32Array::from(values)) as ArrayRef;
            accumulator
                .update_batch(&[values], &group_indices, None, 1)
                .unwrap();
            accumulator
                .evaluate(EmitTo::All)
                .unwrap()
                .as_primitive::<Float32Type>()
                .value(0)
        }

        let positive_nan = f32::NAN;
        let negative_nan = f32::from_bits(f32::NAN.to_bits() | (1 << 31));

        let min_cases = [
            (vec![positive_nan, 1.0], 1.0),
            (vec![1.0, negative_nan], negative_nan),
            (vec![0.0, -0.0], -0.0),
            (vec![positive_nan], positive_nan),
        ];
        for (values, expected) in min_cases {
            let actual = evaluate_grouped_float32(grouped_float32_min()?, values);
            assert_eq!(actual.to_bits(), expected.to_bits());
        }

        let max_cases = [
            (vec![positive_nan, 1.0], positive_nan),
            (vec![1.0, negative_nan], 1.0),
            (vec![-0.0, 0.0], 0.0),
            (vec![negative_nan], negative_nan),
        ];
        for (values, expected) in max_cases {
            let actual = evaluate_grouped_float32(grouped_float32_max()?, values);
            assert_eq!(actual.to_bits(), expected.to_bits());
        }

        Ok(())
    }

    #[test]
    fn interval_min_max() {
        // IntervalYearMonth
        let b = IntervalYearMonthArray::from(vec![
            IntervalYearMonthType::make_value(0, 1),
            IntervalYearMonthType::make_value(5, 34),
            IntervalYearMonthType::make_value(-2, 4),
            IntervalYearMonthType::make_value(7, -4),
            IntervalYearMonthType::make_value(0, 1),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                -2, 4,
            )))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::YearMonth))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalYearMonth(Some(IntervalYearMonthType::make_value(
                5, 34,
            )))
        );

        // IntervalDayTime
        let b = IntervalDayTimeArray::from(vec![
            IntervalDayTimeType::make_value(0, 0),
            IntervalDayTimeType::make_value(5, 454000),
            IntervalDayTimeType::make_value(-34, 0),
            IntervalDayTimeType::make_value(7, -4000),
            IntervalDayTimeType::make_value(1, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(-34, 0)))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::DayTime)).unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalDayTime(Some(IntervalDayTimeType::make_value(7, -4000)))
        );

        // IntervalMonthDayNano
        let b = IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNanoType::make_value(1, 0, 0),
            IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000),
            IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000),
            IntervalMonthDayNanoType::make_value(5, 2, 493_000_000_000),
            IntervalMonthDayNanoType::make_value(1, 0, 0),
        ]);
        let b: ArrayRef = Arc::new(b);

        let mut min =
            MinAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        min.update_batch(&[Arc::clone(&b)]).unwrap();
        let min_res = min.evaluate().unwrap();
        assert_eq!(
            min_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(-593, -33, 13_000_000_000)
            ))
        );

        let mut max =
            MaxAccumulator::try_new(&DataType::Interval(IntervalUnit::MonthDayNano))
                .unwrap();
        max.update_batch(&[Arc::clone(&b)]).unwrap();
        let max_res = max.evaluate().unwrap();
        assert_eq!(
            max_res,
            ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNanoType::make_value(344, 34, -43_000_000_000)
            ))
        );
    }

    #[test]
    fn float_min_max_with_nans() {
        let pos_nan = f32::NAN;
        let zero = 0_f32;
        let neg_inf = f32::NEG_INFINITY;

        let check = |acc: &mut dyn Accumulator, values: &[&[f32]], expected: f32| {
            for batch in values.iter() {
                let batch =
                    Arc::new(Float32Array::from_iter_values(batch.iter().copied()));
                acc.update_batch(&[batch]).unwrap();
            }
            let result = acc.evaluate().unwrap();
            assert_eq!(result, ScalarValue::Float32(Some(expected)));
        };

        // This test checks both comparison between batches (which uses the min_max macro
        // defined above) and within a batch (which uses the arrow min/max compute function
        // and verifies both respect the total order comparison for floats)

        let min = || MinAccumulator::try_new(&DataType::Float32).unwrap();
        let max = || MaxAccumulator::try_new(&DataType::Float32).unwrap();

        check(&mut min(), &[&[zero], &[pos_nan]], zero);
        check(&mut min(), &[&[zero, pos_nan]], zero);
        check(&mut min(), &[&[zero], &[neg_inf]], neg_inf);
        check(&mut min(), &[&[zero, neg_inf]], neg_inf);
        check(&mut max(), &[&[zero], &[pos_nan]], pos_nan);
        check(&mut max(), &[&[zero, pos_nan]], pos_nan);
        check(&mut max(), &[&[zero], &[neg_inf]], zero);
        check(&mut max(), &[&[zero, neg_inf]], zero);
    }

    use rand::Rng;

    fn get_random_vec_i32(len: usize) -> Vec<i32> {
        let mut rng = rand::rng();
        let mut input = Vec::with_capacity(len);
        for _i in 0..len {
            input.push(rng.random_range(0..100));
        }
        input
    }

    fn moving_min_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_min = MovingMin::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..=i].iter().min().unwrap());

            moving_min.push(data[i]);
            if i > n_sliding_window {
                moving_min.pop();
            }
            res.push(*moving_min.min().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    fn moving_max_i32(len: usize, n_sliding_window: usize) -> Result<()> {
        let data = get_random_vec_i32(len);
        let mut expected = Vec::with_capacity(len);
        let mut moving_max = MovingMax::<i32>::new();
        let mut res = Vec::with_capacity(len);
        for i in 0..len {
            let start = i.saturating_sub(n_sliding_window);
            expected.push(*data[start..=i].iter().max().unwrap());

            moving_max.push(data[i]);
            if i > n_sliding_window {
                moving_max.pop();
            }
            res.push(*moving_max.max().unwrap());
        }
        assert_eq!(res, expected);
        Ok(())
    }

    #[test]
    fn sliding_min_all_null_window() -> Result<()> {
        let mut min_acc = SlidingMinAccumulator::try_new(&DataType::Int32)?;

        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None]));
        min_acc.update_batch(&[Arc::clone(&values)])?;
        assert_eq!(min_acc.evaluate()?, ScalarValue::Int32(Some(3)));

        // Retract `3`; the window now contains only the NULL
        let retracted: ArrayRef = Arc::new(Int32Array::from(vec![Some(3)]));
        min_acc.retract_batch(&[Arc::clone(&retracted)])?;
        assert_eq!(min_acc.evaluate()?, ScalarValue::Int32(None));

        // A subsequent non-null value must be picked up again
        let update: ArrayRef = Arc::new(Int32Array::from(vec![Some(7)]));
        min_acc.update_batch(&[Arc::clone(&update)])?;
        assert_eq!(min_acc.evaluate()?, ScalarValue::Int32(Some(7)));

        // Retracting the NULL row must not pop the remaining value
        let null_row: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>]));
        min_acc.retract_batch(&[Arc::clone(&null_row)])?;
        assert_eq!(min_acc.evaluate()?, ScalarValue::Int32(Some(7)));

        Ok(())
    }

    #[test]
    fn sliding_max_all_null_window() -> Result<()> {
        let mut max_acc = SlidingMaxAccumulator::try_new(&DataType::Int32)?;

        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None]));
        max_acc.update_batch(&[Arc::clone(&values)])?;
        assert_eq!(max_acc.evaluate()?, ScalarValue::Int32(Some(3)));

        // Retract `3`; the window now contains only the NULL
        let retracted: ArrayRef = Arc::new(Int32Array::from(vec![Some(3)]));
        max_acc.retract_batch(&[Arc::clone(&retracted)])?;
        assert_eq!(max_acc.evaluate()?, ScalarValue::Int32(None));

        // A subsequent non-null value must be picked up again
        let update: ArrayRef = Arc::new(Int32Array::from(vec![Some(7)]));
        max_acc.update_batch(&[Arc::clone(&update)])?;
        assert_eq!(max_acc.evaluate()?, ScalarValue::Int32(Some(7)));

        // Retracting the NULL row must not disturb the remaining value
        let null_row: ArrayRef = Arc::new(Int32Array::from(vec![None::<i32>]));
        max_acc.retract_batch(&[Arc::clone(&null_row)])?;
        assert_eq!(max_acc.evaluate()?, ScalarValue::Int32(Some(7)));

        Ok(())
    }

    #[test]
    fn moving_min_tests() -> Result<()> {
        moving_min_i32(100, 10)?;
        moving_min_i32(100, 20)?;
        moving_min_i32(100, 50)?;
        moving_min_i32(100, 100)?;
        Ok(())
    }

    #[test]
    fn moving_max_tests() -> Result<()> {
        moving_max_i32(100, 10)?;
        moving_max_i32(100, 20)?;
        moving_max_i32(100, 50)?;
        moving_max_i32(100, 100)?;
        Ok(())
    }

    #[test]
    fn moving_min_max_heap_size_i32() {
        // Fixed-width `T` has no per-element heap payload, so `heap_size`
        // reports exactly the buffer's capacity in bytes.
        let mut moving_min = MovingMin::<i32>::with_capacity(4);
        let mut moving_max = MovingMax::<i32>::with_capacity(4);
        let elem = |_: &i32| 0;

        let buffer_only = moving_min.deque.capacity() * size_of::<(u64, i32)>();
        assert_eq!(moving_min.heap_size(elem), buffer_only);
        assert_eq!(moving_max.heap_size(elem), buffer_only);

        for i in 0..3 {
            moving_min.push(i);
            moving_max.push(i);
        }
        // Elements sit inside the pre-allocated buffers, so still buffer-only.
        assert_eq!(moving_min.heap_size(elem), buffer_only);
        assert_eq!(moving_max.heap_size(elem), buffer_only);
    }

    #[test]
    fn moving_min_max_heap_size_counts_elems() {
        let mut moving_min = MovingMin::<String>::with_capacity(2);
        let mut moving_max = MovingMax::<String>::with_capacity(2);
        let elem = |s: &String| s.capacity();

        moving_min.push("abcdef".to_string());
        moving_max.push("abcdef".to_string());

        let buffers = moving_min.deque.capacity() * size_of::<(u64, String)>();
        let elems = 6;
        assert_eq!(moving_min.heap_size(elem), buffers + elems);
        assert_eq!(moving_max.heap_size(elem), buffers + elems);
    }

    #[test]
    fn test_moving_min_max_empty_pop() {
        let mut moving_min = MovingMin::<i32>::new();
        moving_min.pop(); // empty pop is a no-op
        assert_eq!(moving_min.len(), 0);
        assert!(moving_min.is_empty());
        // Verify it still works correctly after empty pop
        moving_min.push(10);
        moving_min.push(20);
        assert_eq!(moving_min.min(), Some(&10));
        moving_min.pop();
        assert_eq!(moving_min.min(), Some(&20));

        let mut moving_max = MovingMax::<i32>::new();
        moving_max.pop(); // empty pop is a no-op
        assert_eq!(moving_max.len(), 0);
        assert!(moving_max.is_empty());
        // Verify it still works correctly after empty pop
        moving_max.push(20);
        moving_max.push(10);
        assert_eq!(moving_max.max(), Some(&20));
        moving_max.pop();
        assert_eq!(moving_max.max(), Some(&10));
    }

    #[test]
    fn test_moving_min_max_duplicate_heavy() {
        let mut moving_min = MovingMin::<i32>::new();
        let mut moving_max = MovingMax::<i32>::new();

        // Push duplicates
        for _ in 0..5 {
            moving_min.push(5);
            moving_max.push(5);
        }

        assert_eq!(moving_min.len(), 5);
        assert_eq!(moving_max.len(), 5);

        // Ensure min/max query works and we can pop all duplicates correctly
        for i in (1..=5).rev() {
            assert_eq!(moving_min.len(), i);
            assert_eq!(moving_max.len(), i);
            assert_eq!(moving_min.min(), Some(&5));
            assert_eq!(moving_max.max(), Some(&5));
            moving_min.pop();
            moving_max.pop();
        }

        assert!(moving_min.is_empty());
        assert!(moving_max.is_empty());
    }

    #[test]
    fn test_min_max_coerce_types() {
        // the coerced types is same with input types
        let funs: Vec<Box<dyn AggregateUDFImpl>> =
            vec![Box::new(Min::new()), Box::new(Max::new())];
        let input_types = vec![
            vec![DataType::Int32],
            vec![DataType::Decimal128(10, 2)],
            vec![DataType::Decimal256(1, 1)],
            vec![DataType::Utf8],
        ];
        for fun in funs {
            for input_type in &input_types {
                let result = fun.coerce_types(input_type);
                assert_eq!(*input_type, result.unwrap());
            }
        }
    }

    #[test]
    fn test_get_min_max_return_type_coerce_dictionary() -> Result<()> {
        let data_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let result = get_min_max_result_type(&[data_type])?;
        assert_eq!(result, vec![DataType::Utf8]);
        Ok(())
    }

    #[test]
    fn test_min_max_dictionary() -> Result<()> {
        let values = StringArray::from(vec!["b", "c", "a", "🦀", "d"]);
        let keys = Int32Array::from(vec![Some(0), Some(1), Some(2), None, Some(4)]);
        let dict_array =
            DictionaryArray::try_new(keys, Arc::new(values) as ArrayRef).unwrap();
        let dict_array_ref = Arc::new(dict_array) as ArrayRef;
        let rt_type =
            get_min_max_result_type(&[dict_array_ref.data_type().clone()])?[0].clone();

        let mut min_acc = MinAccumulator::try_new(&rt_type)?;
        min_acc.update_batch(&[Arc::clone(&dict_array_ref)])?;
        let min_result = min_acc.evaluate()?;
        assert_eq!(min_result, ScalarValue::Utf8(Some("a".to_string())));

        let mut max_acc = MaxAccumulator::try_new(&rt_type)?;
        max_acc.update_batch(&[Arc::clone(&dict_array_ref)])?;
        let max_result = max_acc.evaluate()?;
        assert_eq!(max_result, ScalarValue::Utf8(Some("d".to_string())));
        Ok(())
    }

    fn dict_scalar(key_type: DataType, inner: ScalarValue) -> ScalarValue {
        ScalarValue::Dictionary(Box::new(key_type), Box::new(inner))
    }

    fn utf8_dict_scalar(key_type: DataType, value: &str) -> ScalarValue {
        dict_scalar(key_type, ScalarValue::Utf8(Some(value.to_string())))
    }

    fn string_dictionary_batch(values: &[&str], keys: &[Option<i32>]) -> ArrayRef {
        string_dictionary_batch_with_keys(Int32Array::from(keys.to_vec()), values)
    }

    fn string_dictionary_batch_with_keys<K>(
        keys: PrimitiveArray<K>,
        values: &[&str],
    ) -> ArrayRef
    where
        K: ArrowDictionaryKeyType,
    {
        let values = Arc::new(StringArray::from(values.to_vec())) as ArrayRef;
        Arc::new(DictionaryArray::try_new(keys, values).unwrap()) as ArrayRef
    }

    fn optional_string_dictionary_batch(
        values: &[Option<&str>],
        keys: &[Option<i32>],
    ) -> ArrayRef {
        let values = Arc::new(StringArray::from(values.to_vec())) as ArrayRef;
        Arc::new(
            DictionaryArray::try_new(Int32Array::from(keys.to_vec()), values).unwrap(),
        ) as ArrayRef
    }

    fn float_dictionary_batch(values: &[f32], keys: &[Option<i32>]) -> ArrayRef {
        let values = Arc::new(Float32Array::from(values.to_vec())) as ArrayRef;
        Arc::new(
            DictionaryArray::try_new(Int32Array::from(keys.to_vec()), values).unwrap(),
        ) as ArrayRef
    }

    fn evaluate_dictionary_accumulator(
        mut acc: impl Accumulator,
        batches: &[ArrayRef],
    ) -> Result<ScalarValue> {
        for batch in batches {
            acc.update_batch(&[Arc::clone(batch)])?;
        }
        acc.evaluate()
    }

    fn assert_dictionary_min_max(
        dict_type: &DataType,
        batches: &[ArrayRef],
        expected_min: &str,
        expected_max: &str,
    ) -> Result<()> {
        let key_type = match dict_type {
            DataType::Dictionary(key_type, _) => key_type.as_ref().clone(),
            other => panic!("expected dictionary type, got {other:?}"),
        };

        let min_result = evaluate_dictionary_accumulator(
            MinAccumulator::try_new(dict_type)?,
            batches,
        )?;
        assert_eq!(min_result, utf8_dict_scalar(key_type.clone(), expected_min));

        let max_result = evaluate_dictionary_accumulator(
            MaxAccumulator::try_new(dict_type)?,
            batches,
        )?;
        assert_eq!(max_result, utf8_dict_scalar(key_type, expected_max));

        Ok(())
    }

    #[test]
    fn test_min_max_dictionary_without_coercion() -> Result<()> {
        let dict_array_ref = string_dictionary_batch(
            &["b", "c", "a", "d"],
            &[Some(0), Some(1), Some(2), Some(3)],
        );
        let dict_type = dict_array_ref.data_type().clone();

        assert_dictionary_min_max(&dict_type, &[dict_array_ref], "a", "d")
    }

    #[test]
    fn test_min_max_dictionary_with_nulls() -> Result<()> {
        let dict_array_ref = string_dictionary_batch(
            &["b", "c", "a"],
            &[None, Some(0), None, Some(1), Some(2)],
        );
        let dict_type = dict_array_ref.data_type().clone();

        assert_dictionary_min_max(&dict_type, &[dict_array_ref], "a", "c")
    }

    #[test]
    fn test_min_max_dictionary_ignores_unreferenced_values() -> Result<()> {
        let dict_array_ref =
            string_dictionary_batch(&["a", "z", "zz_unused"], &[Some(1), Some(1), None]);
        let dict_type = dict_array_ref.data_type().clone();

        assert_dictionary_min_max(&dict_type, &[dict_array_ref], "z", "z")
    }

    #[test]
    fn test_min_max_dictionary_ignores_referenced_null_values() -> Result<()> {
        let dict_array_ref = optional_string_dictionary_batch(
            &[Some("b"), None, Some("a"), Some("d")],
            &[Some(0), Some(1), Some(2), Some(3)],
        );
        let dict_type = dict_array_ref.data_type().clone();

        assert_dictionary_min_max(&dict_type, &[dict_array_ref], "a", "d")
    }

    #[test]
    fn test_min_max_dictionary_multi_batch() -> Result<()> {
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let batch1 = string_dictionary_batch(&["b", "c"], &[Some(0), Some(1)]);
        let batch2 = string_dictionary_batch(&["a", "d"], &[Some(0), Some(1)]);

        assert_dictionary_min_max(&dict_type, &[batch1, batch2], "a", "d")
    }

    #[test]
    fn test_min_max_dictionary_int8_keys() -> Result<()> {
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
        let dict_array_ref = string_dictionary_batch_with_keys(
            Int8Array::from(vec![Some(0), Some(1), Some(2), Some(3)]),
            &["b", "c", "a", "d"],
        );

        assert_dictionary_min_max(&dict_type, &[dict_array_ref], "a", "d")
    }

    #[test]
    fn test_min_max_dictionary_float_with_nans() -> Result<()> {
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float32));
        let batch1 = float_dictionary_batch(&[0.0, f32::NAN], &[Some(0), Some(1)]);
        let batch2 = float_dictionary_batch(&[f32::NEG_INFINITY], &[Some(0)]);

        let min_result = evaluate_dictionary_accumulator(
            MinAccumulator::try_new(&dict_type)?,
            &[Arc::clone(&batch1), Arc::clone(&batch2)],
        )?;
        assert_eq!(
            min_result,
            dict_scalar(
                DataType::Int32,
                ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            )
        );

        let max_result = evaluate_dictionary_accumulator(
            MaxAccumulator::try_new(&dict_type)?,
            &[batch1, batch2],
        )?;
        assert_eq!(
            max_result,
            dict_scalar(DataType::Int32, ScalarValue::Float32(Some(f32::NAN)))
        );

        Ok(())
    }
}
