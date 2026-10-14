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

use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{Array, AsArray, BooleanArray};
use arrow::compute::cast;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type,
    Decimal128Type, Decimal256Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int8Type, Int16Type, Int32Type,
    Int64Type, Schema, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type,
    UInt64Type,
};
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, ScalarValue, assert_eq_or_internal_err};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion_physical_plan::ColumnarValue;

use crate::in_list::{SetMembership, not_in_may_match};

/// A typed, non-null primitive IN-list domain under construction.
pub(crate) struct PrimitiveInListDomain {
    data_type: DataType,
    values: PrimitiveValues,
}

macro_rules! define_primitive_values {
    ($(
        $variant:ident, $arrow_type:ty, $data_pattern:pat,
        $scalar_pattern:pat, $native:expr $(, if $guard:expr)?;
    )+) => {
        enum PrimitiveValues {
            $(
                $variant(Vec<<$arrow_type as ArrowPrimitiveType>::Native>),
            )+
        }

        impl PrimitiveInListDomain {
            pub(crate) fn new(
                data_type: &DataType,
                capacity: usize,
            ) -> Option<Self> {
                // Parameter bindings in these patterns are used by `push` guards,
                // but construction only selects the matching storage type.
                #[expect(
                    unused_variables,
                    reason = "parameter bindings are reused by the generated push guards"
                )]
                let values = match data_type {
                    $(
                        $data_pattern => PrimitiveValues::$variant(
                            Vec::with_capacity(capacity),
                        ),
                    )+
                    _ => return None,
                };
                Some(Self {
                    data_type: data_type.clone(),
                    values,
                })
            }

            /// Adds one non-null scalar whose logical type matches the domain.
            pub(crate) fn push(&mut self, value: &ScalarValue) -> Option<()> {
                match (&self.data_type, &mut self.values, value) {
                    $(
                        ($data_pattern, PrimitiveValues::$variant(values), $scalar_pattern)
                            $(if $guard)? =>
                        {
                            values.push($native);
                            Some(())
                        }
                    )+
                    _ => None,
                }
            }

            pub(crate) fn is_empty(&self) -> bool {
                match &self.values {
                    $(PrimitiveValues::$variant(values) => values.is_empty(),)+
                }
            }

            pub(crate) fn into_expr(
                self,
                membership: SetMembership,
                min: PhysicalExprRef,
                max: PhysicalExprRef,
            ) -> PhysicalExprRef {
                let data_type = self.data_type;
                match self.values {
                    $(
                        PrimitiveValues::$variant(values) => Arc::new(
                            PrimitiveInListPruningExpr::<$arrow_type>::new(
                                membership,
                                data_type,
                                min,
                                max,
                                values,
                            ),
                        ),
                    )+
                }
            }
        }
    };
}

// This single list defines both supported types and expression dispatch.
define_primitive_values! {
    Int8, Int8Type, DataType::Int8,
        ScalarValue::Int8(Some(value)), *value;
    Int16, Int16Type, DataType::Int16,
        ScalarValue::Int16(Some(value)), *value;
    Int32, Int32Type, DataType::Int32,
        ScalarValue::Int32(Some(value)), *value;
    Int64, Int64Type, DataType::Int64,
        ScalarValue::Int64(Some(value)), *value;
    UInt8, UInt8Type, DataType::UInt8,
        ScalarValue::UInt8(Some(value)), *value;
    UInt16, UInt16Type, DataType::UInt16,
        ScalarValue::UInt16(Some(value)), *value;
    UInt32, UInt32Type, DataType::UInt32,
        ScalarValue::UInt32(Some(value)), *value;
    UInt64, UInt64Type, DataType::UInt64,
        ScalarValue::UInt64(Some(value)), *value;
    Decimal32, Decimal32Type, DataType::Decimal32(_precision, scale),
        ScalarValue::Decimal32(Some(value), _value_precision, value_scale), *value,
        if scale == value_scale;
    Decimal64, Decimal64Type, DataType::Decimal64(_precision, scale),
        ScalarValue::Decimal64(Some(value), _value_precision, value_scale), *value,
        if scale == value_scale;
    Decimal128, Decimal128Type, DataType::Decimal128(_precision, scale),
        ScalarValue::Decimal128(Some(value), _value_precision, value_scale), *value,
        if scale == value_scale;
    Decimal256, Decimal256Type, DataType::Decimal256(_precision, scale),
        ScalarValue::Decimal256(Some(value), _value_precision, value_scale), *value,
        if scale == value_scale;
    Date32, Date32Type, DataType::Date32,
        ScalarValue::Date32(Some(value)), *value;
    Date64, Date64Type, DataType::Date64,
        ScalarValue::Date64(Some(value)), *value;
    Time32Second, Time32SecondType, DataType::Time32(TimeUnit::Second),
        ScalarValue::Time32Second(Some(value)), *value;
    Time32Millisecond, Time32MillisecondType,
        DataType::Time32(TimeUnit::Millisecond),
        ScalarValue::Time32Millisecond(Some(value)), *value;
    Time64Microsecond, Time64MicrosecondType,
        DataType::Time64(TimeUnit::Microsecond),
        ScalarValue::Time64Microsecond(Some(value)), *value;
    Time64Nanosecond, Time64NanosecondType,
        DataType::Time64(TimeUnit::Nanosecond),
        ScalarValue::Time64Nanosecond(Some(value)), *value;
    TimestampSecond, TimestampSecondType,
        DataType::Timestamp(TimeUnit::Second, _timezone),
        ScalarValue::TimestampSecond(Some(value), _value_timezone), *value;
    TimestampMillisecond, TimestampMillisecondType,
        DataType::Timestamp(TimeUnit::Millisecond, _timezone),
        ScalarValue::TimestampMillisecond(Some(value), _value_timezone), *value;
    TimestampMicrosecond, TimestampMicrosecondType,
        DataType::Timestamp(TimeUnit::Microsecond, _timezone),
        ScalarValue::TimestampMicrosecond(Some(value), _value_timezone), *value;
    TimestampNanosecond, TimestampNanosecondType,
        DataType::Timestamp(TimeUnit::Nanosecond, _timezone),
        ScalarValue::TimestampNanosecond(Some(value), _value_timezone), *value;
    DurationSecond, DurationSecondType, DataType::Duration(TimeUnit::Second),
        ScalarValue::DurationSecond(Some(value)), *value;
    DurationMillisecond, DurationMillisecondType,
        DataType::Duration(TimeUnit::Millisecond),
        ScalarValue::DurationMillisecond(Some(value)), *value;
    DurationMicrosecond, DurationMicrosecondType,
        DataType::Duration(TimeUnit::Microsecond),
        ScalarValue::DurationMicrosecond(Some(value)), *value;
    DurationNanosecond, DurationNanosecondType,
        DataType::Duration(TimeUnit::Nanosecond),
        ScalarValue::DurationNanosecond(Some(value)), *value;
}

/// Tests an inclusive statistics interval against a sorted primitive domain.
struct PrimitiveInListPruningExpr<T: ArrowPrimitiveType>
where
    T::Native: Eq + Hash + Ord,
{
    membership: SetMembership,
    // Retain decimal metadata and timestamp timezone for statistics casts.
    data_type: DataType,
    min: PhysicalExprRef,
    max: PhysicalExprRef,
    values: Arc<[T::Native]>,
}

impl<T: ArrowPrimitiveType> fmt::Debug for PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrimitiveInListPruningExpr")
            .field("membership", &self.membership)
            .field("data_type", &self.data_type)
            .field("min", &self.min)
            .field("max", &self.max)
            .field("values", &self.values)
            .finish()
    }
}

impl<T: ArrowPrimitiveType> PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn new(
        membership: SetMembership,
        data_type: DataType,
        min: PhysicalExprRef,
        max: PhysicalExprRef,
        mut values: Vec<T::Native>,
    ) -> Self {
        values.sort_unstable();
        values.dedup();
        Self {
            membership,
            data_type,
            min,
            max,
            values: values.into(),
        }
    }

    fn contains(&self, value: T::Native) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    fn may_match(&self, min: Option<T::Native>, max: Option<T::Native>) -> Option<bool> {
        if self.membership == SetMembership::NotIn {
            return not_in_may_match(min.as_ref(), max.as_ref(), |value| {
                self.contains(*value)
            });
        }
        match (min, max) {
            (Some(min), Some(max)) => {
                // Inverted statistics are unusable, not proof that the domain
                // and interval are disjoint.
                if min > max {
                    return None;
                }
                let index = self.values.partition_point(|value| *value < min);
                Some(self.values.get(index).is_some_and(|value| *value <= max))
            }
            (Some(min), None) if self.values.last().is_some_and(|value| *value < min) => {
                Some(false)
            }
            (None, Some(max))
                if self.values.first().is_some_and(|value| *value > max) =>
            {
                Some(false)
            }
            _ => None,
        }
    }
}

impl<T: ArrowPrimitiveType> PartialEq for PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.membership == other.membership
            && self.data_type == other.data_type
            && self.min.eq(&other.min)
            && self.max.eq(&other.max)
            && self.values == other.values
    }
}

impl<T: ArrowPrimitiveType> Eq for PrimitiveInListPruningExpr<T> where
    T::Native: Eq + Hash + Ord
{
}

impl<T: ArrowPrimitiveType> Hash for PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.membership.hash(state);
        self.data_type.hash(state);
        self.min.hash(state);
        self.max.hash(state);
        self.values.hash(state);
    }
}

impl<T: ArrowPrimitiveType> Display for PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}, {}, {} values)",
            self.membership.display_name(),
            self.min,
            self.max,
            self.values.len()
        )
    }
}

impl<T: ArrowPrimitiveType> PhysicalExpr for PrimitiveInListPruningExpr<T>
where
    T::Native: Eq + Hash + Ord,
{
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let min = self.min.evaluate(batch)?.into_array(batch.num_rows())?;
        let max = self.max.evaluate(batch)?.into_array(batch.num_rows())?;
        // Unlike view casts, dictionary-to-primitive casts propagate both key
        // and value NULLs, so no logical-null intersection is necessary here.
        let min = cast(&min, &self.data_type)?;
        let max = cast(&max, &self.data_type)?;
        let min = min.as_primitive::<T>();
        let max = max.as_primitive::<T>();
        let matches: BooleanArray = (0..batch.num_rows())
            .map(|index| {
                let min = min.is_valid(index).then(|| min.value(index));
                let max = max.is_valid(index).then(|| max.value(index));
                self.may_match(min, max)
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(matches)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.min, &self.max]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        assert_eq_or_internal_err!(children.len(), 2);
        Ok(Arc::new(Self {
            membership: self.membership,
            data_type: self.data_type.clone(),
            min: Arc::clone(&children[0]),
            max: Arc::clone(&children[1]),
            values: Arc::clone(&self.values),
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_list::unwrap_scalar;
    use arrow::datatypes::Field;

    #[test]
    fn domain_accepts_compatible_non_null_values() {
        let mut domain = PrimitiveInListDomain::new(&DataType::Int64, 4).unwrap();
        assert!(domain.push(&ScalarValue::Int64(None)).is_none());
        assert!(domain.push(&ScalarValue::Int32(Some(1))).is_none());
        assert!(domain.is_empty());

        let run_end_encoded = ScalarValue::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Int64, true)),
            Box::new(ScalarValue::Int64(Some(1))),
        );
        assert!(domain.push(unwrap_scalar(&run_end_encoded)).is_some());
        assert!(!domain.is_empty());

        let mut decimal =
            PrimitiveInListDomain::new(&DataType::Decimal128(10, 2), 2).unwrap();
        assert!(
            decimal
                .push(&ScalarValue::Decimal128(Some(1), 12, 2))
                .is_some()
        );
        assert!(
            decimal
                .push(&ScalarValue::Decimal128(Some(1), 10, 3))
                .is_none()
        );

        let mut timestamp = PrimitiveInListDomain::new(
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            1,
        )
        .unwrap();
        assert!(
            timestamp
                .push(&ScalarValue::TimestampMicrosecond(
                    Some(1),
                    Some("Asia/Kolkata".into()),
                ))
                .is_some()
        );
    }
}
