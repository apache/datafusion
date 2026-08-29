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

//! [`GroupValues`] trait for storing and interning group keys

use arrow::array::types::{
    Date32Type, Date64Type, Decimal128Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow::array::{ArrayRef, downcast_primitive};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion_common::{Result, not_impl_err};

use datafusion_expr::{EmitTo, GroupSelection};

pub mod multi_group_by;

mod row;
pub use row::GroupValuesRows;
mod single_group_by;
use datafusion_physical_expr::binary_map::OutputType;
use multi_group_by::GroupValuesColumn;

pub(crate) use single_group_by::primitive::HashValue;

use crate::aggregates::{
    group_values::single_group_by::{
        boolean::GroupValuesBoolean, bytes::GroupValuesBytes,
        bytes_view::GroupValuesBytesView, primitive::GroupValuesPrimitive,
    },
    order::GroupOrdering,
};

mod metrics;
mod null_builder;

pub(crate) use metrics::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics,
};

/// Stores the group values during hash aggregation.
///
/// # Background
///
/// In a query such as `SELECT a, b, count(*) FROM t GROUP BY a, b`, the group values
/// identify each group, and correspond to all the distinct values of `(a,b)`.
///
/// ```sql
/// -- Input has 4 rows with 3 distinct combinations of (a,b) ("groups")
/// create table t(a int, b varchar)
/// as values (1, 'a'), (2, 'b'), (1, 'a'), (3, 'c');
///
/// select a, b, count(*) from t group by a, b;
/// ----
/// 1 a 2
/// 2 b 1
/// 3 c 1
/// ```
///
/// # Design
///
/// Managing group values is a performance critical operation in hash
/// aggregation. The major operations are:
///
/// 1. Intern: Quickly finding existing and adding new group values
/// 2. Emit: Returning the group values as an array
///
/// There are multiple specialized implementations of this trait optimized for
/// different data types and number of columns, optimized for these operations.
/// See [`new_group_values`] for details.
///
/// # Group Ids
///
/// Each distinct group in a hash aggregation is identified by a unique group id
/// (usize) which is assigned by instances of this trait. Group ids are
/// continuous without gaps, starting from 0.
pub trait GroupValues: Send {
    /// Calculates the group id for each input row of `cols`, assigning new
    /// group ids as necessary.
    ///
    /// When the function returns, `groups`  must contain the group id for each
    /// row in `cols`.
    ///
    /// If a row has the same value as a previous row, the same group id is
    /// assigned. If a row has a new value, the next available group id is
    /// assigned.
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()>;

    /// Returns the number of bytes of memory used by this [`GroupValues`].
    ///
    /// May be expensive; check the implementation before calling on hot paths.
    fn size(&self) -> usize;

    /// Returns true if this [`GroupValues`] is empty
    fn is_empty(&self) -> bool;

    /// The number of values (distinct group values) stored in this [`GroupValues`]
    fn len(&self) -> usize;

    /// Emits the group values
    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>>;

    /// Materializes selected group values without changing the stored values or
    /// their group indices.
    ///
    /// Rows are returned in the order specified by `selection`. An empty
    /// selection returns one correctly typed empty array per group-value column.
    ///
    /// This method requires exclusive access because implementations may mutate
    /// internal caches or builders, even though stored values are unchanged.
    fn values_preserving(
        &mut self,
        _selection: GroupSelection<'_>,
    ) -> Result<Vec<ArrayRef>> {
        not_impl_err!("Preserving group values are not implemented")
    }

    /// Returns `true` if [`Self::values_preserving`] is implemented.
    fn supports_values_preserving(&self) -> bool {
        false
    }

    /// Clear the contents and shrink the capacity to the size of the batch (free up memory usage)
    fn clear_shrink(&mut self, num_rows: usize);
}

/// Return a specialized implementation of [`GroupValues`] for the given schema.
///
/// [`GroupValues`] implementations choosing logic:
///
///   - If group by single column, and type of this column has
///     the specific [`GroupValues`] implementation, such implementation
///     will be chosen.
///
///   - If group by multiple columns, and all column types have the specific
///     `GroupColumn` implementations, `GroupValuesColumn` will be chosen.
///
///   - Otherwise, the general implementation `GroupValuesRows` will be chosen.
///
/// `GroupColumn`:  crate::aggregates::group_values::multi_group_by::GroupColumn
/// `GroupValuesColumn`: crate::aggregates::group_values::multi_group_by::GroupValuesColumn
/// `GroupValuesRows`: crate::aggregates::group_values::GroupValuesRows
pub fn new_group_values(
    schema: SchemaRef,
    group_ordering: &GroupOrdering,
) -> Result<Box<dyn GroupValues>> {
    if schema.fields.len() == 1 {
        let d = schema.fields[0].data_type();

        macro_rules! downcast_helper {
            ($t:ty, $d:ident) => {
                return Ok(Box::new(GroupValuesPrimitive::<$t>::new($d.clone())))
            };
        }

        downcast_primitive! {
            d => (downcast_helper, d),
            _ => {}
        }

        match d {
            DataType::Date32 => {
                downcast_helper!(Date32Type, d);
            }
            DataType::Date64 => {
                downcast_helper!(Date64Type, d);
            }
            DataType::Time32(t) => match t {
                TimeUnit::Second => downcast_helper!(Time32SecondType, d),
                TimeUnit::Millisecond => downcast_helper!(Time32MillisecondType, d),
                _ => {}
            },
            DataType::Time64(t) => match t {
                TimeUnit::Microsecond => downcast_helper!(Time64MicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(Time64NanosecondType, d),
                _ => {}
            },
            DataType::Timestamp(t, _tz) => match t {
                TimeUnit::Second => downcast_helper!(TimestampSecondType, d),
                TimeUnit::Millisecond => downcast_helper!(TimestampMillisecondType, d),
                TimeUnit::Microsecond => downcast_helper!(TimestampMicrosecondType, d),
                TimeUnit::Nanosecond => downcast_helper!(TimestampNanosecondType, d),
            },
            DataType::Decimal128(_, _) => {
                downcast_helper!(Decimal128Type, d);
            }
            DataType::Utf8 => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Utf8)));
            }
            DataType::LargeUtf8 => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Utf8)));
            }
            DataType::Utf8View => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::Utf8View)));
            }
            DataType::Binary => {
                return Ok(Box::new(GroupValuesBytes::<i32>::new(OutputType::Binary)));
            }
            DataType::LargeBinary => {
                return Ok(Box::new(GroupValuesBytes::<i64>::new(OutputType::Binary)));
            }
            DataType::BinaryView => {
                return Ok(Box::new(GroupValuesBytesView::new(OutputType::BinaryView)));
            }
            DataType::Boolean => {
                return Ok(Box::new(GroupValuesBoolean::new()));
            }
            _ => {}
        }
    }

    if multi_group_by::supported_schema(schema.as_ref()) {
        if matches!(group_ordering, GroupOrdering::None) {
            Ok(Box::new(GroupValuesColumn::<false>::try_new(schema)?))
        } else {
            Ok(Box::new(GroupValuesColumn::<true>::try_new(schema)?))
        }
    } else {
        Ok(Box::new(GroupValuesRows::try_new(schema)?))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, AsArray, BooleanArray, Int32Array, StringArray, StringViewArray,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use datafusion_expr::{EmitTo, GroupSelection};

    use super::new_group_values;
    use crate::aggregates::order::GroupOrdering;

    #[test]
    fn preserving_values_keep_group_indices_valid() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group",
            DataType::Int32,
            true,
        )]));
        let mut group_values = new_group_values(schema, &GroupOrdering::None).unwrap();
        assert!(group_values.supports_values_preserving());

        let input = Arc::new(Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(10),
            None,
            Some(30),
        ])) as ArrayRef;
        let mut groups = vec![];
        group_values.intern(&[input], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0, 2, 3]);

        let selection =
            GroupSelection::try_from_indices(&[3, 0, 2, 0], group_values.len()).unwrap();
        let expected = Int32Array::from(vec![Some(30), Some(10), None, Some(10)]);
        for _ in 0..2 {
            let actual = group_values.values_preserving(selection).unwrap();
            assert_eq!(actual[0].as_primitive::<Int32Type>(), &expected);
        }

        let empty = group_values
            .values_preserving(
                GroupSelection::try_from_indices(&[], group_values.len()).unwrap(),
            )
            .unwrap();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty[0].data_type(), &DataType::Int32);
        assert!(empty[0].is_empty());

        let input =
            Arc::new(Int32Array::from(vec![Some(20), Some(40), None])) as ArrayRef;
        group_values.intern(&[input], &mut groups).unwrap();
        assert_eq!(groups, vec![1, 4, 2]);

        let expected =
            Int32Array::from(vec![Some(10), Some(20), None, Some(30), Some(40)]);
        let actual = group_values
            .values_preserving(GroupSelection::all(group_values.len()))
            .unwrap();
        assert_eq!(actual[0].as_primitive::<Int32Type>(), &expected);

        let error =
            GroupSelection::try_from_indices(&[5], group_values.len()).unwrap_err();
        assert!(error.to_string().contains("out of bounds"));

        let actual = group_values.emit(EmitTo::All).unwrap();
        assert_eq!(actual[0].as_primitive::<Int32Type>(), &expected);
    }

    #[test]
    fn preserving_non_nullable_primitive_and_boolean_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("primitive", DataType::Int32, false),
            Field::new("boolean", DataType::Boolean, false),
        ]));
        let mut group_values = new_group_values(schema, &GroupOrdering::None).unwrap();
        let input = vec![
            Arc::new(Int32Array::from(vec![10, 20, 10])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
        ];
        let mut groups = vec![];
        group_values.intern(&input, &mut groups).unwrap();
        assert_eq!(groups, vec![0, 1, 0]);

        let selection =
            GroupSelection::try_from_indices(&[1, 0, 1], group_values.len()).unwrap();
        let actual = group_values.values_preserving(selection).unwrap();
        assert_eq!(
            actual[0].as_primitive::<Int32Type>(),
            &Int32Array::from(vec![20, 10, 20])
        );
        assert_eq!(
            actual[1].as_boolean(),
            &BooleanArray::from(vec![false, true, false])
        );
    }

    #[test]
    fn preserving_variable_width_values() {
        for data_type in [DataType::Utf8, DataType::Utf8View] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "group",
                data_type.clone(),
                true,
            )]));
            let mut group_values =
                new_group_values(schema, &GroupOrdering::None).unwrap();
            let input: ArrayRef = match data_type {
                DataType::Utf8 => Arc::new(StringArray::from(vec![
                    Some("a"),
                    None,
                    Some("a long value that is not inline"),
                    Some("a"),
                ])),
                DataType::Utf8View => Arc::new(StringViewArray::from(vec![
                    Some("a"),
                    None,
                    Some("a long value that is not inline"),
                    Some("a"),
                ])),
                _ => unreachable!(),
            };
            let mut groups = vec![];
            group_values.intern(&[input], &mut groups).unwrap();
            assert_eq!(groups, vec![0, 1, 2, 0]);

            let selected = group_values
                .values_preserving(
                    GroupSelection::try_from_indices(&[2, 1, 0, 2], 3).unwrap(),
                )
                .unwrap();
            let expected = vec![
                Some("a long value that is not inline"),
                None,
                Some("a"),
                Some("a long value that is not inline"),
            ];
            match data_type {
                DataType::Utf8 => assert_eq!(
                    selected[0].as_string::<i32>(),
                    &StringArray::from(expected)
                ),
                DataType::Utf8View => assert_eq!(
                    selected[0].as_string_view(),
                    &StringViewArray::from(expected)
                ),
                _ => unreachable!(),
            }

            // Reading did not remove values or alter interned indices.
            let input: ArrayRef = match data_type {
                DataType::Utf8 => Arc::new(StringArray::from(vec!["new"])),
                DataType::Utf8View => Arc::new(StringViewArray::from(vec!["new"])),
                _ => unreachable!(),
            };
            group_values.intern(&[input], &mut groups).unwrap();
            assert_eq!(groups, vec![3]);
        }
    }
}
