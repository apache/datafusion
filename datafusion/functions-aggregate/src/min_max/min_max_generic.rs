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

use crate::min_max::{MovingMax, MovingMin};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow::row::{OwnedRow, Row, RowConverter, SortField};
use arrow_schema::SortOptions;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of_val;
use std::ops::Deref;

pub(crate) type GenericMinAccumulator = GenericMinMaxAccumulator<MinAccumulatorHelper>;
pub(crate) type GenericMaxAccumulator = GenericMinMaxAccumulator<MaxAccumulatorHelper>;

pub(crate) type GenericSlidingMinAccumulator =
    GenericSlidingMinMaxAccumulator<GenericMovingMin>;
pub(crate) type GenericSlidingMaxAccumulator =
    GenericSlidingMinMaxAccumulator<GenericMovingMax>;

fn convert_row_to_scalar(
    row_converter: &RowConverter,
    owned_row: &OwnedRow,
) -> Result<ScalarValue> {
    // Convert the row back to array so we can return it
    let converted = row_converter.convert_rows(vec![owned_row.row()])?;

    // Get the first value from the array (as we only have one row)
    ScalarValue::try_from_array(converted[0].deref(), 0)
}

/// Helper trait for min/max accumulators to avoid code duplication
pub(crate) trait GenericMinMaxAccumulatorHelper: Debug + Send + Sync {
    /// Return true if the new value should replace the current value
    /// for minimum the new value should be less than the current value
    /// for maximum the new value should be greater than the current value
    fn should_replace<'a>(current: &Row<'a>, possibly_new: &Row<'a>) -> bool;

    /// Get the minimum/maximum value from an iterator
    fn get_value_from_iter<Item: Ord + Sized, I: Iterator<Item = Item>>(
        iter: I,
    ) -> Option<Item>;
}

#[derive(Debug)]
pub(crate) struct MinAccumulatorHelper;

impl GenericMinMaxAccumulatorHelper for MinAccumulatorHelper {
    /// Should replace the current value if the new value is less than the current value
    #[inline]
    fn should_replace<'a>(current: &Row<'a>, possibly_new: &Row<'a>) -> bool {
        current > possibly_new
    }

    #[inline]
    fn get_value_from_iter<Item: Ord + Sized, I: Iterator<Item = Item>>(
        iter: I,
    ) -> Option<Item> {
        iter.min()
    }
}

#[derive(Debug)]
pub(crate) struct MaxAccumulatorHelper;

impl GenericMinMaxAccumulatorHelper for MaxAccumulatorHelper {
    /// Should replace the current value if the new value is greater than the current value
    #[inline]
    fn should_replace<'a>(current: &Row<'a>, possibly_new: &Row<'a>) -> bool {
        current < possibly_new
    }

    #[inline]
    fn get_value_from_iter<Item: Ord + Sized, I: Iterator<Item = Item>>(
        iter: I,
    ) -> Option<Item> {
        iter.max()
    }
}

/// Accumulator for min/max of lists
/// this accumulator is using arrow-row as the internal representation and a way for comparing
#[derive(Debug)]
pub(crate) struct GenericMinMaxAccumulator<Helper>
where
    Helper: GenericMinMaxAccumulatorHelper,
{
    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The current minimum/maximum value
    current_value: Option<OwnedRow>,

    /// Null row to use for fast filtering
    null_row: OwnedRow,

    phantom_data: PhantomData<Helper>,
}

impl<Helper> GenericMinMaxAccumulator<Helper>
where
    Helper: GenericMinMaxAccumulatorHelper,
{
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![
            (SortField::new_with_options(
                datatype.clone(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )),
        ])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            current_value: None,
            phantom_data: PhantomData,
        })
    }
}

impl<Helper> Accumulator for GenericMinMaxAccumulator<Helper>
where
    Helper: GenericMinMaxAccumulatorHelper,
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let rows = self.row_converter.convert_columns(values)?;

        let wanted_value_in_batch = Helper::get_value_from_iter(
            rows.iter()
                // Filter out nulls
                .filter(|row| row != &self.null_row.row()),
        );

        match (&self.current_value, wanted_value_in_batch) {
            // Update the minimum/maximum based on the should replace logic
            (Some(current_val), Some(current_val_in_batch)) => {
                if Helper::should_replace(&current_val.row(), &current_val_in_batch) {
                    self.current_value = Some(current_val_in_batch.owned());
                }
            }
            // First minimum/maximum for the accumulator
            (None, Some(new_value)) => {
                self.current_value = Some(new_value.owned());
            }
            // If no minimum/maximum in batch (all nulls), do nothing
            (_, None) => {}
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let current_value = self.current_value.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, current_value)
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}

/// Helper trait for sliding min/max accumulators to avoid code duplication
pub(crate) trait GenericSlidingMinMaxAccumulatorHelper:
    Debug + Default + Send + Sync
{
    fn push(&mut self, row: OwnedRow);

    fn pop(&mut self);

    fn value(&self) -> Option<&OwnedRow>;
}

#[derive(Debug, Default)]
pub(crate) struct GenericMovingMin(MovingMin<OwnedRow>);

impl GenericSlidingMinMaxAccumulatorHelper for GenericMovingMin {
    fn push(&mut self, row: OwnedRow) {
        self.0.push(row);
    }

    fn pop(&mut self) {
        self.0.pop();
    }

    fn value(&self) -> Option<&OwnedRow> {
        self.0.min()
    }
}

#[derive(Debug, Default)]
pub(crate) struct GenericMovingMax(MovingMax<OwnedRow>);

impl GenericSlidingMinMaxAccumulatorHelper for GenericMovingMax {
    fn push(&mut self, row: OwnedRow) {
        self.0.push(row);
    }

    fn pop(&mut self) {
        self.0.pop();
    }

    fn value(&self) -> Option<&OwnedRow> {
        self.0.max()
    }
}

#[derive(Debug)]
pub(crate) struct GenericSlidingMinMaxAccumulator<
    MovingWindowHelper: GenericSlidingMinMaxAccumulatorHelper,
> {
    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The current minimum value
    current_value: Option<OwnedRow>,

    moving_helper: MovingWindowHelper,

    /// Null row to use for fast filtering
    null_row: OwnedRow,
}

impl<MovingWindowHelper: GenericSlidingMinMaxAccumulatorHelper>
    GenericSlidingMinMaxAccumulator<MovingWindowHelper>
{
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![
            (SortField::new_with_options(
                datatype.clone(),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )),
        ])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            current_value: None,
            moving_helper: MovingWindowHelper::default(),
        })
    }
}

impl<MovingWindowHelper: GenericSlidingMinMaxAccumulatorHelper> Accumulator
    for GenericSlidingMinMaxAccumulator<MovingWindowHelper>
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Get the current value or null if no value has been seen
        let min = self.current_value.as_ref().unwrap_or(&self.null_row);

        Ok(vec![convert_row_to_scalar(&self.row_converter, min)?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows.iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|row| self.moving_helper.push(row.owned()));

        self.current_value = self.moving_helper.value().cloned();
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows.iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|_| {
                self.moving_helper.pop();
            });

        self.current_value = self.moving_helper.value().cloned();

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let current_value = self.current_value.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, current_value)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}

#[cfg(test)]
mod tests {
    use crate::min_max::{GenericMaxAccumulator, GenericMinAccumulator};
    use arrow::array::builder::{BooleanBuilder, GenericListBuilder};
    use arrow::array::types::Int32Type;
    use arrow::array::{ArrayRef, ListArray};
    use arrow_schema::{DataType, Field};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Accumulator;
    use std::ops::Deref;
    use std::sync::Arc;

    trait CreateArrayAndScalar {
        fn get_data_type() -> DataType;

        fn create_array(&self) -> ArrayRef;
        fn create_scalar(&self) -> ScalarValue;
    }

    impl CreateArrayAndScalar for Vec<Option<Vec<Option<i32>>>> {
        fn get_data_type() -> DataType {
            DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        }

        fn create_array(&self) -> ArrayRef {
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                self.iter().cloned(),
            ))
        }

        fn create_scalar(&self) -> ScalarValue {
            let array = self.create_array();
            ScalarValue::try_from_array(array.deref(), 0).unwrap()
        }
    }

    impl CreateArrayAndScalar for Vec<Option<Vec<Option<bool>>>> {
        fn get_data_type() -> DataType {
            DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true)))
        }

        fn create_array(&self) -> ArrayRef {
            let mut builder = GenericListBuilder::<i32, _>::new(BooleanBuilder::new());

            builder.extend(self.iter().cloned());

            Arc::new(builder.finish())
        }

        fn create_scalar(&self) -> ScalarValue {
            let array = self.create_array();
            ScalarValue::try_from_array(array.deref(), 0).unwrap()
        }
    }

    fn run_test<T: CreateArrayAndScalar>(
        acc: &mut dyn Accumulator,
        values: &[T],
        expected: &T,
    ) -> (ScalarValue, ScalarValue) {
        for batch in values.iter() {
            let batch = batch.create_array();
            acc.update_batch(&[batch]).unwrap();
        }
        let result = acc.evaluate().unwrap();
        let expected_scalar = expected.create_scalar();

        (result, expected_scalar)
    }

    fn test_min_max<T: CreateArrayAndScalar + Clone>(
        values: &[T],
        expected_min: &T,
        expected_max: &T,
    ) {
        {
            let mut acc = GenericMinAccumulator::try_new(&T::get_data_type()).unwrap();
            let (result, expected_scalar) = run_test(&mut acc, values, expected_min);
            assert_eq!(
                result, expected_scalar,
                "min failed, expected: {expected_scalar}, got: {result}"
            );
        }
        {
            let mut acc = GenericMaxAccumulator::try_new(&T::get_data_type()).unwrap();
            let (result, expected_scalar) = run_test(&mut acc, values, expected_max);
            assert_eq!(
                result, expected_scalar,
                "max failed, expected: {expected_scalar}, got: {result}"
            );
        }
    }

    #[test]
    fn basic_i32_list() {
        let values = vec![
            Some(vec![Some(107)]),
            None,
            Some(vec![None, None, Some(45), Some(71), None]),
            Some(vec![None, None]),
        ];
        let expected_min = Some(vec![None, None]);
        let expected_max = Some(vec![Some(107)]);

        test_min_max(&[values], &vec![expected_min], &vec![expected_max]);
    }

    #[test]
    fn basic_bool_list() {
        let values = vec![
            Some(vec![]),
            Some(vec![Some(true)]),
            Some(vec![]),
            Some(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(false),
            ]),
            Some(vec![Some(true), None]),
            Some(vec![None, None, None]),
            None,
            Some(vec![Some(false), Some(true), None, None]),
            Some(vec![None]),
            Some(vec![Some(true)]),
        ];
        let expected_min = Some(vec![]);
        let expected_max = Some(vec![
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
        ]);
        test_min_max(&[values], &vec![expected_min], &vec![expected_max]);
    }
}
