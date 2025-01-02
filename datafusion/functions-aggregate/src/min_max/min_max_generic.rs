use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow_schema::SortOptions;
use datafusion_common::Result;
use std::fmt::Debug;

use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use datafusion_expr::GroupsAccumulator;
use std::mem::size_of_val;
use std::ops::Deref;
use crate::min_max::{MovingMax, MovingMin};

fn convert_row_to_scalar(row_converter: &RowConverter, owned_row: &OwnedRow) -> Result<ScalarValue> {
    // Convert the row back to array so we can return it
    let converted = row_converter.convert_rows(vec![owned_row.row()])?;

    // Get the first value from the array (as we only have one row)
    ScalarValue::try_from_array(converted[0].deref(), 0)
}

/// Accumulator for min of lists
/// this accumulator is using arrow-row as the internal representation and a way for comparing
#[derive(Debug)]
pub(crate) struct GenericMinAccumulator {
    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The data type of the column
    data_type: DataType,

    /// The current minimum value
    min: Option<OwnedRow>,

    /// Null row to use for fast filtering
    null_row: OwnedRow,
}

impl GenericMinAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![(SortField::new_with_options(datatype.clone(), SortOptions {
            descending: false,
            nulls_first: true,
        }))])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            data_type: datatype.clone(),
            min: None,
        })
    }
}

impl Accumulator for GenericMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let rows = self.row_converter.convert_columns(values)?;

        let minimum_in_batch = rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .min();

        match (&self.min, minimum_in_batch) {
            // Update the minimum if the current minium is greater than the batch minimum
            (Some(current_val), Some(min_in_batch)) => {
                if current_val.row() > min_in_batch {
                    self.min = Some(min_in_batch.owned());
                }
            }
            // First minimum for the accumulator
            (None, Some(new_value)) => {
                self.min = Some(new_value.owned());
            }
            // If no minimum in batch (all nulls), do nothing
            (_, None) => {}
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let min = self.min.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, min)
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}


/// Accumulator for max of lists
/// this accumulator is using arrow-row as the internal representation and a way for comparing
#[derive(Debug)]
pub(crate) struct GenericMaxAccumulator {
    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The data type of the column
    data_type: DataType,

    /// The current minimum value
    max: Option<OwnedRow>,

    /// Null row to use for fast filtering
    null_row: OwnedRow,
}

impl GenericMaxAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![(SortField::new_with_options(datatype.clone(), SortOptions {
            descending: false,
            nulls_first: true,
        }))])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            data_type: datatype.clone(),
            max: None,
        })
    }
}

impl Accumulator for GenericMaxAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let rows = self.row_converter.convert_columns(values)?;

        let maximum_in_batch = rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .max();

        match (&self.max, maximum_in_batch) {
            // Update the maximum if the current max is less than the batch maximum
            (Some(current_val), Some(max_in_batch)) => {
                if current_val.row() < max_in_batch {
                    self.max = Some(max_in_batch.owned());
                }
            }
            // First maximum for the accumulator
            (None, Some(new_value)) => {
                self.max = Some(new_value.owned());
            }
            // If no maximum in batch (all nulls), do nothing
            (_, None) => {}
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let max = self.max.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, max)
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}

#[derive(Debug)]
pub(crate) struct GenericSlidingMinAccumulator {

    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The data type of the column
    data_type: DataType,

    /// The current minimum value
    min: Option<OwnedRow>,

    moving_min: MovingMin<OwnedRow>,

    /// Null row to use for fast filtering
    null_row: OwnedRow,
}

impl GenericSlidingMinAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![(SortField::new_with_options(datatype.clone(), SortOptions {
            descending: false,
            nulls_first: true,
        }))])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            data_type: datatype.clone(),
            min: None,
            moving_min: MovingMin::new()
        })
    }
}

impl Accumulator for GenericSlidingMinAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Get the current value or null if no value has been seen
        let min = self.min.as_ref().unwrap_or(&self.null_row);

        Ok(vec![convert_row_to_scalar(&self.row_converter, min)?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|row| self.moving_min.push(row.owned()));

        self.min = self.moving_min.min().cloned();
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|_| {
                self.moving_min.pop();
            });

        self.min = self.moving_min.min().cloned();

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let min = self.min.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, min)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}


#[derive(Debug)]
pub(crate) struct GenericSlidingMaxAccumulator {

    /// Convert the columns to row so we can compare them
    row_converter: RowConverter,

    /// The data type of the column
    data_type: DataType,

    /// The current maximum value
    max: Option<OwnedRow>,

    moving_max: MovingMax<OwnedRow>,

    /// Null row to use for fast filtering
    null_row: OwnedRow,
}

impl GenericSlidingMaxAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        let converter = RowConverter::new(vec![(SortField::new_with_options(datatype.clone(), SortOptions {
            descending: false,
            nulls_first: true,
        }))])?;

        // Create a null row to use for filtering out nulls from the input
        let null_row = {
            let null_array = ScalarValue::try_from(datatype)?.to_array_of_size(1)?;

            let rows = converter.convert_columns(&[null_array])?;

            rows.row(0).owned()
        };

        Ok(Self {
            row_converter: converter,
            null_row,
            data_type: datatype.clone(),
            max: None,
            moving_max: MovingMax::new()
        })
    }
}

impl Accumulator for GenericSlidingMaxAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // Get the current value or null if no value has been seen
        let max = self.max.as_ref().unwrap_or(&self.null_row);

        Ok(vec![convert_row_to_scalar(&self.row_converter, max)?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|row| self.moving_max.push(row.owned()));

        self.max = self.moving_max.max().cloned();
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // TODO - should assert getting only one column?
        let rows = self.row_converter.convert_columns(values)?;

        rows
            .iter()
            // Filter out nulls
            .filter(|row| row != &self.null_row.row())
            .for_each(|_| {
                self.moving_max.pop();
            });

        self.max = self.moving_max.max().cloned();

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        // Get the current value or null if no value has been seen
        let max = self.max.as_ref().unwrap_or(&self.null_row);

        convert_row_to_scalar(&self.row_converter, max)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.row_converter) + self.row_converter.size()
    }
}
