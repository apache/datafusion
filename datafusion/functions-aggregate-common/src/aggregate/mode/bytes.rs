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

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::array::AsArray;
use arrow::array::OffsetSizeTrait;
use arrow::datatypes::DataType;
use datafusion_common::cast::as_list_array;
use datafusion_common::cast::as_primitive_array;
use datafusion_common::utils::array_into_list_array_nullable;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::binary_map::ArrowBytesSet;
use datafusion_physical_expr_common::binary_map::{ArrowBytesMap, OutputType};
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewMap;
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewSet;

#[derive(Debug)]
pub struct BytesModeAccumulator<O: OffsetSizeTrait> {
    values: ArrowBytesSet<O>,
    value_counts: ArrowBytesMap<O, i64>,
}

impl<O: OffsetSizeTrait> BytesModeAccumulator<O> {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            values: ArrowBytesSet::new(output_type),
            value_counts: ArrowBytesMap::new(output_type),
        }
    }
}

impl<O: OffsetSizeTrait> Accumulator for BytesModeAccumulator<O> {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.values.insert(&values[0]);

        self.value_counts
            .insert_or_update(&values[0], |_| 1i64, |count| *count += 1);

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = self.values.take().into_state();
        let payloads: Vec<ScalarValue> = self
            .value_counts
            .take()
            .get_payloads(&values)
            .into_iter()
            .map(|count| match count {
                Some(c) => ScalarValue::Int64(Some(c)),
                None => ScalarValue::Int64(None),
            })
            .collect();

        let values_list = Arc::new(array_into_list_array_nullable(values));
        let payloads_list = ScalarValue::new_list_nullable(&payloads, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_list),
            ScalarValue::List(payloads_list),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        let counts = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        arr.iter()
            .zip(counts.iter())
            .try_for_each(|(maybe_list, maybe_count)| {
                if let (Some(list), Some(count)) = (maybe_list, maybe_count) {
                    // Insert or update the count for each value
                    self.value_counts.insert_or_update(
                        &list,
                        |_| count,
                        |existing_count| *existing_count += count,
                    );
                }
                Ok(())
            })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_index: Option<usize> = None;
        let mut max_count: i64 = 0;

        let values = self.values.take().into_state();
        let counts = self.value_counts.take().get_payloads(&values);

        for (i, count) in counts.into_iter().enumerate() {
            if let Some(c) = count {
                if c > max_count {
                    max_count = c;
                    max_index = Some(i);
                }
            }
        }

        match max_index {
            Some(index) => {
                let array = values.as_string::<O>();
                let mode_value = array.value(index);
                if mode_value.is_empty() {
                    Ok(ScalarValue::Utf8(None))
                } else if O::IS_LARGE {
                    Ok(ScalarValue::LargeUtf8(Some(mode_value.to_string())))
                } else {
                    Ok(ScalarValue::Utf8(Some(mode_value.to_string())))
                }
            }
            None => {
                if O::IS_LARGE {
                    Ok(ScalarValue::LargeUtf8(None))
                } else {
                    Ok(ScalarValue::Utf8(None))
                }
            }
        }
    }

    fn size(&self) -> usize {
        self.values.size() + self.value_counts.size()
    }
}

#[derive(Debug)]
pub struct BytesViewModeAccumulator {
    values: ArrowBytesViewSet,
    value_counts: ArrowBytesViewMap<i64>,
}

impl BytesViewModeAccumulator {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            values: ArrowBytesViewSet::new(output_type),
            value_counts: ArrowBytesViewMap::new(output_type),
        }
    }
}

impl Accumulator for BytesViewModeAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.values.insert(&values[0]);

        self.value_counts
            .insert_or_update(&values[0], |_| 1i64, |count| *count += 1);

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values = self.values.take().into_state();
        let payloads: Vec<ScalarValue> = self
            .value_counts
            .take()
            .get_payloads(&values)
            .into_iter()
            .map(|count| match count {
                Some(c) => ScalarValue::Int64(Some(c)),
                None => ScalarValue::Int64(None),
            })
            .collect();

        let values_list = Arc::new(array_into_list_array_nullable(values));
        let payloads_list = ScalarValue::new_list_nullable(&payloads, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_list),
            ScalarValue::List(payloads_list),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let arr = as_list_array(&states[0])?;
        let counts = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        arr.iter()
            .zip(counts.iter())
            .try_for_each(|(maybe_list, maybe_count)| {
                if let (Some(list), Some(count)) = (maybe_list, maybe_count) {
                    // Insert or update the count for each value
                    self.value_counts.insert_or_update(
                        &list,
                        |_| count,
                        |existing_count| *existing_count += count,
                    );
                }
                Ok(())
            })
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_index: Option<usize> = None;
        let mut max_count: i64 = 0;

        let values = self.values.take().into_state();
        let counts = self.value_counts.take().get_payloads(&values);

        for (i, count) in counts.into_iter().enumerate() {
            if let Some(c) = count {
                if c > max_count {
                    max_count = c;
                    max_index = Some(i);
                }
            }
        }

        match max_index {
            Some(index) => {
                let array = values.as_string_view();
                let mode_value = array.value(index);
                if mode_value.is_empty() {
                    Ok(ScalarValue::Utf8View(None))
                } else {
                    Ok(ScalarValue::Utf8View(Some(mode_value.to_string())))
                }
            }
            None => Ok(ScalarValue::Utf8View(None)),
        }
    }

    fn size(&self) -> usize {
        self.values.size() + self.value_counts.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, GenericByteViewArray, StringArray};
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_mode_accumulator_single_mode_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::<i32>::new(OutputType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::<i32>::new(OutputType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::<i32>::new(OutputType::Utf8);
        let values: ArrayRef =
            Arc::new(StringArray::from(vec![None as Option<&str>, None, None]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8() -> Result<()> {
        let mut acc = BytesModeAccumulator::<i32>::new(OutputType::Utf8);
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_utf8view() -> Result<()> {
        let mut acc = BytesViewModeAccumulator::new(OutputType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
            Some("apple"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_utf8view() -> Result<()> {
        let mut acc = BytesViewModeAccumulator::new(OutputType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            Some("banana"),
            Some("apple"),
            Some("orange"),
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_all_nulls_utf8view() -> Result<()> {
        let mut acc = BytesViewModeAccumulator::new(OutputType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            None as Option<&str>,
            None,
            None,
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(None));
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_utf8view() -> Result<()> {
        let mut acc = BytesViewModeAccumulator::new(OutputType::Utf8View);
        let values: ArrayRef = Arc::new(GenericByteViewArray::from(vec![
            Some("apple"),
            None,
            Some("banana"),
            Some("apple"),
            None,
            None,
            None,
            Some("banana"),
        ]));

        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;

        assert_eq!(result, ScalarValue::Utf8View(Some("apple".to_string())));
        Ok(())
    }
}
