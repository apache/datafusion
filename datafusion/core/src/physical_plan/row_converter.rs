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

//! [`CardinalityAwareRowConverter`] for converting data to
//! [`arrow_row`] format.

use arrow::datatypes::DataType;
use arrow::row::{Row, RowConverter};
use arrow::row::{Rows, SortField};
use arrow_array::cast::AsArray;
use arrow_array::*;
use datafusion_common::{internal_err, DataFusionError, Result};

/// The threshold of the number of values at which to consider a
/// [`DictionaryArray`] "high" cardinality.
///
/// Since [`RowConverter`] blindly generates a mapping for all values,
/// regardless of if they appear in the keys, this value is compared
/// to the length of values.
///
/// The assumption is that the number of potential distinct key values
/// (aka the length of the values array) is a more robust predictor of
/// being "high" cardinality than the actual number of keys used. The
/// intuition for this is that other values in the dictionary could be
/// used in subsequent batches.
///
/// While an argument can made for doing something more sophisticated,
/// this would likely only really make sense if the dictionary
/// interner itself followed a similar approach, which it did not at
/// the time of this writing.
const LOW_CARDINALITY_THRESHOLD: usize = 10;

/// Wrapper around an [`arrow_row`] [`RowConverter` that disables
/// dictionary preservation for high cardinality columns, based on the
/// observed cardinalities in the first columns converted.
///
/// ## Background
///
/// By default, the [`RowConverter`] interns (and keeps a copy of) all
/// values from [`DictionaryArray`]s. For low cardinality columns
/// (with few distinct values) this approach is both faster and more
/// memory efficient. However for high cardinality coumns it is slower
/// and requires more memory. In certain degenerate cases, such as
/// columns of nearly unique values, the `RowConverter` will keep a
/// copy of the entire column.
///
/// See <https://github.com/apache/arrow-datafusion/issues/7200> for
/// more details
#[derive(Debug)]
pub enum CardinalityAwareRowConverter {
    /// Converter is newly initialized, and hasn't yet seen data
    New { fields: Vec<SortField> },
    /// Converter has seen data and can convert data
    Converting { inner: RowConverter },
}

impl CardinalityAwareRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self> {
        Ok(Self::New { fields })
    }

    pub fn size(&self) -> usize {
        match self {
            Self::New { .. } => {
                // TODO account for size of `fields`
                0
            }
            Self::Converting { inner } => inner.size(),
        }
    }

    pub fn empty_rows(&self, row_capacity: usize, data_capacity: usize) -> Result<Rows> {
        let converter = self.converter()?;
        Ok(converter.empty_rows(row_capacity, data_capacity))
    }

    pub fn convert_rows<'a, I>(&self, rows: I) -> Result<Vec<ArrayRef>>
    where
        I: IntoIterator<Item = Row<'a>>,
    {
        let converter = self.converter()?;
        Ok(converter.convert_rows(rows)?)
    }

    pub fn append(&mut self, rows: &mut Rows, columns: &[ArrayRef]) -> Result<()> {
        Ok(self.converter_mut(columns)?.append(rows, columns)?)
    }

    /// Calls [`RowConverter::convert_columns`] after first
    /// initializing the converter based on cardinalities
    pub fn convert_columns(&mut self, columns: &[ArrayRef]) -> Result<Rows> {
        Ok(self.converter_mut(columns)?.convert_columns(columns)?)
    }

    /// Return a mutable reference to the inner converter, creating it if needed
    fn converter_mut(&mut self, columns: &[ArrayRef]) -> Result<&mut RowConverter> {
        if let Self::New { fields } = self {
            let mut updated_fields = fields.clone();
            for (i, col) in columns.iter().enumerate() {
                if let DataType::Dictionary(_, _) = col.data_type() {
                    // see comments on LOW_CARDINALITY_THRESHOLD for
                    // the rationale of this calculation
                    let cardinality = col.as_any_dictionary_opt().unwrap().values().len();
                    if cardinality >= LOW_CARDINALITY_THRESHOLD {
                        updated_fields[i] =
                            updated_fields[i].clone().preserve_dictionaries(false);
                    }
                }
            }
            *self = Self::Converting {
                inner: RowConverter::new(updated_fields)?,
            };
        };

        match self {
            Self::New { .. } => {
                unreachable!();
            }
            Self::Converting { inner } => Ok(inner),
        }
    }

    /// Return a reference to the inner converter, erroring if we have not yet converted a row.
    fn converter(&self) -> Result<&RowConverter> {
        match self {
            Self::New { .. } => internal_err!(
                "CardinalityAwareRowConverter has not converted any rows yet"
            ),
            Self::Converting { inner } => Ok(inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::Rng;
    use uuid::Uuid;

    use arrow::datatypes::DataType;
    use arrow::error::ArrowError;
    use arrow_array::{
        types::Int32Type, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray,
    };
    use arrow_schema::SortOptions;

    use super::*;

    // Generate a record batch with a high cardinality dictionary field
    fn generate_batch_with_cardinality(card: String) -> Result<RecordBatch, ArrowError> {
        let col_a = if card == "high" {
            // building column `a_dict`
            let mut values_vector: Vec<String> = Vec::new();
            for _i in 1..=20 {
                values_vector.push(Uuid::new_v4().to_string());
            }
            let values = StringArray::from(values_vector);

            let mut keys_vector: Vec<i32> = Vec::new();
            for _i in 1..=20 {
                keys_vector.push(rand::thread_rng().gen_range(0..20));
            }
            let keys = Int32Array::from(keys_vector);
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap(),
            );
        } else {
            let values_vector = vec!["a", "b", "c"];
            let values = StringArray::from(values_vector);

            let mut keys_vector: Vec<i32> = Vec::new();
            for _i in 1..=20 {
                keys_vector.push(rand::thread_rng().gen_range(0..2));
            }
            let keys = Int32Array::from(keys_vector);
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap(),
            )
        };

        // building column `b_prim`
        let mut values: Vec<i32> = Vec::new();
        for _i in 1..=20 {
            values.push(rand::thread_rng().gen_range(0..20));
        }
        let col_b: ArrayRef = Arc::new(Int32Array::from(values));

        // building the record batch
        RecordBatch::try_from_iter(vec![("a_dict", col_a), ("b_prim", col_b)])
    }

    #[tokio::test]
    async fn test_with_high_card() {
        let batch = generate_batch_with_cardinality(String::from("high")).unwrap();
        let sort_fields = vec![
            arrow::row::SortField::new_with_options(
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                SortOptions::default(),
            ),
            arrow::row::SortField::new_with_options(
                DataType::Int32,
                SortOptions::default(),
            ),
        ];

        // With the `CardinalityAwareRowConverter`, when the high cardinality dictionary-encoded sort field is
        // converted to the `Row` format, the dictionary encoding is not preserved and we switch to Utf8 encoding.
        let mut converter =
            CardinalityAwareRowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Utf8);

        let mut converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(batch.columns()).unwrap();
        let converted_batch: Vec<Arc<dyn Array>> = converter.convert_rows(&rows).unwrap();
        // With the `RowConverter`, the dictionary encoding is preserved.
        assert_eq!(
            converted_batch[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
    }

    #[tokio::test]
    async fn test_with_low_card() {
        let batch = generate_batch_with_cardinality(String::from("low")).unwrap();
        let sort_fields = vec![
            arrow::row::SortField::new_with_options(
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                SortOptions::default(),
            ),
            arrow::row::SortField::new_with_options(
                DataType::Int32,
                SortOptions::default(),
            ),
        ];
        // With low cardinality dictionary-encoded sort fields, both `CardinalityAwareRowConverter` and `RowConverter`
        // preserves the dictionary encoding.
        let mut converter =
            CardinalityAwareRowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(
            converted_batch[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );

        let mut converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(
            converted_batch[0].data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
    }
}
