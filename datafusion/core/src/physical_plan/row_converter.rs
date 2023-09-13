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
//! [`arrow::row`] format.

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

/// Wrapper around an [`arrow::row`] [`RowConverter`] that disables
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
    New {
        /// Defines the Row conversion
        fields: Vec<SortField>,
    },
    /// Converter has seen data and can convert [`Array`]s to/from
    /// [`Rows`]
    Converting {
        /// Underlying converter
        converter: RowConverter,
        /// if preserve_dictionaries is disabled, the output type can be
        /// different than input.
        output_types: Vec<DataType>,
    },
}

impl CardinalityAwareRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self> {
        Ok(Self::New { fields })
    }

    /// Returns the memory size of the underlying [`RowConverter`] if
    /// any.
    pub fn size(&self) -> usize {
        match self {
            Self::New { .. } => 0,
            Self::Converting { converter, .. } => converter.size(),
        }
    }

    pub fn empty_rows(&self, row_capacity: usize, data_capacity: usize) -> Result<Rows> {
        match self {
            Self::New { .. } => internal_err!(
                "CardinalityAwareRowConverter has not converted any rows yet"
            ),
            Self::Converting { converter, .. } => {
                Ok(converter.empty_rows(row_capacity, data_capacity))
            }
        }
    }

    pub fn convert_rows<'a, I>(&self, rows: I) -> Result<Vec<ArrayRef>>
    where
        I: IntoIterator<Item = Row<'a>>,
    {
        match self {
            Self::New { .. } => internal_err!(
                "CardinalityAwareRowConverter has not converted any rows yet"
            ),
            Self::Converting {
                converter,
                output_types,
            } => {
                // Cast output type if needed. The input type and
                // output type must match exactly (including
                // encodings).
                // https://github.com/apache/arrow-datafusion/discussions/7421
                // could reduce the need for this.
                let output = converter
                    .convert_rows(rows)?
                    .into_iter()
                    .zip(output_types.iter())
                    .map(|(arr, output_type)| {
                        if arr.data_type() != output_type {
                            Ok(arrow::compute::cast(&arr, output_type)?)
                        } else {
                            Ok(arr)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(output)
            }
        }
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
            let mut output_types = vec![];
            for (i, col) in columns.iter().enumerate() {
                output_types.push(col.data_type().clone());
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
                converter: RowConverter::new(updated_fields)?,
                output_types,
            };
        };

        match self {
            Self::New { .. } => {
                unreachable!();
            }
            Self::Converting { converter, .. } => Ok(converter),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rand::Rng;
    use uuid::Uuid;

    use arrow::datatypes::DataType;
    use arrow_array::{
        types::Int32Type, ArrayRef, DictionaryArray, Int32Array, RecordBatch, StringArray,
    };
    use arrow_schema::SortOptions;

    use super::*;

    /// Generate a record batch with two columns:
    ///
    /// `a_dict`: String Dictionary
    /// `b_prim`: Int32Array with random values 0..20
    enum Generator {
        /// "High" cardinality (20 distinct values)
        High,
        /// "Low" cardinality (2 distinct values)
        Low,
    }

    impl Generator {
        fn build(&self) -> RecordBatch {
            let (keys, values) = match self {
                Self::High => {
                    let values: Vec<_> =
                        (0..20).map(|_| Uuid::new_v4().to_string()).collect();
                    let values = StringArray::from(values);

                    let keys: Int32Array = (0..20)
                        .map(|_| rand::thread_rng().gen_range(0..20))
                        .collect();
                    (keys, values)
                }
                Self::Low => {
                    let values = StringArray::from_iter_values(["a", "b", "c"]);
                    let keys: Int32Array = (0..20)
                        .map(|_| rand::thread_rng().gen_range(0..2))
                        .collect();
                    (keys, values)
                }
            };
            let dict =
                DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
            let col_a = Arc::new(dict) as ArrayRef;

            // building column `b_prim`
            let values: Int32Array = (0..20)
                .map(|_| rand::thread_rng().gen_range(0..20))
                .collect();
            let col_b: ArrayRef = Arc::new(values);

            RecordBatch::try_from_iter(vec![("a_dict", col_a), ("b_prim", col_b)])
                .unwrap()
        }

        fn sort_fields(&self) -> Vec<SortField> {
            vec![
                SortField::new_with_options(
                    DataType::Dictionary(
                        Box::new(DataType::Int32),
                        Box::new(DataType::Utf8),
                    ),
                    SortOptions::default(),
                ),
                SortField::new_with_options(DataType::Int32, SortOptions::default()),
            ]
        }
    }

    #[tokio::test]
    async fn test_with_high_card() {
        let generator = Generator::High;
        let batch = generator.build();

        let mut card_converter =
            CardinalityAwareRowConverter::new(generator.sort_fields()).unwrap();
        let rows = card_converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = card_converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch, batch.columns());

        let mut row_converter = RowConverter::new(generator.sort_fields()).unwrap();
        let rows = row_converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = row_converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch, batch.columns());

        // with high cardinality the cardinality aware converter
        // should be lower, as there is no interning of values
        assert!(card_converter.size() < row_converter.size());
    }

    #[tokio::test]
    async fn test_with_low_card() {
        let generator = Generator::Low;
        let batch = generator.build();

        let mut card_converter =
            CardinalityAwareRowConverter::new(generator.sort_fields()).unwrap();
        let rows = card_converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = card_converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch, batch.columns());

        let mut row_converter = RowConverter::new(generator.sort_fields()).unwrap();
        let rows = row_converter.convert_columns(batch.columns()).unwrap();
        let converted_batch = row_converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch, batch.columns());

        // with low cardinality the row converters sizes should be
        // equal as the same converter logic is used
        assert_eq!(card_converter.size(), row_converter.size());
    }
}
