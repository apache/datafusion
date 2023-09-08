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
use arrow::row::{SortField, Rows};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow_array::*;
use arrow_array::cast::AsArray;
use arrow::row::{RowConverter, Row};

const LOW_CARDINALITY_THRESHOLD: usize = 10;

#[derive(Debug)]
pub struct CardinalityAwareRowConverter {
    fields: Option<Vec<SortField>>,
    inner: Option<RowConverter>,
}

impl CardinalityAwareRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self, ArrowError> {
        let converter = RowConverter::new(fields.clone())?;
        Ok(Self {
            fields: Some(fields),
            inner: Some(converter),
        })
    }
    
    pub fn size(&self) -> usize {
        return self.inner.as_ref().unwrap().size();
    }
    
    pub fn empty_rows(&self, row_capacity: usize, data_capacity: usize) -> Rows {
        return self.inner.as_ref().unwrap().empty_rows(row_capacity, data_capacity);
    }

    pub fn convert_rows<'a, I>(&self, rows: I) -> Result<Vec<ArrayRef>, ArrowError>
    where
        I: IntoIterator<Item = Row<'a>>,
    {
        return self.inner.as_ref().unwrap().convert_rows(rows);
    }

    pub fn convert_columns(
        &mut self,
        columns: &[ArrayRef]) -> Result<Rows, ArrowError> {
        if let Some(mut updated_fields) = self.fields.take() {
            for (i, col) in columns.iter().enumerate() {
                if let DataType::Dictionary(_, _) = col.data_type() {
                    let cardinality = col.as_any_dictionary_opt().unwrap().values().len();
                    if cardinality >= LOW_CARDINALITY_THRESHOLD {
                        updated_fields[i] = updated_fields[i].clone().preserve_dictionaries(false);
                    }
                }
            }
            self.inner = Some(RowConverter::new(updated_fields)?);
        }
        self.inner.as_mut().unwrap().convert_columns(columns)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;
    use rand::Rng;

    use arrow::error::ArrowError;
    use arrow::datatypes::DataType;
    use arrow_schema::SortOptions;
    use arrow_array::{StringArray, DictionaryArray, Int32Array, types::Int32Type, RecordBatch, ArrayRef};

    use super::*;

    // Generate a record batch with a high cardinality dictionary field
    fn generate_batch_with_cardinality(card: String) -> Result<RecordBatch, ArrowError> {
        let col_a: ArrayRef;
        if card == "high" {
            // building column `a_dict`
            let mut values_vector: Vec<String> = Vec::new();
            for _i in 1..=20 {
                values_vector.push(String::from(Uuid::new_v4().to_string()));
            }
            let values = StringArray::from(values_vector);

            let mut keys_vector: Vec<i32> = Vec::new();
            for _i in 1..=20 {
                keys_vector.push(rand::thread_rng().gen_range(0..20));
            }
            let keys = Int32Array::from(keys_vector);
            col_a = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
        } else {
            let values_vector = vec!["a", "b", "c"];
            let values = StringArray::from(values_vector);
            
            let mut keys_vector: Vec<i32> = Vec::new();
            for _i in 1..=20 {
                keys_vector.push(rand::thread_rng().gen_range(0..2));
            }
            let keys = Int32Array::from(keys_vector);
            col_a = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
        }

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
            arrow::row::SortField::new_with_options(DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), SortOptions::default()),
            arrow::row::SortField::new_with_options(DataType::Int32, SortOptions::default())
        ];

        // With the `CardinalityAwareRowConverter`, when the high cardinality dictionary-encoded sort field is 
        // converted to the `Row` format, the dictionary encoding is not preserved and we switch to Utf8 encoding.
        let mut converter = CardinalityAwareRowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Utf8);

        let mut converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch: Vec<Arc<dyn Array>> = converter.convert_rows(&rows).unwrap();
        // With the `RowConverter`, the dictionary encoding is preserved.
        assert_eq!(converted_batch[0].data_type(), &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    }

    #[tokio::test]
    async fn test_with_low_card() {
        let batch = generate_batch_with_cardinality(String::from("low")).unwrap();
        let sort_fields = vec![
            arrow::row::SortField::new_with_options(DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), SortOptions::default()),
            arrow::row::SortField::new_with_options(DataType::Int32, SortOptions::default())
        ];
        // With low cardinality dictionary-encoded sort fields, both `CardinalityAwareRowConverter` and `RowConverter`
        // preserves the dictionary encoding.
        let mut converter = CardinalityAwareRowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));

        let mut converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    }
}
