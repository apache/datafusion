use std::sync::Arc;

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
use arrow::row::RowConverter;

const LOW_CARDINALITY_THRESHOLD: usize = 10;

#[derive(Debug)]
pub struct CardinalityAwareRowConverter {
    fields: Option<Vec<SortField>>,
    inner: Option<RowConverter>,
}

impl CardinalityAwareRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self, ArrowError> {
        Ok(Self {
            fields: Some(fields),
            inner: None,
        })
    }
    
    pub fn size(&self) -> usize {
        return self.inner.as_ref().unwrap().size();
    }
    
    pub fn convert_rows(&self, rows: &Rows) -> Result<Vec<ArrayRef>, ArrowError> {
        return self.inner.as_ref().unwrap().convert_rows(rows);
    }

    pub fn convert_columns(
        &mut self,
        columns: &[ArrayRef]) -> Result<Rows, ArrowError> {
        if self.fields != None {
            let mut updated_fields = self.fields.take();
            for (i, col) in columns.iter().enumerate() {
                if let DataType::Dictionary(_, _) = col.data_type() {
                    let cardinality = col.as_any_dictionary_opt().unwrap().values().len();
                    if cardinality >= LOW_CARDINALITY_THRESHOLD {
                        updated_fields.as_mut().unwrap()[i] = updated_fields.as_ref().unwrap()[i].clone().preserve_dictionaries(false);
                    }
                }
            }
            self.inner = Some(RowConverter::new(updated_fields.unwrap())?);
        }
        self.inner.as_mut().unwrap().convert_columns(columns)
    }
}

mod tests {
    use std::sync::Arc;

    use uuid::Uuid;
    use rand::Rng;

    use arrow::error::ArrowError;
    use arrow::datatypes::{DataType, Field};
    use arrow_schema::{Schema, SchemaRef, SortOptions};
    use arrow_array::{StringArray, DictionaryArray, Int32Array, types::Int32Type, RecordBatch, ArrayRef};

    use super::*;

    // Generate a record batch with a high cardinality dictionary field
    fn generate_batch_with_high_card_dict_field() -> Result<RecordBatch, ArrowError> {
        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("a_dict", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), false),
            Field::new("b_prim", DataType::Int32, false),
        ]));

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
        let col_a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());

        // building column `b_prim`
        let mut values: Vec<i32> = Vec::new();
            for _i in 1..=20 {
                values.push(rand::thread_rng().gen_range(0..20));
            }
        let col_b: ArrayRef = Arc::new(Int32Array::from(values));

        // building the record batch
        RecordBatch::try_from_iter(vec![("a_dict", col_a), ("b_prim", col_b)])
    }
    
    // fn generate_batch_with_low_card_dict_field() {}

    #[tokio::test]
    async fn test_cardinality_decision() {
        let batch = generate_batch_with_high_card_dict_field().unwrap();
        let sort_fields = vec![
            arrow::row::SortField::new_with_options(DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), SortOptions::default()),
            arrow::row::SortField::new_with_options(DataType::Int32, SortOptions::default())
        ];
        let mut converter = CardinalityAwareRowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Utf8);

        let mut converter = RowConverter::new(sort_fields.clone()).unwrap();
        let rows = converter.convert_columns(&batch.columns()).unwrap();        
        let converted_batch = converter.convert_rows(&rows).unwrap();
        assert_eq!(converted_batch[0].data_type(), &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)));
    }
}
