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
