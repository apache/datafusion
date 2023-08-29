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
use arrow_array::types::*;
use arrow::row::RowConverter;

const LOW_CARDINALITY_THRESHOLD: usize = 10;

macro_rules! downcast_dict {
    ($array:ident, $key:ident) => {{
        $array
            .as_any()
            .downcast_ref::<DictionaryArray<$key>>()
            .unwrap()
    }};
}

#[derive(Debug)]
pub struct CardinalityAwareRowConverter {
    fields: Vec<SortField>,
    inner: Option<RowConverter>,
    done: bool,
}

impl CardinalityAwareRowConverter {
    pub fn new(fields: Vec<SortField>) -> Result<Self, ArrowError> {
        Ok(Self {
            fields: fields.clone(),
            inner: None,
            done: false,
        })
    }
    
    pub fn size(&self) -> usize {
        match &self.inner {
            Some(inner) => inner.size(),
            None => 0,
        }
    }

    pub fn convert_rows(&self, rows: &Rows) -> Result<Vec<ArrayRef>, ArrowError> {
        self.inner.as_ref().unwrap().convert_rows(rows)
    }

    pub fn convert_columns(
        &mut self,
        columns: &[ArrayRef]) -> Result<Rows, ArrowError> {
        
        if !self.done {
            for (i, col) in columns.iter().enumerate() {
                if let DataType::Dictionary(k, _) = col.data_type() {
                    let cardinality = match k.as_ref() {
                        DataType::Int8 => downcast_dict!(col, Int32Type).values().len(),
                        DataType::Int16 => downcast_dict!(col, Int32Type).values().len(),
                        DataType::Int32 => downcast_dict!(col, Int32Type).values().len(),
                        DataType::Int64 => downcast_dict!(col, Int64Type).values().len(),
                        DataType::UInt16 => downcast_dict!(col, UInt16Type).values().len(),
                        DataType::UInt32 => downcast_dict!(col, UInt32Type).values().len(),
                        DataType::UInt64 => downcast_dict!(col, UInt64Type).values().len(),
                        _ => unreachable!(),
                    };

                    if cardinality >= LOW_CARDINALITY_THRESHOLD {
                        self.fields[i] = self.fields[i].clone().preserve_dictionaries(false);
                    }
                }
            }
            self.inner = Some(RowConverter::new(self.fields.clone())?);
            self.done = true;
        }
        println!("convert_columns");
        self.inner.as_mut().unwrap().convert_columns(columns)
    }
}
