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

use crate::aggregates::group_values::GroupValues;

use arrow::array::{
    ArrayRef, AsArray as _, BooleanArray, BooleanBufferBuilder, NullBufferBuilder,
};
use datafusion_common::Result;
use datafusion_expr::EmitTo;
use std::{mem::size_of, sync::Arc};

#[derive(Debug)]
pub struct GroupValuesBoolean {
    false_group: Option<usize>,
    true_group: Option<usize>,
    null_group: Option<usize>,
}

impl GroupValuesBoolean {
    pub fn new() -> Self {
        Self {
            false_group: None,
            true_group: None,
            null_group: None,
        }
    }
}

impl GroupValues for GroupValuesBoolean {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        let array = cols[0].as_boolean();
        groups.clear();

        for value in array.iter() {
            let index = match value {
                Some(false) => {
                    if let Some(index) = self.false_group {
                        index
                    } else {
                        let index = self.len();
                        self.false_group = Some(index);
                        index
                    }
                }
                Some(true) => {
                    if let Some(index) = self.true_group {
                        index
                    } else {
                        let index = self.len();
                        self.true_group = Some(index);
                        index
                    }
                }
                None => {
                    if let Some(index) = self.null_group {
                        index
                    } else {
                        let index = self.len();
                        self.null_group = Some(index);
                        index
                    }
                }
            };

            groups.push(index);
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.false_group.is_some() as usize
            + self.true_group.is_some() as usize
            + self.null_group.is_some() as usize
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let len = self.len();
        let mut builder = BooleanBufferBuilder::new(len);
        let emit_count = match emit_to {
            EmitTo::All => len,
            EmitTo::First(n) => n,
        };
        builder.append_n(emit_count, false);
        if let Some(idx) = self.true_group.as_mut() {
            if *idx < emit_count {
                builder.set_bit(*idx, true);
                self.true_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        if let Some(idx) = self.false_group.as_mut() {
            if *idx < emit_count {
                // already false, no need to set
                self.false_group = None;
            } else {
                *idx -= emit_count;
            }
        }

        let values = builder.finish();

        let nulls = if let Some(idx) = self.null_group.as_mut() {
            if *idx < emit_count {
                let mut buffer = NullBufferBuilder::new(len);
                buffer.append_n_non_nulls(*idx);
                buffer.append_null();
                buffer.append_n_non_nulls(emit_count - *idx - 1);

                self.null_group = None;
                Some(buffer.finish().unwrap())
            } else {
                *idx -= emit_count;
                None
            }
        } else {
            None
        };

        Ok(vec![Arc::new(BooleanArray::new(values, nulls)) as _])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        self.false_group = None;
        self.true_group = None;
        self.null_group = None;
    }
}
