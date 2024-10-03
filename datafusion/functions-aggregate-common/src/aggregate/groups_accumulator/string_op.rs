// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
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
use arrow::array::{Array, ArrayRef, AsArray, BinaryBuilder, BinaryViewBuilder, BooleanArray}; 
use datafusion_common::{DataFusionError, Result};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use std::sync::Arc;

pub struct StringGroupsAccumulator<F, const VIEW: bool> {
    states: Vec<String>,
    fun: F
}

impl<F, const VIEW: bool>  StringGroupsAccumulator<F, VIEW> 
where 
    F: Fn(&[u8], &[u8]) -> bool + Send + Sync,
{
    pub fn new(s_fn: F) -> Self {
        Self { 
            states: Vec::new(),
            fun: s_fn
        }
    }
}

impl<F, const VIEW: bool>  GroupsAccumulator for StringGroupsAccumulator<F, VIEW> 
where 
    F: Fn(&[u8], &[u8]) -> bool + Send + Sync,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if self.states.len() < total_num_groups {
            self.states.resize(total_num_groups, String::new());
        }

        let input_array = &values[0];

        for (i, &group_index) in group_indices.iter().enumerate() {
            invoke_accumulator::<F, VIEW>(self, input_array, opt_filter, group_index, i)
        }
        
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let states = emit_to.take_needed(&mut self.states);

        let array = if VIEW {
            let mut builder = BinaryViewBuilder::new();

            for value in states {
                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value.as_bytes());
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        } else {
            let mut builder = BinaryBuilder::new();

            for value in states {
                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value.as_bytes());
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        };

        Ok(array)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let states = emit_to.take_needed(&mut self.states);

        let array = if VIEW {
            let mut builder = BinaryViewBuilder::new();

            for value in states {
                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value.as_bytes());
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        } else {
            let mut builder = BinaryBuilder::new();

            for value in states {
                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value.as_bytes());
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        };

        Ok(vec![array])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        if self.states.len() < total_num_groups {
            self.states.resize(total_num_groups, String::new());
        }

        let input_array = &values[0];

        for (i, &group_index) in group_indices.iter().enumerate() {
            invoke_accumulator::<F, VIEW>(self, input_array, opt_filter, group_index, i)
        }

        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let input_array = &values[0];

        if opt_filter.is_none() {
            return Ok(vec![Arc::<dyn Array>::clone(input_array)]);
        }

        let filter = opt_filter.unwrap();
        
        let array = if VIEW {
            let mut builder = BinaryViewBuilder::new();

            for i in 0..values.len() {
                let value = input_array.as_binary_view().value(i);

                if !filter.value(i) {
                    builder.append_null();
                    continue;
                }

                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value);
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        } else {
            let mut builder = BinaryBuilder::new();

            for i in 0..values.len() {
                let value = input_array.as_binary::<i32>().value(i);

                if !filter.value(i) {
                    builder.append_null();
                    continue;
                }

                if value.is_empty() {
                    builder.append_null();
                } else {
                    builder.append_value(value);
                }
            }

            Arc::new(builder.finish()) as ArrayRef
        };

        Ok(vec![array])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.states.iter().map(|s| s.len()).sum()
    }
}

fn invoke_accumulator<F, const VIEW: bool>(accumulator: &mut StringGroupsAccumulator<F, VIEW>, input_array: &ArrayRef, opt_filter: Option<&BooleanArray>, group_index: usize, i: usize) 
where
    F: Fn(&[u8], &[u8]) -> bool + Send + Sync
{
    if let Some(filter) = opt_filter {
        if !filter.value(i) {
            return
        }
    }
    if input_array.is_null(i) {
        return
    }
    
    let value: &[u8] = if VIEW {
        input_array.as_binary_view().value(i)
    } else {
        input_array.as_binary::<i32>().value(i)
    };

    let value_str = std::str::from_utf8(value).map_err(|e| {
        DataFusionError::Execution(format!(
            "could not build utf8 {}",
            e
        ))
    }).expect("failed to build utf8");

    if accumulator.states[group_index].is_empty() {
        accumulator.states[group_index] = value_str.to_string();
    } else {
        let curr_value_bytes = accumulator.states[group_index].as_bytes();
        if (accumulator.fun)(value, curr_value_bytes) {
            accumulator.states[group_index] = value_str.parse().unwrap();
        }
    }
}
