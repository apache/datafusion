use std::sync::Arc;
use arrow::array::{Array, ArrayRef, AsArray, BinaryViewBuilder, BooleanArray};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use datafusion_common::{DataFusionError, Result};

pub struct StringGroupsAccumulatorMin {
    states: Vec<String>,
}

impl Default for StringGroupsAccumulatorMin {
    fn default() -> Self {
        Self::new()
    }
}

impl StringGroupsAccumulatorMin {
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
        }
    }
}

impl GroupsAccumulator for StringGroupsAccumulatorMin {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        // Ensure that self.states has capacity for total_num_groups
        if self.states.len() < total_num_groups {
            self.states.resize(total_num_groups, String::new());
        }

        // Assume values has one element (the input column)
        let input_array = &values[0];

        // Iterate over rows
        for (i, &group_index) in group_indices.iter().enumerate() {
            // Check filter
            if let Some(filter) = opt_filter {
                if !filter.value(i) {
                    continue;
                }
            }

            // Skip null values
            if input_array.is_null(i) {
                continue;
            }

            // Get the binary value at index i
            let value = input_array.as_binary_view().value(i);

            // Convert binary data to a string (assuming UTF-8 encoding)
            let value_str = std::str::from_utf8(value).map_err(|e| {
                DataFusionError::Execution(format!("Invalid UTF-8 sequence: {}", e))
            })?;

            if self.states[group_index].is_empty() {
                self.states[group_index] = value_str.to_string();
            } else {
                let curr_value_bytes = self.states[group_index].as_bytes();
                if value < curr_value_bytes {
                    self.states[group_index] = value_str.parse().unwrap();
                }
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let num_groups = match emit_to {
            EmitTo::All => self.states.len(),
            EmitTo::First(n) => std::cmp::min(n, self.states.len()),
        };

        let mut builder = BinaryViewBuilder::new();

        for i in 0..num_groups {
            let value = &self.states[i];
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_bytes());
            }
        }

        let array = Arc::new(builder.finish()) as ArrayRef;

        match emit_to {
            EmitTo::All => {
                self.states.clear();
            }
            EmitTo::First(n) => {
                self.states.drain(0..n);
            }
        }
        Ok(array)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let num_groups = match emit_to {
            EmitTo::All => self.states.len(),
            EmitTo::First(n) => std::cmp::min(n, self.states.len()),
        };

        let mut builder = BinaryViewBuilder::new();

        for i in 0..num_groups {
            let value = &self.states[i];
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_bytes());
            }
        }

        let array = Arc::new(builder.finish()) as ArrayRef;

        match emit_to {
            EmitTo::All => {
                self.states.clear();
            }
            EmitTo::First(n) => {
                self.states.drain(0..n);
            }
        }
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
            if let Some(filter) = opt_filter {
                if !filter.value(i) {
                    continue;
                }
            }

            if input_array.is_null(i) {
                continue;
            }

            let value = input_array.as_binary_view().value(i);

            let value_str = std::str::from_utf8(value).map_err(|e| {
                DataFusionError::Execution(format!("Invalid UTF-8 sequence: {}", e))
            })?;

            if self.states[group_index].is_empty() {
                self.states[group_index] = value_str.to_string();
            } else {
                let curr_value_bytes = self.states[group_index].as_bytes();
                if value < curr_value_bytes {
                    self.states[group_index] = value_str.parse().unwrap();
                }
            }
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
            return Ok(vec![Arc::<dyn arrow::array::Array>::clone(&input_array)]);
        }

        let filter = opt_filter.unwrap();


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

        let array = Arc::new(builder.finish()) as ArrayRef;
        Ok(vec![array])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.states.iter().map(|s| s.len()).sum()
    }
}
