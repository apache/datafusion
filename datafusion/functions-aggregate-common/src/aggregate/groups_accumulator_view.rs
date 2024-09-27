use std::sync::Arc;
use arrow::array::{Array, ArrayRef, AsArray, BinaryViewArray, BinaryViewBuilder, BooleanArray};
use datafusion_expr_common::groups_accumulator::{EmitTo, GroupsAccumulator};
use datafusion_common::{DataFusionError, Result};

/// An accumulator that concatenates strings for each group.
pub struct GroupsAccumulatorMin {
    states: Vec<String>,
}

impl GroupsAccumulatorMin {
    /// Creates a new `StringGroupsAccumulator`.
    pub fn new() -> Self {
        Self {
            states: Vec::new(),
        }
    }
}

impl GroupsAccumulator for GroupsAccumulatorMin {
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

            if self.states[group_index].len() == 0 {
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

        // Build the output array
        for i in 0..num_groups {
            let value = &self.states[i];
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_bytes());
            }
        }

        let array = Arc::new(builder.finish()) as ArrayRef;

        // Handle internal state according to emit_to
        match emit_to {
            EmitTo::All => {
                // Reset the internal state
                self.states.clear();
            }
            EmitTo::First(n) => {
                // Remove the first n elements from self.states
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

        // Build the state array
        for i in 0..num_groups {
            let value = &self.states[i];
            if value.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(value.as_bytes());
            }
        }

        let array = Arc::new(builder.finish()) as ArrayRef;

        // Handle internal state according to emit_to
        match emit_to {
            EmitTo::All => {
                // Reset the internal state
                self.states.clear();
            }
            EmitTo::First(n) => {
                // Remove the first n elements from self.states
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
        // Ensure that self.states has capacity for total_num_groups
        if self.states.len() < total_num_groups {
            self.states.resize(total_num_groups, String::new());
        }

        // The values are the state arrays from other accumulators
        // We expect that values[0] is an ArrayRef containing the binary data
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

            // Update the state for the group
            if self.states[group_index].len() == 0 {
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
        // For each input value, produce an array representing the state
        let input_array = &values[0];

        // Downcast to BinaryViewArray
        let binary_array = input_array
            .as_any()
            .downcast_ref::<BinaryViewArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected BinaryViewArray".to_string()))?;

        if opt_filter.is_none() {
            // No filter, return the input array as is
            return Ok(vec![input_array.clone()]);
        }

        // Apply the filter
        let filter = opt_filter.unwrap();

        // Build a new array with filtered values
        let mut builder = BinaryViewBuilder::new();

        for i in 0..binary_array.len() {
            if !filter.value(i) {
                builder.append_null();
                continue;
            }

            if binary_array.is_null(i) {
                builder.append_null();
            } else {
                let value = binary_array.value(i);
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
        // Compute the total length of the strings in self.states
        self.states.iter().map(|s| s.len()).sum()
    }
}