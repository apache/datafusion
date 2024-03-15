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
use crate::common::transpose;
use arrow::compute::{cast, SortColumn};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{DataType, SchemaRef};
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::LexOrdering;

/// A [`GroupValues`] making use of order
pub struct FullOrderedGroupValues {
    /// The output schema
    schema: SchemaRef,

    /// The actual group by values, stored in arrow [`Row`] format.
    /// `group_values[i]` holds the group value for group_index `i`.
    ///
    /// The row format is used to compare group keys quickly and store
    /// them efficiently in memory. Quick comparison is especially
    /// important for multi-column group keys.
    ///
    /// [`Row`]: arrow::row::Row
    group_values: Option<Vec<Vec<ScalarValue>>>,
    sort_exprs: LexOrdering,
}

impl FullOrderedGroupValues {
    pub fn try_new(schema: SchemaRef, sort_exprs: LexOrdering) -> Result<Self> {
        Ok(Self {
            schema,
            group_values: None,
            sort_exprs,
        })
    }
}

impl GroupValues for FullOrderedGroupValues {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        // Convert the group keys into the row format
        // Avoid reallocation when https://github.com/apache/arrow-rs/issues/4479 is available
        assert!(!cols.is_empty());

        // tracks to which group each of the input rows belongs
        groups.clear();

        let n_rows = cols[0].len();
        if n_rows == 0 {
            return Ok(());
        }

        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => vec![],
        };

        assert_eq!(cols.len(), self.sort_exprs.len());
        // TODO: May need to change iteration order
        let sort_columns = cols
            .iter()
            .zip(&self.sort_exprs)
            .map(|(col, sort_expr)| SortColumn {
                values: col.clone(),
                options: Some(sort_expr.options),
            })
            .collect::<Vec<_>>();
        let ranges = evaluate_partition_ranges(n_rows, &sort_columns)?;
        assert!(!ranges.is_empty());
        let first_section = &ranges[0];
        let row = get_row_at_idx(cols, first_section.start)?;
        let mut should_insert = false;
        if let Some(last_group) = group_values.last() {
            if !row.eq(last_group) {
                // Group by value changed
                should_insert = true;
            }
        } else {
            should_insert = true;
        }
        if should_insert {
            group_values.push(row);
        }
        groups.extend(vec![
            group_values.len() - 1;
            first_section.end - first_section.start
        ]);
        for range in ranges[1..].iter() {
            let row = get_row_at_idx(cols, range.start)?;
            group_values.push(row);
            groups.extend(vec![group_values.len() - 1; range.end - range.start]);
        }
        assert_eq!(groups.len(), n_rows);

        self.group_values = Some(group_values);

        Ok(())
    }

    fn size(&self) -> usize {
        let group_values_size: usize = self
            .group_values
            .as_ref()
            .map(|items| items.iter().map(ScalarValue::size_of_vec).sum())
            .unwrap_or(0);
        group_values_size
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn len(&self) -> usize {
        self.group_values
            .as_ref()
            .map(|items| items.len())
            .unwrap_or(0)
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let mut group_values = match self.group_values.take() {
            Some(group_values) => group_values,
            None => vec![],
        };

        let mut output: Vec<ArrayRef> = match emit_to {
            EmitTo::All => {
                let res = transpose(group_values);
                self.group_values = None;
                res.into_iter()
                    .map(ScalarValue::iter_to_array)
                    .collect::<Result<Vec<_>>>()?
            }
            EmitTo::First(n) => {
                let first_n_section = group_values.drain(0..n).collect();
                self.group_values = Some(group_values);
                let res = transpose(first_n_section);
                res.into_iter()
                    .map(ScalarValue::iter_to_array)
                    .collect::<Result<Vec<_>>>()?
            }
        };

        // TODO: Materialize dictionaries in group keys (#7647)
        for (field, array) in self.schema.fields.iter().zip(&mut output) {
            let expected = field.data_type();
            if let DataType::Dictionary(_, v) = expected {
                let actual = array.data_type();
                if v.as_ref() != actual {
                    return Err(DataFusionError::Internal(format!(
                        "Converted group rows expected dictionary of {v} got {actual}"
                    )));
                }
                *array = cast(array.as_ref(), expected)?;
            }
        }

        Ok(output)
    }

    fn clear_shrink(&mut self, _batch: &RecordBatch) {
        if let Some(group_values) = &mut self.group_values {
            group_values.clear();
        }
    }
}
