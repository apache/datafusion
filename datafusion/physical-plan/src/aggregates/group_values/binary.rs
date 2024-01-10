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
use arrow_array::{Array, ArrayRef, OffsetSizeTrait, RecordBatch};
use datafusion_expr::EmitTo;
use datafusion_physical_expr::string_map::ArrowStringMap;

/// A [`GroupValues`] storing single column of Utf8/LargeUtf8/Binary/LargeBinary values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesBinary<O: OffsetSizeTrait> {
    /// Map string values to group index
    map: ArrowStringMap<O, usize>,
    /// The total number of groups so far (used to assign group_index)
    num_groups: usize,
}

impl<O: OffsetSizeTrait> GroupValuesBinary<O> {
    pub fn new() -> Self {
        Self {
            map: ArrowStringMap::new(),
            num_groups: 0,
        }
    }
}

impl<O: OffsetSizeTrait> GroupValues for GroupValuesBinary<O> {
    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> datafusion_common::Result<()> {
        assert_eq!(cols.len(), 1);

        // Step 2: look up / add entries in the table
        let arr = &cols[0];

        groups.clear();
        self.map.insert_if_new(
            arr,
            // called for each new group
            |_value| {
                // assign new group index on each insert
                let group_idx = self.num_groups;
                self.num_groups += 1;
                group_idx
            },
            // called for each group
            |group_idx| {
                groups.push(group_idx);
            },
        );

        // ensure we assigned a group to for each row
        assert_eq!(groups.len(), arr.len());
        Ok(())
    }

    fn size(&self) -> usize {
        self.map.size() + std::mem::size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.num_groups == 0
    }

    fn len(&self) -> usize {
        self.num_groups
    }

    fn emit(&mut self, emit_to: EmitTo) -> datafusion_common::Result<Vec<ArrayRef>> {
        let group_values = match emit_to {
            EmitTo::All => {
                // Reset the map to default, and convert it into a single array
                std::mem::take(&mut self.map).into_state()
            }
            EmitTo::First(n) => {
                unsafe {
                    // safety: we know all values we are removing are in the first n groups
                    self.map.take_first_n(n, |group_idx| {
                        if *group_idx < n {
                            true
                        } else {
                            // group will remain, shift payload index down
                            *group_idx = group_idx.checked_sub(n).unwrap();
                            false
                        }
                    })
                }
            }
        };
        self.num_groups -= group_values.len();

        Ok(vec![group_values])
    }

    fn clear_shrink(&mut self, _batch: &RecordBatch) {
        // in theory we could potentially avoid this reallocation and clear the
        // contents of the maps, but for now we just restart from the beginning
        self.map = ArrowStringMap::new();
    }
}
