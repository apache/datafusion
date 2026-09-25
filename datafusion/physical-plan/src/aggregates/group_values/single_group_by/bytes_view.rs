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
use arrow::array::{Array, ArrayRef};
use datafusion_expr::{EmitTo, GroupSelection};
use datafusion_physical_expr::binary_map::OutputType;
use datafusion_physical_expr_common::binary_view_map::{
    ArrowBytesViewMap, INITIAL_MAP_CAPACITY,
};
use std::mem::size_of;

/// A [`GroupValues`] storing single column of Utf8View/BinaryView values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesBytesView {
    /// Map string/binary values to group index
    map: ArrowBytesViewMap<usize>,
    /// The total number of groups so far (used to assign group_index)
    num_groups: usize,
}

impl GroupValuesBytesView {
    pub fn new(output_type: OutputType) -> Self {
        Self {
            // One map holds every group value for the whole query, so it is
            // worth pre-allocating the hash table.
            map: ArrowBytesViewMap::with_capacity(output_type, INITIAL_MAP_CAPACITY),
            num_groups: 0,
        }
    }
}

impl GroupValues for GroupValuesBytesView {
    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> datafusion_common::Result<()> {
        assert_eq!(cols.len(), 1);

        // look up / add entries in the table
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
        self.map.size() + size_of::<Self>()
    }

    fn is_empty(&self) -> bool {
        self.num_groups == 0
    }

    fn len(&self) -> usize {
        self.num_groups
    }

    fn emit(&mut self, emit_to: EmitTo) -> datafusion_common::Result<Vec<ArrayRef>> {
        // Reset the map to default, and convert it into a single array
        let map_contents = self.map.take().into_state();

        let group_values = match emit_to {
            EmitTo::All => {
                self.num_groups -= map_contents.len();
                map_contents
            }
            EmitTo::First(n) if n == self.len() => {
                self.num_groups -= map_contents.len();
                map_contents
            }
            EmitTo::First(n) => {
                // if we only wanted to take the first n, insert the rest back
                // into the map we could potentially avoid this reallocation, at
                // the expense of much more complex code.
                // see https://github.com/apache/datafusion/issues/9195
                let emit_group_values = map_contents.slice(0, n);
                let remaining_group_values =
                    map_contents.slice(n, map_contents.len() - n);

                self.num_groups = 0;
                let mut group_indexes = vec![];
                self.intern(&[remaining_group_values], &mut group_indexes)?;

                // Verify that the group indexes were assigned in the correct order
                assert_eq!(0, group_indexes[0]);

                emit_group_values
            }
        };

        Ok(vec![group_values])
    }

    fn values_preserving(
        &mut self,
        selection: GroupSelection<'_>,
    ) -> datafusion_common::Result<Vec<ArrayRef>> {
        selection.validate_num_groups(self.len())?;
        Ok(vec![self.map.keys(selection.iter())?])
    }

    fn supports_values_preserving(&self) -> bool {
        true
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        // Callers use this to hand memory back before spilling or sorting, so
        // release the map's allocations rather than restoring the warm up
        // capacity that `take` keeps for the emit path.
        self.map.clear_and_release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::array::StringViewArray;

    /// `clear_shrink` is how the aggregate stream hands memory back before it
    /// spills and before the spilled batch is sorted, so the memory it releases
    /// has to actually show up in the size it reports afterwards.
    #[test]
    fn clear_shrink_releases_the_reported_memory() {
        let mut group_values = GroupValuesBytesView::new(OutputType::Utf8View);
        let empty = size_of::<GroupValuesBytesView>();

        // The hash table is pre-allocated at construction, so the map is
        // already well above its own struct size before a single row is
        // interned.
        let warm_size = group_values.size();
        assert!(
            warm_size > empty + INITIAL_MAP_CAPACITY,
            "expected the pre-allocated map to report more than {} bytes, got {warm_size}",
            empty + INITIAL_MAP_CAPACITY
        );

        let values: ArrayRef = Arc::new(StringViewArray::from_iter_values(
            (0..1_000).map(|i| format!("group value number {i}")),
        ));
        let mut groups = vec![];
        group_values
            .intern(&[Arc::clone(&values)], &mut groups)
            .unwrap();
        let populated_size = group_values.size();
        assert!(populated_size > warm_size);

        group_values.clear_shrink(0);

        // Everything the map held is gone: what remains is the struct itself.
        let released_size = group_values.size();
        assert!(
            released_size < empty + 128,
            "expected clear_shrink to release the map, got {released_size} with a struct size of {empty}"
        );
        assert!(
            released_size * 10 < populated_size,
            "expected {released_size} to be far below {populated_size}"
        );

        // The map still works, and warms back up on the next emit.
        group_values.intern(&[values], &mut groups).unwrap();
        assert!(group_values.size() > released_size);
        group_values.emit(EmitTo::All).unwrap();
        assert!(group_values.size() > empty + INITIAL_MAP_CAPACITY);
    }
}
