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

use arrow::array::{Array, ArrayRef};
use datafusion_physical_expr::binary_map::OutputType;
use std::mem::size_of;
use datafusion_expr_common::groups_accumulator::BlocksIndex;
use datafusion_physical_expr_common::blocked_binary_view_map::BlockedArrowBytesViewMap;
use crate::aggregates_blocked::group_values::BlockedGroupValues;

/// A [`GroupValues`] storing single column of Utf8View/BinaryView values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesBytesView {
    /// Map string/binary values to their position, which is the group index
    map: BlockedArrowBytesViewMap<()>,
    /// The total number of groups so far (used to assign group_index)
    num_groups: usize,

    block_size: usize,
}

impl GroupValuesBytesView {
    pub fn new(output_type: OutputType, block_size: usize) -> Self {
        Self {
            map: BlockedArrowBytesViewMap::new(output_type, block_size),
            num_groups: 0,
            block_size,
        }
    }
}

impl BlockedGroupValues for GroupValuesBytesView {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<BlocksIndex>,
    ) -> datafusion_common::Result<()> {
        assert_eq!(cols.len(), 1);

        // look up / add entries in the table
        let arr = &cols[0];

        groups.clear();
        self.map.insert_if_new(
            arr,
            // called for each new group
            |_value| {
                self.num_groups += 1;
            },
            // called for each row with the position of its value, which stays correct
            // after blocks were emitted unlike a group index stored as payload
            |(), group_index| {
                groups.push(group_index);
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


    fn emit_all(&mut self) -> datafusion_common::Result<Vec<Vec<ArrayRef>>> {
        self.num_groups = 0;

        Ok(self.map.take_all().into_iter().map(|block| vec![block]).collect())
    }

    fn emit_first_n(&mut self, n: usize) -> datafusion_common::Result<Vec<ArrayRef>> {
        self.num_groups -= n;
        Ok(vec![self.map.take_n(n)])
    }
    fn emit_block(&mut self) -> datafusion_common::Result<Option<Vec<ArrayRef>>> {
        // Reset the map to default, and convert it into a single array
        let Some(map_contents) = self.map.take_block() else {
            return Ok(None)
        };

        self.num_groups -= map_contents.len();

        Ok(Some(vec![map_contents]))
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        // in theory we could potentially avoid this reallocation and clear the
        // contents of the maps, but for now we just reset the map from the beginning
        self.map.take();
    }
}
