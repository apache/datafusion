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

use std::mem::size_of;

use crate::aggregates::group_values::GroupValues;

use arrow::array::{Array, ArrayRef, OffsetSizeTrait};
use datafusion_common::Result;
use datafusion_expr::EmitTo;
use datafusion_physical_expr_common::binary_map::{
    ArrowBytesMap, ArrowBytesSet, OutputType,
};

enum GroupValuesBytesState<O: OffsetSizeTrait> {
    GroupIds {
        map: ArrowBytesMap<O, usize>,
        num_groups: usize,
    },
    DistinctOnly(ArrowBytesSet<O>),
}

/// A [`GroupValues`] storing single column of Utf8/LargeUtf8/Binary/LargeBinary values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesBytes<O: OffsetSizeTrait> {
    output_type: OutputType,
    state: GroupValuesBytesState<O>,
}

impl<O: OffsetSizeTrait> GroupValuesBytes<O> {
    pub fn new(output_type: OutputType, track_group_ids: bool) -> Self {
        let state = if track_group_ids {
            GroupValuesBytesState::GroupIds {
                map: ArrowBytesMap::new(output_type),
                num_groups: 0,
            }
        } else {
            GroupValuesBytesState::DistinctOnly(ArrowBytesSet::new(output_type))
        };

        Self { output_type, state }
    }

    fn ensure_group_id_tracking(&mut self) {
        if matches!(self.state, GroupValuesBytesState::GroupIds { .. }) {
            return;
        }

        let GroupValuesBytesState::DistinctOnly(set) = &mut self.state else {
            unreachable!();
        };
        let contents = set.take().into_state();
        let mut map = ArrowBytesMap::new(self.output_type);
        let mut num_groups = 0;
        map.insert_if_new(
            &contents,
            |_value| {
                let group_idx = num_groups;
                num_groups += 1;
                group_idx
            },
            |_group_idx| {},
        );
        self.state = GroupValuesBytesState::GroupIds { map, num_groups };
    }

    fn emit_group_ids(
        map: &mut ArrowBytesMap<O, usize>,
        num_groups: &mut usize,
        emit_to: EmitTo,
    ) -> ArrayRef {
        let map_contents = map.take().into_state();

        match emit_to {
            EmitTo::All => {
                *num_groups -= map_contents.len();
                map_contents
            }
            EmitTo::First(n) if n == *num_groups => {
                *num_groups -= map_contents.len();
                map_contents
            }
            EmitTo::First(n) => {
                let emit_group_values = map_contents.slice(0, n);
                let remaining_group_values =
                    map_contents.slice(n, map_contents.len() - n);

                *num_groups = 0;
                map.insert_if_new(
                    &remaining_group_values,
                    |_value| {
                        let group_idx = *num_groups;
                        *num_groups += 1;
                        group_idx
                    },
                    |_group_idx| {},
                );

                emit_group_values
            }
        }
    }

    fn emit_distinct_only(set: &mut ArrowBytesSet<O>, emit_to: EmitTo) -> ArrayRef {
        let set_contents = set.take().into_state();
        match emit_to {
            EmitTo::All => set_contents,
            EmitTo::First(n) if n == set_contents.len() => set_contents,
            EmitTo::First(n) => {
                let emit_group_values = set_contents.slice(0, n);
                let remaining_group_values =
                    set_contents.slice(n, set_contents.len() - n);
                set.insert(&remaining_group_values);
                emit_group_values
            }
        }
    }
}

impl<O: OffsetSizeTrait> GroupValues for GroupValuesBytes<O> {
    fn intern(&mut self, cols: &[ArrayRef], groups: &mut Vec<usize>) -> Result<()> {
        self.ensure_group_id_tracking();
        assert_eq!(cols.len(), 1);

        // look up / add entries in the table
        let arr = &cols[0];
        let GroupValuesBytesState::GroupIds { map, num_groups } = &mut self.state else {
            unreachable!();
        };

        groups.clear();
        map.insert_if_new(
            arr,
            |_value| {
                let group_idx = *num_groups;
                *num_groups += 1;
                group_idx
            },
            |group_idx| groups.push(group_idx),
        );

        // ensure we assigned a group to for each row
        assert_eq!(groups.len(), arr.len());
        Ok(())
    }

    fn intern_no_group_ids(&mut self, cols: &[ArrayRef]) -> Result<()> {
        assert_eq!(cols.len(), 1);

        let arr = &cols[0];
        match &mut self.state {
            GroupValuesBytesState::GroupIds { map, num_groups } => map.insert_if_new(
                arr,
                |_value| {
                    let group_idx = *num_groups;
                    *num_groups += 1;
                    group_idx
                },
                |_group_idx| {},
            ),
            GroupValuesBytesState::DistinctOnly(set) => set.insert(arr),
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + match &self.state {
                GroupValuesBytesState::GroupIds { map, .. } => map.size(),
                GroupValuesBytesState::DistinctOnly(set) => set.size(),
            }
    }

    fn is_empty(&self) -> bool {
        match &self.state {
            GroupValuesBytesState::GroupIds { num_groups, .. } => *num_groups == 0,
            GroupValuesBytesState::DistinctOnly(set) => set.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match &self.state {
            GroupValuesBytesState::GroupIds { num_groups, .. } => *num_groups,
            GroupValuesBytesState::DistinctOnly(set) => set.len(),
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let group_values = match &mut self.state {
            GroupValuesBytesState::GroupIds { map, num_groups } => {
                Self::emit_group_ids(map, num_groups, emit_to)
            }
            GroupValuesBytesState::DistinctOnly(set) => {
                Self::emit_distinct_only(set, emit_to)
            }
        };

        Ok(vec![group_values])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        match &mut self.state {
            GroupValuesBytesState::GroupIds { map, num_groups } => {
                *num_groups = 0;
                map.take();
            }
            GroupValuesBytesState::DistinctOnly(set) => {
                set.take();
            }
        }
    }
}
