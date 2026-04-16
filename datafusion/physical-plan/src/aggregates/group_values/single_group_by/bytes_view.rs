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
use datafusion_expr::EmitTo;
use datafusion_physical_expr::binary_map::OutputType;
use datafusion_physical_expr_common::binary_view_map::{
    ArrowBytesViewMap, ArrowBytesViewSet,
};
use std::mem::size_of;

enum GroupValuesBytesViewState {
    GroupIds {
        map: ArrowBytesViewMap<usize>,
        num_groups: usize,
    },
    DistinctOnly(ArrowBytesViewSet),
}

/// A [`GroupValues`] storing single column of Utf8View/BinaryView values
///
/// This specialization is significantly faster than using the more general
/// purpose `Row`s format
pub struct GroupValuesBytesView {
    output_type: OutputType,
    state: GroupValuesBytesViewState,
}

impl GroupValuesBytesView {
    pub fn new(output_type: OutputType, track_group_ids: bool) -> Self {
        let state = if track_group_ids {
            GroupValuesBytesViewState::GroupIds {
                map: ArrowBytesViewMap::new(output_type),
                num_groups: 0,
            }
        } else {
            GroupValuesBytesViewState::DistinctOnly(ArrowBytesViewSet::new(output_type))
        };

        Self { output_type, state }
    }

    fn ensure_group_id_tracking(&mut self) {
        if matches!(self.state, GroupValuesBytesViewState::GroupIds { .. }) {
            return;
        }

        let GroupValuesBytesViewState::DistinctOnly(set) = &mut self.state else {
            unreachable!();
        };
        let contents = set.take().into_state();
        let mut map = ArrowBytesViewMap::new(self.output_type);
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
        self.state = GroupValuesBytesViewState::GroupIds { map, num_groups };
    }

    fn emit_group_ids(
        map: &mut ArrowBytesViewMap<usize>,
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

    fn emit_distinct_only(set: &mut ArrowBytesViewSet, emit_to: EmitTo) -> ArrayRef {
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

impl GroupValues for GroupValuesBytesView {
    fn intern(
        &mut self,
        cols: &[ArrayRef],
        groups: &mut Vec<usize>,
    ) -> datafusion_common::Result<()> {
        self.ensure_group_id_tracking();
        assert_eq!(cols.len(), 1);

        // look up / add entries in the table
        let arr = &cols[0];
        let GroupValuesBytesViewState::GroupIds { map, num_groups } = &mut self.state
        else {
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

    fn intern_no_group_ids(
        &mut self,
        cols: &[ArrayRef],
    ) -> datafusion_common::Result<()> {
        assert_eq!(cols.len(), 1);

        let arr = &cols[0];
        match &mut self.state {
            GroupValuesBytesViewState::GroupIds { map, num_groups } => map.insert_if_new(
                arr,
                |_value| {
                    let group_idx = *num_groups;
                    *num_groups += 1;
                    group_idx
                },
                |_group_idx| {},
            ),
            GroupValuesBytesViewState::DistinctOnly(set) => set.insert(arr),
        }

        Ok(())
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + match &self.state {
                GroupValuesBytesViewState::GroupIds { map, .. } => map.size(),
                GroupValuesBytesViewState::DistinctOnly(set) => set.size(),
            }
    }

    fn is_empty(&self) -> bool {
        match &self.state {
            GroupValuesBytesViewState::GroupIds { num_groups, .. } => *num_groups == 0,
            GroupValuesBytesViewState::DistinctOnly(set) => set.is_empty(),
        }
    }

    fn len(&self) -> usize {
        match &self.state {
            GroupValuesBytesViewState::GroupIds { num_groups, .. } => *num_groups,
            GroupValuesBytesViewState::DistinctOnly(set) => set.len(),
        }
    }

    fn emit(&mut self, emit_to: EmitTo) -> datafusion_common::Result<Vec<ArrayRef>> {
        let group_values = match &mut self.state {
            GroupValuesBytesViewState::GroupIds { map, num_groups } => {
                Self::emit_group_ids(map, num_groups, emit_to)
            }
            GroupValuesBytesViewState::DistinctOnly(set) => {
                Self::emit_distinct_only(set, emit_to)
            }
        };

        Ok(vec![group_values])
    }

    fn clear_shrink(&mut self, _num_rows: usize) {
        match &mut self.state {
            GroupValuesBytesViewState::GroupIds { map, num_groups } => {
                *num_groups = 0;
                map.take();
            }
            GroupValuesBytesViewState::DistinctOnly(set) => {
                set.take();
            }
        }
    }
}
