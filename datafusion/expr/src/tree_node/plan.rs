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

//! Tree node implementation for logical plan

use crate::LogicalPlan;
use datafusion_common::{tree_node::TreeNode, Result};

impl TreeNode for LogicalPlan {
    fn get_children(&self) -> Vec<Self> {
        self.inputs().into_iter().cloned().collect::<Vec<_>>()
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.get_children();
        if !children.is_empty() {
            let new_children: Result<Vec<_>> =
                children.into_iter().map(transform).collect();
            self.with_new_inputs(new_children?.as_slice())
        } else {
            Ok(self)
        }
    }
}
