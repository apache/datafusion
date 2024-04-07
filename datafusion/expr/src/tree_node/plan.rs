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

use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion,
};
use datafusion_common::Result;

impl TreeNode for LogicalPlan {
    fn apply_children<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.inputs().into_iter().apply_until_stop(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let new_children = self
            .inputs()
            .into_iter()
            .cloned()
            .map_until_stop_and_collect(f)?;
        // Propagate up `new_children.transformed` and `new_children.tnr`
        // along with the node containing transformed children.
        if new_children.transformed {
            new_children.map_data(|new_children| {
                self.with_new_exprs(self.expressions(), new_children)
            })
        } else {
            Ok(new_children.update_data(|_| self))
        }
    }
}
