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

//! ExecutionPlan visitor

use crate::physical_plan::tree_node::{
    TreeNodeVisitable, TreeNodeVisitor, VisitRecursion,
};
use crate::physical_plan::ExecutionPlan;
use std::sync::Arc;

impl TreeNodeVisitable for Arc<dyn ExecutionPlan> {
    /// Performs a depth first walk of a node and
    /// its children, see [`TreeNodeVisitor`] for more details
    fn accept<V: TreeNodeVisitor<N = Arc<dyn ExecutionPlan>>>(
        &self,
        visitor: V,
    ) -> datafusion_common::Result<V> {
        let mut visitor = match visitor.pre_visit(self)? {
            VisitRecursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            VisitRecursion::Stop(visitor) => return Ok(visitor),
        };

        for child in self.children() {
            visitor = child.accept(visitor)?;
        }

        visitor.post_visit(self)
    }
}
