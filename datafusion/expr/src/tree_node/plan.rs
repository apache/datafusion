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
use datafusion_common::tree_node::{Recursion, TreeNodeVisitor};
use datafusion_common::{tree_node::TreeNode, DataFusionError, Result};

impl TreeNode for LogicalPlan {
    fn get_children(&self) -> Vec<Self> {
        self.inputs().into_iter().cloned().collect::<Vec<_>>()
    }

    /// Compared to the default implementation, we need to invoke [`collect_subqueries`]
    /// before visiting its children
    fn collect<F>(&self, op: &mut F) -> Result<()>
    where
        F: FnMut(&Self) -> Result<Recursion>,
    {
        match op(self)? {
            Recursion::Continue => {}
            // If the recursion should stop, do not visit children
            Recursion::Stop => return Ok(()),
            r => {
                return Err(DataFusionError::Execution(format!(
                    "Recursion {r:?} is not supported for collect"
                )))
            }
        };

        self.collect_subqueries(op)?;

        for child in self.get_children() {
            child.collect(op)?;
        }

        Ok(())
    }

    /// To use, define a struct that implements the trait [`TreeNodeVisitor`] and then invoke
    /// [`LogicalPlan::visit`].
    ///
    /// For example, for a logical plan like:
    ///
    /// ```text
    /// Projection: id
    ///    Filter: state Eq Utf8(\"CO\")\
    ///       CsvScan: employee.csv projection=Some([0, 3])";
    /// ```
    ///
    /// The sequence of visit operations would be:
    /// ```text
    /// visitor.pre_visit(Projection)
    /// visitor.pre_visit(Filter)
    /// visitor.pre_visit(CsvScan)
    /// visitor.post_visit(CsvScan)
    /// visitor.post_visit(Filter)
    /// visitor.post_visit(Projection)
    /// ```
    ///
    /// Compared to the default implementation, we need to invoke [`visit_subqueries`]
    /// before visiting its children
    fn visit<V: TreeNodeVisitor<N = Self>>(&self, visitor: &mut V) -> Result<Recursion> {
        match visitor.pre_visit(self)? {
            Recursion::Continue => {}
            // If the recursion should stop, do not visit children
            Recursion::Stop => return Ok(Recursion::Stop),
            r => {
                return Err(DataFusionError::Execution(format!(
                    "Recursion {r:?} is not supported for collect_using"
                )))
            }
        };

        // Now visit any subqueries in expressions
        self.visit_subqueries(visitor)?;

        for child in self.get_children() {
            match child.visit(visitor)? {
                Recursion::Continue => {}
                // If the recursion should stop, do not visit children
                Recursion::Stop => return Ok(Recursion::Stop),
                r => {
                    return Err(DataFusionError::Execution(format!(
                        "Recursion {r:?} is not supported for collect_using"
                    )))
                }
            }
        }

        visitor.post_visit(self)
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
