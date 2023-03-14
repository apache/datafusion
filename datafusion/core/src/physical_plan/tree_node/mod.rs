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

//! This module provides common traits for visiting or rewriting tree nodes easily.

pub mod visitable;

use datafusion_common::Result;

/// Implements the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively walking [`TreeNodeVisitable`]s.
///
/// [`TreeNodeVisitor`] allows keeping the algorithms
/// separate from the code to traverse the structure of the `TreeNodeVisitable`
/// tree and makes it easier to add new types of tree node and
/// algorithms by.
///
/// When passed to[`TreeNodeVisitable::accept`], [`TreeNodeVisitor::pre_visit`]
/// and [`TreeNodeVisitor::post_visit`] are invoked recursively
/// on an node tree.
///
/// If an [`Err`] result is returned, recursion is stopped
/// immediately.
///
/// If [`Recursion::Stop`] is returned on a call to pre_visit, no
/// children of that tree node are visited, nor is post_visit
/// called on that tree node
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type N: TreeNodeVisitable;

    /// Invoked before any children of `node` are visited.
    fn pre_visit(self, node: &Self::N) -> Result<VisitRecursion<Self>>;

    /// Invoked after all children of `node` are visited. Default
    /// implementation does nothing.
    fn post_visit(self, _node: &Self::N) -> Result<Self> {
        Ok(self)
    }
}

/// trait for types that can be visited by [`TreeNodeVisitor`]
pub trait TreeNodeVisitable: Sized {
    /// Return the children of this tree node
    fn get_children(&self) -> Vec<Self>;

    /// Accept a visitor, calling `visit` on all children of this
    fn accept<V: TreeNodeVisitor<N = Self>>(&self, visitor: V) -> Result<V> {
        let mut visitor = match visitor.pre_visit(self)? {
            VisitRecursion::Continue(visitor) => visitor,
            // If the recursion should stop, do not visit children
            VisitRecursion::Stop(visitor) => return Ok(visitor),
        };

        for child in self.get_children() {
            visitor = child.accept(visitor)?;
        }

        visitor.post_visit(self)
    }
}

/// Controls how the visitor recursion should proceed.
pub enum VisitRecursion<V: TreeNodeVisitor> {
    /// Attempt to visit all the children, recursively.
    Continue(V),
    /// Do not visit the children of this tree node, though the walk
    /// of parents of this tree node will not be affected
    Stop(V),
}
