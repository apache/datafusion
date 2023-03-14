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

pub mod rewritable;
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

/// Trait for types that can be visited by [`TreeNodeVisitor`]
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

/// Trait for marking tree node as rewritable
pub trait TreeNodeRewritable: Clone {
    /// Convenience utils for writing optimizers rule: recursively apply the given `op` to the node tree.
    /// When `op` does not apply to a given node, it is left unchanged.
    /// The default tree traversal direction is transform_up(Postorder Traversal).
    fn transform<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Option<Self>>,
    {
        self.transform_up(op)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Option<Self>>,
    {
        let node_cloned = self.clone();
        let after_op = match op(node_cloned)? {
            Some(value) => value,
            None => self,
        };
        after_op.map_children(|node| node.transform_down(op))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Option<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up(op))?;

        let after_op_children_clone = after_op_children.clone();
        let new_node = match op(after_op_children)? {
            Some(value) => value,
            None => after_op_children_clone,
        };
        Ok(new_node)
    }

    /// Transform the tree node using the given [TreeNodeRewriter]
    /// It performs a depth first walk of an node and its children.
    ///
    /// For an node tree such as
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order
    /// ```text
    /// pre_visit(ParentNode)
    /// pre_visit(ChildNode1)
    /// mutate(ChildNode1)
    /// pre_visit(ChildNode2)
    /// mutate(ChildNode2)
    /// mutate(ParentNode)
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`false`] is returned on a call to pre_visit, no
    /// children of that node are visited, nor is mutate
    /// called on that node
    ///
    fn transform_using<R: TreeNodeRewriter<Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Self> {
        let need_mutate = match rewriter.pre_visit(&self)? {
            RewriteRecursion::Mutate => return rewriter.mutate(self),
            RewriteRecursion::Stop => return Ok(self),
            RewriteRecursion::Continue => true,
            RewriteRecursion::Skip => false,
        };

        let after_op_children =
            self.map_children(|node| node.transform_using(rewriter))?;

        // now rewrite this node itself
        if need_mutate {
            rewriter.mutate(after_op_children)
        } else {
            Ok(after_op_children)
        }
    }

    /// Apply transform `F` to the node's children, the transform `F` might have a direction(Preorder or Postorder)
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>;
}

/// Trait for potentially recursively transform an [`TreeNodeRewritable`] node
/// tree. When passed to `TreeNodeRewritable::transform_using`, `TreeNodeRewriter::mutate` is
/// invoked recursively on all nodes of a tree.
pub trait TreeNodeRewriter<N: TreeNodeRewritable>: Sized {
    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, _node: &N) -> Result<RewriteRecursion> {
        Ok(RewriteRecursion::Continue)
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: N) -> Result<N>;
}

/// Controls how the [TreeNodeRewriter] recursion should proceed.
#[allow(dead_code)]
pub enum RewriteRecursion {
    /// Continue rewrite / visit this node tree.
    Continue,
    /// Call 'op' immediately and return.
    Mutate,
    /// Do not rewrite / visit the children of this node.
    Stop,
    /// Keep recursive but skip apply op on this node
    Skip,
}
