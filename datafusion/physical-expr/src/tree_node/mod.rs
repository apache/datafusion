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
//!
//! It's a duplication of the one in the crate `datafusion`.
//! In the future, if the Orphan rule is relaxed for Arc<dyn T>, these duplicated codes can be removed.

pub mod physical_expr;

use datafusion_common::{DataFusionError, Result};

/// Trait for tree node. It can be [`ExecutionPlan`], [`PhysicalExpr`], [`LogicalExpr`], etc.
pub trait TreeNode: Clone {
    /// Return the children of this tree node
    fn get_children(&self) -> Vec<Self>;

    /// Use pre-order to iterate the node on the tree so that we can stop fast for some cases.
    ///
    /// `op` can be used to collect some info from the tree node.
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

        for child in self.get_children() {
            child.collect(op)?;
        }

        Ok(())
    }

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
    fn transform_using<R: TreeNodeRewriter<N = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Self> {
        let need_mutate = match rewriter.pre_visit(&self)? {
            Recursion::Mutate => return rewriter.mutate(self),
            Recursion::Stop => return Ok(self),
            Recursion::Continue => true,
            Recursion::Skip => false,
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

/// Trait for potentially recursively transform an [`TreeNode`] node
/// tree. When passed to `TreeNode::transform_using`, `TreeNodeRewriter::mutate` is
/// invoked recursively on all nodes of a tree.
pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type N: TreeNode;

    /// Invoked before (Preorder) any children of `node` are rewritten /
    /// visited. Default implementation returns `Ok(RewriteRecursion::Continue)`
    fn pre_visit(&mut self, _node: &Self::N) -> Result<Recursion> {
        Ok(Recursion::Continue)
    }

    /// Invoked after (Postorder) all children of `node` have been mutated and
    /// returns a potentially modified node.
    fn mutate(&mut self, node: Self::N) -> Result<Self::N>;
}

/// Controls how the [TreeNode] recursion should proceed.
#[derive(Debug)]
pub enum Recursion {
    /// Continue rewrite / visit this node tree.
    Continue,
    /// Call 'op' immediately and return.
    Mutate,
    /// Do not rewrite / visit the children of this node.
    Stop,
    /// Keep recursive but skip apply op on this node
    Skip,
}
