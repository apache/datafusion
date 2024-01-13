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

//! This module provides common traits for visiting or rewriting tree
//! data structures easily.

use std::sync::Arc;

use crate::Result;

/// If the function returns [`TreeNodeRecursion::Continue`], the normal execution of the
/// function continues. If it returns [`TreeNodeRecursion::Skip`], the function returns
/// with [`TreeNodeRecursion::Continue`] to jump next recursion step, bypassing further
/// exploration of the current step. In case of [`TreeNodeRecursion::Stop`], the function
/// return with [`TreeNodeRecursion::Stop`] and recursion halts.
#[macro_export]
macro_rules! handle_tree_recursion {
    ($EXPR:expr) => {
        match $EXPR {
            TreeNodeRecursion::Continue => {}
            // If the recursion should skip, do not apply to its children, let
            // the recursion continue:
            TreeNodeRecursion::Skip => return Ok(TreeNodeRecursion::Continue),
            // If the recursion should stop, do not apply to its children:
            TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
        }
    };
}

/// Defines a visitable and rewriteable a tree node. This trait is
/// implemented for plans ([`ExecutionPlan`] and [`LogicalPlan`]) as
/// well as expression trees ([`PhysicalExpr`], [`Expr`]) in
/// DataFusion
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
pub trait TreeNode: Sized {
    /// Applies `op` to the node and its children. `op` is applied in a preoder way,
    /// and it is controlled by [`TreeNodeRecursion`], which means result of the `op`
    /// on the self node can cause an early return.
    ///
    /// The `op` closure can be used to collect some info from the
    /// tree node or do some checking for the tree node.
    fn apply<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        op: &mut F,
    ) -> Result<TreeNodeRecursion> {
        handle_tree_recursion!(op(self)?);
        self.apply_children(&mut |node| node.apply(op))
    }

    /// Visit the tree node using the given [TreeNodeVisitor]
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
    /// post_visit(ChildNode1)
    /// pre_visit(ChildNode2)
    /// post_visit(ChildNode2)
    /// post_visit(ParentNode)
    /// ```
    ///
    /// If an Err result is returned, recursion is stopped immediately
    ///
    /// If [`TreeNodeRecursion::Stop`] is returned on a call to pre_visit, no
    /// children of that node will be visited, nor is post_visit
    /// called on that node. Details see [`TreeNodeVisitor`]
    ///
    /// If using the default [`TreeNodeVisitor::post_visit`] that does
    /// nothing, [`Self::apply`] should be preferred.
    fn visit<V: TreeNodeVisitor<N = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        handle_tree_recursion!(visitor.pre_visit(self)?);
        handle_tree_recursion!(self.apply_children(&mut |node| node.visit(visitor))?);
        visitor.post_visit(self)
    }

    /// Transforms the tree using `f_down` while traversing the tree top-down
    /// (pre-preorder) and using `f_up` while traversing the tree bottom-up (post-order).
    ///
    /// E.g. for an tree such as:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order:
    /// ```text
    /// f_down(ParentNode)
    /// f_down(ChildNode1)
    /// f_up(ChildNode1)
    /// f_down(ChildNode2)
    /// f_up(ChildNode2)
    /// f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on how the traversal can be controlled.
    ///
    /// If `f_down` or `f_up` returns [`Err`], recursion is stopped immediately.
    fn transform<FD, FU>(self, f_down: &mut FD, f_up: &mut FU) -> Result<Self>
    where
        FD: FnMut(Self) -> Result<(Transformed<Self>, TreeNodeRecursion)>,
        FU: FnMut(Self) -> Result<Self>,
    {
        let (new_node, tnr) = f_down(self).map(|(t, tnr)| (t.into(), tnr))?;
        match tnr {
            TreeNodeRecursion::Continue => {}
            // If the recursion should skip, do not apply to its children. And let the recursion continue
            TreeNodeRecursion::Skip => return Ok(new_node),
            // If the recursion should stop, do not apply to its children
            TreeNodeRecursion::Stop => {
                panic!("Stop can't be used in TreeNode::transform()")
            }
        }
        let node_with_new_children =
            new_node.map_children(|node| node.transform(f_down, f_up))?;
        f_up(node_with_new_children)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down(op))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' to the node and all of its
    /// children(Preorder Traversal) using a mutable function, `F`.
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_down_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op = op(self)?.into();
        after_op.map_children(|node| node.transform_down_mut(op))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal).
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up<F>(self, op: &F) -> Result<Self>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up(op))?;
        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'op' first to all of its
    /// children and then itself(Postorder Traversal) using a mutable function, `F`.
    /// When the `op` does not apply to a given node, it is left unchanged.
    fn transform_up_mut<F>(self, op: &mut F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let after_op_children = self.map_children(|node| node.transform_up_mut(op))?;
        let new_node = op(after_op_children)?.into();
        Ok(new_node)
    }

    /// Implements the [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for
    /// recursively transforming [`TreeNode`]s.
    ///
    /// E.g. for an tree such as:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// The nodes are visited using the following order:
    /// ```text
    /// TreeNodeRewriter::f_down(ParentNode)
    /// TreeNodeRewriter::f_down(ChildNode1)
    /// TreeNodeRewriter::f_up(ChildNode1)
    /// TreeNodeRewriter::f_down(ChildNode2)
    /// TreeNodeRewriter::f_up(ChildNode2)
    /// TreeNodeRewriter::f_up(ParentNode)
    /// ```
    ///
    /// See [`TreeNodeRecursion`] for more details on how the traversal can be controlled.
    ///
    /// If [`TreeNodeRewriter::f_down()`] or [`TreeNodeRewriter::f_up()`] returns [`Err`],
    /// recursion is stopped immediately.
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(self, rewriter: &mut R) -> Result<Self> {
        let (new_node, tnr) = rewriter.f_down(self)?;
        match tnr {
            TreeNodeRecursion::Continue => {}
            // If the recursion should skip, do not apply to its children. And let the recursion continue
            TreeNodeRecursion::Skip => return Ok(new_node),
            // If the recursion should stop, do not apply to its children
            TreeNodeRecursion::Stop => {
                panic!("Stop can't be used in TreeNode::rewrite()")
            }
        }
        let node_with_new_children =
            new_node.map_children(|node| node.rewrite(rewriter))?;
        rewriter.f_up(node_with_new_children)
    }

    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>;

    /// Apply transform `F` to the node's children, the transform `F` might have a direction(Preorder or Postorder)
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>;
}

/// Implements the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively walking [`TreeNode`]s.
///
/// [`TreeNodeVisitor`] allows keeping the algorithms
/// separate from the code to traverse the structure of the `TreeNode`
/// tree and makes it easier to add new types of tree node and
/// algorithms.
///
/// When passed to[`TreeNode::visit`], [`TreeNodeVisitor::pre_visit`]
/// and [`TreeNodeVisitor::post_visit`] are invoked recursively
/// on an node tree.
///
/// If an [`Err`] result is returned, recursion is stopped
/// immediately.
///
/// If [`TreeNodeRecursion::Stop`] is returned on a call to pre_visit, no
/// children of that tree node are visited, nor is post_visit
/// called on that tree node
///
/// If [`TreeNodeRecursion::Stop`] is returned on a call to post_visit, no
/// siblings of that tree node are visited, nor is post_visit
/// called on its parent tree node
///
/// If [`TreeNodeRecursion::Skip`] is returned on a call to pre_visit, no
/// children of that tree node are visited.
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type N: TreeNode;

    /// Invoked before any children of `node` are visited.
    fn pre_visit(&mut self, node: &Self::N) -> Result<TreeNodeRecursion>;

    /// Invoked after all children of `node` are visited. Default
    /// implementation does nothing.
    fn post_visit(&mut self, _node: &Self::N) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Trait for potentially recursively transform a [`TreeNode`] node tree.
pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree before any children are rewritten /
    /// visited.
    /// Default implementation returns the node unmodified and continues recursion.
    fn f_down(&mut self, node: Self::Node) -> Result<(Self::Node, TreeNodeRecursion)> {
        Ok((node, TreeNodeRecursion::Continue))
    }

    /// Invoked while traversing up the tree after all children have been rewritten /
    /// visited.
    /// Default implementation returns the node unmodified.
    fn f_up(&mut self, node: Self::Node) -> Result<Self::Node> {
        Ok(node)
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,
    /// Skip the current subtree.
    Skip,
    /// Stop recursion.
    Stop,
}

pub enum Transformed<T> {
    /// The item was transformed / rewritten somehow
    Yes(T),
    /// The item was not transformed
    No(T),
}

impl<T> Transformed<T> {
    pub fn into(self) -> T {
        match self {
            Transformed::Yes(t) => t,
            Transformed::No(t) => t,
        }
    }

    pub fn into_pair(self) -> (T, bool) {
        match self {
            Transformed::Yes(t) => (t, true),
            Transformed::No(t) => (t, false),
        }
    }
}

/// Helper trait for implementing [`TreeNode`] that have children stored as Arc's
///
/// If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`TreeNode`]
pub trait DynTreeNode {
    /// Returns all children of the specified TreeNode
    fn arc_children(&self) -> Vec<Arc<Self>>;

    /// construct a new self with the specified children
    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

/// Blanket implementation for Arc for any tye that implements
/// [`DynTreeNode`] (such as [`Arc<dyn PhysicalExpr>`])
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        for child in self.arc_children() {
            handle_tree_recursion!(op(&child)?)
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children =
                children.into_iter().map(transform).collect::<Result<_>>()?;
            let arc_self = Arc::clone(&self);
            self.with_new_arc_children(arc_self, new_children)
        } else {
            Ok(self)
        }
    }
}

/// Instead of implementing [`TreeNode`], it's recommended to implement a [`ConcreteTreeNode`] for
/// trees that contain nodes with payloads. This approach ensures safe execution of algorithms
/// involving payloads, by enforcing rules for detaching and reattaching child nodes.
pub trait ConcreteTreeNode: Sized {
    /// Provides read-only access to child nodes.
    fn children(&self) -> Vec<&Self>;

    /// Detaches the node from its children, returning the node itself and its detached children.
    fn take_children(self) -> (Self, Vec<Self>);

    /// Reattaches updated child nodes to the node, returning the updated node.
    fn with_new_children(self, children: Vec<Self>) -> Result<Self>;
}

impl<T: ConcreteTreeNode> TreeNode for T {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, op: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        for child in self.children() {
            handle_tree_recursion!(op(child)?)
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let (new_self, children) = self.take_children();
        if !children.is_empty() {
            let new_children =
                children.into_iter().map(transform).collect::<Result<_>>()?;
            new_self.with_new_children(new_children)
        } else {
            Ok(new_self)
        }
    }
}
