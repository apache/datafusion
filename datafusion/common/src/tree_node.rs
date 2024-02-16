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

/// This macro is used to determine continuation after a top-down closure is applied
/// during visiting traversals.
///
/// If the function returns [`TreeNodeRecursion::Continue`], the normal execution of the
/// function continues.
/// If it returns [`TreeNodeRecursion::Jump`], the function returns with (propagates up)
/// [`TreeNodeRecursion::Continue`] to jump next recursion step, bypassing further
/// exploration of the current step.
/// In case of [`TreeNodeRecursion::Stop`], the function return with (propagates up)
/// [`TreeNodeRecursion::Stop`] and recursion halts.
#[macro_export]
macro_rules! handle_visit_recursion_down {
    ($EXPR:expr) => {
        match $EXPR {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => return Ok(TreeNodeRecursion::Continue),
            TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
        }
    };
}

/// This macro is used to determine continuation between visiting siblings during visiting
/// traversals.
///
/// If the function returns [`TreeNodeRecursion::Continue`] or
/// [`TreeNodeRecursion::Jump`], the normal execution of the function continues.
/// In case of [`TreeNodeRecursion::Stop`], the function return with (propagates up)
/// [`TreeNodeRecursion::Stop`] and recursion halts.
macro_rules! handle_visit_recursion {
    ($EXPR:expr) => {
        match $EXPR {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
            TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
        }
    };
}

/// This macro is used to determine continuation before a bottom-up closure is applied
/// during visiting traversals.
///
/// If the function returns [`TreeNodeRecursion::Continue`], the normal execution of the
/// function continues.
/// If it returns [`TreeNodeRecursion::Jump`], the function returns with (propagates up)
/// [`TreeNodeRecursion::Jump`], bypassing further bottom-up closures until a top-down
/// closure is found.
/// In case of [`TreeNodeRecursion::Stop`], the function return with (propagates up)
/// [`TreeNodeRecursion::Stop`] and recursion halts.
#[macro_export]
macro_rules! handle_visit_recursion_up {
    ($EXPR:expr) => {
        match $EXPR {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => return Ok(TreeNodeRecursion::Jump),
            TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
        }
    };
}

/// This macro is used to determine continuation during top-down transforming traversals.
///
/// After the bottom-up closure returns with [`Transformed`] depending on the returned
/// [`TreeNodeRecursion`], [`Transformed::and_then()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion_down {
    ($F_DOWN:expr, $F_SELF:expr) => {
        $F_DOWN?.and_then(
            |n| n.map_children($F_SELF),
            Some(TreeNodeRecursion::Continue),
        )
    };
}

/// This macro is used to determine continuation during combined transforming traversals.
///
/// After the bottom-up closure returns with [`Transformed`] depending on the returned
/// [`TreeNodeRecursion`], [`Transformed::and_then()`] decides about recursion
/// continuation and if [`TreeNodeRecursion`] state propagation is needed.
/// And then after recursing into children returns with [`Transformed`] depending on the
/// returned  [`TreeNodeRecursion`], [`Transformed::and_then()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_SELF:expr, $F_UP:expr) => {
        $F_DOWN?.and_then(
            |n| {
                n.map_children($F_SELF)?
                    .and_then($F_UP, Some(TreeNodeRecursion::Jump))
            },
            Some(TreeNodeRecursion::Continue),
        )
    };
}

/// This macro is used to determine continuation during bottom-up transforming traversals.
///
/// After recursing into children returns with [`Transformed`] depending on the returned
/// [`TreeNodeRecursion`], [`Transformed::and_then()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion_up {
    ($NODE:expr, $F_SELF:expr, $F_UP:expr) => {
        $NODE
            .map_children($F_SELF)?
            .and_then($F_UP, Some(TreeNodeRecursion::Jump))
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
    /// Applies `f` to the node and its children. `f` is applied in a preoder way,
    /// and it is controlled by [`TreeNodeRecursion`], which means result of the `f`
    /// on the self node can cause an early return.
    ///
    /// The `f` closure can be used to collect some info from the
    /// tree node or do some checking for the tree node.
    fn apply<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: &mut F,
    ) -> Result<TreeNodeRecursion> {
        handle_visit_recursion_down!(f(self)?);
        self.apply_children(&mut |n| n.apply(f))
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
    /// If using the default [`TreeNodeVisitor::f_up`] that does
    /// nothing, [`Self::apply`] should be preferred.
    fn visit<V: TreeNodeVisitor<Node = Self>>(
        &self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        handle_visit_recursion_down!(visitor.f_down(self)?);
        handle_visit_recursion_up!(self.apply_children(&mut |n| n.visit(visitor))?);
        visitor.f_up(self)
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
    fn transform<FD, FU>(
        self,
        f_down: &mut FD,
        f_up: &mut FU,
    ) -> Result<Transformed<Self>>
    where
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion!(f_down(self), |c| c.transform(f_down, f_up), f_up)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'f' to the node and all of its
    /// children(Preorder Traversal).
    /// When the `f` does not apply to a given node, it is left unchanged.
    fn transform_down<F>(self, f: &F) -> Result<Transformed<Self>>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion_down!(f(self), |c| c.transform_down(f))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'f' to the node and all of its
    /// children(Preorder Traversal) using a mutable function, `F`.
    /// When the `f` does not apply to a given node, it is left unchanged.
    fn transform_down_mut<F>(self, f: &mut F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion_down!(f(self), |c| c.transform_down_mut(f))
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'f' first to all of its
    /// children and then itself(Postorder Traversal).
    /// When the `f` does not apply to a given node, it is left unchanged.
    fn transform_up<F>(self, f: &F) -> Result<Transformed<Self>>
    where
        F: Fn(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion_up!(self, |c| c.transform_up(f), f)
    }

    /// Convenience utils for writing optimizers rule: recursively apply the given 'f' first to all of its
    /// children and then itself(Postorder Traversal) using a mutable function, `F`.
    /// When the `f` does not apply to a given node, it is left unchanged.
    fn transform_up_mut<F>(self, f: &mut F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion_up!(self, |c| c.transform_up_mut(f), f)
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
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(rewriter.f_down(self), |c| c.rewrite(rewriter), |n| {
            rewriter.f_up(n)
        })
    }

    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>;

    /// Apply transform `F` to the node's children, the transform `F` might have a direction(Preorder or Postorder)
    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>;
}

/// Implements the [visitor
/// pattern](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively walking [`TreeNode`]s.
///
/// [`TreeNodeVisitor`] allows keeping the algorithms
/// separate from the code to traverse the structure of the `TreeNode`
/// tree and makes it easier to add new types of tree node and
/// algorithms.
///
/// When passed to[`TreeNode::visit`], [`TreeNodeVisitor::f_down`]
/// and [`TreeNodeVisitor::f_up`] are invoked recursively
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
/// If [`TreeNodeRecursion::Jump`] is returned on a call to pre_visit, no
/// children of that tree node are visited.
pub trait TreeNodeVisitor: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked before any children of `node` are visited.
    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion>;

    /// Invoked after all children of `node` are visited. Default
    /// implementation does nothing.
    fn f_up(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
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
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    /// Invoked while traversing up the tree after all children have been rewritten /
    /// visited.
    /// Default implementation returns the node unmodified.
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug, PartialEq)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,

    /// In top-down traversals skip recursing into children but continue with the next
    /// node, which actually means pruning of the subtree.
    /// In bottom-up traversals bypass calling bottom-up closures till the next leaf node.
    /// In combined traversals bypass calling bottom-up closures till the next top-down
    /// closure.
    Jump,

    /// Stop recursion.
    Stop,
}

pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
    pub tnr: TreeNodeRecursion,
}

impl<T> Transformed<T> {
    pub fn new(data: T, transformed: bool, tnr: TreeNodeRecursion) -> Self {
        Self {
            data,
            transformed,
            tnr,
        }
    }

    pub fn yes(data: T) -> Self {
        Self {
            data,
            transformed: true,
            tnr: TreeNodeRecursion::Continue,
        }
    }

    pub fn no(data: T) -> Self {
        Self {
            data,
            transformed: false,
            tnr: TreeNodeRecursion::Continue,
        }
    }

    pub fn map_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed {
            data: f(self.data),
            transformed: self.transformed,
            tnr: self.tnr,
        }
    }

    pub fn flat_map_data<U, F: FnOnce(T) -> Result<U>>(
        self,
        f: F,
    ) -> Result<Transformed<U>> {
        Ok(Transformed {
            data: f(self.data)?,
            transformed: self.transformed,
            tnr: self.tnr,
        })
    }

    /// This is an important function to decide about recursion continuation and
    /// [`TreeNodeRecursion`] state propagation. Handling [`TreeNodeRecursion::Continue`]
    /// and [`TreeNodeRecursion::Stop`] is always straightforward, but
    /// [`TreeNodeRecursion::Jump`] can behave differently when we are traversing down or
    /// up on a tree.
    fn and_then<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
        return_on_jump: Option<TreeNodeRecursion>,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => {
                if let Some(tnr) = return_on_jump {
                    return Ok(Transformed { tnr, ..self });
                }
            }
            TreeNodeRecursion::Stop => return Ok(self),
        };
        let t = f(self.data)?;
        Ok(Transformed {
            transformed: t.transformed || self.transformed,
            ..t
        })
    }

    pub fn and_then_transform<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        self.and_then(f, None)
    }
}

pub trait TransformedIterator: Iterator {
    fn map_till_continue_and_collect<F>(
        self,
        f: F,
    ) -> Result<Transformed<Vec<Self::Item>>>
    where
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
        Self: Sized;
}

impl<I: Iterator> TransformedIterator for I {
    fn map_till_continue_and_collect<F>(
        self,
        mut f: F,
    ) -> Result<Transformed<Vec<Self::Item>>>
    where
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    {
        let mut new_tnr = TreeNodeRecursion::Continue;
        let mut new_transformed = false;
        let new_data = self
            .map(|i| {
                Ok(match new_tnr {
                    TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                        let Transformed {
                            data,
                            transformed,
                            tnr,
                        } = f(i)?;
                        new_tnr = tnr;
                        new_transformed |= transformed;
                        data
                    }
                    TreeNodeRecursion::Stop => i,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Transformed {
            data: new_data,
            transformed: new_transformed,
            tnr: new_tnr,
        })
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
    ) -> Result<Transformed<Arc<Self>>>;
}

/// Blanket implementation for Arc for any tye that implements
/// [`DynTreeNode`] (such as [`Arc<dyn PhysicalExpr>`])
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        let mut tnr = TreeNodeRecursion::Continue;
        for child in self.arc_children() {
            tnr = f(&child)?;
            handle_visit_recursion!(tnr)
        }
        Ok(tnr)
    }

    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let children = self.arc_children();
        if !children.is_empty() {
            let t = children.into_iter().map_till_continue_and_collect(f)?;
            // TODO: Currently `assert_eq!(t.transformed, t2.transformed)` fails as
            //  `t.transformed` quality comes from if the transformation closures fill the
            //   field correctly.
            //  Once we trust `t.transformed` we can remove the additional check in
            //  `with_new_arc_children()`.
            let arc_self = Arc::clone(&self);
            let t2 = self.with_new_arc_children(arc_self, t.data)?;

            // Propagate up `t.transformed` and `t.tnr` along with the node containing
            // transformed children.
            Ok(Transformed::new(t2.data, t.transformed, t.tnr))
        } else {
            Ok(Transformed::no(self))
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
    fn with_new_children(self, children: Vec<Self>) -> Result<Transformed<Self>>;
}

impl<T: ConcreteTreeNode> TreeNode for T {
    /// Apply the closure `F` to the node's children
    fn apply_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
    where
        F: FnMut(&Self) -> Result<TreeNodeRecursion>,
    {
        let mut tnr = TreeNodeRecursion::Continue;
        for child in self.children() {
            tnr = f(child)?;
            handle_visit_recursion!(tnr)
        }
        Ok(tnr)
    }

    fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
    where
        F: FnMut(Self) -> Result<Transformed<Self>>,
    {
        let (new_self, children) = self.take_children();
        if !children.is_empty() {
            let t = children.into_iter().map_till_continue_and_collect(f)?;
            // TODO: Currently `assert_eq!(t.transformed, t2.transformed)` fails as
            //  `t.transformed` quality comes from if the transformation closures fill the
            //   field correctly.
            //  Once we trust `t.transformed` we can remove the additional check in
            //  `with_new_children()`.
            let t2 = new_self.with_new_children(t.data)?;

            // Propagate up `t.transformed` and `t.tnr` along with the node containing
            // transformed children.
            Ok(Transformed::new(t2.data, t.transformed, t.tnr))
        } else {
            Ok(Transformed::no(new_self))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tree_node::{
        Transformed, TransformedIterator, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
        TreeNodeVisitor,
    };
    use crate::Result;
    use std::fmt::Display;

    struct TestTreeNode<T> {
        children: Vec<TestTreeNode<T>>,
        data: T,
    }

    impl<T> TestTreeNode<T> {
        fn new(children: Vec<TestTreeNode<T>>, data: T) -> Self {
            Self { children, data }
        }
    }

    impl<T> TreeNode for TestTreeNode<T> {
        fn apply_children<F>(&self, f: &mut F) -> Result<TreeNodeRecursion>
        where
            F: FnMut(&Self) -> Result<TreeNodeRecursion>,
        {
            let mut tnr = TreeNodeRecursion::Continue;
            for child in &self.children {
                tnr = f(child)?;
                handle_visit_recursion!(tnr);
            }
            Ok(tnr)
        }

        fn map_children<F>(self, f: F) -> Result<Transformed<Self>>
        where
            F: FnMut(Self) -> Result<Transformed<Self>>,
        {
            Ok(self
                .children
                .into_iter()
                .map_till_continue_and_collect(f)?
                .map_data(|new_children| Self {
                    children: new_children,
                    ..self
                }))
        }
    }

    fn new_test_tree<'a>() -> TestTreeNode<&'a str> {
        let node_a = TestTreeNode::new(vec![], "a");
        let node_b = TestTreeNode::new(vec![], "b");
        let node_d = TestTreeNode::new(vec![node_a], "d");
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c");
        let node_e = TestTreeNode::new(vec![node_c], "e");
        let node_h = TestTreeNode::new(vec![], "h");
        let node_g = TestTreeNode::new(vec![node_h], "g");
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f");
        let node_i = TestTreeNode::new(vec![node_f], "i");
        TestTreeNode::new(vec![node_i], "j")
    }

    fn all_visits<'a>() -> Vec<&'a str> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
    }

    fn f_down_jump_on_e_visits<'a>() -> Vec<&'a str> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
    }

    fn f_up_jump_on_a_visits<'a>() -> Vec<&'a str> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
    }

    type TestVisitorF<T> = Box<dyn FnMut(&TestTreeNode<T>) -> Result<TreeNodeRecursion>>;

    struct TestVisitor<T: Display> {
        visits: Vec<String>,
        fd: TestVisitorF<T>,
        fu: TestVisitorF<T>,
    }

    impl<T: Display> TestVisitor<T> {
        fn new(fd: TestVisitorF<T>, fu: TestVisitorF<T>) -> Self {
            Self {
                visits: vec![],
                fd,
                fu,
            }
        }
    }

    impl<T: Display> TreeNodeVisitor for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.fd)(node)
        }

        fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_up({})", node.data));
            (*self.fu)(node)
        }
    }

    type TestRewriterF<T> =
        Box<dyn FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>>>;

    struct TestRewriter<T: Display> {
        visits: Vec<String>,
        fd: TestRewriterF<T>,
        fu: TestRewriterF<T>,
    }

    impl<T: Display> TestRewriter<T> {
        fn new(fd: TestRewriterF<T>, fu: TestRewriterF<T>) -> Self {
            Self {
                visits: vec![],
                fd,
                fu,
            }
        }
    }

    impl<T: Display> TreeNodeRewriter for TestRewriter<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.fd)(node)
        }

        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            self.visits.push(format!("f_up({})", node.data));
            (*self.fu)(node)
        }
    }

    #[test]
    fn test_visit() -> Result<()> {
        let tree = new_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(|_| Ok(TreeNodeRecursion::Continue)),
            Box::new(|_| Ok(TreeNodeRecursion::Continue)),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, all_visits());

        Ok(())
    }

    #[test]
    fn test_visit_f_down_jump() -> Result<()> {
        let tree = new_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(|node| {
                Ok(if node.data == "e" {
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                })
            }),
            Box::new(|_| Ok(TreeNodeRecursion::Continue)),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, f_down_jump_on_e_visits());

        Ok(())
    }

    #[test]
    fn test_visit_f_up_jump() -> Result<()> {
        let tree = new_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(|_| Ok(TreeNodeRecursion::Continue)),
            Box::new(|node| {
                Ok(if node.data == "a" {
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                })
            }),
        );
        tree.visit(&mut visitor)?;
        assert_eq!(visitor.visits, f_up_jump_on_a_visits());

        Ok(())
    }

    #[test]
    fn test_rewrite() -> Result<()> {
        let tree = new_test_tree();
        let mut rewriter = TestRewriter::new(
            Box::new(|node| Ok(Transformed::no(node))),
            Box::new(|node| Ok(Transformed::no(node))),
        );
        tree.rewrite(&mut rewriter)?;
        assert_eq!(rewriter.visits, all_visits());

        Ok(())
    }

    #[test]
    fn test_rewrite_f_down_jump() -> Result<()> {
        let tree = new_test_tree();
        let mut rewriter = TestRewriter::new(
            Box::new(|node| {
                Ok(if node.data == "e" {
                    Transformed::new(node, false, TreeNodeRecursion::Jump)
                } else {
                    Transformed::no(node)
                })
            }),
            Box::new(|node| Ok(Transformed::no(node))),
        );
        tree.rewrite(&mut rewriter)?;
        assert_eq!(rewriter.visits, f_down_jump_on_e_visits());

        Ok(())
    }

    #[test]
    fn test_rewrite_f_up_jump() -> Result<()> {
        let tree = new_test_tree();
        let mut rewriter = TestRewriter::new(
            Box::new(|node| Ok(Transformed::no(node))),
            Box::new(|node| {
                Ok(if node.data == "a" {
                    Transformed::new(node, false, TreeNodeRecursion::Jump)
                } else {
                    Transformed::no(node)
                })
            }),
        );
        tree.rewrite(&mut rewriter)?;
        assert_eq!(rewriter.visits, f_up_jump_on_a_visits());

        Ok(())
    }
}
