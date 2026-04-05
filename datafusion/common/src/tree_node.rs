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

//! [`TreeNode`] for visiting and rewriting expression and plan trees

use crate::Result;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

/// These macros are used to determine continuation during transforming traversals.
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_CHILD:expr, $F_UP:expr) => {{
        $F_DOWN?
            .transform_children(|n| n.map_children($F_CHILD))?
            .transform_parent($F_UP)
    }};
}

/// These macros are used to determine continuation during transforming traversals.
macro_rules! handle_transform_recursion_in_scope {
    ($F_DOWN:expr, $F_CHILD:expr, $F_UP:expr) => {{
        $F_DOWN?
            .transform_children(|n| n.map_children_in_scope($F_CHILD))?
            .transform_parent($F_UP)
    }};
}

/// API for inspecting and rewriting tree data structures.
///
/// The `TreeNode` API is used to express algorithms separately from traversing
/// the structure of `TreeNode`s, avoiding substantial code duplication.
///
/// This trait is implemented for plans ([`ExecutionPlan`], [`LogicalPlan`]) and
/// expression trees ([`PhysicalExpr`], [`Expr`]) as well as Plan+Payload
/// combinations [`PlanContext`] and [`ExprContext`].
///
/// # Overview
/// There are three categories of TreeNode APIs:
///
/// 1. "Inspecting" APIs to traverse a tree of `&TreeNodes`:
///    [`apply`], [`visit`], [`exists`].
///
/// 2. "Transforming" APIs that traverse and consume a tree of `TreeNode`s
///    producing possibly changed `TreeNode`s: [`transform`], [`transform_up`],
///    [`transform_down`], [`transform_down_up`], and [`rewrite`].
///
/// 3. Internal APIs used to implement the `TreeNode` API: [`apply_children`],
///    and [`map_children`].
///
/// | Traversal Order | Inspecting | Transforming |
/// | --- | --- | --- |
/// | top-down | [`apply`], [`exists`] | [`transform_down`]|
/// | bottom-up | | [`transform`] , [`transform_up`]|
/// | combined with separate `f_down` and `f_up` closures | | [`transform_down_up`] |
/// | combined with `f_down()` and `f_up()` in an object | [`visit`]  | [`rewrite`] |
///
/// **Note**:while there is currently no in-place mutation API that uses `&mut
/// TreeNode`, the transforming APIs are efficient and optimized to avoid
/// cloning.
///
/// [`apply`]: Self::apply
/// [`visit`]: Self::visit
/// [`exists`]: Self::exists
/// [`transform`]: Self::transform
/// [`transform_up`]: Self::transform_up
/// [`transform_down`]: Self::transform_down
/// [`transform_down_up`]: Self::transform_down_up
/// [`rewrite`]: Self::rewrite
/// [`apply_children`]: Self::apply_children
/// [`map_children`]: Self::map_children
///
/// # Terminology
/// The following terms are used in this trait
///
/// * `f_down`: Invoked before any children of the current node are visited.
/// * `f_up`: Invoked after all children of the current node are visited.
/// * `f`: closure that is applied to the current node.
/// * `map_*`: applies a transformation to rewrite owned nodes
/// * `apply_*`:  invokes a function on borrowed nodes
/// * `transform_`: applies a transformation to rewrite owned nodes
///
/// <!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
/// [`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
/// [`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
/// [`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
/// [`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
/// [`PlanContext`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/tree_node/struct.PlanContext.html
/// [`ExprContext`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/tree_node/struct.ExprContext.html
pub trait TreeNode: Sized {
    /// Visit the tree node with a [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeVisitor::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeVisitor::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// # Return Value
    /// Specifies how the tree walk ended. See [`TreeNodeRecursion`] for details.
    ///
    /// # See Also:
    /// * [`Self::apply`] for inspecting nodes with a closure
    /// * [`Self::rewrite`] to rewrite owned `TreeNode`s
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeVisitor::f_down(ParentNode)
    /// TreeNodeVisitor::f_down(ChildNode1)
    /// TreeNodeVisitor::f_up(ChildNode1)
    /// TreeNodeVisitor::f_down(ChildNode2)
    /// TreeNodeVisitor::f_up(ChildNode2)
    /// TreeNodeVisitor::f_up(ParentNode)
    /// ```
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn visit<'n, V: TreeNodeVisitor<'n, Node = Self>>(
        &'n self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        visitor
            .f_down(self)?
            .visit_children(|| self.apply_children(|c| c.visit(visitor)))?
            .visit_parent(|| visitor.f_up(self))
    }

    /// Rewrite the tree node with a [`TreeNodeRewriter`], performing a
    /// depth-first walk of the node and its children.
    ///
    /// [`TreeNodeRewriter::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeRewriter::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// Note: If using the default [`TreeNodeRewriter::f_up`] or
    /// [`TreeNodeRewriter::f_down`] that do nothing, consider using
    /// [`Self::transform_down`] instead.
    ///
    /// # Return Value
    /// The returns value specifies how the tree walk should proceed. See
    /// [`TreeNodeRecursion`] for details. If an [`Err`] is returned, the
    /// recursion stops immediately.
    ///
    /// # See Also
    /// * [`Self::visit`] for inspecting (without modification) `TreeNode`s
    /// * [Self::transform_down_up] for a top-down (pre-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeRewriter::f_down(ParentNode)
    /// TreeNodeRewriter::f_down(ChildNode1)
    /// TreeNodeRewriter::f_up(ChildNode1)
    /// TreeNodeRewriter::f_down(ChildNode2)
    /// TreeNodeRewriter::f_up(ChildNode2)
    /// TreeNodeRewriter::f_up(ParentNode)
    /// ```
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn rewrite<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion!(rewriter.f_down(self), |c| c.rewrite(rewriter), |n| {
            rewriter.f_up(n)
        })
    }

    /// Applies `f` to the node then each of its children, recursively (a
    /// top-down, pre-order traversal).
    ///
    /// The return [`TreeNodeRecursion`] controls the recursion and can cause
    /// an early return.
    ///
    /// # See Also
    /// * [`Self::transform_down`] for the equivalent transformation API.
    /// * [`Self::visit`] for both top-down and bottom up traversal.
    fn apply<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn apply_impl<'n, N: TreeNode, F: FnMut(&'n N) -> Result<TreeNodeRecursion>>(
            node: &'n N,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node)?.visit_children(|| node.apply_children(|c| apply_impl(c, f)))
        }

        apply_impl(self, &mut f)
    }

    /// Recursively rewrite the node's children and then the node using `f`
    /// (a bottom-up post-order traversal).
    ///
    /// A synonym of [`Self::transform_up`].
    fn transform<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up(f)
    }

    /// Recursively rewrite the tree using `f` in a top-down (pre-order)
    /// fashion.
    ///
    /// `f` is applied to the node first, and then its children.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_down<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            f(node)?.transform_children(|n| n.map_children(|c| transform_down_impl(c, f)))
        }

        transform_down_impl(self, &mut f)
    }

    /// Recursively rewrite the node using `f` in a bottom-up (post-order)
    /// fashion.
    ///
    /// `f` is applied to the node's  children first, and then to the node itself.
    ///
    /// # See Also
    /// * [`Self::transform_down`] top-down (pre-order) traversal.
    /// * [Self::transform_down_up] for a combined traversal with closures
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    fn transform_up<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_up_impl<N: TreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            node.map_children(|c| transform_up_impl(c, f))?
                .transform_parent(f)
        }

        transform_up_impl(self, &mut f)
    }

    /// Transforms the node using `f_down` while traversing the tree top-down
    /// (pre-order), and using `f_up` while traversing the tree bottom-up
    /// (post-order).
    ///
    /// The method behaves the same as calling [`Self::transform_down`] followed
    /// by [`Self::transform_up`] on the same node. Use this method if you want
    /// to start the `f_up` process right where `f_down` jumps. This can make
    /// the whole process faster by reducing the number of `f_up` steps.
    ///
    /// # See Also
    /// * [`Self::transform_up`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down] for a top-down (pre-order) traversal.
    /// * [`Self::rewrite`] for a combined traversal with a visitor
    ///
    /// # Example
    /// Consider the following tree structure:
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
    /// See [`TreeNodeRecursion`] for more details on controlling the traversal.
    ///
    /// If `f_down` or `f_up` returns [`Err`], the recursion stops immediately.
    ///
    /// Example:
    /// ```text
    ///                                               |   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                  TreeNodeRecursion::Continue  |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                              \|/  | F |
    ///                                               '   +---+
    ///                                                  /     \ ___________________
    ///                  When `f_down` is           +---+                           \ ---+
    ///                  applied on node "E",       | E |                            | G |
    ///                  it returns with "Jump".    +---+                            +---+
    ///                                               |                                |
    ///                                             +---+                            +---+
    ///                                             | C |                            | H |
    ///                                             +---+                            +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    ///
    /// Instead of starting from leaf nodes, `f_up` starts from the node "E".
    ///                                                   +---+
    ///                                               |   | J |
    ///                                               |   +---+
    ///                                               |     |
    ///                                               |   +---+
    ///                                               |   | I |
    ///                                               |   +---+
    ///                                               |     |
    ///                                              /    +---+
    ///                                            /      | F |
    ///                                          /        +---+
    ///                                        /         /     \ ______________________
    ///                                       |     +---+   .                          \ ---+
    ///                                       |     | E |  /|\  After `f_down` jumps    | G |
    ///                                       |     +---+   |   on node E, `f_up`       +---+
    ///                                        \------| ---/   if applied on node E.      |
    ///                                             +---+                               +---+
    ///                                             | C |                               | H |
    ///                                             +---+                               +---+
    ///                                             /   \
    ///                                        +---+     +---+
    ///                                        | B |     | D |
    ///                                        +---+     +---+
    ///                                                    |
    ///                                                  +---+
    ///                                                  | A |
    ///                                                  +---+
    /// ```
    fn transform_down_up<
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    >(
        self,
        mut f_down: FD,
        mut f_up: FU,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_up_impl<
            N: TreeNode,
            FD: FnMut(N) -> Result<Transformed<N>>,
            FU: FnMut(N) -> Result<Transformed<N>>,
        >(
            node: N,
            f_down: &mut FD,
            f_up: &mut FU,
        ) -> Result<Transformed<N>> {
            handle_transform_recursion!(
                f_down(node),
                |c| transform_down_up_impl(c, f_down, f_up),
                f_up
            )
        }

        transform_down_up_impl(self, &mut f_down, &mut f_up)
    }

    /// Returns true if `f` returns true for any node in the tree.
    ///
    /// Stops recursion as soon as a matching node is found
    fn exists<F: FnMut(&Self) -> Result<bool>>(&self, mut f: F) -> Result<bool> {
        let mut found = false;
        self.apply(|n| {
            Ok(if f(n)? {
                found = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .map(|_| found)
    }

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::map_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to inspect node's children (but not the node
    /// itself).
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`TreeNode`] trait for your own type, you
    /// should implement this method and [`Self::apply_children`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to rewrite the node's children (but not the node itself).
    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

/// API for inspecting and rewriting tree data structures.
///
/// See [`TreeNode`] for more details.
///
/// This add the notion of scopes to [`TreeNode`] and allow you to operate in that.
///
/// Scope is left for implementers to define, for `PhysicalExpr` child is defined in scope if it have the same input schema as current `PhysicalExpr`.
pub trait ScopedTreeNode: TreeNode {
    /// Visit the tree node with a [`TreeNodeVisitor`], performing a
    /// depth-first walk of the node and its children that are in the same scope.
    ///
    /// [`TreeNodeVisitor::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeVisitor::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// # Return Value
    /// Specifies how the tree walk ended. See [`TreeNodeRecursion`] for details.
    ///
    /// # See Also:
    /// * [`Self::apply_in_scope`] for inspecting nodes with a closure
    /// * [`Self::rewrite_in_scope`] to rewrite owned `ScopedTreeNode`s
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeVisitor::f_down(ParentNode)
    /// TreeNodeVisitor::f_down(ChildNode1)
    /// TreeNodeVisitor::f_up(ChildNode1)
    /// TreeNodeVisitor::f_down(ChildNode2)
    /// TreeNodeVisitor::f_up(ChildNode2)
    /// TreeNodeVisitor::f_up(ParentNode)
    /// ```
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn visit_in_scope<'n, V: TreeNodeVisitor<'n, Node = Self>>(
        &'n self,
        visitor: &mut V,
    ) -> Result<TreeNodeRecursion> {
        visitor
            .f_down(self)?
            .visit_children(|| {
                self.apply_children_in_scope(|c| c.visit_in_scope(visitor))
            })?
            .visit_parent(|| visitor.f_up(self))
    }

    /// Rewrite the tree node with a [`TreeNodeRewriter`], performing a
    /// depth-first walk of the node and its children that are in the same scope.
    ///
    /// [`TreeNodeRewriter::f_down()`] is called in top-down order (before
    /// children are visited), [`TreeNodeRewriter::f_up()`] is called in
    /// bottom-up order (after children are visited).
    ///
    /// Note: If using the default [`TreeNodeRewriter::f_up`] or
    /// [`TreeNodeRewriter::f_down`] that do nothing, consider using
    /// [`Self::transform_down_in_scope`] instead.
    ///
    /// # Return Value
    /// The returns value specifies how the tree walk should proceed. See
    /// [`TreeNodeRecursion`] for details. If an [`Err`] is returned, the
    /// recursion stops immediately.
    ///
    /// # See Also
    /// * [`Self::visit_in_scope`] for inspecting (without modification) `ScopedTreeNode`s
    /// * [Self::transform_down_up_in_scope] for a combined top-down and bottom-up traversal.
    /// * [Self::transform_down_in_scope] for a top-down (pre-order) traversal.
    /// * [`Self::transform_up_in_scope`] for a bottom-up (post-order) traversal.
    ///
    /// # Example
    /// Consider the following tree structure:
    /// ```text
    /// ParentNode
    ///    left: ChildNode1
    ///    right: ChildNode2
    /// ```
    ///
    /// Here, the nodes would be visited using the following order:
    /// ```text
    /// TreeNodeRewriter::f_down(ParentNode)
    /// TreeNodeRewriter::f_down(ChildNode1)
    /// TreeNodeRewriter::f_up(ChildNode1)
    /// TreeNodeRewriter::f_down(ChildNode2)
    /// TreeNodeRewriter::f_up(ChildNode2)
    /// TreeNodeRewriter::f_up(ParentNode)
    /// ```
    #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
    fn rewrite_in_scope<R: TreeNodeRewriter<Node = Self>>(
        self,
        rewriter: &mut R,
    ) -> Result<Transformed<Self>> {
        handle_transform_recursion_in_scope!(
            rewriter.f_down(self),
            |c| c.rewrite_in_scope(rewriter),
            |n| { rewriter.f_up(n) }
        )
    }

    /// Applies `f` to the node then each of its children that are in the
    /// same scope, recursively (a top-down, pre-order traversal).
    ///
    /// The return [`TreeNodeRecursion`] controls the recursion and can cause
    /// an early return.
    ///
    /// # See Also
    /// * [`Self::transform_down_in_scope`] for the equivalent transformation API.
    /// * [`Self::visit_in_scope`] for both top-down and bottom up traversal.
    fn apply_in_scope<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn apply_impl<
            'n,
            N: ScopedTreeNode,
            F: FnMut(&'n N) -> Result<TreeNodeRecursion>,
        >(
            node: &'n N,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(node)?.visit_children(|| node.apply_children_in_scope(|c| apply_impl(c, f)))
        }

        apply_impl(self, &mut f)
    }

    /// Recursively rewrite the node's children in scope and then the node
    /// using `f` (a bottom-up post-order traversal).
    ///
    /// A synonym of [`Self::transform_up_in_scope`].
    fn transform_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.transform_up_in_scope(f)
    }

    /// Recursively rewrite the tree using `f` in a top-down (pre-order)
    /// fashion, limited to children in the same scope.
    ///
    /// `f` is applied to the node first, and then its children in scope.
    ///
    /// # See Also
    /// * [`TreeNode::transform_down`] for the same transformation but in all children ignoring scope
    /// * [`Self::transform_up_in_scope`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down_up_in_scope] for a combined traversal with closures
    /// * [`Self::rewrite_in_scope`] for a combined traversal with a visitor
    fn transform_down_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_impl<
            N: ScopedTreeNode,
            F: FnMut(N) -> Result<Transformed<N>>,
        >(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            f(node)?.transform_children(|n| {
                n.map_children_in_scope(|c| transform_down_impl(c, f))
            })
        }

        transform_down_impl(self, &mut f)
    }

    /// Recursively rewrite the node using `f` in a bottom-up (post-order)
    /// fashion, limited to children in the same scope.
    ///
    /// `f` is applied to the node's children in scope first, and then to the node itself.
    ///
    /// # See Also
    /// * [`Self::transform_down_in_scope`] for a top-down (pre-order) traversal.
    /// * [Self::transform_down_up_in_scope] for a combined traversal with closures
    /// * [`Self::rewrite_in_scope`] for a combined traversal with a visitor
    fn transform_up_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_up_impl<N: ScopedTreeNode, F: FnMut(N) -> Result<Transformed<N>>>(
            node: N,
            f: &mut F,
        ) -> Result<Transformed<N>> {
            node.map_children_in_scope(|c| transform_up_impl(c, f))?
                .transform_parent(f)
        }

        transform_up_impl(self, &mut f)
    }

    /// Transforms the node using `f_down` while traversing the tree top-down
    /// (pre-order), and using `f_up` while traversing the tree bottom-up
    /// (post-order), limited to children in the same scope.
    ///
    /// Same as [`TreeNode::transform_down_up`] but limited to the same scope.
    ///
    /// # See Also
    /// * [`Self::transform_up_in_scope`] for a bottom-up (post-order) traversal.
    /// * [Self::transform_down_in_scope] for a top-down (pre-order) traversal.
    /// * [`Self::rewrite_in_scope`] for a combined traversal with a visitor
    fn transform_down_up_in_scope<
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    >(
        self,
        mut f_down: FD,
        mut f_up: FU,
    ) -> Result<Transformed<Self>> {
        #[cfg_attr(feature = "recursive_protection", recursive::recursive)]
        fn transform_down_up_impl<
            N: ScopedTreeNode,
            FD: FnMut(N) -> Result<Transformed<N>>,
            FU: FnMut(N) -> Result<Transformed<N>>,
        >(
            node: N,
            f_down: &mut FD,
            f_up: &mut FU,
        ) -> Result<Transformed<N>> {
            handle_transform_recursion_in_scope!(
                f_down(node),
                |c| transform_down_up_impl(c, f_down, f_up),
                f_up
            )
        }

        transform_down_up_impl(self, &mut f_down, &mut f_up)
    }

    /// Returns true if `f` returns true for any node in the tree
    /// that is in the same scope.
    ///
    /// Stops recursion as soon as a matching node is found
    fn exists_in_scope<F: FnMut(&Self) -> Result<bool>>(&self, mut f: F) -> Result<bool> {
        let mut found = false;
        self.apply_in_scope(|n| {
            Ok(if f(n)? {
                found = true;
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })
        .map(|_| found)
    }

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`ScopedTreeNode`] trait for your own type, you
    /// should implement this method and [`Self::map_children_in_scope`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to inspect node's children that are in the same scope as this node (but not the node
    /// itself), scope is defined by the node.
    fn apply_children_in_scope<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Low-level API used to implement other APIs.
    ///
    /// If you want to implement the [`ScopedTreeNode`] trait for your own type, you
    /// should implement this method and [`Self::apply_children_in_scope`].
    ///
    /// Users should use one of the higher level APIs described on [`Self`].
    ///
    /// Description: Apply `f` to rewrite the node's children in scope (but not the node itself).
    fn map_children_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

/// A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
/// inspecting [`TreeNode`]s via [`TreeNode::visit`].
///
/// See [`TreeNode`] for more details on available APIs
///
/// When passed to [`TreeNode::visit`], [`TreeNodeVisitor::f_down`] and
/// [`TreeNodeVisitor::f_up`] are invoked recursively on the tree.
/// See [`TreeNodeRecursion`] for more details on controlling the traversal.
///
/// # Return Value
/// The returns value of `f_up` and `f_down` specifies how the tree walk should
/// proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
/// the recursion stops immediately.
///
/// Note: If using the default implementations of [`TreeNodeVisitor::f_up`] or
/// [`TreeNodeVisitor::f_down`] that do nothing, consider using
/// [`TreeNode::apply`] instead.
///
/// # See Also:
/// * [`TreeNode::rewrite`] to rewrite owned `TreeNode`s
pub trait TreeNodeVisitor<'n>: Sized {
    /// The node type which is visitable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree, before any children are visited.
    /// Default implementation continues the recursion.
    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    /// Invoked while traversing up the tree after children are visited. Default
    /// implementation continues the recursion.
    fn f_up(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

/// A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
/// rewriting [`TreeNode`]s via [`TreeNode::rewrite`].
///
/// For example you can implement this trait on a struct to rewrite `Expr` or
/// `LogicalPlan` that needs to track state during the rewrite.
///
/// See [`TreeNode`] for more details on available APIs
///
/// When passed to [`TreeNode::rewrite`], [`TreeNodeRewriter::f_down`] and
/// [`TreeNodeRewriter::f_up`] are invoked recursively on the tree.
/// See [`TreeNodeRecursion`] for more details on controlling the traversal.
///
/// # Return Value
/// The returns value of `f_up` and `f_down` specifies how the tree walk should
/// proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
/// the recursion stops immediately.
///
/// Note: If using the default implementations of [`TreeNodeRewriter::f_up`] or
/// [`TreeNodeRewriter::f_down`] that do nothing, consider using
/// [`TreeNode::transform_up`] or [`TreeNode::transform_down`] instead.
///
/// # See Also:
/// * [`TreeNode::visit`] to inspect borrowed `TreeNode`s
pub trait TreeNodeRewriter: Sized {
    /// The node type which is rewritable.
    type Node: TreeNode;

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    /// Invoked while traversing up the tree after all children have been rewritten.
    /// Default implementation returns the node as is and continues recursion.
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,
    /// In top-down traversals, skip recursing into children but continue with
    /// the next node, which actually means pruning of the subtree.
    ///
    /// In bottom-up traversals, bypass calling bottom-up closures till the next
    /// leaf node.
    ///
    /// In combined traversals, if it is the `f_down` (pre-order) phase, execution
    /// "jumps" to the next `f_up` (post-order) phase by shortcutting its children.
    /// If it is the `f_up` (post-order) phase, execution "jumps" to the next `f_down`
    /// (pre-order) phase by shortcutting its parent nodes until the first parent node
    /// having unvisited children path.
    Jump,
    /// Stop recursion.
    Stop,
}

impl TreeNodeRecursion {
    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's children.
    pub fn visit_children<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump => Ok(TreeNodeRecursion::Continue),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's sibling.
    pub fn visit_sibling<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => f(),
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
    /// value and the fact that `f` is visiting the current node's parent.
    pub fn visit_parent<F: FnOnce() -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            TreeNodeRecursion::Continue => f(),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

/// Result of tree walk / transformation APIs
///
/// `Transformed` is a wrapper around the tree node data (e.g. `Expr` or
/// `LogicalPlan`). It is used to indicate whether the node was transformed
/// and how the recursion should proceed.
///
/// [`TreeNode`] API users control the transformation by returning:
/// - The resulting (possibly transformed) node,
/// - `transformed`: flag indicating whether any change was made to the node
/// - `tnr`: [`TreeNodeRecursion`] specifying how to proceed with the recursion.
///
/// At the end of the transformation, the return value will contain:
/// - The final (possibly transformed) tree,
/// - `transformed`: flag indicating whether any change was made to the node
/// - `tnr`: [`TreeNodeRecursion`] specifying how the recursion ended.
///
/// See also
/// * [`Transformed::update_data`] to modify the node without changing the `transformed` flag
/// * [`Transformed::map_data`] for fallable operation that return the same type
/// * [`Transformed::transform_data`] to chain fallable transformations
/// * [`TransformedResult`] for working with `Result<Transformed<U>>`
///
/// # Examples
///
/// Use [`Transformed::yes`] and [`Transformed::no`] to signal that a node was
/// rewritten and the recursion should continue:
///
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
///
/// // Create a new `Transformed` object signaling the node was not rewritten
/// let ret = Transformed::no(expr.clone());
/// assert!(!ret.transformed);
///
/// // Create a new `Transformed` object signaling the node was rewritten
/// let ret = Transformed::yes(expr);
/// assert!(ret.transformed)
/// ```
///
/// Access the node within the `Transformed` object:
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
///
/// // `Transformed` object signaling the node was not rewritten
/// let ret = Transformed::no(expr.clone());
/// // Access the inner object using .data
/// assert_eq!(expr, ret.data);
/// ```
///
/// Transform the node within the `Transformed` object.
///
/// ```
/// # use datafusion_common::tree_node::Transformed;
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn orig_expr() -> i64 { 1 }
/// # fn make_new_expr(i: i64) -> i64 { 2 }
/// let expr = orig_expr();
/// let ret = Transformed::no(expr.clone())
///     .transform_data(|expr| {
///         // closure returns a result and potentially transforms the node
///         // in this example, it does transform the node
///         let new_expr = make_new_expr(expr);
///         Ok(Transformed::yes(new_expr))
///     })
///     .unwrap();
/// // transformed flag is the union of the original ans closure's  transformed flag
/// assert!(ret.transformed);
/// ```
/// # Example APIs that use `TreeNode`
/// - [`TreeNode`],
/// - [`TreeNode::rewrite`],
/// - [`TreeNode::transform_down`],
/// - [`TreeNode::transform_up`],
/// - [`TreeNode::transform_down_up`]
#[derive(PartialEq, Debug)]
pub struct Transformed<T> {
    pub data: T,
    pub transformed: bool,
    pub tnr: TreeNodeRecursion,
}

impl<T> Transformed<T> {
    /// Create a new `Transformed` object with the given information.
    pub fn new(data: T, transformed: bool, tnr: TreeNodeRecursion) -> Self {
        Self {
            data,
            transformed,
            tnr,
        }
    }

    /// Create a `Transformed` with `transformed` and [`TreeNodeRecursion::Continue`].
    pub fn new_transformed(data: T, transformed: bool) -> Self {
        Self::new(data, transformed, TreeNodeRecursion::Continue)
    }

    /// Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn yes(data: T) -> Self {
        Self::new(data, true, TreeNodeRecursion::Continue)
    }

    /// Wrapper for transformed data with [`TreeNodeRecursion::Stop`] statement.
    pub fn complete(data: T) -> Self {
        Self::new(data, true, TreeNodeRecursion::Stop)
    }

    /// Wrapper for unchanged data with [`TreeNodeRecursion::Continue`] statement.
    pub fn no(data: T) -> Self {
        Self::new(data, false, TreeNodeRecursion::Continue)
    }

    /// Applies an infallible `f` to the data of this [`Transformed`] object,
    /// without modifying the `transformed` flag.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed::new(f(self.data), self.transformed, self.tnr)
    }

    /// Applies a fallible `f` (returns `Result`) to the data of this
    /// [`Transformed`] object, without modifying the `transformed` flag.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed::new(data, self.transformed, self.tnr))
    }

    /// Applies a fallible transforming `f` to the data of this [`Transformed`]
    /// object.
    ///
    /// The returned `Transformed` object has the `transformed` flag set if either
    /// `self` or the return value of `f` have the `transformed` flag set.
    pub fn transform_data<U, F: FnOnce(T) -> Result<Transformed<U>>>(
        self,
        f: F,
    ) -> Result<Transformed<U>> {
        f(self.data).map(|mut t| {
            t.transformed |= self.transformed;
            t
        })
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's children.
    pub fn transform_children<F: FnOnce(T) -> Result<Transformed<T>>>(
        mut self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {
                return f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                });
            }
            TreeNodeRecursion::Jump => {
                self.tnr = TreeNodeRecursion::Continue;
            }
            TreeNodeRecursion::Stop => {}
        }
        Ok(self)
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's sibling.
    pub fn transform_sibling<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                f(self.data).map(|mut t| {
                    t.transformed |= self.transformed;
                    t
                })
            }
            TreeNodeRecursion::Stop => Ok(self),
        }
    }

    /// Maps the [`Transformed`] object to the result of the given `f` depending on the
    /// current [`TreeNodeRecursion`] value and the fact that `f` is changing the current
    /// node's parent.
    pub fn transform_parent<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => f(self.data).map(|mut t| {
                t.transformed |= self.transformed;
                t
            }),
            TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(self),
        }
    }
}

/// [`TreeNodeContainer`] contains elements that a function can be applied on or mapped.
/// The elements of the container are siblings so the continuation rules are similar to
/// [`TreeNodeRecursion::visit_sibling`] / [`Transformed::transform_sibling`].
pub trait TreeNodeContainer<'a, T: 'a>: Sized {
    /// Applies `f` to all elements of the container.
    /// This method is usually called from [`TreeNode::apply_children`] implementations as
    /// a node is actually a container of the node's children.
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Maps all elements of the container with `f`.
    /// This method is usually called from [`TreeNode::map_children`] implementations as
    /// a node is actually a container of the node's children.
    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>>;
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Box<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.as_ref().apply_elements(f)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        (*self).map_elements(f)?.map_data(|c| Ok(Self::new(c)))
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T> + Clone> TreeNodeContainer<'a, T> for Arc<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.as_ref().apply_elements(f)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        Arc::unwrap_or_clone(self)
            .map_elements(f)?
            .map_data(|c| Ok(Arc::new(c)))
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Option<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        match self {
            Some(t) => t.apply_elements(f),
            None => Ok(TreeNodeRecursion::Continue),
        }
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.map_or(Ok(Transformed::no(None)), |c| {
            c.map_elements(f)?.map_data(|c| Ok(Some(c)))
        })
    }
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T> for Vec<C> {
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for c in self {
            tnr = c.apply_elements(&mut f)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.into_iter()
            .map(|c| match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                    c.map_elements(&mut f).map(|result| {
                        tnr = result.tnr;
                        transformed |= result.transformed;
                        result.data
                    })
                }
                TreeNodeRecursion::Stop => Ok(c),
            })
            .collect::<Result<Vec<_>>>()
            .map(|data| Transformed::new(data, transformed, tnr))
    }
}

impl<'a, T: 'a, K: Eq + Hash, C: TreeNodeContainer<'a, T>> TreeNodeContainer<'a, T>
    for HashMap<K, C>
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for c in self.values() {
            tnr = c.apply_elements(&mut f)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.into_iter()
            .map(|(k, c)| match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                    c.map_elements(&mut f).map(|result| {
                        tnr = result.tnr;
                        transformed |= result.transformed;
                        (k, result.data)
                    })
                }
                TreeNodeRecursion::Stop => Ok((k, c)),
            })
            .collect::<Result<HashMap<_, _>>>()
            .map(|data| Transformed::new(data, transformed, tnr))
    }
}

impl<'a, T: 'a, C0: TreeNodeContainer<'a, T>, C1: TreeNodeContainer<'a, T>>
    TreeNodeContainer<'a, T> for (C0, C1)
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.0
            .map_elements(&mut f)?
            .map_data(|new_c0| Ok((new_c0, self.1)))?
            .transform_sibling(|(new_c0, c1)| {
                c1.map_elements(&mut f)?
                    .map_data(|new_c1| Ok((new_c0, new_c1)))
            })
    }
}

impl<
    'a,
    T: 'a,
    C0: TreeNodeContainer<'a, T>,
    C1: TreeNodeContainer<'a, T>,
    C2: TreeNodeContainer<'a, T>,
> TreeNodeContainer<'a, T> for (C0, C1, C2)
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))?
            .visit_sibling(|| self.2.apply_elements(&mut f))
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.0
            .map_elements(&mut f)?
            .map_data(|new_c0| Ok((new_c0, self.1, self.2)))?
            .transform_sibling(|(new_c0, c1, c2)| {
                c1.map_elements(&mut f)?
                    .map_data(|new_c1| Ok((new_c0, new_c1, c2)))
            })?
            .transform_sibling(|(new_c0, new_c1, c2)| {
                c2.map_elements(&mut f)?
                    .map_data(|new_c2| Ok((new_c0, new_c1, new_c2)))
            })
    }
}

impl<
    'a,
    T: 'a,
    C0: TreeNodeContainer<'a, T>,
    C1: TreeNodeContainer<'a, T>,
    C2: TreeNodeContainer<'a, T>,
    C3: TreeNodeContainer<'a, T>,
> TreeNodeContainer<'a, T> for (C0, C1, C2, C3)
{
    fn apply_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))?
            .visit_sibling(|| self.2.apply_elements(&mut f))?
            .visit_sibling(|| self.3.apply_elements(&mut f))
    }

    fn map_elements<F: FnMut(T) -> Result<Transformed<T>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        self.0
            .map_elements(&mut f)?
            .map_data(|new_c0| Ok((new_c0, self.1, self.2, self.3)))?
            .transform_sibling(|(new_c0, c1, c2, c3)| {
                c1.map_elements(&mut f)?
                    .map_data(|new_c1| Ok((new_c0, new_c1, c2, c3)))
            })?
            .transform_sibling(|(new_c0, new_c1, c2, c3)| {
                c2.map_elements(&mut f)?
                    .map_data(|new_c2| Ok((new_c0, new_c1, new_c2, c3)))
            })?
            .transform_sibling(|(new_c0, new_c1, new_c2, c3)| {
                c3.map_elements(&mut f)?
                    .map_data(|new_c3| Ok((new_c0, new_c1, new_c2, new_c3)))
            })
    }
}

/// [`TreeNodeRefContainer`] contains references to elements that a function can be
/// applied on. The elements of the container are siblings so the continuation rules are
/// similar to [`TreeNodeRecursion::visit_sibling`].
///
/// This container is similar to [`TreeNodeContainer`], but the lifetime of the reference
/// elements (`T`) are not derived from the container's lifetime.
/// A typical usage of this container is in `Expr::apply_children` when we need to
/// construct a temporary container to be able to call `apply_ref_elements` on a
/// collection of tree node references. But in that case the container's temporary
/// lifetime is different to the lifetime of tree nodes that we put into it.
/// Please find an example use case in `Expr::apply_children` with the `Expr::Case` case.
///
/// Most of the cases we don't need to create a temporary container with
/// `TreeNodeRefContainer`, but we can just call `TreeNodeContainer::apply_elements`.
/// Please find an example use case in `Expr::apply_children` with the `Expr::GroupingSet`
/// case.
pub trait TreeNodeRefContainer<'a, T: 'a>: Sized {
    /// Applies `f` to all elements of the container.
    /// This method is usually called from [`TreeNode::apply_children`] implementations as
    /// a node is actually a container of the node's children.
    fn apply_ref_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &self,
        f: F,
    ) -> Result<TreeNodeRecursion>;
}

impl<'a, T: 'a, C: TreeNodeContainer<'a, T>> TreeNodeRefContainer<'a, T> for Vec<&'a C> {
    fn apply_ref_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for c in self {
            tnr = c.apply_elements(&mut f)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }
}

impl<'a, T: 'a, C0: TreeNodeContainer<'a, T>, C1: TreeNodeContainer<'a, T>>
    TreeNodeRefContainer<'a, T> for (&'a C0, &'a C1)
{
    fn apply_ref_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))
    }
}

impl<
    'a,
    T: 'a,
    C0: TreeNodeContainer<'a, T>,
    C1: TreeNodeContainer<'a, T>,
    C2: TreeNodeContainer<'a, T>,
> TreeNodeRefContainer<'a, T> for (&'a C0, &'a C1, &'a C2)
{
    fn apply_ref_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))?
            .visit_sibling(|| self.2.apply_elements(&mut f))
    }
}

impl<
    'a,
    T: 'a,
    C0: TreeNodeContainer<'a, T>,
    C1: TreeNodeContainer<'a, T>,
    C2: TreeNodeContainer<'a, T>,
    C3: TreeNodeContainer<'a, T>,
> TreeNodeRefContainer<'a, T> for (&'a C0, &'a C1, &'a C2, &'a C3)
{
    fn apply_ref_elements<F: FnMut(&'a T) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        self.0
            .apply_elements(&mut f)?
            .visit_sibling(|| self.1.apply_elements(&mut f))?
            .visit_sibling(|| self.2.apply_elements(&mut f))?
            .visit_sibling(|| self.3.apply_elements(&mut f))
    }
}

/// Transformation helper to process a sequence of iterable tree nodes that are siblings.
pub trait TreeNodeIterator: Iterator {
    /// Apples `f` to each item in this iterator
    ///
    /// Visits all items in the iterator unless
    /// `f` returns an error or `f` returns `TreeNodeRecursion::Stop`.
    ///
    /// # Returns
    /// Error if `f` returns an error or `Ok(TreeNodeRecursion)` from the last invocation
    /// of `f` or `Continue` if the iterator is empty
    fn apply_until_stop<F: FnMut(Self::Item) -> Result<TreeNodeRecursion>>(
        self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Apples `f` to each item in this iterator
    ///
    /// Visits all items in the iterator unless
    /// `f` returns an error or `f` returns `TreeNodeRecursion::Stop`.
    ///
    /// # Returns
    /// Error if `f` returns an error
    ///
    /// Ok(Transformed) such that:
    /// 1. `transformed` is true if any return from `f` had transformed true
    /// 2. `data` from the last invocation of `f`
    /// 3. `tnr` from the last invocation of `f` or `Continue` if the iterator is empty
    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        f: F,
    ) -> Result<Transformed<Vec<Self::Item>>>;
}

impl<I: Iterator> TreeNodeIterator for I {
    fn apply_until_stop<F: FnMut(Self::Item) -> Result<TreeNodeRecursion>>(
        self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        let mut tnr = TreeNodeRecursion::Continue;
        for i in self {
            tnr = f(i)?;
            match tnr {
                TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {}
                TreeNodeRecursion::Stop => return Ok(TreeNodeRecursion::Stop),
            }
        }
        Ok(tnr)
    }

    fn map_until_stop_and_collect<
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
    >(
        self,
        mut f: F,
    ) -> Result<Transformed<Vec<Self::Item>>> {
        let mut tnr = TreeNodeRecursion::Continue;
        let mut transformed = false;
        self.map(|item| match tnr {
            TreeNodeRecursion::Continue | TreeNodeRecursion::Jump => {
                f(item).map(|result| {
                    tnr = result.tnr;
                    transformed |= result.transformed;
                    result.data
                })
            }
            TreeNodeRecursion::Stop => Ok(item),
        })
        .collect::<Result<Vec<_>>>()
        .map(|data| Transformed::new(data, transformed, tnr))
    }
}

/// Transformation helper to access [`Transformed`] fields in a [`Result`] easily.
///
/// # Example
/// Access the internal data of a `Result<Transformed<T>>`
/// as a `Result<T>` using the `data` method:
/// ```
/// # use datafusion_common::Result;
/// # use datafusion_common::tree_node::{Transformed, TransformedResult};
/// # // note use i64 instead of Expr as Expr is not in datafusion-common
/// # fn update_expr() -> i64 { 1 }
/// # fn main() -> Result<()> {
/// let transformed: Result<Transformed<_>> = Ok(Transformed::yes(update_expr()));
/// // access the internal data of the transformed result, or return the error
/// let transformed_expr = transformed.data()?;
/// # Ok(())
/// # }
/// ```
pub trait TransformedResult<T> {
    fn data(self) -> Result<T>;

    fn transformed(self) -> Result<bool>;

    fn tnr(self) -> Result<TreeNodeRecursion>;
}

impl<T> TransformedResult<T> for Result<Transformed<T>> {
    fn data(self) -> Result<T> {
        self.map(|t| t.data)
    }

    fn transformed(self) -> Result<bool> {
        self.map(|t| t.transformed)
    }

    fn tnr(self) -> Result<TreeNodeRecursion> {
        self.map(|t| t.tnr)
    }
}

/// Helper trait for implementing [`TreeNode`] that have children stored as
/// `Arc`s. If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`TreeNode`].
pub trait DynTreeNode {
    /// Returns all children of the specified `TreeNode`.
    fn arc_children(&self) -> Vec<&Arc<Self>>;

    /// Constructs a new node with the specified children.
    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

/// Blanket implementation for any `Arc<T>` where `T` implements [`DynTreeNode`]
/// (such as [`Arc<dyn PhysicalExpr>`]).
impl<T: DynTreeNode + ?Sized> TreeNode for Arc<T> {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.arc_children().into_iter().apply_until_stop(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let children = self.arc_children();
        if !children.is_empty() {
            let new_children = children
                .into_iter()
                .cloned()
                .map_until_stop_and_collect(f)?;
            // Propagate up `new_children.transformed` and `new_children.tnr`
            // along with the node containing transformed children.
            if new_children.transformed {
                let arc_self = Arc::clone(&self);
                new_children.map_data(|new_children| {
                    self.with_new_arc_children(arc_self, new_children)
                })
            } else {
                Ok(Transformed::new(self, false, new_children.tnr))
            }
        } else {
            Ok(Transformed::no(self))
        }
    }
}

/// Helper trait for implementing [`ScopedTreeNode`] that have children stored as
/// `Arc`s. If some trait object, such as `dyn T`, implements this trait,
/// its related `Arc<dyn T>` will automatically implement [`ScopedTreeNode`].
pub trait DynScopedTreeNode: DynTreeNode {
    /// Returns all children of the specified `ScopedTreeNode`.
    fn arc_children_in_scope(&self) -> Vec<&Arc<Self>>;

    /// Constructs a new node with the specified children in scope.
    fn with_new_arc_children_in_scope(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>>;
}

/// Blanket implementation for any `Arc<T>` where `T` implements [`DynScopedTreeNode`]
/// (such as [`Arc<dyn PhysicalExpr>`]).
impl<T: DynScopedTreeNode + ?Sized> ScopedTreeNode for Arc<T> {
    fn apply_children_in_scope<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.arc_children_in_scope().into_iter().apply_until_stop(f)
    }

    fn map_children_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let children_in_scope = self.arc_children_in_scope();
        if !children_in_scope.is_empty() {
            let new_children_in_scope = children_in_scope
                .into_iter()
                .cloned()
                .map_until_stop_and_collect(f)?;
            // Propagate up `new_children_in_scope.transformed` and `new_children_in_scope.tnr`
            // along with the node containing transformed children.
            if new_children_in_scope.transformed {
                let arc_self = Arc::clone(&self);
                new_children_in_scope.map_data(|new_children_in_scope| {
                    self.with_new_arc_children_in_scope(arc_self, new_children_in_scope)
                })
            } else {
                Ok(Transformed::new(self, false, new_children_in_scope.tnr))
            }
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
    fn children(&self) -> &[Self];

    /// Detaches the node from its children, returning the node itself and its detached children.
    fn take_children(self) -> (Self, Vec<Self>);

    /// Reattaches updated child nodes to the node, returning the updated node.
    fn with_new_children(self, children: Vec<Self>) -> Result<Self>;
}

impl<T: ConcreteTreeNode> TreeNode for T {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.children().iter().apply_until_stop(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        let (new_self, children) = self.take_children();
        if !children.is_empty() {
            let new_children = children.into_iter().map_until_stop_and_collect(f)?;
            // Propagate up `new_children.transformed` and `new_children.tnr` along with
            // the node containing transformed children.
            new_children.map_data(|new_children| new_self.with_new_children(new_children))
        } else {
            Ok(Transformed::no(new_self))
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashMap;
    use std::fmt::Display;

    use crate::Result;
    use crate::tree_node::{
        ScopedTreeNode, Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion,
        TreeNodeRewriter, TreeNodeVisitor,
    };

    #[derive(Debug, Eq, Hash, PartialEq, Clone)]
    pub struct TestTreeNode<T> {
        pub(crate) children: Vec<TestTreeNode<T>>,
        pub(crate) children_in_same_scope: Vec<TestTreeNode<T>>,
        pub(crate) data: T,
    }

    impl<T: Clone> TestTreeNode<T> {
        /// Creates a node where all children are in the same scope.
        pub(crate) fn new(children: Vec<TestTreeNode<T>>, data: T) -> Self {
            Self {
                children_in_same_scope: children.clone(),
                children,
                data,
            }
        }

        pub(crate) fn new_leaf(data: T) -> Self {
            Self {
                children: vec![],
                children_in_same_scope: vec![],
                data,
            }
        }

        pub(crate) fn is_leaf(&self) -> bool {
            self.children.is_empty()
        }

        /// Strip children_in_new_scope recursively - used to compare trees
        /// in TreeNode tests where children_in_new_scope is not relevant.
        fn strip_scope(self) -> Self {
            Self {
                children: self.children.into_iter().map(|c| c.strip_scope()).collect(),
                children_in_same_scope: vec![],
                data: self.data,
            }
        }
    }

    impl<T: Clone + PartialEq> TestTreeNode<T> {
        /// Creates a node with explicit `children` (all, in order).
        /// `out_of_scope_children` are children that start a new scope
        /// (i.e., NOT in the current node's scope). The remaining children
        /// are computed as `children_in_same_scope`.
        pub(crate) fn new_mixed(
            all_children: Vec<TestTreeNode<T>>,
            out_of_scope_children: Vec<TestTreeNode<T>>,
            data: T,
        ) -> Self {
            let children_in_same_scope = all_children
                .iter()
                .filter(|c| !out_of_scope_children.contains(c))
                .cloned()
                .collect();
            Self {
                children: all_children,
                children_in_same_scope,
                data,
            }
        }
    }

    impl<T> TreeNode for TestTreeNode<T> {
        fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
            &'n self,
            f: F,
        ) -> Result<TreeNodeRecursion> {
            self.children.apply_elements(f)
        }

        fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
            self,
            f: F,
        ) -> Result<Transformed<Self>> {
            Ok(self
                .children
                .map_elements(f)?
                .update_data(|new_children| Self {
                    children: new_children,
                    ..self
                }))
        }
    }

    impl<T> ScopedTreeNode for TestTreeNode<T> {
        fn apply_children_in_scope<
            'n,
            F: FnMut(&'n Self) -> Result<TreeNodeRecursion>,
        >(
            &'n self,
            f: F,
        ) -> Result<TreeNodeRecursion> {
            self.children_in_same_scope.apply_elements(f)
        }

        fn map_children_in_scope<F: FnMut(Self) -> Result<Transformed<Self>>>(
            self,
            f: F,
        ) -> Result<Transformed<Self>> {
            Ok(self.children_in_same_scope.map_elements(f)?.update_data(
                |new_children| Self {
                    children_in_same_scope: new_children,
                    ..self
                },
            ))
        }
    }

    impl<'a, T: 'a> TreeNodeContainer<'a, Self> for TestTreeNode<T> {
        fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
            &'a self,
            mut f: F,
        ) -> Result<TreeNodeRecursion> {
            f(self)
        }

        fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
            self,
            mut f: F,
        ) -> Result<Transformed<Self>> {
            f(self)
        }
    }

    //                                 J
    //                                 |
    //                                 I
    //                                 |
    //                                 F (mixed)
    //                                /  \
    //                    E (new scope)   G (same scope as F)
    //                    |               |
    //                    C (mixed)       H
    //                  /   \
    // B (Same scope as C)   D (new scope)
    // |
    // A
    //
    // TreeNode (children) traversal visits ALL nodes: J, I, F, E, C, B, A, D, G, H
    //   (new/new_mixed set both children and children_in_new_scope)
    //
    // ScopedTreeNode (children_in_new_scope) traversal visits: J, I, F, G, H
    //   (skips E, C, B, A, D — E and D are in new scopes, the rest are
    //    unreachable via scoped traversal)
    fn test_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_c = TestTreeNode::new_mixed(
            vec![node_b, node_d.clone()],
            vec![node_d],
            "c".to_string(),
        );
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new_mixed(
            vec![node_e.clone(), node_g],
            vec![node_e],
            "f".to_string(),
        );
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    // Continue on all nodes
    // Expected visits in a combined traversal
    fn all_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
            "f_up(b)",
            "f_down(d)",
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // Expected transformed tree after a combined traversal
    fn transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new_leaf("f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    // Expected transformed tree after a top-down traversal
    fn transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // Expected transformed tree after a bottom-up traversal
    fn transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_up(b)".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new_leaf("f_up(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_up(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_up(j)".to_string())
    }

    // f_down Jump on A node
    fn f_down_jump_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
            "f_up(b)",
            "f_down(d)",
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_down Jump on E node
    fn f_down_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_up(e)",
            "f_down(g)",
            "f_down(h)",
            "f_up(h)",
            "f_up(g)",
            "f_up(f)",
            "f_up(i)",
            "f_up(j)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new_leaf("f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_down_jump_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_up Jump on A node
    fn f_up_jump_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
            "f_down(d)",
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_jump_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(f_down(d))".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new_leaf("f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_up_jump_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new_leaf("f_up(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_up(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_up(j)".to_string())
    }

    // f_up Jump on E node
    fn f_up_jump_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
            "f_up(b)",
            "f_down(d)",
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_jump_on_e_transformed_tree() -> TestTreeNode<String> {
        transformed_tree()
    }

    fn f_up_jump_on_e_transformed_up_tree() -> TestTreeNode<String> {
        transformed_up_tree()
    }

    // f_down Stop on A node

    fn f_down_stop_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_down Stop on E node
    fn f_down_stop_on_e_visits() -> Vec<String> {
        vec!["f_down(j)", "f_down(i)", "f_down(f)", "f_down(e)"]
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }

    fn f_down_stop_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // f_up Stop on A node
    fn f_up_stop_on_a_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_down(b)".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "b".to_string());
        let node_d = TestTreeNode::new_leaf("d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    // f_up Stop on E node
    fn f_up_stop_on_e_visits() -> Vec<String> {
        vec![
            "f_down(j)",
            "f_down(i)",
            "f_down(f)",
            "f_down(e)",
            "f_down(c)",
            "f_down(b)",
            "f_down(a)",
            "f_up(a)",
            "f_up(b)",
            "f_down(d)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_e_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new_leaf("f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![node_a], "f_up(b)".to_string());
        let node_d = TestTreeNode::new_leaf("f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new_leaf("h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "i".to_string());
        TestTreeNode::new(vec![node_i], "j".to_string())
    }

    fn down_visits(visits: Vec<String>) -> Vec<String> {
        visits
            .into_iter()
            .filter(|v| v.starts_with("f_down"))
            .collect()
    }

    type TestVisitorF<T> = Box<dyn FnMut(&TestTreeNode<T>) -> Result<TreeNodeRecursion>>;

    struct TestVisitor<T> {
        visits: Vec<String>,
        f_down: TestVisitorF<T>,
        f_up: TestVisitorF<T>,
    }

    impl<T> TestVisitor<T> {
        fn new(f_down: TestVisitorF<T>, f_up: TestVisitorF<T>) -> Self {
            Self {
                visits: vec![],
                f_down,
                f_up,
            }
        }
    }

    impl<'n, T: Display> TreeNodeVisitor<'n> for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_up({})", node.data));
            (*self.f_up)(node)
        }
    }

    fn visit_continue<T>(_: &TestTreeNode<T>) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn visit_event_on<T: PartialEq, D: Into<T>>(
        data: D,
        event: TreeNodeRecursion,
    ) -> impl FnMut(&TestTreeNode<T>) -> Result<TreeNodeRecursion> {
        let d = data.into();
        move |node| {
            Ok(if node.data == d {
                event
            } else {
                TreeNodeRecursion::Continue
            })
        }
    }

    macro_rules! visit_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_VISITS:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut visitor = TestVisitor::new(Box::new($F_DOWN), Box::new($F_UP));
                tree.visit(&mut visitor)?;
                assert_eq!(visitor.visits, $EXPECTED_VISITS);

                Ok(())
            }
        };
    }

    macro_rules! test_apply {
        ($NAME:ident, $F:expr, $EXPECTED_VISITS:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut visits = vec![];
                tree.apply(|node| {
                    visits.push(format!("f_down({})", node.data));
                    $F(node)
                })?;
                assert_eq!(visits, $EXPECTED_VISITS);

                Ok(())
            }
        };
    }

    type TestRewriterF<T> =
        Box<dyn FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>>>;

    struct TestRewriter<T> {
        f_down: TestRewriterF<T>,
        f_up: TestRewriterF<T>,
    }

    impl<T> TestRewriter<T> {
        fn new(f_down: TestRewriterF<T>, f_up: TestRewriterF<T>) -> Self {
            Self { f_down, f_up }
        }
    }

    impl<T: Display> TreeNodeRewriter for TestRewriter<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
            (*self.f_up)(node)
        }
    }

    fn transform_yes<N: Display, T: Display + From<String> + Clone>(
        transformation_name: N,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        move |node| {
            Ok(Transformed::yes(TestTreeNode::new(
                node.children,
                format!("{}({})", transformation_name, node.data).into(),
            )))
        }
    }

    fn transform_and_event_on<
        N: Display,
        T: PartialEq + Display + From<String> + Clone,
        D: Into<T>,
    >(
        transformation_name: N,
        data: D,
        event: TreeNodeRecursion,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        let d = data.into();
        move |node| {
            let new_node = TestTreeNode::new(
                node.children,
                format!("{}({})", transformation_name, node.data).into(),
            );
            Ok(if node.data == d {
                Transformed::new(new_node, true, event)
            } else {
                Transformed::yes(new_node)
            })
        }
    }

    macro_rules! rewrite_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let mut rewriter = TestRewriter::new(Box::new($F_DOWN), Box::new($F_UP));
                let actual = tree.rewrite(&mut rewriter)?;
                let actual_stripped = actual.update_data(|d| d.strip_scope());
                let expected_stripped = ($EXPECTED_TREE).update_data(|d| d.strip_scope());
                assert_eq!(actual_stripped, expected_stripped);

                Ok(())
            }
        };
    }

    macro_rules! transform_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let actual = tree.transform_down_up($F_DOWN, $F_UP)?;
                let actual_stripped = actual.update_data(|d| d.strip_scope());
                let expected_stripped = ($EXPECTED_TREE).update_data(|d| d.strip_scope());
                assert_eq!(actual_stripped, expected_stripped);

                Ok(())
            }
        };
    }

    macro_rules! transform_down_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let actual = tree.transform_down($F)?;
                let actual_stripped = actual.update_data(|d| d.strip_scope());
                let expected_stripped = ($EXPECTED_TREE).update_data(|d| d.strip_scope());
                assert_eq!(actual_stripped, expected_stripped);

                Ok(())
            }
        };
    }

    macro_rules! transform_up_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                let actual = tree.transform_up($F)?;
                let actual_stripped = actual.update_data(|d| d.strip_scope());
                let expected_stripped = ($EXPECTED_TREE).update_data(|d| d.strip_scope());
                assert_eq!(actual_stripped, expected_stripped);

                Ok(())
            }
        };
    }

    visit_test!(test_visit, visit_continue, visit_continue, all_visits());
    visit_test!(
        test_visit_f_down_jump_on_a,
        visit_event_on("a", TreeNodeRecursion::Jump),
        visit_continue,
        f_down_jump_on_a_visits()
    );
    visit_test!(
        test_visit_f_down_jump_on_e,
        visit_event_on("e", TreeNodeRecursion::Jump),
        visit_continue,
        f_down_jump_on_e_visits()
    );
    visit_test!(
        test_visit_f_up_jump_on_a,
        visit_continue,
        visit_event_on("a", TreeNodeRecursion::Jump),
        f_up_jump_on_a_visits()
    );
    visit_test!(
        test_visit_f_up_jump_on_e,
        visit_continue,
        visit_event_on("e", TreeNodeRecursion::Jump),
        f_up_jump_on_e_visits()
    );
    visit_test!(
        test_visit_f_down_stop_on_a,
        visit_event_on("a", TreeNodeRecursion::Stop),
        visit_continue,
        f_down_stop_on_a_visits()
    );
    visit_test!(
        test_visit_f_down_stop_on_e,
        visit_event_on("e", TreeNodeRecursion::Stop),
        visit_continue,
        f_down_stop_on_e_visits()
    );
    visit_test!(
        test_visit_f_up_stop_on_a,
        visit_continue,
        visit_event_on("a", TreeNodeRecursion::Stop),
        f_up_stop_on_a_visits()
    );
    visit_test!(
        test_visit_f_up_stop_on_e,
        visit_continue,
        visit_event_on("e", TreeNodeRecursion::Stop),
        f_up_stop_on_e_visits()
    );

    test_apply!(test_apply, visit_continue, down_visits(all_visits()));
    test_apply!(
        test_apply_f_down_jump_on_a,
        visit_event_on("a", TreeNodeRecursion::Jump),
        down_visits(f_down_jump_on_a_visits())
    );
    test_apply!(
        test_apply_f_down_jump_on_e,
        visit_event_on("e", TreeNodeRecursion::Jump),
        down_visits(f_down_jump_on_e_visits())
    );
    test_apply!(
        test_apply_f_down_stop_on_a,
        visit_event_on("a", TreeNodeRecursion::Stop),
        down_visits(f_down_stop_on_a_visits())
    );
    test_apply!(
        test_apply_f_down_stop_on_e,
        visit_event_on("e", TreeNodeRecursion::Stop),
        down_visits(f_down_stop_on_e_visits())
    );

    rewrite_test!(
        test_rewrite,
        transform_yes("f_down"),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(f_down_jump_on_e_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_up_jump_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_up_jump_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_tree())
    );
    rewrite_test!(
        test_rewrite_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_up_stop_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    rewrite_test!(
        test_rewrite_f_up_stop_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_test!(
        test_transform,
        transform_yes("f_down"),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    transform_test!(
        test_transform_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(transformed_tree())
    );
    transform_test!(
        test_transform_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        transform_yes("f_up"),
        Transformed::yes(f_down_jump_on_e_transformed_tree())
    );
    transform_test!(
        test_transform_f_up_jump_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_tree())
    );
    transform_test!(
        test_transform_f_up_jump_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_tree())
    );
    transform_test!(
        test_transform_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        transform_yes("f_up"),
        Transformed::new(
            f_down_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_up_stop_on_a,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(a)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_test!(
        test_transform_f_up_stop_on_e,
        transform_yes("f_down"),
        transform_and_event_on("f_up", "f_down(e)", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_down_test!(
        test_transform_down,
        transform_yes("f_down"),
        Transformed::yes(transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_jump_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Jump),
        Transformed::yes(f_down_jump_on_a_transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_jump_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Jump),
        Transformed::yes(f_down_jump_on_e_transformed_down_tree())
    );
    transform_down_test!(
        test_transform_down_f_down_stop_on_a,
        transform_and_event_on("f_down", "a", TreeNodeRecursion::Stop),
        Transformed::new(
            f_down_stop_on_a_transformed_down_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_down_test!(
        test_transform_down_f_down_stop_on_e,
        transform_and_event_on("f_down", "e", TreeNodeRecursion::Stop),
        Transformed::new(
            f_down_stop_on_e_transformed_down_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    transform_up_test!(
        test_transform_up,
        transform_yes("f_up"),
        Transformed::yes(transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_jump_on_a,
        transform_and_event_on("f_up", "a", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_a_transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_jump_on_e,
        transform_and_event_on("f_up", "e", TreeNodeRecursion::Jump),
        Transformed::yes(f_up_jump_on_e_transformed_up_tree())
    );
    transform_up_test!(
        test_transform_up_f_up_stop_on_a,
        transform_and_event_on("f_up", "a", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_a_transformed_up_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );
    transform_up_test!(
        test_transform_up_f_up_stop_on_e,
        transform_and_event_on("f_up", "e", TreeNodeRecursion::Stop),
        Transformed::new(
            f_up_stop_on_e_transformed_up_tree(),
            true,
            TreeNodeRecursion::Stop
        )
    );

    //             F
    //          /  |  \
    //       /     |     \
    //    E        C        A
    //    |      /   \
    //    C     B     D
    //  /   \         |
    // B     D        A
    //       |
    //       A
    #[test]
    fn test_apply_and_visit_references() -> Result<()> {
        let node_a = TestTreeNode::new_leaf("a".to_string());
        let node_b = TestTreeNode::new_leaf("b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_a_2 = TestTreeNode::new_leaf("a".to_string());
        let node_b_2 = TestTreeNode::new_leaf("b".to_string());
        let node_d_2 = TestTreeNode::new(vec![node_a_2], "d".to_string());
        let node_c_2 = TestTreeNode::new(vec![node_b_2, node_d_2], "c".to_string());
        let node_a_3 = TestTreeNode::new_leaf("a".to_string());
        let tree = TestTreeNode::new(vec![node_e, node_c_2, node_a_3], "f".to_string());

        let node_f_ref = &tree;
        let node_e_ref = &node_f_ref.children[0];
        let node_c_ref = &node_e_ref.children[0];
        let node_b_ref = &node_c_ref.children[0];
        let node_d_ref = &node_c_ref.children[1];
        let node_a_ref = &node_d_ref.children[0];

        let mut m: HashMap<&TestTreeNode<String>, usize> = HashMap::new();
        tree.apply(|e| {
            *m.entry(e).or_insert(0) += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;

        let expected = HashMap::from([
            (node_f_ref, 1),
            (node_e_ref, 1),
            (node_c_ref, 2),
            (node_d_ref, 2),
            (node_b_ref, 2),
            (node_a_ref, 3),
        ]);
        assert_eq!(m, expected);

        struct TestVisitor<'n> {
            m: HashMap<&'n TestTreeNode<String>, (usize, usize)>,
        }

        impl<'n> TreeNodeVisitor<'n> for TestVisitor<'n> {
            type Node = TestTreeNode<String>;

            fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
                let (down_count, _) = self.m.entry(node).or_insert((0, 0));
                *down_count += 1;
                Ok(TreeNodeRecursion::Continue)
            }

            fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
                let (_, up_count) = self.m.entry(node).or_insert((0, 0));
                *up_count += 1;
                Ok(TreeNodeRecursion::Continue)
            }
        }

        let mut visitor = TestVisitor { m: HashMap::new() };
        tree.visit(&mut visitor)?;

        let expected = HashMap::from([
            (node_f_ref, (1, 1)),
            (node_e_ref, (1, 1)),
            (node_c_ref, (2, 2)),
            (node_d_ref, (2, 2)),
            (node_b_ref, (2, 2)),
            (node_a_ref, (3, 3)),
        ]);
        assert_eq!(visitor.m, expected);

        Ok(())
    }

    #[cfg(feature = "recursive_protection")]
    #[test]
    fn test_large_tree() {
        let mut item = TestTreeNode::new_leaf("initial".to_string());
        for i in 0..3000 {
            item = TestTreeNode::new(vec![item], format!("parent-{i}"));
        }

        let mut visitor =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));

        item.visit(&mut visitor).unwrap();
    }

    // =====================================================================
    // ScopedTreeNode tests
    //
    // Models nested scope boundaries, like expressions with nested lambdas.
    // Each scope has multiple nodes (>=3) to test sibling traversal within
    // a scope, plus some scopes with only 1 in-scope child.
    //
    // OUTER SCOPE (root traversal visits these):
    //   root → and → [plus, gt]
    //     plus → [col_a, col_b]
    //     gt   → [col_c, Lambda1]  ← Lambda1 is out of scope
    //
    //   Scoped traversal: root, and, plus, col_a, col_b, gt, col_c
    //   (7 nodes, Lambda1 not entered)
    //
    // LAMBDA1 SCOPE (Lambda1's own traversal):
    //   lambda1 → [list_col, cmp]
    //     cmp   → [idx_col, Lambda2]  ← Lambda2 is out of scope
    //
    //   Scoped traversal: lambda1, list_col, cmp, idx_col
    //   (4 nodes, Lambda2 not entered)
    //
    // LAMBDA2 SCOPE (Lambda2's own traversal — only 1 in-scope child):
    //   lambda2 → [inner_col]
    //
    //   Scoped traversal: lambda2, inner_col
    //   (2 nodes)
    //
    //                         root
    //                          |      (scoped)
    //                         and  (scoped: [plus, gt])
    //                        /   \
    //                     plus    gt  (mixed: in_scope=[col_c], out=[Lambda1])
    //                    /   \    / \
    //                col_a col_b col_c Lambda1 ← scope boundary
    //                                  |
    //                              (in_scope=[list_col, cmp], out=[])
    //                                /       \
    //                          list_col      cmp (mixed: in_scope=[idx_col], out=[Lambda2])
    //                                        / \
    //                                  idx_col  Lambda2 ← nested scope boundary
    //                                           |
    //                                       (in_scope=[inner_col])
    //                                           |
    //                                       inner_col
    // =====================================================================

    fn scoped_test_tree() -> TestTreeNode<String> {
        // Leaves for outer scope
        let col_a = TestTreeNode::new_leaf("col_a".to_string());
        let col_b = TestTreeNode::new_leaf("col_b".to_string());
        let col_c = TestTreeNode::new_leaf("col_c".to_string());

        let plus = TestTreeNode::new(vec![col_a, col_b], "plus".to_string());

        // --- Innermost lambda scope: Lambda2 → inner_col ---
        let inner_col = TestTreeNode::new_leaf("inner_col".to_string());
        let lambda2 = TestTreeNode::new(vec![inner_col], "lambda2".to_string());

        // --- Middle lambda scope: Lambda1 → list_col, cmp ---
        let list_col = TestTreeNode::new_leaf("list_col".to_string());
        let idx_col = TestTreeNode::new_leaf("idx_col".to_string());
        // cmp has idx_col in scope, Lambda2 out of scope (new scope)
        let cmp = TestTreeNode::new_mixed(
            vec![idx_col.clone(), lambda2.clone()],
            vec![lambda2],
            "cmp".to_string(),
        );
        let lambda1 =
            TestTreeNode::new(vec![list_col, cmp], "lambda1".to_string());

        // --- Outer scope ---
        // gt has col_c in scope, Lambda1 out of scope (new scope)
        let gt = TestTreeNode::new_mixed(
            vec![col_c.clone(), lambda1.clone()],
            vec![lambda1],
            "gt".to_string(),
        );
        let and = TestTreeNode::new(vec![plus, gt], "and".to_string());

        TestTreeNode::new(vec![and], "root".to_string())
    }

    /// Collect all data reachable via children_in_new_scope (scoped DFS).
    fn collect_scoped_data(node: &TestTreeNode<String>) -> Vec<String> {
        let mut result = vec![node.data.clone()];
        for child in &node.children_in_same_scope {
            result.extend(collect_scoped_data(child));
        }
        result
    }

    /// Collect all data reachable via children (non-scoped DFS).
    fn collect_children_data(node: &TestTreeNode<String>) -> Vec<String> {
        let mut result = vec![node.data.clone()];
        for child in &node.children {
            result.extend(collect_children_data(child));
        }
        result
    }

    // Scoped transform helpers that preserve both children and children_in_new_scope
    fn transform_yes_in_scope<N: Display, T: Display + From<String>>(
        transformation_name: N,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        move |node| {
            Ok(Transformed::yes(TestTreeNode {
                children: node.children,
                children_in_same_scope: node.children_in_same_scope,
                data: format!("{}({})", transformation_name, node.data).into(),
            }))
        }
    }

    fn transform_and_event_on_in_scope<
        N: Display,
        T: PartialEq + Display + From<String>,
        D: Into<T>,
    >(
        transformation_name: N,
        data: D,
        event: TreeNodeRecursion,
    ) -> impl FnMut(TestTreeNode<T>) -> Result<Transformed<TestTreeNode<T>>> {
        let d = data.into();
        move |node| {
            let new_node = TestTreeNode {
                children: node.children,
                children_in_same_scope: node.children_in_same_scope,
                data: format!("{}({})", transformation_name, node.data).into(),
            };
            Ok(if node.data == d {
                Transformed::new(new_node, true, event)
            } else {
                Transformed::yes(new_node)
            })
        }
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    // === visit_in_scope ===

    #[test]
    fn test_visit_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));
        tree.visit_in_scope(&mut visitor)?;
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
                "f_up(col_a)",
                "f_down(col_b)",
                "f_up(col_b)",
                "f_up(plus)",
                "f_down(gt)",
                "f_down(col_c)",
                "f_up(col_c)",
                "f_up(gt)",
                "f_up(and)",
                "f_up(root)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_does_not_enter_lambda() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));
        tree.visit_in_scope(&mut visitor)?;
        let out_of_scope = [
            "lambda1",
            "lambda2",
            "list_col",
            "cmp",
            "idx_col",
            "inner_col",
        ];
        for v in &visitor.visits {
            for name in &out_of_scope {
                assert!(
                    !v.contains(name),
                    "should not enter other scope ({name}): {v}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_f_down_jump_on_plus() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(visit_event_on("plus", TreeNodeRecursion::Jump)),
            Box::new(visit_continue),
        );
        tree.visit_in_scope(&mut visitor)?;
        // Jump on Plus: skip Plus's children, continue with sibling gt
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_up(plus)",
                "f_down(gt)",
                "f_down(col_c)",
                "f_up(col_c)",
                "f_up(gt)",
                "f_up(and)",
                "f_up(root)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_f_down_stop_on_col_a() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(visit_event_on("col_a", TreeNodeRecursion::Stop)),
            Box::new(visit_continue),
        );
        tree.visit_in_scope(&mut visitor)?;
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_f_up_jump_on_col_a() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(visit_continue),
            Box::new(visit_event_on("col_a", TreeNodeRecursion::Jump)),
        );
        tree.visit_in_scope(&mut visitor)?;
        // Jump after f_up(col_a): continue with sibling col_b.
        // col_b returns Continue, resetting the tnr, so plus's f_up IS called.
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
                "f_up(col_a)",
                "f_down(col_b)",
                "f_up(col_b)",
                "f_up(plus)",
                "f_down(gt)",
                "f_down(col_c)",
                "f_up(col_c)",
                "f_up(gt)",
                "f_up(and)",
                "f_up(root)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_f_up_stop_on_col_a() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(visit_continue),
            Box::new(visit_event_on("col_a", TreeNodeRecursion::Stop)),
        );
        tree.visit_in_scope(&mut visitor)?;
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
                "f_up(col_a)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_visit_in_scope_f_up_jump_on_plus() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visitor = TestVisitor::new(
            Box::new(visit_continue),
            Box::new(visit_event_on("plus", TreeNodeRecursion::Jump)),
        );
        tree.visit_in_scope(&mut visitor)?;
        // Jump after f_up(plus): skip plus's parent f_up, continue with sibling gt.
        // gt returns Continue which resets the tnr, so f_up(and) and f_up(root) are called.
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
                "f_up(col_a)",
                "f_down(col_b)",
                "f_up(col_b)",
                "f_up(plus)",
                // gt is sibling of plus, so it gets visited
                "f_down(gt)",
                "f_down(col_c)",
                "f_up(col_c)",
                "f_up(gt)",
                // gt returned Continue, resetting the tnr, so f_up(and) is called
                "f_up(and)",
                "f_up(root)",
            ])
        );
        Ok(())
    }

    // === apply_in_scope ===

    #[test]
    fn test_apply_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visits = vec![];
        tree.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(
            visits,
            s(&["root", "and", "plus", "col_a", "col_b", "gt", "col_c"])
        );
        Ok(())
    }

    #[test]
    fn test_apply_in_scope_does_not_enter_lambda() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visits = vec![];
        tree.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        let out_of_scope = [
            "lambda1",
            "lambda2",
            "list_col",
            "cmp",
            "idx_col",
            "inner_col",
        ];
        for name in &out_of_scope {
            assert!(
                !visits.contains(&name.to_string()),
                "{name} should not be visited"
            );
        }
        Ok(())
    }

    #[test]
    fn test_apply_in_scope_jump_on_plus() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visits = vec![];
        tree.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(if n.data == "plus" {
                TreeNodeRecursion::Jump
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        // Jump on Plus skips its children, continues to sibling gt
        assert_eq!(visits, s(&["root", "and", "plus", "gt", "col_c"]));
        Ok(())
    }

    #[test]
    fn test_apply_in_scope_stop_on_col_a() -> Result<()> {
        let tree = scoped_test_tree();
        let mut visits = vec![];
        tree.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(if n.data == "col_a" {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        assert_eq!(visits, s(&["root", "and", "plus", "col_a"]));
        Ok(())
    }

    // === transform_down_in_scope ===

    #[test]
    fn test_transform_down_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_in_scope(transform_yes_in_scope("f_down"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "f_down(col_a)",
                "f_down(col_b)",
                "f_down(gt)",
                "f_down(col_c)",
            ])
        );
        // Lambda internals untouched in children path
        let children = collect_children_data(&result.data);
        assert_eq!(children[0], "f_down(root)");
        assert!(children.contains(&"lambda1".to_string()));
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"list_col".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_transform_down_in_scope_does_not_transform_lambda_internals() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_in_scope(transform_yes_in_scope("f_down"))?;
        let scoped = collect_scoped_data(&result.data);
        let out_of_scope = [
            "lambda1",
            "lambda2",
            "list_col",
            "cmp",
            "idx_col",
            "inner_col",
        ];
        for v in &scoped {
            for name in &out_of_scope {
                assert!(
                    !v.contains(name),
                    "{name} should not be in scoped data: {v}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_transform_down_in_scope_jump_on_plus() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_in_scope(transform_and_event_on_in_scope(
            "f_down",
            "plus",
            TreeNodeRecursion::Jump,
        ))?;
        // Plus is transformed but children skipped, gt still visited
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_down(root)",
                "f_down(and)",
                "f_down(plus)",
                "col_a",
                "col_b",
                "f_down(gt)",
                "f_down(col_c)",
            ])
        );
        Ok(())
    }

    // === transform_up_in_scope ===

    #[test]
    fn test_transform_up_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_up_in_scope(transform_yes_in_scope("f_up"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(root)",
                "f_up(and)",
                "f_up(plus)",
                "f_up(col_a)",
                "f_up(col_b)",
                "f_up(gt)",
                "f_up(col_c)",
            ])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda1".to_string()));
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_transform_up_in_scope_stop_on_col_a() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_up_in_scope(transform_and_event_on_in_scope(
            "f_up",
            "col_a",
            TreeNodeRecursion::Stop,
        ))?;
        // Stop on col_a: only col_a transformed, everything else untouched
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["root", "and", "plus", "f_up(col_a)", "col_b", "gt", "col_c",])
        );
        Ok(())
    }

    // === transform_down_up_in_scope ===

    #[test]
    fn test_transform_down_up_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_up_in_scope(
            transform_yes_in_scope("f_down"),
            transform_yes_in_scope("f_up"),
        )?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(f_down(root))",
                "f_up(f_down(and))",
                "f_up(f_down(plus))",
                "f_up(f_down(col_a))",
                "f_up(f_down(col_b))",
                "f_up(f_down(gt))",
                "f_up(f_down(col_c))",
            ])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda1".to_string()));
        assert!(children.contains(&"lambda2".to_string()));
        Ok(())
    }

    // === exists_in_scope ===

    #[test]
    fn test_exists_in_scope_found_in_scope() -> Result<()> {
        let tree = scoped_test_tree();
        assert!(tree.exists_in_scope(|n| Ok(n.data == "root"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "and"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "plus"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "col_a"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "col_b"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "gt"))?);
        assert!(tree.exists_in_scope(|n| Ok(n.data == "col_c"))?);
        Ok(())
    }

    #[test]
    fn test_exists_in_scope_not_found_lambda1_scope() -> Result<()> {
        let tree = scoped_test_tree();
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "lambda1"))?);
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "list_col"))?);
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "cmp"))?);
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "idx_col"))?);
        Ok(())
    }

    #[test]
    fn test_exists_in_scope_not_found_nested_lambda2_scope() -> Result<()> {
        let tree = scoped_test_tree();
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "lambda2"))?);
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "inner_col"))?);
        Ok(())
    }

    #[test]
    fn test_exists_in_scope_not_found_nonexistent() -> Result<()> {
        let tree = scoped_test_tree();
        assert!(!tree.exists_in_scope(|n| Ok(n.data == "zzz"))?);
        Ok(())
    }

    // === rewrite_in_scope ===

    #[test]
    fn test_rewrite_in_scope_continue() -> Result<()> {
        let tree = scoped_test_tree();
        let mut rewriter = TestRewriter::new(
            Box::new(transform_yes_in_scope("f_down")),
            Box::new(transform_yes_in_scope("f_up")),
        );
        let result = tree.rewrite_in_scope(&mut rewriter)?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(f_down(root))",
                "f_up(f_down(and))",
                "f_up(f_down(plus))",
                "f_up(f_down(col_a))",
                "f_up(f_down(col_b))",
                "f_up(f_down(gt))",
                "f_up(f_down(col_c))",
            ])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda1".to_string()));
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_rewrite_in_scope_f_down_jump_on_plus() -> Result<()> {
        let tree = scoped_test_tree();
        let mut rewriter = TestRewriter::new(
            Box::new(transform_and_event_on_in_scope(
                "f_down",
                "plus",
                TreeNodeRecursion::Jump,
            )),
            Box::new(transform_yes_in_scope("f_up")),
        );
        let result = tree.rewrite_in_scope(&mut rewriter)?;
        // Jump on Plus: children skipped, but sibling gt is visited
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(f_down(root))",
                "f_up(f_down(and))",
                "f_up(f_down(plus))",
                "col_a",
                "col_b",
                "f_up(f_down(gt))",
                "f_up(f_down(col_c))",
            ])
        );
        Ok(())
    }

    // === Nested scope isolation tests ===
    // Each scope is tested with multiple traversal methods to ensure
    // no scope crossing occurs in any direction.

    /// Build the Lambda2 subtree (innermost scope: lambda2 → inner_col)
    fn build_lambda2() -> TestTreeNode<String> {
        let inner_col = TestTreeNode::new_leaf("inner_col".to_string());
        TestTreeNode::new(vec![inner_col], "lambda2".to_string())
    }

    /// Build the Lambda1 subtree (middle scope: lambda1 → [list_col, cmp → idx_col])
    fn build_lambda1() -> TestTreeNode<String> {
        let lambda2 = build_lambda2();
        let list_col = TestTreeNode::new_leaf("list_col".to_string());
        let idx_col = TestTreeNode::new_leaf("idx_col".to_string());
        let cmp = TestTreeNode::new_mixed(
            vec![idx_col.clone(), lambda2.clone()],
            vec![lambda2],
            "cmp".to_string(),
        );
        TestTreeNode::new(vec![list_col, cmp], "lambda1".to_string())
    }

    // --- Lambda1 scope (middle): 4 in-scope nodes ---

    #[test]
    fn test_lambda1_scope_apply() -> Result<()> {
        let lambda1 = build_lambda1();
        let mut visits = vec![];
        lambda1.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(visits, s(&["lambda1", "list_col", "cmp", "idx_col"]));
        assert!(!visits.contains(&"lambda2".to_string()));
        assert!(!visits.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_visit() -> Result<()> {
        let lambda1 = build_lambda1();
        let mut visitor =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));
        lambda1.visit_in_scope(&mut visitor)?;
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(lambda1)",
                "f_down(list_col)",
                "f_up(list_col)",
                "f_down(cmp)",
                "f_down(idx_col)",
                "f_up(idx_col)",
                "f_up(cmp)",
                "f_up(lambda1)",
            ])
        );
        // Must not enter lambda2 scope
        for v in &visitor.visits {
            assert!(
                !v.contains("lambda2"),
                "lambda1 visit must not enter lambda2: {v}"
            );
            assert!(
                !v.contains("inner_col"),
                "lambda1 visit must not enter lambda2: {v}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_transform_down() -> Result<()> {
        let lambda1 = build_lambda1();
        let result = lambda1.transform_down_in_scope(transform_yes_in_scope("TX"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["TX(lambda1)", "TX(list_col)", "TX(cmp)", "TX(idx_col)",])
        );
        // Lambda2 untouched in children path
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_transform_up() -> Result<()> {
        let lambda1 = build_lambda1();
        let result = lambda1.transform_up_in_scope(transform_yes_in_scope("TX"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["TX(lambda1)", "TX(list_col)", "TX(cmp)", "TX(idx_col)",])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_exists() -> Result<()> {
        let lambda1 = build_lambda1();
        // In scope
        assert!(lambda1.exists_in_scope(|n| Ok(n.data == "lambda1"))?);
        assert!(lambda1.exists_in_scope(|n| Ok(n.data == "list_col"))?);
        assert!(lambda1.exists_in_scope(|n| Ok(n.data == "cmp"))?);
        assert!(lambda1.exists_in_scope(|n| Ok(n.data == "idx_col"))?);
        // Out of scope (lambda2's scope)
        assert!(!lambda1.exists_in_scope(|n| Ok(n.data == "lambda2"))?);
        assert!(!lambda1.exists_in_scope(|n| Ok(n.data == "inner_col"))?);
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_rewrite() -> Result<()> {
        let lambda1 = build_lambda1();
        let mut rewriter = TestRewriter::new(
            Box::new(transform_yes_in_scope("f_down")),
            Box::new(transform_yes_in_scope("f_up")),
        );
        let result = lambda1.rewrite_in_scope(&mut rewriter)?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(f_down(lambda1))",
                "f_up(f_down(list_col))",
                "f_up(f_down(cmp))",
                "f_up(f_down(idx_col))",
            ])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_lambda1_scope_transform_down_up() -> Result<()> {
        let lambda1 = build_lambda1();
        let result = lambda1.transform_down_up_in_scope(
            transform_yes_in_scope("f_down"),
            transform_yes_in_scope("f_up"),
        )?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&[
                "f_up(f_down(lambda1))",
                "f_up(f_down(list_col))",
                "f_up(f_down(cmp))",
                "f_up(f_down(idx_col))",
            ])
        );
        let children = collect_children_data(&result.data);
        assert!(children.contains(&"lambda2".to_string()));
        assert!(children.contains(&"inner_col".to_string()));
        Ok(())
    }

    // --- Lambda2 scope (innermost): 2 in-scope nodes ---

    #[test]
    fn test_lambda2_scope_apply() -> Result<()> {
        let lambda2 = build_lambda2();
        let mut visits = vec![];
        lambda2.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(visits, s(&["lambda2", "inner_col"]));
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_visit() -> Result<()> {
        let lambda2 = build_lambda2();
        let mut visitor =
            TestVisitor::new(Box::new(visit_continue), Box::new(visit_continue));
        lambda2.visit_in_scope(&mut visitor)?;
        assert_eq!(
            visitor.visits,
            s(&[
                "f_down(lambda2)",
                "f_down(inner_col)",
                "f_up(inner_col)",
                "f_up(lambda2)",
            ])
        );
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_transform_down() -> Result<()> {
        let lambda2 = build_lambda2();
        let result = lambda2.transform_down_in_scope(transform_yes_in_scope("TX"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["TX(lambda2)", "TX(inner_col)",])
        );
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_transform_up() -> Result<()> {
        let lambda2 = build_lambda2();
        let result = lambda2.transform_up_in_scope(transform_yes_in_scope("TX"))?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["TX(lambda2)", "TX(inner_col)",])
        );
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_transform_down_up() -> Result<()> {
        let lambda2 = build_lambda2();
        let result = lambda2.transform_down_up_in_scope(
            transform_yes_in_scope("f_down"),
            transform_yes_in_scope("f_up"),
        )?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["f_up(f_down(lambda2))", "f_up(f_down(inner_col))",])
        );
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_rewrite() -> Result<()> {
        let lambda2 = build_lambda2();
        let mut rewriter = TestRewriter::new(
            Box::new(transform_yes_in_scope("f_down")),
            Box::new(transform_yes_in_scope("f_up")),
        );
        let result = lambda2.rewrite_in_scope(&mut rewriter)?;
        assert_eq!(
            collect_scoped_data(&result.data),
            s(&["f_up(f_down(lambda2))", "f_up(f_down(inner_col))",])
        );
        Ok(())
    }

    #[test]
    fn test_lambda2_scope_exists() -> Result<()> {
        let lambda2 = build_lambda2();
        assert!(lambda2.exists_in_scope(|n| Ok(n.data == "lambda2"))?);
        assert!(lambda2.exists_in_scope(|n| Ok(n.data == "inner_col"))?);
        // Not in any scope from lambda2's perspective
        assert!(!lambda2.exists_in_scope(|n| Ok(n.data == "lambda1"))?);
        assert!(!lambda2.exists_in_scope(|n| Ok(n.data == "col_a"))?);
        Ok(())
    }

    // --- Outer scope: transform must not affect any inner scope ---

    #[test]
    fn test_outer_scope_transform_does_not_affect_lambda1() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_in_scope(transform_yes_in_scope("TX"))?;
        let all_data = collect_children_data(&result.data);
        assert_eq!(all_data[0], "TX(root)");
        // Lambda1 scope completely untouched
        assert!(all_data.contains(&"lambda1".to_string()));
        assert!(all_data.contains(&"list_col".to_string()));
        assert!(all_data.contains(&"cmp".to_string()));
        assert!(all_data.contains(&"idx_col".to_string()));
        // Lambda2 scope completely untouched
        assert!(all_data.contains(&"lambda2".to_string()));
        assert!(all_data.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_outer_scope_transform_down_up_does_not_affect_inner_scopes() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_down_up_in_scope(
            transform_yes_in_scope("f_down"),
            transform_yes_in_scope("f_up"),
        )?;
        let all_data = collect_children_data(&result.data);
        assert!(all_data.contains(&"lambda1".to_string()));
        assert!(all_data.contains(&"list_col".to_string()));
        assert!(all_data.contains(&"cmp".to_string()));
        assert!(all_data.contains(&"idx_col".to_string()));
        assert!(all_data.contains(&"lambda2".to_string()));
        assert!(all_data.contains(&"inner_col".to_string()));
        Ok(())
    }

    #[test]
    fn test_outer_scope_transform_up_does_not_affect_inner_scopes() -> Result<()> {
        let tree = scoped_test_tree();
        let result = tree.transform_up_in_scope(transform_yes_in_scope("TX"))?;
        let all_data = collect_children_data(&result.data);
        // Lambda1 + Lambda2 scopes completely untouched
        assert!(all_data.contains(&"lambda1".to_string()));
        assert!(all_data.contains(&"list_col".to_string()));
        assert!(all_data.contains(&"cmp".to_string()));
        assert!(all_data.contains(&"idx_col".to_string()));
        assert!(all_data.contains(&"lambda2".to_string()));
        assert!(all_data.contains(&"inner_col".to_string()));
        Ok(())
    }

    // === Edge cases ===

    #[test]
    fn test_leaf_node_scoped_traversal() -> Result<()> {
        let leaf = TestTreeNode::<String>::new_leaf("leaf".to_string());
        let mut visits = vec![];
        leaf.apply_in_scope(|n| {
            visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(visits, vec!["leaf"]);
        Ok(())
    }

    #[test]
    fn test_node_new_has_all_children_in_scope() -> Result<()> {
        let child = TestTreeNode::new_leaf("child".to_string());
        let parent = TestTreeNode::new(vec![child], "parent".to_string());

        let mut scoped_visits = vec![];
        parent.apply_in_scope(|n| {
            scoped_visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(scoped_visits, vec!["parent", "child"]);

        let mut all_visits = vec![];
        parent.apply(|n| {
            all_visits.push(n.data.clone());
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(all_visits, vec!["parent", "child"]);
        Ok(())
    }
}
