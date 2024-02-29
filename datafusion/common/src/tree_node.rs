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
/// [`TreeNodeRecursion`], [`Transformed::try_transform_node_with()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion_down {
    ($F_DOWN:expr, $F_SELF:expr) => {
        $F_DOWN?.try_transform_node_with(
            |n| n.map_children($F_SELF),
            Some(TreeNodeRecursion::Continue),
        )
    };
}

/// This macro is used to determine continuation during combined transforming traversals.
///
/// After the bottom-up closure returns with [`Transformed`] depending on the returned
/// [`TreeNodeRecursion`], [`Transformed::try_transform_node_with()`] decides about recursion
/// continuation and if [`TreeNodeRecursion`] state propagation is needed.
/// And then after recursing into children returns with [`Transformed`] depending on the
/// returned  [`TreeNodeRecursion`], [`Transformed::try_transform_node_with()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion {
    ($F_DOWN:expr, $F_SELF:expr, $F_UP:expr) => {
        $F_DOWN?.try_transform_node_with(
            |n| {
                n.map_children($F_SELF)?
                    .try_transform_node_with($F_UP, Some(TreeNodeRecursion::Jump))
            },
            Some(TreeNodeRecursion::Continue),
        )
    };
}

/// This macro is used to determine continuation during bottom-up transforming traversals.
///
/// After recursing into children returns with [`Transformed`] depending on the returned
/// [`TreeNodeRecursion`], [`Transformed::try_transform_node_with()`] decides about recursion
/// continuation and [`TreeNodeRecursion`] state propagation.
#[macro_export]
macro_rules! handle_transform_recursion_up {
    ($NODE:expr, $F_SELF:expr, $F_UP:expr) => {
        $NODE
            .map_children($F_SELF)?
            .try_transform_node_with($F_UP, Some(TreeNodeRecursion::Jump))
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
        match visitor.f_down(self)? {
            TreeNodeRecursion::Continue => {
                handle_visit_recursion_up!(
                    self.apply_children(&mut |n| n.visit(visitor))?
                );
                visitor.f_up(self)
            }
            TreeNodeRecursion::Jump => visitor.f_up(self),
            TreeNodeRecursion::Stop => Ok(TreeNodeRecursion::Stop),
        }
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
        let pre_visited = rewriter.f_down(self)?;
        match pre_visited.tnr {
            TreeNodeRecursion::Continue => {
                let with_updated_children = pre_visited
                    .data
                    .map_children(|c| c.rewrite(rewriter))?
                    .try_transform_node_with(
                        |n| rewriter.f_up(n),
                        Some(TreeNodeRecursion::Jump),
                    )?;
                Ok(Transformed {
                    transformed: with_updated_children.transformed
                        || pre_visited.transformed,
                    ..with_updated_children
                })
            }
            TreeNodeRecursion::Jump => {
                let pre_visited_transformed = pre_visited.transformed;
                let post_visited = rewriter.f_up(pre_visited.data)?;

                Ok(Transformed {
                    tnr: TreeNodeRecursion::Continue,
                    transformed: post_visited.transformed || pre_visited_transformed,
                    data: post_visited.data,
                })
            }
            TreeNodeRecursion::Stop => Ok(pre_visited),
        }
    }

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

    /// Transforms the tree using `f_down` while traversing the tree top-down
    /// (pre-preorder) and using `f_up` while traversing the tree bottom-up (post-order).
    ///
    /// Use this method if you want to start the `f_up` process right where `f_down` jumps.
    /// This can make the whole process faster by reducing the number of `f_up` steps.
    /// If you don't need this, it's just like using `transform_down_mut` followed by
    /// `transform_up_mut` on the same tree.
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
    ///                  it returns with "jump".    +---+                            +---+                
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
    fn transform_down_up<FD, FU>(
        self,
        f_down: &mut FD,
        f_up: &mut FU,
    ) -> Result<Transformed<Self>>
    where
        FD: FnMut(Self) -> Result<Transformed<Self>>,
        FU: FnMut(Self) -> Result<Transformed<Self>>,
    {
        handle_transform_recursion!(
            f_down(self),
            |c| c.transform_down_up(f_down, f_up),
            f_up
        )
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
    /// Default implementation returns the node unmodified and continues recursion.
    fn f_down(&mut self, _node: &Self::Node) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

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

    /// Invoked while traversing down the tree before any children are rewritten.
    /// Default implementation returns the node unmodified and continues recursion.
    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }

    /// Invoked while traversing up the tree after all children have been rewritten.
    /// Default implementation returns the node unmodified.
    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        Ok(Transformed::no(node))
    }
}

/// Controls how [`TreeNode`] recursions should proceed.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TreeNodeRecursion {
    /// Continue recursion with the next node.
    Continue,

    /// In top-down traversals, skip recursing into children but continue with the next
    /// node, which actually means pruning of the subtree.
    ///
    /// In bottom-up traversals, bypass calling bottom-up closures till the next leaf node.
    ///
    /// In combined traversals, if it is "f_down" (pre-order) phase, execution "jumps" to
    /// next "f_up" (post_order) phase by shortcutting its children. If it is "f_up" (pre-order)
    /// phase, execution "jumps" to next "f_down" (pre_order) phase by shortcutting its parent
    /// nodes until the first parent node having unvisited children path.
    Jump,

    /// Stop recursion.
    Stop,
}

#[derive(PartialEq, Debug)]
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

    /// Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn yes(data: T) -> Self {
        Self {
            data,
            transformed: true,
            tnr: TreeNodeRecursion::Continue,
        }
    }

    /// Wrapper for non-transformed data with [`TreeNodeRecursion::Continue`] statement.
    pub fn no(data: T) -> Self {
        Self {
            data,
            transformed: false,
            tnr: TreeNodeRecursion::Continue,
        }
    }

    /// Applies the given `f` to the data of [`Transformed`] object.
    pub fn update_data<U, F: FnOnce(T) -> U>(self, f: F) -> Transformed<U> {
        Transformed {
            data: f(self.data),
            transformed: self.transformed,
            tnr: self.tnr,
        }
    }

    /// Maps the data of [`Transformed`] object to the result of the given `f`.
    pub fn map_data<U, F: FnOnce(T) -> Result<U>>(self, f: F) -> Result<Transformed<U>> {
        f(self.data).map(|data| Transformed {
            data,
            transformed: self.transformed,
            tnr: self.tnr,
        })
    }

    /// According to the TreeNodeRecursion condition on the node, the function decides
    /// applying the given `f` to the node's data. Handling [`TreeNodeRecursion::Continue`]
    /// and [`TreeNodeRecursion::Stop`] is straightforward, but [`TreeNodeRecursion::Jump`]
    /// can behave differently when we are traversing down or up on a tree. If `return_if_jump`
    /// is `Some`, `jump` condition on the node would stop the recursion with the given
    /// [`TreeNodeRecursion`] statement.
    fn try_transform_node_with<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
        return_if_jump: Option<TreeNodeRecursion>,
    ) -> Result<Transformed<T>> {
        match self.tnr {
            TreeNodeRecursion::Continue => {}
            TreeNodeRecursion::Jump => {
                if let Some(tnr) = return_if_jump {
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

    /// More simple version of [`Self::try_transform_node_with`]. If [`TreeNodeRecursion`]
    /// of the node is [`TreeNodeRecursion::Continue`] or [`TreeNodeRecursion::Jump`],
    /// transformation is applied to the node. Otherwise, it remains as it is.
    pub fn try_transform_node<F: FnOnce(T) -> Result<Transformed<T>>>(
        self,
        f: F,
    ) -> Result<Transformed<T>> {
        self.try_transform_node_with(f, None)
    }
}

pub trait TransformedIterator: Iterator {
    fn map_until_stop_and_collect<F>(self, f: F) -> Result<Transformed<Vec<Self::Item>>>
    where
        F: FnMut(Self::Item) -> Result<Transformed<Self::Item>>,
        Self: Sized;
}

impl<I: Iterator> TransformedIterator for I {
    fn map_until_stop_and_collect<F>(
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
            let new_children = children.into_iter().map_until_stop_and_collect(f)?;
            // Propagate up `new_children.transformed` and `new_children.tnr`
            // along with the node containing transformed children.
            if new_children.transformed {
                let arc_self = Arc::clone(&self);
                new_children.map_data(|children| {
                    self.with_new_arc_children(arc_self, children)
                        .map(|new| new.data)
                })
            } else {
                Ok(Transformed::no(self))
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
            let new_children = children.into_iter().map_until_stop_and_collect(f)?;
            if new_children.transformed {
                // Propagate up `t.transformed` and `t.tnr` along with
                // the node containing transformed children.
                new_children.map_data(|children| {
                    new_self.with_new_children(children).map(|new| new.data)
                })
            } else {
                Ok(Transformed::no(
                    new_self.with_new_children(new_children.data)?.data,
                ))
            }
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

    #[derive(PartialEq, Debug)]
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
                .map_until_stop_and_collect(f)?
                .update_data(|new_children| Self {
                    children: new_children,
                    ..self
                }))
        }
    }

    //       J
    //       |
    //       I
    //       |
    //       F
    //     /   \
    //    E     G
    //    |     |
    //    C     H
    //  /   \
    // B     D
    //       |
    //       A
    fn test_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f".to_string());
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    // Expected transformed tree after a combined traversal
    fn transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    // Expected transformed tree after a top-down traversal
    fn transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_down(g)".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    // Expected transformed tree after a bottom-up traversal
    fn transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(h)".to_string());
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_jump_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_down_jump_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
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
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_down_jump_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_down(h)".to_string());
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
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_jump_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(f_down(h))".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "f_up(f_down(g))".to_string());
        let node_f =
            TestTreeNode::new(vec![node_e, node_g], "f_up(f_down(f))".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_up(f_down(i))".to_string());
        TestTreeNode::new(vec![node_i], "f_up(f_down(j))".to_string())
    }

    fn f_up_jump_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "f_up(h)".to_string());
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
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_down_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_a_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_down(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_down(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
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
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_down_stop_on_e_transformed_down_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "a".to_string());
        let node_b = TestTreeNode::new(vec![], "b".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
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
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_a_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_down(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_down(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_down(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_a_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "d".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "c".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "e".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
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
            "f_up(b)",
            "f_down(d)",
            "f_down(a)",
            "f_up(a)",
            "f_up(d)",
            "f_up(c)",
            "f_up(e)",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect()
    }

    fn f_up_stop_on_e_transformed_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(f_down(a))".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(f_down(b))".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(f_down(d))".to_string());
        let node_c =
            TestTreeNode::new(vec![node_b, node_d], "f_up(f_down(c))".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(f_down(e))".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
        let node_g = TestTreeNode::new(vec![node_h], "g".to_string());
        let node_f = TestTreeNode::new(vec![node_e, node_g], "f_down(f)".to_string());
        let node_i = TestTreeNode::new(vec![node_f], "f_down(i)".to_string());
        TestTreeNode::new(vec![node_i], "f_down(j)".to_string())
    }

    fn f_up_stop_on_e_transformed_up_tree() -> TestTreeNode<String> {
        let node_a = TestTreeNode::new(vec![], "f_up(a)".to_string());
        let node_b = TestTreeNode::new(vec![], "f_up(b)".to_string());
        let node_d = TestTreeNode::new(vec![node_a], "f_up(d)".to_string());
        let node_c = TestTreeNode::new(vec![node_b, node_d], "f_up(c)".to_string());
        let node_e = TestTreeNode::new(vec![node_c], "f_up(e)".to_string());
        let node_h = TestTreeNode::new(vec![], "h".to_string());
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

    impl<T: Display> TreeNodeVisitor for TestVisitor<T> {
        type Node = TestTreeNode<T>;

        fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
            self.visits.push(format!("f_down({})", node.data));
            (*self.f_down)(node)
        }

        fn f_up(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
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
                tree.apply(&mut |node| {
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

    fn transform_yes<N: Display, T: Display + From<String>>(
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
        T: PartialEq + Display + From<String>,
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
                assert_eq!(tree.rewrite(&mut rewriter)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_test {
        ($NAME:ident, $F_DOWN:expr, $F_UP:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(
                    tree.transform_down_up(&mut $F_DOWN, &mut $F_UP,)?,
                    $EXPECTED_TREE
                );

                Ok(())
            }
        };
    }

    macro_rules! transform_down_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_down_mut(&mut $F)?, $EXPECTED_TREE);

                Ok(())
            }
        };
    }

    macro_rules! transform_up_test {
        ($NAME:ident, $F:expr, $EXPECTED_TREE:expr) => {
            #[test]
            fn $NAME() -> Result<()> {
                let tree = test_tree();
                assert_eq!(tree.transform_up_mut(&mut $F)?, $EXPECTED_TREE);

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
}
