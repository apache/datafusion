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

//! Common Subexpression Elimination logic implemented in [`CSE`] can be controlled with
//! a [`CSEController`], that defines how to eliminate common subtrees from a particular
//! [`TreeNode`] tree.

use crate::Result;
use crate::hash_utils::combine_hashes;
use crate::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
    TreeNodeVisitor,
};
use indexmap::IndexMap;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hash, Hasher, RandomState};
use std::marker::PhantomData;
use std::sync::Arc;

/// Hashes the direct content of an [`TreeNode`] without recursing into its children.
///
/// This method is useful to incrementally compute hashes, such as in [`CSE`] which builds
/// a deep hash of a node and its descendants during the bottom-up phase of the first
/// traversal and so avoid computing the hash of the node and then the hash of its
/// descendants separately.
///
/// If a node doesn't have any children then the value returned by `hash_node()` is
/// similar to '.hash()`, but not necessarily returns the same value.
pub trait HashNode {
    fn hash_node<H: Hasher>(&self, state: &mut H);
}

impl<T: HashNode + ?Sized> HashNode for Arc<T> {
    fn hash_node<H: Hasher>(&self, state: &mut H) {
        (**self).hash_node(state);
    }
}

/// The `Normalizeable` trait defines a method to determine whether a node can be normalized.
///
/// Normalization is the process of converting a node into a canonical form that can be used
/// to compare nodes for equality. This is useful in optimizations like Common Subexpression Elimination (CSE),
/// where semantically equivalent nodes (e.g., `a + b` and `b + a`) should be treated as equal.
pub trait Normalizeable {
    fn can_normalize(&self) -> bool;
}

/// The `NormalizeEq` trait extends `Eq` and `Normalizeable` to provide a method for comparing
/// normalized nodes in optimizations like Common Subexpression Elimination (CSE).
///
/// The `normalize_eq` method ensures that two nodes that are semantically equivalent (after normalization)
/// are considered equal in CSE optimization, even if their original forms differ.
///
/// This trait allows for equality comparisons between nodes with equivalent semantics, regardless of their
/// internal representations.
pub trait NormalizeEq: Eq + Normalizeable {
    fn normalize_eq(&self, other: &Self) -> bool;
}

/// Identifier that represents a [`TreeNode`] tree.
///
/// This identifier is designed to be efficient and  "hash", "accumulate", "equal" and
/// "have no collision (as low as possible)"
#[derive(Debug, Eq)]
struct Identifier<'n, N: NormalizeEq> {
    // Hash of `node` built up incrementally during the first, visiting traversal.
    // Its value is not necessarily equal to default hash of the node. E.g. it is not
    // equal to `expr.hash()` if the node is `Expr`.
    hash: u64,
    node: &'n N,
}

impl<N: NormalizeEq> Clone for Identifier<'_, N> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<N: NormalizeEq> Copy for Identifier<'_, N> {}

impl<N: NormalizeEq> Hash for Identifier<'_, N> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl<N: NormalizeEq> PartialEq for Identifier<'_, N> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash && self.node.normalize_eq(other.node)
    }
}

impl<'n, N> Identifier<'n, N>
where
    N: HashNode + NormalizeEq,
{
    fn new(node: &'n N, random_state: &RandomState) -> Self {
        let mut hasher = random_state.build_hasher();
        node.hash_node(&mut hasher);
        let hash = hasher.finish();
        Self { hash, node }
    }

    fn combine(mut self, other: Option<Self>) -> Self {
        other.map_or(self, |other_id| {
            self.hash = combine_hashes(self.hash, other_id.hash);
            self
        })
    }
}

/// A cache that contains the postorder index and the identifier of [`TreeNode`]s by the
/// preorder index of the nodes.
///
/// This cache is filled by [`CSEVisitor`] during the first traversal and is
/// used by [`CSERewriter`] during the second traversal.
///
/// The purpose of this cache is to quickly find the identifier of a node during the
/// second traversal.
///
/// Elements in this array are added during `f_down` so the indexes represent the preorder
/// index of nodes and thus element 0 belongs to the root of the tree.
///
/// The elements of the array are tuples that contain:
/// - Postorder index that belongs to the preorder index. Assigned during `f_up`, start
///   from 0.
/// - The optional [`Identifier`] of the node. If none the node should not be considered
///   for CSE.
///
/// # Example
/// An expression tree like `(a + b)` would have the following `IdArray`:
/// ```text
/// [
///   (2, Some(Identifier(hash_of("a + b"), &"a + b"))),
///   (1, Some(Identifier(hash_of("a"), &"a"))),
///   (0, Some(Identifier(hash_of("b"), &"b")))
/// ]
/// ```
type IdArray<'n, N> = Vec<(usize, Option<Identifier<'n, N>>)>;

#[derive(PartialEq, Eq)]
/// How many times a node is evaluated. A node can be considered common if evaluated
/// surely at least 2 times or surely only once but also conditionally.
enum NodeEvaluation {
    SurelyOnce,
    ConditionallyAtLeastOnce,
    Common,
}

/// A map that contains the evaluation stats of [`TreeNode`]s by their identifiers.
type NodeStats<'n, N> = HashMap<Identifier<'n, N>, NodeEvaluation>;

/// A map that contains the common [`TreeNode`]s and their alias by their identifiers,
/// extracted during the second, rewriting traversal.
type CommonNodes<'n, N> = IndexMap<Identifier<'n, N>, (N, String)>;

type ChildrenList<N> = (Vec<N>, Vec<N>);

/// The [`TreeNode`] specific definition of elimination.
pub trait CSEController {
    /// The type of the tree nodes.
    type Node;

    /// Splits the children to normal and conditionally evaluated ones or returns `None`
    /// if all are always evaluated.
    fn conditional_children(node: &Self::Node) -> Option<ChildrenList<&Self::Node>>;

    // Returns true if a node is valid. If a node is invalid then it can't be eliminated.
    // Validity is propagated up which means no subtree can be eliminated that contains
    // an invalid node.
    // (E.g. volatile expressions are not valid and subtrees containing such a node can't
    // be extracted.)
    fn is_valid(node: &Self::Node) -> bool;

    // Returns true if a node should be ignored during CSE. Contrary to validity of a node,
    // it is not propagated up.
    fn is_ignored(&self, node: &Self::Node) -> bool;

    // Generates a new name for the extracted subtree.
    fn generate_alias(&self) -> String;

    // Replaces a node to the generated alias.
    fn rewrite(&mut self, node: &Self::Node, alias: &str) -> Self::Node;

    // A helper method called on each node during top-down traversal during the second,
    // rewriting traversal of CSE.
    fn rewrite_f_down(&mut self, _node: &Self::Node) {}

    // A helper method called on each node during bottom-up traversal during the second,
    // rewriting traversal of CSE.
    fn rewrite_f_up(&mut self, _node: &Self::Node) {}
}

/// The result of potentially rewriting a list of [`TreeNode`]s to eliminate common
/// subtrees.
#[derive(Debug)]
pub enum FoundCommonNodes<N> {
    /// No common [`TreeNode`]s were found
    No { original_nodes_list: Vec<Vec<N>> },

    /// Common [`TreeNode`]s were found
    Yes {
        /// extracted common [`TreeNode`]
        common_nodes: Vec<(N, String)>,

        /// new [`TreeNode`]s with common subtrees replaced
        new_nodes_list: Vec<Vec<N>>,

        /// original [`TreeNode`]s
        original_nodes_list: Vec<Vec<N>>,
    },
}

/// Go through a [`TreeNode`] tree and generate identifiers for each subtrees.
///
/// An identifier contains information of the [`TreeNode`] itself and its subtrees.
/// This visitor implementation use a stack `visit_stack` to track traversal, which
/// lets us know when a subtree's visiting is finished. When `pre_visit` is called
/// (traversing to a new node), an `EnterMark` and an `NodeItem` will be pushed into stack.
/// And try to pop out a `EnterMark` on leaving a node (`f_up()`). All `NodeItem`
/// before the first `EnterMark` is considered to be sub-tree of the leaving node.
///
/// This visitor also records identifier in `id_array`. Makes the following traverse
/// pass can get the identifier of a node without recalculate it. We assign each node
/// in the tree a series number, start from 1, maintained by `series_number`.
/// Series number represents the order we left (`f_up()`) a node. Has the property
/// that child node's series number always smaller than parent's. While `id_array` is
/// organized in the order we enter (`f_down()`) a node. `node_count` helps us to
/// get the index of `id_array` for each node.
///
/// A [`TreeNode`] without any children (column, literal etc.) will not have identifier
/// because they should not be recognized as common subtree.
struct CSEVisitor<'a, 'n, N, C>
where
    N: NormalizeEq,
    C: CSEController<Node = N>,
{
    /// statistics of [`TreeNode`]s
    node_stats: &'a mut NodeStats<'n, N>,

    /// cache to speed up second traversal
    id_array: &'a mut IdArray<'n, N>,

    /// inner states
    visit_stack: Vec<VisitRecord<'n, N>>,

    /// preorder index, start from 0.
    down_index: usize,

    /// postorder index, start from 0.
    up_index: usize,

    /// a [`RandomState`] to generate hashes during the first traversal
    random_state: &'a RandomState,

    /// a flag to indicate that common [`TreeNode`]s found
    found_common: bool,

    /// if we are in a conditional branch. A conditional branch means that the [`TreeNode`]
    /// might not be executed depending on the runtime values of other [`TreeNode`]s, and
    /// thus can not be extracted as a common [`TreeNode`].
    conditional: bool,

    controller: &'a C,
}

/// Record item that used when traversing a [`TreeNode`] tree.
enum VisitRecord<'n, N>
where
    N: NormalizeEq,
{
    /// Marks the beginning of [`TreeNode`]. It contains:
    /// - The post-order index assigned during the first, visiting traversal.
    EnterMark(usize),

    /// Marks an accumulated subtree. It contains:
    /// - The accumulated identifier of a subtree.
    /// - A accumulated boolean flag if the subtree is valid for CSE.
    ///   The flag is propagated up from children to parent. (E.g. volatile expressions
    ///   are not valid and can't be extracted, but non-volatile children of volatile
    ///   expressions can be extracted.)
    NodeItem(Identifier<'n, N>, bool),
}

impl<'n, N, C> CSEVisitor<'_, 'n, N, C>
where
    N: TreeNode + HashNode + NormalizeEq,
    C: CSEController<Node = N>,
{
    /// Find the first `EnterMark` in the stack, and accumulates every `NodeItem` before
    /// it. Returns a tuple that contains:
    /// - The pre-order index of the [`TreeNode`] we marked.
    /// - The accumulated identifier of the children of the marked [`TreeNode`].
    /// - An accumulated boolean flag from the children of the marked [`TreeNode`] if all
    ///   children are valid for CSE (i.e. it is safe to extract the [`TreeNode`] as a
    ///   common [`TreeNode`] from its children POV).
    ///   (E.g. if any of the children of the marked expression is not valid (e.g. is
    ///   volatile) then the expression is also not valid, so we can propagate this
    ///   information up from children to parents via `visit_stack` during the first,
    ///   visiting traversal and no need to test the expression's validity beforehand with
    ///   an extra traversal).
    fn pop_enter_mark(
        &mut self,
        can_normalize: bool,
    ) -> (usize, Option<Identifier<'n, N>>, bool) {
        let mut node_ids: Vec<Identifier<'n, N>> = vec![];
        let mut is_valid = true;

        while let Some(item) = self.visit_stack.pop() {
            match item {
                VisitRecord::EnterMark(down_index) => {
                    if can_normalize {
                        node_ids.sort_by_key(|i| i.hash);
                    }
                    let node_id = node_ids
                        .into_iter()
                        .fold(None, |accum, item| Some(item.combine(accum)));
                    return (down_index, node_id, is_valid);
                }
                VisitRecord::NodeItem(sub_node_id, sub_node_is_valid) => {
                    node_ids.push(sub_node_id);
                    is_valid &= sub_node_is_valid;
                }
            }
        }
        unreachable!("EnterMark should paired with NodeItem");
    }
}

impl<'n, N, C> TreeNodeVisitor<'n> for CSEVisitor<'_, 'n, N, C>
where
    N: TreeNode + HashNode + NormalizeEq,
    C: CSEController<Node = N>,
{
    type Node = N;

    fn f_down(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        self.id_array.push((0, None));
        self.visit_stack
            .push(VisitRecord::EnterMark(self.down_index));
        self.down_index += 1;

        // If a node can short-circuit then some of its children might not be executed so
        // count the occurrence either normal or conditional.
        Ok(if self.conditional {
            // If we are already in a conditionally evaluated subtree then continue
            // traversal.
            TreeNodeRecursion::Continue
        } else {
            // If we are already in a node that can short-circuit then start new
            // traversals on its normal conditional children.
            match C::conditional_children(node) {
                Some((normal, conditional)) => {
                    normal
                        .into_iter()
                        .try_for_each(|n| n.visit(self).map(|_| ()))?;
                    self.conditional = true;
                    conditional
                        .into_iter()
                        .try_for_each(|n| n.visit(self).map(|_| ()))?;
                    self.conditional = false;

                    TreeNodeRecursion::Jump
                }

                // In case of non-short-circuit node continue the traversal.
                _ => TreeNodeRecursion::Continue,
            }
        })
    }

    fn f_up(&mut self, node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        let (down_index, sub_node_id, sub_node_is_valid) =
            self.pop_enter_mark(node.can_normalize());

        let node_id = Identifier::new(node, self.random_state).combine(sub_node_id);
        let is_valid = C::is_valid(node) && sub_node_is_valid;

        self.id_array[down_index].0 = self.up_index;
        if is_valid && !self.controller.is_ignored(node) {
            self.id_array[down_index].1 = Some(node_id);
            self.node_stats
                .entry(node_id)
                .and_modify(|evaluation| {
                    if *evaluation == NodeEvaluation::SurelyOnce
                        || *evaluation == NodeEvaluation::ConditionallyAtLeastOnce
                            && !self.conditional
                    {
                        *evaluation = NodeEvaluation::Common;
                        self.found_common = true;
                    }
                })
                .or_insert_with(|| {
                    if self.conditional {
                        NodeEvaluation::ConditionallyAtLeastOnce
                    } else {
                        NodeEvaluation::SurelyOnce
                    }
                });
        }
        self.visit_stack
            .push(VisitRecord::NodeItem(node_id, is_valid));
        self.up_index += 1;

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Rewrite a [`TreeNode`] tree by replacing detected common subtrees with the
/// corresponding temporary [`TreeNode`], that column contains the evaluate result of
/// replaced [`TreeNode`] tree.
struct CSERewriter<'a, 'n, N, C>
where
    N: NormalizeEq,
    C: CSEController<Node = N>,
{
    /// statistics of [`TreeNode`]s
    node_stats: &'a NodeStats<'n, N>,

    /// cache to speed up second traversal
    id_array: &'a IdArray<'n, N>,

    /// common [`TreeNode`]s, that are replaced during the second traversal, are collected
    /// to this map
    common_nodes: &'a mut CommonNodes<'n, N>,

    // preorder index, starts from 0.
    down_index: usize,

    controller: &'a mut C,
}

impl<N, C> TreeNodeRewriter for CSERewriter<'_, '_, N, C>
where
    N: TreeNode + NormalizeEq,
    C: CSEController<Node = N>,
{
    type Node = N;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        self.controller.rewrite_f_down(&node);

        let (up_index, node_id) = self.id_array[self.down_index];
        self.down_index += 1;

        // Handle nodes with identifiers only
        if let Some(node_id) = node_id {
            let evaluation = self.node_stats.get(&node_id).unwrap();
            if *evaluation == NodeEvaluation::Common {
                // step index to skip all sub-node (which has smaller series number).
                while self.down_index < self.id_array.len()
                    && self.id_array[self.down_index].0 < up_index
                {
                    self.down_index += 1;
                }

                // We *must* replace all original nodes with same `node_id`, not just the first
                // node which is inserted into the common_nodes. This is because nodes with the same
                // `node_id` are semantically equivalent, but not exactly the same.
                //
                // For example, `a + 1` and `1 + a` are semantically equivalent but not identical.
                // In this case, we should replace the common expression `1 + a` with a new variable
                // (e.g., `__common_cse_1`). So, `a + 1` and `1 + a` would both be replaced by
                // `__common_cse_1`.
                //
                // The final result would be:
                // - `__common_cse_1 as a + 1`
                // - `__common_cse_1 as 1 + a`
                //
                // This way, we can efficiently handle semantically equivalent expressions without
                // incorrectly treating them as identical.
                let rewritten = if let Some((_, alias)) = self.common_nodes.get(&node_id)
                {
                    self.controller.rewrite(&node, alias)
                } else {
                    let node_alias = self.controller.generate_alias();
                    let rewritten = self.controller.rewrite(&node, &node_alias);
                    self.common_nodes.insert(node_id, (node, node_alias));
                    rewritten
                };

                return Ok(Transformed::new(rewritten, true, TreeNodeRecursion::Jump));
            }
        }

        Ok(Transformed::no(node))
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        self.controller.rewrite_f_up(&node);

        Ok(Transformed::no(node))
    }
}

/// The main entry point of Common Subexpression Elimination.
///
/// [`CSE`] requires a [`CSEController`], that defines how common subtrees of a particular
/// [`TreeNode`] tree can be eliminated. The elimination process can be started with the
/// [`CSE::extract_common_nodes()`] method.
pub struct CSE<N, C: CSEController<Node = N>> {
    random_state: RandomState,
    phantom_data: PhantomData<N>,
    controller: C,
}

impl<N, C> CSE<N, C>
where
    N: TreeNode + HashNode + Clone + NormalizeEq,
    C: CSEController<Node = N>,
{
    pub fn new(controller: C) -> Self {
        Self {
            random_state: RandomState::new(),
            phantom_data: PhantomData,
            controller,
        }
    }

    /// Add an identifier to `id_array` for every [`TreeNode`] in this tree.
    fn node_to_id_array<'n>(
        &self,
        node: &'n N,
        node_stats: &mut NodeStats<'n, N>,
        id_array: &mut IdArray<'n, N>,
    ) -> Result<bool> {
        let mut visitor = CSEVisitor {
            node_stats,
            id_array,
            visit_stack: vec![],
            down_index: 0,
            up_index: 0,
            random_state: &self.random_state,
            found_common: false,
            conditional: false,
            controller: &self.controller,
        };
        node.visit(&mut visitor)?;

        Ok(visitor.found_common)
    }

    /// Returns the identifier list for each element in `nodes` and a flag to indicate if
    /// rewrite phase of CSE make sense.
    ///
    /// Returns and array with 1 element for each input node in `nodes`
    ///
    /// Each element is itself the result of [`CSE::node_to_id_array`] for that node
    /// (e.g. the identifiers for each node in the tree)
    fn to_arrays<'n>(
        &self,
        nodes: &'n [N],
        node_stats: &mut NodeStats<'n, N>,
    ) -> Result<(bool, Vec<IdArray<'n, N>>)> {
        let mut found_common = false;
        nodes
            .iter()
            .map(|n| {
                let mut id_array = vec![];
                self.node_to_id_array(n, node_stats, &mut id_array)
                    .map(|fc| {
                        found_common |= fc;

                        id_array
                    })
            })
            .collect::<Result<Vec<_>>>()
            .map(|id_arrays| (found_common, id_arrays))
    }

    /// Replace common subtrees in `node` with the corresponding temporary
    /// [`TreeNode`], updating `common_nodes` with any replaced [`TreeNode`]
    fn replace_common_node<'n>(
        &mut self,
        node: N,
        id_array: &IdArray<'n, N>,
        node_stats: &NodeStats<'n, N>,
        common_nodes: &mut CommonNodes<'n, N>,
    ) -> Result<N> {
        if id_array.is_empty() {
            Ok(Transformed::no(node))
        } else {
            node.rewrite(&mut CSERewriter {
                node_stats,
                id_array,
                common_nodes,
                down_index: 0,
                controller: &mut self.controller,
            })
        }
        .data()
    }

    /// Replace common subtrees in `nodes_list` with the corresponding temporary
    /// [`TreeNode`], updating `common_nodes` with any replaced [`TreeNode`].
    fn rewrite_nodes_list<'n>(
        &mut self,
        nodes_list: Vec<Vec<N>>,
        arrays_list: &[Vec<IdArray<'n, N>>],
        node_stats: &NodeStats<'n, N>,
        common_nodes: &mut CommonNodes<'n, N>,
    ) -> Result<Vec<Vec<N>>> {
        nodes_list
            .into_iter()
            .zip(arrays_list.iter())
            .map(|(nodes, arrays)| {
                nodes
                    .into_iter()
                    .zip(arrays.iter())
                    .map(|(node, id_array)| {
                        self.replace_common_node(node, id_array, node_stats, common_nodes)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Extracts common [`TreeNode`]s and rewrites `nodes_list`.
    ///
    /// Returns [`FoundCommonNodes`] recording the result of the extraction.
    pub fn extract_common_nodes(
        &mut self,
        nodes_list: Vec<Vec<N>>,
    ) -> Result<FoundCommonNodes<N>> {
        let mut found_common = false;
        let mut node_stats = NodeStats::new();

        let id_arrays_list = nodes_list
            .iter()
            .map(|nodes| {
                self.to_arrays(nodes, &mut node_stats)
                    .map(|(fc, id_arrays)| {
                        found_common |= fc;

                        id_arrays
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if found_common {
            let mut common_nodes = CommonNodes::new();
            let new_nodes_list = self.rewrite_nodes_list(
                // Must clone the list of nodes as Identifiers use references to original
                // nodes so we have to keep them intact.
                nodes_list.clone(),
                &id_arrays_list,
                &node_stats,
                &mut common_nodes,
            )?;
            assert!(!common_nodes.is_empty());

            Ok(FoundCommonNodes::Yes {
                common_nodes: common_nodes.into_values().collect(),
                new_nodes_list,
                original_nodes_list: nodes_list,
            })
        } else {
            Ok(FoundCommonNodes::No {
                original_nodes_list: nodes_list,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Result;
    use crate::alias::AliasGenerator;
    use crate::cse::{
        CSE, CSEController, HashNode, IdArray, Identifier, NodeStats, NormalizeEq,
        Normalizeable,
    };
    use crate::tree_node::tests::TestTreeNode;
    use std::collections::HashSet;
    use std::hash::{Hash, Hasher};

    const CSE_PREFIX: &str = "__common_node";

    #[derive(Clone, Copy)]
    pub enum TestTreeNodeMask {
        Normal,
        NormalAndAggregates,
    }

    pub struct TestTreeNodeCSEController<'a> {
        alias_generator: &'a AliasGenerator,
        mask: TestTreeNodeMask,
    }

    impl<'a> TestTreeNodeCSEController<'a> {
        fn new(alias_generator: &'a AliasGenerator, mask: TestTreeNodeMask) -> Self {
            Self {
                alias_generator,
                mask,
            }
        }
    }

    impl CSEController for TestTreeNodeCSEController<'_> {
        type Node = TestTreeNode<String>;

        fn conditional_children(
            _: &Self::Node,
        ) -> Option<(Vec<&Self::Node>, Vec<&Self::Node>)> {
            None
        }

        fn is_valid(_node: &Self::Node) -> bool {
            true
        }

        fn is_ignored(&self, node: &Self::Node) -> bool {
            let is_leaf = node.is_leaf();
            let is_aggr = node.data == "avg" || node.data == "sum";

            match self.mask {
                TestTreeNodeMask::Normal => is_leaf || is_aggr,
                TestTreeNodeMask::NormalAndAggregates => is_leaf,
            }
        }

        fn generate_alias(&self) -> String {
            self.alias_generator.next(CSE_PREFIX)
        }

        fn rewrite(&mut self, node: &Self::Node, alias: &str) -> Self::Node {
            TestTreeNode::new_leaf(format!("alias({}, {})", node.data, alias))
        }
    }

    impl HashNode for TestTreeNode<String> {
        fn hash_node<H: Hasher>(&self, state: &mut H) {
            self.data.hash(state);
        }
    }

    impl Normalizeable for TestTreeNode<String> {
        fn can_normalize(&self) -> bool {
            false
        }
    }

    impl NormalizeEq for TestTreeNode<String> {
        fn normalize_eq(&self, other: &Self) -> bool {
            self == other
        }
    }

    #[test]
    fn id_array_visitor() -> Result<()> {
        let alias_generator = AliasGenerator::new();
        let eliminator = CSE::new(TestTreeNodeCSEController::new(
            &alias_generator,
            TestTreeNodeMask::Normal,
        ));

        let a_plus_1 = TestTreeNode::new(
            vec![
                TestTreeNode::new_leaf("a".to_string()),
                TestTreeNode::new_leaf("1".to_string()),
            ],
            "+".to_string(),
        );
        let avg_c = TestTreeNode::new(
            vec![TestTreeNode::new_leaf("c".to_string())],
            "avg".to_string(),
        );
        let sum_a_plus_1 = TestTreeNode::new(vec![a_plus_1], "sum".to_string());
        let sum_a_plus_1_minus_avg_c =
            TestTreeNode::new(vec![sum_a_plus_1, avg_c], "-".to_string());
        let root = TestTreeNode::new(
            vec![
                sum_a_plus_1_minus_avg_c,
                TestTreeNode::new_leaf("2".to_string()),
            ],
            "*".to_string(),
        );

        let [sum_a_plus_1_minus_avg_c, _] = root.children.as_slice() else {
            panic!("Cannot extract subtree references")
        };
        let [sum_a_plus_1, avg_c] = sum_a_plus_1_minus_avg_c.children.as_slice() else {
            panic!("Cannot extract subtree references")
        };
        let [a_plus_1] = sum_a_plus_1.children.as_slice() else {
            panic!("Cannot extract subtree references")
        };

        // skip aggregates
        let mut id_array = vec![];
        eliminator.node_to_id_array(&root, &mut NodeStats::new(), &mut id_array)?;

        // Collect distinct hashes and set them to 0 in `id_array`
        fn collect_hashes(
            id_array: &mut IdArray<'_, TestTreeNode<String>>,
        ) -> HashSet<u64> {
            id_array
                .iter_mut()
                .flat_map(|(_, id_option)| {
                    id_option.as_mut().map(|node_id| {
                        let hash = node_id.hash;
                        node_id.hash = 0;
                        hash
                    })
                })
                .collect::<HashSet<_>>()
        }

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 3);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    node: &root,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    node: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (3, None),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    node: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (5, None),
            (4, None),
            (7, None),
        ];
        assert_eq!(expected, id_array);

        // include aggregates
        let eliminator = CSE::new(TestTreeNodeCSEController::new(
            &alias_generator,
            TestTreeNodeMask::NormalAndAggregates,
        ));

        let mut id_array = vec![];
        eliminator.node_to_id_array(&root, &mut NodeStats::new(), &mut id_array)?;

        let hashes = collect_hashes(&mut id_array);
        assert_eq!(hashes.len(), 5);

        let expected = vec![
            (
                8,
                Some(Identifier {
                    hash: 0,
                    node: &root,
                }),
            ),
            (
                6,
                Some(Identifier {
                    hash: 0,
                    node: sum_a_plus_1_minus_avg_c,
                }),
            ),
            (
                3,
                Some(Identifier {
                    hash: 0,
                    node: sum_a_plus_1,
                }),
            ),
            (
                2,
                Some(Identifier {
                    hash: 0,
                    node: a_plus_1,
                }),
            ),
            (0, None),
            (1, None),
            (
                5,
                Some(Identifier {
                    hash: 0,
                    node: avg_c,
                }),
            ),
            (4, None),
            (7, None),
        ];
        assert_eq!(expected, id_array);

        Ok(())
    }
}
