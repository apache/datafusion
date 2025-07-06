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

//! Types for plan display

use crate::display::{DisplayAs, DisplayFormatType};
use crate::tree_node::{TreeNode, TreeNodeRecursion};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Formatter;
use std::sync::Arc;
use std::{cmp, fmt};

/// This module implements a tree-like art renderer for arbitrary struct that implement TreeNode trait,
/// based on DuckDB's implementation:
/// <https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/tree_renderer/text_tree_renderer.hpp>
///
/// The rendered output looks like this:
/// ```text
/// ┌───────────────────────────┐
/// │    CoalesceBatchesExec    │
/// └─────────────┬─────────────┘
/// ┌─────────────┴─────────────┐
/// │        HashJoinExec       ├──────────────┐
/// └─────────────┬─────────────┘              │
/// ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
/// │       DataSourceExec      ││       DataSourceExec      │
/// └───────────────────────────┘└───────────────────────────┘
/// ```
///
/// The renderer uses a three-layer approach for each node:
/// 1. Top layer: renders the top borders and connections
/// 2. Content layer: renders the node content and vertical connections
/// 3. Bottom layer: renders the bottom borders and connections
///
/// Each node is rendered in a box of fixed width (NODE_RENDER_WIDTH).
struct TreeRenderVisitor<'a, 'b> {
    /// Write to this formatter
    f: &'a mut Formatter<'b>,
}

impl TreeRenderVisitor<'_, '_> {
    // Unicode box-drawing characters for creating borders and connections.
    const LTCORNER: &'static str = "┌"; // Left top corner
    const RTCORNER: &'static str = "┐"; // Right top corner
    const LDCORNER: &'static str = "└"; // Left bottom corner
    const RDCORNER: &'static str = "┘"; // Right bottom corner

    const TMIDDLE: &'static str = "┬"; // Top T-junction (connects down)
    const LMIDDLE: &'static str = "├"; // Left T-junction (connects right)
    const DMIDDLE: &'static str = "┴"; // Bottom T-junction (connects up)

    const VERTICAL: &'static str = "│"; // Vertical line
    const HORIZONTAL: &'static str = "─"; // Horizontal line

    // TODO: Make these variables configurable.
    const MAXIMUM_RENDER_WIDTH: usize = 240; // Maximum total width of the rendered tree
    const NODE_RENDER_WIDTH: usize = 29; // Width of each node's box
    const MAX_EXTRA_LINES: usize = 30; // Maximum number of extra info lines per node

    /// Main entry point for rendering an execution plan as a tree.
    /// The rendering process happens in three stages for each level of the tree:
    /// 1. Render top borders and connections
    /// 2. Render node content and vertical connections
    /// 3. Render bottom borders and connections
    fn visit<T: FormattedTreeNode>(&mut self, plan: &T) -> Result<(), fmt::Error> {
        let root = RenderTree::create_tree(plan);

        for y in 0..root.height {
            // Start by rendering the top layer.
            self.render_top_layer(&root, y)?;
            // Now we render the content of the boxes
            self.render_box_content(&root, y)?;
            // Render the bottom layer of each of the boxes
            self.render_bottom_layer(&root, y)?;
        }

        Ok(())
    }

    /// Renders the top layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Top corners (┌─┐) for nodes
    /// - Horizontal connections between nodes
    /// - Vertical connections to parent nodes
    fn render_top_layer(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        for x in 0..root.width {
            if root.has_node(x, y) {
                write!(self.f, "{}", Self::LTCORNER)?;
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                if y == 0 {
                    // top level node: no node above this one
                    write!(self.f, "{}", Self::HORIZONTAL)?;
                } else {
                    // render connection to node above this one
                    write!(self.f, "{}", Self::DMIDDLE)?;
                }
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                write!(self.f, "{}", Self::RTCORNER)?;
            } else {
                let mut has_adjacent_nodes = false;
                for i in 0..(root.width - x) {
                    has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
                }
                if !has_adjacent_nodes {
                    // There are no nodes to the right side of this position
                    // no need to fill the empty space
                    continue;
                }
                // there are nodes next to this, fill the space
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH))?;
            }
        }
        writeln!(self.f)?;

        Ok(())
    }

    /// Renders the content layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Node names and extra information
    /// - Vertical borders (│) for boxes
    /// - Vertical connections between nodes
    fn render_box_content(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        let mut extra_info: Vec<Vec<String>> = vec![vec![]; root.width];
        let mut extra_height = 0;

        for (x, extra_info_item) in extra_info.iter_mut().enumerate().take(root.width) {
            if let Some(node) = root.get_node(x, y) {
                Self::split_up_extra_info(
                    &node.extra_text,
                    extra_info_item,
                    Self::MAX_EXTRA_LINES,
                );
                if extra_info_item.len() > extra_height {
                    extra_height = extra_info_item.len();
                }
            }
        }

        let halfway_point = extra_height.div_ceil(2);

        // Render the actual node.
        for render_y in 0..=extra_height {
            for (x, _) in root.nodes.iter().enumerate().take(root.width) {
                if x * Self::NODE_RENDER_WIDTH >= Self::MAXIMUM_RENDER_WIDTH {
                    break;
                }

                let mut has_adjacent_nodes = false;
                for i in 0..(root.width - x) {
                    has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
                }

                if let Some(node) = root.get_node(x, y) {
                    write!(self.f, "{}", Self::VERTICAL)?;

                    // Rigure out what to render.
                    let mut render_text = String::new();
                    if render_y == 0 {
                        render_text = node.name.clone();
                    } else if render_y <= extra_info[x].len() {
                        render_text = extra_info[x][render_y - 1].clone();
                    }

                    render_text = Self::adjust_text_for_rendering(
                        &render_text,
                        Self::NODE_RENDER_WIDTH - 2,
                    );
                    write!(self.f, "{render_text}")?;

                    if render_y == halfway_point && node.child_positions.len() > 1 {
                        write!(self.f, "{}", Self::LMIDDLE)?;
                    } else {
                        write!(self.f, "{}", Self::VERTICAL)?;
                    }
                } else if render_y == halfway_point {
                    let has_child_to_the_right =
                        Self::should_render_whitespace(root, x, y);
                    if root.has_node(x, y + 1) {
                        // Node right below this one.
                        write!(
                            self.f,
                            "{}",
                            Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2)
                        )?;
                        if has_child_to_the_right {
                            write!(self.f, "{}", Self::TMIDDLE)?;
                            // Have another child to the right, Keep rendering the line.
                            write!(
                                self.f,
                                "{}",
                                Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2)
                            )?;
                        } else {
                            write!(self.f, "{}", Self::RTCORNER)?;
                            if has_adjacent_nodes {
                                // Only a child below this one: fill the reset with spaces.
                                write!(
                                    self.f,
                                    "{}",
                                    " ".repeat(Self::NODE_RENDER_WIDTH / 2)
                                )?;
                            }
                        }
                    } else if has_child_to_the_right {
                        // Child to the right, but no child right below this one: render a full
                        // line.
                        write!(
                            self.f,
                            "{}",
                            Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH)
                        )?;
                    } else if has_adjacent_nodes {
                        // Empty spot: render spaces.
                        write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                    }
                } else if render_y >= halfway_point {
                    if root.has_node(x, y + 1) {
                        // Have a node below this empty spot: render a vertical line.
                        write!(
                            self.f,
                            "{}{}",
                            " ".repeat(Self::NODE_RENDER_WIDTH / 2),
                            Self::VERTICAL
                        )?;
                        if has_adjacent_nodes
                            || Self::should_render_whitespace(root, x, y)
                        {
                            write!(
                                self.f,
                                "{}",
                                " ".repeat(Self::NODE_RENDER_WIDTH / 2)
                            )?;
                        }
                    } else if has_adjacent_nodes
                        || Self::should_render_whitespace(root, x, y)
                    {
                        // Empty spot: render spaces.
                        write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                    }
                } else if has_adjacent_nodes {
                    // Empty spot: render spaces.
                    write!(self.f, "{}", " ".repeat(Self::NODE_RENDER_WIDTH))?;
                }
            }
            writeln!(self.f)?;
        }

        Ok(())
    }

    /// Renders the bottom layer of boxes at the given y-level of the tree.
    /// This includes:
    /// - Bottom corners (└─┘) for nodes
    /// - Horizontal connections between nodes
    /// - Vertical connections to child nodes
    fn render_bottom_layer(
        &mut self,
        root: &RenderTree,
        y: usize,
    ) -> Result<(), fmt::Error> {
        for x in 0..=root.width {
            if x * Self::NODE_RENDER_WIDTH >= Self::MAXIMUM_RENDER_WIDTH {
                break;
            }
            let mut has_adjacent_nodes = false;
            for i in 0..(root.width - x) {
                has_adjacent_nodes = has_adjacent_nodes || root.has_node(x + i, y);
            }
            if root.get_node(x, y).is_some() {
                write!(self.f, "{}", Self::LDCORNER)?;
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                if root.has_node(x, y + 1) {
                    // node below this one: connect to that one
                    write!(self.f, "{}", Self::TMIDDLE)?;
                } else {
                    // no node below this one: end the box
                    write!(self.f, "{}", Self::HORIZONTAL)?;
                }
                write!(
                    self.f,
                    "{}",
                    Self::HORIZONTAL.repeat(Self::NODE_RENDER_WIDTH / 2 - 1)
                )?;
                write!(self.f, "{}", Self::RDCORNER)?;
            } else if root.has_node(x, y + 1) {
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH / 2))?;
                write!(self.f, "{}", Self::VERTICAL)?;
                if has_adjacent_nodes || Self::should_render_whitespace(root, x, y) {
                    write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH / 2))?;
                }
            } else if has_adjacent_nodes || Self::should_render_whitespace(root, x, y) {
                write!(self.f, "{}", &" ".repeat(Self::NODE_RENDER_WIDTH))?;
            }
        }
        writeln!(self.f)?;

        Ok(())
    }

    fn extra_info_separator() -> String {
        "-".repeat(Self::NODE_RENDER_WIDTH - 9)
    }

    fn remove_padding(s: &str) -> String {
        s.trim().to_string()
    }

    fn split_up_extra_info(
        extra_info: &HashMap<String, String>,
        result: &mut Vec<String>,
        max_lines: usize,
    ) {
        if extra_info.is_empty() {
            return;
        }

        result.push(Self::extra_info_separator());

        let mut requires_padding = false;
        let mut was_inlined = false;

        // use BTreeMap for repeatable key order
        let sorted_extra_info: BTreeMap<_, _> = extra_info.iter().collect();
        for (key, value) in sorted_extra_info {
            let mut str = Self::remove_padding(value);
            let mut is_inlined = false;
            let available_width = Self::NODE_RENDER_WIDTH - 7;
            let total_size = key.len() + str.len() + 2;
            let is_multiline = str.contains('\n');

            if str.is_empty() {
                str = key.to_string();
            } else if !is_multiline && total_size < available_width {
                str = format!("{key}: {str}");
                is_inlined = true;
            } else {
                str = format!("{key}:\n{str}");
            }

            if is_inlined && was_inlined {
                requires_padding = false;
            }

            if requires_padding {
                result.push(String::new());
            }

            let mut splits: Vec<String> = str.split('\n').map(String::from).collect();
            if splits.len() > max_lines {
                let mut truncated_splits = Vec::new();
                for split in splits.iter().take(max_lines / 2) {
                    truncated_splits.push(split.clone());
                }
                truncated_splits.push("...".to_string());
                for split in splits.iter().skip(splits.len() - max_lines / 2) {
                    truncated_splits.push(split.clone());
                }
                splits = truncated_splits;
            }
            for split in splits {
                Self::split_string_buffer(&split, result);
            }
            if result.len() > max_lines {
                result.truncate(max_lines);
                result.push("...".to_string());
            }

            requires_padding = true;
            was_inlined = is_inlined;
        }
    }

    /// Adjusts text to fit within the specified width by:
    /// 1. Truncating with ellipsis if too long
    /// 2. Center-aligning within the available space if shorter
    fn adjust_text_for_rendering(source: &str, max_render_width: usize) -> String {
        let render_width = source.chars().count();
        if render_width > max_render_width {
            let truncated = &source[..max_render_width - 3];
            format!("{truncated}...")
        } else {
            let total_spaces = max_render_width - render_width;
            let half_spaces = total_spaces / 2;
            let extra_left_space = if total_spaces % 2 == 0 { 0 } else { 1 };
            format!(
                "{}{}{}",
                " ".repeat(half_spaces + extra_left_space),
                source,
                " ".repeat(half_spaces)
            )
        }
    }

    /// Determines if whitespace should be rendered at a given position.
    /// This is important for:
    /// 1. Maintaining proper spacing between sibling nodes
    /// 2. Ensuring correct alignment of connections between parents and children
    /// 3. Preserving the tree structure's visual clarity
    fn should_render_whitespace(root: &RenderTree, x: usize, y: usize) -> bool {
        let mut found_children = 0;

        for i in (0..=x).rev() {
            let node = root.get_node(i, y);
            if root.has_node(i, y + 1) {
                found_children += 1;
            }
            if let Some(node) = node {
                if node.child_positions.len() > 1
                    && found_children < node.child_positions.len()
                {
                    return true;
                }

                return false;
            }
        }

        false
    }

    fn split_string_buffer(source: &str, result: &mut Vec<String>) {
        let mut character_pos = 0;
        let mut start_pos = 0;
        let mut render_width = 0;
        let mut last_possible_split = 0;

        let chars: Vec<char> = source.chars().collect();

        while character_pos < chars.len() {
            // Treating each char as width 1 for simplification
            let char_width = 1;

            // Does the next character make us exceed the line length?
            if render_width + char_width > Self::NODE_RENDER_WIDTH - 2 {
                if start_pos + 8 > last_possible_split {
                    // The last character we can split on is one of the first 8 characters of the line
                    // to not create very small lines we instead split on the current character
                    last_possible_split = character_pos;
                }

                result.push(source[start_pos..last_possible_split].to_string());
                render_width = character_pos - last_possible_split;
                start_pos = last_possible_split;
                character_pos = last_possible_split;
            }

            // check if we can split on this character
            if Self::can_split_on_this_char(chars[character_pos]) {
                last_possible_split = character_pos;
            }

            character_pos += 1;
            render_width += char_width;
        }

        if source.len() > start_pos {
            // append the remainder of the input
            result.push(source[start_pos..].to_string());
        }
    }

    fn can_split_on_this_char(c: char) -> bool {
        (!c.is_ascii_digit() && !c.is_ascii_uppercase() && !c.is_ascii_lowercase())
            && c != '_'
    }
}

/// Trait to connect TreeNode and DisplayAs, which is used to render
/// tree of any TreeNode implementation
pub trait FormattedTreeNode: TreeNode + DisplayAs {
    fn node_name(&self) -> String {
        "".to_string()
    }
}

/// Represents a 2D coordinate in the rendered tree.
/// Used to track positions of nodes and their connections.
struct Coordinate {
    /// Horizontal position in the tree
    x: usize,
    /// Vertical position in the tree
    y: usize,
}

impl Coordinate {
    fn new(x: usize, y: usize) -> Self {
        Coordinate { x, y }
    }
}

/// Represents a node in the render tree, containing information about an execution plan operator
/// and its relationships to other operators.
pub struct RenderTreeNode {
    /// The name of physical `ExecutionPlan`.
    name: String,
    /// Execution info collected from `ExecutionPlan`.
    extra_text: HashMap<String, String>,
    /// Positions of child nodes in the rendered tree.
    child_positions: Vec<Coordinate>,
}

impl RenderTreeNode {
    fn new(name: String, extra_text: HashMap<String, String>) -> Self {
        RenderTreeNode {
            name,
            extra_text,
            child_positions: vec![],
        }
    }

    fn add_child_position(&mut self, x: usize, y: usize) {
        self.child_positions.push(Coordinate::new(x, y));
    }
}

/// Main structure for rendering an execution plan as a tree.
/// Manages a 2D grid of nodes and their layout information.
pub struct RenderTree {
    /// Storage for tree nodes in a flattened 2D grid
    nodes: Vec<Option<Arc<RenderTreeNode>>>,
    /// Total width of the rendered tree
    width: usize,
    /// Total height of the rendered tree
    height: usize,
}

impl RenderTree {
    fn create_tree<T: FormattedTreeNode>(node: &T) -> Self {
        let (width, height) = get_tree_width_height(node);

        let mut result = Self::new(width, height);

        create_tree_recursive(&mut result, node, 0, 0);

        result
    }

    fn new(width: usize, height: usize) -> Self {
        RenderTree {
            nodes: vec![None; (width + 1) * (height + 1)],
            width,
            height,
        }
    }

    fn get_node(&self, x: usize, y: usize) -> Option<Arc<RenderTreeNode>> {
        if x >= self.width || y >= self.height {
            return None;
        }

        let pos = self.get_position(x, y);
        self.nodes.get(pos).and_then(|node| node.clone())
    }

    fn set_node(&mut self, x: usize, y: usize, node: Arc<RenderTreeNode>) {
        let pos = self.get_position(x, y);
        if let Some(slot) = self.nodes.get_mut(pos) {
            *slot = Some(node);
        }
    }

    fn has_node(&self, x: usize, y: usize) -> bool {
        if x >= self.width || y >= self.height {
            return false;
        }

        let pos = self.get_position(x, y);
        self.nodes.get(pos).is_some_and(|node| node.is_some())
    }

    fn get_position(&self, x: usize, y: usize) -> usize {
        y * self.width + x
    }

    fn fmt_display<T: FormattedTreeNode>(node: &T) -> impl fmt::Display + '_ {
        struct Wrapper<'a, T: FormattedTreeNode> {
            node: &'a T,
        }

        impl<T: FormattedTreeNode> fmt::Display for Wrapper<'_, T>
        where
            T: FormattedTreeNode,
        {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                self.node.fmt_as(DisplayFormatType::TreeRender, f)
            }
        }
        Wrapper { node }
    }
}

/// Calculates the required dimensions of the tree.
/// This ensures we allocate enough space for the entire tree structure.
///
/// # Arguments
/// * `plan` - The execution plan to measure
///
/// # Returns
/// * A tuple of (width, height) representing the dimensions needed for the tree
pub fn get_tree_width_height<T: FormattedTreeNode>(plan: &T) -> (usize, usize) {
    let is_empty_ref = &mut true;
    let width = &mut 0;
    let height = &mut 0;
    plan.apply_children(|c| {
        *is_empty_ref = false;
        let (child_width, child_height) = get_tree_width_height(c);
        *width += child_width;
        *height = cmp::max(*height, child_height);
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    if *is_empty_ref {
        return (1, 1);
    }

    *height += 1;

    (*width, *height)
}

/// Recursively builds the render tree structure.
/// Traverses the execution plan and creates corresponding render nodes while
/// maintaining proper positioning and parent-child relationships.
///
/// # Arguments
/// * `result` - The render tree being constructed
/// * `plan` - Current execution plan node being processed
/// * `x` - Horizontal position in the tree
/// * `y` - Vertical position in the tree
///
/// # Returns
/// * The width of the subtree rooted at the current node
pub fn create_tree_recursive<T: FormattedTreeNode>(
    result: &mut RenderTree,
    plan: &T,
    x: usize,
    y: usize,
) -> usize {
    let display_info = RenderTree::fmt_display(plan).to_string();
    let mut extra_info = HashMap::new();

    // Parse the key-value pairs from the formatted string.
    // See DisplayFormatType::TreeRender for details
    for line in display_info.lines() {
        if let Some((key, value)) = line.split_once('=') {
            extra_info.insert(key.to_string(), value.to_string());
        } else {
            extra_info.insert(line.to_string(), "".to_string());
        }
    }

    let mut rendered_node = RenderTreeNode::new(plan.node_name(), extra_info);
    let is_empty_ref = &mut true;
    let width_ref = &mut 0;

    TreeNode::apply_children(plan, |n| {
        *is_empty_ref = false;
        let child_x = x + *width_ref;
        let child_y = y + 1;
        rendered_node.add_child_position(child_x, child_y);
        *width_ref += create_tree_recursive(result, n, child_x, child_y);
        return Ok(TreeNodeRecursion::Continue);
    })
    .unwrap();

    if *is_empty_ref {
        result.set_node(x, y, Arc::new(rendered_node));
        return 1;
    }

    result.set_node(x, y, Arc::new(rendered_node));

    *width_ref
}

// render the whole tree
pub fn tree_render<'a, T: FormattedTreeNode>(n: &'a T) -> impl fmt::Display + use<'a, T> {
    struct Wrapper<'a, T: FormattedTreeNode> {
        n: &'a T,
    }
    impl<T: FormattedTreeNode> fmt::Display for Wrapper<'_, T> {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            let mut visitor = TreeRenderVisitor { f };
            visitor.visit(self.n)
        }
    }

    Wrapper { n: n }
}
