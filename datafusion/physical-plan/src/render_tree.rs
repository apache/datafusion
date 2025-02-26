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

// This code is based on the DuckDB’s implementation:
// https://github.com/duckdb/duckdb/blob/main/src/include/duckdb/common/render_tree.hpp

use std::collections::HashMap;
use std::fmt::Formatter;
use std::sync::Arc;
use std::{cmp, fmt};

use crate::{DisplayFormatType, ExecutionPlan};

// TODO: It's never used.
#[allow(dead_code)]
pub struct Coordinate {
    pub x: usize,
    pub y: usize,
}

impl Coordinate {
    pub fn new(x: usize, y: usize) -> Self {
        Coordinate { x, y }
    }
}

/// `RenderTreeNode` stores display info for `ExecutionPlan`.
pub struct RenderTreeNode {
    /// The name of physical `ExecutionPlan`.
    pub name: String,
    /// Execution info collected from `ExecutionPlan`.
    pub extra_text: HashMap<String, String>,
    /// Current `ExecutionPlan`'s children position.
    pub child_positions: Vec<Coordinate>,
}

impl RenderTreeNode {
    pub fn new(name: String, extra_text: HashMap<String, String>) -> Self {
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

pub struct RenderTree {
    pub nodes: Vec<Option<Arc<RenderTreeNode>>>,
    pub width: usize,
    pub height: usize,
}

impl RenderTree {
    /// Create a render tree based on the root of physical plan node.
    pub fn create_tree(plan: &dyn ExecutionPlan) -> Self {
        let (width, height) = get_tree_width_height(plan);

        let mut result = Self::new(width, height);

        create_tree_recursive(&mut result, plan, 0, 0);

        result
    }

    fn new(width: usize, height: usize) -> Self {
        RenderTree {
            nodes: vec![None; (width + 1) * (height + 1)],
            width,
            height,
        }
    }

    pub fn get_node(&self, x: usize, y: usize) -> Option<Arc<RenderTreeNode>> {
        if x >= self.width || y >= self.height {
            return None;
        }

        let pos = self.get_position(x, y);
        self.nodes.get(pos).and_then(|node| node.clone())
    }

    pub fn set_node(&mut self, x: usize, y: usize, node: Arc<RenderTreeNode>) {
        let pos = self.get_position(x, y);
        if let Some(slot) = self.nodes.get_mut(pos) {
            *slot = Some(node);
        }
    }

    pub fn has_node(&self, x: usize, y: usize) -> bool {
        if x >= self.width || y >= self.height {
            return false;
        }

        let pos = self.get_position(x, y);
        self.nodes.get(pos).is_some_and(|node| node.is_some())
    }

    fn get_position(&self, x: usize, y: usize) -> usize {
        y * self.width + x
    }
}

fn get_tree_width_height(plan: &dyn ExecutionPlan) -> (usize, usize) {
    let children = plan.children();

    if children.is_empty() {
        return (1, 1);
    }

    let mut width = 0;
    let mut height = 0;

    for child in children {
        let (child_width, child_height) = get_tree_width_height(child.as_ref());
        width += child_width;
        height = cmp::max(height, child_height);
    }

    height += 1;

    (width, height)
}

fn fmt_display(plan: &dyn ExecutionPlan) -> impl fmt::Display + '_ {
    struct Wrapper<'a> {
        plan: &'a dyn ExecutionPlan,
    }

    impl fmt::Display for Wrapper<'_> {
        fn fmt(&self, f: &mut Formatter) -> fmt::Result {
            self.plan.fmt_as(DisplayFormatType::TreeRender, f)?;
            Ok(())
        }
    }

    Wrapper { plan }
}

fn create_tree_recursive(
    result: &mut RenderTree,
    plan: &dyn ExecutionPlan,
    x: usize,
    y: usize,
) -> usize {
    let display_info = fmt_display(plan).to_string();
    let mut extra_info = HashMap::new();

    // Parse the key-value pairs from the formatted string.
    for line in display_info.lines() {
        if let Some((key, value)) = line.split_once('=') {
            extra_info.insert(key.to_string(), value.to_string());
        }
    }

    let mut node = RenderTreeNode::new(plan.name().to_string(), extra_info);

    let children = plan.children();

    if children.is_empty() {
        result.set_node(x, y, Arc::new(node));
        return 1;
    }

    let mut width = 0;
    for child in children {
        let child_x = x + width;
        let child_y = y + 1;
        node.add_child_position(child_x, child_y);
        width += create_tree_recursive(result, child.as_ref(), child_x, child_y);
    }

    result.set_node(x, y, Arc::new(node));

    width
}
