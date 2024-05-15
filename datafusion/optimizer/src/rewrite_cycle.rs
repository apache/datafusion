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

/// An API for executing a sequence of [TreeNodeRewriter]s in multiple passes.
///
/// See [RewriteCycle] for more information.
///
use std::ops::ControlFlow;

use datafusion_common::{
    tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    Result,
};

/// A builder with methods for executing a "rewrite cycle".
///
/// Often the results of one optimization rule can uncover more optimizations in other optimization
/// rules. A sequence of optimization rules can be ran in multiple "passes" until there are no
/// more optmizations to make.
///
/// The [RewriteCycle] handles logic for running these multi-pass loops.
/// It applies a sequence of [TreeNodeRewriter]s to a [TreeNode] by calling
/// [TreeNode::rewrite] in a loop - passing the output of one rewrite as the input to the next
/// rewrite - until [RewriteCycle::max_cycles] is reached or until every [TreeNode::rewrite]
/// returns a [Transformed::no] result in a consecutive sequence.
#[derive(Debug)]
pub struct RewriteCycle {
    max_cycles: usize,
}

impl Default for RewriteCycle {
    fn default() -> Self {
        Self::new()
    }
}

impl RewriteCycle {
    /// The default maximum number of completed cycles to run before terminating the rewrite loop.
    /// You can override this default with [Self::with_max_cycles]
    pub const DEFAULT_MAX_CYCLES: usize = 3;

    /// Creates a new [RewriteCycle] with default options.
    pub fn new() -> Self {
        Self {
            max_cycles: Self::DEFAULT_MAX_CYCLES,
        }
    }
    /// Sets the [Self::max_cycles] to run before terminating the rewrite loop.
    pub fn with_max_cycles(mut self, max_cycles: usize) -> Self {
        self.max_cycles = max_cycles;
        self
    }

    /// The maximum number of completed cycles to run before terminating the rewrite loop.
    /// Defaults to [Self::DEFAULT_MAX_CYCLES].
    pub fn max_cycles(&self) -> usize {
        self.max_cycles
    }

    /// Runs a rewrite cycle on the given [TreeNode] using the given callback function to
    /// explicitly handle the cycle iterations.
    ///
    /// The callback function is given a [RewriteCycleState], which manages the short-circuiting
    /// logic of the loop. The function is expected to call [RewriteCycleState::rewrite] for each
    /// individual [TreeNodeRewriter] in the cycle. [RewriteCycleState::rewrite] returns a [RewriteCycleControlFlow]
    /// result, indicating whether the loop should break or continue.
    ///
    /// ```rust
    ///
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use datafusion_expr::{col, lit};
    /// use datafusion_common::{DataFusionError, ToDFSchema};
    /// use datafusion_expr::execution_props::ExecutionProps;
    /// use datafusion_expr::simplify::SimplifyContext;
    /// use datafusion_optimizer::rewrite_cycle::RewriteCycle;
    /// use datafusion_optimizer::simplify_expressions::{Simplifier, ConstEvaluator};
    ///
    /// // Create the schema
    /// let schema = Schema::new(vec![
    ///     Field::new("c1", DataType::Int64, true),
    ///     Field::new("c2", DataType::UInt32, true),
    /// ]).to_dfschema_ref().unwrap();
    ///
    /// // Create the rewriters
    /// let props = ExecutionProps::new();
    /// let context = SimplifyContext::new(&props)
    ///    .with_schema(schema);
    /// let mut simplifier = Simplifier::new(&context);
    /// let mut const_evaluator = ConstEvaluator::try_new(&props).unwrap();
    ///
    /// // ((c4 < 1 or c3 < 2) and c3 < 3) and false
    /// let expr = col("c2")
    ///     .lt(lit(1))
    ///     .or(col("c1").lt(lit(2)))
    ///     .and(col("c1").lt(lit(3)))
    ///     .and(lit(false));
    ///
    /// // run the rewrite cycle loop
    /// let (expr, info) = RewriteCycle::new()
    ///     .with_max_cycles(4)
    ///     .each_cycle(expr, |cycle_state| {
    ///         cycle_state
    ///             .rewrite(&mut const_evaluator)?
    ///             .rewrite(&mut simplifier)
    ///     }).unwrap();
    /// assert_eq!(expr, lit(false));
    /// assert_eq!(info.completed_cycles(), 2);
    /// assert_eq!(info.total_iterations(), 4);
    /// ```
    ///
    pub fn each_cycle<
        Node: TreeNode,
        F: FnMut(
            RewriteCycleState<Node>,
        ) -> RewriteCycleControlFlow<RewriteCycleState<Node>>,
    >(
        &self,
        node: Node,
        mut f: F,
    ) -> Result<(Node, RewriteCycleInfo)> {
        let mut state = RewriteCycleState::new(node);
        if self.max_cycles == 0 {
            return state.finish();
        }
        // run first cycle then record number of rewriters
        state = match f(state) {
            ControlFlow::Break(result) => return result?.finish(),
            ControlFlow::Continue(node) => node,
        };
        state.record_cycle_length();
        if state.is_done() {
            return state.finish();
        }
        // run remaining cycles
        match (1..self.max_cycles).try_fold(state, |state, _| f(state)) {
            ControlFlow::Break(result) => result?.finish(),
            ControlFlow::Continue(state) => state.finish(),
        }
    }
}

/// Iteration state of a rewrite cycle. See [RewriteCycle::each_cycle] for usage examples and information.
#[derive(Debug)]
pub struct RewriteCycleState<Node: TreeNode> {
    node: Node,
    consecutive_unchanged_count: usize,
    rewrite_count: usize,
    cycle_length: Option<usize>,
}

impl<Node: TreeNode> RewriteCycleState<Node> {
    fn new(node: Node) -> Self {
        Self {
            node,
            cycle_length: None,
            consecutive_unchanged_count: 0,
            rewrite_count: 0,
        }
    }

    /// Records the rewrite cycle length based on the current iteration count
    ///
    /// When the total number of writers is not known upfront - such as when using
    /// [RewriteCycle::each_cycle] we need to keep count of the number of [Self::rewrite]
    /// calls and then record the number at the end of the first cycle.
    fn record_cycle_length(&mut self) {
        self.cycle_length = Some(self.rewrite_count);
    }

    /// Returns true when the loop has reached the maximum cycle length or when we've received
    /// consecutive unchanged tree nodes equal to the total number of rewriters.
    fn is_done(&self) -> bool {
        // default value indicates we have not completed a cycle
        let Some(cycle_length) = self.cycle_length else {
            return false;
        };
        self.consecutive_unchanged_count >= cycle_length
    }

    /// Finishes the iteration by consuming the state and returning a [TreeNode] and
    /// [RewriteCycleInfo]
    fn finish(self) -> Result<(Node, RewriteCycleInfo)> {
        Ok((
            self.node,
            RewriteCycleInfo {
                cycle_length: self.cycle_length.unwrap_or(self.rewrite_count),
                total_iterations: self.rewrite_count,
            },
        ))
    }

    /// Calls [TreeNode::rewrite] and determines if the rewrite cycle should break or continue
    /// based on the current [RewriteCycleState].
    pub fn rewrite<R: TreeNodeRewriter<Node = Node> + ?Sized>(
        mut self,
        rewriter: &mut R,
    ) -> RewriteCycleControlFlow<Self> {
        match self.node.rewrite(rewriter) {
            Err(e) => ControlFlow::Break(Err(e)),
            Ok(Transformed {
                data: node,
                transformed,
                ..
            }) => {
                self.node = node;
                self.rewrite_count += 1;
                if transformed {
                    self.consecutive_unchanged_count = 0;
                } else {
                    self.consecutive_unchanged_count += 1;
                }
                if self.is_done() {
                    ControlFlow::Break(Ok(self))
                } else {
                    ControlFlow::Continue(self)
                }
            }
        }
    }
}

/// Information about a rewrite cycle, such as total number of iterations and number of fully
/// completed cycles. This is useful for testing purposes to ensure that optimzation passes are
/// working as expected.
#[derive(Debug, Clone, Copy)]
pub struct RewriteCycleInfo {
    total_iterations: usize,
    cycle_length: usize,
}

impl RewriteCycleInfo {
    /// The total number of **fully completed** cycles.
    pub fn completed_cycles(&self) -> usize {
        self.total_iterations / self.cycle_length
    }

    /// The total number of [TreeNode::rewrite] calls.
    pub fn total_iterations(&self) -> usize {
        self.total_iterations
    }

    /// The number of [TreeNode::rewrite] calls within a single cycle.
    pub fn cycle_length(&self) -> usize {
        self.cycle_length
    }
}

pub type RewriteCycleControlFlow<T> = ControlFlow<Result<T>, T>;
