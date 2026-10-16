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

//! [`CheckContext`]: information about a plan gathered before checks run.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::execution_plan::reset_plan_states;

use crate::physical_plan::exec::{
    collect_output, execute_invalid_partition, task_context_with_pool, wait_for_release,
};
use crate::physical_plan::experiments::{self, Experiment, StreamRun};
use crate::physical_plan::variants::{self, Variant, VariantRun};

/// The output of executing one node: the batches of each output partition
#[derive(Debug, Clone)]
pub struct NodeOutput {
    partitions: Vec<Vec<RecordBatch>>,
}

impl NodeOutput {
    pub(crate) fn new(partitions: Vec<Vec<RecordBatch>>) -> Self {
        Self { partitions }
    }

    /// The batches of each output partition
    pub fn partitions(&self) -> &[Vec<RecordBatch>] {
        &self.partitions
    }

    /// All batches of all partitions, in partition order
    pub fn batches(&self) -> Vec<RecordBatch> {
        self.partitions.concat()
    }

    /// The total number of rows in all partitions
    pub fn num_rows(&self) -> usize {
        self.partition_num_rows().iter().sum()
    }

    /// The number of rows in each partition
    pub fn partition_num_rows(&self) -> Vec<usize> {
        self.partitions
            .iter()
            .map(|batches| batches.iter().map(RecordBatch::num_rows).sum())
            .collect()
    }
}

/// The time allowed for the runs of a [`PlanChecker`]
///
/// [`PlanChecker`]: crate::physical_plan::PlanChecker
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timeouts {
    /// Time allowed for executing a node to completion
    pub execution: Duration,
    /// Time allowed for a stream experiment on inputs that never end, and for
    /// streams and memory to be released
    pub stream: Duration,
}

/// What a [`CheckContext`] gathers
#[derive(Debug, Clone, Copy)]
pub(crate) struct Gather {
    /// Execute every node to completion and record its output
    pub outputs: bool,
    /// Run every variant on every node. Requires `outputs`.
    pub variants: bool,
    /// Run every stream experiment on every node
    pub experiments: bool,
}

/// Information about a plan that checks can use, gathered by the
/// [`PlanChecker`] before any check runs. What it holds depends on the
/// [`CheckKind`]s of the enabled checks:
///
/// - With an execution, variant or stream check, every node of the plan is
///   executed on its own (all of its output partitions, concurrently), and its
///   output is recorded, along with the memory the execution left reserved.
///   Then partition `partition_count` of the node, which does not exist, is
///   executed, and what that did is recorded too.
/// - With a variant check, every [`Variant`] that applies to a node is
///   executed after every node was executed normally, and the outputs are
///   recorded as [`VariantRun`]s.
/// - With a stream check, every [`Experiment`] is run on every node with
///   children, and its [`StreamRun`]s are recorded.
///
/// Each run uses a fresh copy of the node's subtree made with
/// [`reset_plan_states`], so runtime state such as dynamic filters from one
/// execution does not affect the next. The plan passed to the checker is never
/// executed itself. Nodes are identified by address, so a context is only
/// valid for the plan it was created for.
///
/// [`PlanChecker`]: crate::physical_plan::PlanChecker
/// [`CheckKind`]: crate::physical_plan::CheckKind
#[derive(Debug, Default)]
pub struct CheckContext {
    outputs: HashMap<usize, Result<NodeOutput, String>>,
    /// Bytes still reserved after executing each node to completion
    memory: HashMap<usize, usize>,
    /// What executing one partition past the last partition of each node did
    invalid_partitions: HashMap<usize, Option<String>>,
    variant_runs: HashMap<usize, Vec<VariantRun>>,
    stream_runs: HashMap<(usize, Experiment), Vec<StreamRun>>,
}

fn node_key(node: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(node).cast::<()>() as usize
}

impl CheckContext {
    /// Gather what `gather` asks for about every node of `plan`
    pub(crate) async fn gather(
        plan: &Arc<dyn ExecutionPlan>,
        gather: Gather,
        timeouts: Timeouts,
    ) -> Self {
        let mut context = Self::default();
        let mut visited = HashSet::new();
        let mut nodes = vec![];
        let mut stack = vec![Arc::clone(plan)];
        while let Some(node) = stack.pop() {
            stack.extend(node.children().into_iter().cloned());
            let key = node_key(&node);
            if !visited.insert(key) {
                continue;
            }
            if gather.outputs {
                let (output, memory) = execute_node(&node, timeouts).await;
                context.outputs.insert(key, output);
                if let Some(memory) = memory {
                    context.memory.insert(key, memory);
                }
                let outcome = execute_invalid_partition(&node, timeouts.execution).await;
                context.invalid_partitions.insert(key, outcome);
            }
            if gather.experiments {
                for experiment in Experiment::ALL {
                    let runs = experiments::run(&node, experiment, timeouts).await;
                    context.stream_runs.insert((key, experiment), runs);
                }
            }
            nodes.push(node);
        }
        // Variants are sized by the normal outputs of each node and its
        // children, so they run once every node has been executed
        if gather.variants {
            for node in &nodes {
                let runs = variants::run(node, &context, timeouts.execution).await;
                context.variant_runs.insert(node_key(node), runs);
            }
        }
        context
    }

    /// The output of executing `node`, or `None` if it was not executed or
    /// failed
    pub fn output(&self, node: &Arc<dyn ExecutionPlan>) -> Option<&NodeOutput> {
        self.outputs.get(&node_key(node))?.as_ref().ok()
    }

    /// The error from executing `node`, if executing it failed, panicked or
    /// timed out
    pub fn execution_error(&self, node: &Arc<dyn ExecutionPlan>) -> Option<&str> {
        self.outputs
            .get(&node_key(node))?
            .as_ref()
            .err()
            .map(String::as_str)
    }

    /// The outputs of all children of `node`, or `None` if any child was not
    /// executed or failed
    pub fn child_outputs(
        &self,
        node: &Arc<dyn ExecutionPlan>,
    ) -> Option<Vec<&NodeOutput>> {
        node.children()
            .into_iter()
            .map(|child| self.output(child))
            .collect()
    }

    /// The number of bytes that executing `node` to completion left reserved
    /// in the memory pool, after its streams and its copy of the plan were
    /// dropped. `None` if the node was not executed, or executing it failed.
    pub fn memory_reserved_after_execution(
        &self,
        node: &Arc<dyn ExecutionPlan>,
    ) -> Option<usize> {
        self.memory.get(&node_key(node)).copied()
    }

    /// What executing partition `partition_count` of `node`, which does not
    /// exist, did instead of returning an error, on a fresh copy of the node,
    /// polling the stream it returns once. `None` if it returned an error, or
    /// the node was not executed.
    pub(crate) fn invalid_partition_problem(
        &self,
        node: &Arc<dyn ExecutionPlan>,
    ) -> Option<&str> {
        self.invalid_partitions.get(&node_key(node))?.as_deref()
    }

    /// The variant runs of `node`, in the order they ran. Empty if no variant
    /// check is enabled.
    pub fn variant_runs(&self, node: &Arc<dyn ExecutionPlan>) -> &[VariantRun] {
        self.variant_runs
            .get(&node_key(node))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    /// The run of `variant` on `node`, if it ran
    pub fn variant_run(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        variant: Variant,
    ) -> Option<&VariantRun> {
        self.variant_runs(node)
            .iter()
            .find(|run| run.variant == variant)
    }

    /// The output of `node` without its fetch: its normal output if it has no
    /// fetch, and otherwise the output of the plan returned by
    /// `with_fetch(None)`. `None` if that output is not known.
    pub fn unfetched_output(&self, node: &Arc<dyn ExecutionPlan>) -> Option<&NodeOutput> {
        if node.fetch().is_none() {
            return self.output(node);
        }
        self.variant_run(node, Variant::WithoutFetch)?
            .output
            .as_ref()
            .ok()
    }

    /// The runs of `experiment` on `node`. Empty if no stream check is
    /// enabled, or the experiment does not apply to the node, for example
    /// because it has no children.
    pub fn stream_runs(
        &self,
        node: &Arc<dyn ExecutionPlan>,
        experiment: Experiment,
    ) -> &[StreamRun] {
        self.stream_runs
            .get(&(node_key(node), experiment))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

/// Execute every partition of a fresh copy of `node` to completion. Also
/// returns the memory left reserved afterwards, if the execution succeeded.
async fn execute_node(
    node: &Arc<dyn ExecutionPlan>,
    timeouts: Timeouts,
) -> (Result<NodeOutput, String>, Option<usize>) {
    let pool = Arc::new(UnboundedMemoryPool::default());
    let setup = reset_plan_states(Arc::clone(node)).and_then(|node| {
        let task_context = task_context_with_pool(Arc::clone(&pool) as _)?;
        Ok((node, task_context))
    });
    let (node, task_context) = match setup {
        Ok(setup) => setup,
        Err(e) => {
            let error =
                format!("setting up the execution failed: {}", e.strip_backtrace());
            return (Err(error), None);
        }
    };
    let output = collect_output(node, task_context, timeouts.execution)
        .await
        .map_err(|error| error.to_string());
    if output.is_err() {
        return (output, None);
    }
    // Spawned tasks can release their reservations shortly after the streams
    // that own them are dropped
    wait_for_release(|| pool.reserved() == 0, timeouts.stream).await;
    (output, Some(pool.reserved()))
}
