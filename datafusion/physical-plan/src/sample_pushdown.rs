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

//! Sample (TABLESAMPLE) pushdown types.
//!
//! Used by [`ExecutionPlan::try_push_sample`] to communicate per-node
//! pushdown intent. The optimizer rule walking the tree decides what
//! to do with the answer:
//!
//! * [`SamplePushdownResult::Absorbed`] — the node has absorbed the
//!   sample and the rule should drop the surrounding `SampleExec`.
//! * [`SamplePushdownResult::Passthrough`] — the node is row-preserving
//!   (or otherwise commutes with the requested sampling); the rule
//!   should recurse into its single child and rebuild the node from
//!   the result.
//! * [`SamplePushdownResult::Unsupported`] — pushdown stops here. Today
//!   the rule errors; once a generic `SampleExec` filter exists, this
//!   becomes "leave the SampleExec in place."
//!
//! [`ExecutionPlan::try_push_sample`]: crate::ExecutionPlan::try_push_sample

use std::sync::Arc;

use crate::ExecutionPlan;

/// SQL TABLESAMPLE method, mirrored from
/// [`datafusion_expr::SampleMethod`] so the physical layer doesn't
/// have to depend on `datafusion-expr`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SampleMethod {
    /// Block-level sampling (Postgres `SYSTEM`, Hive `BLOCK`).
    System,
}

/// Specification of a sample request. Crossed the trait boundary for
/// `try_push_sample`.
#[derive(Debug, Clone)]
pub struct SampleSpec {
    /// Sampling method.
    pub method: SampleMethod,
    /// Target fraction of rows to keep, in `(0.0, 1.0]`.
    pub fraction: f64,
    /// Optional `REPEATABLE(seed)` for deterministic selection.
    pub seed: Option<u64>,
}

/// Outcome of a `try_push_sample` call on a single node.
#[derive(Debug, Clone)]
pub enum SamplePushdownResult {
    /// The node has absorbed the sample. The rule should treat
    /// `inner` as the replacement for the original node *and* drop
    /// the surrounding `SampleExec`.
    Absorbed {
        /// The new node, configured with sampling.
        inner: Arc<dyn ExecutionPlan>,
    },
    /// The node is row-preserving for the requested sampling. The
    /// rule should:
    ///
    /// 1. Recurse into the node's single child with the same
    ///    [`SampleSpec`].
    /// 2. If the child returns `Absorbed`/`Passthrough`, rebuild
    ///    this node with the new child via `with_new_children` and
    ///    drop the surrounding `SampleExec`.
    Passthrough,
    /// Pushdown stops here. Today the rule treats this as a
    /// planning error; in the future it will leave a generic
    /// `SampleExec` in place.
    Unsupported {
        /// Human-readable reason; included in the error message.
        reason: String,
    },
}

impl SamplePushdownResult {
    /// Convenience: an `Unsupported` with a static reason.
    pub fn unsupported_static(reason: &'static str) -> Self {
        Self::Unsupported {
            reason: reason.to_string(),
        }
    }
}
