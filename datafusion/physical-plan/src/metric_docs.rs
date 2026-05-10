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

//! Static [`MetricsDocumentation`] entries for metrics that are reused
//! across operators (e.g. [`BaselineMetrics`]).
//!
//! Operator-specific metrics live next to their `ExecutionPlan` and are
//! returned by `ExecutionPlan::metrics_documentation`. Common-metrics docs
//! cannot piggy-back on an operator, so they are exposed as `pub static`
//! values and aggregated into the documentation registry in
//! `SessionStateDefaults::default_metric_docs`.
//!
//! [`BaselineMetrics`]: crate::metrics::BaselineMetrics

use std::sync::LazyLock;

use datafusion_expr::{MetricPosition, MetricsDocumentation};

/// Documentation for [`BaselineMetrics`](crate::metrics::BaselineMetrics) —
/// the common metrics exposed by most physical operators.
pub static BASELINE_METRICS_DOC: LazyLock<MetricsDocumentation> = LazyLock::new(|| {
    MetricsDocumentation::builder("BaselineMetrics", MetricPosition::Common)
        .with_description(
            "`BaselineMetrics` are available in most physical operators to capture \
             common measurements.",
        )
        .with_metric(
            "elapsed_compute",
            "CPU time the operator actively spends processing work.",
        )
        .with_metric("output_rows", "Total number of rows the operator produces.")
        .with_metric(
            "output_bytes",
            "Memory usage of all output batches. Note: This value may be \
             overestimated. If multiple output `RecordBatch` instances share \
             underlying memory buffers, their sizes will be counted multiple times.",
        )
        .with_metric(
            "output_batches",
            "Total number of output batches the operator produces.",
        )
        .build()
});

/// All common-metric documentation entries exposed by this crate.
pub fn common_metric_docs() -> Vec<&'static MetricsDocumentation> {
    vec![&BASELINE_METRICS_DOC]
}
