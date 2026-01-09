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

//! Documentation structures for execution metrics and operators.

/// Groupings and exports for metrics documentation (mirrors how function doc sections are exposed).
pub mod metric_doc_sections {
    pub use super::{
        DocumentedExec, DocumentedMetrics, ExecDoc, ExecDocEntry, MetricDoc,
        MetricDocEntry, MetricDocPosition, MetricFieldDoc, exec_docs, metric_docs,
    };
    pub use inventory;
}

/// Whether a metrics struct should be documented as common or operator-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricDocPosition {
    /// Metrics that are reused across operators (for example `BaselineMetrics`).
    Common,
    /// Metrics that are tied to a specific operator.
    Operator,
}

/// Documentation for a single metric field.
#[derive(Debug)]
pub struct MetricFieldDoc {
    /// Name of the metric.
    pub name: &'static str,
    /// Documentation for the metric.
    pub doc: &'static str,
    /// Type name of the metric field.
    pub type_name: &'static str,
}

/// Documentation attached to a metrics struct.
#[derive(Debug)]
pub struct MetricDoc {
    /// Name of the metrics struct (usually ends with `Metrics`).
    pub name: &'static str,
    /// Documentation from the struct-level doc comment.
    pub doc: &'static str,
    /// Documentation for each metric field.
    pub fields: &'static [MetricFieldDoc],
    /// Whether the metrics are common or operator-specific.
    pub position: MetricDocPosition,
}

/// Documentation for an execution plan implementation.
#[derive(Debug)]
pub struct ExecDoc {
    /// Name of the execution plan struct (usually ends with `Exec`).
    pub name: &'static str,
    /// Documentation from the struct-level doc comment.
    pub doc: &'static str,
    /// Metrics exposed by this operator.
    pub metrics: &'static [&'static MetricDoc],
}

/// Trait implemented for metrics structs to expose their documentation.
pub trait DocumentedMetrics {
    /// Static documentation for this metrics struct.
    const DOC: &'static MetricDoc;

    /// Returns the documentation for this metrics struct.
    fn metric_doc() -> &'static MetricDoc {
        Self::DOC
    }
}

/// Trait implemented for execution plan structs to expose their documentation.
pub trait DocumentedExec {
    /// Returns the documentation for this operator.
    fn exec_doc() -> &'static ExecDoc;
}

#[derive(Debug)]
pub struct MetricDocEntry(pub &'static MetricDoc);

#[derive(Debug)]
pub struct ExecDocEntry(pub &'static ExecDoc);

/// Iterate over all registered metrics docs.
pub fn metric_docs() -> impl Iterator<Item = &'static MetricDoc> {
    inventory::iter::<MetricDocEntry>
        .into_iter()
        .map(|entry| entry.0)
}

/// Iterate over all registered execution plan docs.
pub fn exec_docs() -> impl Iterator<Item = &'static ExecDoc> {
    inventory::iter::<ExecDocEntry>
        .into_iter()
        .map(|entry| entry.0)
}

inventory::collect!(MetricDocEntry);
inventory::collect!(ExecDocEntry);
