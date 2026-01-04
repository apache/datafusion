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

//! Integration tests for the `#[metric_doc]` macro.
//!
//! These tests verify:
//! 1. Cross-file usage: metrics structs defined in one file, exec in another
//! 2. Multiple metrics groups: one exec referencing multiple metrics structs

mod test_helpers;

use datafusion_doc::metric_doc_sections::{DocumentedExec, DocumentedMetrics};
use test_helpers::separate_exec::UserDefinedExec;
use test_helpers::separate_metrics::{MetricsGroupA, MetricsGroupB};

/// Test that metrics structs in a separate file correctly implement DocumentedMetrics
#[test]
fn test_cross_file_metrics_have_documented_metrics_trait() {
    // MetricsGroupA should implement DocumentedMetrics
    let doc_a = MetricsGroupA::metric_doc();
    assert_eq!(doc_a.name, "MetricsGroupA");
    assert!(doc_a.doc.contains("First group of metrics"));
    assert_eq!(doc_a.fields.len(), 2);

    // MetricsGroupB should implement DocumentedMetrics
    let doc_b = MetricsGroupB::metric_doc();
    assert_eq!(doc_b.name, "MetricsGroupB");
    assert!(doc_b.doc.contains("Second group of metrics"));
    assert_eq!(doc_b.fields.len(), 2);
}

/// Test that an exec with multiple metrics groups correctly implements DocumentedExec
#[test]
fn test_exec_with_multiple_metrics_groups() {
    let exec_doc = UserDefinedExec::exec_doc();

    // Verify exec documentation
    assert_eq!(exec_doc.name, "UserDefinedExec");
    assert!(exec_doc.doc.contains("user-defined execution plan"));

    // Verify that both metrics groups are linked
    assert_eq!(
        exec_doc.metrics.len(),
        2,
        "Expected 2 metrics groups, got {}",
        exec_doc.metrics.len()
    );

    // Verify the metrics are the correct ones (order should match declaration order)
    let metric_names: Vec<&str> = exec_doc.metrics.iter().map(|m| m.name).collect();
    assert_eq!(
        metric_names[0], "MetricsGroupA",
        "Expected MetricsGroupA in metrics, got {}",
        metric_names[0]
    );
    assert_eq!(
        metric_names[1], "MetricsGroupB",
        "Expected MetricsGroupB in metrics, got {}",
        metric_names[1]
    );
}

/// Test that field documentation is correctly extracted from metrics structs
#[test]
fn test_metrics_field_documentation() {
    let doc_a = MetricsGroupA::metric_doc();

    // Check that field docs are extracted
    let field_names: Vec<&str> = doc_a.fields.iter().map(|f| f.name).collect();
    assert_eq!(field_names[0], "phase_a_time");
    assert_eq!(field_names[1], "phase_a_rows");

    // Check that field descriptions are captured
    let time_field = doc_a.fields.iter().find(|f| f.name == "phase_a_time");
    assert_eq!(
        time_field.unwrap().doc.trim(),
        "Time spent executing phase A"
    );
}
