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

//! Metrics structs defined in a separate file from the exec.
//! This demonstrates that metric_doc works across file boundaries.

#![allow(dead_code)]

use datafusion_macros::metric_doc;

/// First group of metrics for UserDefinedExec.
/// Tracks phase A execution statistics.
#[metric_doc]
pub struct MetricsGroupA {
    /// Time spent executing phase A
    pub phase_a_time: u64,
    /// Number of rows processed in phase A
    pub phase_a_rows: usize,
}

/// Second group of metrics for UserDefinedExec.
/// Tracks phase B execution statistics.
#[metric_doc]
pub struct MetricsGroupB {
    /// Time spent executing phase B
    pub phase_b_time: u64,
    /// Number of rows processed in phase B
    pub phase_b_rows: usize,
}
