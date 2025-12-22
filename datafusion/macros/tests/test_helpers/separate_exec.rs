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

//! Exec struct that references metrics from a different file.
//! This demonstrates cross-file usage and multiple metrics groups.

#![allow(dead_code)]

use datafusion_macros::metric_doc;

// Import metrics from the separate file
use super::separate_metrics::{MetricsGroupA, MetricsGroupB};

/// A user-defined execution plan that demonstrates:
/// 1. Referencing metrics from a different file
/// 2. Having multiple metrics groups
#[metric_doc(MetricsGroupA, MetricsGroupB)]
pub struct UserDefinedExec {
    /// Some internal state
    pub state: String,
}
