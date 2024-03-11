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

use std::sync::Arc;

/// [`AnalyzerRule`]s transform [`LogicalPlan`]s in some way to make
/// the plan valid prior to the rest of the DataFusion optimization process.
///
/// This is different than an [`OptimizerRule`](crate::OptimizerRule)
/// which must preserve the semantics of the `LogicalPlan`, while computing
/// results in a more optimal way.
///
/// For example, an `AnalyzerRule` may resolve [`Expr`]s into more specific
/// forms such as a subquery reference, or do type coercion to ensure the types
/// of operands are correct.
///
/// Use [`SessionState::register_analyzer_rule`] to register additional
/// `AnalyzerRule`s.
///
/// [`SessionState::register_analyzer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionState.html#method.register_analyzer_rule
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;

use crate::LogicalPlan;

pub trait AnalyzerRule {
    /// Rewrite `plan`
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan>;

    /// A human readable name for this analyzer rule
    fn name(&self) -> &str;
}

pub type AnalyzerRuleRef = Arc<dyn AnalyzerRule + Send + Sync>;
