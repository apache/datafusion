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

use datafusion_common::Result;
use datafusion_physical_expr::PhysicalExpr;

/// A source of dynamic runtime filters.
///
/// During query execution, operators implementing this trait can provide
/// filter expressions that other operators can use to dynamically prune data.
pub trait DynamicFilterSource: Send + Sync + std::fmt::Debug + 'static {
    /// Returns a list of filter expressions that can be used for dynamic pruning.
    fn current_filters(&self) -> Result<Vec<Arc<dyn PhysicalExpr>>>;
}
