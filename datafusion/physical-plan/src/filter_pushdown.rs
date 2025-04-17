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

use crate::ExecutionPlan;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

#[derive(Clone, Debug)]
pub struct FilterDescription {
    /// Expressions coming from the parent nodes
    pub filters: Vec<Arc<dyn PhysicalExpr>>,
}

impl Default for FilterDescription {
    fn default() -> Self {
        Self::empty()
    }
}

impl FilterDescription {
    /// Takes the filters out of the struct, leaving an empty vector in its place.
    pub fn take_description(&mut self) -> Vec<Arc<dyn PhysicalExpr>> {
        std::mem::take(&mut self.filters)
    }

    pub fn empty() -> FilterDescription {
        Self { filters: vec![] }
    }
}

#[derive(Debug)]
pub enum FilterPushdownSupport<T> {
    Supported {
        // Filter predicates which can be pushed down through the operator.
        // NOTE that these are not placed into any operator.
        child_descriptions: Vec<FilterDescription>,
        // Possibly updated new operator
        op: T,
        // Whether the node is removed from the plan and the rule should be re-run manually
        // on the new node.
        // TODO: If TreeNodeRecursion supports Revisit mechanism, this flag can be removed
        revisit: bool,
    },
    NotSupported,
}

#[derive(Debug)]
pub struct FilterPushdownResult<T> {
    pub support: FilterPushdownSupport<T>,
    // Filters which cannot be pushed down through the operator.
    // NOTE that caller of try_pushdown_filters() should handle these remanining predicates,
    // possibly introducing a FilterExec on top of this operator.
    pub remaining_description: FilterDescription,
}

pub fn filter_pushdown_not_supported<T>(
    remaining_description: FilterDescription,
) -> FilterPushdownResult<T> {
    FilterPushdownResult {
        support: FilterPushdownSupport::NotSupported,
        remaining_description,
    }
}

pub fn filter_pushdown_transparent<T>(
    plan: Arc<dyn ExecutionPlan>,
    fd: FilterDescription,
) -> FilterPushdownResult<Arc<dyn ExecutionPlan>> {
    let child_descriptions = vec![fd];
    let remaining_description = FilterDescription::empty();

    FilterPushdownResult {
        support: FilterPushdownSupport::Supported {
            child_descriptions,
            op: plan,
            revisit: false,
        },
        remaining_description,
    }
}
