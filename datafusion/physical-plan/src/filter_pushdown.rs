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

use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

#[derive(Debug, Clone)]
pub enum FilterPushdown {
    Supported(Arc<dyn PhysicalExpr>),
    Unsupported(Arc<dyn PhysicalExpr>),
}

#[derive(Debug, Clone)]
pub struct FilterPushdowns {
    pub pushdowns: Vec<FilterPushdown>,
}

impl FilterPushdowns {
    pub fn new(pushdowns: Vec<FilterPushdown>) -> Self {
        Self { pushdowns }
    }

    pub fn all_supported(filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let pushdowns = filters
            .iter()
            .map(|f| FilterPushdown::Supported(Arc::clone(f)))
            .collect();
        Self { pushdowns }
    }

    pub fn all_unsupported(filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let pushdowns = filters
            .iter()
            .map(|f| FilterPushdown::Unsupported(Arc::clone(f)))
            .collect();
        Self { pushdowns }
    }

    /// Transform all filters to supported
    pub fn as_supported(&self) -> Self {
        let pushdowns = self
            .pushdowns
            .iter()
            .map(|f| match f {
                FilterPushdown::Supported(expr) => {
                    FilterPushdown::Supported(Arc::clone(expr))
                }
                FilterPushdown::Unsupported(expr) => {
                    FilterPushdown::Supported(Arc::clone(expr))
                }
            })
            .collect();
        Self { pushdowns }
    }

    pub fn keep_supported(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.pushdowns
            .iter()
            .filter_map(|f| match f {
                FilterPushdown::Supported(expr) => Some(Arc::clone(expr)),
                FilterPushdown::Unsupported(_) => None,
            })
            .collect()
    }

    pub fn keep_unsupported(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.pushdowns
            .iter()
            .filter_map(|f| match f {
                FilterPushdown::Unsupported(expr) => Some(Arc::clone(expr)),
                FilterPushdown::Supported(_) => None,
            })
            .collect()
    }

    pub fn unpack(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.pushdowns
            .iter()
            .map(|f| match f {
                FilterPushdown::Supported(expr) => Arc::clone(expr),
                FilterPushdown::Unsupported(expr) => Arc::clone(expr),
            })
            .collect()
    }

    pub fn into_inner(&self) -> Vec<FilterPushdown> {
        self.pushdowns.clone()
    }

    pub fn supported(
        filters: &[Arc<dyn PhysicalExpr>],
        mut f: impl FnMut(Arc<dyn PhysicalExpr>) -> bool,
    ) -> Self {
        let pushdowns = filters
            .iter()
            .filter(|filt| f(Arc::clone(filt)))
            .map(|f| FilterPushdown::Supported(Arc::clone(f)))
            .collect();
        Self { pushdowns }
    }

    pub fn iter(&self) -> impl Iterator<Item = &FilterPushdown> {
        self.pushdowns.iter()
    }

    pub fn len(&self) -> usize {
        self.pushdowns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pushdowns.is_empty()
    }
}

impl IntoIterator for FilterPushdowns {
    type Item = FilterPushdown;
    type IntoIter = std::vec::IntoIter<FilterPushdown>;

    fn into_iter(self) -> Self::IntoIter {
        self.pushdowns.into_iter()
    }
}

#[derive(Debug, Clone)]
pub struct ChildPushdownResult {
    /// The combined result of pushing down each parent filter into each child.
    pub parent_filters: FilterPushdowns,
    /// The result of pushing down each filter this node provided into each of it's children.
    pub self_filters: Vec<FilterPushdowns>,
}

/// The result of pushing down filters into a node that it returns to its parent.
#[derive(Debug, Clone)]
pub struct FilterPushdownPropagation<T> {
    pub parent_filter_result: FilterPushdowns,
    pub new_node: Option<T>,
}

impl<T> FilterPushdownPropagation<T> {
    /// Create a new [`FilterPushdownPropagation`] that tells the parent node
    /// that echoes back up to the parent the result of pushing down the filters
    /// into the children.
    pub fn transparent(child_pushdown_result: ChildPushdownResult) -> Self {
        Self {
            parent_filter_result: child_pushdown_result.parent_filters,
            new_node: None,
        }
    }

    pub fn unsupported(parent_filters: &[Arc<dyn PhysicalExpr>]) -> Self {
        let unsupported = FilterPushdowns::all_unsupported(parent_filters);
        Self {
            parent_filter_result: unsupported,
            new_node: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilterPushdownPlan {
    pub parent_filters_for_children: Vec<FilterPushdowns>,
    pub self_filters_for_children: Vec<Vec<Arc<dyn PhysicalExpr>>>,
}

impl FilterPushdownPlan {
    pub fn all_unsupported(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        num_children: usize,
    ) -> Self {
        let unsupported = FilterPushdowns::all_unsupported(parent_filters);
        Self {
            parent_filters_for_children: vec![unsupported; num_children],
            self_filters_for_children: vec![vec![]; num_children],
        }
    }

    pub fn all_supported(
        parent_filters: &[Arc<dyn PhysicalExpr>],
        num_children: usize,
    ) -> Self {
        let supported = FilterPushdowns::all_supported(parent_filters);
        Self {
            parent_filters_for_children: vec![supported; num_children],
            self_filters_for_children: vec![vec![]; num_children],
        }
    }

    pub fn with_self_filters_for_children(
        mut self,
        self_filters_for_children: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    ) -> Self {
        self.self_filters_for_children = self_filters_for_children;
        self
    }
}
