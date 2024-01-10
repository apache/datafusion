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

//! This module provides common traits for visiting or rewriting tree nodes easily.

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use crate::physical_expr::{with_new_children_if_necessary, PhysicalExpr};

use datafusion_common::tree_node::{ConcreteTreeNode, DynTreeNode};
use datafusion_common::Result;

impl DynTreeNode for dyn PhysicalExpr {
    fn arc_children(&self) -> Vec<Arc<Self>> {
        self.children()
    }

    fn with_new_arc_children(
        &self,
        arc_self: Arc<Self>,
        new_children: Vec<Arc<Self>>,
    ) -> Result<Arc<Self>> {
        with_new_children_if_necessary(arc_self, new_children)
    }
}

#[derive(Debug, Clone)]
pub struct ExprContext<T: Sized> {
    pub expr: Arc<dyn PhysicalExpr>,
    pub data: T,
    pub children: Vec<Self>,
}

impl<T> ExprContext<T> {
    pub fn new(expr: Arc<dyn PhysicalExpr>, data: T, children: Vec<Self>) -> Self {
        Self {
            expr,
            data,
            children,
        }
    }

    pub fn update_plan_from_children(mut self) -> Result<Self> {
        let children_plans = self.children.iter().map(|c| c.expr.clone()).collect();
        self.expr = self.expr.with_new_children(children_plans)?;
        Ok(self)
    }
}

impl<T: Default> ExprContext<T> {
    pub fn new_default(plan: Arc<dyn PhysicalExpr>) -> Self {
        let children = plan.children().into_iter().map(Self::new_default).collect();
        Self::new(plan, Default::default(), children)
    }
}

impl<T: Display> Display for ExprContext<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "expr: {:?}", self.expr)?;
        write!(f, "data:{}", self.data)?;
        write!(f, "")
    }
}

impl<T> ConcreteTreeNode for ExprContext<T> {
    fn children(&self) -> Vec<&Self> {
        self.children.iter().collect()
    }

    fn take_children(mut self) -> (Self, Vec<Self>) {
        let children = std::mem::take(&mut self.children);
        (self, children)
    }

    fn with_new_children(mut self, children: Vec<Self>) -> Result<Self> {
        let children_plans = children.iter().map(|c| c.expr.clone()).collect::<Vec<_>>();
        self.expr = with_new_children_if_necessary(self.expr, children_plans)?;
        self.children = children;
        Ok(self)
    }
}
