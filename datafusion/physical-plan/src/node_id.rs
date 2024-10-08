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

use datafusion_common::DataFusionError;

// Util for traversing ExecutionPlan tree and annotating node_id
pub struct NodeIdAnnotator {
    next_id: usize,
}

impl NodeIdAnnotator {
    pub fn new() -> Self {
        NodeIdAnnotator { next_id: 0 }
    }

    fn annotate_execution_plan_with_node_id(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let plan_with_id = plan.clone().with_node_id(self.next_id)?.unwrap_or(plan);
        self.next_id += 1;
        Ok(plan_with_id)
    }
}

pub fn annotate_node_id_for_execution_plan(
    plan: &Arc<dyn ExecutionPlan>,
    annotator: &mut NodeIdAnnotator,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let mut new_children: Vec<Arc<dyn ExecutionPlan>> = vec![];
    for child in plan.children() {
        let new_child: Arc<dyn ExecutionPlan> =
            annotate_node_id_for_execution_plan(child, annotator)?;
        new_children.push(new_child);
    }
    let new_plan = plan.clone().with_new_children(new_children)?;
    let new_plan_with_id = annotator.annotate_execution_plan_with_node_id(new_plan)?;
    Ok(new_plan_with_id)
}
