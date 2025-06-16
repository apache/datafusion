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

use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{internal_datafusion_err, Result};
use datafusion_expr::LogicalPlan;
use indexmap::IndexMap;

use crate::delim_candidates_collector::DelimCandidate;

type ID = usize;

pub struct DelimCandidateRewriter {
    candidates: IndexMap<ID, DelimCandidate>,
    cur_id: ID,
    // all the node ids from root to the current node
    stack: Vec<usize>,
}

impl DelimCandidateRewriter {
    pub fn new(candidates: IndexMap<ID, DelimCandidate>) -> Self {
        Self {
            candidates,
            cur_id: 0,
            stack: vec![],
        }
    }
}

impl TreeNodeRewriter for DelimCandidateRewriter {
    type Node = LogicalPlan;

    fn f_down(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        self.stack.push(self.cur_id);
        self.cur_id += 1;

        Ok(Transformed::no(plan))
    }

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        let cur_id = self.stack.pop().ok_or(internal_datafusion_err!(
            "stack cannot be empty during upward traversal"
        ))?;

        let candidate = self
            .candidates
            .get(&cur_id)
            .ok_or(internal_datafusion_err!("can't find candidate"))?;

        if candidate.is_transformed {
            Ok(Transformed::yes(candidate.node.plan.clone()))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
