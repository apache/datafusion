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

use crate::delim_candidates_collector::{DelimCandidate, JoinWithDelimScan};

type ID = usize;

pub struct DelimCandidateRewriter {
    candidates: IndexMap<ID, DelimCandidate>,
    joins: IndexMap<ID, JoinWithDelimScan>,
    cur_id: ID,
    // all the node ids from root to the current node
    stack: Vec<usize>,
}

impl DelimCandidateRewriter {
    pub fn new(
        candidates: IndexMap<ID, DelimCandidate>,
        joins: IndexMap<ID, JoinWithDelimScan>,
    ) -> Self {
        Self {
            candidates,
            joins,
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
        let mut transformed = Transformed::no(plan);

        let cur_id = self.stack.pop().ok_or(internal_datafusion_err!(
            "stack cannot be empty during upward traversal"
        ))?;

        let mut diff = 0;
        if let Some(candidate) = self.candidates.get(&cur_id) {
            if candidate.is_transformed {
                return Ok(Transformed::yes(candidate.node.plan.clone()));
            }
        } else if let Some(join_with_delim_scan) = self.joins.get(&cur_id) {
            if join_with_delim_scan.can_be_eliminated {
                let prev_sub_plan_size = join_with_delim_scan.node.sub_plan_size;
                let mut cur_sub_plan_size = 1;
                if join_with_delim_scan.is_filter_generated {
                    cur_sub_plan_size += 1;
                }

                // perv_sub_plan_size should be larger than cur_sub_plan_size.
                diff = prev_sub_plan_size - cur_sub_plan_size;

                transformed = Transformed::yes(
                    join_with_delim_scan
                        .replacement_plan
                        .clone()
                        .ok_or(internal_datafusion_err!(
                            "stack cannot be empty during upward traversal"
                        ))?
                        .as_ref()
                        .clone(),
                );
            }
        }

        // update tree nodes with id > cur_id.
        if diff != 0 {
            let keys_to_update: Vec<usize> = self
                .joins
                .keys()
                .filter(|&&id| id > cur_id)
                .copied()
                .collect();
            for old_id in keys_to_update {
                if let Some(mut join) = self.joins.swap_remove(&old_id) {
                    let new_id = old_id - diff;
                    join.node.id = new_id;
                    self.joins.insert(new_id, join);
                }
            }
        }

        Ok(transformed)
    }
}
