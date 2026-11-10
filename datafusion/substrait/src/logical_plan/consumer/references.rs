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

//! Resolves references against the enclosing plan and rejects cyclic references.

use super::SubstraitConsumer;
use datafusion::common::{Result, not_impl_err, substrait_datafusion_err, substrait_err};
use datafusion::logical_expr::LogicalPlan;
use std::collections::HashSet;
use std::sync::Mutex;
use substrait::proto::{Plan, ReferenceRel, plan_rel};

pub(super) struct References<'a> {
    plan: &'a Plan,
    visiting: Mutex<HashSet<i32>>,
}

impl<'a> References<'a> {
    pub(super) fn new(plan: &'a Plan) -> Self {
        Self {
            plan,
            visiting: Mutex::new(HashSet::new()),
        }
    }

    pub(super) async fn consume(
        &self,
        consumer: &impl SubstraitConsumer,
        reference: &ReferenceRel,
    ) -> Result<LogicalPlan> {
        let ordinal = reference.subtree_ordinal;
        let relation = usize::try_from(ordinal)
            .ok()
            .and_then(|index| self.plan.relations.get(index))
            .ok_or_else(|| {
                substrait_datafusion_err!("Invalid subtree ordinal {ordinal}")
            })?;
        let input = match relation.rel_type.as_ref() {
            Some(plan_rel::RelType::Rel(rel)) => rel,
            Some(plan_rel::RelType::Root(root)) => {
                // ReferenceRel ignores the output names of a referenced root.
                root.input.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!(
                        "Referenced root {ordinal} is missing its input"
                    )
                })?
            }
            None => return substrait_err!("Referenced relation {ordinal} is empty"),
        };
        if !self.visiting.lock().unwrap().insert(ordinal) {
            return substrait_err!("Cyclic Substrait reference at subtree {ordinal}");
        }
        // Keep only the active recursion path, so repeated references are valid.
        // The guard also removes the visit when conversion errors or is cancelled.
        let _visit = ReferenceVisit {
            visiting: &self.visiting,
            ordinal,
        };
        let body = consumer.consume_rel(input).await?;
        if body.contains_outer_reference() {
            return not_impl_err!(
                "Correlated shared Substrait relations are not supported"
            );
        }
        Ok(body)
    }
}

struct ReferenceVisit<'a> {
    visiting: &'a Mutex<HashSet<i32>>,
    ordinal: i32,
}

impl Drop for ReferenceVisit<'_> {
    fn drop(&mut self) {
        self.visiting.lock().unwrap().remove(&self.ordinal);
    }
}
