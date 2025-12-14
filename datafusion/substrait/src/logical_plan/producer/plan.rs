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

use crate::logical_plan::producer::{
    to_substrait_named_struct, DefaultSubstraitProducer, SubstraitProducer,
};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, SubqueryAlias};
use substrait::proto::{plan_rel, Plan, PlanRel, Rel, RelRoot};
use substrait::version;

/// Convert DataFusion LogicalPlan to Substrait Plan
// Silence deprecation warnings for `extension_uris` during the uri -> urn migration
// See: https://github.com/substrait-io/substrait/issues/856
#[expect(deprecated)]
pub fn to_substrait_plan(
    plan: &LogicalPlan,
    state: &SessionState,
) -> datafusion::common::Result<Box<Plan>> {
    // Parse relation nodes
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported

    let mut producer: DefaultSubstraitProducer = DefaultSubstraitProducer::new(state);
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(RelRoot {
            input: Some(*producer.handle_plan(plan)?),
            names: to_substrait_named_struct(&mut producer, plan.schema())?.names,
        })),
    }];

    // Return parsed plan
    let extensions = producer.get_extensions();
    Ok(Box::new(Plan {
        version: Some(version::version_with_producer("datafusion")),
        extension_uris: vec![],
        extension_urns: vec![],
        extensions: extensions.into(),
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
        parameter_bindings: vec![],
        type_aliases: vec![],
    }))
}

pub fn from_subquery_alias(
    producer: &mut impl SubstraitProducer,
    alias: &SubqueryAlias,
) -> datafusion::common::Result<Box<Rel>> {
    // Do nothing if encounters SubqueryAlias
    // since there is no corresponding relation type in Substrait
    producer.handle_plan(alias.input.as_ref())
}
