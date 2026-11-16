// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::{
    DefaultSubstraitProducer, SubstraitProducer, to_substrait_named_struct,
};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, SubqueryAlias};
use substrait::proto::{Plan, PlanRel, Rel, RelRoot, plan_rel};
use substrait::version;

/// Convert DataFusion LogicalPlan to Substrait Plan
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
