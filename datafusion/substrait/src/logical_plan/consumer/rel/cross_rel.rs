// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::SubstraitConsumer;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};

use datafusion::logical_expr::requalify_sides_if_needed;

use substrait::proto::CrossRel;

pub async fn from_cross_rel(
    consumer: &impl SubstraitConsumer,
    cross: &CrossRel,
) -> datafusion::common::Result<LogicalPlan> {
    let [left, right] = consumer
        .consume_rel_inputs(&[
            cross.left.as_ref().unwrap(),
            cross.right.as_ref().unwrap(),
        ])
        .await?
        .try_into()
        .unwrap();
    let left = LogicalPlanBuilder::from(left);
    let right = LogicalPlanBuilder::from(right);
    let (left, right, _requalified) = requalify_sides_if_needed(left, right)?;
    left.cross_join(right.build()?)?.build()
}
