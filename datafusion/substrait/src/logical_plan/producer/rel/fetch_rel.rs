// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::DFSchema;
use datafusion::logical_expr::Limit;
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{FetchRel, Rel};

pub fn from_limit(
    producer: &mut impl SubstraitProducer,
    limit: &Limit,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(limit.input.as_ref())?;
    let empty_schema = Arc::new(DFSchema::empty());

    let offset_expr = limit
        .skip
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new);
    let count_expr = limit
        .fetch
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new);
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Fetch(Box::new(FetchRel {
            common: None,
            input: Some(input),
            offset_expr,
            count_expr,
            advanced_extension: None,
        }))),
    }))
}
