// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::common::DFSchema;
use datafusion::logical_expr::Limit;
use std::sync::Arc;
use substrait::proto::rel::RelType;
use substrait::proto::{FetchRel, Rel, fetch_rel};

pub fn from_limit(
    producer: &mut impl SubstraitProducer,
    limit: &Limit,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(limit.input.as_ref())?;
    let empty_schema = Arc::new(DFSchema::empty());

    let offset_mode = limit
        .skip
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::OffsetMode::OffsetExpr);
    let count_mode = limit
        .fetch
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::CountMode::CountExpr);
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Fetch(Box::new(FetchRel {
            common: None,
            input: Some(input),
            offset_mode,
            count_mode,
            advanced_extension: None,
        }))),
    }))
}
