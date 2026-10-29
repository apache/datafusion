// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::logical_expr::Filter;
use substrait::proto::rel::RelType;
use substrait::proto::{FilterRel, Rel};

pub fn from_filter(
    producer: &mut impl SubstraitProducer,
    filter: &Filter,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(filter.input.as_ref())?;
    let filter_expr = producer.handle_expr(&filter.predicate, filter.input.schema())?;
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Filter(Box::new(FilterRel {
            common: None,
            input: Some(input),
            condition: Some(Box::new(filter_expr)),
            advanced_extension: None,
        }))),
    }))
}
