// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::SubstraitProducer;
use datafusion::logical_expr::Union;
use substrait::proto::rel::RelType;
use substrait::proto::{Rel, SetRel, set_rel};

pub fn from_union(
    producer: &mut impl SubstraitProducer,
    union: &Union,
) -> datafusion::common::Result<Box<Rel>> {
    let input_rels = union
        .inputs
        .iter()
        .map(|input| producer.handle_plan(input.as_ref()))
        .collect::<datafusion::common::Result<Vec<_>>>()?
        .into_iter()
        .map(|ptr| *ptr)
        .collect();
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Set(SetRel {
            common: None,
            inputs: input_rels,
            op: set_rel::SetOp::UnionAll as i32, // UNION DISTINCT gets translated to AGGREGATION + UNION ALL
            advanced_extension: None,
        })),
    }))
}
