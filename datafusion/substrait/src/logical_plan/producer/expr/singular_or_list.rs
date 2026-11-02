// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::{SubstraitProducer, negate};
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::expr::InList;
use substrait::proto::Expression;
use substrait::proto::expression::{RexType, SingularOrList};

pub fn from_in_list(
    producer: &mut impl SubstraitProducer,
    in_list: &InList,
    schema: &DFSchemaRef,
) -> datafusion::common::Result<Expression> {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let substrait_list = list
        .iter()
        .map(|x| producer.handle_expr(x, schema))
        .collect::<datafusion::common::Result<Vec<Expression>>>()?;
    let substrait_expr = producer.handle_expr(expr, schema)?;

    let substrait_or_list = Expression {
        rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
            value: Some(Box::new(substrait_expr)),
            options: substrait_list,
        }))),
    };

    if *negated {
        Ok(negate(producer, substrait_or_list))
    } else {
        Ok(substrait_or_list)
    }
}
