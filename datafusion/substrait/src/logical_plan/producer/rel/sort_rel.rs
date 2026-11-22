// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::{SubstraitProducer, substrait_sort_field};
use crate::variation_const::DEFAULT_TYPE_VARIATION_REF;
use datafusion::logical_expr::Sort;
use substrait::proto::expression::literal::LiteralType;
use substrait::proto::expression::{Literal, RexType};
use substrait::proto::rel::RelType;
use substrait::proto::{Expression, FetchRel, Rel, SortRel, fetch_rel};

pub fn from_sort(
    producer: &mut impl SubstraitProducer,
    sort: &Sort,
) -> datafusion::common::Result<Box<Rel>> {
    let Sort { expr, input, fetch } = sort;
    let sort_fields = expr
        .iter()
        .map(|e| substrait_sort_field(producer, e, input.schema()))
        .collect::<datafusion::common::Result<Vec<_>>>()?;

    let input = producer.handle_plan(input.as_ref())?;

    let sort_rel = Box::new(Rel {
        rel_type: Some(RelType::Sort(Box::new(SortRel {
            common: None,
            input: Some(input),
            sorts: sort_fields,
            advanced_extension: None,
        }))),
    });

    match fetch {
        Some(amount) => {
            let count_mode =
                Some(fetch_rel::CountMode::CountExpr(Box::new(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: false,
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        literal_type: Some(LiteralType::I64(*amount as i64)),
                    })),
                })));
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                    common: None,
                    input: Some(sort_rel),
                    offset_mode: None,
                    count_mode,
                    advanced_extension: None,
                }))),
            }))
        }
        None => Ok(sort_rel),
    }
}
