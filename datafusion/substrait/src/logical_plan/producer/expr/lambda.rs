// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::{common::DFSchemaRef, logical_expr::expr::Lambda};
use substrait::proto::{
    Expression,
    expression::RexType,
    r#type::{Nullability, Struct},
};

use crate::logical_plan::producer::SubstraitProducer;

pub fn from_lambda(
    producer: &mut impl SubstraitProducer,
    lambda: &Lambda,
    schema: &DFSchemaRef,
) -> Result<Expression, datafusion::error::DataFusionError> {
    Ok(Expression {
        rex_type: Some(RexType::Lambda(Box::new(
            substrait::proto::expression::Lambda {
                parameters: Some(Struct {
                    nullability: Nullability::Required as i32,
                    type_variation_reference: 0,
                    types: lambda
                        .params
                        .iter()
                        .map(|p| producer.lambda_parameter_type(p))
                        .collect::<datafusion::error::Result<_>>()?,
                }),
                body: Some(Box::new(producer.handle_expr(&lambda.body, schema)?)),
            },
        ))),
    })
}
