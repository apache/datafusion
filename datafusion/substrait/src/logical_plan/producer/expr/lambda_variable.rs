// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use datafusion::logical_expr::expr::LambdaVariable;
use substrait::proto::{
    Expression,
    expression::{
        FieldReference, ReferenceSegment, RexType,
        field_reference::{LambdaParameterReference, ReferenceType, RootType},
        reference_segment::{self, StructField},
    },
};

use crate::logical_plan::producer::SubstraitProducer;

pub fn from_lambda_variable(
    producer: &mut impl SubstraitProducer,
    lambda_variable: &LambdaVariable,
    _schema: &datafusion::common::DFSchema,
) -> Result<Expression, datafusion::error::DataFusionError> {
    let (steps_out, field) = producer.lambda_variable(&lambda_variable.name)?;

    Ok(Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(
                    Box::new(StructField { field, child: None }),
                )),
            })),
            root_type: Some(RootType::LambdaParameterReference(
                LambdaParameterReference { steps_out },
            )),
        }))),
    })
}
