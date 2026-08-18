// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
