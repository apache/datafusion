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
