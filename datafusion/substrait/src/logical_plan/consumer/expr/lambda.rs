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

use datafusion::{
    common::{DFSchema, substrait_err},
    prelude::{Expr, lambda},
};
use substrait::proto;

use crate::logical_plan::consumer::SubstraitConsumer;

pub async fn from_lambda(
    consumer: &impl SubstraitConsumer,
    expr: &proto::expression::Lambda,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    let Some(parameters) = expr.parameters.as_ref() else {
        return substrait_err!("Lambda expression without parameters is not allowed");
    };

    let (names, consumer_with_parameters) =
        consumer.with_lambda_parameters(&parameters.types, input_schema)?;

    let Some(body) = expr.body.as_ref() else {
        return substrait_err!("Lambda expression without body is not allowed");
    };

    let body = consumer_with_parameters
        .consume_expression(body, input_schema)
        .await?;

    Ok(lambda(names, body))
}
