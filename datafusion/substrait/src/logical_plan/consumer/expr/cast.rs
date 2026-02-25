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

use crate::logical_plan::consumer::SubstraitConsumer;
use crate::logical_plan::consumer::types::from_substrait_type_without_names;
use datafusion::common::{DFSchema, substrait_err};
use datafusion::logical_expr::{Cast, Expr, TryCast};
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::cast::FailureBehavior::ReturnNull;

pub async fn from_cast(
    consumer: &impl SubstraitConsumer,
    cast: &substrait_expression::Cast,
    input_schema: &DFSchema,
) -> datafusion::common::Result<Expr> {
    match cast.r#type.as_ref() {
        Some(output_type) => {
            let input_expr = Box::new(
                consumer
                    .consume_expression(
                        cast.input.as_ref().unwrap().as_ref(),
                        input_schema,
                    )
                    .await?,
            );
            let data_type = from_substrait_type_without_names(consumer, output_type)?;
            if cast.failure_behavior() == ReturnNull {
                Ok(Expr::TryCast(TryCast::new(input_expr, data_type)))
            } else {
                Ok(Expr::Cast(Cast::new(input_expr, data_type)))
            }
        }
        None => substrait_err!("Cast expression without output type is not allowed"),
    }
}
