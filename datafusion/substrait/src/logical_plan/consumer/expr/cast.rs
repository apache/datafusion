// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::consumer::{
    SubstraitConsumer, field_from_substrait_type_without_names,
};
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
            let field = field_from_substrait_type_without_names(consumer, output_type)?;
            if cast.failure_behavior() == ReturnNull {
                Ok(Expr::TryCast(TryCast::new_from_field(input_expr, field)))
            } else {
                Ok(Expr::Cast(Cast::new_from_field(input_expr, field)))
            }
        }
        None => substrait_err!("Cast expression without output type is not allowed"),
    }
}
